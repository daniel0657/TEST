package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Reverse Proxy & Routing Constants
// ---------------------------------------------------------------------------

const (
	defaultPathXH = "/bermuda-xhttp"
	defaultPathWS = "/bermuda-ws"
)

// HealthResponse models the JSON payload emitted by the /healthz endpoint.
type HealthResponse struct {
	Status     string                   `json:"status"`
	Draining   bool                     `json:"draining"`
	Healthy    bool                     `json:"healthy"`
	UptimeSec  int64                    `json:"uptime_sec"`
	Supervisor SupervisorHealthSnapshot `json:"supervisor"`
	Telemetry  TelemetrySnapshot        `json:"telemetry"`
}

// TelemetrySnapshot provides real-time connection metrics for the gateway.
type TelemetrySnapshot struct {
	OpenConnections   int64 `json:"open_connections"`
	TunnelConnections int64 `json:"tunnel_connections"`
	TotalRequests     int64 `json:"total_requests"`
}

// Gateway encapsulates the edge reverse-proxy router, connection trackers,
// health endpoints, and the decoy camouflage layer.
type Gateway struct {
	sup       *Supervisor
	pathXH    string
	pathWS    string
	backendXH string
	backendWS string

	xhProxy *httputil.ReverseProxy
	wsProxy *httputil.ReverseProxy

	draining    atomic.Bool
	openConns   atomic.Int64
	tunnelConns atomic.Int64
	totalReq    atomic.Int64
	startedAt   time.Time
}

// NewGateway constructs and configures the streaming reverse proxy gateway.
func NewGateway(sup *Supervisor) *Gateway {
	pathXH := getEnv("BERMUDA_PATH_XH", defaultPathXH)
	pathWS := getEnv("BERMUDA_PATH_WS", defaultPathWS)
	backendXH := getEnv("BERMUDA_BACKEND_XH", defaultLoopbackXH)
	backendWS := getEnv("BERMUDA_BACKEND_WS", defaultLoopbackWS)

	tr := newLoopbackTransport()

	gw := &Gateway{
		sup:         sup,
		pathXH:      pathXH,
		pathWS:      pathWS,
		backendXH:   backendXH,
		backendWS:   backendWS,
		xhProxy:     newBackendProxy(backendXH, tr),
		wsProxy:     newBackendProxy(backendWS, tr),
		startedAt:   time.Now(),
	}

	return gw
}

// newLoopbackTransport creates an HTTP transport hyper-tuned for 1-2 concurrent
// line-rate tunnel tenants:
//   - DisableCompression: avoids CPU burn and latency on encrypted payloads;
//   - 32KB Read/Write buffers: reduces kernel syscall overhead during bulk transfers;
//   - Generous connection pool: eliminates reconnection churn on XHTTP POST bursts.
func newLoopbackTransport() *http.Transport {
	dialer := &net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	return &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   96,
		MaxConnsPerHost:       256,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true,
		ReadBufferSize:        32 * 1024, // 32 KB
		WriteBufferSize:       32 * 1024, // 32 KB
		ResponseHeaderTimeout: 60 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// newBackendProxy instantiates a ReverseProxy configured for zero-latency streaming.
// FlushInterval: -1 guarantees immediate, unbuffered chunk propagation for XHTTP.
func newBackendProxy(targetAddr string, tr http.RoundTripper) *httputil.ReverseProxy {
	targetURL, _ := url.Parse("http://" + targetAddr)

	return &httputil.ReverseProxy{
		Transport:     tr,
		FlushInterval: -1,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = targetURL.Scheme
			pr.Out.URL.Host = targetURL.Host
			pr.Out.Host = pr.In.Host
			pr.SetXForwarded()
			pr.Out.Header.Set("X-Accel-Buffering", "no")
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("[Proxy Error] Backend %s unreachable: %v", targetAddr, err)
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "502 Bad Gateway\n")
		},
	}
}

// SetDraining signals the gateway to enter a graceful termination posture.
// /healthz immediately begins returning 503 to shed new edge requests.
func (g *Gateway) SetDraining() {
	g.draining.Store(true)
}

// IsDraining reports whether the gateway is draining in-flight connections.
func (g *Gateway) IsDraining() bool {
	return g.draining.Load()
}

// TrackConnState monitors connection transitions for edge observability.
func (g *Gateway) TrackConnState(_ net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		g.openConns.Add(1)
	case http.StateClosed:
		g.openConns.Add(-1)
	case http.StateHijacked:
		g.openConns.Add(-1)
		g.tunnelConns.Add(1)
	}
}

// Handler returns the primary HTTP multiplexer implementing hot-path bypass
// and camouflage decoys.
func (g *Gateway) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.totalReq.Add(1)

		// 1. Healthcheck Endpoint (Railway Platform Probe)
		if r.URL.Path == "/healthz" {
			g.handleHealthz(w, r)
			return
		}

		// 2. XHTTP Hot-Path (Pass native unwrapped ResponseWriter for zero-buffer streaming)
		if r.URL.Path == g.pathXH || strings.HasPrefix(r.URL.Path, g.pathXH+"/") {
			g.xhProxy.ServeHTTP(w, r)
			return
		}

		// 3. WebSocket Hot-Path (Pass native unwrapped ResponseWriter for 101 Switching Protocols)
		if r.URL.Path == g.pathWS || strings.HasPrefix(r.URL.Path, g.pathWS+"/") {
			g.wsProxy.ServeHTTP(w, r)
			return
		}

		// 4. Active Camouflage: Fake OpenResty 404 for all probing traffic
		camo404(w, r)
	})
}

// handleHealthz responds to orchestration health probes with detailed system status.
func (g *Gateway) handleHealthz(w http.ResponseWriter, r *http.Request) {
	snap := g.sup.Snapshot()
	draining := g.draining.Load()

	healthy := !draining && snap.Running && snap.Ready
	statusStr := "ok"

	code := http.StatusOK
	if draining {
		code = http.StatusServiceUnavailable
		statusStr = "draining"
	} else if !snap.Running {
		code = http.StatusServiceUnavailable
		statusStr = "down"
	} else if !snap.Ready {
		code = http.StatusServiceUnavailable
		statusStr = "starting"
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.WriteHeader(code)

	payload := HealthResponse{
		Status:     statusStr,
		Draining:   draining,
		Healthy:    healthy,
		UptimeSec:  int64(time.Since(g.startedAt).Seconds()),
		Supervisor: snap,
		Telemetry: TelemetrySnapshot{
			OpenConnections:   g.openConns.Load(),
			TunnelConnections: g.tunnelConns.Load(),
			TotalRequests:     g.totalReq.Load(),
		},
	}

	_ = json.NewEncoder(w).Encode(payload)
}

// camo404 renders an authentic OpenResty 404 response to deter passive network scanners.
func camo404(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Server", "openresty")
	w.WriteHeader(http.StatusNotFound)
	_, _ = io.WriteString(w, "<html>\r\n<head><title>404 Not Found</title></head>\r\n<body>\r\n<center><h1>404 Not Found</h1></center>\r\n<hr><center>openresty</center>\r\n</body>\r\n</html>\r\n")
}

// ---------------------------------------------------------------------------
// Transparent ResponseWriter Interface Wrappers
// ---------------------------------------------------------------------------

// tunnelResponseWriter provides full interface compatibility for Flusher,
// Hijacker, and zero-copy io.ReaderFrom.
type tunnelResponseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (w *tunnelResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.statusCode = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *tunnelResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.statusCode = http.StatusOK
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(b)
}

func (w *tunnelResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *tunnelResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, errors.New("underlying ResponseWriter does not support Hijack")
}

func (w *tunnelResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		if !w.wroteHeader {
			w.statusCode = http.StatusOK
			w.wroteHeader = true
		}
		return rf.ReadFrom(r)
	}
	return io.Copy(w.ResponseWriter, r)
}

// getEnv retrieves environment variables with a safe fallback value.
func getEnv(key, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}
