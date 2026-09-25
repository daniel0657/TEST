package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Process Supervisor Configuration Constants
// ---------------------------------------------------------------------------

const (
	defaultXrayBin      = "/usr/local/bin/xray"
	defaultConfigPath   = "/app/config.json"
	defaultAssetDir     = "/usr/local/share/xray"
	defaultXrayMemMB    = 640
	defaultLoopbackWS   = "127.0.0.1:18444"
	defaultLoopbackXH   = "127.0.0.1:18443"
	defaultProbeTimeout = 200 * time.Millisecond
	defaultStartTimeout = 10 * time.Second
	defaultStopTimeout  = 8 * time.Second
	stableWindow        = 60 * time.Second
	maxBackoffInterval  = 5 * time.Second
	initialBackoff      = 100 * time.Millisecond
)

// SupervisorHealthSnapshot captures instantaneous telemetry for /healthz.
type SupervisorHealthSnapshot struct {
	Running     bool   `json:"running"`
	Ready       bool   `json:"ready"`
	Restarts    int32  `json:"restarts"`
	UptimeSec   int64  `json:"uptime_sec"`
	WSInbound   bool   `json:"ws_inbound_ok"`
	XHInbound   bool   `json:"xh_inbound_ok"`
	LastProbeAt string `json:"last_probe_at"`
}

// Supervisor manages the lifecycle, execution, telemetry, and graceful
// teardown of the Xray-core daemon process.
type Supervisor struct {
	binPath      string
	configPath   string
	assetDir     string
	memLimitMB   int
	wsAddr       string
	xhAddr       string
	startTimeout time.Duration
	stopTimeout  time.Duration

	mu        sync.Mutex
	cmd       *exec.Cmd
	startedAt time.Time

	running   atomic.Bool
	ready     atomic.Bool
	restarts  atomic.Int32
	stopFlag  atomic.Bool
	stopOnce  atomic.Bool
	stopped   chan struct{}

	wsOk atomic.Bool
	xhOk atomic.Bool
}

// NewSupervisor initializes the supervisor with hardened production defaults
// and environment variable overrides.
func NewSupervisor() *Supervisor {
	bin := getEnv("BERMUDA_XRAY_BIN", defaultXrayBin)
	cfg := getEnv("BERMUDA_XRAY_CONFIG", defaultConfigPath)
	assets := getEnv("XRAY_LOCATION_ASSET", defaultAssetDir)
	ws := getEnv("BERMUDA_BACKEND_WS", defaultLoopbackWS)
	xh := getEnv("BERMUDA_BACKEND_XH", defaultLoopbackXH)

	return &Supervisor{
		binPath:      bin,
		configPath:   cfg,
		assetDir:     assets,
		memLimitMB:   defaultXrayMemMB,
		wsAddr:       ws,
		xhAddr:       xh,
		startTimeout: defaultStartTimeout,
		stopTimeout:  defaultStopTimeout,
		stopped:      make(chan struct{}),
	}
}

// IsRunning reports whether the child Xray process is currently alive.
func (s *Supervisor) IsRunning() bool {
	return s.running.Load()
}

// IsReady reports whether both loopback inbounds (WS & XHTTP) are accepting traffic.
func (s *Supervisor) IsReady() bool {
	return s.ready.Load()
}

// Restarts returns the cumulative unexpected exit count.
func (s *Supervisor) Restarts() int32 {
	return s.restarts.Load()
}

// Preflight executes a dry-run configuration syntax test before spawning.
func (s *Supervisor) Preflight() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, s.binPath, "run", "-test", "-c", s.configPath)
	cmd.Env = append(os.Environ(), fmt.Sprintf("XRAY_LOCATION_ASSET=%s", s.assetDir))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("xray preflight validation failed: %w, output: %s", err, strings.TrimSpace(string(out)))
	}
	log.Printf("[Supervisor] Preflight validation passed for %s", s.configPath)
	return nil
}

// Run executes the continuous supervision loop until ctx is canceled.
func (s *Supervisor) Run(ctx context.Context) error {
	defer close(s.stopped)

	backoff := initialBackoff

	for {
		if s.stopFlag.Load() || ctx.Err() != nil {
			return nil
		}

		err := s.startAndWait(ctx)
		if err == nil {
			// Graceful termination requested and completed cleanly.
			return nil
		}

		if s.stopFlag.Load() || ctx.Err() != nil {
			return nil
		}

		s.restarts.Add(1)

		s.mu.Lock()
		uptime := time.Since(s.startedAt)
		s.mu.Unlock()

		// If process was stable for over stableWindow, reset backoff.
		if uptime >= stableWindow {
			backoff = initialBackoff
		}

		log.Printf("[Supervisor] Xray exited unexpectedly after %s (err: %v). Scheduling restart (restarts=%d, backoff=%s)...",
			uptime.Round(time.Millisecond), err, s.restarts.Load(), backoff)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoffInterval {
			backoff = maxBackoffInterval
		}
	}
}

// startAndWait spawns a single Xray child process and blocks until it exits.
func (s *Supervisor) startAndWait(ctx context.Context) error {
	s.mu.Lock()
	cmd := exec.Command(s.binPath, "run", "-c", s.configPath)
	
	// Linux Process Group isolation & kernel cleanup flag
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGKILL,
	}

	// Deterministic memory partitioning: Inject dedicated 640MiB ceiling
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("XRAY_LOCATION_ASSET=%s", s.assetDir),
		fmt.Sprintf("GOMEMLIMIT=%dMiB", s.memLimitMB),
		"GODEBUG=madvdontneed=1",
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("stdout pipe creation error: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("stderr pipe creation error: %w", err)
	}

	if err := cmd.Start(); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to start xray process: %w", err)
	}

	s.cmd = cmd
	s.startedAt = time.Now()
	pid := cmd.Process.Pid
	s.mu.Unlock()

	s.running.Store(true)
	s.ready.Store(false)
	log.Printf("[Supervisor] Xray child spawned (pid=%d, pgid=%d, GOMEMLIMIT=%dMiB)", pid, pid, s.memLimitMB)

	// Stream stdout and stderr asynchronously through structured logger
	go s.pumpPipe(stdout, "[Xray-Out]")
	go s.pumpPipe(stderr, "[Xray-Err]")

	// Trigger asynchronous loopback readiness verification
	go s.awaitReadiness(ctx)

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	select {
	case err := <-waitCh:
		s.mu.Lock()
		s.cmd = nil
		s.mu.Unlock()

		s.running.Store(false)
		s.ready.Store(false)
		s.wsOk.Store(false)
		s.xhOk.Store(false)

		if s.stopFlag.Load() {
			log.Printf("[Supervisor] Xray child (pid=%d) terminated cleanly during shutdown", pid)
			return nil
		}
		return fmt.Errorf("xray child (pid=%d) terminated: %w", pid, err)

	case <-ctx.Done():
		s.stopChild(s.stopTimeout)
		<-waitCh // Structural reaping: guarantees zombie elimination
		s.mu.Lock()
		s.cmd = nil
		s.mu.Unlock()
		s.running.Store(false)
		s.ready.Store(false)
		return nil
	}
}

// pumpPipe reads lines from process pipes and outputs them to gateway logs.
func (s *Supervisor) pumpPipe(r io.Reader, prefix string) {
	scanner := bufio.NewScanner(r)
	// 256KB buffer to handle large JSON lines without crashing
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 256*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			log.Printf("%s %s", prefix, line)
		}
	}
}

// awaitReadiness polls loopback inbounds until both accept TCP handshakes.
func (s *Supervisor) awaitReadiness(ctx context.Context) {
	deadline := time.Now().Add(s.startTimeout)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}

		if !s.running.Load() {
			return
		}

		wsOK := probeTCP(s.wsAddr, defaultProbeTimeout)
		xhOK := probeTCP(s.xhAddr, defaultProbeTimeout)

		s.wsOk.Store(wsOK)
		s.xhOk.Store(xhOK)

		if wsOK && xhOK {
			s.ready.Store(true)
			log.Printf("[Supervisor] Loopback inbounds verified ready (WS=%s, XHTTP=%s)", s.wsAddr, s.xhAddr)
			return
		}
	}

	log.Printf("[Supervisor] Warning: Readiness probe timed out after %s. Inbounds still converging.", s.startTimeout)
}

// probeTCP performs an isolated dial test on loopback port.
func probeTCP(addr string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// ProbeNow executes an active on-demand health check for /healthz.
func (s *Supervisor) ProbeNow() (bool, bool) {
	if !s.running.Load() {
		return false, false
	}
	ws := probeTCP(s.wsAddr, defaultProbeTimeout)
	xh := probeTCP(s.xhAddr, defaultProbeTimeout)
	s.wsOk.Store(ws)
	s.xhOk.Store(xh)
	return ws, xh
}

// Snapshot gathers instantaneous health status atomically.
func (s *Supervisor) Snapshot() SupervisorHealthSnapshot {
	s.mu.Lock()
	started := s.startedAt
	s.mu.Unlock()

	var uptime int64
	if s.running.Load() && !started.IsZero() {
		uptime = int64(time.Since(started).Seconds())
	}

	return SupervisorHealthSnapshot{
		Running:     s.running.Load(),
		Ready:       s.ready.Load(),
		Restarts:    s.restarts.Load(),
		UptimeSec:   uptime,
		WSInbound:   s.wsOk.Load(),
		XHInbound:   s.xhOk.Load(),
		LastProbeAt: time.Now().UTC().Format(time.RFC3339),
	}
}

// stopChild sends SIGTERM to the process group, escalates to SIGKILL if necessary,
// and guarantees process reaping.
func (s *Supervisor) stopChild(timeout time.Duration) {
	if !s.stopOnce.CompareAndSwap(false, true) {
		return
	}
	s.stopFlag.Store(true)

	s.mu.Lock()
	cmd := s.cmd
	s.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return
	}

	pid := cmd.Process.Pid
	log.Printf("[Supervisor] Initiating graceful termination for process group -%d (timeout=%s)", pid, timeout)

	// Send SIGTERM to the entire process group
	_ = syscall.Kill(-pid, syscall.SIGTERM)

	waitDone := make(chan struct{})
	go func() {
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			close(waitDone)
			return
		}
		// Poll state briefly
		for i := 0; i < int(timeout/time.Millisecond); i++ {
			if !s.running.Load() {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		close(waitDone)
	}()

	select {
	case <-waitDone:
		log.Printf("[Supervisor] Process group -%d exited gracefully via SIGTERM", pid)
		return
	case <-time.After(timeout):
		log.Printf("[Supervisor] Grace period exceeded. Escalating to SIGKILL on process group -%d", pid)
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}
}

// Stop initiates child termination and blocks until the supervisor loop finishes.
func (s *Supervisor) Stop(timeout time.Duration) {
	s.stopChild(timeout)
	select {
	case <-s.stopped:
	case <-time.After(timeout + 2*time.Second):
	}
}
