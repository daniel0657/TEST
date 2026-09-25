package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Gateway Entrypoint & Lifecycle Constants
// ---------------------------------------------------------------------------

const (
	defaultPort        = "8080"
	defaultSelfMemMB   = 128
	httpDrainTimeout   = 10 * time.Second
	supervisorStopWait = 8 * time.Second
)

// applyMemoryCeiling enforces a hard runtime soft-limit on heap allocations
// for the Go gateway process (128MiB), leaving the remainder of the 1GB
// instance budget to the Xray daemon (640MiB) and Linux kernel buffers (256MiB).
func applyMemoryCeiling(selfMB int) {
	debug.SetMemoryLimit(int64(selfMB) << 20)
	debug.SetGCPercent(50) // More aggressive GC to keep RSS tightly bound under bursts
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	log.Println("[Gateway] Initializing BERMUDA Stealth Gateway NG...")

	// 1. Enforce memory constraints for 2 vCPU / 1 GB PaaS runtime
	applyMemoryCeiling(defaultSelfMemMB)
	log.Printf("[Runtime] Gateway memory ceiling locked at %dMiB (GOGC=50)", defaultSelfMemMB)

	port := getEnv("PORT", defaultPort)

	// 2. Instantiate and preflight validate Xray daemon supervisor
	sup := NewSupervisor()
	if err := sup.Preflight(); err != nil {
		log.Printf("[Gateway] Warning: Supervisor preflight validation issue: %v. Continuing to start...", err)
	}

	// 3. Instantiate zero-buffer streaming reverse proxy gateway
	gw := NewGateway(sup)

	// 4. Capture container lifecycle termination signals (SIGTERM from Railway / SIGINT)
	rootCtx, stopRoot := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopRoot()

	// 5. Run supervisor loop in a dedicated background goroutine
	supErrCh := make(chan error, 1)
	go func() {
		supErrCh <- sup.Run(rootCtx)
	}()

	// 6. Configure HTTP edge server
	// Note: ReadTimeout and WriteTimeout are deliberately OMITTED.
	// VLESS XHTTP and WebSocket tunnels are long-lived, continuous bidirectional
	// streams that must never be severed by intermediate server deadlines.
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           gw.Handler(),
		ReadHeaderTimeout: 5 * time.Second, // Protects against Slowloris attacks
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    32 * 1024,
		ConnState:         gw.TrackConnState,
	}

	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("[Gateway] Edge listener active on :%s (PID %d)", port, os.Getpid())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	// 7. Await termination signal or unexpected fatal failures
	select {
	case err := <-serverErrCh:
		log.Printf("[Gateway] Fatal: HTTP server failure: %v", err)
		gw.SetDraining()
		sup.Stop(supervisorStopWait)
		os.Exit(1)

	case err := <-supErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[Gateway] Fatal: Supervisor loop halted unexpectedly: %v", err)
			gw.SetDraining()
			sup.Stop(supervisorStopWait)
			os.Exit(1)
		}

	case <-rootCtx.Done():
		log.Println("[Gateway] Termination signal (SIGTERM/SIGINT) intercepted. Commencing graceful drain sequence...")
	}

	// ---------------------------------------------------------------------------
	// Graceful Drain State Machine (Ordered Zero-Downtime Teardown)
	// ---------------------------------------------------------------------------

	// Stage 1: Immediately flip /healthz to 503 so Railway's edge mesh sheds incoming traffic
	gw.SetDraining()
	log.Println("[Gateway] Stage 1/3: /healthz flipped to 503 (draining active traffic)")

	// Stage 2: Allow in-flight connections to drain cleanly up to httpDrainTimeout
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), httpDrainTimeout)
	defer cancelDrain()

	if err := srv.Shutdown(drainCtx); err != nil {
		log.Printf("[Gateway] Stage 2/3 Warning: HTTP server drain timeout exceeded: %v. Forcing socket closure.", err)
		_ = srv.Close()
	} else {
		log.Println("[Gateway] Stage 2/3: All edge HTTP connections drained successfully.")
	}

	// Stage 3: Stop child process group cleanly and reap zombie state
	log.Println("[Gateway] Stage 3/3: Teardown child Xray process group...")
	sup.Stop(supervisorStopWait)

	log.Println("[Gateway] BERMUDA Stealth Gateway shutdown complete. Ports released cleanly. Exit 0.")
}
