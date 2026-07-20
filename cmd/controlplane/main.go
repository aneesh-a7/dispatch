// Command controlplane runs the dispatch control plane: the HTTP API,
// the durable job/worker store, the scheduler, and the dead-worker
// reaper. It is the single stateful component in the system: workers
// and the CLI are stateless and can be killed and restarted freely.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aneesh/dispatch/internal/api"
	"github.com/aneesh/dispatch/internal/scheduler"
	"github.com/aneesh/dispatch/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dataDir := flag.String("data-dir", "./data", "directory for the durable WAL")
	heartbeatTTL := flag.Duration("heartbeat-ttl", 15*time.Second, "how long a worker can go silent before it's considered dead")
	reapInterval := flag.Duration("reap-interval", 5*time.Second, "how often to sweep for dead workers")
	flag.Parse()

	st, err := store.Open(*dataDir)
	if err != nil {
		log.Fatalf("controlplane: opening store: %v", err)
	}
	defer st.Close()
	log.Printf("controlplane: store opened at %s (%d jobs, %d workers loaded from WAL)",
		*dataDir, len(st.ListJobs()), len(st.ListWorkers()))

	sched := scheduler.New(st)

	reaper := scheduler.NewReaper(st, *heartbeatTTL, *reapInterval)
	reaperStop := make(chan struct{})
	go reaper.Run(reaperStop)

	srv := api.NewServer(st, sched)
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           srv,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("controlplane: listening on %s", *addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("controlplane: server error: %v", err)
		}
	}()

	// Graceful shutdown: on SIGINT/SIGTERM, stop accepting new work and
	// let in-flight requests finish before exiting, rather than dropping
	// connections mid-request.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Println("controlplane: shutdown signal received, draining...")

	close(reaperStop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("controlplane: graceful shutdown failed: %v", err)
	}
	log.Println("controlplane: stopped")
}
