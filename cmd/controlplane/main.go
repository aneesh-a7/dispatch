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
	"github.com/aneesh/dispatch/internal/config"
	"github.com/aneesh/dispatch/internal/notify"
	"github.com/aneesh/dispatch/internal/scheduler"
	"github.com/aneesh/dispatch/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dataDir := flag.String("data-dir", "./data", "directory for the durable WAL")
	token := flag.String("token", os.Getenv("DISPATCH_TOKEN"), "shared bearer token required on every request (empty disables auth; or $DISPATCH_TOKEN)")
	webhookURL := flag.String("webhook-url", os.Getenv("DISPATCH_WEBHOOK_URL"), "POST job JSON here when a job finishes (or $DISPATCH_WEBHOOK_URL)")
	tlsCert := flag.String("tls-cert", "", "path to a TLS certificate; serves HTTPS when set with -tls-key")
	tlsKey := flag.String("tls-key", "", "path to the TLS private key matching -tls-cert")
	heartbeatTTL := flag.Duration("heartbeat-ttl", 15*time.Second, "how long a worker can go silent before it's considered dead")
	reapInterval := flag.Duration("reap-interval", 5*time.Second, "how often to sweep for dead workers")
	compactInterval := flag.Duration("compact-interval", 1*time.Hour, "how often to compact the WAL")
	configPath := flag.String("config", "", "optional JSON config file; command-line flags override it")
	flag.Parse()

	if *configPath != "" {
		var cfg config.ControlPlane
		if err := config.Load(*configPath, &cfg); err != nil {
			log.Fatalf("controlplane: %v", err)
		}
		applyControlPlaneConfig(cfg, addr, dataDir, token, webhookURL, tlsCert, tlsKey,
			heartbeatTTL, reapInterval, compactInterval)
	}

	st, err := store.Open(*dataDir)
	if err != nil {
		log.Fatalf("controlplane: opening store: %v", err)
	}
	defer st.Close()
	log.Printf("controlplane: store opened at %s (%d jobs, %d workers loaded from WAL)",
		*dataDir, len(st.ListJobs()), len(st.ListWorkers()))

	notifier := notify.New(*webhookURL)
	if *webhookURL != "" {
		log.Printf("controlplane: job notifications will POST to %s", *webhookURL)
	}

	sched := scheduler.New(st)

	reaper := scheduler.NewReaper(st, notifier, *heartbeatTTL, *reapInterval)
	reaperStop := make(chan struct{})
	go reaper.Run(reaperStop)

	compactStop := make(chan struct{})
	go runCompactor(st, *compactInterval, compactStop)

	srv := api.NewServer(st, sched, api.Config{Token: *token, Notifier: notifier})
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           srv,
		ReadHeaderTimeout: 5 * time.Second,
	}

	if *token != "" {
		log.Println("controlplane: auth enabled (bearer token required; /v1/dev/spawn-worker disabled)")
	} else {
		log.Println("controlplane: auth DISABLED; anyone who can reach this port can run commands on your workers")
	}

	serveTLS := *tlsCert != "" && *tlsKey != ""
	if (*tlsCert == "") != (*tlsKey == "") {
		log.Fatal("controlplane: -tls-cert and -tls-key must be set together")
	}

	go func() {
		if serveTLS {
			log.Printf("controlplane: listening on %s (https)", *addr)
			if err := httpServer.ListenAndServeTLS(*tlsCert, *tlsKey); err != nil && err != http.ErrServerClosed {
				log.Fatalf("controlplane: server error: %v", err)
			}
			return
		}
		log.Printf("controlplane: listening on %s (http)", *addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("controlplane: server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Println("controlplane: shutdown signal received, draining...")

	close(reaperStop)
	close(compactStop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("controlplane: graceful shutdown failed: %v", err)
	}
	log.Println("controlplane: stopped")
}

// applyControlPlaneConfig fills in values from a config file for flags the
// user did not pass explicitly. Walking the actually-set flags (rather
// than comparing against defaults) is what makes "flags win over the file"
// exact: a flag passed as its default value still counts as passed.
func applyControlPlaneConfig(cfg config.ControlPlane, addr, dataDir, token, webhookURL, tlsCert, tlsKey *string,
	heartbeatTTL, reapInterval, compactInterval *time.Duration) {

	set := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { set[f.Name] = true })

	applyString := func(name string, from *string, to *string) {
		if from != nil && !set[name] {
			*to = *from
		}
	}
	applyString("addr", cfg.Addr, addr)
	applyString("data-dir", cfg.DataDir, dataDir)
	applyString("token", cfg.Token, token)
	applyString("webhook-url", cfg.WebhookURL, webhookURL)
	applyString("tls-cert", cfg.TLSCert, tlsCert)
	applyString("tls-key", cfg.TLSKey, tlsKey)

	applyDuration := func(name string, from *string, to *time.Duration) {
		if from == nil || set[name] {
			return
		}
		d, err := time.ParseDuration(*from)
		if err != nil {
			log.Fatalf("controlplane: config %s: %v", name, err)
		}
		*to = d
	}
	applyDuration("heartbeat-ttl", cfg.HeartbeatTTL, heartbeatTTL)
	applyDuration("reap-interval", cfg.ReapInterval, reapInterval)
	applyDuration("compact-interval", cfg.CompactInterval, compactInterval)
}

// runCompactor periodically compacts the WAL until stop is closed.
func runCompactor(st *store.Store, interval time.Duration, stop chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := st.Compact(); err != nil {
				log.Printf("controlplane: compaction failed: %v", err)
			} else {
				log.Println("controlplane: WAL compacted")
			}
		case <-stop:
			return
		}
	}
}
