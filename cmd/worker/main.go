// Command worker is the agent that runs on each machine contributing
// compute. It registers with the control plane, sends periodic
// heartbeats so the reaper knows it's alive, and polls for work.
//
// Week 1 executes jobs as plain subprocesses (os/exec). Sandboxing
// (containers/namespaces) is planned as a later layer (see
// docs/ARCHITECTURE.md) and is deliberately not in week 1's scope so
// that leasing/execution/reporting can be gotten right first.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aneesh/dispatch/internal/client"
	"github.com/aneesh/dispatch/internal/config"
	"github.com/aneesh/dispatch/internal/sysinfo"
	"github.com/aneesh/dispatch/internal/types"
)

func main() {
	controlPlaneURL := flag.String("control-plane", envOr("DISPATCH_ADDR", "http://localhost:8080"), "control plane base URL (or $DISPATCH_ADDR)")
	token := flag.String("token", os.Getenv("DISPATCH_TOKEN"), "bearer token, if the control plane requires auth (or $DISPATCH_TOKEN)")
	address := flag.String("address", "local", "informational address reported at registration")
	cpu := flag.Int("cpu", 0, "CPU capacity this worker advertises (0 auto-detects from the machine)")
	memory := flag.Int("memory", 0, "memory capacity this worker advertises in MB (0 auto-detects from the machine)")
	pollInterval := flag.Duration("poll-interval", 2*time.Second, "how often to ask for work when idle")
	heartbeatInterval := flag.Duration("heartbeat-interval", 5*time.Second, "how often to heartbeat (must be well under the control plane's heartbeat-ttl)")
	cancelPollInterval := flag.Duration("cancel-poll-interval", 1*time.Second, "how often a running job checks whether it has been cancelled")
	jobTimeout := flag.Duration("job-timeout", 5*time.Minute, "max time a single job is allowed to run")
	configPath := flag.String("config", "", "optional JSON config file; command-line flags override it")
	flag.Parse()

	if *configPath != "" {
		var cfg config.Worker
		if err := config.Load(*configPath, &cfg); err != nil {
			log.Fatalf("worker: %v", err)
		}
		applyWorkerConfig(cfg, controlPlaneURL, token, address, cpu, memory, pollInterval, jobTimeout)
	}

	resolveCapacity(cpu, memory)

	c := client.New(*controlPlaneURL).WithToken(*token)

	worker, err := c.RegisterWorker(client.RegisterWorkerRequest{
		Address:  *address,
		Capacity: types.Resources{CPU: *cpu, Memory: *memory},
	})
	if err != nil {
		log.Fatalf("worker: registration failed: %v", err)
	}
	log.Printf("worker: registered as %s (cpu=%d memory=%d)", worker.ID, *cpu, *memory)

	stopHeartbeat := make(chan struct{})
	go heartbeatLoop(c, worker.ID, *heartbeatInterval, stopHeartbeat)
	defer close(stopHeartbeat)

	pollLoop(c, worker.ID, *pollInterval, *cancelPollInterval, *jobTimeout)
}

// fallbackCapacity is used when a platform has no memory detection. It is
// deliberately modest: under-promising costs some idle capacity, while
// over-promising gets jobs bin-packed onto a machine that cannot hold
// them.
const fallbackMemoryMB = 4096

// resolveCapacity fills in any capacity the operator left at zero by
// asking the machine. Anything set explicitly (by flag or config file) is
// left alone: a person who typed a number usually meant it, often to hold
// back part of a machine they are also using for something else.
func resolveCapacity(cpu, memory *int) {
	if *cpu == 0 {
		*cpu = sysinfo.DetectCPU()
		log.Printf("worker: detected %d CPUs", *cpu)
	}
	if *memory == 0 {
		if mb, ok := sysinfo.DetectMemoryMB(); ok {
			*memory = mb
			log.Printf("worker: detected %d MB of memory", mb)
		} else {
			*memory = fallbackMemoryMB
			log.Printf("worker: could not detect memory on this platform, defaulting to %d MB (override with -memory)", fallbackMemoryMB)
		}
	}
}

// applyWorkerConfig fills in values from a config file for flags the user
// did not pass explicitly, so an explicit flag always wins over the file.
func applyWorkerConfig(cfg config.Worker, controlPlaneURL, token, address *string, cpu, memory *int,
	pollInterval, jobTimeout *time.Duration) {

	set := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { set[f.Name] = true })

	if cfg.ControlPlane != nil && !set["control-plane"] {
		*controlPlaneURL = *cfg.ControlPlane
	}
	if cfg.Token != nil && !set["token"] {
		*token = *cfg.Token
	}
	if cfg.Address != nil && !set["address"] {
		*address = *cfg.Address
	}
	if cfg.CPU != nil && !set["cpu"] {
		*cpu = *cfg.CPU
	}
	if cfg.Memory != nil && !set["memory"] {
		*memory = *cfg.Memory
	}
	applyDuration := func(name string, from *string, to *time.Duration) {
		if from == nil || set[name] {
			return
		}
		d, err := time.ParseDuration(*from)
		if err != nil {
			log.Fatalf("worker: config %s: %v", name, err)
		}
		*to = d
	}
	applyDuration("poll-interval", cfg.PollInterval, pollInterval)
	applyDuration("job-timeout", cfg.JobTimeout, jobTimeout)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func heartbeatLoop(c *client.Client, workerID string, interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if err := c.Heartbeat(workerID); err != nil {
				// A single missed heartbeat isn't fatal. The control
				// plane tolerates several missed beats (heartbeat-ttl)
				// before declaring this worker dead. Log and keep going.
				log.Printf("worker: heartbeat failed: %v", err)
			}
		}
	}
}

func pollLoop(c *client.Client, workerID string, pollInterval, cancelPollInterval, jobTimeout time.Duration) {
	for {
		job, ok, err := c.Lease(workerID)
		if err != nil {
			log.Printf("worker: lease request failed: %v", err)
			time.Sleep(pollInterval)
			continue
		}
		if !ok {
			time.Sleep(pollInterval)
			continue
		}

		log.Printf("worker: leased job %s: %s %s", job.ID, job.Command, strings.Join(job.Args, " "))
		completion := runJob(c, job, cancelPollInterval, jobTimeout)

		if err := c.CompleteJob(job.ID, completion); err != nil {
			// The job did run (or fail) but the control plane doesn't
			// know yet. If this worker now dies, the reaper's heartbeat
			// timeout will eventually requeue the job: at-least-once
			// execution, not exactly-once. That trade-off (and how to
			// tighten it) belongs in docs/ARCHITECTURE.md, not silently
			// swept under the rug here.
			log.Printf("worker: failed to report completion for job %s: %v", job.ID, err)
		}
	}
}

// runJob executes one job with a context that a background watcher can
// cancel. The watcher polls the control plane for this job's
// CancelRequested flag; when the user cancels, it kills the subprocess by
// cancelling the context. A timeout cancels the same way, so the two are
// told apart by which one tripped: an explicit cancel wins and is reported
// as cancelled rather than failed.
func runJob(c *client.Client, job *types.Job, cancelPollInterval, jobTimeout time.Duration) client.CompleteJobRequest {
	ctx, cancel := context.WithTimeout(context.Background(), jobTimeout)
	defer cancel()

	var cancelled atomic.Bool
	watcherDone := make(chan struct{})
	go watchForCancel(ctx, c, job.ID, cancelPollInterval, &cancelled, cancel, watcherDone)

	output, runErr := execute(ctx, job.Command, job.Args)
	close(watcherDone)

	switch {
	case cancelled.Load():
		log.Printf("worker: job %s cancelled", job.ID)
		return client.CompleteJobRequest{Status: types.JobCancelled, Output: output, Error: "cancelled by request"}
	case runErr != nil:
		log.Printf("worker: job %s failed: %v", job.ID, runErr)
		return client.CompleteJobRequest{Status: types.JobFailed, Output: output, Error: runErr.Error()}
	default:
		log.Printf("worker: job %s succeeded", job.ID)
		return client.CompleteJobRequest{Status: types.JobSucceeded, Output: output}
	}
}

func watchForCancel(ctx context.Context, c *client.Client, jobID string, interval time.Duration, cancelled *atomic.Bool, cancel context.CancelFunc, done <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			j, err := c.GetJob(jobID)
			if err != nil {
				continue // transient; the job keeps running, try again next tick
			}
			if j.CancelRequested {
				cancelled.Store(true)
				cancel() // kills the subprocess via CommandContext
				return
			}
		}
	}
}

func execute(ctx context.Context, command string, args []string) (string, error) {
	cmd := exec.CommandContext(ctx, command, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
