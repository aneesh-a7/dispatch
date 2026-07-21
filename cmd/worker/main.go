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
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aneesh/dispatch/internal/client"
	"github.com/aneesh/dispatch/internal/types"
)

func main() {
	controlPlaneURL := flag.String("control-plane", "http://localhost:8080", "control plane base URL")
	address := flag.String("address", "local", "informational address reported at registration")
	cpu := flag.Int("cpu", 4, "CPU capacity this worker advertises (abstract units)")
	memory := flag.Int("memory", 4096, "memory capacity this worker advertises (MB)")
	pollInterval := flag.Duration("poll-interval", 2*time.Second, "how often to ask for work when idle")
	heartbeatInterval := flag.Duration("heartbeat-interval", 5*time.Second, "how often to heartbeat (must be well under the control plane's heartbeat-ttl)")
	cancelPollInterval := flag.Duration("cancel-poll-interval", 1*time.Second, "how often a running job checks whether it has been cancelled")
	jobTimeout := flag.Duration("job-timeout", 5*time.Minute, "max time a single job is allowed to run")
	flag.Parse()

	c := client.New(*controlPlaneURL)

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
