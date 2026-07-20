// Command worker is the agent that runs on each machine contributing
// compute. It registers with the control plane, sends periodic
// heartbeats so the reaper knows it's alive, and polls for work.
//
// Week 1 executes jobs as plain subprocesses (os/exec). Sandboxing
// (containers/namespaces) is planned as a later layer — see
// docs/ARCHITECTURE.md — and is deliberately not in week 1's scope so
// that leasing/execution/reporting can be gotten right first.
package main

import (
	"context"
	"flag"
	"log"
	"os/exec"
	"strings"
	"time"

	"github.com/aneesh/dispatch/internal/client"
)

func main() {
	controlPlaneURL := flag.String("control-plane", "http://localhost:8080", "control plane base URL")
	address := flag.String("address", "local", "informational address reported at registration")
	pollInterval := flag.Duration("poll-interval", 2*time.Second, "how often to ask for work when idle")
	heartbeatInterval := flag.Duration("heartbeat-interval", 5*time.Second, "how often to heartbeat (must be well under the control plane's heartbeat-ttl)")
	jobTimeout := flag.Duration("job-timeout", 5*time.Minute, "max time a single job is allowed to run")
	flag.Parse()

	c := client.New(*controlPlaneURL)

	worker, err := c.RegisterWorker(*address)
	if err != nil {
		log.Fatalf("worker: registration failed: %v", err)
	}
	log.Printf("worker: registered as %s", worker.ID)

	stopHeartbeat := make(chan struct{})
	go heartbeatLoop(c, worker.ID, *heartbeatInterval, stopHeartbeat)
	defer close(stopHeartbeat)

	pollLoop(c, worker.ID, *pollInterval, *jobTimeout)
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
				// A single missed heartbeat isn't fatal — the control
				// plane tolerates several missed beats (heartbeat-ttl)
				// before declaring this worker dead. Log and keep going.
				log.Printf("worker: heartbeat failed: %v", err)
			}
		}
	}
}

func pollLoop(c *client.Client, workerID string, pollInterval, jobTimeout time.Duration) {
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
		output, runErr := execute(job.Command, job.Args, jobTimeout)

		completion := client.CompleteJobRequest{Output: output}
		if runErr != nil {
			completion.Status = "failed"
			completion.Error = runErr.Error()
			log.Printf("worker: job %s failed: %v", job.ID, runErr)
		} else {
			completion.Status = "succeeded"
			log.Printf("worker: job %s succeeded", job.ID)
		}

		if err := c.CompleteJob(job.ID, completion); err != nil {
			// The job did run (or fail) but the control plane doesn't
			// know yet. If this worker now dies, the reaper's heartbeat
			// timeout will eventually requeue the job — at-least-once
			// execution, not exactly-once. That trade-off (and how to
			// tighten it) belongs in docs/ARCHITECTURE.md, not silently
			// swept under the rug here.
			log.Printf("worker: failed to report completion for job %s: %v", job.ID, err)
		}
	}
}

func execute(command string, args []string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, command, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
