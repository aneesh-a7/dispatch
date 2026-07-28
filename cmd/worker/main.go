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
	"sync"
	"sync/atomic"
	"time"

	"github.com/aneesh/dispatch/internal/client"
	"github.com/aneesh/dispatch/internal/config"
	"github.com/aneesh/dispatch/internal/sandbox"
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
	sandboxed := flag.Bool("sandbox", true, "run jobs with an environment allowlist, an isolated working directory, and OS resource limits where available")
	passEnv := flag.String("pass-env", "", "comma-separated environment variables to forward to jobs on top of the baseline allowlist")
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

	sbOpts := sandbox.Options{Enabled: *sandboxed}
	if *passEnv != "" {
		sbOpts.PassEnv = strings.Split(*passEnv, ",")
	}
	log.Printf("worker: sandbox %s", sandbox.Describe(sbOpts.Enabled))

	stopHeartbeat := make(chan struct{})
	go heartbeatLoop(c, worker.ID, *heartbeatInterval, stopHeartbeat)
	defer close(stopHeartbeat)

	pollLoop(c, worker.ID, *pollInterval, *cancelPollInterval, *jobTimeout, sbOpts)
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

func pollLoop(c *client.Client, workerID string, pollInterval, cancelPollInterval, jobTimeout time.Duration, sbOpts sandbox.Options) {
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
		completion := runJob(c, job, cancelPollInterval, jobTimeout, sbOpts)

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
func runJob(c *client.Client, job *types.Job, cancelPollInterval, jobTimeout time.Duration, sbOpts sandbox.Options) client.CompleteJobRequest {
	ctx, cancel := context.WithTimeout(context.Background(), jobTimeout)
	defer cancel()

	var cancelled atomic.Bool
	watcherDone := make(chan struct{})
	go watchForCancel(ctx, c, job.ID, cancelPollInterval, &cancelled, cancel, watcherDone)

	sink := newOutputSink(c, job.ID, outputFlushInterval)
	output, runErr := execute(ctx, job, sink, sbOpts)
	sink.stop()
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

// execute runs the job, writing its combined stdout and stderr into sink
// as it is produced and returning the whole thing at the end.
//
// This used to be a single cmd.CombinedOutput(), which was simpler but
// meant nothing at all was visible until the process exited. Wiring both
// streams into one writer keeps the interleaving CombinedOutput gave us
// while making the bytes available as they arrive.
func execute(ctx context.Context, job *types.Job, sink *outputSink, sbOpts sandbox.Options) (string, error) {
	sb, err := sandbox.New(job.ID, job.Resources, sbOpts)
	if err != nil {
		return "", err
	}
	defer sb.Close()

	cmd := exec.CommandContext(ctx, job.Command, job.Args...)
	cmd.Stdout = sink
	cmd.Stderr = sink
	sb.Apply(cmd)

	if err := cmd.Start(); err != nil {
		return sink.collected(), err
	}
	// Limits that can only be applied to a live process (cgroup
	// membership, job object assignment) go on here. A failure means the
	// job runs with less isolation than intended, which is worth saying
	// out loud but not worth killing the job over.
	if err := sb.Started(cmd); err != nil {
		log.Printf("worker: job %s started with reduced isolation: %v", job.ID, err)
	}

	// Wait first, collect second, and resist the urge to fold these into
	// one return statement: Go evaluates a return's operands left to
	// right, so `return sink.collected(), cmd.Wait()` reads the buffer
	// before the process has finished writing to it and quietly reports
	// every job as having produced no output.
	runErr := cmd.Wait()
	return sink.collected(), runErr
}

// outputSink is what the running job's stdout and stderr are wired into.
// It keeps a capped copy for the final completion report and forwards new
// bytes to the control plane on a timer so `dispatchctl logs -f` has
// something to follow.
//
// The forwarding is deliberately time-based rather than per-write: a
// program printing a line at a time would otherwise generate one HTTP
// request per line, which is a lot of traffic to watch a progress bar.
type outputSink struct {
	client *client.Client
	jobID  string

	mu       sync.Mutex
	retained []byte // capped copy for the completion report
	dropped  bool   // retained lost bytes to the cap
	pending  []byte // produced but not yet forwarded

	done chan struct{}
	wg   sync.WaitGroup
}

const (
	outputFlushInterval = 700 * time.Millisecond
	// retainedOutputCap bounds what a single job can make this worker
	// hold. A job that prints without limit should not be able to take the
	// worker down with it, and the tail is the part anyone reads.
	retainedOutputCap = 1 << 20 // 1 MiB
)

func newOutputSink(c *client.Client, jobID string, flushEvery time.Duration) *outputSink {
	s := &outputSink{client: c, jobID: jobID, done: make(chan struct{})}
	s.wg.Add(1)
	go s.flushLoop(flushEvery)
	return s
}

// Write satisfies io.Writer for the subprocess's streams. It must not
// block on the network: the job's own progress runs through here, so a
// slow control plane would otherwise apply backpressure to the job
// itself. Buffer here, send from the flush goroutine.
func (s *outputSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, p...)
	s.retained = append(s.retained, p...)
	if overflow := len(s.retained) - retainedOutputCap; overflow > 0 {
		s.retained = s.retained[overflow:]
		s.dropped = true
	}
	return len(p), nil
}

func (s *outputSink) flushLoop(every time.Duration) {
	defer s.wg.Done()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.flush()
		case <-s.done:
			s.flush() // catch whatever the job printed just before exiting
			return
		}
	}
}

func (s *outputSink) flush() {
	s.mu.Lock()
	chunk := s.pending
	s.pending = nil
	s.mu.Unlock()

	if len(chunk) == 0 {
		return
	}
	// Streaming is a convenience, not a guarantee: the authoritative copy
	// goes out with the completion report. A failed push is logged at most
	// once and the bytes are dropped rather than retried, so a struggling
	// control plane cannot make this worker hoard memory.
	if err := s.client.AppendOutput(s.jobID, chunk); err != nil {
		log.Printf("worker: streaming output for job %s failed: %v", s.jobID, err)
	}
}

func (s *outputSink) stop() {
	close(s.done)
	s.wg.Wait()
}

func (s *outputSink) collected() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dropped {
		return "[earlier output truncated by the worker]\n" + string(s.retained)
	}
	return string(s.retained)
}
