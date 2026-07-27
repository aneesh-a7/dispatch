// Command loadtest submits a burst of jobs to a running control plane and
// reports how fast they drained: end-to-end throughput plus lease-latency
// and execution-duration percentiles, all computed from the timestamps
// the control plane already records on each job. It needs at least one
// worker running to make progress.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/aneesh/dispatch/internal/client"
	"github.com/aneesh/dispatch/internal/types"
)

func main() {
	controlPlaneURL := flag.String("control-plane", "http://localhost:8080", "control plane base URL")
	token := flag.String("token", os.Getenv("DISPATCH_TOKEN"), "bearer token, if the control plane requires auth (or $DISPATCH_TOKEN)")
	n := flag.Int("n", 200, "number of jobs to submit")
	commandStr := flag.String("command", defaultCommand(), "command to run for each job (space-separated)")
	priority := flag.Int("priority", 0, "priority for every submitted job")
	cpu := flag.Int("cpu", 0, "CPU units each job requests")
	memory := flag.Int("memory", 0, "memory (MB) each job requests")
	timeout := flag.Duration("timeout", 2*time.Minute, "give up if the jobs have not drained by this long")
	flag.Parse()

	if *n < 1 {
		fmt.Fprintln(os.Stderr, "loadtest: -n must be at least 1")
		os.Exit(1)
	}
	fields := strings.Fields(*commandStr)
	if len(fields) == 0 {
		fmt.Fprintln(os.Stderr, "loadtest: -command must not be empty")
		os.Exit(1)
	}

	c := client.New(*controlPlaneURL).WithToken(*token)

	fmt.Printf("submitting %d jobs (%q) to %s...\n", *n, *commandStr, *controlPlaneURL)
	ids := make(map[string]bool, *n)
	submitStart := time.Now()
	for i := 0; i < *n; i++ {
		job, err := c.SubmitJob(client.SubmitJobRequest{
			Command:    fields[0],
			Args:       fields[1:],
			Priority:   *priority,
			MaxRetries: 0,
			Resources:  types.Resources{CPU: *cpu, Memory: *memory},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "loadtest: submit %d failed: %v\n", i, err)
			os.Exit(1)
		}
		ids[job.ID] = true
	}
	fmt.Printf("submitted in %s; waiting for completion...\n", time.Since(submitStart).Round(time.Millisecond))

	deadline := time.Now().Add(*timeout)
	var finished []*types.Job
	for {
		jobs, err := c.ListJobs()
		if err != nil {
			fmt.Fprintf(os.Stderr, "loadtest: list failed: %v\n", err)
			os.Exit(1)
		}
		finished = finished[:0]
		for _, j := range jobs {
			if ids[j.ID] && j.Status.Terminal() {
				finished = append(finished, j)
			}
		}
		if len(finished) == *n {
			break
		}
		if time.Now().After(deadline) {
			fmt.Fprintf(os.Stderr, "loadtest: timed out with %d/%d finished (is a worker running?)\n", len(finished), *n)
			os.Exit(1)
		}
		time.Sleep(200 * time.Millisecond)
	}

	report(finished)
}

func report(jobs []*types.Job) {
	var leaseMs, execMs []float64
	var firstCreated, lastFinished time.Time
	statusCounts := map[types.JobStatus]int{}
	for _, j := range jobs {
		statusCounts[j.Status]++
		if firstCreated.IsZero() || j.CreatedAt.Before(firstCreated) {
			firstCreated = j.CreatedAt
		}
		if j.FinishedAt != nil && j.FinishedAt.After(lastFinished) {
			lastFinished = *j.FinishedAt
		}
		if j.StartedAt != nil {
			leaseMs = append(leaseMs, float64(j.StartedAt.Sub(j.CreatedAt).Microseconds())/1000)
			if j.FinishedAt != nil {
				execMs = append(execMs, float64(j.FinishedAt.Sub(*j.StartedAt).Microseconds())/1000)
			}
		}
	}

	wall := lastFinished.Sub(firstCreated).Seconds()
	throughput := 0.0
	if wall > 0 {
		throughput = float64(len(jobs)) / wall
	}

	fmt.Println()
	fmt.Printf("jobs completed:   %d\n", len(jobs))
	fmt.Printf("  by status:      %s\n", formatStatuses(statusCounts))
	fmt.Printf("wall time:        %.2fs\n", wall)
	fmt.Printf("throughput:       %.1f jobs/sec\n", throughput)
	fmt.Println()
	printPercentiles("lease latency (queue wait)", leaseMs)
	printPercentiles("execution duration", execMs)
}

func printPercentiles(label string, ms []float64) {
	if len(ms) == 0 {
		fmt.Printf("%-28s no samples\n", label+":")
		return
	}
	sort.Float64s(ms)
	fmt.Printf("%s (ms):\n", label)
	fmt.Printf("  p50 %.1f   p95 %.1f   p99 %.1f   max %.1f\n",
		percentile(ms, 0.50), percentile(ms, 0.95), percentile(ms, 0.99), ms[len(ms)-1])
}

// percentile returns the p-th percentile of a pre-sorted slice using the
// nearest-rank method, which is plenty for reporting demo numbers.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func formatStatuses(counts map[types.JobStatus]int) string {
	var parts []string
	for _, st := range []types.JobStatus{types.JobSucceeded, types.JobFailed, types.JobCancelled} {
		if counts[st] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[st], st))
		}
	}
	return strings.Join(parts, ", ")
}

// defaultCommand is a near-instant no-op appropriate to the host OS, on
// the assumption the workers run the same platform as this tool (true for
// the local, single-machine setup this project targets).
func defaultCommand() string {
	if runtime.GOOS == "windows" {
		return "cmd /c rem"
	}
	return "true"
}
