// Command dispatchctl is the CLI for interacting with a running control
// plane: submitting jobs and checking on their status. This is what
// makes the system feel like a tool you'd actually reach for, instead
// of a backend with no front door.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/aneesh/dispatch/internal/client"
	"github.com/aneesh/dispatch/internal/types"
)

func main() {
	controlPlaneURL := flag.String("control-plane", envOr("DISPATCH_ADDR", "http://localhost:8080"), "control plane base URL")
	token := flag.String("token", os.Getenv("DISPATCH_TOKEN"), "bearer token, if the control plane requires auth (or $DISPATCH_TOKEN)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}

	c := client.New(*controlPlaneURL).WithToken(*token)

	switch args[0] {
	case "submit":
		cmdSubmit(c, args[1:])
	case "status":
		cmdStatus(c, args[1:])
	case "list":
		cmdList(c, args[1:])
	case "cancel":
		cmdCancel(c, args[1:])
	case "logs":
		cmdLogs(c, args[1:])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `dispatchctl - control a dispatch cluster

Usage:
  dispatchctl submit [-priority N] [-retries N] [-cpu N] [-memory MB] [-webhook URL] <command> [args...]
  dispatchctl status <job-id>
  dispatchctl logs [-f] <job-id>
  dispatchctl cancel <job-id>
  dispatchctl list

Flags (global):
  -control-plane   control plane base URL (default http://localhost:8080,
                    or $DISPATCH_ADDR)
  -token           bearer token, if the control plane requires auth
                    (or $DISPATCH_TOKEN)`)
}

func cmdSubmit(c *client.Client, args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	priority := fs.Int("priority", 0, "higher runs first")
	retries := fs.Int("retries", 0, "max retries on failure")
	cpu := fs.Int("cpu", 0, "CPU units this job needs (0 means unconstrained)")
	memory := fs.Int("memory", 0, "memory this job needs in MB (0 means unconstrained)")
	webhook := fs.String("webhook", "", "POST this job's result here when it finishes, overriding the control plane's default")
	every := fs.Duration("every", 0, "re-run this job repeatedly, waiting this long after each run finishes (e.g. 1h)")
	after := fs.String("after", "", "comma-separated job IDs that must succeed before this job runs")
	quiet := fs.Bool("q", false, "print only the job ID, for feeding straight into -after")
	fs.Parse(args)

	var dependsOn []string
	if *after != "" {
		for _, id := range strings.Split(*after, ",") {
			if id = strings.TrimSpace(id); id != "" {
				dependsOn = append(dependsOn, id)
			}
		}
	}

	rest := fs.Args()
	if len(rest) < 1 {
		fmt.Fprintln(os.Stderr, "submit requires a command, e.g.: dispatchctl submit -- echo hello")
		os.Exit(1)
	}

	job, err := c.SubmitJob(client.SubmitJobRequest{
		Command:    rest[0],
		Args:       rest[1:],
		Priority:   *priority,
		MaxRetries: *retries,
		Resources:  types.Resources{CPU: *cpu, Memory: *memory},
		WebhookURL: *webhook,
		DependsOn:  dependsOn,
		Every:      *every,
	})
	fatalIf(err)

	// Chaining jobs means feeding one command's output into the next
	// one's -after, and the friendly multi-line version of this output
	// mentions other job IDs, so anything grepping for an ID picks up
	// the dependencies too and passes nonsense along. -q prints the one
	// thing a script wants.
	if *quiet {
		fmt.Println(job.ID)
		return
	}

	fmt.Printf("submitted job %s (status: %s)\n", job.ID, job.Status)
	if job.Every > 0 {
		fmt.Printf("  recurring: next run queued %s after each finish\n", job.Every)
	}
	if len(job.DependsOn) > 0 {
		fmt.Printf("  waiting on: %s\n", strings.Join(job.DependsOn, ", "))
	}
}

func cmdCancel(c *client.Client, args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: dispatchctl cancel <job-id>")
		os.Exit(1)
	}
	job, err := c.CancelJob(args[0])
	fatalIf(err)
	if job.Status == types.JobCancelled {
		fmt.Printf("cancelled job %s\n", job.ID)
	} else {
		fmt.Printf("cancel requested for job %s (currently %s); the worker will stop it shortly\n", job.ID, job.Status)
	}
}

// cmdLogs prints a job's output, optionally following it as it is
// produced. The control plane serves the live buffer while a job runs and
// the durable record once it has finished, so following across the moment
// a job ends needs no special handling here: the same loop keeps reading
// until the server says the job is done.
func cmdLogs(c *client.Client, args []string) {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	follow := fs.Bool("f", false, "keep printing output until the job finishes")
	interval := fs.Duration("interval", time.Second, "how often to poll when following")
	fs.Parse(args)

	rest := fs.Args()
	if len(rest) != 1 {
		fmt.Fprintln(os.Stderr, "usage: dispatchctl logs [-f] <job-id>")
		os.Exit(1)
	}
	jobID := rest[0]

	var offset int64
	for {
		chunk, err := c.FetchOutput(jobID, offset)
		fatalIf(err)

		// Truncated is only ever set when bytes the caller asked for are
		// genuinely gone, so it always deserves saying. An earlier version
		// of this suppressed it on the first read, which hid the warning
		// in exactly the case it matters most: asking for a chatty job's
		// whole output and quietly getting only the tail.
		if chunk.Truncated {
			fmt.Fprintf(os.Stderr, "[earlier output dropped from the live buffer; showing from byte %d. The full output is kept once the job finishes.]\n", chunk.Offset)
		}
		if chunk.Data != "" {
			fmt.Print(chunk.Data)
		}
		offset = chunk.NextOffset

		if chunk.Done || !*follow {
			return
		}
		time.Sleep(*interval)
	}
}

func cmdStatus(c *client.Client, args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: dispatchctl status <job-id>")
		os.Exit(1)
	}
	job, err := c.GetJob(args[0])
	fatalIf(err)

	b, _ := json.MarshalIndent(job, "", "  ")
	fmt.Println(string(b))
}

func cmdList(c *client.Client, args []string) {
	jobs, err := c.ListJobs()
	fatalIf(err)

	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tSTATUS\tCOMMAND\tRETRIES\tWORKER")
	for _, j := range jobs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d/%d\t%s\n",
			j.ID, j.Status, summarizeCommand(j), j.Retries, j.MaxRetries, j.WorkerID)
	}
	tw.Flush()
}

func summarizeCommand(j *types.Job) string {
	cmd := j.Command
	for _, a := range j.Args {
		cmd += " " + a
	}
	if len(cmd) > 40 {
		cmd = cmd[:37] + "..."
	}
	return cmd
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func fatalIf(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
