// Package notify delivers "your job finished" webhooks.
//
// This is the piece that makes the original problem actually go away.
// Durable scheduling means a job survives a crash, but you still had to
// go look at a dashboard or run a status command to find out it was done.
// A webhook pushes that back to wherever you already are.
package notify

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aneesh/dispatch/internal/types"
)

// Notifier posts to a webhook when a job reaches a terminal state. A nil
// *Notifier is a no-op, so callers can hold one unconditionally without
// checking whether notifications were configured.
type Notifier struct {
	defaultURL string
	http       *http.Client
}

// New returns a Notifier. defaultURL may be empty, in which case only
// jobs carrying their own WebhookURL produce a notification.
func New(defaultURL string) *Notifier {
	return &Notifier{
		defaultURL: defaultURL,
		http:       &http.Client{Timeout: 10 * time.Second},
	}
}

// payload is what receivers get. It carries the full job for anything
// programmatic, plus the same one-line summary under two different keys:
// "text" is what Slack renders, "content" is what Discord renders. Both
// ignore fields they do not recognize, so one payload posts usefully to
// either without dispatch needing per-service client code, while a custom
// receiver can ignore the summary and read job.
type payload struct {
	Text    string     `json:"text"`
	Content string     `json:"content"`
	Event   string     `json:"event"`
	Job     *types.Job `json:"job"`
}

// JobFinished delivers a notification for job, if one is configured.
//
// It returns immediately and delivers in the background: a webhook
// receiver that is slow, down, or hostile must never stall the handler
// that called this, which is finishing a job or reaping a dead worker.
// Delivery is best-effort. The job's real status is already durable in
// the WAL before this is ever called, so a dropped notification costs a
// message, not correctness.
func (n *Notifier) JobFinished(job *types.Job) {
	if n == nil || job == nil {
		return
	}
	url := job.WebhookURL
	if url == "" {
		url = n.defaultURL
	}
	if url == "" {
		return
	}
	go n.deliver(url, job)
}

func (n *Notifier) deliver(url string, job *types.Job) {
	body, err := json.Marshal(payload{
		Text:    summarize(job),
		Content: summarize(job),
		Event:   "job." + string(job.Status),
		Job:     job,
	})
	if err != nil {
		log.Printf("notify: encoding webhook for job %s: %v", job.ID, err)
		return
	}

	// Three tries with a short backoff covers a receiver restarting or a
	// blip in connectivity. Beyond that it is down rather than busy, and
	// queueing notifications durably is a bigger feature than this needs.
	const attempts = 3
	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * 2 * time.Second)
		}
		resp, err := n.http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("notify: webhook for job %s failed (attempt %d/%d): %v", job.ID, i+1, attempts, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode < 300 {
			return
		}
		log.Printf("notify: webhook for job %s returned %d (attempt %d/%d)", job.ID, resp.StatusCode, i+1, attempts)
	}
	log.Printf("notify: giving up on webhook for job %s", job.ID)
}

func summarize(job *types.Job) string {
	cmd := job.Command
	if len(job.Args) > 0 {
		cmd += " " + strings.Join(job.Args, " ")
	}
	if len(cmd) > 80 {
		cmd = cmd[:77] + "..."
	}
	s := "dispatch: job " + job.ID + " " + string(job.Status) + " (" + cmd + ")"
	if job.Error != "" {
		s += "\nerror: " + job.Error
	}
	return s
}
