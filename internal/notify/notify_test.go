package notify

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aneesh/dispatch/internal/types"
)

func testJob() *types.Job {
	return &types.Job{
		ID:      "job_1",
		Command: "echo",
		Args:    []string{"hi"},
		Status:  types.JobSucceeded,
	}
}

// received waits for one webhook body, or fails the test if none arrives.
func received(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case b := <-ch:
		return b
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for webhook delivery")
		return nil
	}
}

func recorder() (*httptest.Server, <-chan []byte) {
	ch := make(chan []byte, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ch <- b
		w.WriteHeader(http.StatusOK)
	}))
	return srv, ch
}

func TestJobFinished_PostsToDefaultURL(t *testing.T) {
	srv, ch := recorder()
	defer srv.Close()

	New(srv.URL).JobFinished(testJob())

	var got payload
	if err := json.Unmarshal(received(t, ch), &got); err != nil {
		t.Fatalf("decoding webhook body: %v", err)
	}
	if got.Job == nil || got.Job.ID != "job_1" {
		t.Errorf("payload job = %+v, want job_1", got.Job)
	}
	if got.Event != "job.succeeded" {
		t.Errorf("event = %q, want job.succeeded", got.Event)
	}
	// Slack reads "text", Discord reads "content". Both have to be present
	// for one payload to be useful to either without per-service code.
	if got.Text == "" || got.Content != got.Text {
		t.Errorf("text=%q content=%q, want both set and identical", got.Text, got.Content)
	}
}

// A per-job URL is what lets one control plane notify different places for
// different work, so it has to win over the configured default.
func TestJobFinished_PerJobURLOverridesDefault(t *testing.T) {
	defaultSrv, defaultCh := recorder()
	defer defaultSrv.Close()
	perJobSrv, perJobCh := recorder()
	defer perJobSrv.Close()

	job := testJob()
	job.WebhookURL = perJobSrv.URL
	New(defaultSrv.URL).JobFinished(job)

	received(t, perJobCh)
	select {
	case <-defaultCh:
		t.Error("default webhook fired even though the job specified its own URL")
	case <-time.After(300 * time.Millisecond):
	}
}

func TestJobFinished_NoURLConfiguredIsNoOp(t *testing.T) {
	srv, ch := recorder()
	defer srv.Close()

	New("").JobFinished(testJob())

	select {
	case <-ch:
		t.Error("webhook fired with no URL configured")
	case <-time.After(300 * time.Millisecond):
	}
}

// Callers hold a *Notifier unconditionally, so a nil one must be safe.
func TestJobFinished_NilNotifierDoesNotPanic(t *testing.T) {
	var n *Notifier
	n.JobFinished(testJob())
}

func TestJobFinished_RetriesAfterServerError(t *testing.T) {
	var attempts int
	ch := make(chan []byte, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		b, _ := io.ReadAll(r.Body)
		ch <- b
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	New(srv.URL).JobFinished(testJob())

	received(t, ch)
	if attempts < 2 {
		t.Errorf("attempts = %d, want at least 2 (first failed)", attempts)
	}
}
