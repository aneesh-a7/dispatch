// Package client is a small typed HTTP client for the control plane API.
// The worker agent and the CLI both use it, so the wire format only has
// one Go-side implementation to keep in sync with internal/api.
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aneesh/dispatch/internal/types"
)

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Client) do(method, path string, body, out any) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("client: marshaling request: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, c.baseURL+path, reqBody)
	if err != nil {
		return fmt.Errorf("client: building request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("client: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("client: %s %s: status %d: %s", method, path, resp.StatusCode, string(msg))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("client: decoding response: %w", err)
	}
	return nil
}

// --- Job endpoints ---------------------------------------------------

type SubmitJobRequest struct {
	Command    string   `json:"command"`
	Args       []string `json:"args"`
	Priority   int      `json:"priority"`
	MaxRetries int      `json:"max_retries"`
}

func (c *Client) SubmitJob(req SubmitJobRequest) (*types.Job, error) {
	var job types.Job
	if err := c.do(http.MethodPost, "/v1/jobs", req, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

func (c *Client) GetJob(id string) (*types.Job, error) {
	var job types.Job
	if err := c.do(http.MethodGet, "/v1/jobs/"+id, nil, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

func (c *Client) ListJobs() ([]*types.Job, error) {
	var jobs []*types.Job
	if err := c.do(http.MethodGet, "/v1/jobs", nil, &jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// --- Worker endpoints --------------------------------------------------

func (c *Client) RegisterWorker(address string) (*types.Worker, error) {
	var w types.Worker
	body := map[string]string{"address": address}
	if err := c.do(http.MethodPost, "/v1/workers/register", body, &w); err != nil {
		return nil, err
	}
	return &w, nil
}

func (c *Client) Heartbeat(workerID string) error {
	return c.do(http.MethodPost, "/v1/workers/"+workerID+"/heartbeat", struct{}{}, nil)
}

// Lease returns (job, true) if work was assigned, or (nil, false) if the
// queue was empty (the control plane responds 204 in that case).
func (c *Client) Lease(workerID string) (*types.Job, bool, error) {
	var job types.Job
	err := c.do(http.MethodPost, "/v1/workers/"+workerID+"/lease", struct{}{}, &job)
	if err != nil {
		return nil, false, err
	}
	if job.ID == "" {
		return nil, false, nil
	}
	return &job, true, nil
}

type CompleteJobRequest struct {
	Status types.JobStatus `json:"status"`
	Output string          `json:"output"`
	Error  string          `json:"error"`
}

func (c *Client) CompleteJob(jobID string, req CompleteJobRequest) error {
	return c.do(http.MethodPost, "/v1/jobs/"+jobID+"/complete", req, nil)
}
