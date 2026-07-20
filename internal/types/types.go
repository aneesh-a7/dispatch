// Package types defines the core domain objects shared by the control
// plane, worker agent, and CLI. Keeping them in one place avoids the
// classic distributed-systems bug of two components silently disagreeing
// about what a "Job" or "Worker" looks like.
package types

import "time"

// JobStatus is the finite state machine a job moves through.
//
//	pending -> running -> succeeded
//	                    -> failed -> pending (retry, if Retries < MaxRetries)
//	                    -> failed (terminal, if retries exhausted)
type JobStatus string

const (
	JobPending   JobStatus = "pending"
	JobRunning   JobStatus = "running"
	JobSucceeded JobStatus = "succeeded"
	JobFailed    JobStatus = "failed"
)

// Job is a unit of work submitted to the control plane.
type Job struct {
	ID       string   `json:"id"`
	Command  string   `json:"command"`
	Args     []string `json:"args"`
	Priority int      `json:"priority"` // higher runs first within the pending queue

	Status   JobStatus `json:"status"`
	WorkerID string    `json:"worker_id,omitempty"`

	Retries    int `json:"retries"`
	MaxRetries int `json:"max_retries"`

	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`

	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// WorkerStatus tracks liveness as observed by the control plane.
type WorkerStatus string

const (
	WorkerAlive WorkerStatus = "alive"
	WorkerDead  WorkerStatus = "dead" // missed too many heartbeats
)

// Worker is a machine/process capable of executing jobs. Workers pull
// work from the control plane rather than having work pushed to them —
// this keeps the control plane from needing to know about worker network
// reachability, which matters once workers are behind NAT/firewalls.
type Worker struct {
	ID      string `json:"id"`
	Address string `json:"address"` // informational; not dialed by the control plane

	Status WorkerStatus `json:"status"`

	RegisteredAt  time.Time `json:"registered_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}
