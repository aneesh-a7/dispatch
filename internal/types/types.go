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
//	pending -> cancelled                 (cancelled before it ever ran)
//	running -> cancelled                 (cancelled mid-flight by the user)
type JobStatus string

const (
	JobPending   JobStatus = "pending"
	JobRunning   JobStatus = "running"
	JobSucceeded JobStatus = "succeeded"
	JobFailed    JobStatus = "failed"
	JobCancelled JobStatus = "cancelled"
)

// Terminal reports whether a status is an end state that no longer moves.
func (s JobStatus) Terminal() bool {
	return s == JobSucceeded || s == JobFailed || s == JobCancelled
}

// Resources describes a job's requested footprint or a worker's total
// capacity. The units are deliberately abstract: CPU is a count of
// scheduling "slots" and Memory is megabytes. A zero value means "no
// declared requirement," so a job left at the default fits on any worker
// and consumes nothing, which keeps resource-aware scheduling opt-in.
type Resources struct {
	CPU    int `json:"cpu"`
	Memory int `json:"memory"`
}

// FitsWithin reports whether r can be satisfied by the free capacity avail.
func (r Resources) FitsWithin(avail Resources) bool {
	return r.CPU <= avail.CPU && r.Memory <= avail.Memory
}

// Minus returns the capacity left after subtracting o. It does not clamp
// at zero; callers derive it from consistent state so it should not go
// negative in practice.
func (r Resources) Minus(o Resources) Resources {
	return Resources{CPU: r.CPU - o.CPU, Memory: r.Memory - o.Memory}
}

// Plus returns the sum of two resource footprints.
func (r Resources) Plus(o Resources) Resources {
	return Resources{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory}
}

// Job is a unit of work submitted to the control plane.
type Job struct {
	ID       string   `json:"id"`
	Command  string   `json:"command"`
	Args     []string `json:"args"`
	Priority int      `json:"priority"` // higher runs first within the pending queue

	Resources Resources `json:"resources"` // requested footprint; zero means unconstrained

	Status   JobStatus `json:"status"`
	WorkerID string    `json:"worker_id,omitempty"`

	// CancelRequested is set when a running job should stop. The worker
	// polls its own job and kills the subprocess when it sees this, then
	// reports back as cancelled. Pending jobs are cancelled outright
	// without ever setting this flag.
	CancelRequested bool `json:"cancel_requested,omitempty"`

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
// work from the control plane rather than having work pushed to them.
// This keeps the control plane from needing to know about worker network
// reachability, which matters once workers are behind NAT/firewalls.
type Worker struct {
	ID      string `json:"id"`
	Address string `json:"address"` // informational; not dialed by the control plane

	Status WorkerStatus `json:"status"`

	// Capacity is the worker's total resource budget, set at registration.
	// Available is derived, not stored: the control plane computes it as
	// Capacity minus whatever its running jobs consume, so it can never
	// drift out of sync with the actual job states in the WAL.
	Capacity  Resources `json:"capacity"`
	Available Resources `json:"available"`

	RegisteredAt  time.Time `json:"registered_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}
