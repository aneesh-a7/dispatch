// Package scheduler contains the control plane's scheduling and failure
// detection logic: priority/FIFO leasing constrained by resource
// bin-packing (see store.LeaseNextJob) plus dead-worker detection and
// requeue. The policy lives in the store so the "find, fit, mutate,
// persist" sequence stays under a single lock; this package is the seam
// where richer policy (affinity, fairness across submitters) would go.
package scheduler

import (
	"log"
	"time"

	"github.com/aneesh/dispatch/internal/notify"
	"github.com/aneesh/dispatch/internal/store"
	"github.com/aneesh/dispatch/internal/types"
)

// Scheduler wraps the store with scheduling policy. Today that policy is
// "highest priority, then oldest, wins" (see store.LeaseNextJob). The
// scheduler package exists as a seam so smarter policies (resource
// bin-packing, affinity, fairness across submitters) can be swapped in
// without changing the API or store layers.
type Scheduler struct {
	store *store.Store
}

func New(s *store.Store) *Scheduler {
	return &Scheduler{store: s}
}

// Lease hands the next eligible job to workerID, or (nil, false) if the
// queue is empty or nothing pending fits the worker's free capacity.
func (sc *Scheduler) Lease(workerID string) (*types.Job, bool) {
	return sc.store.LeaseNextJob(workerID)
}

// Reaper periodically scans for workers that have gone silent and
// reclaims their in-flight work. Distributed systems fail by having
// components disappear without saying goodbye. A scheduler that only
// handles the happy path where every worker calls "complete" isn't
// really a scheduler yet.
type Reaper struct {
	store        *store.Store
	notifier     *notify.Notifier
	heartbeatTTL time.Duration
	interval     time.Duration
}

// NewReaper builds a reaper. n may be nil, in which case no webhooks are
// sent for jobs the reaper fails permanently.
func NewReaper(s *store.Store, n *notify.Notifier, heartbeatTTL, interval time.Duration) *Reaper {
	return &Reaper{store: s, notifier: n, heartbeatTTL: heartbeatTTL, interval: interval}
}

// Run blocks, sweeping on a ticker until stop is closed.
func (r *Reaper) Run(stop <-chan struct{}) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			r.sweep()
		}
	}
}

func (r *Reaper) sweep() {
	r.sweepDeadWorkers()

	// Jobs waiting on a prerequisite that has already failed will never
	// become runnable, so fail them rather than leaving them queued.
	for _, j := range r.store.SweepBlockedJobs() {
		log.Printf("reaper: failing job %s, a dependency did not succeed", j.ID)
		r.notifier.JobFinished(j)
	}

	// Recurring series that have gone quiet get their next run queued.
	// Deliberately silent: queueing the next run is routine bookkeeping,
	// and logging every tick of an hourly job would drown the log.
	r.store.SweepRecurringJobs()
}

func (r *Reaper) sweepDeadWorkers() {
	now := time.Now().UTC()

	deadWorkers := make(map[string]bool)
	for _, w := range r.store.ListWorkers() {
		if w.Status == types.WorkerDead {
			deadWorkers[w.ID] = true
			continue
		}
		if now.Sub(w.LastHeartbeat) > r.heartbeatTTL {
			updated := *w
			updated.Status = types.WorkerDead
			if err := r.store.UpdateWorker(&updated); err != nil {
				log.Printf("reaper: failed to mark worker %s dead: %v", w.ID, err)
				continue
			}
			log.Printf("reaper: worker %s missed heartbeat TTL (%s), marking dead", w.ID, r.heartbeatTTL)
			deadWorkers[w.ID] = true
		}
	}

	if len(deadWorkers) == 0 {
		return
	}

	// Any job "running" on a now-dead worker is orphaned: the worker may
	// have crashed mid-execution and will never call /complete. Requeue
	// it (retrying) or fail it permanently once retries are exhausted.
	for _, j := range r.store.ListJobs() {
		if j.Status != types.JobRunning || !deadWorkers[j.WorkerID] {
			continue
		}
		updated := *j
		updated.UpdatedAt = now
		if j.Retries < j.MaxRetries {
			updated.Retries++
			updated.Status = types.JobPending
			updated.WorkerID = ""
			updated.StartedAt = nil
			log.Printf("reaper: requeuing job %s (attempt %d/%d) after worker %s died",
				j.ID, updated.Retries+1, j.MaxRetries+1, j.WorkerID)
		} else {
			updated.Status = types.JobFailed
			updated.Error = "worker died and retry budget exhausted"
			updated.FinishedAt = &now
			log.Printf("reaper: failing job %s permanently after worker %s died (retries exhausted)",
				j.ID, j.WorkerID)
		}
		if err := r.store.UpdateJob(&updated); err != nil {
			log.Printf("reaper: failed to update orphaned job %s: %v", j.ID, err)
			continue
		}
		if updated.Status.Terminal() {
			// A job whose worker vanished is exactly the case you most
			// want to hear about without going to look for it.
			r.notifier.JobFinished(&updated)
		}
	}
}
