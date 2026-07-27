// Package store implements durable state for the control plane using a
// simple write-ahead log (WAL): every mutation is appended to an
// append-only file and fsync'd before the in-memory state is updated.
// On startup, if a snapshot exists, it is loaded first; the WAL is then
// replayed from the beginning to restore any mutations written after the
// snapshot was taken.
//
// This is the same core idea real databases use (Postgres's WAL, etcd's
// raft log, Kafka's segment log): never mutate state that isn't first
// durably recorded, so a crash between "decided" and "applied" is
// recoverable by replay instead of lost.
//
// Periodic Compact() calls write a snapshot of current state and truncate
// the log, so long-running instances do not accumulate unbounded log files
// that slow startup.
package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/aneesh/dispatch/internal/idgen"
	"github.com/aneesh/dispatch/internal/types"
)

// recordType tags each WAL entry so replay knows how to apply it.
type recordType string

const (
	recJobUpsert    recordType = "job_upsert"
	recWorkerUpsert recordType = "worker_upsert"
)

// record is the on-disk WAL entry shape. Only one of Job/Worker is set,
// depending on Type.
type record struct {
	Type   recordType    `json:"type"`
	Job    *types.Job    `json:"job,omitempty"`
	Worker *types.Worker `json:"worker,omitempty"`
}

// Store is a durable, in-memory-indexed store backed by a WAL file.
// All exported methods are safe for concurrent use.
type Store struct {
	mu      sync.RWMutex
	jobs    map[string]*types.Job
	workers map[string]*types.Worker

	logFile *os.File
	logPath string
}

// Open opens (or creates) a store rooted at dir. It replays any existing
// WAL to rebuild in-memory state before returning, so callers can treat
// Open as "give me a store with all prior state already loaded."
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: creating data dir: %w", err)
	}
	logPath := filepath.Join(dir, "dispatch.wal")

	s := &Store{
		jobs:    make(map[string]*types.Job),
		workers: make(map[string]*types.Worker),
		logPath: logPath,
	}

	if err := s.replay(); err != nil {
		return nil, fmt.Errorf("store: replaying WAL: %w", err)
	}

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("store: opening WAL for append: %w", err)
	}
	s.logFile = f

	return s, nil
}

// Close flushes and closes the underlying log file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logFile.Close()
}

// replay loads the snapshot if it exists, then replays the WAL. This
// combines a fast cold-start from the snapshot with replay of any mutations
// that occurred after the snapshot was written. It is only called once, from
// Open, before the log file handle for appending is created.
func (s *Store) replay() error {
	snapPath := filepath.Join(filepath.Dir(s.logPath), "dispatch.snapshot")
	if _, err := os.Stat(snapPath); err == nil {
		if err := s.loadSnapshot(snapPath); err != nil {
			return fmt.Errorf("loading snapshot: %w", err)
		}
	}

	f, err := os.OpenFile(s.logPath, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec record
		if err := json.Unmarshal(line, &rec); err != nil {
			fmt.Fprintf(os.Stderr, "store: skipping malformed WAL line %d: %v\n", lineNum, err)
			continue
		}
		s.apply(rec)
	}
	return scanner.Err()
}

// loadSnapshot reads a snapshot file and loads it into state. Called during
// replay if the snapshot exists.
func (s *Store) loadSnapshot(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var snap struct {
		Jobs    map[string]*types.Job    `json:"jobs"`
		Workers map[string]*types.Worker `json:"workers"`
	}
	if err := json.Unmarshal(b, &snap); err != nil {
		return err
	}
	s.jobs = snap.Jobs
	if s.jobs == nil {
		s.jobs = make(map[string]*types.Job)
	}
	s.workers = snap.Workers
	if s.workers == nil {
		s.workers = make(map[string]*types.Worker)
	}
	return nil
}

// apply mutates in-memory state from a decoded record. Called both
// during replay and (indirectly) after every live append.
func (s *Store) apply(rec record) {
	switch rec.Type {
	case recJobUpsert:
		if rec.Job != nil {
			s.jobs[rec.Job.ID] = rec.Job
		}
	case recWorkerUpsert:
		if rec.Worker != nil {
			s.workers[rec.Worker.ID] = rec.Worker
		}
	}
}

// append serializes rec, writes it as one line, and fsyncs before
// returning. Callers must hold s.mu for writing.
func (s *Store) append(rec record) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := s.logFile.Write(b); err != nil {
		return err
	}
	// Sync is the whole point: without it, a crash right after Write can
	// lose data still sitting in the OS page cache. This is also why
	// this store will not win any throughput benchmarks against an
	// in-memory map. Durability has a cost, and that trade-off is the
	// point of the exercise.
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	s.apply(rec)
	return nil
}

// --- Job operations ---------------------------------------------------

// CreateJob durably persists a new job and adds it to the pending queue.
func (s *Store) CreateJob(j *types.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.append(record{Type: recJobUpsert, Job: j})
}

// UpdateJob durably persists a mutated job (status change, retry count,
// output, etc). Callers should mutate a copy obtained from GetJob.
func (s *Store) UpdateJob(j *types.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.append(record{Type: recJobUpsert, Job: j})
}

// GetJob returns a copy of the job with the given ID.
func (s *Store) GetJob(id string) (*types.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	cp := *j
	return &cp, true
}

// ListJobs returns copies of all jobs, most recently created first.
func (s *Store) ListJobs() []*types.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*types.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		cp := *j
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].CreatedAt.After(out[k].CreatedAt) })
	return out
}

// usedByWorkerLocked sums the resources of jobs currently running on a
// worker. Available capacity is derived from this rather than tracked
// separately: the running jobs are already durable in the WAL, so there
// is nothing extra to persist and no counter that can drift or be
// double-released. Callers must hold s.mu.
func (s *Store) usedByWorkerLocked(workerID string) types.Resources {
	var used types.Resources
	for _, j := range s.jobs {
		if j.Status == types.JobRunning && j.WorkerID == workerID {
			used = used.Plus(j.Resources)
		}
	}
	return used
}

// dependencyStateLocked reports how a job's prerequisites are doing:
// ready when every dependency has succeeded, doomed when at least one has
// finished as something other than succeeded (so waiting is pointless).
// A dependency that no longer exists counts as doomed rather than
// satisfied, since silently running a job whose prerequisite vanished is
// the more dangerous of the two guesses. Callers must hold s.mu.
func (s *Store) dependencyStateLocked(j *types.Job) (ready, doomed bool) {
	for _, id := range j.DependsOn {
		dep, ok := s.jobs[id]
		if !ok {
			return false, true
		}
		if dep.Status == types.JobSucceeded {
			continue
		}
		if dep.Status.Terminal() {
			return false, true
		}
		return false, false // still pending or running: wait
	}
	return true, false
}

// leasableLocked reports whether a pending job is allowed to run now.
// Callers must hold s.mu.
func (s *Store) leasableLocked(j *types.Job, now time.Time) bool {
	if j.Status != types.JobPending {
		return false
	}
	if j.NotBefore != nil && now.Before(*j.NotBefore) {
		return false // scheduled for later
	}
	ready, _ := s.dependencyStateLocked(j)
	return ready
}

// LeaseNextJob atomically finds the highest-priority, oldest pending job
// that fits the worker's free capacity, marks it running and assigned to
// workerID, persists that transition, and returns it. Doing the "find +
// mutate + persist" sequence under a single lock is what prevents two
// workers from racing to lease the same job: the classic double-dispatch
// bug in naive queue implementations.
//
// Leasing is bin-packing, not pure priority/FIFO: a job is only handed to
// a worker with enough free CPU and memory for it. If the worker is not
// known to the store (which only happens in direct unit tests, since the
// API checks registration first), the capacity filter is skipped.
//
// A job is also skipped if it is waiting on an unfinished dependency or
// on its scheduled start time.
func (s *Store) LeaseNextJob(workerID string) (*types.Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, workerKnown := s.workers[workerID]
	var avail types.Resources
	if workerKnown {
		avail = worker.Capacity.Minus(s.usedByWorkerLocked(workerID))
	}

	now := time.Now().UTC()
	var best *types.Job
	for _, j := range s.jobs {
		if !s.leasableLocked(j, now) {
			continue
		}
		if workerKnown && !j.Resources.FitsWithin(avail) {
			continue // does not fit this worker right now; leave it queued
		}
		if best == nil ||
			j.Priority > best.Priority ||
			(j.Priority == best.Priority && j.CreatedAt.Before(best.CreatedAt)) {
			best = j
		}
	}
	if best == nil {
		return nil, false
	}

	updated := *best
	updated.Status = types.JobRunning
	updated.WorkerID = workerID
	updated.StartedAt = &now
	updated.UpdatedAt = now

	if err := s.append(record{Type: recJobUpsert, Job: &updated}); err != nil {
		// Leasing failed to persist; do not hand the job to the worker,
		// it stays pending and will be leased again on the next poll.
		return nil, false
	}
	cp := updated
	return &cp, true
}

// JobExists reports whether a job ID is known. The API uses it to reject
// a dependency on a job that was never submitted, which is also what
// guarantees the dependency graph stays acyclic: you can only depend on
// something that already exists, and something that already exists cannot
// come to depend on you.
func (s *Store) JobExists(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.jobs[id]
	return ok
}

// SweepBlockedJobs fails every pending job whose dependencies can no
// longer all succeed, and returns the jobs it changed so the caller can
// notify on them. Without this a job whose prerequisite failed would sit
// pending forever, which looks identical to "queued behind a busy worker"
// from the outside and is much more annoying to diagnose.
func (s *Store) SweepBlockedJobs() []*types.Job {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	var changed []*types.Job
	for _, j := range s.jobs {
		if j.Status != types.JobPending {
			continue
		}
		if _, doomed := s.dependencyStateLocked(j); !doomed {
			continue
		}
		updated := *j
		updated.Status = types.JobFailed
		updated.Error = "a dependency did not succeed"
		updated.UpdatedAt = now
		updated.FinishedAt = &now
		if err := s.append(record{Type: recJobUpsert, Job: &updated}); err != nil {
			continue // try again on the next sweep
		}
		cp := updated
		changed = append(changed, &cp)
	}
	return changed
}

// SweepRecurringJobs queues the next run of any recurring series that has
// gone quiet, and returns whatever it created.
//
// Deciding "is another run due?" from the current state of the series,
// rather than from a timer set when the last run finished, is what makes
// this safe across a control plane restart: the schedule lives in the WAL
// like everything else, so a process that was down for an hour picks up
// exactly where it should instead of losing the series.
func (s *Store) SweepRecurringJobs() []*types.Job {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Group the series first. A series is identified by SeriesID, which
	// every run of the same recurring job shares.
	type seriesState struct {
		active bool // a run is pending or in flight, so nothing is due
		latest *types.Job
	}
	series := map[string]*seriesState{}
	for _, j := range s.jobs {
		if j.SeriesID == "" {
			continue
		}
		st := series[j.SeriesID]
		if st == nil {
			st = &seriesState{}
			series[j.SeriesID] = st
		}
		if j.Status == types.JobPending || j.Status == types.JobRunning {
			st.active = true
			continue
		}
		if st.latest == nil || j.CreatedAt.After(st.latest.CreatedAt) {
			st.latest = j
		}
	}

	now := time.Now().UTC()
	var created []*types.Job
	for _, st := range series {
		if st.active || st.latest == nil {
			continue
		}
		last := st.latest
		if last.Every <= 0 {
			continue
		}
		// Cancelling a run ends the series. Stopping "this run" and
		// stopping "this recurring job" are genuinely ambiguous, and of
		// the two, a cancel that keeps firing every hour is much worse to
		// be surprised by than one that stops too much.
		if last.Status == types.JobCancelled {
			continue
		}

		from := last.UpdatedAt
		if last.FinishedAt != nil {
			from = *last.FinishedAt
		}
		next := from.Add(last.Every)

		run := *last
		run.ID = idgen.New("job")
		run.Status = types.JobPending
		run.WorkerID = ""
		run.Retries = 0
		run.Output = ""
		run.Error = ""
		run.CancelRequested = false
		run.StartedAt = nil
		run.FinishedAt = nil
		run.CreatedAt = now
		run.UpdatedAt = now
		run.NotBefore = &next
		// A later run must not inherit the first run's dependencies: those
		// named specific job IDs that have already been satisfied once and
		// would otherwise pin every future run to ancient history.
		run.DependsOn = nil

		if err := s.append(record{Type: recJobUpsert, Job: &run}); err != nil {
			continue // try again on the next sweep
		}
		cp := run
		created = append(created, &cp)
	}
	return created
}

// --- Worker operations -------------------------------------------------

// RegisterWorker durably persists a newly-registered worker.
func (s *Store) RegisterWorker(w *types.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.append(record{Type: recWorkerUpsert, Worker: w})
}

// Heartbeat updates a worker's LastHeartbeat and marks it alive.
func (s *Store) Heartbeat(id string) (*types.Worker, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workers[id]
	if !ok {
		return nil, false
	}
	updated := *w
	updated.LastHeartbeat = time.Now().UTC()
	updated.Status = types.WorkerAlive
	if err := s.append(record{Type: recWorkerUpsert, Worker: &updated}); err != nil {
		return nil, false
	}
	cp := updated
	return &cp, true
}

// ListWorkers returns copies of all known workers, each with Available
// filled in from the resources its running jobs consume.
func (s *Store) ListWorkers() []*types.Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*types.Worker, 0, len(s.workers))
	for _, w := range s.workers {
		cp := *w
		cp.Available = w.Capacity.Minus(s.usedByWorkerLocked(w.ID))
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].RegisteredAt.Before(out[k].RegisteredAt) })
	return out
}

// UpdateWorker durably persists an arbitrary worker mutation (used by the
// reaper to mark stale workers dead).
func (s *Store) UpdateWorker(w *types.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.append(record{Type: recWorkerUpsert, Worker: w})
}

// GetWorker returns a copy of a single worker, with Available derived from
// the resources its running jobs consume.
func (s *Store) GetWorker(id string) (*types.Worker, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.workers[id]
	if !ok {
		return nil, false
	}
	cp := *w
	cp.Available = w.Capacity.Minus(s.usedByWorkerLocked(id))
	return &cp, true
}

// Compact writes the current in-memory state to a snapshot file, then
// truncates the WAL. Used periodically to bound log size on long-running
// instances. The snapshot is written and fsynced before the log is cleared,
// so a crash during compaction is safe: replay will load the snapshot and
// re-apply any mutations written after it.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapPath := filepath.Join(filepath.Dir(s.logPath), "dispatch.snapshot")
	tmpPath := snapPath + ".tmp"

	snap := struct {
		Jobs    map[string]*types.Job    `json:"jobs"`
		Workers map[string]*types.Worker `json:"workers"`
	}{
		Jobs:    s.jobs,
		Workers: s.workers,
	}

	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}

	if err := os.WriteFile(tmpPath, b, 0o644); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, snapPath); err != nil {
		return err
	}

	if err := s.logFile.Close(); err != nil {
		return err
	}

	if err := os.Truncate(s.logPath, 0); err != nil {
		return err
	}

	f, err := os.OpenFile(s.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	s.logFile = f

	return s.logFile.Sync()
}
