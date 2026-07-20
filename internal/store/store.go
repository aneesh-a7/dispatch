// Package store implements durable state for the control plane using a
// simple write-ahead log (WAL): every mutation is appended to an
// append-only file and fsync'd before the in-memory state is updated.
// On startup the log is replayed from the beginning to rebuild state.
//
// This is the same core idea real databases use (Postgres's WAL, etcd's
// raft log, Kafka's segment log): never mutate state that isn't first
// durably recorded, so a crash between "decided" and "applied" is
// recoverable by replay instead of lost.
//
// It deliberately does NOT do log compaction/snapshotting. The log
// grows forever. That's a known, named limitation (see docs/ARCHITECTURE.md)
// rather than an oversight: a compaction pass that periodically rewrites
// the log to only current state is the natural next step.
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

// replay reads the WAL from the start and applies every record to
// rebuild in-memory state. It is only called once, from Open, before the
// log file handle for appending is created.
func (s *Store) replay() error {
	f, err := os.OpenFile(s.logPath, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// WAL lines can be long (job output); grow the buffer past the default 64KB.
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
			// A malformed trailing line usually means the process died
			// mid-write. We skip it rather than fail startup entirely:
			// losing the last unflushed record is expected WAL behavior;
			// silently corrupting earlier history would not be.
			fmt.Fprintf(os.Stderr, "store: skipping malformed WAL line %d: %v\n", lineNum, err)
			continue
		}
		s.apply(rec)
	}
	return scanner.Err()
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

// LeaseNextJob atomically finds the highest-priority, oldest pending job,
// marks it running and assigned to workerID, persists that transition,
// and returns it. Doing the "find + mutate + persist" sequence under a
// single lock is what prevents two workers from racing to lease the same
// job: the classic double-dispatch bug in naive queue implementations.
func (s *Store) LeaseNextJob(workerID string) (*types.Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var best *types.Job
	for _, j := range s.jobs {
		if j.Status != types.JobPending {
			continue
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

	now := time.Now().UTC()
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

// ListWorkers returns copies of all known workers.
func (s *Store) ListWorkers() []*types.Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*types.Worker, 0, len(s.workers))
	for _, w := range s.workers {
		cp := *w
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

// GetWorker returns a copy of a single worker.
func (s *Store) GetWorker(id string) (*types.Worker, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.workers[id]
	if !ok {
		return nil, false
	}
	cp := *w
	return &cp, true
}
