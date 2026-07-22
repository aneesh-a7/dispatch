package store

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aneesh/dispatch/internal/types"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func testJob(id string, priority int, createdAt time.Time) *types.Job {
	return &types.Job{
		ID:        id,
		Command:   "echo",
		Args:      []string{"hi"},
		Priority:  priority,
		Status:    types.JobPending,
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	}
}

func TestCreateAndGetJob(t *testing.T) {
	s := newTestStore(t)
	job := testJob("job-1", 0, time.Now())

	if err := s.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	got, ok := s.GetJob("job-1")
	if !ok {
		t.Fatal("GetJob() ok = false, want true")
	}
	if got.ID != job.ID || got.Command != job.Command {
		t.Errorf("GetJob() = %+v, want match for %+v", got, job)
	}
}

func TestLeaseNextJob_PriorityThenFIFO(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()

	// Deliberately created out of the order we expect them leased in.
	low := testJob("low-priority-old", 0, now)
	high := testJob("high-priority-new", 10, now.Add(1*time.Second))
	lowNewer := testJob("low-priority-newer", 0, now.Add(2*time.Second))

	for _, j := range []*types.Job{low, high, lowNewer} {
		if err := s.CreateJob(j); err != nil {
			t.Fatalf("CreateJob(%s) error = %v", j.ID, err)
		}
	}

	// Highest priority should win regardless of creation order.
	leased, ok := s.LeaseNextJob("worker-1")
	if !ok || leased.ID != "high-priority-new" {
		t.Fatalf("first lease = %+v, ok=%v; want high-priority-new", leased, ok)
	}

	// Among equal priority, oldest (FIFO) should win.
	leased, ok = s.LeaseNextJob("worker-1")
	if !ok || leased.ID != "low-priority-old" {
		t.Fatalf("second lease = %+v, ok=%v; want low-priority-old", leased, ok)
	}

	leased, ok = s.LeaseNextJob("worker-1")
	if !ok || leased.ID != "low-priority-newer" {
		t.Fatalf("third lease = %+v, ok=%v; want low-priority-newer", leased, ok)
	}

	// Queue should now be empty.
	if _, ok := s.LeaseNextJob("worker-1"); ok {
		t.Fatal("LeaseNextJob() on empty queue: ok = true, want false")
	}
}

// TestLeaseNextJob_NoDoubleDispatch is the important one: many workers
// racing to lease from a small pool of jobs must never result in the
// same job being handed to two different workers. This is exactly the
// bug a naive "find pending job, then separately update it" (without a
// shared lock across both steps) would produce.
func TestLeaseNextJob_NoDoubleDispatch(t *testing.T) {
	s := newTestStore(t)
	const numJobs = 50
	const numWorkers = 20

	now := time.Now()
	for i := 0; i < numJobs; i++ {
		id := jobIDForTest(i)
		if err := s.CreateJob(testJob(id, 0, now.Add(time.Duration(i)*time.Millisecond))); err != nil {
			t.Fatalf("CreateJob(%s) error = %v", id, err)
		}
	}

	var (
		wg          sync.WaitGroup
		mu          sync.Mutex
		leasedCount = make(map[string]int) // job ID -> number of times leased
	)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerID := jobIDForTest(w) + "-worker"
		go func(workerID string) {
			defer wg.Done()
			for {
				job, ok := s.LeaseNextJob(workerID)
				if !ok {
					return
				}
				mu.Lock()
				leasedCount[job.ID]++
				mu.Unlock()
			}
		}(workerID)
	}
	wg.Wait()

	if len(leasedCount) != numJobs {
		t.Errorf("leased %d distinct jobs, want %d", len(leasedCount), numJobs)
	}
	for id, count := range leasedCount {
		if count != 1 {
			t.Errorf("job %s leased %d times, want exactly 1 (double-dispatch bug)", id, count)
		}
	}
}

// TestLeaseNextJob_ResourceBinPacking checks that a worker is only handed
// jobs that fit its free capacity, and that finishing a job frees the
// capacity again (Available is derived from running jobs, so releasing is
// automatic).
func TestLeaseNextJob_ResourceBinPacking(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()

	worker := &types.Worker{
		ID: "w-cap", Address: "local", Status: types.WorkerAlive,
		Capacity:     types.Resources{CPU: 4, Memory: 4096},
		RegisteredAt: now, LastHeartbeat: now,
	}
	if err := s.RegisterWorker(worker); err != nil {
		t.Fatalf("RegisterWorker() error = %v", err)
	}

	big := testJob("big", 10, now) // highest priority but needs the whole box
	big.Resources = types.Resources{CPU: 4, Memory: 4096}
	small := testJob("small", 0, now.Add(time.Second))
	small.Resources = types.Resources{CPU: 1, Memory: 512}
	for _, j := range []*types.Job{big, small} {
		if err := s.CreateJob(j); err != nil {
			t.Fatalf("CreateJob(%s) error = %v", j.ID, err)
		}
	}

	// The big job leases first (highest priority, and it fits an empty worker).
	leased, ok := s.LeaseNextJob("w-cap")
	if !ok || leased.ID != "big" {
		t.Fatalf("first lease = %+v, ok=%v; want big", leased, ok)
	}
	// Worker is now full, so the small job must not be leased even though
	// it is the only thing left pending.
	if leased, ok := s.LeaseNextJob("w-cap"); ok {
		t.Fatalf("second lease = %+v, ok=%v; want no fit (worker full)", leased, ok)
	}
	if avail := mustWorker(t, s, "w-cap").Available; avail != (types.Resources{}) {
		t.Fatalf("Available with big job running = %+v, want zero", avail)
	}

	// Finish the big job; its capacity should come back and the small one fits.
	big.Status = types.JobSucceeded
	big.WorkerID = "w-cap"
	if err := s.UpdateJob(big); err != nil {
		t.Fatalf("UpdateJob() error = %v", err)
	}
	if avail := mustWorker(t, s, "w-cap").Available; avail != (types.Resources{CPU: 4, Memory: 4096}) {
		t.Fatalf("Available after big job done = %+v, want full capacity", avail)
	}
	leased, ok = s.LeaseNextJob("w-cap")
	if !ok || leased.ID != "small" {
		t.Fatalf("third lease = %+v, ok=%v; want small", leased, ok)
	}
}

func mustWorker(t *testing.T, s *Store, id string) *types.Worker {
	t.Helper()
	w, ok := s.GetWorker(id)
	if !ok {
		t.Fatalf("GetWorker(%s) ok = false, want true", id)
	}
	return w
}

func TestReplay_RecoversStateAfterReopen(t *testing.T) {
	dir := t.TempDir()

	s1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	job := testJob("persisted-job", 3, time.Now())
	if err := s1.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	worker := &types.Worker{ID: "w-1", Address: "local", Status: types.WorkerAlive,
		RegisteredAt: time.Now(), LastHeartbeat: time.Now()}
	if err := s1.RegisterWorker(worker); err != nil {
		t.Fatalf("RegisterWorker() error = %v", err)
	}
	leased, ok := s1.LeaseNextJob("w-1")
	if !ok {
		t.Fatal("LeaseNextJob() ok = false, want true")
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopen against the same directory: simulates a process restart.
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer s2.Close()

	got, ok := s2.GetJob("persisted-job")
	if !ok {
		t.Fatal("GetJob() after reopen: ok = false, want true")
	}
	if got.Status != types.JobRunning || got.WorkerID != "w-1" {
		t.Errorf("GetJob() after reopen = %+v, want Status=running WorkerID=w-1", got)
	}
	if got.ID != leased.ID {
		t.Errorf("recovered job ID = %s, want %s", got.ID, leased.ID)
	}

	if _, ok := s2.GetWorker("w-1"); !ok {
		t.Error("GetWorker() after reopen: ok = false, want true")
	}
}

func TestCompact_ReducesLogSize(t *testing.T) {
	dir := t.TempDir()

	s1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	now := time.Now()
	job := testJob("job-1", 0, now)
	if err := s1.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	worker := &types.Worker{ID: "w-1", Address: "local", Status: types.WorkerAlive,
		Capacity: types.Resources{CPU: 4, Memory: 4096}, RegisteredAt: now, LastHeartbeat: now}
	if err := s1.RegisterWorker(worker); err != nil {
		t.Fatalf("RegisterWorker() error = %v", err)
	}

	// Write a lot of updates to grow the WAL.
	for i := 0; i < 100; i++ {
		job.UpdatedAt = now.Add(time.Duration(i) * time.Second)
		if err := s1.UpdateJob(job); err != nil {
			t.Fatalf("UpdateJob() error = %v", err)
		}
	}

	// Get WAL size before compaction.
	walInfo, err := os.Stat(s1.logPath)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	walSizeBefore := walInfo.Size()

	// Compact should write a snapshot and truncate the WAL.
	if err := s1.Compact(); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	snapPath := filepath.Join(dir, "dispatch.snapshot")
	if _, err := os.Stat(snapPath); os.IsNotExist(err) {
		t.Fatal("Compact() did not create snapshot file")
	}

	// WAL should be much smaller (only a fresh header or empty).
	walInfo, err = os.Stat(s1.logPath)
	if err != nil {
		t.Fatalf("Stat() after compact error = %v", err)
	}
	walSizeAfter := walInfo.Size()
	if walSizeAfter >= walSizeBefore {
		t.Fatalf("WAL size after compact = %d, before = %d; want reduction", walSizeAfter, walSizeBefore)
	}

	if err := s1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopen and verify state is recovered from the snapshot.
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer s2.Close()

	got, ok := s2.GetJob("job-1")
	if !ok {
		t.Fatal("GetJob() after reopen: ok = false, want true")
	}
	if got.UpdatedAt.Unix() != job.UpdatedAt.Unix() {
		t.Errorf("GetJob() UpdatedAt = %v, want %v", got.UpdatedAt, job.UpdatedAt)
	}

	if _, ok := s2.GetWorker("w-1"); !ok {
		t.Fatal("GetWorker() after reopen: ok = false, want true")
	}
}

func jobIDForTest(i int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if i < len(letters) {
		return "job-" + string(letters[i])
	}
	return "job-" + string(letters[i%len(letters)]) + string(letters[i/len(letters)])
}
