package store

import (
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

	// Reopen against the same directory — simulates a process restart.
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

func jobIDForTest(i int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if i < len(letters) {
		return "job-" + string(letters[i])
	}
	return "job-" + string(letters[i%len(letters)]) + string(letters[i/len(letters)])
}
