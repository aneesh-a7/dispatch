package store

import (
	"testing"
	"time"

	"github.com/aneesh/dispatch/internal/types"
)

func finish(t *testing.T, s *Store, id string, status types.JobStatus) {
	t.Helper()
	j, ok := s.GetJob(id)
	if !ok {
		t.Fatalf("GetJob(%s) ok = false", id)
	}
	now := time.Now().UTC()
	j.Status = status
	j.FinishedAt = &now
	j.UpdatedAt = now
	if err := s.UpdateJob(j); err != nil {
		t.Fatalf("UpdateJob(%s) error = %v", id, err)
	}
}

// A dependent job must stay queued until its prerequisite succeeds, even
// though nothing else is competing for the worker.
func TestLeaseNextJob_WaitsForDependency(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()

	first := testJob("first", 0, now)
	second := testJob("second", 10, now.Add(time.Second)) // higher priority on purpose
	second.DependsOn = []string{"first"}
	for _, j := range []*types.Job{first, second} {
		if err := s.CreateJob(j); err != nil {
			t.Fatalf("CreateJob(%s) error = %v", j.ID, err)
		}
	}

	// Priority would normally put "second" first; the dependency outranks it.
	leased, ok := s.LeaseNextJob("w1")
	if !ok || leased.ID != "first" {
		t.Fatalf("first lease = %+v, ok=%v; want first", leased, ok)
	}
	if leased, ok := s.LeaseNextJob("w1"); ok {
		t.Fatalf("second lease = %+v; want nothing (dependency unfinished)", leased)
	}

	finish(t, s, "first", types.JobSucceeded)

	leased, ok = s.LeaseNextJob("w1")
	if !ok || leased.ID != "second" {
		t.Fatalf("lease after dependency succeeded = %+v, ok=%v; want second", leased, ok)
	}
}

// A job whose prerequisite failed can never run, so it must be failed
// rather than left looking merely queued.
func TestSweepBlockedJobs_FailsWhenDependencyFails(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()

	if err := s.CreateJob(testJob("first", 0, now)); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}
	second := testJob("second", 0, now.Add(time.Second))
	second.DependsOn = []string{"first"}
	if err := s.CreateJob(second); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}

	// While the dependency is merely unfinished, nothing is swept.
	if changed := s.SweepBlockedJobs(); len(changed) != 0 {
		t.Fatalf("swept %d jobs before the dependency finished, want 0", len(changed))
	}

	finish(t, s, "first", types.JobFailed)

	changed := s.SweepBlockedJobs()
	if len(changed) != 1 || changed[0].ID != "second" {
		t.Fatalf("swept = %+v, want just second", changed)
	}
	got, _ := s.GetJob("second")
	if got.Status != types.JobFailed {
		t.Errorf("second status = %s, want failed", got.Status)
	}
	if got.Error == "" {
		t.Error("second has no error message explaining why it failed")
	}
	// Sweeping again must not re-report an already-failed job.
	if changed := s.SweepBlockedJobs(); len(changed) != 0 {
		t.Errorf("second sweep returned %d jobs, want 0 (not idempotent)", len(changed))
	}
}

// Cancelling a prerequisite is just as fatal to its dependents as failing.
func TestSweepBlockedJobs_FailsWhenDependencyCancelled(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()

	if err := s.CreateJob(testJob("first", 0, now)); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}
	second := testJob("second", 0, now.Add(time.Second))
	second.DependsOn = []string{"first"}
	if err := s.CreateJob(second); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}

	finish(t, s, "first", types.JobCancelled)

	if changed := s.SweepBlockedJobs(); len(changed) != 1 {
		t.Fatalf("swept %d jobs, want 1", len(changed))
	}
	got, _ := s.GetJob("second")
	if got.Status != types.JobFailed {
		t.Errorf("second status = %s, want failed", got.Status)
	}
}

func TestJobExists(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateJob(testJob("real", 0, time.Now())); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}
	if !s.JobExists("real") {
		t.Error("JobExists(real) = false, want true")
	}
	if s.JobExists("imaginary") {
		t.Error("JobExists(imaginary) = true, want false")
	}
}

// --- recurring jobs ------------------------------------------------------

func recurringJob(id string, every time.Duration, createdAt time.Time) *types.Job {
	j := testJob(id, 0, createdAt)
	j.Every = every
	j.SeriesID = id
	return j
}

func TestSweepRecurringJobs_QueuesNextRunAfterFinish(t *testing.T) {
	s := newTestStore(t)

	if err := s.CreateJob(recurringJob("run1", time.Hour, time.Now())); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}

	// Nothing is due while the first run is still queued.
	if created := s.SweepRecurringJobs(); len(created) != 0 {
		t.Fatalf("created %d runs while the series was active, want 0", len(created))
	}

	finish(t, s, "run1", types.JobSucceeded)

	created := s.SweepRecurringJobs()
	if len(created) != 1 {
		t.Fatalf("created %d runs, want 1", len(created))
	}
	next := created[0]
	if next.ID == "run1" {
		t.Error("next run reused the previous run's ID")
	}
	if next.SeriesID != "run1" {
		t.Errorf("next run SeriesID = %q, want run1", next.SeriesID)
	}
	if next.Status != types.JobPending {
		t.Errorf("next run status = %s, want pending", next.Status)
	}
	if next.NotBefore == nil {
		t.Fatal("next run has no NotBefore, so it would run immediately")
	}
	if !next.NotBefore.After(time.Now()) {
		t.Errorf("next run NotBefore = %v, want a future time", next.NotBefore)
	}

	// The freshly queued run makes the series active again, so a second
	// sweep must not stack up another copy.
	if created := s.SweepRecurringJobs(); len(created) != 0 {
		t.Errorf("second sweep created %d more runs, want 0 (duplicate series runs)", len(created))
	}
}

// A scheduled run must not be leasable before its time, or the interval
// means nothing.
func TestLeaseNextJob_RespectsNotBefore(t *testing.T) {
	s := newTestStore(t)

	future := time.Now().UTC().Add(time.Hour)
	j := testJob("later", 0, time.Now())
	j.NotBefore = &future
	if err := s.CreateJob(j); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}

	if leased, ok := s.LeaseNextJob("w1"); ok {
		t.Fatalf("leased %+v before NotBefore, want nothing", leased)
	}

	past := time.Now().UTC().Add(-time.Minute)
	j.NotBefore = &past
	if err := s.UpdateJob(j); err != nil {
		t.Fatalf("UpdateJob error = %v", err)
	}
	if _, ok := s.LeaseNextJob("w1"); !ok {
		t.Error("job not leasable after NotBefore passed")
	}
}

// Cancelling is how you stop a recurring job. If the series kept going
// after a cancel there would be no way to turn one off.
func TestSweepRecurringJobs_CancelStopsTheSeries(t *testing.T) {
	s := newTestStore(t)

	if err := s.CreateJob(recurringJob("run1", time.Hour, time.Now())); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}
	finish(t, s, "run1", types.JobCancelled)

	if created := s.SweepRecurringJobs(); len(created) != 0 {
		t.Errorf("created %d runs after a cancel, want 0", len(created))
	}
}

// A failed run should still schedule the next one: a recurring job that
// silently stopped the first time it errored would be worse than useless.
func TestSweepRecurringJobs_FailureStillRecurs(t *testing.T) {
	s := newTestStore(t)

	if err := s.CreateJob(recurringJob("run1", time.Hour, time.Now())); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}
	finish(t, s, "run1", types.JobFailed)

	if created := s.SweepRecurringJobs(); len(created) != 1 {
		t.Errorf("created %d runs after a failure, want 1", len(created))
	}
}

// A one-shot job must never sprout a successor.
func TestSweepRecurringJobs_IgnoresNonRecurringJobs(t *testing.T) {
	s := newTestStore(t)

	if err := s.CreateJob(testJob("once", 0, time.Now())); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}
	finish(t, s, "once", types.JobSucceeded)

	if created := s.SweepRecurringJobs(); len(created) != 0 {
		t.Errorf("created %d runs for a non-recurring job, want 0", len(created))
	}
}

// The first run's dependencies were satisfied once, by specific job IDs
// that are now in the past. Carrying them forward would pin every future
// run to that same history.
func TestSweepRecurringJobs_NextRunDropsDependencies(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()

	if err := s.CreateJob(testJob("setup", 0, now)); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}
	rec := recurringJob("run1", time.Hour, now.Add(time.Second))
	rec.DependsOn = []string{"setup"}
	if err := s.CreateJob(rec); err != nil {
		t.Fatalf("CreateJob error = %v", err)
	}

	finish(t, s, "setup", types.JobSucceeded)
	finish(t, s, "run1", types.JobSucceeded)

	created := s.SweepRecurringJobs()
	if len(created) != 1 {
		t.Fatalf("created %d runs, want 1", len(created))
	}
	if len(created[0].DependsOn) != 0 {
		t.Errorf("next run inherited DependsOn = %v, want none", created[0].DependsOn)
	}
}
