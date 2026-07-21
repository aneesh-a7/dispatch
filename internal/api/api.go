// Package api exposes the control plane's HTTP interface. Workers and
// the CLI both talk to this same API. There is no separate internal
// protocol, which keeps the system easy to reason about and easy to
// test with plain curl.
package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/aneesh/dispatch/internal/idgen"
	"github.com/aneesh/dispatch/internal/scheduler"
	"github.com/aneesh/dispatch/internal/store"
	"github.com/aneesh/dispatch/internal/types"
	"github.com/aneesh/dispatch/internal/webui"
)

type Server struct {
	store *store.Store
	sched *scheduler.Scheduler
	mux   *http.ServeMux
}

func NewServer(s *store.Store, sc *scheduler.Scheduler) *Server {
	srv := &Server{store: s, sched: sc, mux: http.NewServeMux()}
	srv.routes()
	return srv
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// routes wires up the API using Go 1.22's method+pattern ServeMux
// matching (e.g. "POST /v1/jobs/{id}"), which removes the need for a
// third-party router for a surface area this small.
func (s *Server) routes() {
	s.mux.HandleFunc("POST /v1/jobs", s.handleSubmitJob)
	s.mux.HandleFunc("GET /v1/jobs", s.handleListJobs)
	s.mux.HandleFunc("GET /v1/jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("DELETE /v1/jobs/{id}", s.handleCancelJob)

	s.mux.HandleFunc("POST /v1/workers/register", s.handleRegisterWorker)
	s.mux.HandleFunc("GET /v1/workers", s.handleListWorkers)
	s.mux.HandleFunc("POST /v1/workers/{id}/heartbeat", s.handleHeartbeat)
	s.mux.HandleFunc("POST /v1/workers/{id}/lease", s.handleLease)

	s.mux.HandleFunc("POST /v1/jobs/{id}/complete", s.handleCompleteJob)

	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)

	// Local dev convenience powering the dashboard's "Add worker" button.
	// Unlike every route above, this one acts on the control plane's own
	// machine rather than on shared state. See handleSpawnWorker.
	s.mux.HandleFunc("POST /v1/dev/spawn-worker", s.handleSpawnWorker)

	// Dashboard: catch-all, lowest priority in Go 1.22's pattern matching,
	// so every /v1/* route above still wins. Served on the same port as
	// the API: one process, one port, nothing extra to deploy.
	s.mux.Handle("/", webui.Handler())
}

// --- request/response payloads -----------------------------------------

type submitJobRequest struct {
	Command    string          `json:"command"`
	Args       []string        `json:"args"`
	Priority   int             `json:"priority"`
	MaxRetries int             `json:"max_retries"`
	Resources  types.Resources `json:"resources"`
}

type registerWorkerRequest struct {
	Address  string          `json:"address"`
	Capacity types.Resources `json:"capacity"`
}

// defaultWorkerCapacity is applied when a worker registers without stating
// one, so a worker is never accidentally unable to run resource-tagged
// jobs. The numbers are arbitrary units, sized so a handful of small jobs
// pack onto one worker.
var defaultWorkerCapacity = types.Resources{CPU: 4, Memory: 4096}

type completeJobRequest struct {
	Status types.JobStatus `json:"status"` // "succeeded" or "failed"
	Output string          `json:"output"`
	Error  string          `json:"error"`
}

// --- handlers ------------------------------------------------------------

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Command == "" {
		writeError(w, http.StatusBadRequest, "command is required")
		return
	}
	if req.MaxRetries < 0 {
		req.MaxRetries = 0
	}
	if req.Resources.CPU < 0 {
		req.Resources.CPU = 0
	}
	if req.Resources.Memory < 0 {
		req.Resources.Memory = 0
	}

	now := time.Now().UTC()
	job := &types.Job{
		ID:         idgen.New("job"),
		Command:    req.Command,
		Args:       req.Args,
		Priority:   req.Priority,
		Resources:  req.Resources,
		MaxRetries: req.MaxRetries,
		Status:     types.JobPending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := s.store.CreateJob(job); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to persist job: "+err.Error())
		return
	}
	log.Printf("api: submitted job %s (%s %v)", job.ID, job.Command, job.Args)
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.store.ListJobs())
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, ok := s.store.GetJob(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// handleCancelJob stops a job. A pending job is cancelled outright, since
// no worker has it yet. A running job cannot be stopped from here (the
// subprocess lives on the worker), so this sets CancelRequested and lets
// the worker, which polls its own job, kill the process and report back
// as cancelled. Already-finished jobs cannot be cancelled.
func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, ok := s.store.GetJob(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if job.Status.Terminal() {
		writeError(w, http.StatusConflict, "job already "+string(job.Status))
		return
	}

	now := time.Now().UTC()
	job.UpdatedAt = now
	if job.Status == types.JobPending {
		job.Status = types.JobCancelled
		job.FinishedAt = &now
		log.Printf("api: cancelled pending job %s", id)
	} else {
		// running: signal the worker; it will do the actual killing.
		job.CancelRequested = true
		log.Printf("api: cancel requested for running job %s on worker %s", id, job.WorkerID)
	}
	if err := s.store.UpdateJob(job); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to persist cancellation: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req registerWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	capacity := req.Capacity
	if capacity == (types.Resources{}) {
		capacity = defaultWorkerCapacity
	}
	now := time.Now().UTC()
	worker := &types.Worker{
		ID:            idgen.New("worker"),
		Address:       req.Address,
		Status:        types.WorkerAlive,
		Capacity:      capacity,
		RegisteredAt:  now,
		LastHeartbeat: now,
	}
	if err := s.store.RegisterWorker(worker); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to persist worker: "+err.Error())
		return
	}
	log.Printf("api: registered worker %s (%s)", worker.ID, worker.Address)
	writeJSON(w, http.StatusCreated, worker)
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.store.ListWorkers())
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	worker, ok := s.store.Heartbeat(id)
	if !ok {
		writeError(w, http.StatusNotFound, "worker not found; register first")
		return
	}
	writeJSON(w, http.StatusOK, worker)
}

// handleLease is polled by workers asking "do you have work for me?".
// Pull-based leasing (rather than the control plane pushing jobs) keeps
// workers dumb and stateless from the control plane's point of view:
// they can come and go, and the control plane never needs to dial them.
func (s *Server) handleLease(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.store.GetWorker(id); !ok {
		writeError(w, http.StatusNotFound, "worker not found; register first")
		return
	}
	job, ok := s.sched.Lease(id)
	if !ok {
		w.WriteHeader(http.StatusNoContent) // queue empty; worker should back off and retry
		return
	}
	log.Printf("api: leased job %s to worker %s", job.ID, id)
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleCompleteJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req completeJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Status != types.JobSucceeded && req.Status != types.JobFailed && req.Status != types.JobCancelled {
		writeError(w, http.StatusBadRequest, `status must be "succeeded", "failed", or "cancelled"`)
		return
	}

	job, ok := s.store.GetJob(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	now := time.Now().UTC()
	job.Output = req.Output
	job.UpdatedAt = now
	job.FinishedAt = &now
	job.CancelRequested = false

	if req.Status == types.JobFailed && job.Retries < job.MaxRetries {
		// Still have retry budget: requeue instead of terminally failing.
		// A cancellation never lands here; the user asked it to stop, so it
		// stays stopped regardless of remaining retries.
		job.Status = types.JobPending
		job.Retries++
		job.WorkerID = ""
		job.StartedAt = nil
		job.FinishedAt = nil
		job.Error = req.Error
		log.Printf("api: job %s failed, retrying (%d/%d): %s", id, job.Retries, job.MaxRetries, req.Error)
	} else {
		job.Status = req.Status
		job.Error = req.Error
		log.Printf("api: job %s finished as %s", id, job.Status)
	}

	if err := s.store.UpdateJob(job); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to persist completion: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// handleMetrics emits a Prometheus text-format exposition. Beyond bare
// status counts it reports the two latencies you actually reach for when
// something feels slow: lease latency (how long a job waited in the queue
// before a worker picked it up) and execution duration (how long it then
// ran). Both are exposed as a running sum plus a count, the same shape
// Prometheus histograms use for their _sum/_count, so a scrape can derive
// an average without this endpoint keeping any state between calls.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	jobs := s.store.ListJobs()
	workers := s.store.ListWorkers()

	counts := map[types.JobStatus]int{}
	retriesTotal := 0
	var leaseSum, execSum float64
	var leaseCount, execCount int
	for _, j := range jobs {
		counts[j.Status]++
		retriesTotal += j.Retries
		if j.StartedAt != nil {
			leaseSum += j.StartedAt.Sub(j.CreatedAt).Seconds()
			leaseCount++
			if j.FinishedAt != nil {
				execSum += j.FinishedAt.Sub(*j.StartedAt).Seconds()
				execCount++
			}
		}
	}

	aliveWorkers := 0
	var capTotal, capAvail types.Resources
	for _, wk := range workers {
		if wk.Status == types.WorkerAlive {
			aliveWorkers++
		}
		capTotal = capTotal.Plus(wk.Capacity)
		capAvail = capAvail.Plus(wk.Available)
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmtInt := func(name string, v int) {
		w.Write([]byte(name + " " + strconv.Itoa(v) + "\n"))
	}
	fmtFloat := func(name string, v float64) {
		w.Write([]byte(name + " " + strconv.FormatFloat(v, 'f', 4, 64) + "\n"))
	}
	fmtInt("dispatch_jobs_pending", counts[types.JobPending])
	fmtInt("dispatch_jobs_running", counts[types.JobRunning])
	fmtInt("dispatch_jobs_succeeded", counts[types.JobSucceeded])
	fmtInt("dispatch_jobs_failed", counts[types.JobFailed])
	fmtInt("dispatch_jobs_cancelled", counts[types.JobCancelled])
	fmtInt("dispatch_job_retries_total", retriesTotal)
	fmtFloat("dispatch_lease_latency_seconds_sum", leaseSum)
	fmtInt("dispatch_lease_latency_seconds_count", leaseCount)
	fmtFloat("dispatch_execution_seconds_sum", execSum)
	fmtInt("dispatch_execution_seconds_count", execCount)
	fmtInt("dispatch_workers_total", len(workers))
	fmtInt("dispatch_workers_alive", aliveWorkers)
	fmtInt("dispatch_worker_cpu_total", capTotal.CPU)
	fmtInt("dispatch_worker_cpu_available", capAvail.CPU)
	fmtInt("dispatch_worker_memory_total", capTotal.Memory)
	fmtInt("dispatch_worker_memory_available", capAvail.Memory)
}

// --- small helpers -------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: failed to encode response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
