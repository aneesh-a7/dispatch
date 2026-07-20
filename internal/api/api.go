// Package api exposes the control plane's HTTP interface. Workers and
// the CLI both talk to this same API — there is no separate internal
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

	s.mux.HandleFunc("POST /v1/workers/register", s.handleRegisterWorker)
	s.mux.HandleFunc("GET /v1/workers", s.handleListWorkers)
	s.mux.HandleFunc("POST /v1/workers/{id}/heartbeat", s.handleHeartbeat)
	s.mux.HandleFunc("POST /v1/workers/{id}/lease", s.handleLease)

	s.mux.HandleFunc("POST /v1/jobs/{id}/complete", s.handleCompleteJob)

	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)

	// Dashboard: catch-all, lowest priority in Go 1.22's pattern matching,
	// so every /v1/* route above still wins. Served on the same port as
	// the API: one process, one port, nothing extra to deploy.
	s.mux.Handle("/", webui.Handler())
}

// --- request/response payloads -----------------------------------------

type submitJobRequest struct {
	Command    string   `json:"command"`
	Args       []string `json:"args"`
	Priority   int      `json:"priority"`
	MaxRetries int      `json:"max_retries"`
}

type registerWorkerRequest struct {
	Address string `json:"address"`
}

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

	now := time.Now().UTC()
	job := &types.Job{
		ID:         idgen.New("job"),
		Command:    req.Command,
		Args:       req.Args,
		Priority:   req.Priority,
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

func (s *Server) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req registerWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	now := time.Now().UTC()
	worker := &types.Worker{
		ID:            idgen.New("worker"),
		Address:       req.Address,
		Status:        types.WorkerAlive,
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
	if req.Status != types.JobSucceeded && req.Status != types.JobFailed {
		writeError(w, http.StatusBadRequest, `status must be "succeeded" or "failed"`)
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

	if req.Status == types.JobFailed && job.Retries < job.MaxRetries {
		// Still have retry budget: requeue instead of terminally failing.
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

// handleMetrics emits a minimal Prometheus text-format exposition. Real
// counters (jobs leased, jobs failed, lease latency) are cheap to add on
// top of this shape in week 2/3; the point in week 1 is that the
// endpoint and format exist from day one rather than bolted on later.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	jobs := s.store.ListJobs()
	workers := s.store.ListWorkers()

	counts := map[types.JobStatus]int{}
	for _, j := range jobs {
		counts[j.Status]++
	}
	aliveWorkers := 0
	for _, wk := range workers {
		if wk.Status == types.WorkerAlive {
			aliveWorkers++
		}
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmtLine := func(name string, v int) {
		w.Write([]byte(name + " " + strconv.Itoa(v) + "\n"))
	}
	fmtLine("dispatch_jobs_pending", counts[types.JobPending])
	fmtLine("dispatch_jobs_running", counts[types.JobRunning])
	fmtLine("dispatch_jobs_succeeded", counts[types.JobSucceeded])
	fmtLine("dispatch_jobs_failed", counts[types.JobFailed])
	fmtLine("dispatch_workers_total", len(workers))
	fmtLine("dispatch_workers_alive", aliveWorkers)
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
