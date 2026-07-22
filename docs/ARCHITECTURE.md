# Architecture & Design Decisions

This document exists so that every corner cut in this codebase is a
decision, not an accident. Anything not in "What's deliberately out of
scope" below is either handled or a known week-2/3 gap noted inline in
the code.

## Core design choices

### Pull-based leasing, not push-based dispatch

The control plane never initiates a connection to a worker. Workers poll
`POST /v1/workers/{id}/lease`. This trades a small amount of latency
(bounded by `-poll-interval`) for a much simpler failure model: the
control plane doesn't need to know if a worker is reachable, behind NAT,
or on a different network. It only needs to know if a worker is
*talking to it*, which is exactly what heartbeats measure.

### Write-ahead log instead of an embedded database

`internal/store` is a hand-rolled append-only log: every state
transition is JSON-serialized, `fsync`'d, and only then applied to the
in-memory maps that actually serve reads. On startup, the log is
replayed from byte zero to rebuild state.

This was a deliberate choice over reaching for SQLite or an embedded
KV store: the goal of this project is to demonstrate understanding of
*why* durable systems are built the way they are, not to demonstrate
knowing how to import a database driver. A WAL is the mechanism
underneath Postgres, etcd's Raft log, and Kafka. Building the smallest
correct version of that mechanism is more informative than wrapping one.

**Trade-off knowingly accepted:** every write pays an `fsync`, so this
will never be fast under heavy write load. That's the right trade for a
job scheduler (a lost job is worse than a slow submission) and the wrong
trade for something like a metrics pipeline, worth saying explicitly
rather than leaving as an unstated assumption.

### At-least-once job execution, not exactly-once

If a worker successfully runs a job but crashes (or loses network)
*before* its `POST /v1/jobs/{id}/complete` call lands, the control plane
has no way to distinguish "job never ran" from "job ran but the report
was lost." The reaper will eventually time out that worker and requeue
the job, meaning it can run twice.

This is a real, common distributed-systems trade-off (see: at-least-once
vs. exactly-once delivery in any message queue). Getting to exactly-once
would require either idempotency keys the *job* itself understands (out
of this system's control (the job is an arbitrary shell command) or a
two-phase commit between worker and control plane, which trades
complexity for a guarantee that most job types don't actually need.
Commands run by this system should be written to be safely re-runnable
(idempotent) where that matters, the same expectation most real batch
schedulers place on their jobs.

### Single control-plane node, no HA, no consensus

There is exactly one control plane process and it is a single point of
failure. This is the single biggest thing cut from "production-grade" to
fit the scope of this project, and it's cut on purpose rather than
half-attempted.

**What full HA would require**, roughly, in order of what I'd build:
1. **Replicated log**: the WAL in `internal/store` would need to be
   replicated to a quorum of nodes before being considered committed.
   This is what Raft (or Paxos) actually buys you. etcd is the reference
   implementation; a from-scratch version would start with leader
   election and log replication and add membership changes later.
2. **Leader election**: only the elected leader accepts writes (job
   submission, leasing); followers redirect or proxy to the leader.
3. **Client redirect / retry logic**: `dispatchctl` and the worker's HTTP
   client would need to handle "not the leader, retry against X."
4. **Split-brain handling**: a network partition must never let two
   nodes both believe they're the leader and hand out conflicting leases.
   This is precisely what quorum-based consensus prevents, and naive
   leader-election-by-heartbeat does not.

None of this is implemented. It's flagged here because knowing the next
step and choosing not to build it in a 3-week scope is a different thing
than not knowing it's needed.

### Retry semantics

A job that fails gets requeued (not failed) as long as `Retries <
MaxRetries`, whether the failure was reported by the worker (bad exit
code) or inferred by the reaper (worker died mid-execution). Both paths
share the same state machine so there's exactly one place
(`types.JobStatus` transitions) that defines what "failed" means.

### Resource-aware scheduling (bin-packing)

Leasing is not pure priority/FIFO. A job carries a `Resources` request
(CPU units and memory) and a worker carries a `Capacity`. `LeaseNextJob`
hands a worker the highest-priority, oldest pending job that still fits
its free capacity, and skips jobs that do not. A zero request fits
anywhere and consumes nothing, so resource-awareness is opt-in and older
untagged jobs behave exactly as before.

The interesting decision is that a worker's *available* capacity is not
stored. It is derived on read as `Capacity` minus the resources of the
jobs currently running on it. The running jobs are already durable in the
WAL, so there is nothing extra to persist, no separate counter that can
drift, and no explicit "release on completion" step to forget: a job
leaving the running state frees its capacity automatically. The cost is
recomputing a small sum on each lease and each worker read, which at this
scale is free.

### Job cancellation

`DELETE /v1/jobs/{id}` stops a job. A pending job is cancelled outright,
since no worker holds it. A running job is a different problem: the
subprocess lives on the worker, and the control plane never dials
workers (see pull-based leasing above), so it cannot reach in and kill
it. Instead it sets a `CancelRequested` flag on the job. The worker,
while running a job, polls its own job record and cancels the
subprocess's context the moment it sees the flag, then reports back as
`cancelled`. A cancellation is terminal and beats any remaining retry
budget: the user asked it to stop, so it stays stopped.

This reuses the existing pull model rather than adding a push channel. It
costs a little latency (bounded by the worker's cancel-poll interval),
the same trade already made for leasing.

### Metrics

`/metrics` is plain Prometheus text, no client library. Beyond status
counts it exposes lease latency (queue wait) and execution duration as a
running `_sum` plus `_count`, the shape Prometheus histograms use, so a
scrape can derive an average without the endpoint holding any state
between calls. Everything is computed from the timestamps already on each
job. `cmd/loadtest` uses the same timestamps to report throughput and
lease/exec percentiles for a burst of jobs.

### Log compaction

The WAL would grow forever if left alone, so the control plane runs a
periodic compaction pass: every hour (configurable via `-compact-interval`),
it writes a snapshot of current state and truncates the log. On startup, if
a snapshot exists, it's loaded first and then the WAL is replayed. This
speeds up restarts for long-running instances without sacrificing durability:
the snapshot is written and fsynced before the log is cleared, so a crash
during compaction can be recovered by loading the snapshot and re-playing
any mutations written after it.

## What's deliberately out of scope

- **Sandboxed execution.** Workers run jobs as plain subprocesses via
  `os/exec`. No namespace/cgroup isolation, no container runtime. A
  malicious or buggy job has full access to the worker's environment.
  This is the single most important thing to fix before running
  anything untrusted, and is the natural next phase of this project.
- **Auth.** The HTTP API has no authentication. Fine for a local/trusted
  network; not fine for anything exposed beyond that. The dashboard's
  "add worker" endpoint (which opens a local terminal) leans on the same
  assumption and should never be exposed on a shared control plane.
