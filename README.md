# dispatch

A small distributed job scheduler: submit shell commands, have them run on
whichever of your machines has a free worker, survive crashes without
losing track of what was running.

I built this because I kept manually SSHing into whichever laptop/desktop
was free to kick off long-running scripts (Monte Carlo simulations,
batch data pulls) and then forgetting which machine was running what.
`dispatch` is the smallest version of "a scheduler" that actually solves
that: a control plane that tracks jobs durably, and worker agents that
pull work and report back.

It is not Kubernetes. It does not try to be. It's scoped to the parts of
"distributed job scheduling" that are actually hard and actually
interesting: durable state, failure detection, and at-least-once
execution, without the surface area of a real orchestrator.

## How it works

```
                 ┌─────────────────┐
  dispatchctl ──▶│  control plane   │◀── worker (polls for work)
  (CLI)          │  - HTTP API      │◀── worker
                 │  - WAL store     │◀── worker
                 │  - scheduler     │
                 │  - reaper        │
                 └──────────────────┘
```

- **Control plane**: the only stateful component. Exposes an HTTP API,
  persists every state transition to a write-ahead log before applying
  it in memory, and runs a scheduler (job leasing) and a reaper
  (dead-worker detection).
- **Worker**: stateless. Registers, heartbeats, polls for a job, runs it
  as a subprocess, reports the result. Can be killed and restarted
  freely: it holds no state the control plane depends on.
- **dispatchctl**: CLI for submitting jobs and checking status.

Workers *pull* work rather than having it pushed to them. This means the
control plane never needs to open a connection to a worker (no NAT/firewall
problems) and workers can come and go without the control plane needing
to know anything about their reachability ahead of time.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design
decisions and trade-offs: what this does, what it deliberately doesn't
do yet, and why.

## Quickstart

Requires Go 1.22+. No external dependencies, pure standard library.

```bash
# Terminal 1: start the control plane
go run ./cmd/controlplane

# Terminal 2: start a worker
go run ./cmd/worker

# Terminal 3: submit work
go run ./cmd/dispatchctl submit -- echo "hello dispatch"
go run ./cmd/dispatchctl submit -priority 5 -retries 2 -- python3 my_script.py
go run ./cmd/dispatchctl submit -cpu 2 -memory 1024 -- ./heavy_job

go run ./cmd/dispatchctl list
go run ./cmd/dispatchctl status <job-id>
go run ./cmd/dispatchctl cancel <job-id>
```

Or open **http://localhost:8080** in a browser once the control plane is
running. The dashboard is built around a live cluster view: each worker
is a small animated sprite that reacts as work happens (it grabs a job
when it starts, cheers on success, shudders on failure), jobs slide from
the queue onto a worker, and a bar under each sprite shows how much of
its capacity is in use. A stats strip up top tracks running, queued,
throughput, and average queue wait. You can submit jobs from a form,
click any queued or running job to cancel it, and hit "Add worker" to
open a terminal on the same machine with a worker ready to run.

Multiple workers can be started against the same control plane. Jobs are
leased to whichever worker has free capacity, one at a time per worker,
with no double-dispatch (see `Store.LeaseNextJob`).

## What happens when a worker dies mid-job

Kill a worker (`kill -9`) while it's running a job. The control plane's
reaper notices the missed heartbeats within `-heartbeat-ttl` (default
15s), marks the worker dead, and requeues its in-flight job (or fails it
permanently if the retry budget is exhausted). No job silently
disappears because its worker vanished.

## What happens when the control plane crashes

Kill `-9` the control plane process and restart it against the same
`-data-dir`. It replays its write-ahead log on startup and comes back
with every job and worker exactly where it left off. Nothing is lost
except whatever hadn't been fsync'd yet (which, by construction, is
nothing that was ever acknowledged to a caller).

## Resource-aware scheduling and cancellation

Jobs can declare a CPU/memory request and workers advertise a capacity
(`-cpu`, `-memory`). Leasing is bin-packing: a job only goes to a worker
with enough room for it, and a worker's free capacity is tracked as its
running jobs consume and release it. A job left at the default (zero)
fits anywhere and consumes nothing, so this stays opt-in.

Cancel a job with `dispatchctl cancel <job-id>`, the dashboard, or
`DELETE /v1/jobs/{id}`. A pending job is dropped immediately; a running
job is signalled to its worker, which kills the subprocess and reports it
as cancelled. Cancelling beats any remaining retries.

## Metrics and load testing

`/metrics` serves plain Prometheus text: job status counts, retry and
cancellation totals, worker capacity, and lease-latency and
execution-duration sums/counts (so a scrape can derive averages). To get
throughput numbers, run the bundled load test against a live cluster:

```bash
go run ./cmd/loadtest -n 500          # submits 500 jobs, reports jobs/sec + p50/p95/p99
```

## CLI reference

```
dispatchctl submit [-priority N] [-retries N] [-cpu N] [-memory MB] <command> [args...]
dispatchctl status <job-id>
dispatchctl cancel <job-id>
dispatchctl list
```

## Project layout

```
cmd/
  controlplane/   entrypoint: HTTP server, store, scheduler, reaper
  worker/         entrypoint: registration, heartbeats, polling, execution
  dispatchctl/    entrypoint: CLI
  loadtest/       entrypoint: throughput/latency load generator
internal/
  types/          shared domain types (Job, Worker, Resources)
  store/          write-ahead log + in-memory index
  scheduler/      resource-aware leasing + dead-worker reaper
  api/            HTTP handlers
  client/         typed HTTP client (shared by worker + CLI)
  idgen/          stdlib-only sortable ID generation
  webui/          embedded live dashboard (static HTML/CSS/JS, served by the control plane)
docs/
  ARCHITECTURE.md design decisions, trade-offs, what's deliberately out of scope
```

## Status

Durable control plane, pull-based workers, priority + resource-aware
(bin-packing) leasing, retries, dead-worker reaping, job cancellation,
Prometheus metrics, a load-test tool, and a live sprite dashboard are all
built and manually tested end to end. Still deliberately out of scope:
sandboxed execution, WAL compaction, auth, and a multi-node (HA) control
plane. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the reasoning
on each.
