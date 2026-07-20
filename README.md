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

go run ./cmd/dispatchctl list
go run ./cmd/dispatchctl status <job-id>
```

Or open **http://localhost:8080** in a browser once the control plane is
running: a live dashboard shows jobs and workers updating in real time,
with a form to submit new jobs without touching the CLI.

Multiple workers can be started against the same control plane. Jobs
are leased to whichever worker polls first, one at a time, with no
double-dispatch (see `Store.LeaseNextJob`).

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

## CLI reference

```
dispatchctl submit [-priority N] [-retries N] <command> [args...]
dispatchctl status <job-id>
dispatchctl list
```

## Project layout

```
cmd/
  controlplane/   entrypoint: HTTP server, store, scheduler, reaper
  worker/         entrypoint: registration, heartbeats, polling, execution
  dispatchctl/    entrypoint: CLI
internal/
  types/          shared domain types (Job, Worker)
  store/          write-ahead log + in-memory index
  scheduler/      job leasing + dead-worker reaper
  api/            HTTP handlers
  client/         typed HTTP client (shared by worker + CLI)
  idgen/          stdlib-only sortable ID generation
  webui/          embedded live dashboard (static HTML/CSS/JS, served by the control plane)
docs/
  ARCHITECTURE.md design decisions, trade-offs, what's deliberately out of scope
```

## Status

This is the week-1 skeleton: correctness and durability first. Not yet
built: resource-aware scheduling (bin-packing by CPU/mem), job
cancellation, sandboxed execution, and a real metrics/observability
story beyond the bare `/metrics` counters. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for what's next and why it
was cut from this pass.
