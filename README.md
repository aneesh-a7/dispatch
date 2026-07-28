# dispatch

I have, more than once, forgotten which of my machines was two hours into
a Monte Carlo run and only found out because the desktop's fan finally
spun back down. `dispatch` exists so that stops happening: submit a
shell command, it runs on whichever machine currently has a free worker,
and if something dies mid-job you don't have to reconstruct where it was
running from memory and vibes.

It's a small distributed job scheduler: a control plane that tracks jobs
durably, and worker agents that pull work and report back.

It is not Kubernetes. It's not trying to be. It's scoped to the parts of
"distributed job scheduling" that are actually hard and actually worth
building yourself: durable state, failure detection, at-least-once
execution. No auto-scaling groups, no YAML novels, no admission
controllers.

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
decisions and trade-offs, including a couple I went back and forth on
longer than I'd like to admit.

## Quickstart

Grab the archive for your platform from the
[latest release](https://github.com/aneesh-a7/dispatch/releases/latest),
unpack it, and put the three binaries somewhere on your `PATH`. There is
nothing to install and no runtime to babysit: they are statically linked
and depend on nothing but the OS.

```bash
# Terminal 1: start the control plane
dispatch-controlplane

# Terminal 2: start a worker
dispatch-worker

# Terminal 3: submit work
dispatchctl submit -- echo "hello dispatch"
dispatchctl submit -priority 5 -retries 2 -- python3 my_script.py
dispatchctl submit -cpu 2 -memory 1024 -- ./heavy_job

dispatchctl list
dispatchctl status <job-id>
dispatchctl cancel <job-id>
```

Building from source instead needs Go 1.22+ and no other dependencies:
substitute `go run ./cmd/controlplane`, `go run ./cmd/worker`, and
`go run ./cmd/dispatchctl` for the three commands above.

Or open **http://localhost:8080** in a browser once the control plane is
running, where the cluster is a farm.

Every job is a crop. It waits as a seed by the shed while it's queued,
gets planted the moment a worker leases it, and grows while it runs.
Succeed and it ripens and gets harvested; fail and it withers brown;
cancel it and a farmhand yanks it out of the ground and flings it off
the field, which is the most satisfying way I could think of to render
`DELETE /v1/jobs/{id}`. Workers are the farmhands, walking out to
whatever they're tending and back to their cottage when idle, so with a
few of them running you can watch the whole crew work the field at once.

A seed tied down with a dashed ring is waiting on a prerequisite. A
small sundial peg means the job recurs. A cottage with its lights out is
a worker that missed its heartbeats.

None of this is decoration over a separate toy model: every position and
animation is derived from the same `/v1/jobs` and `/v1/workers` records
the CLI reads, so what you're watching really is the scheduler. There's
a stats strip up top, a form to sow new jobs, and an "Add worker" button
that opens a terminal on the same machine with a farmhand ready to go.

Multiple workers can be started against the same control plane. Jobs are
leased to whichever worker has free capacity, one at a time per worker,
with no double-dispatch (see `Store.LeaseNextJob`).

## What a job can and cannot touch

Jobs run sandboxed by default. The part that matters most is the
environment: a worker started with `DISPATCH_TOKEN` in its environment
used to hand that token to every job it ran, so anyone who could submit
a job could also walk off with the cluster credential. Jobs now get an
allowlist of the variables a program actually needs and nothing else,
plus their own temporary working directory that's deleted when they
finish.

```
worker: sandbox environment allowlist, isolated working directory,
        PID/mount/IPC/UTS namespaces, cgroup v2 memory and CPU limits
```

The worker prints what's actually in force at startup, because a sandbox
whose limits you can't see is worse than none. What you get depends on
the platform:

- **Linux**: PID, mount, IPC, and UTS namespaces, plus cgroup v2 memory
  and CPU caps. Network is deliberately *not* isolated: most batch jobs
  exist to fetch or upload something. If you want that too, run the
  worker itself inside a network namespace.
- **Windows**: a Job Object with kill-on-close (so a job that spawns
  children can't leave orphans behind) and a memory ceiling.
- **Elsewhere**: the environment and working-directory isolation only,
  and it says so rather than implying limits it isn't enforcing.

Resource limits come from the job's own request, which closes a loop that
was previously open on one end: `-cpu 2 -memory 512` is both what the
scheduler bin-packs against *and* what the OS enforces, instead of the
number being an honour-system hint.

If a job legitimately needs a variable from the worker's environment,
forward it explicitly rather than opening the whole thing back up:

```bash
dispatch-worker -pass-env MY_API_ENDPOINT,HTTPS_PROXY
```

`-sandbox=false` restores the old inherit-everything behaviour if you
need it.

Worth being clear about the limits: this stops a job from casually
reading the worker's secrets, filling its disk with scratch files, or
eating all its memory. It is not a defence against someone actively
trying to break out, and it does not sandbox the *worker*, which still
runs whatever command it's given as your user.

## Watching a job while it runs

`dispatchctl logs -f` tails a job's output as it's produced, rather than
making you wait for the whole thing at the end:

```bash
dispatchctl logs -f <job-id>
```

Drop the `-f` for a one-shot read. Either way the same command works on a
job that's still running and one that finished last week: while a job
runs, output comes from a live buffer on the control plane; once it
finishes, from the durable record. Following across the moment a job ends
just works, and `-f` exits on its own when the job does.

The live buffer keeps the last 256KB per running job and is held in
memory only, never written to the write-ahead log. A job that prints
faster than you can read will have its earlier output scroll out of that
window, and `logs` tells you when that has happened. The complete output
is still recorded when the job finishes, so nothing is lost permanently.
Workers cap what they retain for that final report at 1MB, which is the
one place output really can be dropped for good, and it says so in the
output when it happens.

## Chaining jobs and repeating them

Two flags cover most of what turns a pile of one-off commands into
something resembling a workflow.

`-after` holds a job until the jobs it names have succeeded:

```bash
FETCH=$(dispatchctl submit -- ./fetch-data.sh | grep -o 'job_[^ ]*')
dispatchctl submit -after "$FETCH" -- ./transform.sh
```

A job can wait on several (`-after id1,id2`), and a waiting job doesn't
occupy a worker while it waits. If any prerequisite ends up failed or
cancelled, the dependent job is failed too, with an error saying why,
rather than sitting in the queue forever looking like it's just being
patient.

You can only depend on a job that already exists, which is a small
restriction with a nice consequence: it makes a dependency cycle
impossible to express in the first place, so there's no cycle detector to
get wrong.

`-every` re-runs a job forever:

```bash
dispatchctl submit -every 1h -- ./sync-backups.sh
```

The interval is measured from when the previous run *finishes*, not from
when it started, so a run that occasionally takes longer than its
interval falls behind rather than overlapping with itself. Each run is a
separate job with its own ID, output, and history, all sharing a
`series_id`. A failed run doesn't stop the schedule (the next one is
still queued, same as cron), but cancelling one does.

Since a run is only cancellable while it exists, the reliable way to stop
a recurring job is to cancel it while it's waiting between runs. Cancel
it mid-execution and you're racing the job: if it finishes before its
worker notices the cancellation, that run succeeded and the series
carries on.

## Getting told when a job finishes

The whole reason this project exists is to stop babysitting a terminal,
so I didn't want a system I still had to go check on like a crockpot.
Point the control plane at a webhook URL and every job that reaches a
terminal state (succeeded, failed, or cancelled) POSTs its result there
instead of waiting for you to come ask:

```bash
dispatch-controlplane -webhook-url https://hooks.slack.com/services/...
```

The payload carries the full job as JSON under `job`, plus a one-line
summary under both `text` (what Slack renders) and `content` (what
Discord renders), so a Slack or Discord webhook URL works as-is with no
glue code, while your own receiver can ignore the summary and read the
structured job.

A single job can override the default with `-webhook`, which is useful
when most work should notify one place and one long-running job should
notify somewhere else:

```bash
dispatchctl submit -webhook https://example.com/hook -- ./nightly-backup
```

Delivery is best-effort and happens in the background: it retries a few
times, then gives up and logs. A job's recorded status never depends on
whether its notification got through. A failure that still has retries
left does not notify, so you get one message per job outcome rather than
one per attempt.

## Running it where other people can reach it

By default there is no authentication, which is the right default for a
control plane bound to localhost or a home LAN you already trust. The
moment it is reachable by anyone else, set a shared token:

```bash
dispatch-controlplane -token "$(openssl rand -hex 32)"
dispatch-worker  -token <same-token>
dispatchctl      -token <same-token> list
```

Workers and the CLI both also read `$DISPATCH_TOKEN`, so you can export
it once instead of passing it every time. The dashboard prompts for the
token on first load and remembers it.

Every route requires the token except `GET /healthz`, which stays open so
uptime checks do not need credentials. Turning auth on also removes the
dashboard's "Add worker" button endpoint entirely: that one opens a
terminal on the control plane's own host, which only makes sense when
you are the person sitting at it.

For TLS, either pass a certificate directly:

```bash
dispatch-controlplane -tls-cert cert.pem -tls-key key.pem
```

or, for anything facing the public internet, terminate TLS at a reverse
proxy (Caddy and nginx both do automatic certificate renewal in about a
line of config) and point it at dispatch over localhost.

## Config files

Flags are fine right up until you're SSHed into the third machine at
midnight trying to remember whether this one takes `-memory` in MB or
you just made that convention up on the first machine and forgot. Any
flag can come from a JSON file instead, and an explicitly passed flag
always wins over the file:

```json
{
  "addr": ":8080",
  "data_dir": "/var/lib/dispatch",
  "token": "...",
  "webhook_url": "https://hooks.slack.com/services/...",
  "heartbeat_ttl": "30s"
}
```

```bash
dispatch-controlplane -config dispatch.json
```

## What happens when a worker dies mid-job

Kill a worker (`kill -9`) while it's running a job. The control plane's
reaper notices the missed heartbeats within `-heartbeat-ttl` (default
15s), marks the worker dead, and requeues its in-flight job (or fails it
permanently if the retry budget is exhausted). No job silently
disappears because its worker vanished, and nobody has to notice the
silence and go investigate. That used to be my job. Now it's the
reaper's.

## What happens when the control plane crashes

Kill `-9` the control plane process and restart it against the same
`-data-dir`. It replays its write-ahead log on startup and comes back
with every job and worker exactly where it left off. Nothing is lost
except whatever hadn't been fsync'd yet, which, by construction, is
nothing that was ever acknowledged to a caller.

## Resource-aware scheduling and cancellation

Jobs can declare a CPU/memory request and workers advertise a capacity.
Leasing is bin-packing: a job only goes to a worker with enough room for
it, and a worker's free capacity is tracked as its running jobs consume
and release it. A job left at the default (zero) fits anywhere and
consumes nothing, so this stays opt-in.

Workers measure their own capacity at startup, so `-cpu` and `-memory`
are only worth setting when you want to hold part of a machine back:

```
worker: detected 14 CPUs
worker: detected 32188 MB of memory
```

Anything you set explicitly is left alone, which is the point: telling a
16-core desktop to only advertise 4 cores because you're also using it is
a legitimate thing to want, and auto-detection shouldn't quietly override
you.

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
dispatchctl [-control-plane URL] [-token TOKEN] <command>

  submit [-priority N] [-retries N] [-cpu N] [-memory MB] [-webhook URL]
         [-after id1,id2] [-every 1h] <command> [args...]
  status <job-id>
  logs [-f] <job-id>
  cancel <job-id>
  list
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
  api/            HTTP handlers + bearer-token auth
  client/         typed HTTP client (shared by worker + CLI)
  notify/         job-finished webhook delivery
  config/         optional JSON config files
  sysinfo/        per-platform CPU/memory detection for worker capacity
  livelog/        in-memory output buffers for running jobs (not durable, on purpose)
  sandbox/        per-platform job isolation (env allowlist, namespaces, resource caps)
  idgen/          stdlib-only sortable ID generation
  webui/          embedded live dashboard (static HTML/CSS/JS, served by the control plane)
docs/
  ARCHITECTURE.md design decisions, trade-offs, what's deliberately out of scope
  ROADMAP.md      what's next and why it's next
```

## Status

Built and tested end to end: a durable control plane with WAL compaction,
pull-based workers, priority + resource-aware (bin-packing) leasing,
job dependencies, recurring jobs, live output streaming, sandboxed job
execution, retries,
dead-worker reaping, job cancellation, bearer-token auth, TLS,
job-finished webhooks, JSON config files, auto-detected worker capacity,
Prometheus metrics, a load-test tool, and a live sprite dashboard.

Still deliberately out of scope: sandboxed execution (jobs run as plain
subprocesses with full access to their worker's environment) and a
multi-node HA control plane. Both are real gaps, not oversights. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the reasoning on each and
[docs/ROADMAP.md](docs/ROADMAP.md) for what comes next.
