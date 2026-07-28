# Roadmap: making dispatch usable by someone other than its author

Everything in the "current state" section of `docs/ARCHITECTURE.md` was
built and tested by one person (me) across a laptop, a desktop, and
whatever else happened to be on the same trusted network at the time.
That covers the original problem: stop SSHing into whichever machine is
free. It does not cover the next one, which is what got me writing this
document instead of just moving on to something shinier: someone else
running dispatch across their own machines, possibly one of which is a
rented server reachable from the open internet, without babysitting a
terminal to find out when a job finishes.

This document lays out what was missing to close that gap, in the order
it was worth building, plus a longer backlog of ideas that are real but
not urgent. It complements `docs/ARCHITECTURE.md` rather than replacing
it: that file explains what exists and why; this one explains what's
next and why it's next.

## What actually stopped someone else from using this

Five concrete gaps, not vague ones:

1. **No auth.** Any client that can reach the control plane's port can
   submit jobs (arbitrary shell commands, executed on someone's worker),
   list everything, and cancel anything. Fine on localhost or a home LAN
   you fully trust. Not fine the moment the control plane is reachable
   from anywhere else, which is required for "a friend's machine" or "a
   rented VPS" to participate.
2. **No TLS.** Even once there's a token, plain HTTP means that token
   (and job output, which might contain anything the job printed) travels
   in the clear if the control plane is ever exposed past localhost.
3. **No prebuilt binaries.** The only way to run dispatch was `go run`
   from a source checkout. That's a nonstarter for someone who doesn't
   already have Go installed and doesn't want to.
4. **No way to find out a job finished without checking.** This was the
   one that mattered most, since the entire premise of this project is
   "stop babysitting terminals." The dashboard and `dispatchctl status`
   still required you to go look. Nothing pushed a "job 4821 finished"
   signal back to you. Until that existed, the core problem wasn't
   actually solved once you closed the laptop, which is a fairly
   embarrassing thing to realize about your own scheduler.
5. **No config file.** Flags are fine for one worker. They get brittle
   fast once there are several workers with different capacities and
   tokens to keep straight, especially across restarts.

## Build order

**Status: all five phases below are built, tested, and shipped.** What
follows is kept as the record of what was decided and why, since the
reasoning outlives the checkbox. The backlog after it is what remains.

### 1. Auth (done; do this first, everything else assumes it exists)

A shared bearer token, checked by middleware in front of the existing
`http.ServeMux`, opt-in so nothing breaks for the current single-user
localhost workflow:

- `-token` flag / `DISPATCH_TOKEN` env var on the control plane. If unset,
  behavior is unchanged (no auth, same as today).
- Every route except `GET /healthz` requires
  `Authorization: Bearer <token>` when a token is configured.
  `/healthz` stays open so a load balancer or uptime check doesn't need
  credentials.
- Compare with `subtle.ConstantTimeCompare`, not `==`. A string equality
  check leaks timing information proportional to how many leading bytes
  match, which is a real (if narrow) attack surface for anything guessing
  a shared secret over a network.
- `internal/client` gains a token field, sends the header on every
  request. Worker and `dispatchctl` both get a `-token`/`DISPATCH_TOKEN`
  flag that just gets threaded through to the client.
- The dashboard prompts for a token once (if the control plane returns
  401), stores it in `localStorage`, and attaches it to every fetch.
- `POST /v1/dev/spawn-worker` (the dashboard's "Add worker" button, which
  opens a local terminal running `go run ./cmd/worker`) is a local
  convenience that should never be reachable from anything but the
  machine running the control plane. Once a token is configured, disable
  this route outright rather than just gating it behind the token, since
  spawning a process on the host is a different risk class than the rest
  of the API.

Deliberately out of scope for this pass: per-worker or per-user tokens,
a token-issuing/rotation flow, roles or permissions. One shared secret
matches the actual shape of the problem (a small group of trusted
machines/people), and a token-management system is not worth building
before anyone has asked for it.

### 2. Distribution (done)

- A GitHub Actions workflow that builds `linux/darwin/windows` x
  `amd64/arm64` binaries on tag push and attaches them to a GitHub
  Release. Pure `go build` with `GOOS`/`GOARCH` set, no cross-compilation
  toolchain needed since this has zero cgo dependencies.
- Rewrote the README quickstart around downloading a release binary as
  the primary path, with `go run` kept as the "building from source"
  alternative underneath.
- No install script for now. (Notably: I'm not repeating the `curl | sh`
  pattern I've seen elsewhere for this project's own releases either. A
  `Copy-Item`/`tar -xzf` + "put it on your PATH" instruction is a couple
  more lines in the README and doesn't ask anyone to pipe a downloaded
  script straight into a shell.)

### 3. Webhooks on job completion (done)

The actual fix for gap #4, and the feature I was most looking forward to
building. When a job reaches a terminal state (succeeded, failed, or
cancelled), the control plane POSTs the job's JSON to a configured URL:

- `-webhook-url` flag / `DISPATCH_WEBHOOK_URL` env on the control plane
  sets a default applied to every job.
- Optional per-job `webhook_url` field on submit overrides the default,
  so one control plane can notify different places for different jobs.
- Fired from a goroutine, not inline in the completion handler: a slow or
  dead receiver must never stall `handleCompleteJob` or block the
  scheduler. A couple of retries with a short timeout, then give up and
  log; the job's status is still recorded correctly either way, the
  webhook is a notification, not part of the durability story.
- No signature/HMAC verification in the first pass (there's no secret
  exchange story yet without more auth machinery than is justified here);
  documented plainly that anyone pointing this at a public endpoint should
  treat the payload as untrusted-source-adjacent on the receiving end.
- This is deliberately just an HTTP POST rather than baked-in Slack/
  Discord/email integrations: a webhook URL is the universal primitive,
  and Discord/Slack both accept a plain POST to a webhook URL directly,
  so no per-service client code is needed to get real notifications
  working.

### 4. TLS (done)

- `-tls-cert` / `-tls-key` flags on the control plane, using stdlib
  `http.ListenAndServeTLS` (zero new dependencies).
- Documented two paths in the README: a self-signed cert for testing
  (`go run` snippet using `crypto/tls`'s cert generation, or plain
  `openssl req`), and, for anything actually exposed to the internet,
  terminating TLS at a reverse proxy (Caddy or nginx) in front of
  dispatch instead. Automatic certificate renewal is a solved problem at
  that layer; hand-rolling ACME inside the control plane would be a lot
  of code to duplicate something Caddy does in one line of config.

### 5. Config file (done)

- Optional `-config path.json` read once at startup. Plain JSON (stdlib
  `encoding/json`, already a dependency everywhere else in this repo), no
  new parser needed. Flags passed on the command line override values
  from the file, so scripts that already pass flags keep working
  unchanged.
- Mainly useful once someone's running more than one worker: capacity,
  token, and control-plane URL per machine, checked into a file instead
  of retyped into a command line each time, which is the kind of thing I
  only appreciated after doing it by hand one too many times.

## Round two: workflows (done)

With the deployment gaps closed, the next thing standing between dispatch
and actual daily use was that it could only run one command, once, on a
machine whose capacity I'd typed in by hand. Three additions, all shipped:

### Job dependencies (done)

`-after id1,id2` holds a job until those jobs succeed. A prerequisite
that fails or is cancelled fails its dependents rather than stranding
them in the queue.

The design note worth keeping: dependencies may only name jobs that
already exist, so a cycle is impossible to express and there's no cycle
detection to write. That fell out of the input validation I wanted
anyway, which is the good kind of accident. Resolution lives in the
reaper's sweep so that one implementation covers every way a prerequisite
can end.

### Recurring jobs (done)

`-every 1h` re-runs a job indefinitely, each run a separate job sharing a
`series_id`. The interval runs from the previous run finishing, which
makes overlapping runs structurally impossible instead of something to
handle. Whether a run is due is derived from the series' own durable
records rather than a timer, so restarts don't lose schedules and the
sweep can't double-queue.

Failing keeps the series going; cancelling ends it.

### Job sandboxing (done)

Environment allowlist and per-job working directory everywhere; Linux
namespaces plus cgroup v2 caps; Windows job objects with kill-on-close.
The worker logs what is actually in force, since a sandbox whose limits
you cannot see is worse than none.

The finding worth recording: the auth work had accidentally created the
hole this closes. A worker holding `DISPATCH_TOKEN` in its environment
was passing it to every job, so "may submit a job" silently meant "may
read the credential that controls submission." Auth and isolation are
less separable than this document originally assumed.

Job resource requests are now enforced, not just scheduled against,
which closes a loop that had been open at one end the whole time.

### Live output streaming (done)

`dispatchctl logs [-f]` tails a running job. Workers forward output as
it's produced instead of handing over one lump at the end, and the same
command reads a running job (live buffer) and a finished one (durable
record) without the caller caring which.

The design note worth keeping: the live buffer is deliberately not
durable. Output chunks in the WAL would mean an fsync per chunk of a
chatty job's stdout, which is the write pattern that path is worst at,
and the complete output is already recorded once on completion anyway.
This is the clearest example in the project of something that should not
be persisted, which felt worth being explicit about given how much of
the rest of this codebase is an argument for durability.

Bounded at 256KB per running job on the control plane and 1MB retained
per job on the worker, both of which announce themselves when they bite
rather than silently handing back a stream with a hole in it.

### Auto-detected worker capacity (done)

Workers measure their own CPU and memory at startup. Per-platform,
stdlib-only, behind build tags, with an honest "couldn't detect, using
the default" path for platforms without an implementation. Explicit
flags always win.

## Backlog: real ideas, not yet scheduled

Each of these is a legitimate next step after the above, listed with what
it is, why it'd matter, and the trade-off that's kept it out of the
near-term plan.

- **Escape-proof job isolation.** The sandbox that shipped covers
  environment, working directory, namespaces and resource caps, which is
  the accident-and-snooping tier. Going further means seccomp filters,
  user-namespace remapping, and a read-only root, and past a certain
  point it means admitting a container runtime is the right tool. Worth
  doing only if this ever needs to run genuinely untrusted code, which
  is a different project from the one I set out to build.
- **Multi-node HA control plane.** Removes the single point of failure.
  Needs a replicated log (Raft-style), leader election, and client
  redirect on failover. A large, well-scoped project on its own; not
  worth starting before the simpler gaps above are closed, since a
  single-node control plane with auth and TLS already covers "a small
  group running real workloads," just not "zero-downtime through a node
  failure."
- **Background service wrappers.** A systemd unit file, a launchd plist,
  and Windows service registration so the control plane and worker run
  as managed background services instead of foreground terminal
  processes that die when the terminal closes. Pure packaging, no code
  changes, but real friction removed for anyone running this
  unattended.
- **Cron-expression schedules.** `-every` takes a duration, which covers
  "run this periodically" but not "run this at 3am on weekdays." A real
  cron parser is a self-contained addition (the scheduling machinery it
  would feed already exists), but it's only worth it once I actually want
  a wall-clock schedule, and so far I haven't.
- **Stopping a recurring series outright.** Today you stop one by
  cancelling the run that's waiting between executions. That works, but
  cancelling mid-execution races the job, and there's no single "stop
  this series" verb. A `dispatchctl cancel -series <id>` would be
  unambiguous. Small, and worth doing the first time the current
  behaviour actually annoys me.
- **Per-client rate limiting / quotas.** Only starts to matter once one
  control plane is shared by more than a couple of trusted people. Not
  worth building until the auth work above surfaces an actual need for
  it.
- **Job templates.** Save a common command + resources + priority
  combination under a short name, submit by name instead of retyping the
  full command. A CLI/store convenience, not a scheduler change.

## What stays off the table

Same list as `docs/ARCHITECTURE.md`, restated here because it's easy to
lose track of once the project has more moving parts:

- No SQLite/bbolt/other embedded DB. The hand-rolled WAL (now with
  compaction) is the point of the project, not a placeholder for one.
- No third-party HTTP router or web framework.
- No UUID library.
- Nothing named after Kubernetes, container orchestration, or any
  specific employer.
- No dependency added to solve a problem the standard library already
  solves reasonably well (see the config file and TLS sections above:
  both stay stdlib-only on purpose).
