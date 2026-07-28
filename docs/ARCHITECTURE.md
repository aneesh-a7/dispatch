# Architecture & Design Decisions

This document exists so that every corner cut in this codebase is a
decision, not an accident. It's also, honestly, closer to a running
journal than a spec: most of these trade-offs got made at some point
between "that seems right" and "that's the only way I can make this
finish before I run out of evening." Anything not in "What's deliberately
out of scope" below is either handled or a known gap noted inline in the
code.

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

I could have pulled in SQLite and been done with this in an afternoon.
I didn't, on purpose. The point of this project, for me, was to actually
understand *why* durable systems are built the way they are, not to
prove I know how to import a database driver. A WAL is the mechanism
underneath Postgres, etcd's Raft log, and Kafka. Building the smallest
correct version of that mechanism taught me more than wrapping one ever
would have, and it's more fun to argue about at 1am, which counts for
something on a solo project.

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
of this system's control, since the job is an arbitrary shell command)
or a two-phase commit between worker and control plane, which trades
complexity for a guarantee that most job types don't actually need.
Commands run by this system should be written to be safely re-runnable
(idempotent) where that matters, the same expectation most real batch
schedulers place on their jobs.

### Single control-plane node, no HA, no consensus

There is exactly one control plane process and it is a single point of
failure. This is the biggest thing cut from "production-grade" to fit
the scope of this project, and it's the one I'm least casual about: it's
cut on purpose, not half-attempted, but it's still the first question
I'd expect a good reviewer to ask.

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
step and choosing not to build it yet is a different thing than not
knowing it's needed, and I want the difference on record.

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

The part I actually enjoyed figuring out is that a worker's *available*
capacity is not stored anywhere. It's derived on read as `Capacity`
minus the resources of the jobs currently running on it. The running
jobs are already durable in the WAL, so there is nothing extra to
persist, no separate counter that can drift, and no explicit "release on
completion" step I could forget to write: a job leaving the running
state frees its capacity automatically, because there was never a number
sitting around to forget to update. The cost is recomputing a small sum
on each lease and each worker read, which at this scale is free.

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

### Job dependencies

A job can name jobs that must succeed before it runs. Leasing skips
anything whose prerequisites aren't all `succeeded` yet, so a waiting job
costs nothing while it waits: it isn't holding a worker, it just isn't
eligible.

The part I'm happiest with is what the API rejects. A dependency must
name a job that already exists, which means a job can only ever depend on
its own past. That makes a cycle unrepresentable rather than merely
detected: there is no cycle checker in this codebase because there is
nowhere for a cycle to come from. I started out planning to write a
depth-first cycle detector and realized partway through that the
validation I already wanted for typos had made it dead code.

The other half is what happens when a prerequisite fails. Leasing alone
would leave the dependent job pending forever, which from the outside
looks exactly like "queued behind busy workers" and is miserable to
diagnose. So the reaper sweeps for jobs whose dependencies can no longer
all succeed and fails them with a reason attached. Putting it in the
reaper rather than in the completion handler means one implementation
covers every way a prerequisite can end, including the reaper failing it
after a worker died.

A missing dependency counts as failed rather than satisfied. Of the two
ways to be wrong about a prerequisite that vanished, running the job
anyway is the one that can do damage.

### Recurring jobs

`Every` on a job makes it a series. Once no run of that series is queued
or in flight, the reaper's sweep queues the next one with a `NotBefore`
of the last run's finish plus the interval, and leasing refuses to touch
a job before its `NotBefore`.

Two decisions worth spelling out:

**The interval runs from finish, not from start.** This is the difference
between "every hour" and "an hour after each run ends," and picking the
second one deletes an entire category of problem: a run can never overlap
its own successor, so a job that usually takes 5 minutes and occasionally
takes 90 cannot pile up copies of itself while you aren't looking. It
falls behind schedule instead, which is the failure mode you can actually
notice and reason about. Wall-clock scheduling is the more familiar
model, but it comes with "what do I do if the previous run is still
going?" and every answer to that is worse than not having the question.

**Whether the next run is due is derived from the series, not from a
timer.** There's no goroutine per recurring job and nothing in memory
that a restart could lose. The sweep reads the same durable job records
everything else reads, which means a control plane that was down for an
hour comes back and picks the series up correctly instead of dropping it.
It also makes the sweep naturally idempotent: the run it just queued is
itself the evidence that the series is active, so running the sweep twice
cannot double-queue.

Failing doesn't stop a series (cron doesn't stop either, and a backup job
that silently gave up the first time it errored would be worse than no
backup job). Cancelling does. "Stop this run" and "stop this recurring
job" are genuinely ambiguous, and between the two, a cancel that keeps
firing every hour is a much worse surprise than one that stops too much.

### Job sandboxing

Jobs used to be plain `os/exec` children: full inherited environment, the
worker's own working directory, no resource ceiling. Fine when every job
is a script you wrote, which is how this started, and steadily less fine
once anyone else can submit work.

The environment was the genuinely sharp edge, and I did not see it until
I went looking. A worker started with `DISPATCH_TOKEN` in its environment
passed that token to every job it ran. So the auth work, which was
supposed to control *who may submit a job*, had quietly made every job a
way to read the credential that controls submission. Auth and isolation
turn out not to be separable in the way the roadmap implied: getting one
right without the other left a hole shaped exactly like the thing I
thought I had closed.

An allowlist fixes it, and it is an allowlist rather than a denylist on
purpose. A denylist has to anticipate every secret anyone might ever put
in a worker's environment, and it only takes one nobody thought of.

The rest is per-platform, and the package reports what is actually in
force rather than implying a uniform guarantee:

- **Linux**: PID, mount, IPC and UTS namespaces via `SysProcAttr`, plus
  cgroup v2 memory and CPU limits written as plain files. Network is
  deliberately not isolated: most batch jobs exist to move data
  somewhere, and a namespace with no route out breaks the common case to
  defend against the rare one. Someone who wants that can run the worker
  inside a network namespace, which composes better than deciding it
  here.
- **Windows**: a Job Object providing a memory ceiling and, more usefully,
  kill-on-close, which is what makes a job's whole process tree actually
  disappear instead of leaving grandchildren behind.
- **Anything else**: the portable protections, and an honest log line
  saying that is all.

Limits are taken from the job's declared `Resources`, which closes a loop
that was previously open at one end. The same numbers the scheduler
bin-packs against are now the numbers the kernel enforces, so a job
asking for 512MB is both *scheduled* as needing 512MB and *stopped* at
512MB. Before this, the request was an honour-system hint that the
scheduler trusted completely.

Failures here are deliberately soft. If a cgroup cannot be created or a
job object cannot be assigned, the job runs with less isolation and the
worker says so. Refusing to run work because a limit could not be applied
would be its own kind of outage, and the honest log line is worth more
than the strictness.

What this is not: a defence against someone actively trying to escape.
There is no seccomp filter, no user namespace remapping, no read-only
root. It stops a job from casually reading the worker's secrets, filling
its disk, or eating its memory. Anything stronger means a container
runtime, which is a dependency this project does not want.

### Live output streaming

Workers used to call `cmd.CombinedOutput()`, which meant a job's output
existed only as a lump handed over at the end. A job that had been
running for twenty minutes could tell you nothing about what it was
doing, which for the long batch scripts this thing exists to run is most
of the time you actually care.

Now the worker wires both streams into one writer that keeps a copy for
the final report and forwards new bytes to the control plane every
700ms. `dispatchctl logs -f` polls for them.

Three decisions carried this:

**The live buffer is the one piece of control plane state that is
deliberately not durable.** Putting output chunks in the WAL would mean
an fsync per chunk of a chatty job's stdout, which is precisely the write
pattern the WAL is worst at, and it would slow down the job submissions
that genuinely need durability. It would also be redundant, because the
complete output is already written once in the completion report. Losing
the live tail to a restart costs you a progress bar, not a record. This
is the clearest case in the project of something that should *not* go
through the durable path, and it was worth being explicit about that
rather than reflexively persisting everything.

**Flushing is on a timer, not per write.** A program printing a line at a
time would otherwise generate one HTTP request per line. And the sink's
`Write` never blocks on the network: the job's own output goes through
it, so a slow control plane would otherwise apply backpressure to the
job itself, letting the monitoring system slow down the thing it is
monitoring.

**Memory is bounded in both directions, and says so when it bites.** The
control plane keeps the last 256KB per running job, dropping oldest
first, because someone watching a long job almost always wants to know
what it is doing now. A reader who has fallen behind that window is told
`truncated` rather than being quietly handed a stream with a hole in it.
The worker separately caps what it retains for the final report at 1MB
and prefixes a note when it drops anything, because that copy is the one
that is gone for good.

Buffers are freed on completion, but that alone would leak: a job can
also stop running because the reaper decided its worker died, a path
that never touches the output code. Rather than chase every exit, the
reaper reconciles buffers against the set of jobs actually running on
each sweep. A buffer can then survive at most one sweep interval no
matter how its job ended, including ways I have not thought of yet.

### Worker capacity detection

Workers measure their own CPU and memory at startup instead of defaulting
to numbers I picked. The scheduler bin-packs against whatever a worker
claims, so a wrong number is quietly expensive in both directions: too
high and jobs get packed onto a machine that can't hold them, too low and
the machine sits half-idle while work queues. Neither announces itself.

`runtime.NumCPU` handles cores everywhere and already respects affinity
and container limits. Memory is per-platform (`/proc/meminfo`,
`GlobalMemoryStatusEx`, `sysctl hw.memsize`) behind build tags, all
stdlib so the release builds keep cross-compiling from one runner with
nothing installed. A platform without an implementation returns "don't
know" and the worker falls back to a documented default and says so in
the log, rather than inventing a number.

Explicitly-set values are never overridden. Capping a machine you're also
using yourself is a real thing to want, and auto-detection that argues
with you about it is worse than none.

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

I put this off for a while, partly because "the log grows forever" is a
fun thing to write in a known-limitations list and partly because it
felt like the kind of problem that only becomes real after weeks of
uptime, which is exactly the situation where you'd rather it already be
solved than find out the hard way.

### Auth

A single shared bearer token, checked by middleware in front of the mux.
Until this existed, the only adversary this thing had to worry about was
me, forgetting I'd left port 8080 open to a network I didn't fully
trust. Auth is opt-in: with no token configured nothing changes, which
keeps the original localhost setup a one-command affair. One shared
secret (rather than per-user credentials, roles, or a token-issuing
flow) matches the actual shape of the problem, a small set of machines
and people who already trust each other, and none of that machinery is
worth building before someone has actually asked for it.

Two details that matter more than they look:

- The comparison uses `subtle.ConstantTimeCompare`. A plain `==` returns
  as soon as it hits a differing byte, so how long a rejection takes
  leaks how many leading bytes the guess got right, which is enough to
  recover a secret one byte at a time given enough tries.
- `GET /healthz` stays open. Everything else requires the token. An
  uptime check or load balancer should not need the cluster's credentials
  to ask "is this process alive?", and the handler reports nothing else.

Configuring a token also removes `POST /v1/dev/spawn-worker` from the mux
entirely rather than gating it. That route opens a terminal on the
control plane's own host, which only makes sense when the browser and the
control plane are the same machine. Deleting it means a leaked token is
not also a shell on the box.

### Job-finished webhooks

Durable scheduling meant a job survived a crash, but you still had to go
look at a dashboard to find out it had finished, which is most of the
original problem I set out to fix in the first place. When a job reaches
a terminal state the control plane POSTs it to a configured URL (global
default, overridable per job).

Delivery runs in a goroutine and is best-effort: a slow, down, or hostile
receiver must never stall the handler that called it, which is completing
a job or reaping a dead worker. It retries a few times and gives up. The
job's real status is already durable in the WAL before any of this, so a
dropped notification costs a message, not correctness.

Only terminal transitions notify. A failure with retry budget left goes
back to pending, so you get one message per job outcome rather than one
per attempt. All three places a job can end (the completion handler, the
pending-cancel path, and the reaper failing an orphan permanently) call
the same notifier.

The payload carries the full job plus the same one-line summary under
both `text` and `content`. Slack renders `text`, Discord renders
`content`, and both ignore keys they do not know, so one payload posts
usefully to either without dispatch shipping per-service client code,
while a custom receiver ignores the summary and reads `job`.

## What's deliberately out of scope

- **Escape-proof isolation.** The sandbox above raises the floor a long
  way, but there is no seccomp filter, no user-namespace remapping, and
  no read-only root filesystem. It is protection against accidents and
  casual snooping, not against someone determined to break out. That
  needs a container runtime, and a container runtime is a dependency
  this project has decided not to take.
- **Per-user credentials, rotation, and roles.** One shared token is the
  whole auth story. Anyone holding it can do anything except spawn a
  worker process on the host.
- **Webhook signing.** Receivers cannot currently verify a payload came
  from your control plane, so treat the data as untrusted on the
  receiving end.
- **Multi-node HA control plane.** Still a single point of failure. See
  the note above on what full HA would require.
