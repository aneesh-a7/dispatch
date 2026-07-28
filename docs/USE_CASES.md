# What people actually use this for

Every example here runs on v0.2.0 as written. They are ordered roughly by
how much of the system they lean on, starting with the one this project
was built for.

A note on what dispatch is not, since it saves reading further if you
need one of these: it is not a CI system (no repo checkouts, no build
caching), not a data-pipeline framework (no dataframes, no schemas), and
not a container orchestrator (jobs are processes, not images). It runs
shell commands on machines you own, durably, and tells you what happened.

## 1. Long batch runs across whatever machine is free

The original problem. You have a laptop, a desktop, and maybe a spare
box, and you keep SSHing into whichever one is idle to kick off a two
hour job, then forgetting which one you picked.

```bash
# On the machine that will keep track
dispatch-controlplane -webhook-url "$SLACK_WEBHOOK"

# On each machine that will do work
dispatch-worker
```

Workers measure their own CPU and memory, so you do not have to tell the
desktop it is bigger than the laptop. Then submit from anywhere:

```bash
dispatchctl submit -cpu 4 -memory 8192 -- python3 simulate.py --trials 1e7
dispatchctl submit -cpu 1 -memory 512  -- python3 fetch_prices.py
```

The big job only lands on a machine with room for it. The small one fills
in around it. You get a Slack message when each finishes, so you can
close the laptop.

**Leans on:** resource-aware scheduling, auto-detected capacity, webhooks.

## 2. A nightly pipeline that is three jobs, not one script

The usual way to run "fetch, then transform, then publish" is a single
bash script with `set -e`, which works until step two fails at 3am and
you have no idea whether step one had finished.

```bash
FETCH=$(dispatchctl submit -q -- ./fetch.sh)
XFORM=$(dispatchctl submit -q -after "$FETCH" -- ./transform.sh)
dispatchctl submit -after "$XFORM" -retries 2 -- ./publish.sh
```

`-q` prints just the job ID, which is what you want when feeding one
submit into the next one's `-after`.

Each step is a separate job with its own output, timing and exit status.
If `transform` fails, `publish` is failed too with a reason attached
rather than sitting queued forever looking like it is merely waiting. And
because retries are per job, only `publish` retries, not the expensive
fetch that already succeeded.

To make it nightly, put the whole thing behind one recurring job:

```bash
dispatchctl submit -every 24h -- ./nightly.sh
```

Recurrence is measured from when the previous run *finishes*, so a night
where the pipeline takes six hours does not start the next one on top of
itself.

**Leans on:** dependencies, per-job retries, recurring jobs.

## 3. Replacing a scatter of crontabs

Cron is fine until it is on four machines and you cannot remember which.
Worse, cron tells you nothing: no history, no output unless you set up
mail, and no notion of whether last night's run actually happened.

```bash
dispatchctl submit -every 6h  -- /usr/local/bin/backup-photos.sh
dispatchctl submit -every 1h  -- /usr/local/bin/check-certs.sh
dispatchctl submit -every 30m -- /usr/local/bin/sync-notes.sh
```

Every run is a job with recorded output and duration, `dispatchctl list`
shows the lot, and a failure notifies you instead of being discovered in
March. The schedule survives a control plane restart because it lives in
the write-ahead log, not in a timer in memory.

The thing cron genuinely does better: wall-clock schedules. `-every`
takes a duration, so "3am on weekdays" is not expressible yet.

**Leans on:** recurring jobs, durable state, webhooks.

## 4. Watching a long job instead of waiting for it

A four hour transcode or training run is unbearable if the only two
states are "running" and "done."

```bash
JOB=$(dispatchctl submit -cpu 8 -- ffmpeg -i input.mkv -c:v libx265 out.mkv | grep -o 'job_[^ ]*')
dispatchctl logs -f "$JOB"
```

Output streams as it is produced and `-f` exits by itself when the job
does. If it is obviously going wrong twenty minutes in:

```bash
dispatchctl cancel "$JOB"
```

which kills the process rather than politely asking, and takes about two
seconds even when the command is a shell wrapping something else.

**Leans on:** live output streaming, cancellation.

## 5. A queue you can walk away from

Rendering three hundred frames, converting a photo library, running the
same model over a directory of inputs. The work is embarrassingly
parallel and the only real requirement is that nothing is silently
dropped.

```bash
for f in frames/*.blend; do
  dispatchctl submit -cpu 2 -memory 4096 -retries 1 -- blender -b "$f" -o //out/ -f 1
done
```

Submit them all, and workers pull as they free up. If a machine dies
mid-frame, the reaper notices the missed heartbeats and requeues that
frame on another worker, once, because that is the retry budget you set.
Nothing is lost because a machine went to sleep.

**Leans on:** pull-based leasing, dead-worker reaping, retries.

## 6. Letting other people submit work

The moment the control plane is reachable by anyone but you, two things
change.

```bash
dispatch-controlplane -token "$(openssl rand -hex 32)" \
                      -tls-cert cert.pem -tls-key key.pem
dispatch-worker -token "$DISPATCH_TOKEN"
```

Auth means only people holding the token can submit. Sandboxing, which
is on by default, means what they submit cannot read the worker's
environment, litter in its working directory, or exceed the memory it
asked for. Those are two different problems and you need both: a token
controls who may submit a job, not what a job does once it is running.

If a job legitimately needs a variable, forward it explicitly:

```bash
dispatch-worker -pass-env HTTPS_PROXY,MY_API_ENDPOINT
```

Be clear-eyed about the level of protection. This stops accidents and
casual snooping. It is not a defence against someone actively trying to
escape, and if you are running genuinely hostile code you want a
container runtime, not this.

**Leans on:** bearer-token auth, TLS, sandboxing.

## 7. Finding out how fast your own hardware actually is

Less a use case than a thing worth doing once.

```bash
go run ./cmd/loadtest -n 500
```

Submits 500 trivial jobs and reports throughput plus p50/p95/p99 for both
queue wait and execution. Useful for sizing `-poll-interval` sensibly, and
for seeing what the per-write `fsync` in the WAL actually costs you, which
is the main deliberate trade in the whole design.

**Leans on:** metrics, the load generator.

## Where it stops

Worth knowing before you build something on it:

- **One control plane.** It is durable and restarts cleanly, but while it
  is down nothing gets scheduled. No HA.
- **At-least-once, not exactly-once.** A job can run twice if a worker
  dies between finishing and reporting. Write jobs to be re-runnable
  where that matters.
- **No wall-clock schedules.** `-every 1h`, not "every Tuesday".
- **No artifact handling.** Jobs read and write their own files. Dispatch
  moves no data, only commands and their output.
