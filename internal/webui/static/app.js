// Plain JS, no build step, no framework. This talks to the same /v1/*
// JSON API that the CLI and worker use.
//
// The stage is a farm. A job is a crop: it waits as a seed by the shed,
// gets planted when a worker leases it, grows while it runs, and is
// harvested, withers, or gets torn out of the ground depending on how it
// ends. A worker is a farmhand who walks out to whatever it is tending.
//
// None of that is decoration over a separate model. Every position and
// animation below is derived from the same job and worker records the
// CLI sees, so what you are watching really is the scheduler.

const POLL_INTERVAL_MS = 1500;
const THROUGHPUT_WINDOW_MS = 60000;

const els = {
  connStatus: document.getElementById("conn-status"),
  workersBody: document.querySelector("#workers-table tbody"),
  workersEmpty: document.getElementById("workers-empty"),
  workerCount: document.getElementById("worker-count"),
  jobsBody: document.querySelector("#jobs-table tbody"),
  jobsEmpty: document.getElementById("jobs-empty"),
  jobCount: document.getElementById("job-count"),
  form: document.getElementById("submit-form"),
  feedback: document.getElementById("submit-feedback"),
  addWorker: document.getElementById("add-worker"),
  clusterHint: document.getElementById("cluster-hint"),
};

let lastWorkers = [];
let lastJobs = [];

// --- small helpers -------------------------------------------------------

function timeAgo(iso) {
  if (!iso) return "-";
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 1000));
  if (seconds < 2) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.floor(minutes / 60)}h ago`;
}

function badge(status) {
  return `<span class="badge ${status}">${status}</span>`;
}

function truncate(s, n) {
  return s.length > n ? s.slice(0, n - 1) + "..." : s;
}

function shortId(id, n = 6) {
  const parts = id.split("_");
  return parts[parts.length - 1].slice(0, n);
}

function jobCommand(job) {
  return [job.command, ...(job.args || [])].join(" ");
}

function resLabel(res) {
  if (!res || (!res.cpu && !res.memory)) return "-";
  return `${res.cpu || 0}cpu/${res.memory || 0}mb`;
}

function isTerminal(status) {
  return status === "succeeded" || status === "failed" || status === "cancelled";
}

// --- auth ----------------------------------------------------------------
//
// The control plane may require a bearer token. It is kept in
// localStorage so a reload does not ask again. A 401 clears it and
// re-prompts, which also covers the token changing on the server.

const TOKEN_KEY = "dispatch.token";

function getToken() {
  return localStorage.getItem(TOKEN_KEY) || "";
}

function promptForToken(message) {
  const entered = window.prompt(message || "This control plane requires a token:");
  if (entered === null) return false;
  localStorage.setItem(TOKEN_KEY, entered.trim());
  return true;
}

function authFetch(path, opts = {}) {
  const token = getToken();
  const headers = new Headers(opts.headers || {});
  if (token) headers.set("Authorization", `Bearer ${token}`);
  return fetch(path, { ...opts, headers }).then((res) => {
    if (res.status !== 401) return res;
    localStorage.removeItem(TOKEN_KEY);
    const again = promptForToken(
      token ? "Token rejected. Enter the control plane token:" : "This control plane requires a token:"
    );
    if (!again) return res;
    const retryHeaders = new Headers(opts.headers || {});
    retryHeaders.set("Authorization", `Bearer ${getToken()}`);
    return fetch(path, { ...opts, headers: retryHeaders });
  });
}

async function fetchJSON(path) {
  const res = await authFetch(path);
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  if (res.status === 204) return [];
  return res.json();
}

async function cancelJob(id) {
  try {
    const res = await authFetch(`/v1/jobs/${id}`, { method: "DELETE" });
    if (!res.ok && res.status !== 409) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.error || `status ${res.status}`);
    }
    poll();
  } catch (err) {
    console.error("cancel failed:", err);
  }
}

// --- stats strip ---------------------------------------------------------

const prevStats = {};

function setStat(id, value) {
  const el = document.querySelector(`#${id} .stat-value`);
  if (!el) return;
  const str = String(value);
  if (el.textContent !== str) {
    el.textContent = str;
    if (prevStats[id] !== undefined) {
      el.classList.remove("bump");
      void el.offsetWidth;
      el.classList.add("bump");
    }
  }
  prevStats[id] = str;
}

function renderStats(workers, jobs) {
  let running = 0, pending = 0, done = 0, failed = 0;
  let leaseSum = 0, leaseCount = 0, recentDone = 0;
  const now = Date.now();
  for (const j of jobs) {
    if (j.status === "running") running++;
    else if (j.status === "pending") pending++;
    else if (j.status === "succeeded") done++;
    else if (j.status === "failed") failed++;
    if (j.started_at) {
      leaseSum += (new Date(j.started_at) - new Date(j.created_at)) / 1000;
      leaseCount++;
    }
    if (j.finished_at && now - new Date(j.finished_at).getTime() < THROUGHPUT_WINDOW_MS) {
      recentDone++;
    }
  }
  const alive = workers.filter((w) => w.status === "alive").length;

  setStat("stat-running", running);
  setStat("stat-queued", pending);
  setStat("stat-workers", `${alive}/${workers.length}`);
  setStat("stat-throughput", recentDone);
  setStat("stat-lease", leaseCount ? (leaseSum / leaseCount).toFixed(1) + "s" : "-");
  setStat("stat-done", done);
  setStat("stat-failed", failed);
}

// --- the farm ------------------------------------------------------------

const farm = {
  stage: document.getElementById("stage"),
  plots: document.getElementById("plots"),
  // Cottages get their own layer because layoutFarm rebuilds .plots
  // wholesale on every resize. Sharing that container meant a resize
  // silently deleted every cottage and left the reconciler holding
  // detached elements it kept happily styling.
  cottages: document.getElementById("cottages"),
  crops: document.getElementById("crops"),
  hands: document.getElementById("hands"),
  seeds: document.getElementById("seeds"),
  noWorkers: document.getElementById("no-workers"),

  plotPoints: [],           // every plot position on the field
  plotOf: new Map(),        // jobId -> plot index, held for the job's life
  takenPlots: new Set(),
  cropEls: new Map(),       // jobId -> element
  seedEls: new Map(),       // jobId -> element
  handEls: new Map(),       // workerId -> element
  cottageEls: new Map(),    // workerId -> element
  homes: new Map(),         // workerId -> {x, y}
  harvested: new Set(),     // jobIds whose ending animation already played
  prevJobs: new Map(),      // last poll's status, for detecting transitions
};

// layout recomputes plot and cottage positions for the current stage size.
// Plot positions are index-stable, so a crop keeps its patch of dirt for
// as long as it lives even when the window is resized around it.
function layoutFarm() {
  const rect = farm.stage.getBoundingClientRect();
  const w = rect.width, h = rect.height;

  const left = 138, right = w - 26;
  const top = 52, bottom = h - 78;
  const cellW = 62, cellH = 56;

  const cols = Math.max(1, Math.floor((right - left) / cellW));
  const rows = Math.max(1, Math.floor((bottom - top) / cellH));

  farm.plotPoints = [];
  for (let r = 0; r < rows; r++) {
    for (let c = 0; c < cols; c++) {
      farm.plotPoints.push({
        x: left + cellW / 2 + c * cellW,
        y: top + cellH / 2 + r * cellH,
      });
    }
  }

  farm.plots.innerHTML = farm.plotPoints
    .map((p) => `<div class="plot" style="left:${p.x}px;top:${p.y}px"></div>`)
    .join("");

  farm.homeRow = h - 34;
  farm.homeLeft = 150;
  farm.homeGap = Math.min(96, Math.max(70, (w - 190) / 6));
}

function plotPointFor(jobId) {
  if (farm.plotOf.has(jobId)) {
    const i = farm.plotOf.get(jobId);
    return farm.plotPoints[i] || farm.plotPoints[0] || { x: 200, y: 120 };
  }
  // Take the lowest free plot so the field fills in reading order rather
  // than scattering, which makes it much easier to see how much work is
  // actually in flight.
  let idx = farm.plotPoints.findIndex((_, i) => !farm.takenPlots.has(i));
  if (idx === -1) idx = farm.plotOf.size % Math.max(1, farm.plotPoints.length);
  farm.plotOf.set(jobId, idx);
  farm.takenPlots.add(idx);
  return farm.plotPoints[idx] || { x: 200, y: 120 };
}

function releasePlot(jobId) {
  if (!farm.plotOf.has(jobId)) return;
  farm.takenPlots.delete(farm.plotOf.get(jobId));
  farm.plotOf.delete(jobId);
}

function homeFor(workerId, index) {
  if (!farm.homes.has(workerId)) {
    farm.homes.set(workerId, index);
  }
  const i = farm.homes.get(workerId);
  return { x: farm.homeLeft + i * farm.homeGap, y: farm.homeRow };
}

// --- farmhands -----------------------------------------------------------

function makeHandEl(worker) {
  const el = document.createElement("div");
  el.className = "farmhand";
  // The inner markup is the original worker sprite, unchanged: every
  // existing animation keys off .worker, .working, .dead and .react-*,
  // so reusing the class means the sprites behave exactly as before.
  el.innerHTML = `
    <div class="worker">
      <div class="sprite">
        <div class="flash"></div>
        <div class="body">
          <span class="eye left"></span>
          <span class="eye right"></span>
        </div>
      </div>
      <div class="worker-name">${shortId(worker.id)}</div>
    </div>`;
  return el;
}

function moveHand(el, x, y) {
  const to = `translate(${x}px, ${y}px) translate(-50%, -100%)`;
  if (el.dataset.pos === to) return;
  // Lean into the walk only while actually travelling.
  el.classList.add("walking");
  clearTimeout(el._walkTimer);
  el._walkTimer = setTimeout(() => el.classList.remove("walking"), 1150);
  el.dataset.pos = to;
  el.style.transform = to;
}

function reconcileHands(workers, jobs) {
  const seen = new Set();

  workers.forEach((w, i) => {
    seen.add(w.id);
    let el = farm.handEls.get(w.id);
    if (!el) {
      el = makeHandEl(w);
      farm.hands.appendChild(el);
      farm.handEls.set(w.id, el);

      const cottage = document.createElement("div");
      cottage.className = "cottage";
      farm.cottages.appendChild(cottage);
      farm.cottageEls.set(w.id, cottage);

      // Drop a new hand at its cottage without animating in from 0,0.
      const home = homeFor(w.id, i);
      el.style.transition = "none";
      moveHand(el, home.x, home.y);
      void el.offsetWidth;
      el.style.transition = "";
    }

    const home = homeFor(w.id, i);
    const cottage = farm.cottageEls.get(w.id);
    if (cottage) {
      cottage.style.left = home.x + "px";
      cottage.style.top = home.y + "px";
      cottage.classList.toggle("dark", w.status !== "alive");
    }

    const inner = el.querySelector(".worker");
    inner.classList.toggle("dead", w.status === "dead");

    // Walk to whatever this hand is tending. With several jobs running on
    // one worker (bin-packing), it tends the one it picked up most
    // recently, which is what makes a busy worker visibly move around the
    // field rather than stand still.
    const mine = jobs
      .filter((j) => j.status === "running" && j.worker_id === w.id)
      .sort((a, b) => new Date(b.started_at || 0) - new Date(a.started_at || 0));

    inner.classList.toggle("working", mine.length > 0 && w.status === "alive");

    if (mine.length > 0 && w.status === "alive") {
      const p = plotPointFor(mine[0].id);
      moveHand(el, p.x, p.y + 20);
    } else {
      moveHand(el, home.x, home.y);
    }
  });

  for (const [id, el] of farm.handEls) {
    if (!seen.has(id)) {
      el.remove();
      farm.handEls.delete(id);
      const c = farm.cottageEls.get(id);
      if (c) c.remove();
      farm.cottageEls.delete(id);
      farm.homes.delete(id);
    }
  }
  farm.noWorkers.style.display = workers.length ? "none" : "block";
}

// --- crops and seeds -----------------------------------------------------

function makeCropEl(job) {
  const el = document.createElement("div");
  el.className = "crop";
  el.dataset.id = job.id;
  el.title = jobCommand(job);
  el.innerHTML = `
    <div class="stem"></div>
    <div class="leaf l"></div>
    <div class="leaf r"></div>
    <div class="head"></div>`;
  return el;
}

// growthStage maps how long a job has been running onto how tall its
// crop is. It is honest about progress in the only way possible for an
// arbitrary shell command: it shows elapsed time, not percent complete,
// because nothing here knows how far along the work actually is.
function growthStage(job) {
  if (!job.started_at) return 0;
  const secs = (Date.now() - new Date(job.started_at).getTime()) / 1000;
  if (secs < 1.5) return 0;
  if (secs < 4) return 1;
  if (secs < 9) return 2;
  return 3;
}

// cropClass composes a crop's classes so the modifiers (recurring,
// blocked) survive every state change. Building the class string in one
// place is what stops a fast recurring job from losing its sundial the
// instant it finishes, which is exactly when you are looking at it.
function cropClass(job, state) {
  let cls = `crop stage-${state === "growing" ? growthStage(job) : 3}`;
  if (state && state !== "growing") cls += " " + state;
  if (job.every > 0) cls += " recurring";
  if (state === "growing" && (job.depends_on || []).length) cls += " blocked";
  return cls;
}

function renderSeeds(pending) {
  const seen = new Set();
  pending.forEach((job, i) => {
    seen.add(job.id);
    let el = farm.seedEls.get(job.id);
    if (!el) {
      el = document.createElement("div");
      el.title = jobCommand(job);
      farm.seeds.appendChild(el);
      farm.seedEls.set(job.id, el);
    }
    // A job waiting on a prerequisite is still pending, so it lives in
    // the seed pile rather than the field. Marking it here rather than on
    // a crop is the difference between the state being visible and the
    // style being dead code: a blocked job never reaches the ground.
    const blocked = (job.depends_on || []).length > 0;
    el.className = "seed" + (blocked ? " blocked" : "") + (job.every > 0 ? " recurring" : "");
    el.title = jobCommand(job) + (blocked ? " (waiting on a prerequisite)" : "");
    // Seeds pile up beside the shed in a small heap.
    const col = i % 3, row = Math.floor(i / 3);
    el.style.left = 30 + col * 17 + "px";
    el.style.top = 148 + row * 15 + "px";
    el.style.display = row > 5 ? "none" : "";
    el.style.animationDelay = (i % 7) * 0.18 + "s";
  });
  for (const [id, el] of farm.seedEls) {
    if (!seen.has(id)) {
      el.classList.add("leaving");
      setTimeout(() => el.remove(), 300);
      farm.seedEls.delete(id);
    }
  }
}

function renderCrops(jobs) {
  const live = new Set();

  // Growing crops: anything a worker currently holds.
  for (const job of jobs) {
    if (job.status !== "running") continue;
    live.add(job.id);
    let el = farm.cropEls.get(job.id);
    if (!el) {
      el = makeCropEl(job);
      farm.crops.appendChild(el);
      farm.cropEls.set(job.id, el);
    }
    const p = plotPointFor(job.id);
    el.style.left = p.x + "px";
    el.style.top = p.y + "px";
    el.className = cropClass(job, "growing");
  }

  // Endings. Each plays once, then the crop is cleared from the field.
  for (const job of jobs) {
    if (!isTerminal(job.status) || farm.harvested.has(job.id)) continue;
    farm.harvested.add(job.id);

    let el = farm.cropEls.get(job.id);
    if (!el) {
      // A job that finished before we ever saw it grow still deserves its
      // moment, so plant one just to end it.
      el = makeCropEl(job);
      farm.crops.appendChild(el);
      farm.cropEls.set(job.id, el);
      const p = plotPointFor(job.id);
      el.style.left = p.x + "px";
      el.style.top = p.y + "px";
      el.className = cropClass(job, "growing");
      void el.offsetWidth;
    }
    live.add(job.id);

    if (job.status === "succeeded") {
      el.className = cropClass(job, "done");
      setTimeout(() => harvest(job.id), 900);
    } else if (job.status === "cancelled") {
      el.className = cropClass(job, "uprooted");
      setTimeout(() => harvest(job.id), 800);
    } else {
      el.className = cropClass(job, "failed");
      setTimeout(() => harvest(job.id), 1500);
    }
  }

  for (const [id] of farm.cropEls) {
    if (!live.has(id)) harvest(id);
  }
}

function harvest(jobId) {
  const el = farm.cropEls.get(jobId);
  if (!el) return;
  farm.cropEls.delete(jobId);
  releasePlot(jobId);
  el.style.opacity = "0";
  setTimeout(() => el.remove(), 350);
}

// fireEvents diffs this poll against the last and pokes the sprite that
// caused each change, so every job transition is visible on a farmhand.
function fireEvents(jobs) {
  for (const j of jobs) {
    const prev = farm.prevJobs.get(j.id);
    const was = prev ? prev.status : null;
    if (j.status === "running" && was !== "running") {
      react(j.worker_id, "react-grab");
    } else if (was && !isTerminal(was)) {
      if (j.status === "succeeded") react(j.worker_id, "react-cheer");
      else if (j.status === "failed") react(j.worker_id, "react-oops");
      else if (j.status === "cancelled") react(j.worker_id || prev.workerId, "react-drop");
      else if (was === "running" && j.status === "pending") react(prev.workerId, "react-oops");
    }
  }
  farm.prevJobs = new Map(jobs.map((j) => [j.id, { status: j.status, workerId: j.worker_id }]));
}

function react(workerId, cls) {
  const host = farm.handEls.get(workerId);
  if (!host) return;
  const el = host.querySelector(".worker");
  if (!el) return;
  el.classList.remove("react-grab", "react-cheer", "react-oops", "react-drop");
  void el.offsetWidth;
  el.classList.add(cls);
  setTimeout(() => el.classList.remove(cls), 700);
}

function renderFarm(workers, jobs) {
  if (!farm.plotPoints.length) layoutFarm();

  const pending = jobs
    .filter((j) => j.status === "pending")
    .sort((a, b) => (a.created_at || "").localeCompare(b.created_at || ""));

  renderSeeds(pending);
  renderCrops(jobs);
  reconcileHands(workers, jobs);

  els.clusterHint.textContent = pending.length ? `(${pending.length} waiting to plant)` : "";
}

// Click a crop to uproot it.
farm.crops.addEventListener("click", (e) => {
  const crop = e.target.closest(".crop");
  if (!crop || crop.classList.contains("uprooted")) return;
  cancelJob(crop.dataset.id);
});

// --- tables --------------------------------------------------------------

function renderWorkers(workers) {
  els.workerCount.textContent = `(${workers.length})`;
  els.workersEmpty.style.display = workers.length ? "none" : "block";
  els.workersBody.innerHTML = workers
    .map((w) => {
      const cap = w.capacity || { cpu: 0, memory: 0 };
      const avail = w.available || cap;
      const cpuUsed = cap.cpu - avail.cpu;
      const memUsed = cap.memory - avail.memory;
      const bar = (used, total, mem) => {
        const pct = total > 0 ? Math.min(100, (used / total) * 100) : 0;
        return `<span class="mini-bar"><span class="track"><span class="fill${mem ? " mem" : ""}" style="width:${pct}%"></span></span><span class="num">${used}/${total}</span></span>`;
      };
      return `
      <tr>
        <td title="${w.id}">${shortId(w.id, 10)}</td>
        <td>${badge(w.status)}</td>
        <td>${bar(cpuUsed, cap.cpu, false)} ${bar(memUsed, cap.memory, true)}</td>
        <td>${timeAgo(w.last_heartbeat)}</td>
      </tr>`;
    })
    .join("");
}

function renderJobs(jobs) {
  els.jobCount.textContent = `(${jobs.length})`;
  els.jobsEmpty.style.display = jobs.length ? "none" : "block";
  els.jobsBody.innerHTML = jobs
    .map((j) => {
      const full = jobCommand(j);
      const cancelable = j.status === "pending" || j.status === "running";
      const action = cancelable
        ? `<button class="btn-cancel" data-cancel="${j.id}">uproot</button>`
        : "";
      return `
      <tr>
        <td title="${j.id}">${truncate(j.id, 20)}</td>
        <td>${badge(j.status)}</td>
        <td title="${full}">${truncate(full, 34)}</td>
        <td>${resLabel(j.resources)}</td>
        <td>${j.priority}</td>
        <td>${j.retries}/${j.max_retries}</td>
        <td>${j.worker_id ? shortId(j.worker_id, 8) : "-"}</td>
        <td>${timeAgo(j.updated_at)}</td>
        <td>${action}</td>
      </tr>`;
    })
    .join("");
}

els.jobsBody.addEventListener("click", (e) => {
  const btn = e.target.closest("[data-cancel]");
  if (btn) cancelJob(btn.getAttribute("data-cancel"));
});

// --- poll loop -----------------------------------------------------------

async function poll() {
  try {
    const [workers, jobs] = await Promise.all([
      fetchJSON("/v1/workers"),
      fetchJSON("/v1/jobs"),
    ]);
    lastWorkers = workers || [];
    lastJobs = jobs || [];
    fireEvents(lastJobs); // before render, so reactions land on live sprites
    renderStats(lastWorkers, lastJobs);
    renderFarm(lastWorkers, lastJobs);
    renderWorkers(lastWorkers);
    renderJobs(lastJobs);
    els.connStatus.innerHTML = '<span class="dot"></span> connected';
    els.connStatus.className = "conn-status ok";
  } catch (err) {
    els.connStatus.innerHTML = '<span class="dot"></span> connection lost - retrying...';
    els.connStatus.className = "conn-status error";
    console.error("dashboard poll failed:", err);
  }
}

// Crops keep growing between polls, so tick the growth stages on their own
// timer. Without this a job that runs for eight seconds would visibly
// jump between sizes once per poll instead of growing.
setInterval(() => {
  for (const [id, el] of farm.cropEls) {
    const job = lastJobs.find((j) => j.id === id);
    if (!job || job.status !== "running") continue;
    const want = cropClass(job, "growing");
    if (el.className !== want) el.className = want;
  }
}, 500);

let resizeTimer = null;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    layoutFarm();
    renderFarm(lastWorkers, lastJobs);
  }, 150);
});

els.addWorker.addEventListener("click", async () => {
  const original = els.addWorker.textContent;
  els.addWorker.disabled = true;
  els.addWorker.textContent = "opening terminal...";
  try {
    const res = await authFetch("/v1/dev/spawn-worker", { method: "POST" });
    if (res.status === 404) {
      // Auth is on, which removes this route: spawning a process on the
      // host only makes sense when the browser and the control plane are
      // the same machine.
      els.addWorker.remove();
      els.clusterHint.textContent = "(start farmhands with: dispatch-worker -token ...)";
      return;
    }
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.error || `status ${res.status}`);
    }
    els.addWorker.textContent = "farmhand arriving...";
    setTimeout(poll, 3000);
    setTimeout(() => {
      els.addWorker.textContent = original;
      els.addWorker.disabled = false;
    }, 4000);
  } catch (err) {
    els.addWorker.textContent = original;
    els.addWorker.disabled = false;
    els.clusterHint.textContent = `(${err.message})`;
    console.error("spawn worker failed:", err);
  }
});

els.form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const data = new FormData(els.form);
  const command = data.get("command").trim();
  const argsRaw = data.get("args").trim();
  const args = argsRaw ? argsRaw.split(/\s+/) : [];
  const num = (name) => parseInt(data.get(name), 10) || 0;

  els.feedback.textContent = "sowing...";
  els.feedback.className = "feedback";

  try {
    const res = await authFetch("/v1/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        command,
        args,
        priority: num("priority"),
        max_retries: num("retries"),
        resources: { cpu: num("cpu"), memory: num("memory") },
      }),
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.error || `status ${res.status}`);
    }
    const job = await res.json();
    els.feedback.textContent = `submitted ${job.id}`;
    els.feedback.className = "feedback ok";
    els.form.reset();
    ["priority", "retries", "cpu", "memory"].forEach((id) => (document.getElementById(id).value = "0"));
    poll();
  } catch (err) {
    els.feedback.textContent = `error: ${err.message}`;
    els.feedback.className = "feedback error";
  }
});

layoutFarm();
poll();
setInterval(poll, POLL_INTERVAL_MS);
