// Plain JS, no build step, no framework. This talks to the same /v1/*
// JSON API that the CLI and worker use. The only dashboard-specific
// endpoint is POST /v1/dev/spawn-worker, a local convenience behind the
// "Add worker" button; every number and animation here is derived from
// the same job and worker data any client sees.

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

async function fetchJSON(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  if (res.status === 204) return [];
  return res.json();
}

async function cancelJob(id) {
  try {
    const res = await fetch(`/v1/jobs/${id}`, { method: "DELETE" });
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

// --- cluster scene -------------------------------------------------------
//
// The scene keeps long-lived DOM elements keyed by id and moves them as
// each poll's snapshot changes, rather than re-rendering from scratch.
// That is what lets a chip glide from the queue onto a worker, and lets a
// sprite react to an event exactly once: transitions are found by diffing
// this poll's jobs against the previous poll's.

const scene = {
  stage: document.getElementById("stage"),
  overlay: document.getElementById("chips-overlay"),
  queueBox: document.getElementById("queue-box"),
  workerRow: document.getElementById("worker-row"),
  noWorkers: document.getElementById("no-workers"),
  workerEls: new Map(), // workerId -> element
  chips: new Map(), // jobId -> { el, inner, status, placed }
  finished: new Set(), // jobIds whose finish animation already played
  prevJobs: new Map(), // jobId -> { status, workerId } from the last poll
};

function makeWorkerEl(worker) {
  const el = document.createElement("div");
  el.className = "worker";
  el.innerHTML = `
    <div class="sprite">
      <div class="flash"></div>
      <div class="body">
        <span class="eye left"></span>
        <span class="eye right"></span>
      </div>
    </div>
    <div class="worker-name">${shortId(worker.id)}</div>
    <div class="cap-bars">
      <div class="cap cpu"><span class="cap-fill"></span></div>
      <div class="cap mem"><span class="cap-fill"></span></div>
    </div>`;
  return el;
}

function react(workerId, cls) {
  const el = scene.workerEls.get(workerId);
  if (!el) return;
  el.classList.remove("react-grab", "react-cheer", "react-oops", "react-drop");
  void el.offsetWidth;
  el.classList.add(cls);
  setTimeout(() => el.classList.remove(cls), 650);
}

// fireEvents compares this poll to the last and triggers a sprite reaction
// for each meaningful transition, so every job event shows up on a sprite.
function fireEvents(jobs) {
  for (const j of jobs) {
    const prev = scene.prevJobs.get(j.id);
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
  scene.prevJobs = new Map(jobs.map((j) => [j.id, { status: j.status, workerId: j.worker_id }]));
}

function isTerminal(status) {
  return status === "succeeded" || status === "failed" || status === "cancelled";
}

function updateCapacityBars(el, worker) {
  const setBar = (sel, used, total) => {
    const fill = el.querySelector(sel);
    if (!fill) return;
    const pct = total > 0 ? Math.min(100, (used / total) * 100) : 0;
    fill.style.width = pct + "%";
    fill.classList.toggle("full", pct >= 99.5);
  };
  const cap = worker.capacity || { cpu: 0, memory: 0 };
  const avail = worker.available || cap;
  setBar(".cap.cpu .cap-fill", cap.cpu - avail.cpu, cap.cpu);
  setBar(".cap.mem .cap-fill", cap.memory - avail.memory, cap.memory);
}

function reconcileWorkers(workers) {
  const seen = new Set();
  for (const w of workers) {
    seen.add(w.id);
    let el = scene.workerEls.get(w.id);
    if (!el) {
      el = makeWorkerEl(w);
      scene.workerRow.appendChild(el);
      scene.workerEls.set(w.id, el);
    }
    el.classList.toggle("dead", w.status === "dead");
    el.classList.remove("working"); // re-derived from running jobs below
    updateCapacityBars(el, w);
  }
  for (const [id, el] of scene.workerEls) {
    if (!seen.has(id)) {
      el.remove();
      scene.workerEls.delete(id);
    }
  }
  scene.noWorkers.style.display = workers.length ? "none" : "block";
}

function ensureChip(job) {
  let rec = scene.chips.get(job.id);
  if (rec) return rec;
  const el = document.createElement("div");
  el.className = "chip spawning";
  el.dataset.id = job.id;
  const inner = document.createElement("span");
  inner.className = "chip-inner";
  inner.textContent = truncate(jobCommand(job), 16);
  el.appendChild(inner);
  scene.overlay.appendChild(el);
  rec = { el, inner, status: "", placed: false };
  scene.chips.set(job.id, rec);
  return rec;
}

function moveChip(rec, x, y) {
  if (!rec.placed) {
    rec.el.style.transition = "none";
    rec.el.style.transform = `translate(${x}px, ${y}px)`;
    void rec.el.offsetWidth;
    rec.el.style.transition = "";
    rec.el.classList.remove("spawning");
    rec.placed = true;
  } else {
    rec.el.style.transform = `translate(${x}px, ${y}px)`;
  }
}

function setChipState(rec, state) {
  if (rec.status === state) return;
  rec.el.classList.remove("pending", "running", "done", "failed", "cancelled");
  rec.el.classList.add(state);
  rec.status = state;
}

function playOnce(rec, cls) {
  rec.inner.classList.remove("pop", "shake");
  void rec.inner.offsetWidth;
  rec.inner.classList.add(cls);
}

function removeChip(id) {
  const rec = scene.chips.get(id);
  if (!rec) return;
  scene.chips.delete(id);
  rec.el.classList.add("leaving");
  setTimeout(() => rec.el.remove(), 300);
}

function workerChipTarget(workerId, stageRect) {
  const wEl = scene.workerEls.get(workerId);
  if (!wEl) return null;
  const wr = wEl.getBoundingClientRect();
  return {
    x: wr.left - stageRect.left + wr.width / 2 - 58,
    y: wr.top - stageRect.top - 20,
  };
}

function renderScene(workers, jobs) {
  reconcileWorkers(workers);

  const stageRect = scene.stage.getBoundingClientRect();
  const queueRect = scene.queueBox.getBoundingClientRect();
  const active = new Set();

  for (const j of jobs) {
    if (j.status === "running" && j.worker_id) {
      const wEl = scene.workerEls.get(j.worker_id);
      if (wEl) wEl.classList.add("working");
    }
  }

  const pending = jobs
    .filter((j) => j.status === "pending")
    .sort((a, b) => (a.created_at || "").localeCompare(b.created_at || ""));
  pending.forEach((j, i) => {
    active.add(j.id);
    const rec = ensureChip(j);
    setChipState(rec, "pending");
    const x = queueRect.left - stageRect.left + 10;
    const maxY = queueRect.bottom - stageRect.top - 28;
    const y = Math.min(queueRect.top - stageRect.top + 34 + i * 30, maxY);
    moveChip(rec, x, y);
  });

  for (const j of jobs) {
    if (j.status !== "running") continue;
    active.add(j.id);
    const rec = ensureChip(j);
    setChipState(rec, "running");
    const target = workerChipTarget(j.worker_id, stageRect);
    if (target) moveChip(rec, target.x, target.y);
  }

  for (const j of jobs) {
    if (!isTerminal(j.status)) continue;
    if (scene.finished.has(j.id)) continue;
    scene.finished.add(j.id);

    const rec = ensureChip(j);
    if (!rec.placed) {
      const target = workerChipTarget(j.worker_id, stageRect);
      if (target) moveChip(rec, target.x, target.y);
    }
    if (j.status === "succeeded") {
      setChipState(rec, "done");
      playOnce(rec, "pop");
    } else {
      setChipState(rec, j.status === "cancelled" ? "cancelled" : "failed");
      playOnce(rec, "shake");
    }
    active.add(j.id);
    setTimeout(() => removeChip(j.id), 1300);
  }

  for (const [id] of scene.chips) {
    if (!active.has(id)) removeChip(id);
  }

  els.clusterHint.textContent = pending.length ? `(${pending.length} queued)` : "";
}

// click a queued or running chip to cancel it
scene.overlay.addEventListener("click", (e) => {
  const chip = e.target.closest(".chip");
  if (!chip) return;
  const rec = scene.chips.get(chip.dataset.id);
  if (rec && (rec.status === "pending" || rec.status === "running")) {
    cancelJob(chip.dataset.id);
  }
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
        ? `<button class="btn-cancel" data-cancel="${j.id}">cancel</button>`
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
    fireEvents(lastJobs); // must run before renderScene so reactions land on sprites
    renderStats(lastWorkers, lastJobs);
    renderScene(lastWorkers, lastJobs);
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

let resizeTimer = null;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => renderScene(lastWorkers, lastJobs), 150);
});

els.addWorker.addEventListener("click", async () => {
  const original = els.addWorker.textContent;
  els.addWorker.disabled = true;
  els.addWorker.textContent = "opening terminal...";
  try {
    const res = await fetch("/v1/dev/spawn-worker", { method: "POST" });
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.error || `status ${res.status}`);
    }
    els.addWorker.textContent = "worker starting...";
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

  els.feedback.textContent = "submitting...";
  els.feedback.className = "feedback";

  try {
    const res = await fetch("/v1/jobs", {
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

poll();
setInterval(poll, POLL_INTERVAL_MS);
