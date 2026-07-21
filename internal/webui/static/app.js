// Plain JS, no build step, no framework. This talks to the same /v1/*
// JSON API that the CLI and worker use. The only dashboard-specific
// endpoint is POST /v1/dev/spawn-worker, a local convenience behind the
// "Add worker" button; everything else here is built from the same job
// and worker data any client sees.

const POLL_INTERVAL_MS = 1500;

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

function timeAgo(iso) {
  if (!iso) return "\u2014";
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 1000));
  if (seconds < 2) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ago`;
}

function badge(status) {
  return `<span class="badge ${status}">${status}</span>`;
}

function truncate(s, n) {
  return s.length > n ? s.slice(0, n - 1) + "\u2026" : s;
}

async function fetchJSON(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  if (res.status === 204) return [];
  return res.json();
}

function renderWorkers(workers) {
  els.workerCount.textContent = `(${workers.length})`;
  els.workersEmpty.style.display = workers.length ? "none" : "block";
  els.workersBody.innerHTML = workers
    .map(
      (w) => `
      <tr>
        <td>${w.id}</td>
        <td>${w.address || "\u2014"}</td>
        <td>${badge(w.status)}</td>
        <td>${timeAgo(w.last_heartbeat)}</td>
      </tr>`
    )
    .join("");
}

function renderJobs(jobs) {
  els.jobCount.textContent = `(${jobs.length})`;
  els.jobsEmpty.style.display = jobs.length ? "none" : "block";
  els.jobsBody.innerHTML = jobs
    .map((j) => {
      const cmd = truncate([j.command, ...(j.args || [])].join(" "), 44);
      return `
      <tr>
        <td title="${j.id}">${truncate(j.id, 22)}</td>
        <td>${badge(j.status)}</td>
        <td title="${[j.command, ...(j.args || [])].join(" ")}">${cmd}</td>
        <td>${j.priority}</td>
        <td>${j.retries}/${j.max_retries}</td>
        <td>${j.worker_id ? truncate(j.worker_id, 16) : "\u2014"}</td>
        <td>${timeAgo(j.updated_at)}</td>
      </tr>`;
    })
    .join("");
}

// --- cluster scene -------------------------------------------------------
//
// The scene is a live picture of the same data the tables show. It has to
// animate transitions (a job flying from the queue onto a worker, popping
// green when it finishes), and the API is a plain snapshot poll with no
// event stream. So the scene keeps its own long-lived DOM elements keyed
// by id and moves them as each poll's snapshot changes, rather than
// re-rendering from scratch the way the tables do.

const scene = {
  stage: document.getElementById("stage"),
  overlay: document.getElementById("chips-overlay"),
  queueBox: document.getElementById("queue-box"),
  workerRow: document.getElementById("worker-row"),
  noWorkers: document.getElementById("no-workers"),
  workerEls: new Map(), // workerId -> element
  chips: new Map(),     // jobId -> { el, inner, status, placed }
  finished: new Set(),  // jobIds whose finish animation has already played
};

function shortId(id, n = 6) {
  const parts = id.split("_");
  return parts[parts.length - 1].slice(0, n);
}

function chipLabel(job) {
  return truncate([job.command, ...(job.args || [])].join(" "), 16);
}

function makeWorkerEl(worker) {
  const el = document.createElement("div");
  el.className = "worker";
  el.innerHTML = `
    <div class="sprite">
      <div class="sprite-body">
        <span class="eye left"></span>
        <span class="eye right"></span>
      </div>
      <div class="sprite-shadow"></div>
    </div>
    <div class="worker-label">${shortId(worker.id)}</div>`;
  return el;
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
  const inner = document.createElement("span");
  inner.className = "chip-inner";
  inner.textContent = chipLabel(job);
  el.appendChild(inner);
  scene.overlay.appendChild(el);
  rec = { el, inner, status: "", placed: false };
  scene.chips.set(job.id, rec);
  return rec;
}

function moveChip(rec, x, y) {
  if (!rec.placed) {
    // First placement: jump straight to the spot without animating (a chip
    // should not fly in from the corner), then fade in from .spawning.
    rec.el.style.transition = "none";
    rec.el.style.transform = `translate(${x}px, ${y}px)`;
    void rec.el.offsetWidth; // force reflow so the next change animates
    rec.el.style.transition = "";
    rec.el.classList.remove("spawning");
    rec.placed = true;
  } else {
    rec.el.style.transform = `translate(${x}px, ${y}px)`;
  }
}

function setChipState(rec, state) {
  if (rec.status === state) return;
  rec.el.classList.remove("pending", "running", "done", "failed");
  rec.el.classList.add(state);
  rec.status = state;
}

function playOnce(rec, cls) {
  rec.inner.classList.remove(cls);
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
    x: wr.left - stageRect.left + wr.width / 2 - 55,
    y: wr.top - stageRect.top - 16,
  };
}

function renderScene(workers, jobs) {
  reconcileWorkers(workers);

  const stageRect = scene.stage.getBoundingClientRect();
  const queueRect = scene.queueBox.getBoundingClientRect();
  const active = new Set();

  // Light up workers that are holding a running job.
  for (const j of jobs) {
    if (j.status === "running" && j.worker_id) {
      const wEl = scene.workerEls.get(j.worker_id);
      if (wEl) wEl.classList.add("working");
    }
  }

  // Pending jobs stack in the queue lane, oldest at the top.
  const pending = jobs
    .filter((j) => j.status === "pending")
    .sort((a, b) => (a.created_at || "").localeCompare(b.created_at || ""));
  pending.forEach((j, i) => {
    active.add(j.id);
    const rec = ensureChip(j);
    // A job bouncing back here from a worker (retry) flashes red on arrival.
    if (rec.status === "running") playOnce(rec, "shake");
    setChipState(rec, "pending");
    const x = queueRect.left - stageRect.left + 8;
    const maxY = queueRect.bottom - stageRect.top - 26;
    const y = Math.min(queueRect.top - stageRect.top + 30 + i * 28, maxY);
    moveChip(rec, x, y);
  });

  // Running jobs sit on top of their worker.
  for (const j of jobs) {
    if (j.status !== "running") continue;
    active.add(j.id);
    const rec = ensureChip(j);
    setChipState(rec, "running");
    const target = workerChipTarget(j.worker_id, stageRect);
    if (target) moveChip(rec, target.x, target.y);
  }

  // Finished jobs play their pop/shake once, then leave. The finished set
  // keeps them from reappearing on later polls (the API keeps returning
  // them indefinitely).
  for (const j of jobs) {
    if (j.status !== "succeeded" && j.status !== "failed") continue;
    if (scene.finished.has(j.id)) continue;
    scene.finished.add(j.id);

    const rec = ensureChip(j);
    if (!rec.placed) {
      // Never saw it run (a fast job finished within one poll): drop it on
      // its worker so it still gets a visible finish there.
      const target = workerChipTarget(j.worker_id, stageRect);
      if (target) moveChip(rec, target.x, target.y);
    }
    const ok = j.status === "succeeded";
    setChipState(rec, ok ? "done" : "failed");
    playOnce(rec, ok ? "pop" : "shake");
    active.add(j.id);
    setTimeout(() => removeChip(j.id), 1300);
  }

  // Drop any chip whose job is no longer in play (and not mid-finish).
  for (const [id] of scene.chips) {
    if (!active.has(id)) removeChip(id);
  }

  els.clusterHint.textContent = pending.length
    ? `(${pending.length} queued)`
    : "";
}

async function poll() {
  try {
    const [workers, jobs] = await Promise.all([
      fetchJSON("/v1/workers"),
      fetchJSON("/v1/jobs"),
    ]);
    lastWorkers = workers || [];
    lastJobs = jobs || [];
    renderWorkers(lastWorkers);
    renderJobs(lastJobs);
    renderScene(lastWorkers, lastJobs);
    els.connStatus.textContent = "connected";
    els.connStatus.className = "conn-status ok";
  } catch (err) {
    els.connStatus.textContent = "connection lost \u2014 retrying\u2026";
    els.connStatus.className = "conn-status error";
    console.error("dashboard poll failed:", err);
  }
}

// Kept so the scene can be re-laid-out on resize without waiting for the
// next poll (worker positions are read from the live DOM).
let lastWorkers = [];
let lastJobs = [];
let resizeTimer = null;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => renderScene(lastWorkers, lastJobs), 150);
});

els.addWorker.addEventListener("click", async () => {
  const original = els.addWorker.textContent;
  els.addWorker.disabled = true;
  els.addWorker.textContent = "opening terminal\u2026";
  try {
    const res = await fetch("/v1/dev/spawn-worker", { method: "POST" });
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.error || `status ${res.status}`);
    }
    // go run compiles before the worker registers, so give it a moment and
    // then refresh rather than waiting for the next scheduled tick.
    els.addWorker.textContent = "worker starting\u2026";
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
  const priority = parseInt(data.get("priority"), 10) || 0;
  const maxRetries = parseInt(data.get("retries"), 10) || 0;

  els.feedback.textContent = "submitting\u2026";
  els.feedback.className = "feedback";

  try {
    const res = await fetch("/v1/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        command,
        args,
        priority,
        max_retries: maxRetries,
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
    document.getElementById("priority").value = "0";
    document.getElementById("retries").value = "0";
    poll(); // refresh immediately rather than waiting for the next tick
  } catch (err) {
    els.feedback.textContent = `error: ${err.message}`;
    els.feedback.className = "feedback error";
  }
});

poll();
setInterval(poll, POLL_INTERVAL_MS);
