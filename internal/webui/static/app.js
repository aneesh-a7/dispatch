// Plain JS, no build step, no framework. This talks to the same /v1/*
// JSON API that the CLI and worker use. There is no dashboard-specific
// backend code beyond serving these static files.

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

async function poll() {
  try {
    const [workers, jobs] = await Promise.all([
      fetchJSON("/v1/workers"),
      fetchJSON("/v1/jobs"),
    ]);
    renderWorkers(workers || []);
    renderJobs(jobs || []);
    els.connStatus.textContent = "connected";
    els.connStatus.className = "conn-status ok";
  } catch (err) {
    els.connStatus.textContent = "connection lost \u2014 retrying\u2026";
    els.connStatus.className = "conn-status error";
    console.error("dashboard poll failed:", err);
  }
}

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
