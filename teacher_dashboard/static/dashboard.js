const state = {
  bootstrap: null,
  learners: [],
  atRiskRows: [],
  campaignJobs: [],
  syncLogRows: [],
  detailWorkRows: [],
  detailStudentId: 0,
  reports: [],
  sortState: {
    learners: { key: "full_name", dir: "asc" },
    detailWork: { key: "due_date", dir: "asc" },
    atRisk: { key: "full_name", dir: "asc" },
    campaignJobs: { key: "run_at", dir: "desc" },
    syncLog: { key: "synced_at", dir: "desc" },
  },
};

function el(id) {
  return document.getElementById(id);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function fmtNum(value) {
  return Number(value || 0).toLocaleString();
}

function fmtPct(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return "0%";
  return `${num.toFixed(2)}%`;
}

function fmtDate(value) {
  if (!value) return "-";
  const text = String(value).replace("T", " ");
  return text.slice(0, 19);
}

function statusBadge(status) {
  const s = (status || "").toLowerCase();
  if (s === "submitted" || s === "graded" || s === "late") {
    return "success";
  }
  if (s === "missing") {
    return "danger";
  }
  if (s.includes("pending") || s.includes("running")) {
    return "pending";
  }
  if (s.includes("failed")) {
    return "danger";
  }
  return "warn";
}

function showToast(message, type = "ok") {
  const stack = el("toast-stack");
  if (!stack) return;

  const toast = document.createElement("div");
  toast.className = `toast ${type === "error" ? "error" : ""}`;
  toast.textContent = message;
  stack.appendChild(toast);

  setTimeout(() => {
    toast.remove();
  }, 3200);
}

async function apiFetch(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });

  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    const payload = await response.json();
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.error || `Request failed (${response.status})`);
    }
    return payload.data;
  }

  if (!response.ok) {
    throw new Error(`Request failed (${response.status})`);
  }

  return response;
}

function activeCourseId() {
  return Number(el("global-course").value || 0);
}

function activeThreshold() {
  return Number(el("global-threshold").value || 3);
}

function activeSearch() {
  return String(el("global-search").value || "").trim();
}

function downloadCsv(url) {
  const a = document.createElement("a");
  a.href = url;
  a.target = "_blank";
  document.body.appendChild(a);
  a.click();
  a.remove();
}

function setCompletionRing(percent) {
  const ring = el("completion-ring");
  const text = el("completion-text");
  const clamped = Math.max(0, Math.min(100, Number(percent || 0)));
  ring.style.setProperty("--completion-angle", `${(clamped / 100) * 360}deg`);
  text.textContent = `${clamped.toFixed(1)}%`;
}

function setupTabs() {
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");

      const tab = btn.dataset.tab;
      document.querySelectorAll(".tab-panel").forEach((panel) => panel.classList.remove("active"));
      el(`tab-${tab}`).classList.add("active");
    });
  });
}

function setupHeaderActions() {
  el("btn-refresh-all").addEventListener("click", refreshAll);
  el("btn-export-learners").addEventListener("click", () => {
    const q = new URLSearchParams({
      course_id: String(activeCourseId()),
      search: activeSearch(),
    });
    downloadCsv(`/api/export/students.csv?${q.toString()}`);
  });

  el("global-course").addEventListener("change", refreshAll);
  el("global-threshold").addEventListener("change", () => {
    refreshOverview();
    refreshAtRisk();
  });

  el("global-search").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      refreshLearners();
    }
  });
}

function setupSectionActions() {
  el("btn-learners-refresh").addEventListener("click", refreshLearners);
  el("btn-learners-export").addEventListener("click", () => {
    const q = new URLSearchParams({
      course_id: String(activeCourseId()),
      search: activeSearch(),
    });
    downloadCsv(`/api/export/students.csv?${q.toString()}`);
  });

  el("btn-reports-refresh").addEventListener("click", refreshReports);
  el("btn-reports-export").addEventListener("click", () => {
    const q = new URLSearchParams({ course_id: String(activeCourseId()) });
    downloadCsv(`/api/export/reports.csv?${q.toString()}`);
  });

  el("btn-atrisk-refresh").addEventListener("click", refreshAtRisk);

  el("btn-campaign-refresh").addEventListener("click", refreshCampaignJobs);
  el("btn-campaign-run-due").addEventListener("click", () => runDueCampaignJobs(false));
  el("btn-campaign-create").addEventListener("click", createCampaign);

  el("btn-maint-backup").addEventListener("click", createBackup);
  el("btn-maint-rebuild").addEventListener("click", rebuildSummaries);
  el("btn-maint-schema").addEventListener("click", initSchema);
  el("btn-sync-refresh").addEventListener("click", refreshSyncLog);
  el("btn-sync-classroom")?.addEventListener("click", startClassroomSync);
  el("sync-period")?.addEventListener("change", toggleSyncCustomRange);
  toggleSyncCustomRange();

  document.querySelector('[data-action="refresh-overview"]').addEventListener("click", refreshOverview);

  el("detail-close").addEventListener("click", () => {
    el("learner-detail").classList.add("hidden");
  });
}

function setupTableSorting() {
  const bindings = [
    { headId: "learners-head", sortKey: "learners", render: renderLearnersTable },
    { headId: "detail-work-head", sortKey: "detailWork", render: renderDetailWorkTable },
    { headId: "atrisk-head", sortKey: "atRisk", render: renderAtRiskTable },
    { headId: "campaign-jobs-head", sortKey: "campaignJobs", render: renderCampaignJobsTable },
    { headId: "sync-log-head", sortKey: "syncLog", render: renderSyncLogTable },
  ];

  bindings.forEach(({ headId, sortKey, render }) => {
    const head = el(headId);
    if (!head) return;

    head.addEventListener("click", (event) => {
      const th = event.target.closest("th.sortable[data-sort-key]");
      if (!th) return;

      const key = th.dataset.sortKey;
      if (!key) return;

      const current = state.sortState[sortKey];
      if (!current) return;

      if (current.key === key) {
        current.dir = current.dir === "asc" ? "desc" : "asc";
      } else {
        current.key = key;
        current.dir = "asc";
      }

      render();
    });

    updateSortHeaders(headId, sortKey);
  });
}

function setupDelegatedActions() {
  el("learners-table").addEventListener("click", async (event) => {
    const btn = event.target.closest("button[data-action]");
    if (!btn) return;

    const action = btn.dataset.action;
    const studentId = Number(btn.dataset.studentId || 0);
    if (!studentId) return;

    if (action === "view-learner") {
      await openLearnerDetail(studentId);
      return;
    }

    if (action === "unlink-learner") {
      if (!confirm("Unlink this learner's Telegram account?")) return;
      try {
        await apiFetch(`/api/students/${studentId}/unlink`, { method: "POST" });
        showToast("Learner unlinked");
        await Promise.all([refreshLearners(), refreshOverview()]);
      } catch (error) {
        showToast(error.message, "error");
      }
      return;
    }

    if (action === "rebuild-learner") {
      try {
        await apiFetch(`/api/students/${studentId}/rebuild-summary`, {
          method: "POST",
          body: JSON.stringify({ course_id: activeCourseId() }),
        });
        showToast("Summary rebuilt for learner");
        await Promise.all([refreshLearners(), refreshOverview(), refreshAtRisk()]);
      } catch (error) {
        showToast(error.message, "error");
      }
    }
  });

  el("reports-list").addEventListener("click", async (event) => {
    const btn = event.target.closest("button[data-action]");
    if (!btn) return;

    const action = btn.dataset.action;
    if (action !== "verify-report") return;

    const studentId = Number(btn.dataset.studentId || 0);
    const assignmentId = Number(btn.dataset.assignmentId || 0);
    const approved = btn.dataset.approved === "1";
    const reviewer = String(el("report-reviewer").value || "Web Dashboard").trim() || "Web Dashboard";

    try {
      await apiFetch("/api/reports/verify", {
        method: "POST",
        body: JSON.stringify({
          student_id: studentId,
          assignment_id: assignmentId,
          approved,
          reviewer,
        }),
      });
      showToast(approved ? "Report approved" : "Report denied");
      await Promise.all([refreshReports(), refreshOverview(), refreshLearners(), refreshAtRisk()]);
    } catch (error) {
      showToast(error.message, "error");
    }
  });
}

function sortRows(rows, sortState, options = {}) {
  const ordered = [...(rows || [])];
  const { key, dir } = sortState || {};
  const numericKeys = options.numericKeys || new Set();
  const dateKeys = options.dateKeys || new Set();
  const tieBreaker = options.tieBreaker || "full_name";

  if (!key || !dir) {
    return ordered;
  }

  ordered.sort((a, b) => {
    const rawA = a?.[key];
    const rawB = b?.[key];

    const missingA = rawA === null || rawA === undefined || rawA === "";
    const missingB = rawB === null || rawB === undefined || rawB === "";

    if (missingA && missingB) return 0;
    if (missingA) return 1;
    if (missingB) return -1;

    let cmp = 0;
    if (dateKeys.has(key)) {
      const aTime = Date.parse(String(rawA));
      const bTime = Date.parse(String(rawB));
      const safeA = Number.isFinite(aTime) ? aTime : 0;
      const safeB = Number.isFinite(bTime) ? bTime : 0;
      cmp = safeA - safeB;
    } else if (numericKeys.has(key)) {
      const aNum = Number(rawA || 0);
      const bNum = Number(rawB || 0);
      cmp = aNum - bNum;
    } else {
      cmp = String(rawA).localeCompare(String(rawB), undefined, {
        numeric: true,
        sensitivity: "base",
      });
    }

    if (cmp === 0 && tieBreaker) {
      cmp = String(a?.[tieBreaker] || "").localeCompare(String(b?.[tieBreaker] || ""), undefined, {
        numeric: true,
        sensitivity: "base",
      });
    }

    return dir === "desc" ? -cmp : cmp;
  });

  return ordered;
}

function updateSortHeaders(headId, sortKey) {
  const head = el(headId);
  if (!head) return;

  const current = state.sortState[sortKey];
  if (!current) return;

  head.querySelectorAll("th.sortable").forEach((th) => {
    th.classList.remove("asc", "desc");
    if (th.dataset.sortKey === current.key) {
      th.classList.add(current.dir);
    }
  });
}

function renderLearnersTable() {
  const tbody = el("learners-table");
  const rows = sortRows(state.learners, state.sortState.learners, {
    numericKeys: new Set(["id", "total_missing", "avg_all_pct", "completion_pct"]),
    tieBreaker: "full_name",
  });
  updateSortHeaders("learners-head", "learners");

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="9" class="report-meta">No learners found for this filter.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map((row) => {
      const missingClass = Number(row.total_missing || 0) > 0 ? "danger" : "success";
      const tg = row.telegram_id ? escapeHtml(row.telegram_id) : "-";
      return `
        <tr>
          <td>${fmtNum(row.id)}</td>
          <td>${escapeHtml(row.full_name)}</td>
          <td>${escapeHtml(row.course_name || "-")}</td>
          <td>${escapeHtml(row.lms_id || "-")}</td>
          <td>${tg}</td>
          <td><span class="badge ${missingClass}">${fmtNum(row.total_missing)}</span></td>
          <td>${fmtPct(row.avg_all_pct)}</td>
          <td>${Number(row.completion_pct || 0).toFixed(1)}%</td>
          <td>
            <div class="row-actions">
              <button class="btn btn-mini" data-action="view-learner" data-student-id="${row.id}">View</button>
              <button class="btn btn-mini" data-action="rebuild-learner" data-student-id="${row.id}">Rebuild</button>
              <button class="btn btn-mini danger" data-action="unlink-learner" data-student-id="${row.id}">Unlink</button>
            </div>
          </td>
        </tr>
      `;
    })
    .join("");
}

function renderDetailWorkTable() {
  const workBody = el("detail-work");
  const studentId = Number(state.detailStudentId || 0);
  const rows = sortRows(state.detailWorkRows, state.sortState.detailWork, {
    numericKeys: new Set(["score_points", "score_max"]),
    dateKeys: new Set(["due_date", "proof_uploaded_at"]),
    tieBreaker: "title",
  });
  updateSortHeaders("detail-work-head", "detailWork");

  if (!rows.length) {
    workBody.innerHTML = `<tr><td colspan="5" class="report-meta">No assignment rows for this learner/class.</td></tr>`;
    return;
  }

  workBody.innerHTML = rows
    .map((row) => {
      const scorePoints = Number(row.score_points || 0);
      const scoreMax = Number(row.score_max || 0);
      const score = scoreMax > 0 ? `${scorePoints.toFixed(2)} / ${scoreMax.toFixed(2)}` : "-";
      const proof = row.proof_uploaded_at
        ? `<a class="link-btn" target="_blank" href="/api/proof/${studentId}/${row.assignment_id}">View proof</a>`
        : "-";

      return `
        <tr>
          <td>${escapeHtml(row.title)}</td>
          <td><span class="badge ${statusBadge(row.status)}">${escapeHtml(row.status)}</span></td>
          <td>${score}</td>
          <td>${fmtDate(row.due_date)}</td>
          <td>${proof}</td>
        </tr>
      `;
    })
    .join("");
}

function renderAtRiskTable() {
  const tbody = el("atrisk-table");
  const rows = sortRows(state.atRiskRows, state.sortState.atRisk, {
    numericKeys: new Set([
      "total_missing",
      "total_assigned",
      "total_submitted",
      "avg_all_pct",
      "points_earned",
      "points_possible",
    ]),
    dateKeys: new Set(["last_synced"]),
    tieBreaker: "full_name",
  });
  updateSortHeaders("atrisk-head", "atRisk");

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="9" class="report-meta">No at-risk learners at this threshold.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map((row) => `
      <tr>
        <td>${escapeHtml(row.full_name)}</td>
        <td>${escapeHtml(row.telegram_id || "-")}</td>
        <td>${escapeHtml(row.course_name || "-")}</td>
        <td><span class="badge danger">${fmtNum(row.total_missing)}</span></td>
        <td>${fmtNum(row.total_assigned)}</td>
        <td>${fmtNum(row.total_submitted)}</td>
        <td>${fmtPct(row.avg_all_pct)}</td>
        <td>${Number(row.points_earned || 0).toFixed(2)} / ${Number(row.points_possible || 0).toFixed(2)}</td>
        <td>${fmtDate(row.last_synced)}</td>
      </tr>
    `)
    .join("");
}

function renderCampaignJobsTable() {
  const tbody = el("campaign-jobs-table");
  const rows = sortRows(state.campaignJobs, state.sortState.campaignJobs, {
    numericKeys: new Set(["id", "target_count", "sent_count"]),
    dateKeys: new Set(["run_at"]),
    tieBreaker: "id",
  });
  updateSortHeaders("campaign-jobs-head", "campaignJobs");

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="8" class="report-meta">No campaign jobs yet.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map((row) => `
      <tr>
        <td>${fmtNum(row.id)}</td>
        <td>${escapeHtml(row.template_key)}</td>
        <td>${fmtDate(row.run_at)}</td>
        <td><span class="badge ${statusBadge(row.status)}">${escapeHtml(row.status)}</span></td>
        <td>${fmtNum(row.target_count)}</td>
        <td>${fmtNum(row.sent_count)}</td>
        <td>${escapeHtml(row.created_by || "-")}</td>
        <td>${escapeHtml(row.error || "-")}</td>
      </tr>
    `)
    .join("");
}

function renderSyncLogTable() {
  const tbody = el("sync-log-table");
  const rows = sortRows(state.syncLogRows, state.sortState.syncLog, {
    numericKeys: new Set(["rows_added", "rows_updated", "course_id"]),
    dateKeys: new Set(["synced_at"]),
    tieBreaker: "synced_at",
  });
  updateSortHeaders("sync-log-head", "syncLog");

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="6" class="report-meta">No sync log entries yet.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map((row) => `
      <tr>
        <td>${fmtDate(row.synced_at)}</td>
        <td>${escapeHtml(row.source || "-")}</td>
        <td>${fmtNum(row.rows_added)}</td>
        <td>${fmtNum(row.rows_updated)}</td>
        <td>${fmtNum(row.course_id)}</td>
        <td>${escapeHtml(row.notes || "-")}</td>
      </tr>
    `)
    .join("");
}

function renderCourses(courses) {
  const courseSelect = el("global-course");
  courseSelect.innerHTML = "";

  const allOpt = document.createElement("option");
  allOpt.value = "0";
  allOpt.textContent = "All classes";
  courseSelect.appendChild(allOpt);

  courses.forEach((course) => {
    const opt = document.createElement("option");
    opt.value = String(course.id);
    opt.textContent = `${course.name}`;
    courseSelect.appendChild(opt);
  });
}

async function refreshOverview() {
  const q = new URLSearchParams({
    course_id: String(activeCourseId()),
    threshold: String(activeThreshold()),
  });

  const data = await apiFetch(`/api/overview?${q.toString()}`);
  const totals = data.totals || {};

  el("kpi-students").textContent = fmtNum(totals.students);
  el("kpi-registered").textContent = `Registered: ${fmtNum(totals.registered)}`;
  el("kpi-assignments").textContent = fmtNum(totals.assignments);
  el("kpi-submissions").textContent = `Submissions: ${fmtNum(totals.submissions)}`;
  el("kpi-missing").textContent = fmtNum(totals.missing);
  el("kpi-pending").textContent = `Pending reports: ${fmtNum(totals.pending_reports)}`;
  el("kpi-avg").textContent = fmtPct(totals.avg_overall);
  el("kpi-atrisk").textContent = `At-risk learners: ${fmtNum(totals.at_risk)}`;

  const statusRows = data.status_breakdown || [];
  const statusTotal = statusRows.reduce((sum, row) => sum + Number(row.total || 0), 0) || 1;
  const statusContainer = el("status-breakdown");
  statusContainer.innerHTML = statusRows.length
    ? statusRows
        .map((row) => {
          const pct = (Number(row.total || 0) * 100) / statusTotal;
          return `
            <div class="stack-row">
              <div class="stack-label">
                <span>${escapeHtml(row.status)}</span>
                <strong>${fmtNum(row.total)} (${pct.toFixed(1)}%)</strong>
              </div>
              <div class="bar"><span style="width:${pct.toFixed(2)}%"></span></div>
            </div>
          `;
        })
        .join("")
    : `<p class="report-meta">No submission statuses yet.</p>`;

  const topMissing = el("top-missing");
  const topRows = data.top_missing_assignments || [];
  topMissing.innerHTML = topRows.length
    ? topRows
        .map(
          (row) => `
            <div class="stack-row">
              <div class="stack-label">
                <span>${escapeHtml(row.title)}</span>
                <strong>${fmtNum(row.missing_count)} missing</strong>
              </div>
              <small class="report-meta">${escapeHtml(row.course_name)}</small>
            </div>
          `
        )
        .join("")
    : `<p class="report-meta">No missing assignments right now.</p>`;

  setCompletionRing(totals.completion_rate || 0);

  const latest = data.latest_sync;
  el("latest-sync").textContent = latest
    ? `Latest sync: ${fmtDate(latest.synced_at)} (${latest.source || "unknown source"})`
    : "No sync information available.";
}

async function refreshLearners() {
  const q = new URLSearchParams({
    course_id: String(activeCourseId()),
    search: activeSearch(),
    limit: "500",
  });

  const rows = await apiFetch(`/api/students?${q.toString()}`);
  state.learners = rows;
  renderLearnersTable();
}

async function openLearnerDetail(studentId) {
  const q = new URLSearchParams({
    course_id: String(activeCourseId()),
    limit: "240",
  });

  const data = await apiFetch(`/api/students/${studentId}?${q.toString()}`);
  const student = data.student || {};
  const summary = data.summary || {};
  const work = data.work || [];

  el("detail-title").textContent = `${student.full_name || "Learner"} - ${data.course_name || "Class"}`;

  el("detail-summary").innerHTML = `
    <div class="metric-pill"><strong>${fmtNum(summary.total_assigned)}</strong>Assigned</div>
    <div class="metric-pill"><strong>${fmtNum(summary.total_submitted)}</strong>Submitted</div>
    <div class="metric-pill"><strong>${fmtNum(summary.total_missing)}</strong>Missing</div>
    <div class="metric-pill"><strong>${fmtPct(summary.avg_all_pct)}</strong>Overall</div>
  `;

  state.detailStudentId = studentId;
  state.detailWorkRows = work;
  renderDetailWorkTable();

  // Keep learners table in sync with rebuilt summary values shown in detail view.
  const learnerIdx = state.learners.findIndex((row) => Number(row.id) === Number(studentId));
  if (learnerIdx >= 0) {
    const current = state.learners[learnerIdx];
    const totalAssigned = Number(summary.total_assigned ?? current.total_assigned ?? 0);
    const totalSubmitted = Number(summary.total_submitted ?? current.total_submitted ?? 0);
    const totalMissing = Number(summary.total_missing ?? current.total_missing ?? 0);
    const avgAllPct = Number(summary.avg_all_pct ?? current.avg_all_pct ?? 0);
    const completionPct = totalAssigned > 0 ? (totalSubmitted * 100.0) / totalAssigned : 0;

    state.learners[learnerIdx] = {
      ...current,
      total_assigned: totalAssigned,
      total_submitted: totalSubmitted,
      total_missing: totalMissing,
      avg_all_pct: avgAllPct,
      completion_pct: completionPct,
      last_synced: summary.last_synced || current.last_synced || "",
    };
    renderLearnersTable();
  }

  el("learner-detail").classList.remove("hidden");
}

async function refreshReports() {
  const q = new URLSearchParams({ course_id: String(activeCourseId()) });
  const rows = await apiFetch(`/api/pending-reports?${q.toString()}`);
  state.reports = rows;

  const host = el("reports-list");
  if (!rows.length) {
    host.innerHTML = `<p class="report-meta">No pending reports.</p>`;
    return;
  }

  host.innerHTML = rows
    .map((row) => {
      const proofBlock = row.proof_file_id
        ? `<div class="row-actions">
             <a class="link-btn" target="_blank" href="/api/proof/${row.student_id}/${row.assignment_id}">Preview evidence</a>
             <a class="link-btn" target="_blank" href="/api/proof/${row.student_id}/${row.assignment_id}?download=1">Download</a>
           </div>`
        : `<div class="report-meta">No evidence uploaded</div>`;

      return `
        <article class="report-card">
          <h4>${escapeHtml(row.full_name)}</h4>
          <div class="report-meta">${escapeHtml(row.course_name)} | Assignment #${fmtNum(row.assignment_id)}</div>
          <div><strong>${escapeHtml(row.assignment_title)}</strong></div>
          <div class="report-meta">Reported: ${fmtDate(row.flagged_at)}</div>
          ${row.flag_note ? `<div class="report-meta">Note: ${escapeHtml(row.flag_note)}</div>` : ""}
          ${row.proof_caption ? `<div class="report-meta">Proof caption: ${escapeHtml(row.proof_caption)}</div>` : ""}
          ${proofBlock}
          <div class="row-actions">
            <button class="btn btn-mini" data-action="verify-report" data-approved="1" data-student-id="${row.student_id}" data-assignment-id="${row.assignment_id}">Approve</button>
            <button class="btn btn-mini danger" data-action="verify-report" data-approved="0" data-student-id="${row.student_id}" data-assignment-id="${row.assignment_id}">Deny</button>
          </div>
        </article>
      `;
    })
    .join("");
}

async function refreshAtRisk() {
  const q = new URLSearchParams({
    course_id: String(activeCourseId()),
    threshold: String(activeThreshold()),
  });
  const rows = await apiFetch(`/api/at-risk?${q.toString()}`);
  state.atRiskRows = rows;
  renderAtRiskTable();
}

function renderCampaignPreview() {
  const templateKey = el("campaign-template").value;
  const textWrap = el("campaign-text-wrap");
  const customText = el("campaign-text").value || "";

  if (templateKey === "custom") {
    textWrap.classList.remove("hidden");
  } else {
    textWrap.classList.add("hidden");
  }

  const template =
    templateKey === "custom"
      ? customText
      : (state.bootstrap?.campaign_templates || []).find((t) => t.key === templateKey)?.text || "";

  const preview = template
    .replaceAll("{first_name}", "Learner")
    .replaceAll("{missing_count}", "3")
    .replaceAll("{missing_list}", "- Algebra Quiz\\n- Geometry Worksheet\\n- Homework 4");

  el("campaign-preview").textContent = preview || "No template selected.";
}

function toggleCustomSchedule() {
  const scheduleKey = el("campaign-schedule").value;
  const wrap = el("campaign-runat-wrap");
  if (scheduleKey === "custom") {
    wrap.classList.remove("hidden");
  } else {
    wrap.classList.add("hidden");
  }
}

function setupCampaignForm() {
  const templateSelect = el("campaign-template");
  const scheduleSelect = el("campaign-schedule");

  templateSelect.addEventListener("change", renderCampaignPreview);
  el("campaign-text").addEventListener("input", renderCampaignPreview);

  scheduleSelect.addEventListener("change", toggleCustomSchedule);
}

async function refreshCampaignJobs() {
  const rows = await apiFetch("/api/campaign-jobs?limit=60");
  state.campaignJobs = rows;
  renderCampaignJobsTable();
}

async function runDueCampaignJobs(silent = false) {
  try {
    const data = await apiFetch("/api/campaign-jobs/run-due", { method: "POST" });
    const status = `${fmtNum(data.messages_sent)} / ${fmtNum(data.messages_targeted)} delivered`;
    el("campaign-worker-status").textContent = `Last run: processed ${fmtNum(data.processed_jobs)} job(s), ${status}.`;

    if (!silent) {
      showToast(`Campaign run finished: ${status}`);
    }
    await Promise.all([refreshCampaignJobs(), refreshOverview()]);
    return data;
  } catch (error) {
    if (!silent) {
      showToast(error.message, "error");
    }
    throw error;
  }
}

async function createCampaign() {
  const templateKey = el("campaign-template").value;
  const scheduleKey = el("campaign-schedule").value;
  const runAt = el("campaign-run-at").value;
  const createdBy = String(el("campaign-created-by").value || "web_dashboard").trim() || "web_dashboard";
  const templateText = String(el("campaign-text").value || "").trim();

  if (templateKey === "custom" && !templateText) {
    showToast("Custom campaign text is required", "error");
    return;
  }

  try {
    const data = await apiFetch("/api/campaign-jobs", {
      method: "POST",
      body: JSON.stringify({
        template_key: templateKey,
        schedule_key: scheduleKey,
        run_at: runAt,
        template_text: templateText,
        created_by: createdBy,
      }),
    });

    if (data?.immediate_result) {
      const immediate = data.immediate_result;
      showToast(
        `Campaign sent now: ${fmtNum(immediate.sent_count)} / ${fmtNum(immediate.target_count)} delivered`
      );
      if (immediate.error) {
        showToast(immediate.error, "error");
      }
      await Promise.all([refreshCampaignJobs(), refreshOverview()]);
    } else {
      showToast("Campaign scheduled");
      await refreshCampaignJobs();
    }
  } catch (error) {
    showToast(error.message, "error");
  }
}

async function refreshSyncLog() {
  const q = new URLSearchParams({
    course_id: String(activeCourseId()),
    limit: "200",
  });
  const rows = await apiFetch(`/api/sync-log?${q.toString()}`);
  state.syncLogRows = rows;
  renderSyncLogTable();
}

async function createBackup() {
  try {
    const data = await apiFetch("/api/maintenance/backup", { method: "POST" });
    showToast(`Backup created: ${data.backup_file}`);
    await refreshSyncLog();
  } catch (error) {
    showToast(error.message, "error");
  }
}

async function rebuildSummaries() {
  try {
    const data = await apiFetch("/api/maintenance/rebuild-summaries", {
      method: "POST",
      body: JSON.stringify({ course_id: activeCourseId() }),
    });
    showToast(`Rebuilt ${fmtNum(data.rebuilt)} summary rows`);
    await Promise.all([refreshOverview(), refreshLearners(), refreshAtRisk(), refreshSyncLog()]);
  } catch (error) {
    showToast(error.message, "error");
  }
}

async function initSchema() {
  if (!confirm("Initialize schema from database/schema.sql?")) return;
  try {
    await apiFetch("/api/maintenance/init-schema", { method: "POST" });
    showToast("Schema initialized");
    await refreshAll();
  } catch (error) {
    showToast(error.message, "error");
  }
}

function hydrateCampaignControls() {
  const templates = state.bootstrap?.campaign_templates || [];
  const schedules = state.bootstrap?.schedule_options || [];

  el("campaign-template").innerHTML = templates
    .map((tpl) => `<option value="${escapeHtml(tpl.key)}">${escapeHtml(tpl.label)}</option>`)
    .join("");

  el("campaign-schedule").innerHTML = schedules
    .map((s) => `<option value="${escapeHtml(s.key)}">${escapeHtml(s.label)}</option>`)
    .join("");

  renderCampaignPreview();
  toggleCustomSchedule();
}

async function refreshAll() {
  try {
    await Promise.all([
      refreshOverview(),
      refreshLearners(),
      refreshReports(),
      refreshAtRisk(),
      refreshCampaignJobs(),
      refreshSyncLog(),
    ]);
    showToast("Dashboard refreshed");
  } catch (error) {
    showToast(error.message, "error");
  }
}

async function boot() {
  try {
    state.bootstrap = await apiFetch("/api/bootstrap");
    renderCourses(state.bootstrap.courses || []);

    el("global-course").value = String(state.bootstrap.defaults?.course_id || 0);
    el("global-threshold").value = String(state.bootstrap.defaults?.at_risk_threshold || 3);

    const senderEnabled = Boolean(state.bootstrap.campaign_sender_enabled);
    const pollSeconds = Number(state.bootstrap.campaign_poll_seconds || 30);
    if (!senderEnabled) {
      el("campaign-worker-status").textContent =
        "Campaign sender is disabled: BOT_TOKEN missing in .env";
      el("btn-campaign-run-due").disabled = true;
    } else {
      el("campaign-worker-status").textContent =
        `Background sender checks due jobs every ${pollSeconds}s.`;
    }

    setupTabs();
    setupHeaderActions();
    setupSectionActions();
    setupTableSorting();
    setupDelegatedActions();
    setupCampaignForm();
    hydrateCampaignControls();

    await refreshAll();
  } catch (error) {
    showToast(error.message, "error");
  }
}

// ── Google Classroom Sync ────────────────────────────────────────────────────

let _syncPollTimer = null;

function toggleSyncCustomRange() {
  const period = el("sync-period")?.value || "30";
  const customWrap = el("sync-custom-range");
  if (!customWrap) return;

  const isCustom = period === "custom";
  customWrap.classList.toggle("hidden", !isCustom);

  if (!isCustom) return;

  const startInput = el("sync-start-date");
  const endInput = el("sync-end-date");
  if (!startInput || !endInput) return;

  if (!endInput.value) {
    const today = new Date();
    endInput.value = today.toISOString().slice(0, 10);
  }
  if (!startInput.value) {
    const start = new Date();
    start.setDate(start.getDate() - 30);
    startInput.value = start.toISOString().slice(0, 10);
  }
}

async function startClassroomSync() {
  const btn = el("btn-sync-classroom");
  const statusEl = el("sync-classroom-status");
  const days = el("sync-period")?.value || "30";
  const payload = { days };
  let rangeLabel = `window=${days}`;

  if (!btn || !statusEl) return;
  if (_syncPollTimer) return;

  if (days === "custom") {
    const startDate = String(el("sync-start-date")?.value || "").trim();
    const endDate = String(el("sync-end-date")?.value || "").trim();
    if (!startDate || !endDate) {
      showToast("Please select both start and end date for custom range", "error");
      return;
    }
    if (startDate > endDate) {
      showToast("Start date cannot be after end date", "error");
      return;
    }
    payload.start_date = startDate;
    payload.end_date = endDate;
    rangeLabel = `${startDate} to ${endDate}`;
  }

  btn.disabled = true;
  statusEl.textContent = `Starting sync (${rangeLabel})...`;

  try {
    const data = await apiFetch("/api/sync-classroom", {
      method: "POST",
      body: JSON.stringify(payload),
    });

    const runningLabel =
      data.days === "custom" && data.start_date && data.end_date
        ? `${data.start_date} to ${data.end_date}`
        : `window=${data.days}`;
    statusEl.textContent = `Sync running (${runningLabel})...`;
    _syncPollTimer = setInterval(pollClassroomSyncStatus, 2000);
    await pollClassroomSyncStatus();
  } catch (err) {
    statusEl.textContent = "Sync error: " + err.message;
    btn.disabled = false;
  }
}

async function pollClassroomSyncStatus() {
  const btn = el("btn-sync-classroom");
  const statusEl = el("sync-classroom-status");
  if (!btn || !statusEl) return;

  try {
    const data = await apiFetch("/api/sync-classroom/status");

    if (data.status === "running" || data.status === "queued") {
      statusEl.textContent = data.message || "Sync running...";
      return;
    }

    clearInterval(_syncPollTimer);
    _syncPollTimer = null;
    btn.disabled = false;

    if (data.status === "done") {
      statusEl.textContent = data.message || "Sync complete";
      const added = Number(data.stats?.submissions_added || 0);
      const updated = Number(data.stats?.submissions_updated || 0);
      showToast(
        `Classroom sync complete: ${fmtNum(added)} added, ${fmtNum(updated)} updated`
      );
      await refreshAll();
    } else if (data.status === "error") {
      statusEl.textContent = data.message || "Sync failed";
      showToast(`Sync failed: ${data.message || "unknown error"}`, "error");
    } else {
      statusEl.textContent = data.message || "";
    }
  } catch (err) {
    clearInterval(_syncPollTimer);
    _syncPollTimer = null;
    btn.disabled = false;
    statusEl.textContent = "Status check failed: " + err.message;
  }
}

window.addEventListener("DOMContentLoaded", boot);
