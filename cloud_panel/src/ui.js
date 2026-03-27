function layout({ title, body, active = "dashboard" }) {
  const navLink = (href, label, key) =>
    `<a class="nav-link ${active === key ? "active" : ""}" href="${href}">${label}</a>`;
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${title}</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #071019;
      --panel: #0f1b2b;
      --panel-2: #13243a;
      --text: #e7f0fa;
      --muted: #9ab0c9;
      --accent: #66c2ff;
      --danger: #ff7b7b;
      --success: #56d39b;
      --border: rgba(255,255,255,0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, "Segoe UI", Arial, sans-serif;
      background: linear-gradient(180deg, #05101b 0%, #0a1727 100%);
      color: var(--text);
    }
    a { color: var(--accent); text-decoration: none; }
    .shell { max-width: 1280px; margin: 0 auto; padding: 24px; }
    .topbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      margin-bottom: 20px;
      padding: 18px 20px;
      border: 1px solid var(--border);
      border-radius: 16px;
      background: rgba(15, 27, 43, 0.92);
      backdrop-filter: blur(8px);
    }
    .title h1 { margin: 0 0 6px 0; font-size: 28px; }
    .title p { margin: 0; color: var(--muted); }
    .nav { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 20px; }
    .nav-link {
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: var(--panel);
      color: var(--text);
    }
    .nav-link.active {
      background: var(--accent);
      color: #04121f;
      border-color: transparent;
      font-weight: 700;
    }
    .grid { display: grid; gap: 16px; }
    .grid-4 { grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
    .grid-2 { grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); }
    .card {
      border: 1px solid var(--border);
      border-radius: 16px;
      background: rgba(15, 27, 43, 0.92);
      padding: 18px;
    }
    .card h2, .card h3 { margin-top: 0; }
    .metric-value { font-size: 28px; font-weight: 700; margin: 10px 0 4px 0; }
    .metric-label { color: var(--muted); font-size: 13px; }
    .actions { display: flex; flex-wrap: wrap; gap: 10px; }
    button, input, textarea, select {
      font: inherit;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--panel-2);
      color: var(--text);
      padding: 10px 12px;
    }
    button {
      background: var(--accent);
      color: #03111b;
      border: none;
      cursor: pointer;
      font-weight: 700;
    }
    button.secondary { background: #20344d; color: var(--text); }
    button.danger { background: var(--danger); color: #230b0b; }
    form { display: grid; gap: 12px; }
    .field-grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
    label { display: grid; gap: 6px; font-size: 14px; color: var(--muted); }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    th, td {
      text-align: left;
      border-bottom: 1px solid var(--border);
      padding: 10px 8px;
      vertical-align: top;
    }
    pre {
      background: #0c1522;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .status-ok { color: var(--success); }
    .status-bad { color: var(--danger); }
    .muted { color: var(--muted); }
    .pill {
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      background: #20344d;
      font-size: 12px;
      color: var(--text);
    }
    .toast {
      position: fixed;
      right: 20px;
      bottom: 20px;
      min-width: 280px;
      max-width: 440px;
      padding: 14px 16px;
      border-radius: 12px;
      background: rgba(7, 16, 25, 0.96);
      border: 1px solid var(--border);
      display: none;
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="topbar">
      <div class="title">
        <h1>Bilibili Cloud Control Panel</h1>
        <p>Cloudflare Workers + Cloudflare Access + Google Cloud Run</p>
      </div>
      <div class="muted" id="viewer-email">Loading identity...</div>
    </div>
    <div class="nav">
      ${navLink("/", "Dashboard", "dashboard")}
      ${navLink("/#runtime-config", "Runtime Config", "runtime")}
      ${navLink("/#control", "Control", "control")}
      ${navLink("/#exports", "Exports", "exports")}
      ${navLink("/docs", "Docs", "docs")}
    </div>
    ${body}
  </div>
  <div class="toast" id="toast"></div>
</body>
</html>`;
}

export function renderDashboardHtml(defaultEnvKeys) {
  return layout({
    title: "Bilibili Cloud Control Panel",
    active: "dashboard",
    body: `
      <div class="grid grid-4">
        <div class="card"><div class="metric-label">Cloud Run Health</div><div class="metric-value" id="metric-health">-</div><div class="metric-label" id="metric-health-detail">Waiting for data</div></div>
        <div class="card"><div class="metric-label">Tracking Authors</div><div class="metric-value" id="metric-authors">-</div><div class="metric-label">Active selected authors</div></div>
        <div class="card"><div class="metric-label">Active Watch Videos</div><div class="metric-value" id="metric-watch-videos">-</div><div class="metric-label">Current interaction/comment watchlist</div></div>
        <div class="card"><div class="metric-label">Pending Meta/Media</div><div class="metric-value" id="metric-queue">-</div><div class="metric-label">Videos still pending complete base crawl</div></div>
        <div class="card"><div class="metric-label">Stat Snapshots Total</div><div class="metric-value" id="metric-stat-total">-</div><div class="metric-label" id="metric-stat-24h">Last 24h: -</div></div>
        <div class="card"><div class="metric-label">Comment Snapshots Total</div><div class="metric-value" id="metric-comment-total">-</div><div class="metric-label" id="metric-comment-24h">Last 24h: -</div></div>
        <div class="card"><div class="metric-label">Scheduler State</div><div class="metric-value" id="metric-scheduler">-</div><div class="metric-label" id="metric-scheduler-detail">-</div></div>
        <div class="card"><div class="metric-label">Paused Until</div><div class="metric-value" id="metric-paused-until">-</div><div class="metric-label" id="metric-pause-reason">-</div></div>
      </div>

      <div class="grid grid-2" style="margin-top: 18px;">
        <section class="card" id="runtime-config">
          <h2>Runtime Config</h2>
          <p class="muted">Cloud Run 环境变量更新会触发新 revision；Tracker runtime config 则直接热更新到 BigQuery 控制表。</p>
          <form id="env-form">
            <div class="field-grid" id="env-fields"></div>
            <div class="actions">
              <button type="submit">Update Cloud Run Env</button>
              <button type="button" class="secondary" id="reload-env">Reload Current Env</button>
            </div>
          </form>
          <hr style="border-color: rgba(255,255,255,0.08); margin: 18px 0;" />
          <form id="runtime-form">
            <div class="field-grid">
              <label>TRACKER_CRAWL_INTERVAL_HOURS<input name="crawl_interval_hours" type="number" min="1" /></label>
              <label>TRACKER_TRACKING_WINDOW_DAYS<input name="tracking_window_days" type="number" min="1" /></label>
              <label>TRACKER_COMMENT_LIMIT<input name="comment_limit" type="number" min="1" /></label>
              <label>TRACKER_AUTHOR_BOOTSTRAP_DAYS<input name="author_bootstrap_days" type="number" min="1" /></label>
              <label>TRACKER_MAX_VIDEOS_PER_CYCLE<input name="max_videos_per_cycle" type="number" min="1" /></label>
            </div>
            <div class="actions">
              <button type="submit">Update Tracker Runtime Config</button>
            </div>
          </form>
        </section>

        <section class="card" id="control">
          <h2>Control</h2>
          <div class="actions">
            <button id="run-now">Run Once</button>
            <button id="run-force" class="secondary">Force Run</button>
            <button id="pause-tracker" class="danger">Pause Tracker</button>
            <button id="resume-tracker" class="secondary">Resume Tracker</button>
            <button id="pause-scheduler" class="danger">Pause Scheduler</button>
            <button id="resume-scheduler" class="secondary">Resume Scheduler</button>
          </div>
          <div style="margin-top: 18px;">
            <h3>Selected Authors Upload</h3>
            <form id="authors-form">
              <label>Source Name<input name="source_name" value="selected_authors" /></label>
              <label>CSV File<input name="file" type="file" accept=".csv" /></label>
              <div class="actions">
                <button type="submit">Upload Authors CSV</button>
              </div>
            </form>
          </div>
          <div style="margin-top: 18px;">
            <h3>Cloud Run Revision Summary</h3>
            <pre id="service-summary">Loading...</pre>
          </div>
        </section>
      </div>

      <div class="grid grid-2" style="margin-top: 18px;">
        <section class="card" id="exports">
          <h2>Exports</h2>
          <div class="actions">
            <button data-export="/api/tracker/export/meta-media-queue">Download Meta/Media Queue</button>
            <button data-export="/api/tracker/export/watchlist">Download Watchlist</button>
            <button data-export="/api/tracker/export/authors">Download Authors</button>
          </div>
          <p class="muted" style="margin-top: 12px;">导出的 CSV 由 Worker 代理 Cloud Tracker 接口返回，浏览器可直接下载。</p>
        </section>

        <section class="card">
          <h2>Recent Run Logs</h2>
          <div id="run-logs-table">Loading...</div>
        </section>
      </div>

      <script>
        const defaultEnvKeys = ${JSON.stringify(defaultEnvKeys)};

        const state = {
          status: null,
          metrics: null,
          controlPlane: null,
        };

        function showToast(message, isError = false) {
          const toast = document.getElementById("toast");
          toast.style.display = "block";
          toast.style.borderColor = isError ? "rgba(255,123,123,0.4)" : "rgba(102,194,255,0.35)";
          toast.textContent = message;
          window.clearTimeout(window.__toastTimer);
          window.__toastTimer = window.setTimeout(() => { toast.style.display = "none"; }, 4200);
        }

        async function api(path, init = {}) {
          const response = await fetch(path, init);
          if (!response.ok) {
            const text = await response.text();
            throw new Error(text || ("HTTP " + response.status));
          }
          return response.json();
        }

        function errorText(error) {
          return error?.message || String(error);
        }

        function renderRunLogs(rows) {
          if (!rows.length) {
            return "<p class='muted'>No run logs yet.</p>";
          }
          return "<table><thead><tr><th>Started</th><th>Status</th><th>Phase</th><th>Tracked</th><th>Message</th></tr></thead><tbody>" +
            rows.map(row => "<tr>" +
              "<td>" + (row.started_at || "") + "</td>" +
              "<td><span class='pill'>" + (row.status || "") + "</span></td>" +
              "<td>" + (row.phase || "") + "</td>" +
              "<td>" + (row.tracked_count ?? "-") + "</td>" +
              "<td>" + (row.message || "") + "</td>" +
            "</tr>").join("") +
            "</tbody></table>";
        }

        function setText(id, value) {
          const node = document.getElementById(id);
          if (node) node.textContent = value;
        }

        function hydrateEnvForm(envMap) {
          const container = document.getElementById("env-fields");
          container.innerHTML = "";
          for (const key of defaultEnvKeys) {
            const label = document.createElement("label");
            label.innerHTML = key + "<input name='" + key + "' value='" + (envMap[key] || "") + "' />";
            container.appendChild(label);
          }
        }

        function hydrateRuntimeForm(control) {
          const form = document.getElementById("runtime-form");
          for (const key of ["crawl_interval_hours", "tracking_window_days", "comment_limit", "author_bootstrap_days", "max_videos_per_cycle"]) {
            if (form.elements[key]) {
              form.elements[key].value = control?.[key] ?? "";
            }
          }
        }

        function renderDashboard() {
          const status = state.status || {};
          const metrics = state.metrics?.metrics || {};
          const watchlistCounts = state.metrics?.watchlist_counts || {};
          const activeAuthorCount = state.metrics?.active_author_count ?? "-";
          const control = status.control || {};
          const controlPlane = state.controlPlane || {};
          const cloudRun = controlPlane.cloudRun || {};
          const scheduler = controlPlane.scheduler || {};
          setText("viewer-email", state.status?.viewer?.email || "Authenticated");
          setText("metric-health", status.health?.tracker_health?.status || "unknown");
          setText("metric-health-detail", cloudRun.latestReadyRevision || "No revision data");
          setText("metric-authors", String(activeAuthorCount));
          setText("metric-watch-videos", String(watchlistCounts.active ?? metrics.active_watch_video_count ?? 0));
          setText("metric-queue", String(metrics.meta_media_queue_pending ?? 0));
          setText("metric-stat-total", String(metrics.stat_snapshot_total ?? 0));
          setText("metric-stat-24h", "Last 24h: " + String(metrics.stat_snapshot_last_24h ?? 0));
          setText("metric-comment-total", String(metrics.comment_snapshot_total ?? 0));
          setText("metric-comment-24h", "Last 24h: " + String(metrics.comment_snapshot_last_24h ?? 0));
          setText("metric-scheduler", scheduler.state || "unknown");
          setText("metric-scheduler-detail", scheduler.schedule || "No schedule");
          setText("metric-paused-until", control.paused_until || "-");
          setText("metric-pause-reason", control.pause_reason || "No pause reason");
          if (state.metrics?.cache?.stale) {
            setText("metric-comment-24h", "Last 24h: " + String(metrics.comment_snapshot_last_24h ?? 0) + " (stale)");
          }
          document.getElementById("service-summary").textContent = JSON.stringify({
            uri: cloudRun.uri,
            revision: cloudRun.latestReadyRevision,
            image: cloudRun.image,
            timeout: cloudRun.timeout,
            schedulerState: scheduler.state,
          }, null, 2);
        }

        async function reloadAll() {
          const [status, metrics, controlPlane, runLogs] = await Promise.allSettled([
            api("/api/status"),
            api("/api/tracker/metrics"),
            api("/api/gcp/status"),
            api("/api/tracker/run-logs?limit=12"),
          ]);

          const warnings = [];
          if (status.status === "fulfilled") {
            state.status = status.value;
          } else {
            warnings.push("Status refresh failed: " + errorText(status.reason));
          }

          if (metrics.status === "fulfilled") {
            state.metrics = metrics.value;
            if (metrics.value?.cache?.stale) {
              warnings.push("Metrics fallback to cached data: " + (metrics.value.cache.error || "upstream unavailable"));
            }
          } else {
            warnings.push("Metrics refresh failed: " + errorText(metrics.reason));
          }

          if (controlPlane.status === "fulfilled") {
            state.controlPlane = controlPlane.value;
          } else {
            warnings.push("Cloud Run status refresh failed: " + errorText(controlPlane.reason));
          }

          if (controlPlane.status === "fulfilled") {
            hydrateEnvForm(controlPlane.value.cloudRun?.env || {});
          }
          if (status.status === "fulfilled") {
            hydrateRuntimeForm(status.value.control || {});
          }

          renderDashboard();

          if (runLogs.status === "fulfilled") {
            document.getElementById("run-logs-table").innerHTML = renderRunLogs(runLogs.value.rows || []);
          } else if (!state.status && !state.controlPlane && !state.metrics) {
            document.getElementById("run-logs-table").innerHTML = "<p class='muted'>Unable to load run logs right now.</p>";
            warnings.push("Run logs refresh failed: " + errorText(runLogs.reason));
          } else {
            warnings.push("Run logs refresh failed: " + errorText(runLogs.reason));
          }

          if (warnings.length) {
            showToast(warnings[0], true);
          }
        }

        document.getElementById("env-form").addEventListener("submit", async (event) => {
          event.preventDefault();
          const form = new FormData(event.currentTarget);
          const env = {};
          for (const key of defaultEnvKeys) env[key] = form.get(key);
          await api("/api/gcp/env", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ env }),
          });
          showToast("Cloud Run environment update requested.");
          await reloadAll();
        });

        document.getElementById("reload-env").addEventListener("click", reloadAll);

        document.getElementById("runtime-form").addEventListener("submit", async (event) => {
          event.preventDefault();
          const form = new FormData(event.currentTarget);
          const payload = {};
          for (const [key, value] of form.entries()) payload[key] = Number(value);
          await api("/api/tracker/config", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(payload),
          });
          showToast("Tracker runtime config updated.");
          await reloadAll();
        });

        document.getElementById("run-now").addEventListener("click", async () => {
          const data = await api("/api/tracker/run", { method: "POST" });
          showToast("Tracker run started: " + data.status);
          await reloadAll();
        });

        document.getElementById("run-force").addEventListener("click", async () => {
          const data = await api("/api/tracker/run?force=true", { method: "POST" });
          showToast("Forced tracker run: " + data.status);
          await reloadAll();
        });

        document.getElementById("pause-tracker").addEventListener("click", async () => {
          await api("/api/tracker/pause", { method: "POST" });
          showToast("Tracker paused.");
          await reloadAll();
        });

        document.getElementById("resume-tracker").addEventListener("click", async () => {
          await api("/api/tracker/resume", { method: "POST" });
          showToast("Tracker resumed.");
          await reloadAll();
        });

        document.getElementById("pause-scheduler").addEventListener("click", async () => {
          await api("/api/gcp/scheduler/pause", { method: "POST" });
          showToast("Scheduler paused.");
          await reloadAll();
        });

        document.getElementById("resume-scheduler").addEventListener("click", async () => {
          await api("/api/gcp/scheduler/resume", { method: "POST" });
          showToast("Scheduler resumed.");
          await reloadAll();
        });

        document.getElementById("authors-form").addEventListener("submit", async (event) => {
          event.preventDefault();
          const formData = new FormData(event.currentTarget);
          const response = await fetch("/api/tracker/authors/upload", { method: "POST", body: formData });
          if (!response.ok) throw new Error(await response.text());
          showToast("Author CSV uploaded.");
          await reloadAll();
        });

        document.querySelectorAll("[data-export]").forEach((button) => {
          button.addEventListener("click", () => {
            window.open(button.getAttribute("data-export"), "_blank");
          });
        });

        reloadAll().catch((error) => {
          showToast(errorText(error), true);
          document.getElementById("run-logs-table").innerHTML = "<p class='muted'>Dashboard loaded with partial data.</p>";
        });
      </script>
    `,
  });
}

export function renderDocsHtml() {
  return layout({
    title: "Bilibili Cloud Control Panel Docs",
    active: "docs",
    body: `
      <section class="card">
        <h2>Docs</h2>
        <p class="muted">这个页面概览说明 Cloudflare 控制台如何与 Cloud Run Tracker 协同工作。</p>
        <h3>1. 角色划分</h3>
        <ul>
          <li>Cloudflare Workers：公网控制台、鉴权入口、Google Cloud Admin API 代理。</li>
          <li>Cloud Run Tracker：真正执行视频发现、互动快照、评论切片写入 BigQuery。</li>
          <li>Cloud Scheduler：定时触发 Cloud Run Tracker 周期任务。</li>
        </ul>
        <h3>2. 推荐控制语义</h3>
        <ul>
          <li>停止周期任务：优先暂停 Cloud Scheduler Job。</li>
          <li>暂停追踪：通过 Tracker 的运行时配置进入 pause 模式。</li>
          <li>修改环境变量：通过 Cloud Run Admin API 更新 service revision。</li>
        </ul>
        <h3>3. 关键 Secrets</h3>
        <ul>
          <li><code>GOOGLE_SERVICE_ACCOUNT_JSON</code></li>
          <li><code>TRACKER_ADMIN_TOKEN</code></li>
          <li><code>CF_ACCESS_ALLOWED_EMAILS</code>（可选）</li>
        </ul>
        <h3>4. 部署建议</h3>
        <ul>
          <li>用 Cloudflare Access 保护整个控制台。</li>
          <li>每个用户独立部署一套 Worker + Cloud Run + Scheduler。</li>
          <li>让 Worker 保存单租户自己的 GCP 配置与服务账号密钥。</li>
        </ul>
        <h3>5. 下一步</h3>
        <p>更详细的部署说明会在仓库文档中单独提供，包括 Wrangler、Cloudflare Access、Google Service Account、Cloud Run 与 Cloud Scheduler 的逐步配置方法。</p>
      </section>
    `,
  });
}
