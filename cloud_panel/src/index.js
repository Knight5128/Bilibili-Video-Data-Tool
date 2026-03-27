import { jsonResponse, requireAccess, textResponse } from "./access.js";
import {
  fetchControlPlaneStatus,
  getCloudRunService,
  getSchedulerJob,
  updateCloudRunEnv,
  pauseSchedulerJob,
  resumeSchedulerJob,
} from "./google.js";
import {
  getTrackerAuthors,
  getTrackerMetrics,
  getTrackerRunLogs,
  getTrackerStatus,
  proxyTrackerCsv,
  runTrackerCycle,
  updateTrackerConfig,
} from "./tracker.js";
import { renderDashboardHtml, renderDocsHtml } from "./ui.js";

const METRICS_CACHE_TTL_MS = 15_000;
let trackerMetricsCache = {
  data: null,
  expiresAt: 0,
};

function getDefaultEnvKeys(env) {
  return (env.TRACKER_DEFAULT_ENV_KEYS || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

async function getCachedTrackerMetrics(env) {
  const now = Date.now();
  if (trackerMetricsCache.data && trackerMetricsCache.expiresAt > now) {
    return {
      ...trackerMetricsCache.data,
      cache: { hit: true, stale: false, ttl_ms: Math.max(0, trackerMetricsCache.expiresAt - now) },
    };
  }

  try {
    const data = await getTrackerMetrics(env);
    trackerMetricsCache = {
      data,
      expiresAt: now + METRICS_CACHE_TTL_MS,
    };
    return {
      ...data,
      cache: { hit: false, stale: false, ttl_ms: METRICS_CACHE_TTL_MS },
    };
  } catch (error) {
    if (trackerMetricsCache.data) {
      return {
        ...trackerMetricsCache.data,
        cache: {
          hit: true,
          stale: true,
          ttl_ms: 0,
          error: error instanceof Error ? error.message : String(error),
        },
      };
    }
    throw error;
  }
}

function jsonError(error, status = 500) {
  return jsonResponse(
    {
      error: error instanceof Error ? error.message : String(error),
    },
    status,
  );
}

async function handleApi(request, env, user) {
  const url = new URL(request.url);
  const path = url.pathname;

  if (path === "/api/me") {
    return jsonResponse({ email: user.email });
  }

  if (path === "/api/status") {
    const status = await getTrackerStatus(env);
    return jsonResponse({
      viewer: { email: user.email },
      health: {
        tracker_health: { status: "ok", service: "bilibili-cloud-tracker", source: "/admin/status" },
        tracker_http_ok: true,
        tracker_http_status: 200,
      },
      ...status,
    });
  }

  if (path === "/api/tracker/metrics") {
    return jsonResponse(await getCachedTrackerMetrics(env));
  }

  if (path === "/api/tracker/run-logs") {
    const limit = Number(url.searchParams.get("limit") || "20");
    return jsonResponse(await getTrackerRunLogs(env, limit));
  }

  if (path === "/api/tracker/authors" && request.method === "GET") {
    return jsonResponse(await getTrackerAuthors(env));
  }

  if (path === "/api/tracker/authors/upload" && request.method === "POST") {
    const response = await fetch(`${env.TRACKER_BASE_URL.replace(/\/$/, "")}/admin/authors/upload`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.TRACKER_ADMIN_TOKEN}`,
      },
      body: await request.formData(),
    });
    const text = await response.text();
    return new Response(text, {
      status: response.status,
      headers: { "content-type": response.headers.get("content-type") || "application/json; charset=utf-8" },
    });
  }

  if (path === "/api/tracker/config" && request.method === "POST") {
    return jsonResponse(await updateTrackerConfig(env, await request.json()));
  }

  if (path === "/api/tracker/run" && request.method === "POST") {
    return jsonResponse(await runTrackerCycle(env, url.searchParams.get("force") === "true"));
  }

  if (path === "/api/tracker/pause" && request.method === "POST") {
    const pausedUntil = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
    return jsonResponse(
      await updateTrackerConfig(env, {
        paused_until: pausedUntil,
        pause_reason: "Paused from Cloudflare control panel.",
      }),
    );
  }

  if (path === "/api/tracker/resume" && request.method === "POST") {
    return jsonResponse(await updateTrackerConfig(env, { clear_pause: true }));
  }

  if (path === "/api/tracker/export/meta-media-queue") {
    const response = await proxyTrackerCsv(env, "/admin/export/meta-media-queue");
    return new Response(await response.text(), {
      status: 200,
      headers: {
        "content-type": response.headers.get("content-type") || "text/csv; charset=utf-8",
        "content-disposition": response.headers.get("content-disposition") || 'attachment; filename="tracker_meta_media_queue.csv"',
      },
    });
  }

  if (path === "/api/tracker/export/watchlist") {
    const response = await proxyTrackerCsv(env, "/admin/watchlist?format=csv&only_active=true");
    return new Response(await response.text(), {
      status: 200,
      headers: {
        "content-type": response.headers.get("content-type") || "text/csv; charset=utf-8",
        "content-disposition": response.headers.get("content-disposition") || 'attachment; filename="tracker_watchlist.csv"',
      },
    });
  }

  if (path === "/api/tracker/export/authors") {
    const response = await proxyTrackerCsv(env, "/admin/authors?format=csv");
    return new Response(await response.text(), {
      status: 200,
      headers: {
        "content-type": response.headers.get("content-type") || "text/csv; charset=utf-8",
        "content-disposition": response.headers.get("content-disposition") || 'attachment; filename="tracker_authors.csv"',
      },
    });
  }

  if (path === "/api/gcp/status") {
    return jsonResponse(await fetchControlPlaneStatus(env));
  }

  if (path === "/api/gcp/env" && request.method === "GET") {
    const service = await getCloudRunService(env);
    return jsonResponse(service);
  }

  if (path === "/api/gcp/env" && request.method === "POST") {
    const payload = await request.json();
    return jsonResponse(await updateCloudRunEnv(env, payload.env || {}));
  }

  if (path === "/api/gcp/scheduler" && request.method === "GET") {
    return jsonResponse(await getSchedulerJob(env));
  }

  if (path === "/api/gcp/scheduler/pause" && request.method === "POST") {
    return jsonResponse(await pauseSchedulerJob(env));
  }

  if (path === "/api/gcp/scheduler/resume" && request.method === "POST") {
    return jsonResponse(await resumeSchedulerJob(env));
  }

  return jsonResponse({ error: "Not found" }, 404);
}

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      if (url.pathname === "/healthz") {
        return jsonResponse({ status: "ok", service: "bilibili-cloud-panel" });
      }

      const access = requireAccess(request, env);
      if (!access.ok) {
        return access.response;
      }

      if (url.pathname.startsWith("/api/")) {
        return await handleApi(request, env, access);
      }

      if (url.pathname === "/docs") {
        return textResponse(renderDocsHtml(), 200, "text/html; charset=utf-8");
      }

      if (url.pathname === "/") {
        return textResponse(renderDashboardHtml(getDefaultEnvKeys(env)), 200, "text/html; charset=utf-8");
      }

      return jsonResponse({ error: "Not found" }, 404);
    } catch (error) {
      return jsonError(error);
    }
  },
};
