from __future__ import annotations

from datetime import datetime, timezone

from flask import Flask, jsonify, request

from .admin import csv_response, parse_owner_mid_upload, require_admin
from .runner import TrackerRunner
from .settings import TrackerSettings


def create_app(settings: TrackerSettings | None = None) -> Flask:
    app = Flask(__name__)
    runner = TrackerRunner(settings)
    app.config["tracker_runner"] = runner

    @app.get("/healthz")
    def healthz():
        return jsonify({"status": "ok", "service": "bilibili-cloud-tracker"})

    @app.get("/")
    def index():
        return jsonify(
            {
                "service": "bilibili-cloud-tracker",
                "health": "/healthz",
                "status": "/admin/status",
                "manual_run": "/run",
            }
        )

    @app.post("/run")
    @require_admin
    def run_cycle():
        force = bool(request.args.get("force", "").lower() in {"1", "true", "yes"})
        report = runner.run_cycle(force=force)
        status_code = 200 if report.status in {"success", "paused", "skipped"} else 500
        return jsonify(report.to_dict()), status_code

    @app.post("/admin/authors/upload")
    @require_admin
    def upload_authors():
        if "file" not in request.files:
            return jsonify({"error": "请通过 multipart/form-data 上传 file 字段。"}), 400
        upload = request.files["file"]
        try:
            owner_mids = parse_owner_mid_upload(upload)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        source_name = (request.form.get("source_name") or upload.filename or "manual_upload").strip()
        saved = runner.tracker_store.replace_author_sources(
            owner_mids=owner_mids,
            source_name=source_name,
            payload={"filename": upload.filename, "uploaded_at": datetime.now(timezone.utc).isoformat()},
        )
        return jsonify({"status": "ok", "source_name": source_name, "owner_count": saved})

    @app.post("/admin/config/update")
    @require_admin
    def update_config():
        payload = request.get_json(silent=True) or {}
        allowed_fields = {
            "crawl_interval_hours",
            "tracking_window_days",
            "comment_limit",
            "author_bootstrap_days",
            "max_videos_per_cycle",
            "pause_reason",
        }
        updates = {key: value for key, value in payload.items() if key in allowed_fields}
        paused_until = payload.get("paused_until")
        if paused_until is not None:
            updates["paused_until"] = datetime.fromisoformat(paused_until) if paused_until else None
        elif payload.get("clear_pause"):
            updates["paused_until"] = None
            updates["pause_reason"] = ""
            updates["consecutive_risk_hits"] = 0
            updates["last_risk_at"] = None
        control = runner.tracker_store.update_control(updates)
        return jsonify(
            {
                "status": "ok",
                "control": {
                    **control,
                    "paused_until": control["paused_until"].isoformat() if control.get("paused_until") else None,
                    "last_risk_at": control["last_risk_at"].isoformat() if control.get("last_risk_at") else None,
                    "lock_until": control["lock_until"].isoformat() if control.get("lock_until") else None,
                },
            }
        )

    @app.get("/admin/status")
    @require_admin
    def status():
        return jsonify(runner.status())

    @app.get("/admin/metrics")
    @require_admin
    def metrics():
        return jsonify(
            {
                "metrics": runner.tracker_store.dashboard_metrics(),
                "watchlist_counts": runner.tracker_store.count_watchlist_statuses(),
                "active_author_count": runner.tracker_store.count_active_authors(),
            }
        )

    @app.get("/admin/run-logs")
    @require_admin
    def run_logs():
        limit = int(request.args.get("limit", "20"))
        rows = runner.tracker_store.list_recent_run_logs(limit=max(1, min(limit, 200)))
        return jsonify({"count": len(rows), "rows": rows})

    @app.get("/admin/authors")
    @require_admin
    def authors():
        rows = runner.tracker_store.list_author_sources()
        if request.args.get("format", "json").lower() == "csv":
            return csv_response("tracker_authors.csv", runner.tracker_store.rows_to_csv(rows))
        return jsonify({"count": len(rows), "rows": rows})

    @app.get("/admin/watchlist")
    @require_admin
    def watchlist():
        only_active = request.args.get("only_active", "1").lower() not in {"0", "false", "no"}
        rows = runner.tracker_store.watchlist_rows(only_active=only_active)
        if request.args.get("format", "json").lower() == "csv":
            return csv_response("tracker_watchlist.csv", runner.tracker_store.rows_to_csv(rows))
        return jsonify({"count": len(rows), "rows": rows})

    @app.get("/admin/export/meta-media-queue")
    @require_admin
    def export_meta_media_queue():
        rows = runner.tracker_store.export_meta_media_queue_rows()
        pending_only = request.args.get("pending_only", "1").lower() not in {"0", "false", "no"}
        if pending_only:
            rows = [row for row in rows if not row["meta_crawled"] or not row["media_crawled"]]
        if request.args.get("mark_exported", "0").lower() in {"1", "true", "yes"}:
            runner.tracker_store.mark_queue_exported([row["bvid"] for row in rows])
        if request.args.get("format", "csv").lower() == "json":
            return jsonify({"count": len(rows), "rows": rows})
        return csv_response("tracker_meta_media_queue.csv", runner.tracker_store.rows_to_csv(rows))

    return app

def _build_default_app() -> Flask:
    try:
        return create_app()
    except Exception as exc:  # noqa: BLE001
        fallback = Flask(__name__)

        @fallback.get("/healthz")
        def _healthz():
            return jsonify({"status": "error", "service": "bilibili-cloud-tracker", "error": str(exc)}), 500

        return fallback


app = _build_default_app()
