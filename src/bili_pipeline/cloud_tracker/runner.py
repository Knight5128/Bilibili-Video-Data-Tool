from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from bili_pipeline.storage import BigQueryCrawlerStore

from .discovery import discover_author_videos, discover_rankboard_videos
from .settings import TrackerSettings
from .store import TrackerStore
from .tracking import SnapshotTaskResult, snapshot_videos


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class TrackerRunReport:
    run_id: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    phase: str
    discovered_count: int = 0
    tracked_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    messages: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def log(self, message: str) -> None:
        self.messages.append(message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "status": self.status,
            "phase": self.phase,
            "discovered_count": self.discovered_count,
            "tracked_count": self.tracked_count,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "skipped_count": self.skipped_count,
            "messages": self.messages,
            "details": self.details,
        }


class TrackerRunner:
    def __init__(self, settings: TrackerSettings | None = None) -> None:
        self.settings = settings or TrackerSettings.from_env()
        self.settings.require_gcp()
        self.tracker_store = TrackerStore(self.settings.gcp_config, table_prefix=self.settings.table_prefix)
        self.crawler_store = BigQueryCrawlerStore(self.settings.gcp_config)
        self.credential = self.settings.build_credential()

    def run_cycle(self, *, force: bool = False) -> TrackerRunReport:
        started_at = _utcnow()
        report = TrackerRunReport(
            run_id=uuid.uuid4().hex,
            started_at=started_at,
            finished_at=None,
            status="running",
            phase="bootstrap",
        )
        defaults = self.settings.control_defaults()
        self.tracker_store.ensure_control_row(defaults)

        if not self.tracker_store.acquire_lock(report.run_id, self.settings.lock_ttl_minutes):
            report.status = "skipped"
            report.phase = "lock"
            report.skipped_count = 1
            report.log("已有任务仍在运行，本次周期跳过。")
            report.finished_at = _utcnow()
            self._persist_report(report)
            return report

        try:
            control = self.tracker_store.get_control()
            if not force and control.get("paused_until") is not None and control["paused_until"] > _utcnow():
                report.status = "paused"
                report.phase = "pause_check"
                report.skipped_count = 1
                report.log("当前处于风控暂停窗口，本次周期跳过。")
                report.details["paused_until"] = control["paused_until"].isoformat()
                report.details["pause_reason"] = control.get("pause_reason", "")
                report.finished_at = _utcnow()
                self._persist_report(report)
                return report

            if force:
                self.tracker_store.clear_risk_backoff()

            tracking_window_days = int(control.get("tracking_window_days") or self.settings.tracking_window_days)
            comment_limit = int(control.get("comment_limit") or self.settings.comment_limit)
            max_videos_per_cycle = int(control.get("max_videos_per_cycle") or self.settings.max_videos_per_cycle)
            self.settings.author_bootstrap_days = int(
                control.get("author_bootstrap_days") or self.settings.author_bootstrap_days
            )

            report.phase = "discovery"
            rankboard_rows = []
            try:
                rankboard_rows = discover_rankboard_videos(self.settings, logger=report.log)
            except Exception as exc:  # noqa: BLE001
                # Keep the cycle alive when rankboard discovery is rate-limited or flaky.
                report.log(f"[DISCOVERY] 排行榜发现失败，已跳过：{exc}")
                report.details["rankboard_error"] = str(exc)
            author_rows = self.tracker_store.list_author_sources()
            author_discovered, author_failures = discover_author_videos(
                self.settings,
                author_rows=author_rows,
                tracking_window_days=tracking_window_days,
                credential=self.credential,
                logger=report.log,
            )
            for owner_mid, error in author_failures:
                self.tracker_store.mark_author_checked(owner_mid, success=False, error=error)
            discovered_by_owner = {row["owner_mid"] for row in author_rows}
            successful_owner_mids = discovered_by_owner - {owner_mid for owner_mid, _ in author_failures}
            for owner_mid in successful_owner_mids:
                self.tracker_store.mark_author_checked(int(owner_mid), success=True)

            discovered_rows = rankboard_rows + author_discovered
            self.tracker_store.upsert_discovered_videos(discovered_rows)
            report.discovered_count = len(discovered_rows)
            report.details["rankboard_discovered_count"] = len(rankboard_rows)
            report.details["author_discovered_count"] = len(author_discovered)
            report.details["author_failure_count"] = len(author_failures)

            report.phase = "tracking"
            self.tracker_store.expire_watchlist()
            active_rows = self.tracker_store.list_active_watch_videos(limit=max_videos_per_cycle)
            report.tracked_count = len(active_rows)
            bvids = [row["bvid"] for row in active_rows]

            def _on_snapshot(result: SnapshotTaskResult) -> None:
                if result.stat_ok:
                    self.tracker_store.record_snapshot_success(result.bvid, stat_time=_utcnow())
                elif result.stat_error:
                    self.tracker_store.record_snapshot_failure(result.bvid, stage="stat", error=result.stat_error)
                if result.comment_ok:
                    self.tracker_store.record_snapshot_success(result.bvid, comment_time=_utcnow())
                elif result.comment_error:
                    self.tracker_store.record_snapshot_failure(result.bvid, stage="comment", error=result.comment_error)

            snapshot_results = snapshot_videos(
                bvids=bvids,
                comment_limit=comment_limit,
                credential=self.credential,
                store=self.crawler_store,
                settings=self.settings,
                on_result=_on_snapshot,
            )
            for result in snapshot_results:
                if result.stat_ok and result.comment_ok:
                    report.success_count += 1
                else:
                    report.failed_count += 1
                if result.risk_detected:
                    control = self.tracker_store.apply_risk_backoff(
                        reason=result.risk_error or "触发 bilibili-api 风控，已自动暂停。",
                        base_minutes=self.settings.risk_pause_minutes,
                        max_minutes=self.settings.risk_pause_max_minutes,
                    )
                    report.status = "paused"
                    report.phase = "risk_backoff"
                    report.log(f"触发风控暂停：{result.risk_error}")
                    report.details["paused_until"] = control["paused_until"].isoformat() if control["paused_until"] else None
                    report.details["pause_reason"] = control.get("pause_reason", "")
                    break

            if report.status == "running":
                report.status = "success"
                report.phase = "complete"
                if snapshot_results:
                    self.tracker_store.clear_risk_backoff()
            report.finished_at = _utcnow()
            self._persist_report(report)
            return report
        except Exception as exc:  # noqa: BLE001
            report.status = "failed"
            report.phase = "exception"
            report.failed_count += 1
            report.log(f"任务异常终止：{exc}")
            report.details["exception"] = str(exc)
            report.finished_at = _utcnow()
            self._persist_report(report)
            raise
        finally:
            self.tracker_store.release_lock(report.run_id)

    def status(self) -> dict[str, Any]:
        control = self.tracker_store.ensure_control_row(self.settings.control_defaults())
        counts = self.tracker_store.count_watchlist_statuses()
        return {
            "config": self.settings.to_safe_dict(),
            "control": {
                **control,
                "paused_until": control["paused_until"].isoformat() if control.get("paused_until") else None,
                "last_risk_at": control["last_risk_at"].isoformat() if control.get("last_risk_at") else None,
                "lock_until": control["lock_until"].isoformat() if control.get("lock_until") else None,
            },
            "watchlist_counts": counts,
            "author_source_count": len(self.tracker_store.list_author_sources()),
            "recent_runs": self.tracker_store.list_recent_run_logs(self.settings.status_history_limit),
        }

    def _persist_report(self, report: TrackerRunReport) -> None:
        self.tracker_store.insert_run_log(
            run_id=report.run_id,
            started_at=report.started_at,
            finished_at=report.finished_at,
            status=report.status,
            phase=report.phase,
            discovered_count=report.discovered_count,
            tracked_count=report.tracked_count,
            success_count=report.success_count,
            failed_count=report.failed_count,
            skipped_count=report.skipped_count,
            message=report.messages[-1] if report.messages else report.status,
            details=report.to_dict(),
        )
