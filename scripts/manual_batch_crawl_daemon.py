from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bilibili_api import Credential
from bili_pipeline.models import GCPStorageConfig


DEFAULT_STREAM_DATA_TIME_WINDOW_HOURS = 14 * 24
LOCAL_GCP_CONFIG_PATH = PROJECT_ROOT / ".local" / "bilibili-datahub.gcp.config.json"
DEFAULT_GCP_CONFIG = {
    "gcp_project_id": "",
    "bigquery_dataset": "bili_video_data_crawler",
    "gcs_bucket_name": "",
    "gcp_region": "",
    "credentials_path": "",
    "gcs_object_prefix": "bilibili-media",
    "gcs_public_base_url": "",
}


@dataclass
class DaemonState:
    current_status: str = "idle"
    current_session_dir: str | None = None
    current_started_at: datetime | None = None
    last_result: dict[str, Any] | None = None
    last_error: str = ""
    last_finished_at: datetime | None = None
    worker_thread: threading.Thread | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _build_env_credential() -> Credential | None:
    sessdata = os.getenv("BILI_SESSDATA", "").strip()
    bili_jct = os.getenv("BILI_BILI_JCT", "").strip()
    buvid3 = os.getenv("BILI_BUVID3", "").strip()
    if not sessdata and not bili_jct and not buvid3:
        return None
    return Credential(sessdata=sessdata, bili_jct=bili_jct, buvid3=buvid3)


def _load_gcp_config() -> GCPStorageConfig:
    payload = dict(DEFAULT_GCP_CONFIG)
    if LOCAL_GCP_CONFIG_PATH.exists():
        try:
            loaded = json.loads(LOCAL_GCP_CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            loaded = {}
        if isinstance(loaded, dict):
            payload.update({key: loaded.get(key, value) for key, value in payload.items()})
    return GCPStorageConfig(
        project_id=str(payload.get("gcp_project_id", "")).strip(),
        bigquery_dataset=str(payload.get("bigquery_dataset", "")).strip(),
        gcs_bucket_name=str(payload.get("gcs_bucket_name", "")).strip(),
        gcp_region=str(payload.get("gcp_region", "")).strip(),
        credentials_path=str(payload.get("credentials_path", "")).strip(),
        object_prefix=str(payload.get("gcs_object_prefix", "")).strip(),
        public_base_url=str(payload.get("gcs_public_base_url", "")).strip(),
    )


def _print_status(state: DaemonState, next_run_at: datetime) -> None:
    with state.lock:
        payload = {
            "timestamp": _now().isoformat(timespec="seconds"),
            "status": state.current_status,
            "current_session_dir": state.current_session_dir,
            "current_started_at": state.current_started_at.isoformat(timespec="seconds") if state.current_started_at else None,
            "last_finished_at": state.last_finished_at.isoformat(timespec="seconds") if state.last_finished_at else None,
            "next_run_at": next_run_at.isoformat(timespec="seconds"),
            "last_error": state.last_error,
            "last_result": state.last_result,
        }
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def _run_once(args: argparse.Namespace, state: DaemonState) -> None:
    from bili_pipeline.datahub.manual_batch_runner import run_manual_realtime_batch_crawl

    started_at = _now()
    with state.lock:
        state.current_status = "running"
        state.current_started_at = started_at
        state.current_session_dir = None
        state.last_error = ""

    try:
        gcp_config = _load_gcp_config()
        credential = _build_env_credential()
        result = run_manual_realtime_batch_crawl(
            gcp_config=gcp_config,
            stream_data_time_window_hours=int(args.stream_data_time_window_hours),
            parallelism=int(args.parallelism),
            comment_limit=int(args.comment_limit),
            consecutive_failure_limit=int(args.consecutive_failure_limit),
            credential=credential,
        )
        with state.lock:
            state.current_status = "idle"
            state.current_session_dir = result.session_dir
            state.current_started_at = None
            state.last_finished_at = _now()
            state.last_result = result.to_dict()
            state.last_error = result.error
        print(json.dumps({"event": "manual_batch_crawl_finished", "result": result.to_dict()}, ensure_ascii=False), flush=True)
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.current_status = "idle"
            state.current_started_at = None
            state.last_finished_at = _now()
            state.last_error = str(exc)
            state.last_result = {
                "status": "failed",
                "error": str(exc),
                "started_at": started_at.isoformat(timespec="seconds"),
                "finished_at": state.last_finished_at.isoformat(timespec="seconds") if state.last_finished_at else None,
            }
        print(json.dumps({"event": "manual_batch_crawl_failed", "error": str(exc)}, ensure_ascii=False), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run manual realtime batch crawl every few hours and print status heartbeats.")
    parser.add_argument("--interval-hours", type=float, default=6.0, help="Interval between manual crawl starts.")
    parser.add_argument("--status-interval-minutes", type=float, default=15.0, help="How often to print current status.")
    parser.add_argument(
        "--stream-data-time-window-hours",
        type=int,
        default=DEFAULT_STREAM_DATA_TIME_WINDOW_HOURS,
        help="Only crawl videos whose pubdate is within this many hours.",
    )
    parser.add_argument("--parallelism", type=int, default=2, help="Manual batch crawl parallelism.")
    parser.add_argument("--comment-limit", type=int, default=10, help="Comment snapshot size per video.")
    parser.add_argument("--consecutive-failure-limit", type=int, default=10, help="Stop after this many consecutive failures.")
    parser.add_argument("--skip-initial-run", action="store_true", help="Wait until the first full interval before starting.")
    args = parser.parse_args()

    interval = timedelta(hours=float(args.interval_hours))
    status_interval = timedelta(minutes=float(args.status_interval_minutes))
    state = DaemonState()
    next_run_at = _now() + interval if args.skip_initial_run else _now()
    last_status_print_at: datetime | None = None

    print(
        json.dumps(
            {
                "event": "manual_batch_crawl_daemon_started",
                "interval_hours": float(args.interval_hours),
                "status_interval_minutes": float(args.status_interval_minutes),
                "stream_data_time_window_hours": int(args.stream_data_time_window_hours),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    try:
        while True:
            now = _now()
            with state.lock:
                worker_alive = state.worker_thread is not None and state.worker_thread.is_alive()
            if now >= next_run_at and not worker_alive:
                worker = threading.Thread(target=_run_once, args=(args, state), daemon=True)
                with state.lock:
                    state.worker_thread = worker
                worker.start()
                next_run_at = now + interval

            if last_status_print_at is None or now - last_status_print_at >= status_interval:
                _print_status(state, next_run_at)
                last_status_print_at = now

            time.sleep(30)
    except KeyboardInterrupt:
        print(json.dumps({"event": "manual_batch_crawl_daemon_stopped"}, ensure_ascii=False), flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
