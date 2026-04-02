from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bili_pipeline.datahub.background_tasks import (  # noqa: E402
    finalize_background_task_status,
    load_background_task_config,
    load_background_task_status,
    load_credential_from_cookie_file,
    register_active_background_task,
    update_background_task_status,
)
from bili_pipeline.datahub.config import build_gcp_config  # noqa: E402
from bili_pipeline.datahub.manual_batch_runner import run_manual_realtime_batch_crawl  # noqa: E402
from bili_pipeline.datahub.manual_media_runner import run_manual_media_mode_a, run_manual_media_mode_b  # noqa: E402
from bili_pipeline.models import GCPStorageConfig, MediaDownloadStrategy  # noqa: E402
from bili_pipeline.storage import BigQueryCrawlerStore  # noqa: E402


def _now() -> str:
    return datetime.now().isoformat()


def _build_media_strategy(gcp_config: GCPStorageConfig, payload: dict) -> MediaDownloadStrategy:
    return MediaDownloadStrategy(
        max_height=int(payload.get("max_height", 1080)),
        chunk_size_mb=int(payload.get("chunk_size_mb", 4)),
        storage_backend="gcs",
        gcp_config=gcp_config,
    )


def _build_cookie_provider(cookie_path: str):
    return lambda: load_credential_from_cookie_file(cookie_path)


def _read_uploaded_frames(file_paths: list[str]) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for file_path in file_paths:
        path = Path(file_path)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            frames.append(pd.read_csv(path))
        elif suffix in {".xlsx", ".xls"}:
            frames.append(pd.read_excel(path))
        else:
            raise ValueError(f"不支持的上传文件类型：{path.name}")
    return frames


def _run_task(task_dir: Path, config: dict) -> dict:
    payload = dict(config.get("payload") or {})
    gcp_config = build_gcp_config(dict(config.get("gcp_payload") or {}))
    media_strategy = _build_media_strategy(gcp_config, payload)
    cookie_path = str(config.get("cookie_path") or "").strip()
    credential_provider = _build_cookie_provider(cookie_path) if cookie_path else None
    batch_size = int(config.get("cookie_refresh_batch_size") or 100)
    task_kind = str(config.get("task_kind") or "").strip()

    if task_kind == "manual_dynamic_batch":
        result = run_manual_realtime_batch_crawl(
            gcp_config=gcp_config,
            stream_data_time_window_hours=int(payload["stream_data_time_window_hours"]),
            parallelism=int(payload["parallelism"]),
            comment_limit=int(payload["comment_limit"]),
            consecutive_failure_limit=int(payload["consecutive_failure_limit"]),
            media_strategy=media_strategy,
            max_height=int(payload["max_height"]),
            chunk_size_mb=int(payload["chunk_size_mb"]),
            video_pool_root=payload["video_pool_root"],
            manual_crawls_root_dir=payload["manual_crawls_root_dir"],
            selected_flooring_csvs=payload.get("selected_flooring_csvs") or [],
            selected_uid_task_dirs=payload.get("selected_uid_task_dirs") or [],
            credential_provider=credential_provider,
            cookie_refresh_batch_size=batch_size,
        )
        return result.to_dict()

    store = BigQueryCrawlerStore(gcp_config)
    if task_kind == "manual_media_mode_a":
        result = run_manual_media_mode_a(
            store=store,
            gcp_config=gcp_config,
            manual_crawls_root_dir=payload["manual_crawls_root_dir"],
            enable_sleep_resume=bool(payload["enable_sleep_resume"]),
            sleep_minutes=int(payload["sleep_minutes"]),
            parallelism=int(payload["parallelism"]),
            comment_limit=int(payload["comment_limit"]),
            consecutive_failure_limit=int(payload["consecutive_failure_limit"]),
            media_strategy=media_strategy,
            max_height=int(payload["max_height"]),
            chunk_size_mb=int(payload["chunk_size_mb"]),
            credential_provider=credential_provider,
            cookie_refresh_batch_size=batch_size,
        )
        return result.to_dict()

    if task_kind == "manual_media_mode_b":
        uploaded_file_paths = list(payload.get("uploaded_file_paths") or [])
        uploaded_names = list(payload.get("uploaded_names") or [])
        result = run_manual_media_mode_b(
            uploaded_frames=_read_uploaded_frames(uploaded_file_paths),
            uploaded_names=uploaded_names,
            store=store,
            gcp_config=gcp_config,
            manual_crawls_root_dir=payload["manual_crawls_root_dir"],
            enable_sleep_resume=bool(payload["enable_sleep_resume"]),
            sleep_minutes=int(payload["sleep_minutes"]),
            parallelism=int(payload["parallelism"]),
            comment_limit=int(payload["comment_limit"]),
            consecutive_failure_limit=int(payload["consecutive_failure_limit"]),
            media_strategy=media_strategy,
            max_height=int(payload["max_height"]),
            chunk_size_mb=int(payload["chunk_size_mb"]),
            credential_provider=credential_provider,
            cookie_refresh_batch_size=batch_size,
        )
        return result.to_dict()

    raise ValueError(f"未知后台任务类型：{task_kind}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-dir", required=True)
    args = parser.parse_args()

    task_dir = Path(args.task_dir).resolve()
    config = load_background_task_config(task_dir)
    scope = str(config.get("scope") or config.get("task_kind") or "datahub_task")
    register_active_background_task(scope, task_dir=task_dir, pid=os.getpid())
    update_background_task_status(
        task_dir,
        {
            "status": "running",
            "task_kind": config.get("task_kind"),
            "scope": scope,
            "task_dir": str(task_dir),
            "pid": os.getpid(),
            "started_at": _now(),
            "last_progress_at": _now(),
        },
    )
    result: dict | None = None
    try:
        result = _run_task(task_dir, config)
    except Exception as exc:  # noqa: BLE001
        finalize_background_task_status(
            task_dir,
            scope=scope,
            task_kind=str(config.get("task_kind") or "").strip() or None,
            pid=os.getpid(),
            status="failed",
            error={"type": exc.__class__.__name__, "message": str(exc)},
        )
        raise
    else:
        finalize_background_task_status(
            task_dir,
            scope=scope,
            task_kind=str(config.get("task_kind") or "").strip() or None,
            pid=os.getpid(),
            status="completed",
            result=result,
        )
        return 0
    finally:
        current_status = str(load_background_task_status(task_dir).get("status") or "").strip().lower()
        if current_status in {"queued", "running"}:
            finalize_background_task_status(
                task_dir,
                scope=scope,
                task_kind=str(config.get("task_kind") or "").strip() or None,
                pid=os.getpid(),
                status="failed",
                error={
                    "type": "UncleanWorkerExit",
                    "message": "Background task exited before a final status was written.",
                },
                stale=True,
            )


if __name__ == "__main__":
    raise SystemExit(main())
