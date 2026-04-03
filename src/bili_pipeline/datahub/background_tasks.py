from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from bili_pipeline.crawl_api import DEFAULT_VIDEO_DATA_OUTPUT_DIR

from .shared import build_credential_from_cookie


DEFAULT_BACKGROUND_TASKS_ROOT = DEFAULT_VIDEO_DATA_OUTPUT_DIR / "background_tasks"
DEFAULT_COOKIE_PATH = Path(".local") / "bilibili-datahub.cookie.txt"
STATUS_FILENAME = "task_status.json"
CONFIG_FILENAME = "task_config.json"
RESULT_FILENAME = "task_result.json"


@dataclass(slots=True)
class BatchedCrawlOutcome:
    reports: list[Any]
    credential_refresh_count: int


def load_cookie_text(path: Path | str | None = None) -> str:
    target = Path(path or DEFAULT_COOKIE_PATH)
    if not target.exists():
        return ""
    return target.read_text(encoding="utf-8").strip()


def save_cookie_text(cookie_text: str, *, path: Path | str | None = None) -> Path:
    target = Path(path or DEFAULT_COOKIE_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text((cookie_text or "").strip(), encoding="utf-8")
    return target


def load_credential_from_cookie_file(path: Path | str | None = None):
    return build_credential_from_cookie(load_cookie_text(path))


def create_background_task_dir(
    task_kind: str,
    *,
    root_dir: Path | str | None = None,
    started_at: datetime | None = None,
) -> Path:
    root = Path(root_dir or DEFAULT_BACKGROUND_TASKS_ROOT)
    root.mkdir(parents=True, exist_ok=True)
    token = (started_at or datetime.now()).strftime("%Y%m%d_%H%M%S")
    candidate = root / f"{task_kind}_{token}"
    suffix = 2
    while candidate.exists():
        candidate = root / f"{task_kind}_{token}_{suffix}"
        suffix += 1
    candidate.mkdir(parents=True, exist_ok=True)
    return candidate


def update_background_task_status(task_dir: Path | str, payload: dict[str, Any]) -> Path:
    target = Path(task_dir) / STATUS_FILENAME
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target


def write_background_task_result(task_dir: Path | str, payload: Any) -> Path:
    target = Path(task_dir) / RESULT_FILENAME
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target


def load_background_task_result(task_dir: Path | str) -> Any:
    target = Path(task_dir) / RESULT_FILENAME
    if not target.exists():
        return {}
    return json.loads(target.read_text(encoding="utf-8"))


def _is_json_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _summarize_background_task_result(value: Any, *, depth: int = 0) -> Any:
    if _is_json_scalar(value):
        return value
    if isinstance(value, list):
        if not value:
            return []
        if all(_is_json_scalar(item) for item in value) and len(value) <= 20:
            return list(value)
        return {"type": "omitted_list", "count": len(value)}
    if isinstance(value, dict):
        summary: dict[str, Any] = {}
        omitted_fields: dict[str, Any] = {}
        for key, item in value.items():
            if _is_json_scalar(item):
                summary[key] = item
            elif isinstance(item, list):
                if not item:
                    summary[key] = []
                elif all(_is_json_scalar(entry) for entry in item) and len(item) <= 20:
                    summary[key] = list(item)
                else:
                    omitted_fields[key] = {"type": "list", "count": len(item)}
            elif isinstance(item, dict):
                if depth >= 2:
                    omitted_fields[key] = {"type": "dict", "count": len(item)}
                else:
                    summary[key] = _summarize_background_task_result(item, depth=depth + 1)
            else:
                summary[key] = str(item)
        if omitted_fields:
            summary["omitted_fields"] = omitted_fields
        return summary
    return str(value)


def _externalize_background_task_result(task_dir: Path | str, payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    working = dict(payload)
    if "result" not in working:
        return working, False
    result = working.get("result")
    if result is None:
        return working, False
    existing_result_path = str(working.get("result_path") or "").strip()
    if existing_result_path:
        return working, False
    result_path = write_background_task_result(task_dir, result)
    working["result_path"] = str(result_path)
    working["result"] = _summarize_background_task_result(result)
    return working, True


def load_background_task_status(task_dir: Path | str) -> dict[str, Any]:
    target = Path(task_dir) / STATUS_FILENAME
    if not target.exists():
        return {}
    payload = json.loads(target.read_text(encoding="utf-8"))
    compacted_payload, changed = _externalize_background_task_result(task_dir, payload)
    if changed:
        update_background_task_status(task_dir, compacted_payload)
    return compacted_payload


def write_background_task_config(task_dir: Path | str, payload: dict[str, Any]) -> Path:
    target = Path(task_dir) / CONFIG_FILENAME
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target


def load_background_task_config(task_dir: Path | str) -> dict[str, Any]:
    target = Path(task_dir) / CONFIG_FILENAME
    return json.loads(target.read_text(encoding="utf-8"))


def _load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _parse_iso_datetime(raw_value: Any) -> datetime | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _iter_manual_crawls_roots(task_dir: Path) -> list[Path]:
    config = _load_json_if_exists(task_dir / CONFIG_FILENAME)
    payload = dict(config.get("payload") or {})
    manual_crawls_root_text = str(payload.get("manual_crawls_root_dir") or "").strip()
    candidate_roots: list[Path] = []
    if manual_crawls_root_text:
        candidate_roots.append(Path(manual_crawls_root_text))
    fallback_root = task_dir.parent.parent / "manual_crawls"
    if fallback_root not in candidate_roots:
        candidate_roots.append(fallback_root)
    return candidate_roots


def _find_manual_dynamic_session_dir(task_dir: Path) -> Path | None:
    candidate_roots = _iter_manual_crawls_roots(task_dir)
    task_status = load_background_task_status(task_dir)
    task_started_at = _parse_iso_datetime(task_status.get("started_at"))
    best_match: tuple[float, Path] | None = None
    for manual_crawls_root in candidate_roots:
        if not manual_crawls_root.exists():
            continue
        for session_dir in manual_crawls_root.glob("manual_crawl_stat_comment_*"):
            if not session_dir.is_dir():
                continue
            session_state = _load_json_if_exists(session_dir / "manual_crawl_state.json")
            if not session_state:
                continue
            session_started_at = _parse_iso_datetime(session_state.get("started_at"))
            if task_started_at is None or session_started_at is None:
                continue
            delta_seconds = abs((session_started_at - task_started_at).total_seconds())
            if delta_seconds > 300:
                continue
            score = (delta_seconds, session_dir)
            if best_match is None or score[0] < best_match[0]:
                best_match = score
    return best_match[1] if best_match is not None else None


def _find_manual_media_session_dir(task_dir: Path, *, prefix: str, mode: str) -> Path | None:
    candidate_roots = _iter_manual_crawls_roots(task_dir)
    task_status = load_background_task_status(task_dir)
    task_started_at = _parse_iso_datetime(task_status.get("started_at"))
    best_match: tuple[datetime, float, Path] | None = None
    for manual_crawls_root in candidate_roots:
        if not manual_crawls_root.exists():
            continue
        for session_dir in manual_crawls_root.glob(f"{prefix}_*"):
            if not session_dir.is_dir():
                continue
            session_state = _load_json_if_exists(session_dir / "manual_crawl_media_state.json")
            if not session_state or str(session_state.get("mode") or "").strip().upper() != mode.upper():
                continue
            session_started_at = _parse_iso_datetime(session_state.get("started_at"))
            if task_started_at is None or session_started_at is None:
                continue
            delta_seconds = abs((session_started_at - task_started_at).total_seconds())
            if delta_seconds > 300:
                continue
            finished_at = _parse_iso_datetime(session_state.get("finished_at")) or session_started_at
            score = (finished_at, -delta_seconds, session_dir)
            if best_match is None or score[0] > best_match[0] or (score[0] == best_match[0] and score[1] > best_match[1]):
                best_match = score
    return best_match[2] if best_match is not None else None


def _recover_manual_dynamic_task_result(task_dir: Path) -> tuple[str, dict[str, Any], dict[str, Any]] | None:
    session_dir = _find_manual_dynamic_session_dir(task_dir)
    if session_dir is None:
        return None
    manual_state = _load_json_if_exists(session_dir / "manual_crawl_state.json")
    batch_state = _load_json_if_exists(session_dir / "batch_crawl_state.json")
    if bool(batch_state.get("completed_all")):
        return (
            "completed",
            {
                "status": "completed",
                "session_dir": str(session_dir),
                "session_completed_at": batch_state.get("session_completed_at"),
                "recovered_from_stale": True,
            },
            {
                "type": "RecoveredFromBatchState",
                "message": "Recovered completed status from batch crawl state after worker exited without final status.",
            },
        )
    if batch_state.get("session_completed_at"):
        return (
            "partial",
            {
                "status": "partial",
                "session_dir": str(session_dir),
                "session_completed_at": batch_state.get("session_completed_at"),
                "recovered_from_stale": True,
            },
            {
                "type": "RecoveredPartialFromBatchState",
                "message": "Recovered partial status from batch crawl state after worker exited without final status.",
            },
        )
    state_status = str(manual_state.get("status") or "").strip().lower()
    if state_status in {"completed", "partial", "skipped", "failed"}:
        return (
            state_status,
            {
                "status": state_status,
                "session_dir": str(session_dir),
                "recovered_from_stale": True,
            },
            {
                "type": "RecoveredFromManualState",
                "message": "Recovered task status from manual crawl state after worker exited without final status.",
            },
        )
    return None


def _recover_stale_background_task_result(task_dir: Path, task_kind: str) -> tuple[str, dict[str, Any], dict[str, Any]] | None:
    if task_kind == "manual_dynamic_batch":
        return _recover_manual_dynamic_task_result(task_dir)
    if task_kind == "manual_media_mode_a":
        session_dir = _find_manual_media_session_dir(task_dir, prefix="manual_crawl_media_mode_A", mode="A")
        if session_dir is None:
            return None
        state = _load_json_if_exists(session_dir / "manual_crawl_media_state.json")
        status = str(state.get("status") or "").strip().lower()
        if status in {"completed", "partial", "skipped", "failed"}:
            return (
                status,
                {
                    "status": status,
                    "mode": "A",
                    "session_dir": str(session_dir),
                    "finished_at": state.get("finished_at"),
                    "recovered_from_stale": True,
                },
                {
                    "type": "RecoveredFromManualMediaState",
                    "message": "Recovered media Mode A status from manual media state after worker exited without final status.",
                },
            )
        return None
    if task_kind == "manual_media_mode_b":
        session_dir = _find_manual_media_session_dir(task_dir, prefix="manual_crawl_media_mode_B", mode="B")
        if session_dir is None:
            return None
        state = _load_json_if_exists(session_dir / "manual_crawl_media_state.json")
        status = str(state.get("status") or "").strip().lower()
        if status in {"completed", "partial", "skipped", "failed"}:
            return (
                status,
                {
                    "status": status,
                    "mode": "B",
                    "session_dir": str(session_dir),
                    "finished_at": state.get("finished_at"),
                    "recovered_from_stale": True,
                },
                {
                    "type": "RecoveredFromManualMediaState",
                    "message": "Recovered media Mode B status from manual media state after worker exited without final status.",
                },
            )
        return None
    return None


def register_active_background_task(
    scope: str,
    *,
    task_dir: Path | str,
    registry_root: Path | str | None = None,
    pid: int | None = None,
) -> Path:
    root = Path(registry_root or DEFAULT_BACKGROUND_TASKS_ROOT)
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"active_{scope}.json"
    payload = {
        "scope": scope,
        "task_dir": str(Path(task_dir)),
        "pid": int(pid) if pid is not None else None,
        "updated_at": datetime.now().isoformat(),
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target


def load_active_background_task(scope: str, *, registry_root: Path | str | None = None) -> dict[str, Any] | None:
    target = Path(registry_root or DEFAULT_BACKGROUND_TASKS_ROOT) / f"active_{scope}.json"
    if not target.exists():
        return None
    return json.loads(target.read_text(encoding="utf-8"))


def is_background_task_process_running(pid: Any | None) -> bool:
    try:
        process_id = int(pid)
    except (TypeError, ValueError):
        return False
    if process_id <= 0:
        return False
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except SystemError:
        return False
    except OSError:
        return False
    return True


def finalize_background_task_status(
    task_dir: Path | str,
    *,
    scope: str,
    task_kind: str | None,
    pid: int | None,
    status: str,
    result: Any | None = None,
    error: dict[str, Any] | None = None,
    recovery: dict[str, Any] | None = None,
    stale: bool = False,
) -> Path:
    existing = load_background_task_status(task_dir)
    finished_at = datetime.now().isoformat()
    payload: dict[str, Any] = dict(existing)
    payload.update(
        {
            "status": status,
            "scope": scope,
            "task_kind": task_kind or existing.get("task_kind"),
            "task_dir": str(Path(task_dir)),
            "pid": int(pid) if pid is not None else existing.get("pid"),
            "finished_at": finished_at,
            "last_progress_at": finished_at,
        }
    )
    payload.pop("result", None)
    payload.pop("error", None)
    payload.pop("recovery", None)
    if result is not None:
        payload["result"] = result
    if error is not None:
        payload["error"] = error
    if recovery is not None:
        payload["recovery"] = recovery
    if stale:
        payload["stale"] = True
    elif "stale" in payload:
        payload.pop("stale", None)
    payload, _ = _externalize_background_task_result(task_dir, payload)
    return update_background_task_status(task_dir, payload)


def load_registered_background_task_status(
    scope: str,
    *,
    registry_root: Path | str | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    root = Path(registry_root or DEFAULT_BACKGROUND_TASKS_ROOT)
    registry_payload = load_active_background_task(scope, registry_root=root)
    if not registry_payload or not registry_payload.get("task_dir"):
        return registry_payload, None
    task_dir = Path(str(registry_payload["task_dir"]))
    if not task_dir.exists():
        fallback_task_dir = root / task_dir.name
        if not fallback_task_dir.exists():
            return registry_payload, None
        task_dir = fallback_task_dir
        register_active_background_task(
            scope,
            task_dir=task_dir,
            registry_root=root,
            pid=registry_payload.get("pid"),
        )
        registry_payload = load_active_background_task(scope, registry_root=root)
    status_payload = load_background_task_status(task_dir)
    if not status_payload:
        return registry_payload, None
    status = str(status_payload.get("status") or "").strip().lower()
    pid = status_payload.get("pid", registry_payload.get("pid"))
    task_kind = str(status_payload.get("task_kind") or registry_payload.get("task_kind") or "").strip()
    stale_recoverable_failure = (
        status == "failed"
        and bool(status_payload.get("stale"))
        and str((status_payload.get("error") or {}).get("type") or "").strip() in {"TaskProcessMissing", "UncleanWorkerExit"}
    )
    if (status in {"queued", "running"} or stale_recoverable_failure) and not is_background_task_process_running(pid):
        recovered = _recover_stale_background_task_result(task_dir, task_kind)
        if recovered is not None:
            recovered_status, recovered_result, recovered_recovery = recovered
            finalize_background_task_status(
                task_dir,
                scope=scope,
                task_kind=task_kind or None,
                pid=int(pid) if pid is not None else None,
                status=recovered_status,
                result=recovered_result,
                recovery=recovered_recovery,
                stale=True,
            )
            return registry_payload, load_background_task_status(task_dir)
        finalize_background_task_status(
            task_dir,
            scope=scope,
            task_kind=task_kind or None,
            pid=int(pid) if pid is not None else None,
            status="failed",
            error={
                "type": "TaskProcessMissing",
                "message": "Background task process is no longer running; marking stale task as failed.",
            },
            stale=True,
        )
        status_payload = load_background_task_status(task_dir)
    return registry_payload, status_payload


def background_task_is_running(scope: str, *, registry_root: Path | str | None = None) -> bool:
    _, status_payload = load_registered_background_task_status(scope, registry_root=registry_root)
    if not status_payload:
        return False
    status = status_payload.get("status")
    return status in {"queued", "running"}


def clear_active_background_task(scope: str, *, registry_root: Path | str | None = None, task_dir: Path | str | None = None) -> None:
    target = Path(registry_root or DEFAULT_BACKGROUND_TASKS_ROOT) / f"active_{scope}.json"
    if not target.exists():
        return
    if task_dir is not None:
        payload = json.loads(target.read_text(encoding="utf-8"))
        if str(payload.get("task_dir") or "") != str(Path(task_dir)):
            return
    target.unlink(missing_ok=True)


def _read_csv_rows(csv_path: Path | str) -> tuple[list[str], list[dict[str, str]]]:
    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [{key: (value if value is not None else "") for key, value in row.items()} for row in reader]
    return fieldnames, rows


def _write_csv_rows(fieldnames: list[str], rows: list[dict[str, str]], path: Path | str) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    return target


def build_background_worker_command(task_dir: Path | str) -> list[str]:
    project_root = Path(__file__).resolve().parents[3]
    worker_script = project_root / "scripts" / "datahub_background_worker.py"
    return [sys.executable, str(worker_script), "--task-dir", str(Path(task_dir))]


def launch_background_worker(task_dir: Path | str) -> int:
    task_path = Path(task_dir)
    task_path.mkdir(parents=True, exist_ok=True)
    stdout_path = task_path / "worker_stdout.log"
    stderr_path = task_path / "worker_stderr.log"
    command = build_background_worker_command(task_path)
    creationflags = 0
    if os.name == "nt":
        creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        creationflags |= getattr(subprocess, "DETACHED_PROCESS", 0)
    project_root = Path(__file__).resolve().parents[3]
    with stdout_path.open("a", encoding="utf-8") as stdout_handle, stderr_path.open("a", encoding="utf-8") as stderr_handle:
        process = subprocess.Popen(  # noqa: S603
            command,
            cwd=str(project_root),
            stdout=stdout_handle,
            stderr=stderr_handle,
            creationflags=creationflags,
        )
    return int(process.pid)


def run_batched_crawl_from_csv(
    csv_path: Path | str,
    *,
    batch_size: int,
    crawl_fn,
    credential_provider=None,
    credential: Any | None = None,
    should_retry_remaining_fn=None,
    max_remaining_retries_per_batch: int = 1,
    **crawl_kwargs,
) -> BatchedCrawlOutcome:
    fieldnames, rows = _read_csv_rows(csv_path)
    if not fieldnames:
        raise ValueError("CSV 缺少表头。")
    effective_batch_size = max(1, int(batch_size or 1))
    should_retry = should_retry_remaining_fn or (lambda _report: False)
    session_dir = Path(crawl_kwargs.get("session_dir") or Path(csv_path).parent)
    batches_dir = session_dir / "_background_batches"
    batches_dir.mkdir(parents=True, exist_ok=True)

    reports: list[Any] = []
    credential_refresh_count = 0

    for batch_index in range(0, len(rows), effective_batch_size):
        batch_rows = rows[batch_index : batch_index + effective_batch_size]
        batch_no = batch_index // effective_batch_size + 1
        current_csv_path = _write_csv_rows(fieldnames, batch_rows, batches_dir / f"batch_input_{batch_no}.csv")
        remaining_retry_count = 0
        while True:
            current_credential = credential_provider() if credential_provider is not None else credential
            if credential_provider is not None:
                credential_refresh_count += 1
            report = crawl_fn(
                current_csv_path,
                credential=current_credential,
                source_csv_name=Path(current_csv_path).name,
                **crawl_kwargs,
            )
            reports.append(report)
            remaining_csv_path = str(getattr(report, "remaining_csv_path", "") or "").strip()
            remaining_count = int(getattr(report, "remaining_count", 0) or 0)
            if bool(getattr(report, "completed_all", False)) or remaining_count == 0 or not remaining_csv_path:
                break
            if remaining_retry_count >= max(0, int(max_remaining_retries_per_batch)):
                break
            if not should_retry(report):
                break
            remaining_retry_count += 1
            current_csv_path = Path(remaining_csv_path)

    return BatchedCrawlOutcome(
        reports=reports,
        credential_refresh_count=credential_refresh_count,
    )
