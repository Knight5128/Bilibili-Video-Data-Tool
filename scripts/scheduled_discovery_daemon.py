from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bili_pipeline.datahub.scheduled_discovery_runner import run_scheduled_discovery_cycle


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run daily hot + rankboard discovery and uid_expansion on a fixed interval."
    )
    parser.add_argument("--tracking_ups_path", required=True, help="Path to the CSV file containing owner_mid column.")
    parser.add_argument("--interval-hours", type=float, default=3.0, help="Interval between cycle starts.")
    parser.add_argument(
        "--uid-expansion-window-days",
        type=int,
        default=14,
        help="How many recent days of uploads to include in each uid_expansion cycle.",
    )
    parser.add_argument("--skip-initial-run", action="store_true", help="Wait until the first full interval before starting.")
    args = parser.parse_args()

    tracking_path = Path(args.tracking_ups_path).expanduser().resolve()
    interval = timedelta(hours=float(args.interval_hours))
    next_run_at = _now() + interval if args.skip_initial_run else _now()

    print(
        json.dumps(
            {
                "event": "scheduled_discovery_daemon_started",
                "tracking_ups_path": tracking_path.as_posix(),
                "interval_hours": float(args.interval_hours),
                "uid_expansion_window_days": int(args.uid_expansion_window_days),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    try:
        while True:
            now = _now()
            if now >= next_run_at:
                print(
                    json.dumps(
                        {
                            "event": "scheduled_discovery_cycle_started",
                            "timestamp": now.isoformat(timespec="seconds"),
                            "tracking_ups_path": tracking_path.as_posix(),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                try:
                    result = run_scheduled_discovery_cycle(
                        tracking_ups_path=tracking_path,
                        uid_expansion_window_days=int(args.uid_expansion_window_days),
                    )
                except Exception as exc:  # noqa: BLE001
                    print(
                        json.dumps(
                            {
                                "event": "scheduled_discovery_cycle_failed",
                                "timestamp": _now().isoformat(timespec="seconds"),
                                "error": str(exc),
                            },
                            ensure_ascii=False,
                        ),
                        flush=True,
                    )
                else:
                    print(
                        json.dumps(
                            {
                                "event": "scheduled_discovery_cycle_finished",
                                "result": result.to_dict(),
                            },
                            ensure_ascii=False,
                        ),
                        flush=True,
                    )
                next_run_at = now + interval
            time.sleep(30)
    except KeyboardInterrupt:
        print(json.dumps({"event": "scheduled_discovery_daemon_stopped"}, ensure_ascii=False), flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
