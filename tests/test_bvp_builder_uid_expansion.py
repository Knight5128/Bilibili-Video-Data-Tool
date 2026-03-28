from __future__ import annotations

from datetime import datetime
import importlib.util
from pathlib import Path
import sys
import tempfile
import types
import unittest

from bili_pipeline.models import DiscoverResult, VideoPoolEntry


class _FakeContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    def code(self, *_args, **_kwargs) -> None:
        return None


class _FakeStreamlit(types.ModuleType):
    def __init__(self) -> None:
        super().__init__("streamlit")
        self.session_state = {}
        self.sidebar = _FakeContext()

    def __getattr__(self, name: str):  # noqa: ANN204
        def _noop(*args, **kwargs):  # noqa: ANN002, ANN003
            if name == "tabs":
                labels = args[0] if args else []
                return [_FakeContext() for _ in labels]
            if name == "columns":
                spec = args[0] if args else 1
                count = spec if isinstance(spec, int) else len(spec)
                return [_FakeContext() for _ in range(count)]
            if name in {"container", "empty", "expander", "form", "spinner"}:
                return _FakeContext()
            if name in {"button", "form_submit_button"}:
                return False
            if name == "file_uploader":
                return None
            if name in {"text_input", "text_area"}:
                return kwargs.get("value", "")
            if name in {"number_input", "slider"}:
                return kwargs.get("value", 0)
            if name == "checkbox":
                return kwargs.get("value", False)
            if name == "date_input":
                return kwargs.get("value")
            if name == "selectbox":
                options = args[1] if len(args) > 1 else kwargs.get("options", [])
                return options[0] if options else None
            if name == "multiselect":
                return kwargs.get("default", [])
            return None

        return _noop


sys.modules.setdefault("streamlit", _FakeStreamlit())


MODULE_PATH = Path(__file__).resolve().parent.parent / "bvp-builder.py"
SPEC = importlib.util.spec_from_file_location("bvp_builder_module", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
bvp_builder = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bvp_builder
SPEC.loader.exec_module(bvp_builder)


class BvpBuilderUidExpansionTest(unittest.TestCase):
    def _write_original_uids(self, session_dir: Path, owner_mids: list[int]) -> None:
        session_dir.mkdir(parents=True, exist_ok=True)
        bvp_builder._save_owner_mid_csv(owner_mids, session_dir / bvp_builder.UID_EXPANSION_ORIGINAL_UIDS_FILENAME)

    def _write_state(self, session_dir: Path, **state: str) -> None:
        payload = dict(state)
        for key, value in list(payload.items()):
            if isinstance(value, datetime):
                payload[key] = value.isoformat()
        bvp_builder._save_uid_expansion_state(session_dir, payload)

    class _Placeholder:
        def __init__(self) -> None:
            self.rendered = ""

        def code(self, text: str, language=None) -> None:  # noqa: ANN001, ARG002
            self.rendered = text

    class _BrokenPlaceholder:
        def code(self, *_args, **_kwargs) -> None:
            raise RuntimeError("socket closed")

    def test_load_owner_history_task_starts_excludes_current_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            uid_expansions_root = root_dir / bvp_builder.UID_EXPANSION_DIRNAME
            previous_session = uid_expansions_root / "uid_expansion_previous"
            current_session = uid_expansions_root / "uid_expansion_current"

            self._write_original_uids(previous_session, [101, 202])
            self._write_state(previous_session, task_started_at=datetime(2026, 3, 1, 10, 0, 0))
            self._write_original_uids(current_session, [101])
            self._write_state(current_session, task_started_at=datetime(2026, 3, 20, 10, 0, 0))

            task_starts = bvp_builder._load_owner_history_task_starts(
                root_dir,
                excluded_session_dirs=[current_session],
            )

            self.assertEqual(datetime(2026, 3, 1, 10, 0, 0), task_starts[101])
            self.assertEqual(datetime(2026, 3, 1, 10, 0, 0), task_starts[202])

    def test_build_owner_since_overrides_uses_latest_history_task_day(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            uid_expansions_root = root_dir / bvp_builder.UID_EXPANSION_DIRNAME
            older_session = uid_expansions_root / "uid_expansion_20250923_20260322_080000"
            latest_session = uid_expansions_root / "uid_expansion_20250923_20260325_203500"

            self._write_original_uids(older_session, [101, 202])
            self._write_state(older_session, task_started_at=datetime(2026, 3, 22, 8, 0, 0))
            self._write_original_uids(latest_session, [101])
            self._write_state(latest_session, task_started_at=datetime(2026, 3, 25, 20, 35, 0))

            overrides, reused_owner_count = bvp_builder._build_owner_since_overrides(
                [101, 202, 303],
                root_dir,
                requested_start_at=datetime(2025, 9, 23, 0, 0, 0),
                requested_end_at=datetime(2026, 3, 30, 23, 59, 59),
            )

            self.assertEqual(2, reused_owner_count)
            self.assertEqual(datetime(2026, 3, 25, 0, 0, 0), overrides[101])
            self.assertEqual(datetime(2026, 3, 22, 0, 0, 0), overrides[202])
            self.assertNotIn(303, overrides)

    def test_resolve_uid_history_task_started_at_prefers_legacy_part_run_started_at(self) -> None:
        legacy_state = {
            "requested_window_start_at": "2025-09-23T00:00:00",
            "requested_window_end_at": "2026-03-25T16:27:23",
            "parts": [
                {"part_number": 3, "run_started_at": "20260324_233600"},
                {"part_number": 1, "run_started_at": "20260322_214916"},
            ],
            "updated_at": "2026-03-25T17:09:23",
        }

        task_started_at = bvp_builder._resolve_uid_history_task_started_at(
            legacy_state,
            Path("uid_expansion_180_days_20260322_214916"),
        )

        self.assertEqual(datetime(2026, 3, 22, 21, 49, 16), task_started_at)

    def test_resolve_uid_history_task_started_at_ignores_stale_late_task_started_at(self) -> None:
        legacy_state = {
            "task_started_at": "2026-03-25T18:57:26",
            "requested_window_start_at": "2025-09-23T00:00:00",
            "requested_window_end_at": "2026-03-26T20:33:36",
            "parts": [
                {"part_number": 7, "run_started_at": "2026-03-25T18:57:26"},
                {"part_number": 3, "run_started_at": "20260324_233600"},
                {"part_number": 1, "run_started_at": "20260322_214916"},
            ],
            "updated_at": "2026-03-26T20:41:27",
        }

        task_started_at = bvp_builder._resolve_uid_history_task_started_at(
            legacy_state,
            Path("uid_expansion_20250923_20260322_214916"),
        )

        self.assertEqual(datetime(2026, 3, 22, 21, 49, 16), task_started_at)

    def test_build_owner_since_overrides_uses_legacy_history_session_start_day(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            uid_expansions_root = root_dir / bvp_builder.UID_EXPANSION_DIRNAME
            legacy_session = uid_expansions_root / "uid_expansion_180_days_20260322_214916"

            self._write_original_uids(legacy_session, [101])
            bvp_builder._save_uid_expansion_state(
                legacy_session,
                {
                    "requested_window_start_at": "2025-09-23T00:00:00",
                    "requested_window_end_at": "2026-03-25T16:27:23",
                    "parts": [
                        {"part_number": 6, "run_started_at": "20260325_162723"},
                        {"part_number": 1, "run_started_at": "20260322_214916"},
                    ],
                    "updated_at": "2026-03-25T17:09:23",
                },
            )

            overrides, reused_owner_count = bvp_builder._build_owner_since_overrides(
                [101],
                root_dir,
                requested_start_at=datetime(2025, 9, 23, 0, 0, 0),
                requested_end_at=datetime(2026, 3, 25, 18, 33, 37),
            )

            self.assertEqual(1, reused_owner_count)
            self.assertEqual(datetime(2026, 3, 22, 0, 0, 0), overrides[101])

    def test_prepare_uid_expansion_session_uses_requested_start_and_task_started_at(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)

            session = bvp_builder._prepare_uid_expansion_session(
                root_dir,
                uploaded_file_names=["my_fav_ups.csv"],
                owner_mids=[101, 202],
                requested_start_at=datetime(2025, 9, 23, 0, 0, 0),
                requested_end_at=datetime(2026, 3, 25, 20, 35, 0),
                task_started_at=datetime(2026, 3, 25, 20, 35, 0),
            )

            self.assertTrue(session.is_new_session)
            self.assertEqual(
                "uid_expansion_20250923_20260325_203500",
                session.session_dir.name,
            )

    def test_prepare_uid_expansion_session_reuses_existing_remaining_uid_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            session_dir = (
                root_dir
                / bvp_builder.UID_EXPANSION_DIRNAME
                / "uid_expansion_20250923_20260325_203500"
            )
            session_dir.mkdir(parents=True, exist_ok=True)
            bvp_builder._save_owner_mid_csv([101, 202], session_dir / "remaining_uids_part_1.csv")

            session = bvp_builder._prepare_uid_expansion_session(
                root_dir,
                uploaded_file_names=["remaining_uids_part_1.csv"],
                owner_mids=[101, 202],
                requested_start_at=datetime(2025, 9, 23, 0, 0, 0),
                requested_end_at=datetime(2026, 3, 30, 9, 0, 0),
                task_started_at=datetime(2026, 3, 30, 9, 0, 0),
            )

            self.assertFalse(session.is_new_session)
            self.assertEqual(session_dir, session.session_dir)
            self.assertEqual(2, session.part_number)

    def test_drop_existing_uid_expansion_duplicates_filters_existing_bvids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            session_dir = root_dir / bvp_builder.UID_EXPANSION_DIRNAME / "uid_expansion_previous"
            session_dir.mkdir(parents=True, exist_ok=True)
            (session_dir / "videolist_part_1.csv").write_text(
                "bvid,owner_mid\nBV_DUPLICATED,101\nBV_OLD,202\n",
                encoding="utf-8",
            )
            result = DiscoverResult(
                entries=[
                    VideoPoolEntry(
                        bvid="BV_DUPLICATED",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=datetime(2026, 3, 25, 12, 0, 0),
                        last_seen_at=datetime(2026, 3, 25, 12, 0, 0),
                        owner_mid=101,
                    ),
                    VideoPoolEntry(
                        bvid="BV_NEW",
                        source_type="author_expand",
                        source_ref="owner:303",
                        discovered_at=datetime(2026, 3, 25, 12, 0, 0),
                        last_seen_at=datetime(2026, 3, 25, 12, 0, 0),
                        owner_mid=303,
                    ),
                ],
                owner_mids=[101, 303],
            )

            filtered_result, removed_count = bvp_builder._drop_existing_uid_expansion_duplicates(
                result,
                root_dir,
            )

            self.assertEqual(1, removed_count)
            self.assertEqual(["BV_NEW"], [entry.bvid for entry in filtered_result.entries])

    def test_save_task_logs_wraps_timestamp_markers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            logs: list[str] = []
            placeholder = self._Placeholder()

            bvp_builder._append_log(logs, placeholder, "[INFO]: first line")
            saved_path = bvp_builder._save_task_logs("unit_test", logs, log_dir=Path(tmp_dir))

            self.assertIsNotNone(saved_path)
            saved_text = saved_path.read_text(encoding="utf-8")
            self.assertTrue(saved_text.startswith("[TIMESTAMP][BEGIN] "))
            self.assertIn("[INFO]: first line", saved_text)
            self.assertIn("[TIMESTAMP][END] ", saved_text)
            self.assertIn("[TIMESTAMP][BEGIN] ", placeholder.rendered)

    def test_append_log_ignores_placeholder_failures_and_mirrors_to_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            logs: list[str] = []
            mirror_path = Path(tmp_dir) / "running.log"

            bvp_builder._append_log(
                logs,
                self._BrokenPlaceholder(),
                "[INFO]: keep running",
                mirror_path=mirror_path,
            )

            self.assertTrue(logs)
            self.assertTrue(mirror_path.exists())
            self.assertIn("[INFO]: keep running", mirror_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
