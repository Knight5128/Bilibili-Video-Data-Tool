from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types
import unittest


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
SPEC = importlib.util.spec_from_file_location("bvp_builder_custom_full_export_module", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
bvp_builder = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bvp_builder
SPEC.loader.exec_module(bvp_builder)


class BvpBuilderCustomFullExportTest(unittest.TestCase):
    def test_custom_seed_selection_includes_rankboard_token(self) -> None:
        selection = bvp_builder.CustomSeedSelection(
            include_daily_hot=True,
            weekly_weeks=3,
            include_column_top=False,
            include_rankboard=True,
        )

        self.assertEqual(["daily_hot", "weeklymustsee_3", "rankboard"], selection.active_tokens())

    def test_load_rankboard_boards_reads_csv_mapping(self) -> None:
        boards = bvp_builder._load_rankboard_boards()

        self.assertGreaterEqual(len(boards), 10)
        self.assertEqual(0, boards[0].rid)
        self.assertEqual("全站", boards[0].name)

    def test_extract_owner_mids_from_rows_deduplicates_and_collects_missing_bvids(self) -> None:
        owner_mids, missing_bvids = bvp_builder._extract_owner_mids_from_rows(
            [
                {"bvid": "BV1", "owner_mid": 101},
                {"bvid": "BV2", "owner_mid": "101"},
                {"bvid": "BV3", "owner_mid": None},
                {"bvid": "BV4", "owner_mid": ""},
            ]
        )

        self.assertEqual([101], owner_mids)
        self.assertEqual(["BV3", "BV4"], missing_bvids)


if __name__ == "__main__":
    unittest.main()
