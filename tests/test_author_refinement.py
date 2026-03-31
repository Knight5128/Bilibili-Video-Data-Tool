from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from bili_pipeline.datahub.author_refinement import (
    AUTHOR_REFINEMENT_ORIGINAL_AUTHORS_FILENAME,
    AuthorMetadataBatchOutcome,
    build_followup_author_refinement_session,
    build_session_expanded_author_list,
    crawl_author_metadata_with_guardrails,
    merge_author_metadata,
    parse_priority_retention_rules,
    prepare_author_refinement_session,
    record_author_refinement_part,
    save_remaining_author_csv,
    sample_authors_with_priority_rules,
    stratified_sample_by_followers,
)


class AuthorRefinementTest(unittest.TestCase):
    def test_stratified_sample_uses_ceil_ratio_and_preserves_bin_proportions(self) -> None:
        df = pd.DataFrame(
            [
                {"owner_mid": 1, "owner_follower_count": 10},
                {"owner_mid": 2, "owner_follower_count": 11},
                {"owner_mid": 3, "owner_follower_count": 12},
                {"owner_mid": 4, "owner_follower_count": 13},
                {"owner_mid": 5, "owner_follower_count": 100},
                {"owner_mid": 6, "owner_follower_count": 110},
                {"owner_mid": 7, "owner_follower_count": 120},
                {"owner_mid": 8, "owner_follower_count": 130},
                {"owner_mid": 9, "owner_follower_count": 1000},
                {"owner_mid": 10, "owner_follower_count": 1100},
                {"owner_mid": 11, "owner_follower_count": 1200},
                {"owner_mid": 12, "owner_follower_count": 1300},
            ]
        )

        refined_full_df, sampled_df, bin_summary_df = stratified_sample_by_followers(
            df,
            sample_ratio=0.5,
            bin_count=3,
            random_state=7,
        )

        self.assertEqual(12, len(refined_full_df))
        self.assertEqual(6, len(sampled_df))
        self.assertTrue(refined_full_df["refinement_keep"].sum() == 6)
        self.assertEqual([2, 2, 2], bin_summary_df["sampled_count"].tolist())

    def test_stratified_sample_uses_ceil_for_non_integer_total_target(self) -> None:
        df = pd.DataFrame(
            [
                {"owner_mid": 1, "owner_follower_count": 10},
                {"owner_mid": 2, "owner_follower_count": 11},
                {"owner_mid": 3, "owner_follower_count": 100},
                {"owner_mid": 4, "owner_follower_count": 101},
                {"owner_mid": 5, "owner_follower_count": 1000},
            ]
        )

        refined_full_df, sampled_df, _bin_summary_df = stratified_sample_by_followers(
            df,
            sample_ratio=0.3,
            bin_count=3,
            random_state=7,
        )

        self.assertEqual(5, len(refined_full_df))
        self.assertEqual(2, len(sampled_df))
        self.assertTrue(refined_full_df["refinement_keep"].sum() == 2)

    def test_stratified_sample_zero_ratio_returns_empty_subset(self) -> None:
        df = pd.DataFrame(
            [
                {"owner_mid": 1, "owner_follower_count": 10},
                {"owner_mid": 2, "owner_follower_count": 20},
                {"owner_mid": 3, "owner_follower_count": 30},
            ]
        )

        refined_full_df, sampled_df, bin_summary_df = stratified_sample_by_followers(df, sample_ratio=0.0)

        self.assertEqual(0, len(sampled_df))
        self.assertFalse(refined_full_df["refinement_keep"].any())
        self.assertTrue((bin_summary_df["sampled_count"] == 0).all())

    def test_parse_priority_retention_rules_sorts_descending(self) -> None:
        rules = parse_priority_retention_rules("1000000,0.5\n5000000,1.0\n2000000,0.8")

        self.assertEqual([5000000, 2000000, 1000000], [rule.min_follower_count for rule in rules])
        self.assertEqual([1.0, 0.8, 0.5], [rule.retention_ratio for rule in rules])

    def test_priority_rules_keep_high_follower_segments_and_redistribute_remaining_slots(self) -> None:
        df = pd.DataFrame(
            [
                {"owner_mid": 1, "owner_follower_count": 6000000},
                {"owner_mid": 2, "owner_follower_count": 3000000},
                {"owner_mid": 3, "owner_follower_count": 2500000},
                {"owner_mid": 4, "owner_follower_count": 900000},
                {"owner_mid": 5, "owner_follower_count": 800000},
                {"owner_mid": 6, "owner_follower_count": 700000},
                {"owner_mid": 7, "owner_follower_count": 600000},
                {"owner_mid": 8, "owner_follower_count": 500000},
            ]
        )
        rules = parse_priority_retention_rules("5000000,1.0\n2000000,0.5")

        refined_full_df, sampled_df, _bin_summary_df, priority_summary_df = sample_authors_with_priority_rules(
            df,
            sample_ratio=0.5,
            priority_rules=rules,
            random_state=7,
        )

        self.assertEqual(8, len(refined_full_df))
        self.assertEqual(4, len(sampled_df))
        self.assertIn(1, sampled_df["owner_mid"].tolist())
        self.assertEqual(2, int(priority_summary_df["保留作者数"].sum()))
        self.assertTrue(refined_full_df["refinement_keep"].sum() == 4)

    def test_merge_author_metadata_supports_custom_owner_mid_column(self) -> None:
        source_df = pd.DataFrame(
            [
                {"up_mid": 1001, "note": "A"},
                {"up_mid": 1002, "note": "B"},
            ]
        )
        metadata_df = pd.DataFrame(
            [
                {"owner_mid": 1001, "owner_name": "Alpha", "owner_follower_count": 10},
                {"owner_mid": 1002, "owner_name": "Beta", "owner_follower_count": 20},
            ]
        )

        merged = merge_author_metadata(source_df, metadata_df, "up_mid")

        self.assertEqual(["Alpha", "Beta"], merged["owner_name"].tolist())
        self.assertEqual([10, 20], merged["owner_follower_count"].tolist())
        self.assertEqual(["A", "B"], merged["note"].tolist())

    @patch("bili_pipeline.datahub.author_refinement.crawl_author_metadata")
    def test_guardrail_stops_after_consecutive_risk_errors(self, mock_crawl) -> None:
        mock_crawl.side_effect = [
            {"owner_mid": 1, "owner_name": "A"},
            RuntimeError("状态码 412，触发风控"),
            RuntimeError("状态码 412，触发风控"),
        ]

        outcome = crawl_author_metadata_with_guardrails(
            [1, 2, 3, 4],
            consecutive_risk_limit=2,
            request_pause_seconds=0.0,
        )

        self.assertTrue(outcome.stopped_due_to_risk)
        self.assertEqual(1, outcome.successful_owner_count)
        self.assertEqual([3, 4], outcome.remaining_owner_mids)
        self.assertEqual(2, outcome.risk_failure_count)

    def test_prepare_session_reuses_existing_remaining_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir)
            session_dir = output_root / "author_refinement_20260330_120000"
            session_dir.mkdir(parents=True, exist_ok=True)
            save_remaining_author_csv([101, 202], session_dir / "remaining_authors_part_1.csv")

            session = prepare_author_refinement_session(
                output_root,
                "remaining_authors_part_1.csv",
                [101, 202],
                datetime(2026, 3, 30, 12, 30, 0),
            )

            self.assertFalse(session.is_new_session)
            self.assertEqual(session_dir, session.session_dir)
            self.assertEqual(2, session.part_number)

    def test_build_followup_session_advances_to_next_part(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            session_dir = Path(tmp_dir)
            pd.DataFrame([{"owner_mid": 1}]).to_csv(session_dir / "author_metadata_part_1.csv", index=False, encoding="utf-8-sig")
            save_remaining_author_csv([2, 3], session_dir / "remaining_authors_part_1.csv")

            followup = build_followup_author_refinement_session(session_dir)

            self.assertEqual(session_dir, followup.session_dir)
            self.assertEqual(2, followup.part_number)
            self.assertFalse(followup.is_new_session)

    def test_build_session_expanded_author_list_merges_parts_back_to_original(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            session_dir = Path(tmp_dir)
            export_original = pd.DataFrame(
                [
                    {"up_mid": 1001, "note": "A"},
                    {"up_mid": 1002, "note": "B"},
                ]
            )
            export_original.to_csv(session_dir / AUTHOR_REFINEMENT_ORIGINAL_AUTHORS_FILENAME, index=False, encoding="utf-8-sig")
            (session_dir / "author_refinement_state.json").write_text(
                '{"owner_mid_column": "up_mid", "parts": []}\n',
                encoding="utf-8",
            )
            pd.DataFrame(
                [
                    {"owner_mid": 1001, "owner_name": "Alpha", "owner_follower_count": 10},
                ]
            ).to_csv(session_dir / "author_metadata_part_1.csv", index=False, encoding="utf-8-sig")
            pd.DataFrame(
                [
                    {"owner_mid": 1002, "owner_name": "Beta", "owner_follower_count": 20},
                ]
            ).to_csv(session_dir / "author_metadata_part_2.csv", index=False, encoding="utf-8-sig")

            merged = build_session_expanded_author_list(session_dir)

            self.assertEqual(["Alpha", "Beta"], merged["owner_name"].tolist())
            self.assertEqual([10, 20], merged["owner_follower_count"].tolist())
            self.assertEqual(["A", "B"], merged["note"].tolist())

    def test_record_part_marks_completed_when_no_remaining_authors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            session_dir = Path(tmp_dir)
            logs_dir = session_dir / "logs"
            logs_dir.mkdir(parents=True, exist_ok=True)
            from bili_pipeline.datahub.author_refinement import AuthorRefinementSessionContext

            session = AuthorRefinementSessionContext(
                root_dir=session_dir.parent,
                session_dir=session_dir,
                logs_dir=logs_dir,
                part_number=1,
                is_new_session=True,
            )
            outcome = AuthorMetadataBatchOutcome(
                metadata_df=pd.DataFrame([{"owner_mid": 1, "owner_name": "A"}]),
                failures=[],
                remaining_owner_mids=[],
                successful_owner_count=1,
                processed_owner_count=1,
                risk_failure_count=0,
                stopped_due_to_risk=False,
                last_risk_error="",
            )

            state = record_author_refinement_part(
                session,
                owner_mid_column="owner_mid",
                source_name="authors.csv",
                input_owner_count=1,
                outcome=outcome,
                metadata_part_path=session_dir / "author_metadata_part_1.csv",
                accumulated_path=session_dir / "expanded_author_list_accumulated.csv",
                remaining_path=None,
                failures_path=None,
                log_path=None,
                run_started_at="2026-03-30T12:00:00",
                run_finished_at="2026-03-30T12:05:00",
            )

            self.assertTrue(state["completed_all"])


if __name__ == "__main__":
    unittest.main()
