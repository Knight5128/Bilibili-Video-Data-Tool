from __future__ import annotations

import base64
import json
from datetime import datetime
from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

from bili_pipeline.cloud_tracker.admin import parse_owner_mid_upload
from bili_pipeline.crawl_api import (
    TEST_CRAWLS_OUTPUT_DIR,
    crawl_full_video_bundle,
    crawl_latest_comments,
    crawl_media_assets,
    crawl_stat_snapshot,
    crawl_video_meta,
    stream_media_to_store,
)
from bili_pipeline.datahub.config import (
    DEFAULT_AUTO_CONFIG,
    DEFAULT_GCP_CONFIG,
    LOCAL_AUTO_CONFIG_PATH,
    LOCAL_GCP_CONFIG_PATH,
    build_gcp_config,
)
from bili_pipeline.datahub.discover_ops import (
    BVID_TO_UIDS_OUTPUT_DIR,
    DEFAULT_UID_EXPANSION_START_DATE,
    DEFAULT_VIDEO_POOL_OUTPUT_DIR,
    FULL_SITE_FLOORINGS_LOGS_DIR,
    CustomSeedSelection,
    build_custom_seed_result,
    build_owner_since_overrides,
    build_rankboard_result,
    build_result_from_owner_mids_with_guardrails,
    coerce_end_datetime,
    coerce_start_datetime,
    decode_uploaded_text,
    display_path,
    drop_existing_uid_expansion_duplicates,
    extract_bvids,
    extract_failed_owner_mids_from_text,
    extract_owner_mids,
    extract_owner_mids_from_rows,
    load_full_export_tids,
    load_rankboard_boards,
    normalize_output_root,
    owner_mid_dataframe,
    prepare_uid_expansion_session,
    record_uid_expansion_part,
    save_owner_mid_csv,
    summarize_exception,
    write_uid_expansion_summary,
)
from bili_pipeline.datahub.local_cycle_runner import DataHubLocalCycleRunner
from bili_pipeline.datahub.manual_batch_runner import (
    DEFAULT_STREAM_DATA_TIME_WINDOW_HOURS,
    MANUAL_CRAWLS_OUTPUT_DIR,
    discover_manual_batch_source_csvs,
    run_manual_realtime_batch_crawl,
)
from bili_pipeline.datahub.shared import (
    append_live_log,
    build_credential_from_cookie,
    load_json_config,
    persist_live_log_snapshot,
    render_centered_header,
    save_json_config,
    save_timestamped_task_log,
)
from bili_pipeline.discover import resolve_owner_mids_from_bvids
from bili_pipeline.discover.export_csv import (
    discover_entries_to_full_export_rows,
    discover_entries_to_rows,
    export_discover_result_csv,
    export_rows_csv,
    rankboard_entries_to_rows,
)
from bili_pipeline.field_reference import (
    FIELD_REFERENCE_DOC_NAME,
    SECTION_TITLES,
    build_field_reference_rows,
    sync_field_reference_markdown,
)
from bili_pipeline.models import CrawlTaskMode, GCPStorageConfig, MediaDownloadStrategy
from bili_pipeline.storage import BigQueryCrawlerStore, GcsMediaStore
from bili_pipeline.utils.bilibili_jump import (
    build_owner_space_url,
    build_video_url,
    normalize_bvid,
    normalize_owner_mid,
    open_in_default_browser,
)
from bili_pipeline.utils.file_merge import (
    build_deduplicated_output_path,
    deduplicate_dataframe,
    export_dataframe,
    merge_dataframes,
    parse_comma_separated_keys,
    read_uploaded_dataframe,
    resolve_output_path,
    validate_columns,
)
from bili_pipeline.utils.streamlit_night_sky import render_night_sky_background


APP_DIR = Path(__file__).resolve().parent
LOGO_PATH = APP_DIR / "assets" / "logos" / "bvd-crawler.png"
FIELD_REFERENCE_DOC_PATH = APP_DIR / FIELD_REFERENCE_DOC_NAME


def _inject_field_reference_panel_styles() -> None:
    st.markdown(
        """
        <style>
        div[data-testid="column"]:has(#field-reference-sticky-anchor) {
            position: sticky;
            top: 1rem;
            align-self: flex-start;
        }

        div[data-testid="column"]:has(#field-reference-sticky-anchor) > div[data-testid="stVerticalBlock"] {
            max-height: calc(100vh - 2rem);
            overflow-y: auto;
            padding-right: 0.25rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _sync_field_reference_doc_once() -> None:
    if st.session_state.get("field_reference_doc_synced"):
        return
    sync_field_reference_markdown(FIELD_REFERENCE_DOC_PATH)
    st.session_state["field_reference_doc_synced"] = True


@st.cache_data(show_spinner=False)
def _get_field_reference_df() -> pd.DataFrame:
    return pd.DataFrame(build_field_reference_rows())


def _render_field_reference_panel(field_reference_df: pd.DataFrame) -> None:
    st.subheader("字段定义速查")
    st.caption("启动后常驻右侧，调试接口时可随时展开对照。")
    with st.expander("展开 / 收起字段定义", expanded=True):
        selected_section = st.selectbox(
            "选择要查看的数据模块",
            options=["全部"] + list(SECTION_TITLES.keys()),
            format_func=lambda item: "全部模块" if item == "全部" else SECTION_TITLES[item],
        )
        active_df = field_reference_df if selected_section == "全部" else field_reference_df[field_reference_df["数据模块"] == selected_section]
        st.dataframe(active_df, width="stretch", hide_index=True, height=360)


def _build_media_strategy(
    *,
    max_height: int,
    chunk_size_mb: int,
    gcp_config: GCPStorageConfig,
) -> MediaDownloadStrategy:
    return MediaDownloadStrategy(
        max_height=max_height,
        chunk_size_mb=chunk_size_mb,
        storage_backend="gcs",
        gcp_config=gcp_config,
    )


def _load_asset_bytes(*, asset_row: dict, strategy: MediaDownloadStrategy) -> bytes:
    if not strategy.use_gcs_media() or strategy.gcp_config is None:
        raise ValueError("当前未启用可用的 Google Cloud Storage 配置，无法回读远程媒体文件。")
    return GcsMediaStore(strategy.gcp_config).download_object_bytes(str(asset_row.get("object_key") or ""))


def _show_json(title: str, payload: dict) -> None:
    st.write(title)
    st.json(payload)


def _show_meta_result(result) -> None:
    _show_json("MetaResult", result.to_dict())
    with st.expander("查看标签详情", expanded=False):
        st.json(result.tag_details)
    with st.expander("查看视频权限对象", expanded=False):
        st.json(result.rights)
    with st.expander("查看上传用户资料", expanded=False):
        st.json(result.uploader_profile)
    with st.expander("查看上传用户关系快照", expanded=False):
        st.json(result.uploader_relation)
    with st.expander("查看上传用户空间概览", expanded=False):
        st.json(result.uploader_overview)


def _read_uploaded_files(uploaded_files) -> list[pd.DataFrame]:
    dfs: list[pd.DataFrame] = []
    for uploaded_file in uploaded_files:
        dfs.append(read_uploaded_dataframe(uploaded_file))
    return dfs


def _merge_and_deduplicate_by_column(dfs: list[pd.DataFrame], column_name: str) -> pd.DataFrame:
    for df in dfs:
        validate_columns(df, [column_name], column_name)
    merged = pd.concat(dfs, ignore_index=True)
    deduplicated, _ = deduplicate_dataframe(merged, dedupe_keys=[column_name])
    return deduplicated.reset_index(drop=True)


def _load_gcp_form_defaults() -> dict[str, str]:
    return load_json_config(LOCAL_GCP_CONFIG_PATH, DEFAULT_GCP_CONFIG)


def _load_auto_form_defaults() -> dict[str, object]:
    return load_json_config(LOCAL_AUTO_CONFIG_PATH, DEFAULT_AUTO_CONFIG)


def _build_default_log_dir(prefix: str, started_at: datetime) -> Path:
    return TEST_CRAWLS_OUTPUT_DIR / f"{prefix}_{started_at.strftime('%Y%m%d_%H%M%S')}"


page_config = {"page_title": "Bilibili DataHub", "layout": "wide"}
if LOGO_PATH.exists():
    page_config["page_icon"] = str(LOGO_PATH)
st.set_page_config(**page_config)
_inject_field_reference_panel_styles()
render_night_sky_background()
render_centered_header(st, "Bilibili DataHub", LOGO_PATH)
_sync_field_reference_doc_once()
st.markdown(
    "<p style='text-align: center; margin-bottom: 1rem;'>统一整合视频列表发现、批量抓取、接口调试、BigQuery/GCS 查看与本地自动批量抓取的一体化本地应用。</p>",
    unsafe_allow_html=True,
)

gcp_defaults = _load_gcp_form_defaults()
auto_defaults = _load_auto_form_defaults()

with st.sidebar:
    st.subheader("全局设置")
    comment_limit_default = st.number_input("默认评论条数", min_value=1, max_value=100, value=10, step=1)
    chunk_size_mb = st.number_input("媒体分块大小（MB）", min_value=1, max_value=64, value=4, step=1)
    max_height = st.number_input("媒体最大分辨率（高度）", min_value=360, max_value=2160, value=1080, step=120)
    consecutive_failure_limit = st.number_input("手动批量抓取连续失败暂停阈值", min_value=1, max_value=100, value=10, step=1)
    cookie_text = st.text_area("可选：B 站 Cookie（仅当前会话）", value="", height=80)
    st.divider()
    st.subheader("Google Cloud 配置")
    gcp_project_id = st.text_input("GCP Project ID", value=gcp_defaults["gcp_project_id"])
    bigquery_dataset = st.text_input("BigQuery Dataset", value=gcp_defaults["bigquery_dataset"])
    gcs_bucket_name = st.text_input("GCS Bucket 名称", value=gcp_defaults["gcs_bucket_name"])
    gcp_region = st.text_input("GCP Region（可选）", value=gcp_defaults["gcp_region"])
    credentials_path = st.text_input("服务账号 JSON 路径（可选）", value=gcp_defaults["credentials_path"])
    gcs_object_prefix = st.text_input("GCS 对象前缀", value=gcp_defaults["gcs_object_prefix"])
    gcs_public_base_url = st.text_input("公共访问基础 URL（可选）", value=gcp_defaults["gcs_public_base_url"])
    trigger_save_gcp_config = st.button("保存 GCP 配置", width="stretch")
    st.divider()
    st.subheader("自动批量抓取设置")
    auto_crawl_interval_hours = st.number_input("自动批量抓取触发间隔（小时）", min_value=1, max_value=24, value=int(auto_defaults["crawl_interval_hours"]), step=1)
    auto_tracking_window_days = st.number_input("作者追踪窗口（天）", min_value=1, max_value=90, value=int(auto_defaults["tracking_window_days"]), step=1)
    auto_comment_limit = st.number_input("自动流程评论条数", min_value=1, max_value=100, value=int(auto_defaults["comment_limit"]), step=1)
    auto_max_videos_per_cycle = st.number_input("单轮最大追踪视频数", min_value=1, max_value=5000, value=int(auto_defaults["max_videos_per_cycle"]), step=1)
    auto_request_pause_seconds = st.number_input("自动流程请求延迟（秒）", min_value=0.0, max_value=30.0, value=float(auto_defaults["request_pause_seconds"]), step=0.5)
    auto_risk_pause_minutes = st.number_input("风控暂停起始分钟数", min_value=1, max_value=720, value=int(auto_defaults["risk_pause_minutes"]), step=1)
    auto_risk_pause_max_minutes = st.number_input("风控暂停最大分钟数", min_value=1, max_value=1440, value=int(auto_defaults["risk_pause_max_minutes"]), step=1)
    auto_lock_ttl_minutes = st.number_input("运行锁超时分钟数", min_value=1, max_value=720, value=int(auto_defaults["lock_ttl_minutes"]), step=1)
    auto_author_overlap_minutes = st.number_input("作者增量重叠分钟数", min_value=0, max_value=1440, value=int(auto_defaults["author_overlap_minutes"]), step=30)
    auto_pending_parallelism = st.number_input("待补元数据/媒体抓取并发度", min_value=1, max_value=16, value=int(auto_defaults["pending_once_batch_parallelism"]), step=1)
    trigger_save_auto_config = st.button("保存自动批量抓取配置", width="stretch")

gcp_payload = {
    "gcp_project_id": gcp_project_id,
    "bigquery_dataset": bigquery_dataset,
    "gcs_bucket_name": gcs_bucket_name,
    "gcp_region": gcp_region,
    "credentials_path": credentials_path,
    "gcs_object_prefix": gcs_object_prefix,
    "gcs_public_base_url": gcs_public_base_url,
}
auto_payload = {
    **DEFAULT_AUTO_CONFIG,
    "crawl_interval_hours": int(auto_crawl_interval_hours),
    "tracking_window_days": int(auto_tracking_window_days),
    "comment_limit": int(auto_comment_limit),
    "max_videos_per_cycle": int(auto_max_videos_per_cycle),
    "request_pause_seconds": float(auto_request_pause_seconds),
    "risk_pause_minutes": int(auto_risk_pause_minutes),
    "risk_pause_max_minutes": int(auto_risk_pause_max_minutes),
    "lock_ttl_minutes": int(auto_lock_ttl_minutes),
    "author_overlap_minutes": int(auto_author_overlap_minutes),
    "pending_once_batch_parallelism": int(auto_pending_parallelism),
}

if trigger_save_gcp_config:
    saved_path = save_json_config(LOCAL_GCP_CONFIG_PATH, gcp_payload)
    st.sidebar.success(f"GCP 配置已保存：{saved_path}")
if trigger_save_auto_config:
    saved_path = save_json_config(LOCAL_AUTO_CONFIG_PATH, auto_payload)
    st.sidebar.success(f"自动批量抓取配置已保存：{saved_path}")

active_credential = build_credential_from_cookie(cookie_text)
active_gcp_config = build_gcp_config(gcp_payload)
active_media_strategy = _build_media_strategy(
    max_height=int(max_height),
    chunk_size_mb=int(chunk_size_mb),
    gcp_config=active_gcp_config,
)

store = None
store_init_error = None
if active_gcp_config.is_enabled():
    try:
        store = BigQueryCrawlerStore(active_gcp_config)
    except Exception as exc:  # noqa: BLE001
        store_init_error = str(exc)

local_runner = None
local_runner_error = None
if active_gcp_config.is_enabled():
    try:
        local_runner = DataHubLocalCycleRunner(
            gcp_config=active_gcp_config,
            auto_config=auto_payload,
            credential=active_credential,
        )
    except Exception as exc:  # noqa: BLE001
        local_runner_error = str(exc)

field_reference_df = _get_field_reference_df()
main_col, reference_col = st.columns([3.4, 1.6], gap="large")

with reference_col:
    st.markdown("<div id='field-reference-sticky-anchor'></div>", unsafe_allow_html=True)
    with st.container(border=True):
        _render_field_reference_panel(field_reference_df)

with main_col:
    tab_discover, tab_merge, tab_debug, tab_auto, tab_manual_batch, tab_db, tab_quick_jump, tab_tid = st.tabs(
        ["视频列表构建", "文件拼接及去重", "数据抓取调试", "自动批量抓取", "手动批量抓取", "BigQuery / GCS 数据查看", "快捷跳转", "tid 与分区名称对应"]
    )

with tab_discover:
    st.subheader("视频列表构建")
    full_export_tab, owner_expand_tab, bvid_lookup_tab, failed_uid_tab = st.tabs(
        ["自定义全量导出", "导出作者一段时间内上传视频列表", "BVID 回查作者 UID", "从日志提取失败作者 UID"]
    )

    with full_export_tab:
        full_daily_hot = st.checkbox("抓取当日全站热门（默认400条）", value=True)
        full_weekly_enabled = st.checkbox("抓取过去 n 期每周必看", value=True)
        full_weekly_weeks = st.number_input("n（每周必看期数）", min_value=1, max_value=520, value=12, step=1, disabled=not full_weekly_enabled)
        full_column_top = st.checkbox("抓取全部分区的当天主流视频", value=True)
        full_rankboard = st.checkbox("抓取实时排行榜（每个分区实时显示100条）", value=False)
        full_selection = CustomSeedSelection(
            include_daily_hot=bool(full_daily_hot),
            weekly_weeks=int(full_weekly_weeks) if full_weekly_enabled else 0,
            include_column_top=bool(full_column_top),
            include_rankboard=bool(full_rankboard),
        )
        full_default_name = (
            f"{'-'.join(full_selection.active_tokens())}-{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            if full_selection.active_tokens()
            else f"custom_full_export-{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        full_out_path = st.text_input(
            "输出视频列表 CSV 文件路径",
            value=str(DEFAULT_VIDEO_POOL_OUTPUT_DIR / "full_site_floorings" / full_default_name),
        )
        full_log_placeholder = st.empty()
        if st.button("开始执行自定义全量导出"):
            logs: list[str] = []

            def _log(message: str) -> None:
                append_live_log(logs, full_log_placeholder, message)

            if not full_selection.active_tokens():
                st.warning("请至少选择一种抓取来源。")
            else:
                try:
                    valid_tids = load_full_export_tids() if full_selection.include_column_top else []
                    rankboard_boards = load_rankboard_boards() if full_selection.include_rankboard else []
                    result = build_custom_seed_result(full_selection, valid_tids, logger=_log) if (full_selection.include_daily_hot or full_selection.weekly_weeks > 0 or full_selection.include_column_top) else None
                    rankboard_result = build_rankboard_result(rankboard_boards, logger=_log) if full_selection.include_rankboard else None
                    rows: list[dict] = []
                    if result is not None:
                        rows.extend(discover_entries_to_full_export_rows(result.entries))
                    if rankboard_result is not None:
                        rows.extend(rankboard_entries_to_rows(rankboard_result.entries))
                    saved = export_rows_csv(rows, full_out_path)
                    owner_mids, missing_bvids = extract_owner_mids_from_rows(rows)
                    if missing_bvids:
                        fallback_owner_mids, _failed_bvids = resolve_owner_mids_from_bvids(missing_bvids)
                        for owner_mid in fallback_owner_mids:
                            if owner_mid not in owner_mids:
                                owner_mids.append(owner_mid)
                    authors_saved = save_owner_mid_csv(owner_mids, saved.with_name(f"{saved.stem}_authors.csv"))
                except Exception as exc:  # noqa: BLE001
                    st.error(f"自定义全量导出失败：{exc}")
                else:
                    st.success(f"已导出视频列表：{saved}")
                    st.caption(f"作者 UID 列表：`{display_path(authors_saved, APP_DIR)}`")
                    st.dataframe(pd.DataFrame(rows).head(200), width="stretch", hide_index=True)
                finally:
                    log_path = save_timestamped_task_log("custom_full_export", logs, log_dir=FULL_SITE_FLOORINGS_LOGS_DIR)
                    if log_path is not None:
                        st.caption(f"运行日志：`{display_path(log_path, APP_DIR)}`")

    with owner_expand_tab:
        owner_files = st.file_uploader("上传保存作者 ID 的 CSV/XLSX 文件（可多选）", type=["csv", "xlsx", "xls"], accept_multiple_files=True)
        owner_column = st.text_input("作者 ID 所在列名", value="owner_mid")
        owner_start_date = st.date_input("start_date", value=DEFAULT_UID_EXPANSION_START_DATE)
        owner_end_date = st.date_input("end_date", value=datetime.now().date())
        owner_out_path = st.text_input("输出根目录（自动创建 uid_expansions/...）", value=str(DEFAULT_VIDEO_POOL_OUTPUT_DIR))
        owner_log_placeholder = st.empty()
        if st.button("开始导出作者一段时间内上传视频列表"):
            owner_logs: list[str] = []
            live_log_path: Path | None = None
            session = None
            requested_start_at = None
            requested_end_at = None
            owner_since_overrides = None
            reused_owner_count = 0
            owner_mids: list[int] = []
            task_started_at = None
            outcome = None
            saved = None
            remaining_saved = None
            summary_saved = None

            def _owner_log(message: str) -> None:
                append_live_log(owner_logs, owner_log_placeholder, message, mirror_path=live_log_path)

            try:
                if not owner_files:
                    raise ValueError("请先上传至少一个 CSV/XLSX 文件。")
                owner_dfs = _read_uploaded_files(owner_files)
                prepared_owner_df = _merge_and_deduplicate_by_column(owner_dfs, owner_column.strip())
                owner_mids, invalid_count = extract_owner_mids(prepared_owner_df, owner_column.strip())
                if invalid_count:
                    st.warning(f"检测到 {invalid_count} 条无法解析的作者 ID，已忽略。")
                if not owner_mids:
                    raise ValueError("未从上传文件中解析出有效作者 ID。")
                task_started_at = datetime.now().replace(microsecond=0)
                requested_start_at = coerce_start_datetime(owner_start_date)
                requested_end_at = coerce_end_datetime(owner_end_date, reference_now=task_started_at)
                output_root = normalize_output_root(owner_out_path)
                session = prepare_uid_expansion_session(
                    output_root,
                    [uploaded_file.name for uploaded_file in owner_files],
                    owner_mids,
                    requested_start_at,
                    requested_end_at,
                    task_started_at,
                    logger=_owner_log,
                )
                live_log_path = session.logs_dir / f"uid_expansion_part_{session.part_number}_running.log"
                persist_live_log_snapshot(owner_logs, live_log_path)
                owner_since_overrides, reused_owner_count = build_owner_since_overrides(
                    owner_mids,
                    output_root,
                    requested_start_at,
                    requested_end_at,
                    excluded_session_dirs=[session.session_dir],
                    logger=_owner_log,
                )
                if session.is_new_session:
                    session.session_dir.mkdir(parents=True, exist_ok=True)
                    save_owner_mid_csv(owner_mids, session.session_dir / "original_uids.csv")
                outcome = build_result_from_owner_mids_with_guardrails(
                    owner_mids,
                    requested_start_at,
                    requested_end_at,
                    owner_since_overrides=owner_since_overrides,
                    logger=_owner_log,
                )
                outcome.result, _removed_count = drop_existing_uid_expansion_duplicates(outcome.result, output_root, logger=_owner_log)
                saved = export_discover_result_csv(outcome.result, session.session_dir / f"videolist_part_{session.part_number}.csv")
                remaining_saved = None
                if outcome.remaining_owner_mids:
                    remaining_saved = save_owner_mid_csv(outcome.remaining_owner_mids, session.session_dir / f"remaining_uids_part_{session.part_number}.csv")
            except Exception as exc:  # noqa: BLE001
                _owner_log(f"[ERROR]: 作者批量抓取任务失败：{summarize_exception(exc)}")
                st.error(f"作者视频扩展失败：{exc}")
            else:
                st.success(f"已导出：{saved}")
                if remaining_saved is not None:
                    st.caption(f"剩余作者 UID：`{display_path(remaining_saved, APP_DIR)}`")
                if summary_saved is not None:
                    st.caption(f"任务总结：`{display_path(summary_saved, APP_DIR)}`")
                st.dataframe(pd.DataFrame(discover_entries_to_rows(outcome.result.entries)).head(200), width="stretch", hide_index=True)
            finally:
                if session is not None:
                    log_path = save_timestamped_task_log(f"uid_expansion_part_{session.part_number}", owner_logs, log_dir=session.logs_dir)
                    if live_log_path is not None:
                        live_log_path.unlink(missing_ok=True)
                    if log_path is not None:
                        st.caption(f"运行日志：`{display_path(log_path, APP_DIR)}`")
                    if log_path is not None and saved is not None and outcome is not None and requested_start_at is not None and requested_end_at is not None and owner_since_overrides is not None and task_started_at is not None:
                        state = record_uid_expansion_part(
                            session,
                            requested_start_at,
                            requested_end_at,
                            owner_since_overrides,
                            reused_owner_count,
                            len(owner_mids),
                            outcome,
                            saved,
                            remaining_saved,
                            log_path,
                            task_started_at.isoformat(timespec="seconds"),
                            datetime.now().isoformat(timespec="seconds"),
                        )
                        if not outcome.remaining_owner_mids:
                            summary_saved = write_uid_expansion_summary(session.session_dir, state)
                            st.caption(f"任务总结：`{display_path(summary_saved, APP_DIR)}`")

    with bvid_lookup_tab:
        bvid_files = st.file_uploader("上传保存 BVID 的 CSV/XLSX 文件（可多选）", type=["csv", "xlsx", "xls"], accept_multiple_files=True)
        bvid_column = st.text_input("BVID 所在列名", value="bvid")
        bvid_out_path = st.text_input("输出 CSV 文件路径", value=str(BVID_TO_UIDS_OUTPUT_DIR / f"bvid_to_uids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
        if st.button("开始 BVID 回查作者 UID"):
            try:
                if not bvid_files:
                    raise ValueError("请先上传文件。")
                bvid_dfs = _read_uploaded_files(bvid_files)
                prepared_bvid_df = _merge_and_deduplicate_by_column(bvid_dfs, bvid_column.strip())
                bvids = extract_bvids(prepared_bvid_df, bvid_column.strip())
                owner_mids, failed_bvids = resolve_owner_mids_from_bvids(bvids)
                saved = save_owner_mid_csv(owner_mids, Path(bvid_out_path))
            except Exception as exc:  # noqa: BLE001
                st.error(f"BVID 回查失败：{exc}")
            else:
                st.success(f"已导出：{saved}")
                if failed_bvids:
                    st.warning(f"有 {len(failed_bvids)} 个 BVID 未能解析作者。")
                st.dataframe(owner_mid_dataframe(owner_mids).head(200), width="stretch", hide_index=True)

    with failed_uid_tab:
        uploaded_logs = st.file_uploader("上传 .log/.txt 抓取日志文件（可多选）", type=["log", "txt"], accept_multiple_files=True)
        manual_log_text = st.text_area("或直接粘贴日志文本", value="", height=180)
        failed_uid_out_path = st.text_input("失败作者 UID 导出路径", value=str(DEFAULT_VIDEO_POOL_OUTPUT_DIR / f"failed_owner_mids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
        if st.button("提取失败作者 UID"):
            try:
                texts: list[str] = []
                for uploaded in uploaded_logs or []:
                    texts.append(decode_uploaded_text(uploaded.getvalue()))
                if manual_log_text.strip():
                    texts.append(manual_log_text)
                if not texts:
                    raise ValueError("请上传日志文件或粘贴日志文本。")
                owner_mids = sorted(set(owner_mid for text in texts for owner_mid in extract_failed_owner_mids_from_text(text)))
                saved = save_owner_mid_csv(owner_mids, Path(failed_uid_out_path))
            except Exception as exc:  # noqa: BLE001
                st.error(f"提取失败作者 UID 失败：{exc}")
            else:
                st.success(f"已导出：{saved}")
                st.dataframe(owner_mid_dataframe(owner_mids).head(200), width="stretch", hide_index=True)

with tab_merge:
    st.subheader("文件拼接及去重")
    uploaded_files = st.file_uploader("上传多个 CSV/XLSX 文件", type=["csv", "xlsx", "xls"], accept_multiple_files=True)
    sort_keys_raw = st.text_input("排序键（逗号分隔，留空则默认按 bvid 降序）", value="")
    dedupe_enabled = st.checkbox("对拼接结果去重", value=False)
    dedupe_keys_raw = st.text_input("去重键（逗号分隔）", value="bvid", disabled=not dedupe_enabled)
    keep_keys_raw = st.text_input("保留列（逗号分隔，可留空）", value="", disabled=not dedupe_enabled)
    merge_out_path = st.text_input("输出文件路径", value=str(DEFAULT_VIDEO_POOL_OUTPUT_DIR / f"merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
    if st.button("开始拼接及去重"):
        try:
            if not uploaded_files:
                raise ValueError("请先上传至少一个文件。")
            dfs = _read_uploaded_files(uploaded_files)
            sort_keys = parse_comma_separated_keys(sort_keys_raw)
            merged = merge_dataframes(dfs, sort_keys)
            saved = export_dataframe(merged, resolve_output_path(merge_out_path))
            dedup_saved = None
            if dedupe_enabled:
                dedupe_keys = parse_comma_separated_keys(dedupe_keys_raw)
                keep_keys = parse_comma_separated_keys(keep_keys_raw) or None
                deduplicated, _time_col = deduplicate_dataframe(merged, dedupe_keys=dedupe_keys, keep_keys=keep_keys)
                dedup_saved = export_dataframe(deduplicated, build_deduplicated_output_path(saved))
        except Exception as exc:  # noqa: BLE001
            st.error(f"拼接失败：{exc}")
        else:
            st.success(f"拼接结果已导出：{saved}")
            if dedup_saved is not None:
                st.caption(f"去重结果：`{display_path(dedup_saved, APP_DIR)}`")
            st.dataframe(merged.head(200), width="stretch", hide_index=True)

with tab_debug:
    st.subheader("数据抓取调试")
    single_tab, api_tab = st.tabs(["单 bvid 全流程", "四类接口调试"])

    with single_tab:
        if store_init_error:
            st.warning(f"当前 GCP 存储尚未就绪：{store_init_error}")
        col_a, col_b = st.columns([2, 1])
        with col_a:
            single_bvid = st.text_input("输入 bvid", value="", key="single_bvid")
        with col_b:
            enable_media_single = st.checkbox("抓取媒体", value=True, key="single_enable_media")
        if st.button("开始单视频顺序抓取"):
            if store is None:
                st.warning("请先完成 GCP 配置。")
            elif not single_bvid.strip():
                st.warning("请先输入 bvid。")
            else:
                try:
                    summary = crawl_full_video_bundle(
                        single_bvid.strip(),
                        enable_media=enable_media_single,
                        task_mode=CrawlTaskMode.FULL_BUNDLE,
                        comment_limit=int(comment_limit_default),
                        gcp_config=active_gcp_config,
                        max_height=int(max_height),
                        chunk_size_mb=int(chunk_size_mb),
                        media_strategy=active_media_strategy,
                        credential=active_credential,
                    )
                except Exception as exc:  # noqa: BLE001
                    st.error(f"抓取失败：{exc}")
                else:
                    if summary.errors:
                        st.warning("流程结束，但部分阶段失败。")
                    else:
                        st.success("单视频全流程抓取成功。")
                    _show_json("流程摘要", summary.to_dict())

    with api_tab:
        meta_tab, stat_tab, comment_tab, media_tab = st.tabs(["Meta", "Stat", "Comment", "Media"])
        with meta_tab:
            meta_bvid = st.text_input("Meta: bvid", key="meta_bvid")
            if st.button("调用 Meta 接口"):
                result = crawl_video_meta(meta_bvid.strip(), gcp_config=active_gcp_config, credential=active_credential)
                _show_meta_result(result)
        with stat_tab:
            stat_bvid = st.text_input("Stat: bvid", key="stat_bvid")
            if st.button("调用 Stat 接口"):
                result = crawl_stat_snapshot(stat_bvid.strip(), gcp_config=active_gcp_config, credential=active_credential)
                _show_json("StatSnapshot", result.to_dict())
        with comment_tab:
            comment_bvid = st.text_input("Comment: bvid", key="comment_bvid")
            comment_limit = st.number_input("Comment: limit", min_value=1, max_value=100, value=int(comment_limit_default), step=1)
            if st.button("调用 Comment 接口"):
                result = crawl_latest_comments(comment_bvid.strip(), limit=int(comment_limit), gcp_config=active_gcp_config, credential=active_credential)
                _show_json("CommentSnapshot", result.to_dict())
        with media_tab:
            media_bvid = st.text_input("Media: bvid", key="media_bvid")
            use_streaming_api = st.checkbox("使用 stream_media_to_store", value=True)
            if st.button("调用 Media 接口"):
                if use_streaming_api:
                    result = stream_media_to_store(media_bvid.strip(), strategy=active_media_strategy, credential=active_credential)
                else:
                    result = crawl_media_assets(
                        media_bvid.strip(),
                        strategy=active_media_strategy,
                        gcp_config=active_gcp_config,
                        max_height=int(max_height),
                        chunk_size_mb=int(chunk_size_mb),
                        credential=active_credential,
                    )
                _show_json("MediaResult", result.to_dict())

with tab_auto:
    st.subheader("自动批量抓取")
    if local_runner_error:
        st.warning(f"自动批量抓取尚未就绪：{local_runner_error}")
    author_upload = st.file_uploader("上传作者列表 CSV（必须包含 owner_mid 列）", type=["csv"], key="auto_author_upload")
    source_name = st.text_input("作者列表来源名称", value="selected_authors")
    upload_col, run_col, queue_col = st.columns(3)
    if upload_col.button("上传 / 替换作者列表", width="stretch"):
        try:
            if local_runner is None:
                raise ValueError("自动批量抓取尚未就绪。")
            if author_upload is None:
                raise ValueError("请先上传作者 CSV。")
            owner_mids = parse_owner_mid_upload(type("Upload", (), {"read": lambda self=None: author_upload.getvalue()})())
            saved_count = local_runner.replace_author_sources(
                owner_mids=owner_mids,
                source_name=source_name.strip() or "selected_authors",
                payload={"filename": author_upload.name, "uploaded_at": datetime.now().isoformat()},
            )
        except Exception as exc:  # noqa: BLE001
            st.error(f"上传作者列表失败：{exc}")
        else:
            st.success(f"已更新作者列表，共 {saved_count} 个作者。")
    if run_col.button("手动触发一轮自动批量抓取", width="stretch"):
        try:
            if local_runner is None:
                raise ValueError("自动批量抓取尚未就绪。")
            result = local_runner.run_cycle()
        except Exception as exc:  # noqa: BLE001
            st.error(f"自动批量抓取失败：{exc}")
        else:
            if result.tracker_report.get("status") == "success":
                st.success("自动批量抓取本轮执行成功。")
            else:
                st.warning(f"本轮状态：{result.tracker_report.get('status')}")
            st.json(result.to_dict())
    include_media_pending = st.checkbox("处理待补一次性数据时同步抓取媒体", value=True)
    pending_limit = st.number_input("本次最多处理多少条待补视频", min_value=1, max_value=10000, value=100, step=10)
    if queue_col.button("抓取待补元数据 / 媒体数据", width="stretch"):
        try:
            if local_runner is None:
                raise ValueError("自动批量抓取尚未就绪。")
            report = local_runner.crawl_pending_once_data(
                include_media=include_media_pending,
                limit=int(pending_limit),
                parallelism=int(auto_pending_parallelism),
                credential=active_credential,
                media_strategy=active_media_strategy,
                max_height=int(max_height),
                chunk_size_mb=int(chunk_size_mb),
            )
            if report is None:
                st.info("当前没有待补的一次性数据。")
            else:
                _show_json("待补任务报告", report.to_dict())
        except Exception as exc:  # noqa: BLE001
            st.error(f"处理待补数据失败：{exc}")

    if local_runner is not None:
        try:
            status_payload = local_runner.status()
        except Exception as exc:  # noqa: BLE001
            st.warning(f"读取自动批量抓取状态失败：{exc}")
        else:
            metrics = status_payload.get("metrics", {})
            st.caption(
                f"活跃作者数 {status_payload.get('author_source_count', 0)}，"
                f"待补元数据/媒体总数 {metrics.get('meta_media_queue_total', 0)}，"
                f"仍待处理 {metrics.get('meta_media_queue_pending', 0)}。"
            )
            with st.expander("最近运行记录", expanded=True):
                st.json(status_payload.get("recent_runs", []))
            with st.expander("当前作者源", expanded=False):
                st.dataframe(pd.DataFrame(status_payload.get("author_source_count") and local_runner.tracker_store.list_author_sources() or []), width="stretch", hide_index=True)
            with st.expander("待补元数据/媒体清单", expanded=False):
                st.dataframe(pd.DataFrame(status_payload.get("meta_media_queue_rows", [])).head(200), width="stretch", hide_index=True)

with tab_manual_batch:
    st.subheader("手动批量抓取")
    if store_init_error:
        st.warning(f"当前 GCP 存储尚未就绪：{store_init_error}")
    st.caption(
        "点击开始后，会自动汇总 `outputs/video_pool/full_site_floorings/` 下非 `_authors.csv` 的视频列表，"
        "以及 `outputs/video_pool/uid_expansions/` 目录及子目录下所有以 `videolist` 开头的 CSV，"
        "再按发布时间窗口筛选后，仅抓取实时互动量 / 评论数据并上传到 BigQuery。"
    )
    stream_data_time_window_hours = st.number_input(
        "STREAM_DATA_TIME_WINDOW（小时）",
        min_value=1,
        max_value=24 * 365,
        value=DEFAULT_STREAM_DATA_TIME_WINDOW_HOURS,
        step=24,
    )
    parallelism = st.number_input("并发度", min_value=1, max_value=16, value=2, step=1, key="manual_batch_parallelism")
    source_csv_paths = discover_manual_batch_source_csvs()
    st.caption(f"当前匹配到 {len(source_csv_paths)} 个源 CSV；任务目录将写入：`{MANUAL_CRAWLS_OUTPUT_DIR.as_posix()}`")
    with st.expander("查看本次会被纳入候选池的源 CSV", expanded=False):
        if source_csv_paths:
            st.dataframe(
                pd.DataFrame({"source_csv_path": [display_path(path, APP_DIR) for path in source_csv_paths]}),
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("暂未发现符合约定命名规则的视频列表 CSV。")
    if st.button("开始手动批量抓取"):
        try:
            if store is None:
                raise ValueError("请先完成 GCP 配置。")
            report = run_manual_realtime_batch_crawl(
                gcp_config=active_gcp_config,
                stream_data_time_window_hours=int(stream_data_time_window_hours),
                parallelism=int(parallelism),
                comment_limit=int(comment_limit_default),
                consecutive_failure_limit=int(consecutive_failure_limit),
                credential=active_credential,
                media_strategy=active_media_strategy,
                max_height=int(max_height),
                chunk_size_mb=int(chunk_size_mb),
            )
        except Exception as exc:  # noqa: BLE001
            st.error(f"手动批量抓取失败：{exc}")
        else:
            if report.status == "completed":
                st.success("手动批量抓取完成。")
            elif report.status == "partial":
                st.warning("手动批量抓取已结束，但仍有部分视频失败并已保留剩余清单。")
            else:
                st.info("本轮没有可抓取的视频，或已按条件跳过。")
            _show_json("手动批量抓取运行报告", report.to_dict())

with tab_db:
    st.subheader("BigQuery / GCS 数据查看")
    if store_init_error:
        st.warning(f"当前 GCP 存储尚未就绪：{store_init_error}")
    inspect_bvid = st.text_input("输入要查看的 bvid", value="")
    db_view_col, asset_view_col = st.columns(2)
    if db_view_col.button("查看该 bvid 的结构化数据", width="stretch"):
        try:
            if store is None:
                raise ValueError("请先完成 GCP 配置。")
            video_row = store.fetch_video_row(inspect_bvid.strip())
            stat_row = store.fetch_latest_stat_snapshot_row(inspect_bvid.strip())
            comment_row = store.fetch_latest_comment_snapshot_row(inspect_bvid.strip())
        except Exception as exc:  # noqa: BLE001
            st.error(f"读取结构化数据失败：{exc}")
        else:
            if video_row is not None:
                st.json(video_row)
            if stat_row is not None:
                st.json(stat_row)
            if comment_row is not None:
                st.json(comment_row)
    if asset_view_col.button("查看该 bvid 的媒体资产", width="stretch"):
        try:
            if store is None:
                raise ValueError("请先完成 GCP 配置。")
            asset_rows = store.fetch_all_asset_rows(inspect_bvid.strip())
        except Exception as exc:  # noqa: BLE001
            st.error(f"读取媒体资产失败：{exc}")
        else:
            st.dataframe(pd.DataFrame(asset_rows), width="stretch", hide_index=True)
            with st.expander("导出该 bvid 的媒体文件为本地文件", expanded=False):
                video_row = next((row for row in asset_rows if row.get("asset_type") == "video"), None)
                audio_row = next((row for row in asset_rows if row.get("asset_type") == "audio"), None)
                if video_row:
                    video_bytes = _load_asset_bytes(asset_row=video_row, strategy=active_media_strategy)
                    st.download_button("下载视频文件（.m4s）", data=video_bytes, file_name=f"{inspect_bvid.strip()}_video.m4s", mime="video/mp4")
                if audio_row:
                    audio_bytes = _load_asset_bytes(asset_row=audio_row, strategy=active_media_strategy)
                    st.download_button("下载音频文件（.m4s）", data=audio_bytes, file_name=f"{inspect_bvid.strip()}_audio.m4s", mime="audio/mp4")

with tab_quick_jump:
    st.subheader("快捷跳转")
    video_col, owner_col = st.columns(2)
    with video_col:
        quick_jump_bvid = st.text_input("视频 BVID", value="", key="quick_jump_bvid")
        if st.button("跳转到视频页", key="jump_video", width="stretch"):
            normalized_bvid = normalize_bvid(quick_jump_bvid)
            if normalized_bvid is None:
                st.warning("请输入有效的 BVID。")
            else:
                video_url = build_video_url(normalized_bvid)
                if open_in_default_browser(video_url):
                    st.success(f"已打开视频页：{video_url}")
    with owner_col:
        quick_jump_owner_mid = st.text_input("作者 ID", value="", key="quick_jump_owner_mid")
        if st.button("跳转到作者主页", key="jump_owner", width="stretch"):
            normalized_owner_mid = normalize_owner_mid(quick_jump_owner_mid)
            if normalized_owner_mid is None:
                st.warning("请输入有效的作者 ID。")
            else:
                owner_url = build_owner_space_url(normalized_owner_mid)
                if open_in_default_browser(owner_url):
                    st.success(f"已打开作者主页：{owner_url}")

with tab_tid:
    from bili_pipeline.bilibili_zones import find_by_tid, list_zones, unique_mains

    st.subheader("tid 与分区名称对应")
    col_a, col_b = st.columns([1, 2])
    with col_a:
        q_tid = st.number_input("按 tid 精确查询", min_value=1, max_value=999999, value=17, step=1)
    with col_b:
        q_text = st.text_input("关键字搜索（主分区/名称/代号）", value="")
    matches = find_by_tid(int(q_tid))
    if matches:
        st.dataframe([{"tid": z.tid, "主分区": z.main, "分区名称": z.name, "代号": z.code, "url路由": z.route} for z in matches], width="stretch", hide_index=True)
    zones = list(list_zones())
    mains = ["全部"] + unique_mains(zones)
    sel_main = st.selectbox("按主分区筛选", options=mains, index=0)

    def _hit(z) -> bool:
        if sel_main != "全部" and z.main != sel_main:
            return False
        if not q_text.strip():
            return True
        target = q_text.strip().lower()
        return target in z.main.lower() or target in z.name.lower() or target in z.code.lower()

    filtered = [z for z in zones if _hit(z)]
    st.dataframe([{"tid": z.tid, "主分区": z.main, "分区名称": z.name, "代号": z.code, "url路由": z.route} for z in filtered], width="stretch", hide_index=True)
