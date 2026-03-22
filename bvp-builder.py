from __future__ import annotations

import base64
from datetime import datetime
from html import escape
from pathlib import Path
import time

import pandas as pd
import streamlit as st

from bili_pipeline.bilibili_zones import find_by_tid, list_zones, unique_mains
from bili_pipeline.config import DiscoverConfig
from bili_pipeline.discover import (
    BilibiliUserRecentVideoSource,
    VideoPoolBuilder,
    build_full_site_result,
    load_valid_partition_tids,
    resolve_owner_mids_from_bvids,
)
from bili_pipeline.discover.export_csv import discover_entries_to_rows, export_discover_result_csv
from bili_pipeline.discover.real_demo import build_real_result
from bili_pipeline.models import DiscoverResult
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
from bili_pipeline.utils.bilibili_jump import (
    build_owner_space_url,
    build_video_url,
    normalize_bvid,
    normalize_owner_mid,
    open_in_default_browser,
)
from bili_pipeline.utils.streamlit_night_sky import render_night_sky_background


APP_DIR = Path(__file__).resolve().parent
LOGO_PATH = APP_DIR / "assets" / "logos" / "bvp-builder.png"
VALID_TAGS_PATH = APP_DIR / "all_valid_tags.csv"
CUSTOM_EXPORT_REQUEST_INTERVAL_SECONDS = 1.2
CUSTOM_EXPORT_REQUEST_JITTER_SECONDS = 0.8
CUSTOM_EXPORT_MAX_RETRIES = 4
CUSTOM_EXPORT_RETRY_BACKOFF_SECONDS = 5.0
CUSTOM_EXPORT_BATCH_SIZE = 20
CUSTOM_EXPORT_BATCH_PAUSE_SECONDS = 8.0


def _build_logo_data_uri(logo_path: Path) -> str | None:
    if not logo_path.exists():
        return None
    mime_types = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".svg": "image/svg+xml",
        ".ico": "image/x-icon",
    }
    mime_type = mime_types.get(logo_path.suffix.lower())
    if mime_type is None:
        return None
    encoded = base64.b64encode(logo_path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _render_centered_header(title: str, logo_path: Path) -> None:
    safe_title = escape(title)
    logo_uri = _build_logo_data_uri(logo_path)
    if logo_uri is None:
        st.markdown(f"<h1 style='text-align: center; margin-bottom: 0.25rem;'>{safe_title}</h1>", unsafe_allow_html=True)
        return
    st.markdown(
        f"""
        <div style="display: flex; justify-content: center; align-items: center; gap: 0.75rem; margin-bottom: 0.25rem;">
            <img src="{logo_uri}" alt="{safe_title} logo" style="height: 3.5rem; width: 3.5rem; object-fit: contain;" />
            <h1 style="margin: 0;">{safe_title}</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _preview_discover_result(result, limit: int = 200) -> None:
    preview = pd.DataFrame(discover_entries_to_rows(result.entries)).head(limit)
    if preview.empty:
        st.info("本次结果为空。")
        return
    st.dataframe(preview, width="stretch", hide_index=True)


def _read_uploaded_files(uploaded_files) -> list[pd.DataFrame]:
    dfs: list[pd.DataFrame] = []
    for uploaded_file in uploaded_files:
        try:
            dfs.append(read_uploaded_dataframe(uploaded_file))
        except Exception as e:  # noqa: BLE001
            st.error(f"读取文件失败：{uploaded_file.name}（{e}）")
            return []
    return dfs


def _merge_and_deduplicate_by_column(
    uploaded_files,
    dfs: list[pd.DataFrame],
    column_name: str,
    label: str,
) -> pd.DataFrame | None:
    normalized_column = column_name.strip()
    if not normalized_column:
        st.error(f"请填写{label}所在的列名。")
        return None

    for idx, df in enumerate(dfs):
        try:
            validate_columns(df, [normalized_column], label)
        except ValueError as e:
            st.error(f"第 {idx + 1} 个文件（{uploaded_files[idx].name}）校验失败：{e}")
            return None

    merged = pd.concat(dfs, ignore_index=True)
    deduplicated, _ = deduplicate_dataframe(merged, dedupe_keys=[normalized_column])
    return deduplicated.reset_index(drop=True)


def _extract_owner_mids(df: pd.DataFrame, column_name: str) -> tuple[list[int], int]:
    parsed = pd.to_numeric(df[column_name], errors="coerce")
    non_empty_mask = df[column_name].notna() & df[column_name].astype("string").str.strip().ne("")
    invalid_count = int((parsed.isna() & non_empty_mask).sum())
    owner_mids = [int(value) for value in parsed.dropna().tolist()]
    return list(dict.fromkeys(owner_mids)), invalid_count


def _extract_bvids(df: pd.DataFrame, column_name: str) -> list[str]:
    values = df[column_name].astype("string").fillna("").str.strip()
    bvids = [value for value in values.tolist() if value]
    return list(dict.fromkeys(bvids))


def _chunk_list(values: list[int], chunk_size: int) -> list[list[int]]:
    if chunk_size <= 0:
        return [values]
    return [values[start : start + chunk_size] for start in range(0, len(values), chunk_size)]


def _summarize_exception(exc: Exception, limit: int = 160) -> str:
    summary = " ".join(str(exc).split())
    if not summary:
        return exc.__class__.__name__
    if len(summary) <= limit:
        return summary
    return f"{summary[: limit - 3]}..."


def _build_result_from_owner_mids_with_guardrails(
    owner_mids: list[int],
    lookback_days: int,
    logger=None,
) -> tuple[DiscoverResult, list[int]]:
    normalized_owner_mids = sorted({int(owner_mid) for owner_mid in owner_mids})
    if not normalized_owner_mids:
        return DiscoverResult(entries=[], owner_mids=[]), []

    builder = VideoPoolBuilder(
        config=DiscoverConfig(lookback_days=lookback_days),
        hot_sources=[],
        partition_sources=[],
        author_source=BilibiliUserRecentVideoSource(
            page_size=30,
            max_pages=20,
            request_interval_seconds=CUSTOM_EXPORT_REQUEST_INTERVAL_SECONDS,
            request_jitter_seconds=CUSTOM_EXPORT_REQUEST_JITTER_SECONDS,
            max_retries=CUSTOM_EXPORT_MAX_RETRIES,
            retry_backoff_seconds=CUSTOM_EXPORT_RETRY_BACKOFF_SECONDS,
        ),
    )
    batches = _chunk_list(normalized_owner_mids, CUSTOM_EXPORT_BATCH_SIZE)
    merged_entries = {}
    failed_owner_mids: list[int] = []

    for batch_index, batch_owner_mids in enumerate(batches, start=1):
        batch_failed_owner_mids: list[int] = []
        if logger is not None:
            logger(
                f"[INFO]: 开始处理第 {batch_index}/{len(batches)} 批作者，共 {len(batch_owner_mids)} 个。"
            )

        def _on_batch_error(owner_mid: int, _index: int, _total: int, exc: Exception) -> None:
            batch_failed_owner_mids.append(owner_mid)
            if logger is not None:
                logger(f"[WARN]: 作者 {owner_mid} 抓取失败，已跳过。原因：{_summarize_exception(exc)}")

        batch_result = builder.build_from_owner_mids(
            batch_owner_mids,
            error_callback=_on_batch_error,
        )
        before_count = len(merged_entries)
        for entry in batch_result.entries:
            merged_entries.setdefault(entry.bvid, entry)

        failed_owner_mids.extend(batch_failed_owner_mids)
        if logger is not None:
            logger(
                f"[INFO]: 第 {batch_index}/{len(batches)} 批完成，新增 {len(merged_entries) - before_count} 条视频，累计 {len(merged_entries)} 条。"
            )
            if batch_failed_owner_mids:
                logger(f"[WARN]: 本批有 {len(batch_failed_owner_mids)} 个作者抓取失败。")
            if batch_index < len(batches):
                logger(f"[INFO]: 批次间暂停 {int(CUSTOM_EXPORT_BATCH_PAUSE_SECONDS)} 秒，降低请求频率。")
        if batch_index < len(batches):
            time.sleep(CUSTOM_EXPORT_BATCH_PAUSE_SECONDS)

    result = DiscoverResult(
        entries=sorted(merged_entries.values(), key=lambda item: item.discovered_at),
        owner_mids=normalized_owner_mids,
    )
    return result, list(dict.fromkeys(failed_owner_mids))


def _append_log(logs: list[str], placeholder, message: str) -> None:
    logs.append(message)
    placeholder.code("\n".join(logs), language=None)


def _load_full_export_tids() -> list[int]:
    return load_valid_partition_tids(VALID_TAGS_PATH)


page_config = {"page_title": "Bilibili Video Pool Builder", "layout": "centered"}
if LOGO_PATH.exists():
    page_config["page_icon"] = str(LOGO_PATH)
st.set_page_config(**page_config)
render_night_sky_background()
_render_centered_header("Bilibili Video Pool Builder", LOGO_PATH)

tab_full_export, tab_export, tab_tid, tab_custom_export, tab_merge, tab_quick_jump = st.tabs(
    ["全量导出视频列表", "按分区导出视频列表", "tid与分区名称对应", "自定义导出视频列表", "文件拼接及去重", "快捷跳转"]
)

with tab_full_export:
    st.subheader("全量导出视频列表")
    st.caption(
        "一键汇总全站热门榜单、过去若干周的每周必看，以及 `all_valid_tags.csv` 中全部有效分区在 "
        "`lookback_days` 内的投稿视频，并自动按 BVID 去重导出。"
    )

    valid_tid_count = None
    try:
        valid_tid_count = len(_load_full_export_tids())
    except Exception as e:  # noqa: BLE001
        st.error(f"读取有效分区列表失败：{e}")

    with st.form("full_export_params"):
        full_lookback_days = st.number_input("lookback_days", min_value=1, max_value=3650, value=90, step=1)
        default_full_name = f"full_site_video_pool_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        full_out_path = st.text_input("输出 CSV 文件路径", value=str(Path("outputs") / default_full_name))
        full_submitted = st.form_submit_button("开始全量抓取并导出")

    if valid_tid_count is not None:
        weeks_to_fetch = int(full_lookback_days) // 7 + 1
        st.caption(
            f"将抓取：1 份全站热门榜单 + 过去 {weeks_to_fetch} 周每周必看 + {valid_tid_count} 个有效分区的近期投稿。"
        )

    full_log_placeholder = st.empty()
    if full_submitted:
        full_logs: list[str] = []
        try:
            valid_tids = _load_full_export_tids()
        except Exception as e:  # noqa: BLE001
            st.error(f"读取有效分区列表失败：{e}")
        else:
            def _log(message: str) -> None:
                _append_log(full_logs, full_log_placeholder, message)

            with st.spinner("正在抓取全站视频并构建 video_pool..."):
                result = build_full_site_result(
                    lookback_days=int(full_lookback_days),
                    valid_tids=valid_tids,
                    logger=_log,
                )
                saved = export_discover_result_csv(result, full_out_path)

            _log(f"[INFO]: 导出完成：{saved}")
            st.success(f"已导出：{saved}")
            st.caption(f"共 {len(result.entries)} 条 entries（展示前 200 条预览）")
            _preview_discover_result(result)

with tab_export:
    with st.form("params"):
        tid = st.number_input("tid", min_value=1, max_value=999999, value=17, step=1)
        lookback_days = st.number_input("lookback_days", min_value=1, max_value=3650, value=90, step=1)
        default_name = f"video_pool_tid{int(tid)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        out_path = st.text_input("输出 CSV 文件路径", value=str(Path("outputs") / default_name))
        submitted = st.form_submit_button("开始构建并导出")

    if submitted:
        with st.spinner("正在拉取数据并构建 video_pool..."):
            result = build_real_result(tid=int(tid), lookback_days=int(lookback_days))
            saved = export_discover_result_csv(result, out_path)

        st.success(f"已导出：{saved}")
        st.caption(f"共 {len(result.entries)} 条 entries（展示前 200 条预览）")
        _preview_discover_result(result)

with tab_tid:
    st.subheader("tid 与分区名称对应")
    st.caption("数据来源：bilibili-API-collect 的「视频分区一览」页面（已固化到本项目，便于离线查阅）。")

    col_a, col_b = st.columns([1, 2])
    with col_a:
        q_tid = st.number_input("按 tid 精确查询", min_value=1, max_value=999999, value=17, step=1)
    with col_b:
        q_text = st.text_input("关键字搜索（主分区/名称/代号）", value="")

    matches = find_by_tid(int(q_tid))
    if matches:
        st.write("精确查询结果")
        st.dataframe(
            [
                {"tid": z.tid, "主分区": z.main, "分区名称": z.name, "代号": z.code, "url路由": z.route}
                for z in matches
            ],
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("未找到该 tid 的记录（可能是该页面未覆盖或已变更）。")

    zones = list(list_zones())
    mains = ["全部"] + unique_mains(zones)
    sel_main = st.selectbox("按主分区筛选", options=mains, index=0)

    def _hit(z) -> bool:
        if sel_main != "全部" and z.main != sel_main:
            return False
        if not q_text.strip():
            return True
        t = q_text.strip().lower()
        return t in z.main.lower() or t in z.name.lower() or t in z.code.lower()

    filtered = [z for z in zones if _hit(z)]
    st.write(f"共 {len(filtered)} 条（可在表格右上角下载）")
    st.dataframe(
        [{"tid": z.tid, "主分区": z.main, "分区名称": z.name, "代号": z.code, "url路由": z.route} for z in filtered],
        width="stretch",
        hide_index=True,
    )


with tab_custom_export:
    st.subheader("自定义导出视频列表")
    st.caption(
        "支持按作者 ID 列表，或按 BVID 列表反查作者后，批量导出这些作者在 lookback_days 内上传的全部视频。"
        " 当前已内置保守限速、指数退避重试与分批冷却，以降低长批量导出时的风控触发概率。"
    )

    tab_custom_owner, tab_custom_bvid = st.tabs(["按作者ID导出", "按BVID反查作者导出"])

    with tab_custom_owner:
        owner_files = st.file_uploader(
            "上传保存作者 ID 的 CSV/XLSX 文件（可多选）",
            type=["csv", "xlsx", "xls"],
            accept_multiple_files=True,
            key="custom_owner_files",
        )
        owner_column = st.text_input(
            "作者 ID 所在列名",
            value="owner_mid",
            key="custom_owner_column",
        )
        owner_lookback_days = st.number_input(
            "lookback_days",
            min_value=1,
            max_value=3650,
            value=90,
            step=1,
            key="custom_owner_lookback_days",
        )
        default_owner_name = f"custom_owner_video_pool_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        owner_out_path = st.text_input(
            "输出 CSV 文件路径",
            value=str(Path("outputs") / default_owner_name),
            key="custom_owner_out_path",
        )
        owner_log_placeholder = st.empty()

        if st.button("开始按作者 ID 抓取并导出", key="custom_owner_submit"):
            owner_logs: list[str] = []

            def _owner_log(message: str) -> None:
                _append_log(owner_logs, owner_log_placeholder, message)

            if not owner_files:
                st.warning("请先上传至少一个 CSV/XLSX 文件。")
            else:
                owner_dfs = _read_uploaded_files(owner_files)
                prepared_owner_df = None
                if owner_dfs:
                    prepared_owner_df = _merge_and_deduplicate_by_column(
                        owner_files,
                        owner_dfs,
                        owner_column,
                        "作者 ID 列",
                    )

                if prepared_owner_df is not None:
                    owner_mids, invalid_count = _extract_owner_mids(prepared_owner_df, owner_column.strip())
                    if invalid_count:
                        st.warning(f"检测到 {invalid_count} 条无法解析为整数的作者 ID，已自动忽略。")

                    if not owner_mids:
                        st.warning("未从上传文件中解析出有效的作者 ID。")
                    else:
                        _owner_log(f"[INFO]: 去重后共有 {len(owner_mids)} 个作者待抓取。")
                        with st.spinner("正在根据作者 ID 抓取视频并导出..."):
                            result, failed_owner_mids = _build_result_from_owner_mids_with_guardrails(
                                owner_mids,
                                int(owner_lookback_days),
                                logger=_owner_log,
                            )
                            saved = export_discover_result_csv(result, owner_out_path)

                        if failed_owner_mids:
                            preview_failed_owner_mids = ", ".join(str(owner_mid) for owner_mid in failed_owner_mids[:10])
                            if len(failed_owner_mids) > 10:
                                preview_failed_owner_mids += " ..."
                            st.warning(
                                f"有 {len(failed_owner_mids)} 个作者抓取失败并被跳过：{preview_failed_owner_mids}"
                            )
                        st.success(f"已导出：{saved}")
                        st.caption(
                            f"输入作者数 {len(owner_mids)}，导出视频数 {len(result.entries)}（展示前 200 条预览）"
                        )
                        _preview_discover_result(result)

    with tab_custom_bvid:
        bvid_files = st.file_uploader(
            "上传保存 BVID 的 CSV/XLSX 文件（可多选）",
            type=["csv", "xlsx", "xls"],
            accept_multiple_files=True,
            key="custom_bvid_files",
        )
        bvid_column = st.text_input(
            "BVID 所在列名",
            value="bvid",
            key="custom_bvid_column",
        )
        bvid_lookback_days = st.number_input(
            "lookback_days",
            min_value=1,
            max_value=3650,
            value=90,
            step=1,
            key="custom_bvid_lookback_days",
        )
        default_bvid_name = f"custom_bvid_video_pool_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        bvid_out_path = st.text_input(
            "输出 CSV 文件路径",
            value=str(Path("outputs") / default_bvid_name),
            key="custom_bvid_out_path",
        )
        bvid_log_placeholder = st.empty()

        if st.button("开始按 BVID 反查作者并导出", key="custom_bvid_submit"):
            bvid_logs: list[str] = []

            def _bvid_log(message: str) -> None:
                _append_log(bvid_logs, bvid_log_placeholder, message)

            if not bvid_files:
                st.warning("请先上传至少一个 CSV/XLSX 文件。")
            else:
                bvid_dfs = _read_uploaded_files(bvid_files)
                prepared_bvid_df = None
                if bvid_dfs:
                    prepared_bvid_df = _merge_and_deduplicate_by_column(
                        bvid_files,
                        bvid_dfs,
                        bvid_column,
                        "BVID 列",
                    )

                if prepared_bvid_df is not None:
                    bvids = _extract_bvids(prepared_bvid_df, bvid_column.strip())
                    if not bvids:
                        st.warning("未从上传文件中解析出有效的 BVID。")
                    else:
                        _bvid_log(f"[INFO]: 去重后共有 {len(bvids)} 个 BVID 待反查作者。")

                        def _on_bvid_progress(_bvid: str, index: int, total: int, _owner_mid: int | None) -> None:
                            if index == 1 or index == total or index % 20 == 0:
                                _bvid_log(f"[INFO]: BVID 反查进度 {index}/{total}。")

                        def _on_bvid_error(bvid: str, index: int, total: int, exc: Exception) -> None:
                            _bvid_log(
                                f"[WARN]: BVID 反查失败 {index}/{total}：{bvid}。原因：{_summarize_exception(exc)}"
                            )

                        with st.spinner("正在根据 BVID 反查作者并导出..."):
                            owner_mids, failed_bvids = resolve_owner_mids_from_bvids(
                                bvids,
                                request_interval_seconds=CUSTOM_EXPORT_REQUEST_INTERVAL_SECONDS,
                                request_jitter_seconds=CUSTOM_EXPORT_REQUEST_JITTER_SECONDS,
                                max_retries=CUSTOM_EXPORT_MAX_RETRIES,
                                retry_backoff_seconds=CUSTOM_EXPORT_RETRY_BACKOFF_SECONDS,
                                progress_callback=_on_bvid_progress,
                                error_callback=_on_bvid_error,
                            )

                            if owner_mids:
                                _bvid_log(f"[INFO]: 已解析出 {len(owner_mids)} 个唯一作者，开始抓取作者近期视频。")
                                result, failed_owner_mids = _build_result_from_owner_mids_with_guardrails(
                                    owner_mids,
                                    int(bvid_lookback_days),
                                    logger=_bvid_log,
                                )
                                saved = export_discover_result_csv(result, bvid_out_path)
                            else:
                                result = None
                                saved = None
                                failed_owner_mids = []

                        if failed_bvids:
                            preview_failed = ", ".join(failed_bvids[:10])
                            if len(failed_bvids) > 10:
                                preview_failed += " ..."
                            st.warning(f"有 {len(failed_bvids)} 个 BVID 未能解析出作者：{preview_failed}")
                        if failed_owner_mids:
                            preview_failed_owner_mids = ", ".join(str(owner_mid) for owner_mid in failed_owner_mids[:10])
                            if len(failed_owner_mids) > 10:
                                preview_failed_owner_mids += " ..."
                            st.warning(
                                f"有 {len(failed_owner_mids)} 个作者近期视频抓取失败并被跳过：{preview_failed_owner_mids}"
                            )

                        if not owner_mids or result is None or saved is None:
                            st.warning("未能根据上传的 BVID 解析出有效作者，因此没有导出结果。")
                        else:
                            st.success(f"已导出：{saved}")
                            st.caption(
                                "输入 BVID 数 "
                                f"{len(bvids)}，解析出作者数 {len(owner_mids)}，导出视频数 {len(result.entries)}"
                                "（展示前 200 条预览）"
                            )
                            _preview_discover_result(result)


with tab_merge:
    st.subheader("CSV/XLSX 文件拼接及去重")
    st.caption("上传多个本地 CSV/XLSX 文件，先拼接并导出；可选再基于指定键去重，并额外导出一份去重结果。")

    uploaded_files = st.file_uploader(
        "选择要拼接的文件（可多选）",
        type=["csv", "xlsx", "xls"],
        accept_multiple_files=True,
    )

    sort_keys_raw = st.text_input(
        "排序键（逗号分隔，可留空）",
        value="",
        help="当留空时，默认按主键 bvid 降序排序；当填写时，所有排序键必须在每个表格中都存在。",
    )

    enable_dedup = st.checkbox("对输出文件进行去重", value=False)
    dedupe_keys_raw = ""
    keep_keys_raw = ""
    if enable_dedup:
        dedupe_keys_raw = st.text_input(
            "去重键（逗号分隔，可留空）",
            value="",
            help="留空时默认按整行内容去重；去重时优先保留获取时间最晚的记录，若时间相同则保留拼接结果中排在前面的一条。",
        )
        keep_keys_raw = st.text_input(
            "保留键（逗号分隔，留空则默认保留全部键）",
            value="",
            help="仅影响去重后导出的附加文件；留空时保留全部列。",
        )

    default_merge_name = f"merged_video_pool_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    out_merge_path = st.text_input(
        "输出文件路径",
        value=str(Path("outputs") / default_merge_name),
    )

    do_merge = st.button("开始拼接并导出")

    if do_merge:
        if not uploaded_files:
            st.warning("请先上传至少一个 CSV/XLSX 文件。")
        else:
            dfs: list[pd.DataFrame] = []
            for f in uploaded_files:
                try:
                    df = read_uploaded_dataframe(f)
                except Exception as e:  # noqa: BLE001
                    st.error(f"读取文件失败：{f.name}（{e}）")
                    dfs = []
                    break
                dfs.append(df)

            if dfs:
                sort_keys = parse_comma_separated_keys(sort_keys_raw)
                for idx, df in enumerate(dfs):
                    try:
                        validate_columns(df, sort_keys, "排序键")
                    except ValueError as e:
                        st.error(f"第 {idx + 1} 个文件（{uploaded_files[idx].name}）校验失败：{e}")
                        dfs = []
                        break

            if dfs:
                try:
                    merged = merge_dataframes(dfs, sort_keys)
                    merged_out_path = resolve_output_path(out_merge_path)
                    export_dataframe(merged, merged_out_path)

                    st.success(f"已导出拼接文件：{merged_out_path}")
                    st.caption(f"拼接结果共 {len(merged)} 条记录（展示前 200 条预览）")
                    st.dataframe(merged.head(200), width="stretch", hide_index=True)

                    if enable_dedup:
                        dedupe_keys = parse_comma_separated_keys(dedupe_keys_raw)
                        keep_keys = parse_comma_separated_keys(keep_keys_raw)
                        deduplicated, dedupe_time_column = deduplicate_dataframe(
                            merged,
                            dedupe_keys=dedupe_keys,
                            keep_keys=keep_keys or None,
                        )
                        dedup_out_path = build_deduplicated_output_path(merged_out_path)
                        export_dataframe(deduplicated, dedup_out_path)

                        if dedupe_time_column is not None:
                            st.success(f"已导出去重文件：{dedup_out_path}（按 {dedupe_time_column} 保留最新记录）")
                        else:
                            st.success(f"已导出去重文件：{dedup_out_path}（未识别到获取时间列，重复项保留拼接结果中靠前的一条）")

                        st.caption(f"去重结果共 {len(deduplicated)} 条记录（展示前 200 条预览）")
                        st.dataframe(deduplicated.head(200), width="stretch", hide_index=True)
                except Exception as e:  # noqa: BLE001
                    st.error(f"拼接或导出失败：{e}")


with tab_quick_jump:
    st.subheader("快捷跳转")
    st.caption("输入单个 BVID 或作者 ID，点击后自动使用系统默认浏览器打开对应页面。")

    video_col, owner_col = st.columns(2)

    with video_col:
        quick_jump_bvid = st.text_input(
            "视频 BVID",
            value="",
            key="quick_jump_bvid",
            help="支持直接输入 BVID，也支持粘贴视频链接后自动提取。",
        )
        if st.button("跳转到视频页", key="quick_jump_bvid_submit", width="stretch"):
            normalized_bvid = normalize_bvid(quick_jump_bvid)
            if normalized_bvid is None:
                st.warning("请输入有效的 BVID。")
            else:
                video_url = build_video_url(normalized_bvid)
                if open_in_default_browser(video_url):
                    st.success(f"已打开视频页：{video_url}")
                else:
                    st.error("未能调用系统默认浏览器，请检查本机浏览器关联设置。")

    with owner_col:
        quick_jump_owner_mid = st.text_input(
            "作者 ID",
            value="",
            key="quick_jump_owner_mid",
            help="支持直接输入作者 ID，也支持粘贴作者主页链接后自动提取。",
        )
        if st.button("跳转到作者主页", key="quick_jump_owner_mid_submit", width="stretch"):
            normalized_owner_mid = normalize_owner_mid(quick_jump_owner_mid)
            if normalized_owner_mid is None:
                st.warning("请输入有效的作者 ID。")
            else:
                owner_url = build_owner_space_url(normalized_owner_mid)
                if open_in_default_browser(owner_url):
                    st.success(f"已打开作者主页：{owner_url}")
                else:
                    st.error("未能调用系统默认浏览器，请检查本机浏览器关联设置。")

