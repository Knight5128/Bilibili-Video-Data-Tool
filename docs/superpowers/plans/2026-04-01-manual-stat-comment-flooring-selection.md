# Manual Stat Comment Flooring Selection Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 让 `手动批量抓取-动态数据` 不再自动绑定最新 `daily_hot`/`rankboard`，而是由用户手动多选 `full_site_floorings` 下任意视频列表 CSV，并与所选 `uid_expansion` 视频列表合并后统一筛窗、去重、抓取。

**Architecture:** 保持现有 `manual_batch_runner` 负责“发现来源 CSV -> 合并 -> 时间窗口筛选 -> 去重 -> 导出 filtered CSV -> 触发 crawl”的主流程，只把 full-site 来源从“固定取最新两个文件”改成“显式接收用户选择的 flooring 文件名列表”。`bilibili-datahub.py` 负责列出可选 flooring 文件并把选择传给 runner，测试覆盖来源发现、去重优先级与前端文案对应行为。

**Tech Stack:** Python, pandas, Streamlit, unittest

---

### Task 1: 先写 runner 失败测试

**Files:**
- Modify: `tests/test_manual_batch_runner.py`
- Modify: `src/bili_pipeline/datahub/manual_batch_runner.py`

**Step 1: Write the failing test**
- 为 `discover_manual_batch_source_csvs()` 增加“仅返回用户所选 flooring CSV + 所选 uid 任务 CSV”的测试。
- 为 `run_manual_realtime_batch_crawl()` 增加“未选择 flooring CSV 时直接报错”的测试。
- 为 `run_manual_realtime_batch_crawl()` 增加“可选择非 latest flooring 文件参与合并”的测试。

**Step 2: Run test to verify it fails**
- Run: `python -m pytest tests/test_manual_batch_runner.py -q`
- Expected: 旧实现仍会自动附加 latest `daily_hot` / `rankboard`，因此新增测试失败。

**Step 3: Write minimal implementation**
- 为 runner 增加 `selected_flooring_csvs` 参数。
- 将 flooring 来源发现改为严格使用用户显式选择，而非自动挑最新文件。

**Step 4: Run test to verify it passes**
- Run: `python -m pytest tests/test_manual_batch_runner.py -q`
- Expected: PASS

### Task 2: 更新 DataHub 前端

**Files:**
- Modify: `bilibili-datahub.py`

**Step 1: Update UI inputs**
- 列出 `outputs/video_pool/full_site_floorings/` 下可选视频列表 CSV。
- 将“自动附加最新榜单”的说明改成“手动多选 flooring CSV”。
- 在提交表单时把 `selected_flooring_csvs` 传给 `run_manual_realtime_batch_crawl()`。

**Step 2: Verify behavior**
- 至少确保代码路径与参数名一致，无明显未定义符号或旧文案残留。

### Task 3: 更新说明文档

**Files:**
- Modify: `Bilibili_Video_Data_Crawler.md`

**Step 1: Update usage docs**
- 修改“核心功能”“典型使用场景”“当前使用方式”中关于 `手动批量抓取-动态数据` 的描述。
- 明确说明该页现在由用户自行选择 `full_site_floorings` 视频列表与 `uid_expansion` 任务目录，再统一合并筛选。

### Task 4: 最终验证

**Files:**
- Modify: `tests/test_manual_batch_runner.py`
- Modify: `src/bili_pipeline/datahub/manual_batch_runner.py`
- Modify: `bilibili-datahub.py`
- Modify: `Bilibili_Video_Data_Crawler.md`

**Step 1: Run focused verification**
- Run: `python -m pytest tests/test_manual_batch_runner.py -q`
- Run: `python -m pytest tests/test_realtime_watchlist_runner.py -q`

**Step 2: Run lints for changed files**
- 检查最近修改文件的诊断，确认没有新引入的错误。
