# Manual Media/Meta Split And Truncation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将 `手动批量抓取-媒体/元数据` 拆分为独立的媒体抓取与元数据抓取任务，并为媒体抓取加入可选的前 N 秒截断与随时停止能力。

**Architecture:** 在保留现有后台任务、批量抓取骨架与 DataHub 页面结构的前提下，把模式 A / 模式 B 中“媒体”和“元数据”拆成独立 runner 和独立 UI 模块；将媒体截断与停止检查下沉到媒体下载层和后台任务层；将原先基于风控 sleep 的恢复方式改成与动态数据批量抓取一致的跳过式推进。

**Tech Stack:** Python, Streamlit, pandas, bilibili-api, aiohttp, BigQuery / GCS, unittest

---

### Task 1: 扩展测试以覆盖拆分后的 runner 语义

**Files:**
- Modify: `tests/test_manual_media_runner.py`

**Step 1: Write the failing test**

```python
def test_run_manual_media_mode_b_media_only_passes_truncate_seconds():
    ...

def test_run_manual_media_mode_b_meta_only_uses_meta_task_mode_without_media():
    ...

def test_run_manual_media_mode_a_stops_without_sleep_when_stop_requested():
    ...
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_manual_media_runner.py -q`
Expected: FAIL，因为当前 runner 还没有媒体/元数据拆分、截断参数与停止状态。

**Step 3: Write minimal implementation**

```python
# 为 runner 增加目标类型、截断参数与 stop checker 支持
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_manual_media_runner.py -q`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_manual_media_runner.py src/bili_pipeline/datahub/manual_media_runner.py
git commit -m "test: cover split manual media crawl modes"
```

### Task 2: 扩展抓取模式与待补清单查询能力

**Files:**
- Modify: `src/bili_pipeline/models/crawl.py`
- Modify: `src/bili_pipeline/crawl_api.py`
- Modify: `src/bili_pipeline/storage/gcp_store.py`
- Modify: `src/bili_pipeline/datahub/manual_media_runner.py`
- Test: `tests/test_manual_media_runner.py`

**Step 1: Write the failing test**

```python
def test_sync_manual_waitlist_can_target_meta_or_media():
    ...
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_manual_media_runner.py -q`
Expected: FAIL，因为当前只有合并版 waitlist 和 `ONCE_ONLY` 模式。

**Step 3: Write minimal implementation**

```python
# 新增只抓 meta / 只抓 media 的 task mode
# 拆分 BigQuery store 中的 completed 判断与 waitlist 导出接口
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_manual_media_runner.py -q`
Expected: PASS

**Step 5: Commit**

```bash
git add src/bili_pipeline/models/crawl.py src/bili_pipeline/crawl_api.py src/bili_pipeline/storage/gcp_store.py src/bili_pipeline/datahub/manual_media_runner.py tests/test_manual_media_runner.py
git commit -m "feat: split manual meta and media crawl runners"
```

### Task 3: 为媒体下载加入截断与协作式停止

**Files:**
- Modify: `src/bili_pipeline/models/crawl.py`
- Modify: `src/bili_pipeline/media/direct_stream.py`
- Modify: `scripts/datahub_background_worker.py`
- Modify: `src/bili_pipeline/datahub/background_tasks.py`
- Test: `tests/test_manual_media_runner.py`
- Test: `tests/test_datahub_background_tasks.py`

**Step 1: Write the failing test**

```python
def test_media_strategy_keeps_truncate_seconds_in_dict():
    ...

def test_stop_signal_marks_background_task_as_stopped():
    ...
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_manual_media_runner.py tests/test_datahub_background_tasks.py -q`
Expected: FAIL，因为当前既没有 truncate seconds，也没有 stop signal。

**Step 3: Write minimal implementation**

```python
# strategy 新增 truncate_seconds / stop checker
# 后台任务支持写入 stop 请求与 stopped 终态
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_manual_media_runner.py tests/test_datahub_background_tasks.py -q`
Expected: PASS

**Step 5: Commit**

```bash
git add src/bili_pipeline/models/crawl.py src/bili_pipeline/media/direct_stream.py src/bili_pipeline/datahub/background_tasks.py scripts/datahub_background_worker.py tests/test_manual_media_runner.py tests/test_datahub_background_tasks.py
git commit -m "feat: add truncation and stop control for manual media tasks"
```

### Task 4: 更新 DataHub 页面与后台任务配置

**Files:**
- Modify: `bilibili-datahub.py`
- Modify: `scripts/datahub_background_worker.py`
- Test: `tests/test_manual_media_runner.py`

**Step 1: Write the failing test**

```python
# 复用 runner 测试确保 UI 传参的目标字段与 truncate_seconds 进入任务配置
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_manual_media_runner.py -q`
Expected: FAIL，因为页面和后台 worker 还没有使用新的字段。

**Step 3: Write minimal implementation**

```python
# Streamlit 中拆分模式 A/B 的媒体与元数据模块，并加入停止按钮
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_manual_media_runner.py -q`
Expected: PASS

**Step 5: Commit**

```bash
git add bilibili-datahub.py scripts/datahub_background_worker.py
git commit -m "feat: split DataHub manual media and metadata panels"
```

### Task 5: 更新 README 与说明文档

**Files:**
- Modify: `README.md`
- Modify: `Bilibili_DataHub.md`

**Step 1: Write the failing test**

No automated test. Review docs against the implemented behavior.

**Step 2: Run verification**

Run: `python -m pytest tests/test_manual_media_runner.py tests/test_datahub_background_tasks.py -q`
Expected: PASS

**Step 3: Write minimal implementation**

```markdown
- 说明模式 A / B 已拆分为媒体与元数据任务
- 说明媒体抓取支持 `音视频截断长度`
- 说明后台任务支持随时停止
```

**Step 4: Run verification**

Run: `python -m pytest tests/test_manual_media_runner.py tests/test_datahub_background_tasks.py -q`
Expected: PASS

**Step 5: Commit**

```bash
git add README.md Bilibili_DataHub.md
git commit -m "docs: update manual media and metadata workflow"
```
