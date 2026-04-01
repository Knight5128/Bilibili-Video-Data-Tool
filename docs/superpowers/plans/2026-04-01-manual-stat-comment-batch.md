# Manual Stat Comment Batch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `手动批量抓取-动态数据` watchlist-author workflow with a direct videolist-based stat/comment crawl flow that uses selected `uid_expansions` task directories plus the latest `daily_hot` and `rankboard` CSVs.

**Architecture:** Keep the UI thin inside `bilibili-datahub.py` and move source discovery, merging, window pruning, dedupe, and session-directory persistence into `manual_batch_runner.py`. Reuse the existing CSV-based `REALTIME_ONLY` crawl path so the behavior change stays focused on input assembly and session retention.

**Tech Stack:** Python, Streamlit, pandas, unittest

---

**Working directory for all commands below:** `D:\Schoolworks\Thesis\bilibili-data`

### Task 1: Extend manual batch source discovery

**Files:**
- Modify: `src/bili_pipeline/datahub/manual_batch_runner.py`
- Test: `tests/test_manual_batch_runner.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_discover_manual_batch_source_csvs_uses_selected_uid_directories_only():
    ...

def test_discover_manual_batch_source_csvs_uses_latest_flooring_files():
    ...

def test_discover_manual_batch_source_csvs_breaks_flooring_ties_by_desc_filename():
    ...
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_manual_batch_runner.ManualBatchRunnerTest`
Expected: FAIL because source discovery still scans all uid-expansion directories and all flooring CSVs.

- [ ] **Step 3: Write minimal implementation**

```python
def discover_manual_batch_source_csvs(..., selected_uid_task_dirs=None):
    # include only selected uid task directories
    # include latest daily_hot and latest rankboard by st_mtime
    # if st_mtime ties, pick descending filename for stability
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_manual_batch_runner.ManualBatchRunnerTest`
Expected: PASS for the new source discovery cases.

### Task 2: Update batch session naming and retained filtered CSV behavior

**Files:**
- Modify: `src/bili_pipeline/datahub/manual_batch_runner.py`
- Test: `tests/test_manual_batch_runner.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_run_manual_realtime_batch_crawl_uses_stat_comment_session_prefix():
    ...

def test_run_manual_realtime_batch_crawl_saves_filtered_video_list_before_crawl():
    ...

def test_run_manual_realtime_batch_crawl_applies_tracking_window():
    ...

def test_run_manual_realtime_batch_crawl_dedupes_by_spec_priority_order():
    ...

def test_run_manual_realtime_batch_crawl_counts_missing_pubdate_rows():
    ...

def test_run_manual_realtime_batch_crawl_preserves_remaining_csv_in_session_dir():
    ...
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_manual_batch_runner.ManualBatchRunnerTest`
Expected: FAIL because the session prefix is still `manual_crawl_...` or the retained CSV behavior is not asserted.

- [ ] **Step 3: Write minimal implementation**

```python
session_dir = manual_crawls_root / f"manual_crawl_stat_comment_{YYYYMMDD_HHMMSS}"
filtered_csv_path = session_dir / "filtered_video_list.csv"
# sort by parsed pubdate desc
# then source priority uid_expansions > daily_hot > rankboard
# then source_csv_path asc
# dedupe by bvid keeping first row
# apply tracking window before crawl export
# exclude missing pubdate from crawl input but count in result stats
# pass the same session_dir into crawl_bvid_list_from_csv so remaining_csv stays inside it
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_manual_batch_runner.ManualBatchRunnerTest`
Expected: PASS for naming and retained-CSV assertions.

### Task 3: Replace the DataHub manual dynamic page inputs and execution path

**Files:**
- Modify: `bilibili-datahub.py`

- [ ] **Step 1: Write the failing test or define observable behavior**

```python
# No page test harness exists here; verify behavior through runner API and
# narrow UI edits instead of introducing low-value Streamlit tests.
```

- [ ] **Step 2: Run the targeted runner tests first**

Run: `python -m unittest tests.test_manual_batch_runner.ManualBatchRunnerTest`
Expected: PASS, proving the backend contract is ready before UI wiring.

- [ ] **Step 3: Write minimal implementation**

```python
# remove author upload / watchlist state UI
# add multi-select for uid_expansion task directories
# restrict selectable directories to direct children of outputs/video_pool/uid_expansions
# show latest daily_hot / rankboard file paths
# block run when no uid_expansion directory is selected
# block run when latest daily_hot or rankboard is missing
# report readable error when selected directories contain no valid videolist_part_*.csv
# call run_manual_realtime_batch_crawl(..., selected_uid_task_dirs=...)
```

- [ ] **Step 4: Run the targeted tests again**

Run: `python -m unittest tests.test_manual_batch_runner.ManualBatchRunnerTest`
Expected: PASS unchanged.

### Task 4: Update crawler documentation

**Files:**
- Modify: `Bilibili_Video_Data_Crawler.md`

- [ ] **Step 1: Write the doc delta**

```markdown
- 手动批量抓取-动态数据：多选 uid_expansion 任务目录，自动附加最新 daily_hot / rankboard，整合去重后直接抓取评论/互动量。
```

- [ ] **Step 2: Review doc against new behavior**

Run: Manual read-through of `Bilibili_Video_Data_Crawler.md`
Expected: No remaining references that claim the page depends on local author CSVs or watchlist root state.

### Task 5: Verify end-to-end changed files

**Files:**
- Modify: `src/bili_pipeline/datahub/manual_batch_runner.py`
- Modify: `bilibili-datahub.py`
- Modify: `tests/test_manual_batch_runner.py`
- Modify: `Bilibili_Video_Data_Crawler.md`

- [ ] **Step 1: Run targeted tests**

Run: `python -m unittest tests.test_manual_batch_runner -v`
Expected: PASS

- [ ] **Step 2: Run lint diagnostics on edited files**

Run: Cursor lints for the modified files
Expected: No new lint errors.

- [ ] **Step 3: Spot-check session artifacts contract**

Run: inspect result fields / retained file naming via test assertions
Expected: session directories are named `manual_crawl_stat_comment_YYYYMMDD_HHMMSS` and store `filtered_video_list.csv`.

- [ ] **Step 4: Perform minimal manual UI verification**

Run: `streamlit run bilibili-datahub.py`, then inspect the `手动批量抓取-动态数据` page
Expected:
- a multi-select for `uid_expansion` task directories is visible
- the page shows the auto-selected latest `daily_hot` and `rankboard` files
- a run creates `outputs/video_data/manual_crawls/manual_crawl_stat_comment_*` with `filtered_video_list.csv` and state/log artifacts

- [ ] **Step 5: Verify empty/invalid input boundaries**

Run: exercise the runner or UI with missing flooring files, no selected uid directories, empty selected videolist set, and an all-outside-window candidate set
Expected:
- readable error for no selected uid directories
- readable error for missing latest `daily_hot` or `rankboard`
- readable error for selected directories with no valid `videolist_part_*.csv`
- skipped run still creates the session directory and retained filtered CSV when the post-window list is empty
