# 手动批量抓取-动态数据回归为 videolist 直抓方案

## 背景

当前 `手动批量抓取-动态数据` 页面走的是 `realtime_watchlist_runner` 流程：先读取并维护本地作者清单，再抓取热门、排行榜与作者近窗口新稿，然后才进入评论/互动量抓取。该流程在“作者近窗口新稿”阶段耗时较长，导致用户在进入实际抓取前需要等待较久。

本次调整要回归到更直接的方案：不再维护本地作者清单，而是直接复用已经产出的 `video_pool` 视频列表结果，完成窗口裁剪与去重后立刻开始评论/互动量抓取。

## 目标

- 移除 `手动批量抓取-动态数据` 对 `realtime_watchlist_authors.csv` 等本地作者清单状态的依赖。
- 页面允许用户选择 `outputs/video_pool/uid_expansions/` 下的一个或多个任务目录作为输入来源。
- 系统自动附加 `outputs/video_pool/full_site_floorings/` 下最新创建的 `daily_hot-*.csv` 与 `rankboard-*.csv`。
- 合并所有来源的视频列表后，按追踪窗口裁剪并按 `bvid` 去重。
- 每次运行都在 `outputs/video_data/manual_crawls/` 下创建新任务目录，命名格式为 `manual_crawl_stat_comment_<date>_<time>`。
- 在该任务目录中先留存本轮整合去重后的待抓视频列表，再进入 `REALTIME_ONLY` 的评论/互动量抓取，便于检查与续跑。

## 非目标

- 不保留旧的“本地作者清单 + 根待抓队列”持久化机制。
- 本页对应的新流程不再读取、写入或校验 `realtime_watchlist_authors.csv`、`realtime_watchlist_current.csv`、`realtime_watchlist_state.json` 等 watchlist 根状态文件。
- 不修改 `手动批量抓取-媒体/元数据` 页面行为。
- 不引入新的视频发现逻辑，只复用已有 `uid_expansions` 与 `full_site_floorings` 结果。

## 方案对比

### 方案 A：页面切换为 videolist 直抓

- 直接替换当前 `手动批量抓取-动态数据` 页面交互。
- 页面只负责收集用户选择的 `uid_expansion` 任务目录与抓取参数。
- 后端基于所选目录 + 最新 `daily_hot`/`rankboard` 文件生成待抓视频列表，然后立刻执行动态数据抓取。

优点：
- 最贴合用户诉求，能最大程度减少等待时间。
- 交互更直观，输入来源可追溯。

缺点：
- 需要调整当前页面 UI、文案和状态展示。

### 方案 B：保留旧流程并增加快速模式

- 在现有页面中增加“快速模式”开关，在作者清单模式和 videolist 直抓模式之间切换。

优点：
- 向后兼容原流程。

缺点：
- 页面复杂度变高。
- 与用户“不再维护作者清单”的目标不一致。

### 方案 C：完全依赖后台全量扫描

- 不让用户选择任务目录，后台自动扫描全部 `uid_expansions` 历史目录并结合最新 `full_site_floorings` 执行抓取。

优点：
- 前端最简单。

缺点：
- 输入范围过大，不利于用户控制任务边界。
- 容易把不想参与本轮抓取的历史任务也卷入。

## 选定方案

采用方案 A。

前端改为“选择一个或多个 `uid_expansion` 任务目录 + 自动附加最新 `daily_hot`/`rankboard`”的交互；后台复用并扩展现有 `manual_batch_runner` 的汇总、过滤、去重和批量抓取能力。

## 架构与组件改动

### 1. `bilibili-datahub.py`

- 重写 `手动批量抓取-动态数据` 标签页。
- 删除本地作者清单上传、作者状态展示、根待抓队列状态展示与相关校验。
- 新增 `uid_expansions` 任务目录扫描与多选 UI。
- 显示本轮自动命中的最新 `daily_hot-*.csv` 与 `rankboard-*.csv` 文件。
- 调用新的手动动态抓取 runner，并展示其返回的运行报告。

### 2. `src/bili_pipeline/datahub/manual_batch_runner.py`

- 将当前“扫描全部 `uid_expansions`/`full_site_floorings`”的能力改为可配置输入来源。
- 新增“只读取用户选中的 `uid_expansion` 任务目录”的发现逻辑；任务目录范围限定为 `outputs/video_pool/uid_expansions/` 的直接子目录，多选时仅扫描被选目录下的 `videolist_part_*.csv`。
- 新增“只附加最新 `daily_hot` 与最新 `rankboard`”的发现逻辑，而不是读取全部 `full_site_floorings` CSV。
- 调整任务目录命名规则为 `manual_crawl_stat_comment_<date>_<time>`，其中 `<date>_<time>` 使用本地时间 `YYYYMMDD_HHMMSS` 格式，示例：`manual_crawl_stat_comment_20260401_153000`。
- 在任务目录中落盘整合去重后的待抓视频列表，固定文件名为 `filtered_video_list.csv`，然后调用 `crawl_bvid_list_from_csv(..., task_mode=REALTIME_ONLY)`。

### 3. 测试

- 更新/新增 `tests/test_manual_batch_runner.py`：
  - 验证仅采纳用户选中的 `uid_expansion` 目录。
  - 验证仅采纳最新 `daily_hot` 和最新 `rankboard` 文件。
  - 验证窗口过滤与 `bvid` 去重仍生效。
  - 验证任务目录命名为 `manual_crawl_stat_comment_*`。
  - 验证任务目录中先留存整合后的待抓 CSV，并用它作为抓取输入。

### 4. 文档

- 更新 `Bilibili_Video_Data_Crawler.md` 中关于 `手动批量抓取-动态数据` 的描述。
- 将“本地作者清单 + realtime watchlist”改为“多选 `uid_expansion` 任务目录 + 自动附加最新 `daily_hot`/`rankboard` + 直接抓取评论/互动量”。

## 数据流

1. 用户在页面中选择一个或多个 `uid_expansion` 任务目录。
2. 系统扫描所选目录下的 `videolist_part_*.csv`。
3. 系统在 `full_site_floorings` 中分别挑选最新创建的一份 `daily_hot-*.csv` 和一份 `rankboard-*.csv`。这里的“最新”固定定义为文件系统 `st_mtime` 最大的文件；若 `st_mtime` 相同，则按文件名降序取第一份，确保结果稳定。
4. 读取所有候选 CSV，合并为候选视频列表。
5. 根据 `pubdate` 执行追踪窗口裁剪。
6. 去重前先按 `pubdate` 解析结果降序排序；若 `pubdate` 相同，则按来源优先级 `uid_expansions > daily_hot > rankboard` 排序；若仍相同，则按 `source_csv_path` 升序排序。随后按 `bvid` 去重并保留首条记录。
7. 缺少 `pubdate` 的记录不进入本轮待抓列表，但仍计入运行报告中的跳过统计。
8. 在新的 `manual_crawl_stat_comment_<date>_<time>` 任务目录下写入整合后的留存 CSV `filtered_video_list.csv` 与状态文件。
9. 直接基于该 CSV 进入 `REALTIME_ONLY` 抓取。
10. 若抓取中断或产生剩余清单，用户可基于任务目录内文件检查或续跑。

## 错误处理

- 若未选择任何 `uid_expansion` 任务目录，则前端阻止启动并提示用户先选择来源。
- 本设计不支持“仅使用 flooring 文件、不选任何 `uid_expansion` 目录”的运行方式。
- 若 `full_site_floorings` 中缺少最新 `daily_hot` 或 `rankboard` 文件，则作为可读错误提示，不进入抓取。
- 若选中的任务目录下没有有效 `videolist_part_*.csv`，则提示输入为空。
- 若候选视频在窗口裁剪后为空，则仍创建任务目录并保留筛选后 CSV，但跳过实际抓取。
- 若批量抓取阶段产生 `remaining_csv`，则保留在同一任务目录中供后续检查和续跑。

## 验证计划

- 运行 `manual_batch_runner` 相关测试，确认来源发现、窗口过滤、去重与任务目录命名符合预期。
- 进行一次页面层面的最小手工验证：
  - 能看到 `uid_expansion` 任务目录多选控件；
  - 能展示自动命中的最新 `daily_hot` 与 `rankboard`；
  - 运行后能在 `outputs/video_data/manual_crawls/manual_crawl_stat_comment_*` 下看到留存 CSV 和状态文件。

## 风险

- 旧的 `realtime_watchlist_runner` 仍保留在代码库中，但不再服务于该页面；需要注意避免页面文案和文档残留旧表述。
- 某些历史 `videolist` CSV 可能缺少 `pubdate`；这类记录会在窗口裁剪阶段被跳过，可能让用户误以为数据缺失，因此运行报告中需保留统计信息。
- “最新 `daily_hot` / `rankboard`”依赖文件创建时间或命名排序规则，选择策略必须在实现中固定且可解释。
