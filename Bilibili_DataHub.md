## Bilibili DataHub 说明

**Bilibili DataHub** 是当前推荐使用的本地统一入口，用于把原先分散在 `Bilibili_Video_Pool_Builder`、`Bilibili Video Data Crawler` 和本地化 `Cloud Tracker` 中的核心能力整合到同一个 Streamlit 页面与同一套本地配置中。

### 核心功能

- **统一的视频列表发现入口**：在同一页面中完成“当日热门 / 每周必看 / 全分区当天主流视频 / UGC 实时排行榜”视频列表抓取，并自动生成配套作者 UID 列表。
- **作者历史视频扩展**：继续支持基于 `owner_mid` 列表的增量 `uid_expansion` 任务，保留 `original_uids.csv`、`videolist_part_n.csv`、`remaining_uids_part_n.csv` 与总结日志的续跑机制。
- **作者数据洞察与精简**：可上传作者列表并抓取 `MetaResult` 级作者字段（如昵称、签名、性别、等级、认证状态、会员类型、粉丝数、关注数、公开视频数），支持在连续风控报错达到阈值后自动暂停、导出 `remaining_authors_part_n.csv` 供后续续跑；抓取任务结束时只落盘累计扩充结果，后续需由用户手动上传完整作者清单并点击按钮，才会生成完整汇总、可视化图表与分层精简结果。
- **CSV/XLSX 文件拼接及去重**：可对多个本地导出结果统一排序、拼接，并按指定键去重。
- **数据抓取调试**：继续支持单 `bvid` 全流程抓取、四类接口调试，以及 BigQuery / GCS 数据查看与媒体文件回读导出。
- **手动批量抓取-动态数据**：在本页直接多选 `outputs/video_pool/full_site_floorings/` 下的一个或多个现成视频列表 CSV，并可选叠加一个或多个 `uid_expansion` 任务目录中的 `videolist_part_*.csv`；系统会将这些来源合并后按「追踪窗口」筛选、去重，再只抓取评论/互动量等实时数据并上传至 BigQuery。
- **手动批量抓取-媒体/元数据**：支持两种补抓模式。模式 A 会基于当前 BigQuery Dataset 中“已存在动态数据、但尚未同时具备 `videos` 元数据记录与 `assets` 中视频轨/音频轨记录”的视频，维护待补清单并按清单抓取；模式 B 支持用户手动上传一个或多个包含 `bvid` 列的文件，先拼接去重、再剔除当前 Dataset 中已完成媒体/元数据的视频后抓取。
- **本地自动批量抓取**：在前端中可手动触发一轮“最新排行榜视频列表 + 作者源最新视频列表 + 实时评论/互动量抓取”；同样的逻辑也可通过统一脚本入口执行。
- **待补元数据/媒体清单**：自动批量抓取阶段发现的新视频会同步进入 `tracker_meta_media_queue` 对应的待补队列；后续既可在“自动批量抓取”页直接消化，也可在“手动批量抓取-媒体/元数据”页按 Dataset 缺口或手动上传清单集中补抓。
- **作者源管理**：在前端上传包含 `owner_mid` 列的 CSV，可直接替换自动批量抓取所使用的作者列表。

### 典型使用场景

- **本地长期运行**：把原先不稳定的云端定时方案收回本地，改为手动触发或由本地 Agent 定时调用统一脚本。
- **学术分析数据底座维护**：先发现视频样本，再按需要补抓评论、互动量、元数据和媒体，保持数据底座持续更新。
- **风控敏感场景**：借助运行锁、风控暂停和请求延迟参数，避免重叠运行与过激请求。

### 当前使用方式（简要）

- 启动统一前端：

```bash
streamlit run bilibili-datahub.py
```

- 执行一轮本地自动批量抓取：

```bash
python bilibili-datahub_runner.py
```

- 启动常驻的旧版手动批量抓取脚本（默认每 6 小时一轮、每 15 分钟打印状态；仍走 `manual_batch_runner` 的历史 CSV 汇总路径，不等同于本页 realtime watchlist 流程）：

```bash
python scripts/manual_batch_crawl_daemon.py
```

- 启动常驻的视频列表发现 + uid_expansion 脚本（默认每 3 小时一轮）：

```bash
python scripts/scheduled_discovery_daemon.py --tracking_ups_path tracking_ups_v1.csv
```

- `bilibili-datahub.py` 当前主要包含十个主标签页：
  - **视频列表构建**：承接原 `Bilibili_Video_Pool_Builder` 的自定义全量导出、作者视频扩展、BVID 回查作者 UID、失败 UID 提取。
  - **作者数据洞察&精简**：抓取作者元数据、支持断点续跑、查看粉丝量分布、叠加类别属性，并按粉丝量分层比例精简作者列表；也支持直接上传扩充后的作者表进行可视化。
  - **文件拼接及去重**：拼接多个 CSV/XLSX 并按指定键去重。
  - **数据抓取调试**：单视频全流程、四类接口调试。
  - **自动批量抓取**：作者列表上传、手动触发一轮自动批量抓取、查看运行状态、消化待补元数据/媒体队列。
  - **手动批量抓取-动态数据**：手动选择 `full_site_floorings` 视频列表 CSV，并可选叠加 `uid_expansion` 任务目录；系统合并窗口内清单后抓取实时互动量 / 评论数据。
  - **手动批量抓取-媒体/元数据**：维护待补媒体/元数据清单，或基于手动上传清单去重后补抓视频元数据与媒体文件。
  - **BigQuery / GCS 数据查看**：查看结构化数据和媒体资产，并导出媒体文件。
  - **快捷跳转**：打开视频页或作者主页。
  - **tid 与分区名称对应**：查询分区映射。

- Google Cloud 配置和自动批量抓取配置会分别保存在本地 `.local/` 下的 DataHub 配置文件中；B 站 Cookie 仍只保存在当前会话内存中。

### 从 GitHub clone 到本地成功运行 DataHub

下面这套流程面向“刚从 GitHub clone 本项目到本地、希望直接在自己电脑上跑 `bilibili-datahub.py`”的用户。以下命令默认使用 **Windows PowerShell**。

#### 1. clone 项目并进入目录

```powershell
git clone <YOUR_GITHUB_REPO_URL>
cd bilibili-data
```

如果你的仓库目录名不是 `bilibili-data`，请把下面命令中的路径替换成你自己的实际路径。

#### 2. 创建虚拟环境并安装依赖

如果使用 `uv`：

```powershell
uv sync
```

如果使用 `pip`：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
```

之后建议都在激活虚拟环境后再运行：

```powershell
.\.venv\Scripts\Activate.ps1
```

#### 3. 安装并登录 Google Cloud CLI

先确认本机已安装 `gcloud`、`bq`。若尚未安装，请先安装 Google Cloud CLI。

登录：

```powershell
gcloud auth login
gcloud auth application-default login
```

设置默认项目：

```powershell
$PROJECT_ID = "your-gcp-project-id"
gcloud config set project $PROJECT_ID
```

查看当前配置：

```powershell
gcloud config list
```

#### 4. 准备 BigQuery Dataset 和 GCS Bucket

建议 `Bilibili DataHub` 与你已有的 `Bilibili Video Data Crawler` 复用同一个 Dataset / Bucket，这样结构化数据、媒体文件和自动批量抓取控制表都在同一套资源里。

```powershell
$REGION = "asia-east1"
$BQ_DATASET = "bili_video_data_crawler"
$GCS_BUCKET = "your-bili-datahub-bucket"
```

创建 BigQuery Dataset：

```powershell
bq --location=$REGION mk --dataset "$PROJECT_ID`:$BQ_DATASET"
```

查看：

```powershell
bq ls
```

创建 GCS Bucket：

```powershell
gcloud storage buckets create "gs://$GCS_BUCKET" --location=$REGION --uniform-bucket-level-access
```

查看：

```powershell
gcloud storage buckets list
```

#### 5. 创建 DataHub 专用 Service Account

建议不要直接用个人账号长期跑本地采集，而是创建一个专用服务账号，供 `bilibili-datahub.py` 和 `bilibili-datahub_runner.py` 访问 BigQuery / GCS。

```powershell
$SERVICE_ACCOUNT = "bili-datahub-local"
gcloud iam service-accounts create $SERVICE_ACCOUNT --display-name="Bilibili DataHub Local Service Account"
```

构造服务账号邮箱：

```powershell
$SERVICE_ACCOUNT_EMAIL = "${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com"
```

查看：

```powershell
gcloud iam service-accounts list
```

#### 6. 给 Service Account 分配权限

最少建议分配下面几个角色：

```powershell
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/storage.objectAdmin"
```

如果你后续还想让它读写更多追踪相关控制信息，保留以上三个角色通常已经足够本地 DataHub 使用。

#### 7. 创建本地 Service Account JSON

推荐把 JSON 放到项目下的 `.local/` 目录，例如：

```powershell
New-Item -ItemType Directory -Force ".\.local" | Out-Null
$SA_KEY_PATH = (Resolve-Path ".\.local").Path + "\bili-datahub-sa.json"
gcloud iam service-accounts keys create $SA_KEY_PATH --iam-account=$SERVICE_ACCOUNT_EMAIL
```

确认文件已经生成：

```powershell
Get-Item $SA_KEY_PATH
```

可选：也可以同时设置环境变量，便于脚本或其他工具复用：

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS = $SA_KEY_PATH
```

如果你想长期保留这个环境变量，也可以手动写入系统用户环境变量；但对 `Bilibili DataHub` 来说，直接在前端填写 JSON 路径即可。

#### 8. 启动 DataHub 前端

```powershell
.\.venv\Scripts\Activate.ps1
streamlit run bilibili-datahub.py
```

启动后，在左侧边栏填写以下字段：

- `GCP Project ID`：`$PROJECT_ID`
- `BigQuery Dataset`：`$BQ_DATASET`
- `GCS Bucket 名称`：`$GCS_BUCKET`
- `GCP Region（可选）`：`$REGION`
- `服务账号 JSON 路径（可选）`：上一步生成的 `$SA_KEY_PATH`
- `GCS 对象前缀`：例如 `bilibili-media`
- `公共访问基础 URL（可选）`：没有可先留空

然后点击：

- `保存 GCP 配置`

保存后，这些信息会写入本地：

- `.local/bilibili-datahub.gcp.config.json`

下次启动 `bilibili-datahub.py` 时会自动回填。

#### 9. 首次运行时建议做的检查

建议先做六步最小验证：

1. 在 `数据抓取调试` 里找一个 `bvid`，执行一次 `Meta` 或 `单 bvid 全流程`。
2. 到 `BigQuery / GCS 数据查看` 里查看这个 `bvid` 是否已经能查到结构化记录。
3. 到 `自动批量抓取` 页上传一个只包含 `owner_mid` 列的小 CSV，手动触发一轮自动批量抓取，确认 `tracker_*` 控制表和 `meta_media_queue` 已开始写入。
4. 到 `手动批量抓取-动态数据` 页先选择一个或多个 `full_site_floorings` 视频列表 CSV，并按需补充 `uid_expansion` 任务目录，再执行一轮任务，确认 `outputs/video_data/manual_crawls/` 下已生成新的 `manual_crawl_stat_comment_<时间戳>/` 会话目录（内含筛选清单与 `logs/`）。
5. 到 `手动批量抓取-媒体/元数据` 页先执行一次 `同步待补媒体/元数据视频清单`，再视需要触发模式 A 或模式 B，确认对应任务目录和清单文件已经生成。
6. 到 `作者数据洞察&精简` 页上传一个作者 CSV，确认 `outputs/author_refinements/author_refinement_<date>_<time>/` 下已生成扩充作者表、精简版作者表、HTML 图表与日志。

#### 10. 运行本地自动批量抓取脚本

当你已经在前端保存过 GCP 配置与自动批量抓取配置后，可以直接用统一脚本触发一轮：

```powershell
.\.venv\Scripts\Activate.ps1
python bilibili-datahub_runner.py
```

如果要忽略当前暂停窗口并强制执行一轮：

```powershell
python bilibili-datahub_runner.py --force
```

该脚本会自动读取：

- `.local/bilibili-datahub.gcp.config.json`
- `.local/bilibili-datahub.auto.config.json`

因此很适合被本地 Agent、Windows 任务计划程序或其他调度工具定时调用。

#### 11. 可选：提高评论与媒体抓取成功率

如果你希望评论接口和媒体抓取更稳定，可以在前端左侧 `B 站 Cookie` 文本框中填入：

- `SESSDATA`
- `bili_jct`
- `buvid3`

格式示例：

```text
SESSDATA=xxx; bili_jct=xxx; buvid3=xxx
```

注意：

- 这部分 Cookie 只保存在当前 Streamlit 会话内存中，不会写入本地 JSON 配置。
- `bilibili-datahub_runner.py` 脚本模式如果也要使用登录态，需要额外在系统环境变量中设置：

```powershell
$env:BILI_SESSDATA = "your-sessdata"
$env:BILI_BILI_JCT = "your-bili-jct"
$env:BILI_BUVID3 = "your-buvid3"
```

#### 12. 常见失败点排查

- `BigQuery / GCS 连接失败`：先检查服务账号 JSON 路径、项目 ID、Bucket 名称、Dataset 名称是否与实际一致。
- `google.auth` 或 `google.cloud` 相关报错：通常是虚拟环境未正确安装依赖，重新激活 `.venv` 后执行 `pip install -e .` 或 `uv sync`。
- `权限不足`：检查 Service Account 是否已经拿到 `roles/bigquery.dataEditor`、`roles/bigquery.jobUser`、`roles/storage.objectAdmin`。
- `自动批量抓取脚本能跑但前端查不到数据`：确认脚本读取到的是同一份 `.local/bilibili-datahub.gcp.config.json`，且没有在不同目录下启动。
- `手动批量抓取-动态数据` 报错或清单为空：确认至少选择了一个 `full_site_floorings` 视频列表 CSV 或一个 `uid_expansion` 任务目录；适当放宽「追踪窗口（小时）」，并检查所选 CSV 是否包含有效 `bvid` / `pubdate`。
- `手动批量抓取-动态数据任务中断后想排查`：查看本轮会话目录 `outputs/video_data/manual_crawls/manual_crawl_stat_comment_<时间戳>/` 下的 `manual_crawl_state.json`、`filtered_video_list.csv` 与 `logs/`；若批量子任务中断，也可继续查看同目录下保留的剩余 CSV。
- `手动批量抓取-媒体/元数据` 看不到待补条目：先确认当前侧边栏填写的是目标 BigQuery Dataset；模式 A 只会纳入已进入 `video_stat_snapshots` 或 `topn_comment_snapshots`、但尚未同时具备 `videos` 与 `assets(video+audio)` 的视频。
- `手动批量抓取-媒体/元数据` 在风控后暂停：查看对应任务目录下的 `manual_crawl_media_state.json`、`batch_crawl_state.json`、`remaining_bvids_part_n.csv` 与 `logs/`；模式 A 会在任务结束后刷新根目录 waitlist，模式 B 会把剩余清单保留在当前任务目录中。

### 自动批量抓取说明

- 一轮自动流程固定做三件事：
  - 拉取最新排行榜视频列表；
  - 拉取作者源中各作者的最新发布视频列表；
  - 对本轮进入追踪范围的视频抓取评论快照和互动量快照。

- 新发现的视频会自动加入 `meta_media_queue` 对应队列，用于后续补抓元数据和媒体。
- 运行过程中继续复用 BigQuery 中的 `tracker_*` 控制表实现：
  - **运行锁**：避免同一时间多轮任务重叠；
  - **风控暂停窗口**：触发风控后自动暂停，等待下一次脚本调用或前端手动重试；
  - **作者源 / watchlist / run_logs**：沿用现有结构，避免破坏旧数据。

### 手动批量抓取-动态数据说明

- 本页只抓取**实时数据**（评论快照、互动量快照），不处理元数据与媒体。
- **输入**：用户在页内显式选择的 `full_site_floorings` 视频列表 CSV，以及可选的 `uid_expansion` 任务目录；系统会读取所选任务目录下的 `videolist_part_*.csv`。不再在本页维护作者 CSV，也不再自动附加最新 `daily_hot` / `rankboard`。
- **每轮流程（概要）**：读取所选 `full_site_floorings` 视频列表 CSV → 读取所选 `uid_expansion` 任务目录中的 `videolist_part_*.csv` → 合并候选视频 → 按本轮启动时刻与「追踪窗口」过滤发布时间 → 去重 → 生成本轮抓取清单并调用与批量实时模式相同的入库逻辑。
- **窗口裁剪说明**：无有效 `pubdate` 的条目不会进入或留在当前有效 watchlist 中。
- **每轮会话目录** `manual_crawl_stat_comment_<时间戳>/` 中会保存 `filtered_video_list.csv`、`manual_crawl_state.json` 与 `logs/`；若底层批量任务部分失败，同目录下还会保留剩余 CSV 供继续执行。
- **风控睡眠说明**：发现阶段若识别为风控错误，会按当前设置持续睡眠后重试，直到成功或被用户手动中断；非风控错误则直接中止本轮任务。

### 手动批量抓取-媒体/元数据说明

- 本页专门处理**一次性数据**，即视频元数据与媒体文件（视频轨 + 音频轨）。
- **模式 A：基于数据集缺失条目**
  - 点击 `同步待补媒体/元数据视频清单` 后，程序会在当前 BigQuery Dataset 中计算：
    - 候选集：存在于 `video_stat_snapshots` 或 `topn_comment_snapshots` 的视频；
    - 已完成集：同时存在于 `videos` 表，且 `assets` 表中同时具有 `asset_type=video` 与 `asset_type=audio` 的视频；
    - 待补集：候选集减已完成集。
  - 同步结果会写到 `outputs/video_data/manual_crawls/manual_crawl_media_waitlist_<DatasetName>.csv`。
  - 点击 `按清单抓取媒体/元数据` 后，每次都会新建独立任务目录 `manual_crawl_media_mode_A_<date>_<time>/`，并按当前 waitlist 抓取。
  - 若遇到风控，当前任务目录会直接留档；如启用了睡眠机制，则等待指定分钟数后再开启一个新的 Mode A 任务目录继续抓取。若遇到 `WinError`，则直接停止并保留结果。
- **模式 B：基于手动上传清单**
  - 可上传一个或多个包含 `bvid` 列的 CSV/XLSX 文件。
  - 点击 `去重并抓取` 后，会新建 `manual_crawl_media_mode_B_<date>_<time>/` 目录，先拼接去重，再剔除当前 Dataset 中已经同时具备 `videos` 与 `assets(video+audio)` 的视频。
  - 若触发风控，系统会把剩余待抓视频清单保存在当前任务目录中，睡眠后继续在同一任务目录内追加下一 part；若遇到 `WinError`，则直接停止。
- 两种模式都会保留任务级状态文件、批量抓取状态文件和日志文件，便于中断后排查与续跑。

### 常驻脚本说明

- `scripts/manual_batch_crawl_daemon.py`：
  - 默认每 6 小时触发一轮旧版“手动批量抓取”脚本流程，当前仍通过 `manual_batch_runner` 扫描历史 CSV 后抓取实时数据；
  - 默认每 15 分钟向 PowerShell 打印一次当前状态；
  - 支持通过 `--stream-data-time-window-hours`、`--parallelism`、`--comment-limit`、`--consecutive-failure-limit` 调整任务参数。
  - 该脚本**不会**自动触发前端 `手动批量抓取-动态数据` 页的 realtime watchlist 编排流程。
- `scripts/scheduled_discovery_daemon.py`：
  - 默认每 3 小时执行一轮“热门 400 + 全站实时排行榜”视频列表抓取；
  - 同一轮中还会基于 `--tracking_ups_path` 指定的作者列表，执行一次 `uid_expansion`；
  - `uid_expansion` 默认抓取最近 14 天，可通过 `--uid-expansion-window-days` 调整。

### 维护约定

- 当对 `bilibili-datahub.py`、`bilibili-datahub_runner.py`、`src/bili_pipeline/datahub/` 或自动批量抓取相关 BigQuery 队列行为进行实质性修改时，应同步检查本说明文档。
- 若修改同时影响 `Bilibili_Video_Pool_Builder` 或 `Bilibili Video Data Crawler` 的原始能力描述，也要同步回看对应两份旧文档，保持三份说明的一致性。
