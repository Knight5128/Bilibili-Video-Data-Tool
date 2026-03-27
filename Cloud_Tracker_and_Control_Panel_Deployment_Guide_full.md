下面给你一份可以直接照着跑的、**从 0 到可用**的完整部署流程。  
我按你当前项目的结构来写，目标是最终得到：

- 一个跑在 **Google Cloud Run** 上的 `Bilibili Cloud Tracker`
- 一个定时触发它的 **Cloud Scheduler**
- 一个跑在 **Cloudflare Workers** 上的公网控制台
- 一个用 **Cloudflare Access** 保护起来的管理入口


---

## 0. 先确认本地前提

你本机需要有这些工具：

- `gcloud`
- `bq`
- `python`
- `uv`
- `node`
- `npm`
- `wrangler`

检查命令：

```powershell
gcloud --version
bq version
python --version
uv --version
node --version
npm --version
npx wrangler --version
```

如果 `wrangler` 还没登录：

```powershell
npx wrangler login
```

如果 `gcloud` 还没登录：

```powershell
gcloud auth login #重新登录后会进入官方案例页面
gcloud auth application-default login
```

---

## 1. 进入项目目录

```powershell
cd "d:\Schoolworks\Thesis\bilibili-data"
```

---

## 2. 先定义整套部署用变量

下面这些你可以直接复制到 PowerShell，然后把值改成你自己的。

```powershell
$PROJECT_ID = "bilibili-m2s2vp-research"
$REGION = "us-central1"
$BQ_DATASET = "bili_video_data_crawler"
$GCS_BUCKET = "bilibili-video-data-testbucket"
$RUN_SERVICE = "bilibili-cloud-tracker"
$SCHEDULER_JOB = "bilibili-cloud-tracker-2h"
$SCHEDULER_LOCATION = $REGION
$SERVICE_ACCOUNT = "bili-tracker-sa"
$SERVICE_ACCOUNT_EMAIL = "$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com"
$IMAGE = "gcr.io/$PROJECT_ID/$RUN_SERVICE"
$TRACKER_ADMIN_TOKEN = "HEMmsRizcUezfN8NE8Spi4ZYsC1YZDKMtotkXAPo0"
$SESSDATA_SECRET_NAME = "bili-sessdata"
$BILI_JCT_SECRET_NAME = "bili-jct"
$BUVID3_SECRET_NAME = "bili-buvid3"
$TRACKER_ADMIN_SECRET_NAME = "tracker-admin-token"
```

如果你暂时**不打算**把 B 站 Cookie 放进云端，可以先只用：

```powershell
$PROJECT_ID = "your-gcp-project-id"
$REGION = "us-central1"
$BQ_DATASET = "bili_video_data_crawler"
$GCS_BUCKET = "bilibili-video-data-testbucket"
$RUN_SERVICE = "bilibili-cloud-tracker"
$SCHEDULER_JOB = "bilibili-cloud-tracker-2h"
$SCHEDULER_LOCATION = $REGION
$SERVICE_ACCOUNT = "bili-tracker-sa"
$SERVICE_ACCOUNT_EMAIL = "$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com"
$IMAGE = "gcr.io/$PROJECT_ID/$RUN_SERVICE"
$TRACKER_ADMIN_TOKEN = "HEMmsRizcUezfN8NE8Spi4ZYsC1YZDKMtotkXAPo0"
$TRACKER_ADMIN_SECRET_NAME = "tracker-admin-token"
```

### 手动生成属于自己的Admin Token
```powershell
$TRACKER_ADMIN_SECRET_NAME = "tracker-admin-token"
$bytes = New-Object byte[] 32; [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes); $TRACKER_ADMIN_TOKEN = [Convert]::ToBase64String($bytes)
```

---

## 3. 选择当前 GCP 项目

```powershell
gcloud config set project $PROJECT_ID
```

验证：

```powershell
gcloud config get-value project
```

---

## 4. 启用需要的 GCP API

```powershell
gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com #新启用的API启用完成后会显示：Operation "operations/acf.p2-821794372172-0cd43743-e80e-4959-b19e-9e3b7f79f881" finished successfully.
gcloud services enable bigquery.googleapis.com #之前启用好的API，在运行指令后会快速返回空值
gcloud services enable storage.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable secretmanager.googleapis.com
gcloud services enable iam.googleapis.com
```


---

## 5. 创建 BigQuery Dataset

```powershell
bq --location=$REGION mk --dataset "$PROJECT_ID`:$BQ_DATASET"
```

如果提示已存在，可以忽略。

查看：

```powershell
bq ls
```

---

## 6. 创建 GCS Bucket

```powershell
gcloud storage buckets create "gs://$GCS_BUCKET" --location=$REGION --uniform-bucket-level-access
```

查看：

```powershell
gcloud storage buckets list
```

---

## 7. 创建 Cloud Run 用的 Service Account

```powershell
gcloud iam service-accounts create $SERVICE_ACCOUNT --display-name="Bilibili Tracker Service Account"
```

查看：

```powershell
gcloud iam service-accounts list
```

---

## 8. 给 Service Account 赋权

```powershell
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/storage.objectAdmin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/secretmanager.secretAccessor"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/run.invoker"
```

如果你之后要让这个账号也有更多 Cloud Run 管理能力，可以再补：

```powershell
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/run.viewer"
```

---

## 9. 把 Tracker Admin Token 放入 Secret Manager

```powershell
$TRACKER_ADMIN_TOKEN | gcloud secrets create $TRACKER_ADMIN_SECRET_NAME --data-file=-
```

如果 secret 已存在，更新版本：

```powershell
$TRACKER_ADMIN_TOKEN | gcloud secrets versions add $TRACKER_ADMIN_SECRET_NAME --data-file=-
```

---

## 10. 可选：把 B 站 Cookie 放入 Secret Manager
**通过浏览器Cookie-Editor拓展获取!!!!!!!!!!!!!!!!!!!!!!!**

如果你想提高评论和接口成功率，分别创建这些 secret。

### 10.1 SESSDATA

```powershell
"your-sessdata" | gcloud secrets create $SESSDATA_SECRET_NAME --data-file=-
```

若已存在：

```powershell
"your-sessdata" | gcloud secrets versions add $SESSDATA_SECRET_NAME --data-file=-
```

### 10.2 bili_jct

```powershell
"your-bili-jct" | gcloud secrets create $BILI_JCT_SECRET_NAME --data-file=-
```

若已存在：

```powershell
"your-bili-jct" | gcloud secrets versions add $BILI_JCT_SECRET_NAME --data-file=-
```

### 10.3 buvid3

```powershell
"your-buvid3" | gcloud secrets create $BUVID3_SECRET_NAME --data-file=-
```

若已存在：

```powershell
"your-buvid3" | gcloud secrets versions add $BUVID3_SECRET_NAME --data-file=-
```

---

## 11. 构建 Cloud Run 镜像 (修改文件后从这步开始重新构建gcloud镜像并部署!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!)

在项目根目录运行：

```powershell
先在 bilibili-data 根目录新建一个文件，比如 cloudbuild.tracker.yaml，内容如下：

steps:
  - name: gcr.io/cloud-builders/docker
    args:
      - build
      - -f
      - cloud_tracker/Dockerfile
      - -t
      - ${_IMAGE}
      - .
images:
  - ${_IMAGE}
然后执行：
gcloud builds submit . --config cloudbuild.tracker.yaml --substitutions _IMAGE=$IMAGE
```

构建完成后，可以查看镜像：

```powershell
gcloud container images list --repository "gcr.io/$PROJECT_ID"
```

---

## 12. 部署 Cloud Run 服务（重新构建镜像后接着重新部署）

### 12.1 最小可用部署

```powershell
gcloud run deploy $RUN_SERVICE `
  --image $IMAGE `
  --region $REGION `
  --platform managed `
  --service-account $SERVICE_ACCOUNT_EMAIL `
  --allow-unauthenticated `
  --concurrency 1 `
  --max-instances 1 `
  --timeout 1800 `
  --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=$BQ_DATASET,GCS_BUCKET=$GCS_BUCKET,GCP_REGION=$REGION,TRACKER_CRAWL_INTERVAL_HOURS=2,TRACKER_TRACKING_WINDOW_DAYS=14,TRACKER_COMMENT_LIMIT=10,TRACKER_AUTHOR_BOOTSTRAP_DAYS=14,TRACKER_MAX_VIDEOS_PER_CYCLE=2000,TRACKER_SNAPSHOT_WORKERS=1,TRACKER_TABLE_PREFIX=tracker" `
  --set-secrets "TRACKER_ADMIN_TOKEN=$TRACKER_ADMIN_SECRET_NAME:latest"
```

### 12.2 如果同时带 B 站 Cookie

```powershell
gcloud run deploy $RUN_SERVICE `
  --image $IMAGE `
  --region $REGION `
  --platform managed `
  --service-account $SERVICE_ACCOUNT_EMAIL `
  --allow-unauthenticated `
  --concurrency 1 `
  --max-instances 1 `
  --timeout 1800 `
  --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=$BQ_DATASET,GCS_BUCKET=$GCS_BUCKET,GCP_REGION=$REGION,TRACKER_CRAWL_INTERVAL_HOURS=2,TRACKER_TRACKING_WINDOW_DAYS=14,TRACKER_COMMENT_LIMIT=10,TRACKER_AUTHOR_BOOTSTRAP_DAYS=14,TRACKER_MAX_VIDEOS_PER_CYCLE=2000,TRACKER_SNAPSHOT_WORKERS=1,TRACKER_TABLE_PREFIX=tracker" `
  --set-secrets "TRACKER_ADMIN_TOKEN=${TRACKER_ADMIN_SECRET_NAME}:latest,BILI_SESSDATA=${SESSDATA_SECRET_NAME}:latest,BILI_BILI_JCT=${BILI_JCT_SECRET_NAME}:latest,BILI_BUVID3=${BUVID3_SECRET_NAME}:latest"
```

---

## 13. 获取 Cloud Run URL

```powershell
$CLOUD_RUN_URL = gcloud run services describe $RUN_SERVICE --region $REGION --format="value(status.url)"
$CLOUD_RUN_URL
# 首次部署后直接回给出URL:
# Done.
# Service [bilibili-cloud-tracker] revision [bilibili-cloud-tracker-00001-fx8] has been deployed and is serving 100 percent of traffic.
# Service URL: https://bilibili-cloud-tracker-821794372172.us-central1.run.app
# Service URL: https://bilibili-cloud-tracker-821794372172.us-central1.run.app
```

---

## 14. 验证 Cloud Run 是否正常

### 14.1 健康检查

```powershell
Invoke-WebRequest -Uri "$CLOUD_RUN_URL/healthz"
```

### 14.2 管理状态接口

```powershell
Invoke-WebRequest `
  -Uri "$CLOUD_RUN_URL/admin/status" `
  -Headers @{ Authorization = "Bearer $TRACKER_ADMIN_TOKEN" }
```

如果能返回 JSON，说明后端已经通了。

---

## 15. 手动上传精选作者 CSV

假设你的作者文件在：

```powershell
$TRACKING_AUTHOR_CSV = "D:\\Schoolworks\\Thesis\\bilibili-data\\outputs\\video_pool\\bvid_to_uids\\tracking_ups_v1_20260327_165321.csv"
```

上传命令：

```powershell
curl.exe -X POST "$CLOUD_RUN_URL/admin/authors/upload" `
  -H "Authorization: Bearer $TRACKER_ADMIN_TOKEN" `
  -F "file=@$TRACKING_AUTHOR_CSV" `
  -F "source_name=selected_authors"
```

---

## 16. 手动跑一次 Tracker 周期任务

```powershell
Invoke-WebRequest `
  -Method POST `
  -Uri "$CLOUD_RUN_URL/run" `
  -Headers @{ Authorization = "Bearer $TRACKER_ADMIN_TOKEN" }
```

强制跑一轮：

```powershell
Invoke-WebRequest `
  -Method POST `
  -Uri "$CLOUD_RUN_URL/run?force=true" `
  -Headers @{ Authorization = "Bearer $TRACKER_ADMIN_TOKEN" }
```

---

## 17. 检查 BigQuery 是否已经开始写入

```powershell
bq query --use_legacy_sql=false "SELECT COUNT(*) AS cnt FROM \`$PROJECT_ID.$BQ_DATASET.tracker_run_logs\`"
bq query --use_legacy_sql=false "SELECT COUNT(*) AS cnt FROM \`$PROJECT_ID.$BQ_DATASET.tracker_video_watchlist\`"
bq query --use_legacy_sql=false "SELECT COUNT(*) AS cnt FROM \`$PROJECT_ID.$BQ_DATASET.video_stat_snapshots\`"
bq query --use_legacy_sql=false "SELECT COUNT(*) AS cnt FROM \`$PROJECT_ID.$BQ_DATASET.topn_comment_snapshots\`"
```

---

## 18. 创建 Cloud Scheduler 定时任务

### 18.1 推荐直接让 Scheduler 调 Cloud Run

```powershell
gcloud scheduler jobs create http $SCHEDULER_JOB `
  --location $SCHEDULER_LOCATION `
  --schedule "0 */2 * * *" `
  --http-method POST `
  --uri "$CLOUD_RUN_URL/run" `
  --headers "Authorization=Bearer $TRACKER_ADMIN_TOKEN"
```

### 18.2 如果 job 已存在，先删掉再建

```powershell
gcloud scheduler jobs delete $SCHEDULER_JOB --location $SCHEDULER_LOCATION
```

然后重新执行上面的 create。

### 18.3 手动触发一次 Scheduler

```powershell
gcloud scheduler jobs run $SCHEDULER_JOB --location $SCHEDULER_LOCATION
```

### 18.4 查看 Scheduler 状态

```powershell
gcloud scheduler jobs describe $SCHEDULER_JOB --location $SCHEDULER_LOCATION
```

### 18.5 暂停 / 恢复 Scheduler

```powershell
gcloud scheduler jobs pause $SCHEDULER_JOB --location $SCHEDULER_LOCATION
gcloud scheduler jobs resume $SCHEDULER_JOB --location $SCHEDULER_LOCATION
```

---

# 第二部分：部署 Cloudflare Workers 控制台

---

## 19. 进入 Worker 项目目录

```powershell
cd "d:\Schoolworks\Thesis\bilibili-data\cloud_panel"
```

---

## 20. 安装依赖

```powershell
npm install
```

---

## 21. 登录 Cloudflare

```powershell
npx wrangler login
```

---

## 22. 修改 `wrangler.toml`

打开文件：

```powershell
notepad .\wrangler.toml
```

把这些值改成你的真实值：

- `TRACKER_BASE_URL`
- `GCP_PROJECT_ID`
- `GCP_REGION`
- `CLOUD_RUN_SERVICE`
- `CLOUD_SCHEDULER_LOCATION`
- `CLOUD_SCHEDULER_JOB`

例如：

```toml
name = "bilibili-cloud-panel"
main = "src/index.js"
compatibility_date = "2026-03-27"

[observability]
enabled = true

[vars]
CF_ACCESS_REQUIRED = "true"
TRACKER_BASE_URL = "https://your-cloud-run-url"
GCP_PROJECT_ID = "your-gcp-project-id"
GCP_REGION = "asia-east1"
CLOUD_RUN_SERVICE = "bilibili-cloud-tracker"
CLOUD_SCHEDULER_LOCATION = "asia-east1"
CLOUD_SCHEDULER_JOB = "bilibili-cloud-tracker-2h"
CLOUD_RUN_TIMEOUT_SECONDS = "1800"
CLOUD_RUN_MAX_INSTANCES = "1"
CLOUD_RUN_CONCURRENCY = "1"
TRACKER_DEFAULT_ENV_KEYS = "GCP_PROJECT_ID,BQ_DATASET,GCS_BUCKET,GCP_REGION,TRACKER_CRAWL_INTERVAL_HOURS,TRACKER_TRACKING_WINDOW_DAYS,TRACKER_COMMENT_LIMIT,TRACKER_AUTHOR_BOOTSTRAP_DAYS,TRACKER_MAX_VIDEOS_PER_CYCLE,TRACKER_SNAPSHOT_WORKERS"
```

---

## 23. 给 Worker 写入必需的 Secret

### 23.1 写入 Tracker Admin Token

```powershell
npx wrangler secret put TRACKER_ADMIN_TOKEN
```

然后粘贴你前面设置的同一个 token。

### 23.2 写入 Google Service Account JSON

先在 GCP 里创建一个给 Worker 用的管理型 Service Account。

---

## 24. 创建给 Cloudflare Worker 使用的 Google Service Account

回到任意目录，继续执行：

```powershell
cd "d:\Schoolworks\Thesis\bilibili-data"
```

定义一个新的账号名：

```powershell
$WORKER_SA = "bili-panel-sa"
$WORKER_SA_EMAIL = "$WORKER_SA@$PROJECT_ID.iam.gserviceaccount.com"
```

创建：

```powershell
gcloud iam service-accounts create $WORKER_SA --display-name="Bilibili Control Panel Service Account"
```

赋权：

```powershell
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$WORKER_SA_EMAIL" --role="roles/run.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$WORKER_SA_EMAIL" --role="roles/cloudscheduler.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$WORKER_SA_EMAIL" --role="roles/run.viewer"
```

导出 JSON key：

```powershell
$WORKER_SA_KEY = "$env:TEMP\bili-panel-sa.json"
gcloud iam service-accounts keys create $WORKER_SA_KEY --iam-account=$WORKER_SA_EMAIL
```

把这个 JSON 写入 Wrangler secret：

```powershell
Get-Content $WORKER_SA_KEY -Raw | npx wrangler secret put GOOGLE_SERVICE_ACCOUNT_JSON
```
---

## 25. 重要：限制允许访问控制台的邮箱，和后面的Cloudflare Access配合!!!!!!!!!!!!!!!!!!!!!!!!
*gmail在此处貌似收不到验证码，因此需额外添加一个个人邮箱，进行控制台的登录*


```powershell
npx wrangler secret put CF_ACCESS_ALLOWED_EMAILS
```

输入示例：

```text
you@example.com
```

如果你现在这个 secret 里已经有一个邮箱，想再加第二个，CF_ACCESS_ALLOWED_EMAILS 的值要写成逗号分隔的一整串邮箱，然后重新覆盖写入。

直接运行：

npx wrangler secret put CF_ACCESS_ALLOWED_EMAILS
然后输入类似这样的一行内容：

first@example.com,second@example.com

---

## 26. 本地预检查 Worker 构建

```powershell
npx wrangler deploy --dry-run
```

如果通过，说明 Worker 构建没有问题。

---

## 27. 本地启动 Worker 调试

```powershell
npm run dev
```

本地一般会给你一个地址，例如：

```text
http://127.0.0.1:8787
```

你可以先访问：

- `/`
- `/docs`
- `/api/status`

---

## 28. 正式部署 Worker

```powershell
npm run deploy
```

部署成功后会得到类似：

```text
https://bilibili-cloud-panel.<subdomain>.workers.dev
```

记下来：

```powershell
$WORKER_URL = "https://your-worker-url.workers.dev"
```

---

# 第三部分：配置 Cloudflare Access

这个步骤**主要在 Cloudflare Dashboard 完成**，不是命令行最方便的部分。我给你最短路径。

---

## 29. 在 Cloudflare Zero Trust 中创建 Access 应用

进入：

- Cloudflare Dashboard
- Zero Trust
- Access
- Applications
- Add an application

选择：

- `Self-hosted`

填写：

- Application name: `Bilibili Cloud Control Panel`
- Domain: 你的 Worker 域名或自定义域名

策略建议：

- Allow
- Emails:
  - 你的邮箱
  - 或团队成员邮箱

如果你已经在 Worker secret 里配置了 `CF_ACCESS_ALLOWED_EMAILS`，这里和 Worker 内部会形成双重保护。

---

## 30. 验证 Access 是否生效

浏览器打开：

```text
https://your-worker-url.workers.dev
```

你应该先看到 Cloudflare Access 登录，再进入控制台页面。

---

# 第四部分：上线后验证

---

## 31. 验证 Worker 能否拿到 Tracker 状态

在浏览器打开控制台首页，观察这些项目是否正常显示：

- Cloud Run Health
- Tracking Authors
- Active Watch Videos
- Pending Meta/Media
- Scheduler State
- Recent Run Logs

如果页面空白，先用浏览器开发者工具看 `/api/status` 是否报错。

---

## 32. 用控制台做第一次真实操作

建议顺序：

1. 打开首页 Dashboard
2. 点击 `Upload Authors CSV`
3. 点击 `Run Once`
4. 看 `Recent Run Logs`
5. 看 `Pending Meta/Media`
6. 看 `Active Watch Videos`

---

## 33. 验证导出功能

在控制台点击：

- `Download Meta/Media Queue`
- `Download Watchlist`
- `Download Authors`

或者直接访问：

```text
https://your-worker-url.workers.dev/api/tracker/export/meta-media-queue
https://your-worker-url.workers.dev/api/tracker/export/watchlist
https://your-worker-url.workers.dev/api/tracker/export/authors
```

---

## 34. 验证 Worker 是否能控制 Google Cloud

在控制台里测试：

- Pause Scheduler
- Resume Scheduler
- Update Cloud Run Env
- Pause Tracker
- Resume Tracker

如果有问题，通常就是 Worker 使用的 Google Service Account 权限不够。

---

# 第五部分：常用运维命令

---

## 35. 查看 Cloud Run 服务详情

```powershell
gcloud run services describe $RUN_SERVICE --region $REGION
```

---

## 36. 查看 Cloud Run 日志

```powershell
gcloud run services logs read $RUN_SERVICE --region $REGION --limit 100
```

---

## 37. 更新 Cloud Run 环境变量

```powershell
gcloud run services update $RUN_SERVICE `
  --region $REGION `
  --update-env-vars "TRACKER_TRACKING_WINDOW_DAYS=14,TRACKER_COMMENT_LIMIT=10"
```

---

## 38. 重新部署 Cloud Run 镜像

```powershell
gcloud builds submit --tag $IMAGE -f cloud_tracker/Dockerfile .
gcloud run deploy $RUN_SERVICE `
  --image $IMAGE `
  --region $REGION `
  --platform managed `
  --service-account $SERVICE_ACCOUNT_EMAIL
```

---

## 39. 更新 Worker Secret

### 更新 Tracker token

```powershell
npx wrangler secret put TRACKER_ADMIN_TOKEN
```

### 更新 Google Service Account JSON

```powershell
Get-Content $WORKER_SA_KEY -Raw | npx wrangler secret put GOOGLE_SERVICE_ACCOUNT_JSON
```

更新后重新部署 Worker：

```powershell
npm run deploy
```

---

## 40. Worker 改动后的推荐重新部署流程!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

适用场景：

- 你修改了 `cloud_panel/src/` 下的页面、接口、Access 校验或导出逻辑
- 你修改了 `wrangler.toml`
- 你更新了 Worker 侧使用的 secret

如果你这次改的是 `cloud_tracker/`、`src/bili_pipeline/` 或其他 Cloud Run 后端逻辑，**不要只重部署 Worker**，而是回到前面的 Cloud Run 重部署流程。

### 40.1 进入 Worker 项目目录

```powershell
cd "D:\Schoolworks\Thesis\bilibili-data\cloud_panel"
```

### 40.2 如果改了依赖，先安装依赖

只有在以下情况才需要执行：

- 改了 `package.json`
- 新增了 npm 依赖

```powershell
npm install
```

### 40.3 如果改了 secret，先更新 secret

例如更新允许登录邮箱：

```powershell
npx wrangler secret put CF_ACCESS_ALLOWED_EMAILS
```

例如更新 Tracker token：

```powershell
npx wrangler secret put TRACKER_ADMIN_TOKEN
```

例如更新 Worker 用的 Google Service Account JSON：

```powershell
Get-Content $WORKER_SA_KEY -Raw | npx wrangler secret put GOOGLE_SERVICE_ACCOUNT_JSON
```

说明：

- `wrangler secret put` 会覆盖该 secret 的旧值
- 如果是邮箱白名单，记得一次性输入全部允许邮箱，并用英文逗号分隔

### 40.4 如果改了 `wrangler.toml`，先确认变量是否正确

重点检查：

- `CF_ACCESS_REQUIRED`
- `TRACKER_BASE_URL`
- `GCP_PROJECT_ID`
- `GCP_REGION`
- `CLOUD_RUN_SERVICE`
- `CLOUD_SCHEDULER_JOB`

尤其是当 Cloud Run URL、项目 ID 或调度器名称发生变化时，先改对这里，再重新部署 Worker。

### 40.5 先做一次本地预检查

```powershell
npx wrangler deploy --dry-run
```

如果这里失败，优先先修正构建或配置问题，不要直接正式部署。

### 40.6 正式重新部署 Worker

```powershell
npm run deploy
```

部署成功后，Wrangler 会输出新的 `workers.dev` 地址或确认当前 Worker 已更新。

### 40.7 部署后验证

建议至少验证这几项：

1. 打开：

```text
https://your-worker-url.workers.dev/healthz
```

应返回健康检查 JSON。

2. 用浏览器打开：

```text
https://your-worker-url.workers.dev
```

应先经过 Cloudflare Access 登录，然后进入控制台页面。

3. 在控制台里至少点一次与你本次修改相关的功能。

例如：

- 改了页面展示，就检查 Dashboard / Runtime Config / Docs 是否正常
- 改了控制逻辑，就测试 Run Once、Pause Scheduler、Resume Scheduler
- 改了导出逻辑，就重新测试 Exports

### 40.8 一句话判断这次该不该重部署 Worker

- 只改了 `cloud_panel/`：重部署 Worker
- 只改了 `cloud_tracker/` 或 `src/bili_pipeline/`：重部署 Cloud Run
- 两边都改了：先重部署 Cloud Run，再重部署 Worker

---

## 41. 暂停整个系统的推荐方式

### 方式 A：暂停 Scheduler

```powershell
gcloud scheduler jobs pause $SCHEDULER_JOB --location $SCHEDULER_LOCATION
```

### 方式 B：让 Tracker 进入 pause

```powershell
Invoke-WebRequest `
  -Method POST `
  -Uri "$CLOUD_RUN_URL/admin/config/update" `
  -Headers @{
    Authorization = "Bearer $TRACKER_ADMIN_TOKEN"
    "Content-Type" = "application/json"
  } `
  -Body '{"paused_until":"2099-12-31T23:59:59+00:00","pause_reason":"manual pause"}'
```

---

# 第六部分：最短可执行命令清单

如果你只想快速跑通，按这个顺序执行：

```powershell
cd "d:\Schoolworks\Thesis\bilibili-data"
gcloud auth login
gcloud auth application-default login
gcloud config set project your-gcp-project-id

gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable secretmanager.googleapis.com
gcloud services enable iam.googleapis.com
```

```powershell
$PROJECT_ID = "your-gcp-project-id"
$REGION = "asia-east1"
$BQ_DATASET = "bili_video_data_crawler"
$GCS_BUCKET = "$PROJECT_ID-bili-media"
$RUN_SERVICE = "bilibili-cloud-tracker"
$SCHEDULER_JOB = "bilibili-cloud-tracker-2h"
$SCHEDULER_LOCATION = $REGION
$SERVICE_ACCOUNT = "bili-tracker-sa"
$SERVICE_ACCOUNT_EMAIL = "$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com"
$IMAGE = "gcr.io/$PROJECT_ID/$RUN_SERVICE"
$TRACKER_ADMIN_TOKEN = "replace-with-a-long-random-secret-token"
$TRACKER_ADMIN_SECRET_NAME = "tracker-admin-token"
```

```powershell
bq --location=$REGION mk --dataset "$PROJECT_ID`:$BQ_DATASET"
gcloud storage buckets create "gs://$GCS_BUCKET" --location=$REGION --uniform-bucket-level-access
gcloud iam service-accounts create $SERVICE_ACCOUNT --display-name="Bilibili Tracker Service Account"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/storage.objectAdmin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="roles/secretmanager.secretAccessor"
$TRACKER_ADMIN_TOKEN | gcloud secrets create $TRACKER_ADMIN_SECRET_NAME --data-file=-
```

```powershell
gcloud builds submit --tag $IMAGE -f cloud_tracker/Dockerfile .
```

```powershell
gcloud run deploy $RUN_SERVICE `
  --image $IMAGE `
  --region $REGION `
  --platform managed `
  --service-account $SERVICE_ACCOUNT_EMAIL `
  --allow-unauthenticated `
  --concurrency 1 `
  --max-instances 1 `
  --timeout 1800 `
  --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=$BQ_DATASET,GCS_BUCKET=$GCS_BUCKET,GCP_REGION=$REGION,TRACKER_CRAWL_INTERVAL_HOURS=2,TRACKER_TRACKING_WINDOW_DAYS=14,TRACKER_COMMENT_LIMIT=10,TRACKER_AUTHOR_BOOTSTRAP_DAYS=14,TRACKER_MAX_VIDEOS_PER_CYCLE=2000,TRACKER_SNAPSHOT_WORKERS=1,TRACKER_TABLE_PREFIX=tracker" `
  --set-secrets "TRACKER_ADMIN_TOKEN=$TRACKER_ADMIN_SECRET_NAME:latest"
```

```powershell
$CLOUD_RUN_URL = gcloud run services describe $RUN_SERVICE --region $REGION --format="value(status.url)"
Invoke-WebRequest -Uri "$CLOUD_RUN_URL/healthz"
```

```powershell
gcloud scheduler jobs create http $SCHEDULER_JOB `
  --location $SCHEDULER_LOCATION `
  --schedule "0 */2 * * *" `
  --http-method POST `
  --uri "$CLOUD_RUN_URL/run" `
  --headers "Authorization=Bearer $TRACKER_ADMIN_TOKEN"
```

```powershell
cd "d:\Schoolworks\Thesis\bilibili-data\cloud_panel"
npm install
npx wrangler login
```

然后：
1. 手动修改 `wrangler.toml`
2. 写入 `TRACKER_ADMIN_TOKEN`
3. 创建 Worker 专用 Google SA 并导入 `GOOGLE_SERVICE_ACCOUNT_JSON`
4. 执行：

```powershell
npx wrangler deploy --dry-run
npm run deploy
```

最后去 Cloudflare Dashboard 配 Access。

---

# 附录：中途修改实践经验

## 本地对项目进行修改后：重新构建镜像 + 重新部署 Cloud Run + 验证上传接口

适用场景：
- 已经在本地修改了 `cloud_tracker/`、`src/bili_pipeline/cloud_tracker/` 或相关后端逻辑
- 需要把最新修复重新部署到线上 Cloud Run
- 部署完成后想立刻验证 `/admin/authors/upload` 是否恢复正常

建议在 `d:\Schoolworks\Thesis\bilibili-data` 目录下执行。

### 1. 进入项目目录并重新准备变量（每次重新打开都要重新准备）

```powershell
cd "D:\Schoolworks\Thesis\bilibili-data"

$PROJECT_ID = "bilibili-m2s2vp-research"
$REGION = "us-central1"
$BQ_DATASET = "bili_video_data_crawler"
$GCS_BUCKET = "bilibili-video-data-testbucket"
$RUN_SERVICE = "bilibili-cloud-tracker"
$SERVICE_ACCOUNT = "bili-tracker-sa"
$SERVICE_ACCOUNT_EMAIL = "$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com"
$IMAGE = "gcr.io/$PROJECT_ID/$RUN_SERVICE"
$TRACKER_ADMIN_SECRET_NAME = "tracker-admin-token"
```

如果你线上版本还依赖 B 站 Cookie，再额外准备：

```powershell
$SESSDATA_SECRET_NAME = "bili-sessdata"
$BILI_JCT_SECRET_NAME = "bili-jct"
$BUVID3_SECRET_NAME = "bili-buvid3"
```

---

### 2. 重新构建并上传最新镜像

```powershell
gcloud builds submit --tag $IMAGE -f cloud_tracker/Dockerfile .
```

执行成功后，说明本地最新代码已经被打进新的容器镜像。

---

### 3. 重新部署 Cloud Run

#### 3.1 不带 B 站 Cookie 的最小重部署

```powershell
gcloud run deploy $RUN_SERVICE `
  --image $IMAGE `
  --region $REGION `
  --platform managed `
  --service-account $SERVICE_ACCOUNT_EMAIL `
  --allow-unauthenticated `
  --concurrency 1 `
  --max-instances 1 `
  --timeout 1800 `
  --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=$BQ_DATASET,GCS_BUCKET=$GCS_BUCKET,GCP_REGION=$REGION,TRACKER_CRAWL_INTERVAL_HOURS=2,TRACKER_TRACKING_WINDOW_DAYS=14,TRACKER_COMMENT_LIMIT=10,TRACKER_AUTHOR_BOOTSTRAP_DAYS=14,TRACKER_MAX_VIDEOS_PER_CYCLE=2000,TRACKER_SNAPSHOT_WORKERS=1,TRACKER_TABLE_PREFIX=tracker" `
  --set-secrets "TRACKER_ADMIN_TOKEN=$TRACKER_ADMIN_SECRET_NAME:latest"
```

#### 3.2 带 B 站 Cookie 的重部署

```powershell
gcloud run deploy $RUN_SERVICE `
  --image $IMAGE `
  --region $REGION `
  --platform managed `
  --service-account $SERVICE_ACCOUNT_EMAIL `
  --allow-unauthenticated `
  --concurrency 1 `
  --max-instances 1 `
  --timeout 1800 `
  --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=$BQ_DATASET,GCS_BUCKET=$GCS_BUCKET,GCP_REGION=$REGION,TRACKER_CRAWL_INTERVAL_HOURS=2,TRACKER_TRACKING_WINDOW_DAYS=14,TRACKER_COMMENT_LIMIT=10,TRACKER_AUTHOR_BOOTSTRAP_DAYS=14,TRACKER_MAX_VIDEOS_PER_CYCLE=2000,TRACKER_SNAPSHOT_WORKERS=1,TRACKER_TABLE_PREFIX=tracker" `
  --set-secrets "TRACKER_ADMIN_TOKEN=$TRACKER_ADMIN_SECRET_NAME:latest,BILI_SESSDATA=$SESSDATA_SECRET_NAME:latest,BILI_BILI_JCT=$BILI_JCT_SECRET_NAME:latest,BILI_BUVID3=$BUVID3_SECRET_NAME:latest"
```

---

### 4. 重新获取 Cloud Run URL 并做健康检查

```powershell
$CLOUD_RUN_URL = gcloud run services describe $RUN_SERVICE --region $REGION --format="value(status.url)"
$CLOUD_RUN_URL
Invoke-WebRequest -Uri "$CLOUD_RUN_URL/healthz"
```

如果 `/healthz` 返回 JSON，说明新 revision 已经成功启动。

---

### 5. 验证作者 CSV 上传接口

假设本地作者文件路径是：

```powershell
$TRACKING_AUTHOR_CSV = "D:\Schoolworks\Thesis\bilibili-data\outputs\video_pool\bvid_to_uids\tracking_ups_v1_20260327_165321.csv"
```

如果你手头已经知道 Admin Token，可以直接赋值：

```powershell
$TRACKER_ADMIN_TOKEN = "<你的 TRACKER_ADMIN_TOKEN>"
```

然后执行上传：

```powershell
curl.exe -X POST "$CLOUD_RUN_URL/admin/authors/upload" `
  -H "Authorization: Bearer $TRACKER_ADMIN_TOKEN" `
  -F "file=@$TRACKING_AUTHOR_CSV" `
  -F "source_name=selected_authors"
```

成功时应返回类似：

```json
{"status":"ok","source_name":"selected_authors","owner_count":2952}
```

如果这里仍然返回 `500`，优先去看 Cloud Run 日志，而不是只看命令行里的 HTML 报错页。

---

### 6. 查看最新 Cloud Run 日志

```powershell
gcloud run services logs read $RUN_SERVICE --region $REGION --limit 100
```

如果你只想快速定位上传接口报错，也可以先看最近的 `/admin/authors/upload` 请求附近日志。

---

### 7. 这次修复的关键点

本次实践里，`/admin/authors/upload` 报 `500 Internal Server Error` 的真实原因不是 `curl` 命令本身，也不是本地 CSV 缺列，而是服务端在替换作者列表时执行了 BigQuery 不允许的无条件删除语句：

```sql
DELETE FROM `...author_sources`
```

BigQuery 要求 `DELETE` 必须带 `WHERE`，所以修复后需要重新构建镜像并重新部署，线上服务才会真正生效。

另外，上传接口也建议保留对 CSV 解析异常的 `400` 返回，这样以后如果文件缺少 `owner_mid` 列，就不会再表现成难以定位的 HTML `500`。