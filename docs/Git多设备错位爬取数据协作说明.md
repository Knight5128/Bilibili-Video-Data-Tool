# Git多设备错位爬取数据协作说明

## 目的

本文用于说明如何通过 Git / GitHub 在多台设备之间同步 `bilibili-data` 项目的代码、`outputs/` 目录中的爬取产物，以及 `docs/` 文档，以支持错位接力式爬取。

这里的“错位爬取”指：

- 设备 A 跑完一轮后提交并推送结果
- 设备 B 拉取最新状态后继续跑下一轮
- 两台设备不同时修改同一个进行中的任务状态

## 追踪范围

当前 `.gitignore` 已调整为允许 Git 追踪：

- `outputs/` 下全部文件
- `docs/` 下全部文件

这意味着以下内容可以被同步到 GitHub：

- `outputs/video_pool/uid_expansions/` 下的 `videolist_part_*.csv`
- `outputs/video_pool/uid_expansions/` 下的 `remaining_uids_part_*.csv`
- `uid_expansion_state.json`
- 日志文件与 summary 文件
- 其他输出目录中的 CSV、JSON、日志等结果文件

## 推荐协作原则

### 1. 同一时刻只允许一台设备继续某个任务

尤其是 `uid_expansion` 任务，同一个 session 下会同时写入：

- `uid_expansion_state.json`
- `remaining_uids_part_*.csv`
- `videolist_part_*.csv`
- `logs/`

如果两台设备同时继续同一个 session，极易出现文件内容分叉，导致：

- state 被覆盖
- `remaining_uids` 对不上
- 同一 part 编号在不同设备上产生不同结果
- Git merge 冲突难以人工判断

### 2. 每次开始爬取前先同步

在任意设备开始新一轮爬取前，先执行：

```powershell
git pull --rebase
```

确保本地拿到最新的：

- 代码逻辑
- `outputs/` 中的任务状态
- 上一台设备刚产生的剩余 UID 文件和导出结果

### 3. 每次结束后立刻提交并推送

一轮任务结束后，尽快执行：

```powershell
git add .
git commit -m "sync crawl outputs after device run"
git push
```

这样另一台设备接手时可以直接在最新状态上续跑。

## 推荐操作流程

## 场景 A：设备 A 跑完，设备 B 接着跑

### 设备 A

1. 先同步仓库

```powershell
git pull --rebase
```

2. 运行一次爬取任务

3. 确认 `outputs/` 中已生成最新文件，例如：

- `videolist_part_n.csv`
- `remaining_uids_part_n.csv`
- `uid_expansion_state.json`
- 对应日志文件

4. 提交并推送

```powershell
git add .
git commit -m "sync uid expansion outputs after part n"
git push
```

### 设备 B

1. 拉取最新状态

```powershell
git pull --rebase
```

2. 打开 DataHub 或相关脚本

3. 使用刚同步下来的文件继续：

- 如果是中断续跑，优先使用 `remaining_uids_part_n.csv`
- 如果是重新补抓整批作者，使用 `original_uids.csv` 新开 session

4. 跑完后继续提交并推送

```powershell
git add .
git commit -m "continue uid expansion on device B"
git push
```

## `uid_expansion` 的建议做法

### 中断续跑

如果你只是接着上一次未完成的 session 继续跑：

- 上传对应 session 中最新的 `remaining_uids_part_n.csv`
- 继续生成新的 `part_(n+1)`
- 跑完后提交 `outputs/` 变化

这是最适合双设备接力的方式。

### 重新补抓

如果你想让一批作者重新按新的窗口规则补抓：

- 使用该 session 的 `original_uids.csv`
- 创建一个新的 `uid_expansion` session
- 跑完后再提交

这种方式适合：

- 修复了时间窗口逻辑后重新补数据
- 想从更早日期重新覆盖抓取
- 不希望污染旧 session 的续跑链路

## 提交信息建议

建议把“代码改动”和“数据同步”分开提交。

### 代码改动

```powershell
git add bvp-builder.py src/ docs/
git commit -m "fix uid expansion history start resolution"
```

### 数据同步

```powershell
git add outputs/
git commit -m "sync outputs after crawl on laptop"
```

这样后续回看历史时更容易分辨：

- 哪次提交改了逻辑
- 哪次提交只是同步了爬取结果

## 冲突处理建议

如果 `git pull --rebase` 时出现冲突，优先检查这些文件：

- `outputs/video_pool/uid_expansions/*/uid_expansion_state.json`
- `outputs/video_pool/uid_expansions/*/remaining_uids_part_*.csv`
- `outputs/video_pool/uid_expansions/*/videolist_part_*.csv`
- 各类 `.log`

处理原则：

- 如果只有一台设备在继续某个 session，理论上不应频繁冲突
- 如果发生冲突，通常说明两台设备曾同时改动同一任务
- 这时应先停下，人工确认哪台设备的结果应保留

## 仓库体积注意事项

由于现在 `outputs/` 全部纳入版本管理，仓库体积会持续增长。请注意：

- CSV 和日志文件会快速增大仓库历史
- 拉取和推送速度会逐渐变慢
- 单文件过大时可能接近 GitHub 限制

如果后续 `outputs/` 体积明显变大，建议考虑：

- 定期归档旧任务目录
- 拆分数据仓库与代码仓库
- 使用 Git LFS 管理超大文件

## 最简执行模板

每次切换设备时，按下面顺序执行即可：

### 接手前

```powershell
git pull --rebase
```

### 爬取后

```powershell
git add .
git commit -m "sync outputs after crawl"
git push
```

## 一句话规则

可以把这套协作方式记成一句话：

**先拉取，再爬取；爬完立即提交推送；同一个 session 不双机并发。**
