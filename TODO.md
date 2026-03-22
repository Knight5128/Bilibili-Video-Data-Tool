# 文档目的

此文档只用作我-User的个人笔记及待办备忘录，任何Agent在浏览到此文档时直接略过即可。

# 我的待办清单

- bilibili video pool crawler的后续改进：
    - ✅去重模块
    - ✅新模块A:上传作者uid csv，抓取他最近k天的视频
    - ✅新模块B：上传视频uid csv，抓取其作者近k天的所有视频
    - ✅单bvid网页链接跳转
- bilibili video data crawler的后续改进：
    - ✅单bvid网页链接跳转
    - ✅把对于一个bvid所实时抓取的所有属性都列出来
    - ✅抓取字段的定义.md文档+前端页面（先定义好，后面要**持续更新**）
    - ✅选好数据库
    - ❓设置视频下载长度截断（或者直接忽略视频长度超过一定时长的音视频模态，而只关注其时长属性，下周一问sj）
    - 探索更多视频元数据及上传者个人元数据

# 补充-B站url格式

- 单一视频播放: https://www.bilibili.com/video/<bvid>
- 单个用户/UP主个人主页: https://space.bilibili.com/<owner_mid>
- 单个UP主的视频列表: https://space.bilibili.com/<owner_mid>/upload/video