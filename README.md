# EmbyCleaner

一个给 Emby/Jellyfin 风格媒体库做“查重、洗版、清理、计划任务”的轻量 Web 工具。

它不是那种花里胡哨的演示玩具，核心思路很直接：
**先把媒体库索引进本地数据库，再按不同规则找出可疑重复项，最后由你确认处理。**

> ⚠️ 这玩意会涉及真实删除媒体文件。别把它直接裸奔到公网，更别闭眼乱点批量删除。

---

## 功能概览

### 扫描模式

- **番号查重**：按 AV 番号归组，适合找同片多版本
- **智能洗版**：同名资源分组，对比分辨率/体积，保留更优版本
- **同大查重**：按文件大小归组，快速抓完全重复副本
- **时长查重**：按播放时长归组，适合不同文件名的同内容资源
- **缺失封面**：列出没有海报的条目
- **极小文件**：查找异常小文件、样片、残片之类垃圾

### 其他能力

- Web 管理界面
- 手动全量同步 Emby 库
- 定时扫描任务（cron）
- 删除前确认
- 忽略列表 / 黑名单
- Webhook 通知
- 本地 SQLite 缓存，避免每次都全量打 Emby API

---

## 技术栈

- **Backend**: FastAPI
- **DB**: SQLite
- **ORM**: SQLAlchemy
- **Frontend**: 原生 HTML + JS 模板页
- **Deploy**: Docker / Docker Compose

---

## 目录结构

```text
.
├── app.py                 # FastAPI 入口
├── core/
│   ├── db.py              # 数据库模型与配置存取
│   ├── responses.py       # 通用响应封装
│   └── schemas.py         # 请求模型
├── services/
│   ├── scanner.py         # 扫描与推荐逻辑
│   └── scheduler.py       # cron 匹配逻辑
├── templates/
│   └── index.html         # 前端页面
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## 运行方式

### 方式一：Docker Compose

```yaml
services:
  embycleaner:
    container_name: embyclean
    build:
      context: .
    image: okhao/emby_cleaner:dev
    ports:
      - "19898:19898"
    volumes:
      - ./data:/app/data
    environment:
      TZ: Asia/Shanghai
    restart: unless-stopped
    init: true
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://127.0.0.1:19898/api/health"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 20s
```

启动：

```bash
docker compose up -d --build
```

打开：

```text
http://你的服务器IP:19898
```

---

### 方式二：本地 Python 运行

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 19898
```

---

## 首次配置

进入页面后，先填写这些配置：

- **Emby Host**：你的 Emby 地址
  - 例如：`http://192.168.1.10:8096`
- **User**：Emby 用户名
- **Password**：Emby 密码
- **Webhook**：可选，任务通知地址
- **Sync Cron**：可选，全量同步的 cron 表达式

保存后，程序会：

1. 用用户名密码向 Emby 登录
2. 获取 AccessToken
3. 拉取媒体库和条目索引到本地 SQLite

### 配置说明

程序会把配置写入本地数据库（`/app/data/emby.db`），主要字段包括：

- `host`
- `user`
- `pwd`
- `webhook_url`
- `cron_sync`
- 一些扫描推荐偏好项

> 注意：当前版本**没有内建鉴权系统**，也不是多用户后台。谁能打开页面，谁就能操作你的媒体库。别把它暴露到公网。

---

## 扫描逻辑说明

### 1) 番号查重

从名称或路径里提取番号，按番号分组。
适合日系资源、多版本整理场景。

### 2) 智能洗版

按媒体名称归组，对比：

- 分辨率
- 体积
- 海报状态

用于挑出“更像该保留版本”的条目。

### 3) 同大查重

按文件大小归组。
同体积文件通常是硬重复、重复刮削或多路径副本。

### 4) 时长查重

按时长分组（保留两位小数）。
适合不同命名但内容几乎相同的视频。

### 5) 缺失封面

列出没有 `Primary` 海报的项目。

### 6) 极小文件

找小于阈值的文件，也可叠加时长阈值。
适合清理样片、花絮残留、错误切片。

---

## 删除与推荐策略

程序不会只会傻删，它会根据偏好给出“推荐保留 / 推荐删除”：

- 番号模式：可优先保留 `UC` / `C` / 原版
- 同大模式：可按路径长短、名称长短等规则决定保留项
- 时长模式：可选保留最大或最小文件
- 智能模式：可选偏向最高分辨率或最低分辨率

但说到底，**推荐只是推荐，不是神谕**。批量删之前自己看一眼，别回头怪工具太诚实。

---

## API 概览

部分接口如下：

- `GET /api/health`：健康检查
- `GET /api/status`：状态概览
- `GET /api/config`：读取配置
- `POST /api/config`：保存配置
- `GET /api/libraries`：读取媒体库
- `POST /api/sync`：触发全量同步
- `POST /api/scan`：执行扫描
- `POST /api/delete`：删除条目
- `POST /api/ignore`：加入忽略列表
- `POST /api/test_webhook`：测试通知

如果你要反向代理或做外层鉴权，可以基于这些接口接自己的网关。

---

## 数据与持久化

默认数据目录：

```text
/app/data
```

其中主要是：

- `emby.db`
- `emby.db-shm`
- `emby.db-wal`

这些文件属于**运行期数据**，不应该提交进 Git。

---

## 安全建议

这段很重要，别装瞎：

1. **不要把 19898 端口直接暴露到公网**
2. 最好放在内网、Tailscale、ZeroTier、VPN 或反向代理鉴权后面
3. 不要把 `.env`、数据库、token、私钥之类文件提交到仓库
4. Git remote 不要带明文 token
5. 如果你已经把 token 发出来或写进远端 URL，**立刻撤销重建**

---

## 开发备注

### 构建镜像

```bash
docker build -t okhao/emby_cleaner:test .
```

### 推送开发镜像

仓库自带脚本：

```bash
bash push-dev.sh
```

脚本会：

1. 构建 `okhao/emby_cleaner:test`
2. 推送到镜像仓库

---

## 常见问题

### 1. 为什么页面能开，但扫描没结果？

通常是下面几种破事：

- Emby 地址填错
- 用户名/密码不对
- Emby API 连不上
- 还没先做全量同步
- 目标库里本来就没命中当前扫描规则

### 2. 为什么要本地缓存数据库？

因为每次都直接全量打 Emby API 很蠢，慢，还容易把服务打烦。
本地缓存后，扫描速度和交互体验会稳定很多。

### 3. 这东西支持公开访问吗？

不建议。
当前版本不是为公网多用户安全场景设计的。

---

## 免责声明

本项目会对你的媒体库执行查重、忽略、删除等操作。
虽然有确认与推荐逻辑，但**删除造成的损失由使用者自己承担**。

换句人话：
**先备份，后清理。别拿生产库裸测。**

---

## License

Released under the [MIT License](LICENSE).
