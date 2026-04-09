# EmbyCleaner

一个面向 Emby 媒体库的轻量清理面板：
**先全量同步到本地缓存，再做查重/洗版/缺陷扫描，最后由你确认处理。**

它的定位很明确：
- 适合家庭媒体库、私有整理场景
- 提供 Web UI、计划任务、Webhook 通知
- 支持删除、忽略、刷新元数据
- 不做公网多用户后台，不内建权限系统

> ⚠️ 会涉及真实删除。先备份，再用。不要直接裸露到公网。

---

## 现在支持什么

### 扫描模式

- **番号查重（av）**
  - 从名称/路径提取番号后分组
  - 适合同片多版本整理
- **智能洗版（smart）**
  - 按同名资源分组
  - 结合分辨率、体积、海报状态给出推荐保留项
- **同大查重（size）**
  - 按文件大小分组
  - 适合抓硬重复、副本
- **时长查重（duration）**
  - 仅在文件所在目录内按时长分组
  - 同目录内按 1 位小数精度分组（四舍五入到 0.1 秒）
  - 同目录内落在同一 0.1 秒分组的资源视为重复
  - 适合同目录下不同文件名但内容近似相同的资源
- **缺失封面（noposter）**
  - 列出没有 Primary 海报的项目
- **极小文件（tiny）**
  - 找异常小文件，也可叠加时长阈值

### 运行能力

- FastAPI Web 面板
- Emby 全量同步到本地 SQLite
- 系统级定时全量同步（cron）
- 任务级定时扫描（cron）
- 手动立即执行任务
- 批量删除 / 批量忽略
- Webhook 通知
- 健康检查接口

---

## 工作原理

### 1. 全量同步链路

全量同步会：

1. 用配置里的用户名/密码调用 Emby 登录接口
2. 获取 AccessToken
3. 拉取媒体库列表
4. 遍历媒体项并写入本地 `media_items`
5. 缓存这些字段：
   - Emby ID
   - 名称
   - 路径
   - 分辨率
   - 文件大小
   - 时长
   - 海报状态
   - 所属库 ID
   - 创建时间
   - 标签识别结果（`C` / `UC` / `U`）

扫描任务默认都基于**本地缓存**执行，不是每次直接全库打 Emby API。
这样更稳，也更快。

### 2. 调度链

当前代码里有两类定时机制：

#### A. 系统同步定时（`cron_sync`）

- 配在全局配置里
- 用于定时触发一次**全量同步**
- 调度循环按分钟对齐检查
- 同一分钟内有去重保护，避免重复触发

#### B. 审计任务定时（`/api/tasks`）

- 每个任务单独配置 `cron`
- 到点后执行对应扫描模式
- 会回填任务状态：
  - `last_run`
  - `last_status`
  - `last_found`
  - `last_message`
  - `last_duration_ms`
- 手动执行和定时执行现在走的是**同一套任务执行链**，状态口径一致

### 3. cron 行为说明

- 使用 5 段标准 cron 表达式（分 时 日 月 周）
- 调度循环是按分钟粒度跑的，不支持秒级调度
- 配置保存时会做基础校验
- 无效 cron 不会执行，任务状态会标记为错误

示例：

```cron
*/30 * * * *
0 4 * * *
15 3 * * 1
```

---

## 配置项说明

配置保存在 `/app/data/emby.db` 里的 `configs` 表。

### 基础连接配置

- `host`
  - Emby 地址
  - 例：`http://192.168.1.10:8096`
- `user`
  - Emby 用户名
- `pwd`
  - Emby 密码
- `webhook_url`
  - 可选，任务通知地址
- `cron_sync`
  - 可选，系统全量同步 cron
- `ssl_verify`
  - 是否校验证书
  - 默认 `true`
  - 关闭后会走不校验证书的 HTTP 客户端

### 推荐策略偏好

- `pref.av.keep_priority`
  - `tag_uc` / `tag_c` / `tag_raw`
- `pref.size.keep`
  - `path_long` / `path_short` / `name_long` / `name_short`
- `pref.duration.keep`
  - `min` / `max`
- `pref.smart.keep`
  - `reso_max` / `reso_min`
- `pref.batch.confirm`
  - `true` / `false`

### 运行态统计

- `last_sync_ts`
- `cleaned_count`
- `saved_space`

---

## API 概览

主要接口：

- `GET /api/health`
  - 健康检查
- `GET /api/status`
  - 服务状态、连接状态、同步状态、统计信息
- `GET /api/config`
- `POST /api/config`
- `GET /api/libraries`
- `POST /api/sync`
  - 触发一次全量同步
- `GET /api/scan`
  - 执行扫描并返回序列化结果
- `GET /api/tasks`
- `POST /api/tasks`
- `PUT /api/tasks/{id}`
- `DELETE /api/tasks/{id}`
- `POST /api/tasks/{id}/run`
  - 立即执行任务
- `GET /api/ignore`
- `POST /api/ignore`
- `DELETE /api/ignore/{row_id}`
- `POST /api/delete`
- `POST /api/refresh`
- `POST /api/test_webhook`
- `GET /api/logs`
- `POST /api/logs/clear`

---

## 部署

### Docker Compose

```yaml
services:
  embycleaner:
    image: okhao/emby_cleaner:latest
    container_name: embyclean
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
docker compose up -d
```

打开：

```text
http://<你的主机IP>:19898
```

### 本地 Python 运行

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 19898
```

---

## 首次使用建议流程

1. 打开页面
2. 填写 Emby 地址、用户名、密码
3. 可选填写 Webhook 和系统同步 cron
4. 保存配置
5. 手动执行一次全量同步
6. 先跑扫描，确认推荐结果
7. 再决定是否删除/忽略
8. 最后再开定时任务

别一上来就配自动删。
先看命中结果是不是符合你库里的命名习惯。

---

## 删除、忽略、刷新是怎么干的

### 删除

- 删除调用的是 Emby 接口：`DELETE /Items/{id}`
- 后台并发执行
- 成功后会同步删本地缓存记录
- 会累计 `cleaned_count` 和 `saved_space`
- 删除汇总会延迟聚合后再发 webhook

### 忽略

- 忽略是**按扫描模式隔离**的，不是全局一个黑名单
- 同一个媒体项可以在某种模式里忽略，但在另一种模式继续参与扫描

### 刷新

- 调用 Emby 的 Refresh 接口
- 成功后会延迟做一次本地校准，更新封面/时间/大小等字段

---

## 已知限制

这块尽量说实话。

1. **没有内建鉴权**
   - 谁能打开页面，谁就能操作
   - 不适合直接公网暴露

2. **主要面向 Emby**
   - 虽然项目风格接近 Emby/Jellyfin 场景
   - 但当前 API 实现按 Emby 接口写的
   - Jellyfin 不能保证直接兼容

3. **扫描基于本地缓存，不是实时直连库**
   - 如果刚改完媒体库，没同步，结果可能旧

4. **推荐规则是启发式，不是绝对正确**
   - 尤其是智能洗版、番号识别、时长归组
   - 仍然需要人工确认

5. **定时器是分钟粒度**
   - 不是秒级任务系统

6. **删除结果受 Emby 后端行为影响**
   - 如果 Emby 端删除策略、挂载权限、媒体路径有问题，接口可能返回失败

7. **SQLite 适合单实例**
   - 不建议多实例同时写同一个数据目录

---

## 排障口径

### 页面能打开，但状态一直离线

优先查：

- `host` 是否写对
- 用户名密码是否正确
- Emby 是否能从当前容器访问到
- 是否 HTTPS 自签证书问题
  - 必要时检查 `ssl_verify`

### 保存配置报 cron 错误

说明表达式格式不合法。
请改成标准 5 段 cron。

### 扫描结果为空

常见原因：

- 还没做全量同步
- 目标媒体库本来就没有命中当前规则
- 该模式下项目被忽略过
- 刚改完库，但本地缓存还没刷新

### 定时任务不执行

优先查：

- 任务是否启用
- `cron` 是否合法
- 当前时间是否真的命中该分钟
- `last_run` 是否刚执行过，触发了 60 秒去重保护
- 查看 `/api/logs`

### 删除失败

优先查：

- Emby 用户权限是否足够
- Emby 后端是否允许删除
- 挂载路径是否可写
- 媒体是否被占用

### Webhook 没收到

优先查：

- `webhook_url` 是否可达
- 目标端是否接受 JSON：
  - `title`
  - `text`
- 先调用一次 `POST /api/test_webhook`

---

## 安全建议

最重要的几条：

1. **不要直接把 19898 暴露到公网**
2. 放到内网、VPN、Tailscale、ZeroTier，或者反代鉴权后面
3. 不要把 `/app/data/emby.db` 提交进仓库
4. 不要把 token、密码、私钥写进 Git remote、README、compose 示例
5. 如果你已经泄露凭证，立刻轮换

---

## 项目结构

```text
.
├── app.py
├── core/
│   ├── db.py
│   ├── responses.py
│   └── schemas.py
├── services/
│   ├── scanner.py
│   └── scheduler.py
├── templates/
│   └── index.html
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## 构建与发布

本地构建：

```bash
docker build -t okhao/emby_cleaner:latest .
```

如果你维护 Docker Hub：

```bash
docker push okhao/emby_cleaner:latest
```

GitHub 默认主分支：`main`

---

## 免责声明

这个工具会对你的媒体库执行扫描、忽略、刷新、删除等操作。
**任何误删、误判、数据损失，责任都在使用者自己。**

一句话：
**先备份，再清理。**

---

## License

[MIT](./LICENSE)
