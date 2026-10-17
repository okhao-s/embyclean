import os, re, logging, asyncio, httpx, time, collections, uuid
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import suppress
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, or_
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from core.db import init_db, SessionLocal, Config, IgnoredItem, AuditTask, MediaItem, get_conf, set_conf
from core.schemas import DeleteRequest, IgnoreRequest, RefreshRequest, TaskReq, ConfigRequest
from core.responses import ok, err
import services.scanner as scanner_service
from services.scanner import MODE_MAP
from services.scheduler import cron_matches, is_valid_cron

app = FastAPI()
# 扫描结果动辄 5-8MB JSON，gzip 后小一个量级；大库时前端等待时间明显下降
app.add_middleware(GZipMiddleware, minimum_size=1024)
init_db()
templates = Jinja2Templates(directory="templates")

# --- 日志系统 ---
log_buffer = []
log_base = 0          # 被裁掉的行数，前端靠它做增量拉取
LOG_BUFFER_MAX = 20000
class MemoryHandler(logging.Handler):
    def emit(self, record):
        global log_base
        try:
            msg = self.format(record); ts = datetime.now().strftime("%H:%M:%S")
            log_buffer.append(f"[{ts}] {msg}")
            if len(log_buffer) > LOG_BUFFER_MAX:
                log_buffer.pop(0); log_base += 1
        except Exception:
            pass

logger = logging.getLogger("EmbyCleaner")
logger.setLevel(logging.INFO); logger.addHandler(logging.StreamHandler())
mem = MemoryHandler(); logger.addHandler(mem)
def sys_log(msg: str): logger.info(msg)

# --- Webhook ---
class WebhookBuffer:
    def __init__(self):
        self.count = 0; self.size = 0; self.timer_task = None; self.lock = asyncio.Lock()
    async def add(self, size_bytes):
        async with self.lock:
            if size_bytes <= 0:
                return
            self.count += 1; self.size += size_bytes
            if not self.timer_task: self.timer_task = asyncio.create_task(self._flush_later())
    async def _flush_later(self):
        await asyncio.sleep(5)
        async with self.lock:
            count, size = self.count, self.size
            self.count = 0; self.size = 0; self.timer_task = None
        # 发送放在锁外：webhook 慢(最长 20s)时不能卡住删除流程里的 add()
        if count > 0:
            await self._send(count, size)
    async def _send(self, count, size):
        sz = f"{size/1048576:.2f} MB" if size < 1073741824 else f"{size/1073741824:.2f} GB"
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = f"🛰️ **EmbyCleaner 任务报告**\n━━━━━━━━━━━━━━\n⏱️ 时间: {ts}\n📦 数量: {count} 个项目\n💾 空间: {sz}\n━━━━━━━━━━━━━━\n✅ 清理执行完毕"
        await send_webhook_standalone("清理完成", msg, raw=True)

wb_buffer = WebhookBuffer()

sync_lock = asyncio.Lock(); global_token = ""; current_sync_lib = ""; last_sync_trigger_slot = ""
# Emby 侧并发要克制：一次删/刷太多会让 Emby 的库监视+元数据队列爆掉，表现就是 Emby 卡死
DELETE_CONCURRENCY = int(os.getenv("DELETE_CONCURRENCY", "4"))
REFRESH_CONCURRENCY = int(os.getenv("REFRESH_CONCURRENCY", "2"))
# 删除走同步返回：批量删除整体最多等这么久，超时回 504 让前端提示重新扫描核对
DELETE_TIMEOUT = float(os.getenv("DELETE_TIMEOUT", "180"))
# 单次请求上限，避免一次点几百个把 Emby 锁死
DELETE_MAX_BATCH = int(os.getenv("DELETE_MAX_BATCH", "500"))
# 后台操作任务表（delete / refresh 共用）：接口立即返回 job_id，
# 前端轮询 /api/ops/status 拿进度与真实成败，多个任务可在前端叠加成一条进度条
DELETE_JOBS: dict = {}
DELETE_JOB_TTL = float(os.getenv("DELETE_JOB_TTL", "900"))
DELETE_JOB_KEEP = 20
REFRESH_MAX_BATCH = int(os.getenv("REFRESH_MAX_BATCH", "200"))
STATUS_LOG_INTERVAL = 300
# /api/status 远端探测缓存（秒）：前端 5s 轮询一次，没有缓存会把 Emby 打满
STATUS_PROBE_TTL = float(os.getenv("STATUS_PROBE_TTL", "15"))
_status_probe_cache = {"key": "", "at": 0.0, "data": None}
last_status_log_at = {}

# Emby 请求统计：/api/status 里能看到最近 60s 打了多少请求，方便判断是谁把 Emby 压死的
EMBY_STATS = {"total": 0, "errors": 0, "recent": collections.deque(maxlen=2000)}


async def _emby_on_request(request):
    # AsyncClient 的 event hook 必须是可 await 的协程，同步函数会报
    # "'NoneType' object can't be awaited" 并把所有 Emby 请求打挂
    EMBY_STATS["total"] += 1
    EMBY_STATS["recent"].append(time.time())


async def _emby_on_response(response):
    if response.status_code >= 400:
        EMBY_STATS["errors"] += 1


def emby_rpm() -> int:
    cutoff = time.time() - 60
    recent = EMBY_STATS["recent"]
    while recent and recent[0] < cutoff:
        recent.popleft()
    return len(recent)


# 连接/读取超时压短：Emby 卡住时不能让请求堆在 120s 上，否则越打越死
EMBY_TIMEOUT = httpx.Timeout(float(os.getenv("EMBY_TIMEOUT", "60")), connect=5.0)
EMBY_LIMITS = httpx.Limits(max_connections=10, max_keepalive_connections=5)
client = httpx.AsyncClient(timeout=EMBY_TIMEOUT, verify=True, trust_env=False, limits=EMBY_LIMITS,
                           event_hooks={"request": [_emby_on_request], "response": [_emby_on_response]})
insecure_client = httpx.AsyncClient(timeout=EMBY_TIMEOUT, verify=False, trust_env=False, limits=EMBY_LIMITS,
                                    event_hooks={"request": [_emby_on_request], "response": [_emby_on_response]})
# Emby 走内网：trust_env=False，别让宿主/容器的 HTTP_PROXY、ALL_PROXY 把内网请求劫走
# （容器里设了 ALL_PROXY=socks5://... 又没装 socksio 时，httpx 在 import 阶段就直接抛 ImportError）
# Webhook 可能指向公网：单独一个客户端，尽量沿用环境代理。
# 但如果环境里配了 socks 代理而没装 socksio，httpx 在构造阶段就会抛 ImportError，
# 整个进程连 import 都过不去 —— 这里退化为不走代理，而不是让服务起不来。
def _make_webhook_client():
    try:
        return httpx.AsyncClient(timeout=20.0)
    except Exception as exc:
        sys_log(f"[WEBHOOK] ⚠️ 环境代理不可用（{exc}），通知改为直连")
        return httpx.AsyncClient(timeout=20.0, trust_env=False)

webhook_client = _make_webhook_client()
EMBY_HEADERS = {"X-Emby-Client": "Cleaner", "X-Emby-Device-Name": "Server", "X-Emby-Device-Id": "v1.2-Precision", "X-Emby-Client-Version": "4.9"}

def ssl_verify_enabled(db):
    return str(get_conf(db, "ssl_verify") or "true").strip().lower() not in {"0", "false", "no", "off"}

def emby_client(db):
    return client if ssl_verify_enabled(db) else insecure_client

def emby_headers(token: str):
    return EMBY_HEADERS | {"X-Emby-Token": token}

def log_status_once(key: str, message: str):
    now = time.time()
    last = last_status_log_at.get(key, 0)
    if now - last >= STATUS_LOG_INTERVAL:
        last_status_log_at[key] = now
        sys_log(message)


def utc_now_ts() -> str:
    return str(datetime.now(timezone.utc).timestamp())

def utc_now_ts_float() -> float:
    return datetime.now(timezone.utc).timestamp()


def invalidate_runtime_state(reset_sync_progress: bool = False):
    global global_token, current_sync_lib
    global_token = ""
    _status_probe_cache.update({"key": "", "at": 0.0, "data": None})
    if reset_sync_progress:
        current_sync_lib = ""


def _perform_scan(mode: str, lib: str = "", param_s: str = "100", param_d: str = "0", duration_scope: str = "dir", duration_precision: str = "second"):
    db = SessionLocal()
    try:
        return scanner_service.perform_internal_scan(db, mode, lib, param_s, param_d, duration_scope, duration_precision)
    finally:
        db.close()


def _chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


PRUNE_MIN_INTERVAL = float(os.getenv("PRUNE_MIN_INTERVAL", "300"))
_last_prune_at = 0.0


async def prune_missing_media_items(candidate_ids: List[str], trigger: str = "scan") -> int:
    global _last_prune_at
    ids = [str(x).strip() for x in candidate_ids if str(x).strip()]
    if not ids:
        return 0
    # 每 5 分钟最多反查一次远端，且同步期间不掺和：扫描很频繁时这个反查会把 Emby 打成筛子
    now = time.time()
    if now - _last_prune_at < PRUNE_MIN_INTERVAL or sync_lock.locked():
        return 0
    _last_prune_at = now

    db = SessionLocal()
    try:
        h, t = get_conf(db, "host"), await get_token(db)
        if not h or not t:
            return 0

        missing_ids = set()
        for chunk in _chunked(ids, 100):
            try:
                res = await emby_client(db).get(
                    f"{h.rstrip('/')}/emby/Items",
                    params={"Ids": ",".join(chunk), "Fields": "Path"},
                    headers=emby_headers(t),
                )
            except Exception as e:
                sys_log(f"[PRUNE] ⚠️ {trigger} 反查远端存在性失败: {e}")
                return 0

            if res.status_code != 200:
                sys_log(f"[PRUNE] ⚠️ {trigger} 反查远端存在性失败: HTTP {res.status_code}")
                return 0

            remote_ids = {str(x.get("Id")) for x in res.json().get("Items", []) if x.get("Id")}
            missing_ids.update(set(chunk) - remote_ids)

        if not missing_ids:
            return 0

        # 先收集孤儿记录的 size，再删除
        orphan_items = db.query(MediaItem).filter(MediaItem.emby_id.in_(list(missing_ids))).all()
        orphan_size = sum(getattr(i, 'size', 0) or 0 for i in orphan_items)
        deleted = db.query(MediaItem).filter(MediaItem.emby_id.in_(list(missing_ids))).delete(synchronize_session=False)
        # 将孤儿记录的 size 也计入 saved_space，保证统计完整
        if orphan_size > 0:
            cfg_ss = db.query(Config).filter(Config.key == "saved_space").first()
            if cfg_ss:
                cfg_ss.value = str(int(cfg_ss.value or 0) + orphan_size)
            else:
                db.add(Config(key="saved_space", value=str(orphan_size)))
        # 同时更新 cleaned_count
        cfg_cc = db.query(Config).filter(Config.key == "cleaned_count").first()
        if cfg_cc:
            cfg_cc.value = str(int(cfg_cc.value or 0) + deleted)
        else:
            db.add(Config(key="cleaned_count", value=str(deleted)))
        db.commit()
        sys_log(f"[PRUNE] 🧹 {trigger} 清掉 {deleted} 条远端已不存在的本地缓存 (含 {orphan_size/1048576:.1f} MB)")
        return deleted
    finally:
        db.close()


async def perform_scan_async(mode: str, lib: str = "", param_s: str = "100", param_d: str = "0", duration_scope: str = "", duration_precision: str = ""):
    if mode == 'duration':
        duration_scope = (duration_scope or '').strip().lower()
        duration_precision = (duration_precision or '').strip().lower()
        pref_db = SessionLocal()
        try:
            if not duration_scope:
                duration_scope = get_conf(pref_db, 'pref.duration.scope') or 'dir'
            if not duration_precision:
                duration_precision = get_conf(pref_db, 'pref.duration.precision') or 'second'
        finally:
            pref_db.close()
    findings = await asyncio.to_thread(_perform_scan, mode, lib, param_s, param_d, duration_scope or 'dir', duration_precision or 'second')
    candidate_ids = [item.emby_id for group in findings for item in group.get("items", []) if getattr(item, "emby_id", "")]
    if candidate_ids:
        pruned = await prune_missing_media_items(candidate_ids, trigger=f"scan:{mode}")
        if pruned > 0:
            findings = await asyncio.to_thread(_perform_scan, mode, lib, param_s, param_d, duration_scope, duration_precision or 'second')
    return findings


async def execute_audit_task(db, task: AuditTask, trigger: str = "计划"):
    started_at = time.time()
    task.last_run = utc_now_ts()
    db.commit()
    findings = await perform_scan_async(task.mode, task.libraries)
    count = sum(len(f["items"]) for f in findings) if findings else 0
    task.last_found = count
    task.last_duration_ms = int((time.time() - started_at) * 1000)
    if count > 0:
        task.last_status = "matched"
        task.last_message = f"发现 {count} 个待处理项"
        db.commit()
        await send_webhook(db, f"{trigger}任务: {task.name}", f"模式: {MODE_MAP.get(task.mode, task.mode)}\n发现: {count} 个待处理项\n耗时: {task.last_duration_ms}ms")
    else:
        task.last_status = "clean"
        task.last_message = "未发现待处理项"
        db.commit()
    return count


def serialize_findings(findings):
    res = []
    for f in findings:
        items = []
        for x in f.get("items", []):
            # sync_gen 是同步内部标记，不往外吐，省点带宽
            row = {c.name: getattr(x, c.name) for c in x.__table__.columns if c.name != "sync_gen"}
            row.update({
                'display_path': os.path.dirname(x.path) + "/" if x.path else "",
                'recommend_action': getattr(x, 'recommend_action', ''),
                'recommend_reason': getattr(x, 'recommend_reason', ''),
            })
            items.append(row)
        res.append({
            "title": f.get("title", ""),
            "group_key": f.get("group_key", ""),
            "ignore_scope": f.get("ignore_scope", "item"),
            "group_meta": f.get("group_meta", {}),
            "items": items,
            "summary": {
                "keep": sum(1 for i in items if i.get('recommend_action') == 'keep'),
                "delete": sum(1 for i in items if i.get('recommend_action') == 'delete'),
                "total": len(items),
            }
        })
    return res

async def get_token(db, force=False):
    global global_token
    if global_token and not force:
        return global_token
    h, u, p = get_conf(db, "host"), get_conf(db, "user"), get_conf(db, "pwd")
    if not h or not u:
        global_token = ""
        return ""
    try:
        r = await emby_client(db).post(
            f"{h.rstrip('/')}/Users/AuthenticateByName",
            json={"Username": u, "Pw": p},
            headers=EMBY_HEADERS,
            timeout=10.0,
        )
        if r.status_code == 200:
            global_token = r.json().get("AccessToken", "")
            return global_token
        global_token = ""
        log_status_once("auth_http_error", f"[AUTH] ❌ Emby 登录失败: HTTP {r.status_code}")
    except Exception as e:
        global_token = ""
        log_status_once("auth_exception", f"[AUTH] ❌ Emby 登录异常: {e}")
    return ""

async def _scheduled_sync():
    try:
        await do_sync("计划")
    except Exception as e:
        sys_log(f"[SCHED] ❌ 计划同步异常: {e}")

async def send_webhook_standalone(command, detail, raw=False):
    db = SessionLocal()
    try:
        await send_webhook(db, command, detail, raw)
    finally:
        db.close()

async def send_webhook(db, command, detail, raw=False):
    url = get_conf(db, "webhook_url")
    if not url: return
    text_content = detail if raw else f"🛰️ **EmbyCleaner 通知**\n```\n[任务] : {command}\n[详情] : {detail}\n```"
    try:
        await webhook_client.post(url, json={"title": f"EmbyCleaner: {command}", "text": text_content})
    except Exception as e:
        sys_log(f"[WEBHOOK] ❌ 发送失败: {e}")

async def do_sync(trigger="手动"):
    global current_sync_lib
    if sync_lock.locked():
        sys_log(f"[SYNC] ⚠️ 已有同步进行中，忽略本次 {trigger} 请求")
        return False
    async with sync_lock:
        current_sync_lib = "初始化..."
        sys_log(f"[SYNC] >>> {trigger}同步启动...")
        db = SessionLocal()
        try:
            h, t = get_conf(db, "host"), await get_token(db)
            if not h or not t:
                sys_log("[SYNC] ❌ Emby 未配置或授权失败")
                return False
            res = await emby_client(db).get(f"{h.rstrip('/')}/Library/MediaFolders", headers=emby_headers(t))
            if res.status_code != 200:
                sys_log(f"[SYNC] ❌ 获取媒体库失败: HTTP {res.status_code}")
                return False
            libs = res.json().get("Items", [])
            # 不再先 DELETE：新一次同步写新的 sync_gen，全部灌完再清旧代。
            # 否则同步窗口内扫描恒空，中途失败整个缓存就没了。
            sync_gen = int(time.time())
            tot = 0; seen_ids = set()
            for l in libs:
                lib_id = l['Id']; lib_name = l.get('Name', 'Unknown'); current_sync_lib = lib_name; start_index = 0
                while True:
                    params = {"ParentId": lib_id, "Recursive": "true", "IncludeItemTypes": "Movie,Video,Series", "Fields": "Path,MediaSources,ImageTags,DateCreated,ProviderIds", "StartIndex": start_index, "Limit": 1000}
                    try:
                        res_items = await emby_client(db).get(f"{h.rstrip('/')}/emby/Items", params=params, headers=emby_headers(t))
                        if res_items.status_code != 200:
                            sys_log(f"[SYNC] ❌ 拉取媒体项失败 [{lib_name}] start={start_index}: HTTP {res_items.status_code}")
                            break
                        data = res_items.json(); items = data.get("Items", []); total_count = data.get("TotalRecordCount", 0)
                    except Exception as e:
                        sys_log(f"[SYNC] ❌ 拉取媒体项失败 [{lib_name}] start={start_index}: {e}")
                        break
                    if not items:
                        break
                    buf = []
                    for i in items:
                        if i["Id"] in seen_ids:
                            continue
                        seen_ids.add(i["Id"]); path = i.get("Path", ""); w, s = 0, 0
                        d = 0.0
                        date_created = i.get("DateCreated", "") or ""
                        if i.get("MediaSources"):
                            ms = i["MediaSources"][0]; s = ms.get("Size", 0)
                            ticks = ms.get("RunTimeTicks", 0)
                            if ticks:
                                d = float(ticks) / 10000000.0
                            if ms.get("MediaStreams"):
                                w = ms["MediaStreams"][0].get("Width", 0)
                        c, uc, u = scanner_service.decorate_media_flags(path, i.get('Name', ''))
                        tmdb_id = str((i.get("ProviderIds") or {}).get("Tmdb") or "").strip()
                        buf.append({
                            "emby_id": i["Id"], "name": i.get("Name", ""), "path": path,
                            "resolution": w, "size": s, "duration": d,
                            "has_poster": "Primary" in i.get("ImageTags", {}),
                            "library_id": lib_id, "date_created": date_created,
                            "tag_c": c, "tag_uc": uc, "tag_u": u,
                            "tmdb_id": tmdb_id, "sync_gen": sync_gen,
                        })
                    if buf:
                        # emby_id 上有 UNIQUE 约束：换代同步时旧行还在表里，
                        # 直接 INSERT 会撞唯一键把整次同步打回滚（曾经踩过）。
                        # 用 SQLite 的 upsert 原地覆盖，行不会消失。
                        stmt = sqlite_insert(MediaItem).values(buf)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["emby_id"],
                            set_={k: getattr(stmt.excluded, k) for k in (
                                "name", "path", "resolution", "size", "duration", "has_poster",
                                "library_id", "date_created", "tag_c", "tag_uc", "tag_u",
                                "tmdb_id", "sync_gen",
                            )},
                        )
                        db.execute(stmt); db.commit(); tot += len(buf)
                    sys_log(f"[SYNC] ⏳ 索引 [{lib_name}]: {min(start_index + len(items), total_count)} / {total_count}")
                    start_index += len(items)
                    if start_index >= total_count:
                        break
            # 收尾：删掉上一代残留（含旧版本没有 sync_gen 的 NULL 行）
            stale = db.query(MediaItem).filter(
                or_(MediaItem.sync_gen != sync_gen, MediaItem.sync_gen.is_(None))
            ).delete(synchronize_session=False)
            db.commit()
            set_conf(db, "last_sync_ts", utc_now_ts())
            sys_log(f"[SYNC] ✅ 同步完成 (共 {tot} 条，清理旧缓存 {stale} 条)")
            await send_webhook(db, "全量同步", f"入库 {tot} 条。")
            return True
        except Exception as e:
            sys_log(f"[SYNC] ❌ 异常: {e}")
            return False
        finally:
            db.close(); current_sync_lib = ""

async def delayed_single_update(ids: List[str], host: str, token: str):
    """刷新后校准本地缓存：先并发取远端（限 4 并发），再串行写库。

    旧实现逐条 await，200 条就是 200 次串行往返；而把同一个 SQLAlchemy Session
    丢进 gather 里并发写又不安全，所以拆成「并发取 / 串行写」两段。
    """
    await asyncio.sleep(8)
    ids = [str(x).strip() for x in (ids or []) if str(x).strip()]
    if not ids:
        return
    db = SessionLocal(); updated = 0
    try:
        client = emby_client(db)
        base = host.rstrip('/')
        sem = asyncio.Semaphore(4)

        async def fetch_one(eid):
            async with sem:
                await asyncio.sleep(0.1)  # 错峰，别在刷新刚下发时继续压 Emby
                try:
                    res = await client.get(
                        f"{base}/emby/Items",
                        params={"Ids": eid, "Fields": "Path,MediaSources,ImageTags,DateCreated,ProviderIds"},
                        headers=emby_headers(token),
                        timeout=20.0,
                    )
                    if res.status_code == 200:
                        items = res.json().get("Items", [])
                        if items:
                            return eid, items[0]
                except Exception as e:
                    sys_log(f"[UPDATE] ❌ 热更新失败 [{eid}]: {e}")
                return eid, None

        fetched = await asyncio.gather(*[fetch_one(e) for e in ids])
        for eid, item_data in fetched:
            if not item_data:
                continue
            local_item = db.query(MediaItem).filter(MediaItem.emby_id == eid).first()
            if not local_item:
                continue
            local_item.has_poster = "Primary" in item_data.get("ImageTags", {})
            local_item.date_created = item_data.get("DateCreated", "") or local_item.date_created or ""
            local_item.path = item_data.get("Path", "") or local_item.path or ""
            local_item.name = item_data.get("Name", "") or local_item.name or ""
            # 刷新后 TMDB 标识可能才补上，跟着校准，否则 TMDB 查重会漏掉它
            tmdb_id = str((item_data.get("ProviderIds") or {}).get("Tmdb") or "").strip()
            if tmdb_id:
                local_item.tmdb_id = tmdb_id
            if item_data.get("MediaSources"):
                src = item_data["MediaSources"][0]
                local_item.size = src.get("Size", 0)
                # RunTimeTicks 是 100ns 单位，本地 duration 存的是秒（do_sync 里 /1e7）。
                # 直接写 ticks 会让时长查重凭空放大 1000 万倍。
                ticks = src.get("RunTimeTicks", 0)
                local_item.duration = float(ticks) / 10000000.0 if ticks else 0.0
                if src.get("Width"):
                    local_item.resolution = src.get("Width", 0)
            updated += 1
        db.commit()
        if updated > 0:
            sys_log(f"[UPDATE] 🔥 热更新完成: 校准 {updated} 个项目")
    finally:
        db.close()

async def scheduler_loop():
    global last_sync_trigger_slot
    while True:
        now_ts = utc_now_ts_float()
        sleep_for = 60 - (now_ts % 60)
        if sleep_for <= 0 or sleep_for > 60:
            sleep_for = 60
        await asyncio.sleep(sleep_for + 0.2)
        db = SessionLocal()
        try:
            now = datetime.now().replace(second=0, microsecond=0)
            now_ts = utc_now_ts_float()
            cs = get_conf(db, "cron_sync")
            current_slot = now.strftime("%Y-%m-%d %H:%M")
            if cs and cron_matches(cs, now):
                if sync_lock.locked():
                    sys_log(f"[SCHED] ⚠️ 命中系统同步 cron，但当前已有同步进行中，跳过本分钟触发 [{current_slot}]")
                    last_sync_trigger_slot = current_slot
                elif last_sync_trigger_slot != current_slot:
                    last_sync_trigger_slot = current_slot
                    asyncio.create_task(_scheduled_sync())
            ts = db.query(AuditTask).filter(AuditTask.enabled == True).all()
            for t in ts:
                try:
                    if not is_valid_cron(t.cron):
                        t.last_status = "error"
                        t.last_message = "cron 表达式无效"
                        t.last_duration_ms = 0
                        db.commit()
                        log_status_once(f"task_invalid_cron_{t.id}", f"[SCHED] ⚠️ 跳过无效 cron 任务 [{t.id}:{t.name}] -> {t.cron}")
                        continue
                    if not cron_matches(t.cron, now):
                        continue
                    if now_ts - float(t.last_run or "0") < 60:
                        continue
                    await execute_audit_task(db, t, "定时")
                except Exception as e:
                    t.last_status = "error"
                    t.last_message = str(e)
                    t.last_duration_ms = 0
                    db.commit()
                    sys_log(f"[SCHED] ❌ 任务执行失败 [{t.id}:{t.name}]: {e}")
        except Exception as e:
            sys_log(f"[SCHED] ❌ 调度异常: {e}")
        finally:
            db.close()

scheduler_task = None

@app.on_event("startup")
async def startup_event():
    global scheduler_task
    sys_log("[SYSTEM] 🚀 服务就绪...")
    scheduler_task = asyncio.create_task(scheduler_loop())

@app.on_event("shutdown")
async def shutdown_event():
    global scheduler_task
    if scheduler_task:
        scheduler_task.cancel()
        with suppress(asyncio.CancelledError):
            await scheduler_task
    await client.aclose()
    await insecure_client.aclose()
    await webhook_client.aclose()

@app.get("/api/health")
def health_api():
    return {"status": "ok", "sync_running": sync_lock.locked()}

@app.get("/api/status")
async def st_api():
    db = SessionLocal()
    sn, sv, sid, con = "离线", "", "", False
    h = get_conf(db, "host")
    # 前端每 5s 轮询一次，每个标签页都要打一次 Emby 登录 + System/Info。
    # 远端探测结果缓存一小段时间，多标签页/多客户端时不再把 Emby 打满。
    probe_key = f"{h}|{get_conf(db, 'user')}|{ssl_verify_enabled(db)}"
    now_mono = time.monotonic()
    cached = _status_probe_cache
    cache_valid = bool(h and cached["data"] and cached["key"] == probe_key
                       and now_mono - cached["at"] < STATUS_PROBE_TTL)
    if cache_valid:
        sn, sv, sid, con = cached["data"]
    else:
        t = ""
        if h:
            try:
                t = await asyncio.wait_for(get_token(db), timeout=6.0)
            except Exception as e:
                log_status_once("status_token_timeout", f"[STATUS] ⚠️ 获取 token 超时/失败: {e}")
                t = ""
        if h and t:
            try:
                r = await emby_client(db).get(f"{h.rstrip('/')}/System/Info", headers=emby_headers(t), timeout=5.0)
                if r.status_code == 200:
                    info = r.json(); sid = info.get("Id") or info.get("id") or info.get("ServerId") or ""
                    sn, sv, con = info.get("ServerName"), info.get("Version"), True
                elif r.status_code in [401, 403]:
                    await get_token(db, force=True)
            except Exception as e:
                log_status_once("status_info_error", f"[STATUS] ⚠️ 获取服务状态失败: {e}")
        if h:
            _status_probe_cache.update({"key": probe_key, "at": now_mono, "data": (sn, sv, sid, con)})
    res = {"local_cache": db.query(MediaItem).count(), "cleaned_count": get_conf(db, "cleaned_count") or "0", "saved_space": get_conf(db, "saved_space") or "0", "is_syncing": sync_lock.locked(), "sync_lib": current_sync_lib, "connected": con, "server_name": sn, "server_id": sid, "server_ver": sv, "user_name": get_conf(db, "user"), "sync_cron": get_conf(db, "cron_sync"), "last_log": log_buffer[-1] if log_buffer else "就绪", "status_checked_at": int(time.time()), "emby_rpm": emby_rpm(), "emby_requests": EMBY_STATS["total"], "emby_errors": EMBY_STATS["errors"]}
    db.close(); return res

@app.post("/api/config")
def cfg_post(c: ConfigRequest):
    db = SessionLocal()
    try:
        old_host = get_conf(db, "host")
        old_user = get_conf(db, "user")
        old_pwd = get_conf(db, "pwd")
        host = (c.host or "").strip().rstrip('/')
        user = (c.user or "").strip()
        webhook = (c.webhook or "").strip()
        cron_sync = (c.cron_sync or "").strip()
        if not is_valid_cron(cron_sync):
            return err(status="error", message="cron_sync 表达式无效")
        set_conf(db, "host", host)
        set_conf(db, "user", user)
        if c.pwd:
            set_conf(db, "pwd", c.pwd)
        set_conf(db, "webhook_url", webhook)
        set_conf(db, "cron_sync", cron_sync)
        if old_host != host or old_user != user or (c.pwd and old_pwd != c.pwd):
            invalidate_runtime_state(reset_sync_progress=False)
        prefs = getattr(c, 'prefs', None) or {}
        if prefs:
            set_conf(db, "pref.av.keep_priority", prefs.get("av_keep_priority", "tag_uc"))
            set_conf(db, "pref.size.keep", prefs.get("size_keep", "path_long"))
            set_conf(db, "pref.duration.keep", prefs.get("duration_keep", "min"))
            set_conf(db, "pref.duration.scope", prefs.get("duration_scope", "dir"))
            set_conf(db, "pref.duration.precision", prefs.get("duration_precision", "second"))
            set_conf(db, "pref.smart.keep", prefs.get("smart_keep", "reso_max"))
            set_conf(db, "pref.batch.confirm", prefs.get("confirm_batch", "true"))
            set_conf(db, "pref.refresh.full", prefs.get("refresh_full", "false"))
        if get_conf(db, "ssl_verify") == "":
            set_conf(db, "ssl_verify", "true")
        sys_log(f"[CONFIG] ✅ 配置已保存 host={host} cron={cron_sync}")
        return ok(status="ok")
    except Exception as e:
        sys_log(f"[CONFIG] ❌ 保存失败: {e}")
        return err(status="error", message=str(e))
    finally:
        db.close()

@app.get("/api/config")
def cfg_get():
    db = SessionLocal()
    r = {
        "host": get_conf(db, "host"),
        "user": get_conf(db, "user"),
        "webhook": get_conf(db, "webhook_url"),
        "cron_sync": get_conf(db, "cron_sync"),
        "prefs": {
            "av_keep_priority": get_conf(db, "pref.av.keep_priority") or "tag_uc",
            "size_keep": get_conf(db, "pref.size.keep") or "path_long",
            "duration_keep": get_conf(db, "pref.duration.keep") or "min",
            "duration_scope": get_conf(db, "pref.duration.scope") or "dir",
            "duration_precision": get_conf(db, "pref.duration.precision") or "second",
            "smart_keep": get_conf(db, "pref.smart.keep") or "reso_max",
            "confirm_batch": get_conf(db, "pref.batch.confirm") or "true",
            "refresh_full": get_conf(db, "pref.refresh.full") or "false",
        }
    }
    db.close(); return r

@app.get("/api/tasks")
def tasks_get(): db = SessionLocal(); r = db.query(AuditTask).order_by(AuditTask.id.asc()).all(); db.close(); return r
@app.post("/api/tasks")
def task_post(t: TaskReq):
    db = SessionLocal()
    try:
        if not is_valid_cron(t.cron):
            return err(status="error", message="任务 cron 表达式无效")
        db.add(AuditTask(name=t.name, mode=t.mode, cron=t.cron, libraries=t.libraries, enabled=t.enabled))
        db.commit()
        return {"status": "ok"}
    finally:
        db.close()
@app.put("/api/tasks/{tid}")
def task_put(tid: int, t: TaskReq):
    db = SessionLocal()
    try:
        if not is_valid_cron(t.cron):
            return err(status="error", message="任务 cron 表达式无效")
        x = db.query(AuditTask).filter(AuditTask.id == tid).first()
        if x:
            x.name, x.mode, x.cron, x.libraries, x.enabled = t.name, t.mode, t.cron, t.libraries, t.enabled
            db.commit()
        return ok(status="ok")
    finally:
        db.close()
@app.delete("/api/tasks/{id}")
def task_del(id: int):
    db = SessionLocal(); db.query(AuditTask).filter(AuditTask.id == id).delete(); db.commit(); db.close(); return {"status": "ok"}

@app.post("/api/tasks/{id}/run")
async def task_run_now(id: int):
    db = SessionLocal()
    try:
        t = db.query(AuditTask).filter(AuditTask.id == id).first()
        if not t:
            return err(status="error", message="任务不存在")
        count = await execute_audit_task(db, t, "手动")
        return ok(status="ok", found=count, task=t.name, last_status=t.last_status, last_message=t.last_message, last_duration_ms=t.last_duration_ms)
    except Exception as e:
        t = db.query(AuditTask).filter(AuditTask.id == id).first()
        if t:
            t.last_status = "error"
            t.last_message = str(e)
            t.last_duration_ms = 0
            db.commit()
        sys_log(f"[TASK] ❌ 手动执行失败 [{id}]: {e}")
        return err(status="error", message=str(e))
    finally:
        db.close()

@app.get("/api/scan")
async def scan_api(mode: str, lib: str = "", param_s: str = "100", param_d: str = "0", duration_scope: str = "dir", duration_precision: str = "second"):
    findings = await perform_scan_async(mode, lib, param_s, param_d, duration_scope, duration_precision)
    return serialize_findings(findings)

# 🚀 修改：黑名单 API 返回 mode 和 id
@app.get("/api/ignore")
def ignore_get(limit: int = 500, offset: int = 0):
    db = SessionLocal()
    q = db.query(IgnoredItem).order_by(IgnoredItem.id.desc())
    items = q.offset(max(offset, 0)).limit(min(max(limit, 1), 1000)).all()
    res = [{
        "id": i.id,
        "emby_id": i.emby_id,
        "name": i.name if i.name else i.emby_id,
        "mode": i.mode,
        "scope_type": i.scope_type,
        "scope_key": i.scope_key,
    } for i in items]
    db.close()
    return res

# 🚀 修改：duration 支持按分组忽略，其它模式保持按 item
@app.post("/api/ignore")
async def ignore_post(r: IgnoreRequest):
    db = SessionLocal()
    try:
        if r.mode == 'duration' and r.group_keys:
            added = 0
            for group_key in [str(x).strip() for x in r.group_keys if str(x).strip()]:
                exists = db.query(IgnoredItem).filter(
                    IgnoredItem.mode == r.mode,
                    IgnoredItem.scope_type == 'group',
                    IgnoredItem.scope_key == group_key
                ).first()
                if exists:
                    continue
                name = f"时长分组忽略 {group_key[-8:]}"
                db.add(IgnoredItem(emby_id=group_key, name=name, mode=r.mode, scope_type='group', scope_key=group_key))
                added += 1
                sys_log(f"[IGNORE] 🚫 已忽略时长分组: {group_key}")
            db.commit()
            return {"status": "ok", "added": added, "scope": "group"}

        eids = [str(x).strip() for x in (r.ids or []) if str(x).strip()]
        if not eids:
            return {"status": "ok", "scope": "item", "added": 0}
        # 批量查已忽略项与名称，避免逐条 SELECT（几百条时是几百次往返）
        existing = {row.emby_id for row in db.query(IgnoredItem.emby_id).filter(
            IgnoredItem.mode == r.mode, IgnoredItem.emby_id.in_(eids)).all()}
        names = {row.emby_id: row.name for row in db.query(MediaItem.emby_id, MediaItem.name).filter(
            MediaItem.emby_id.in_(eids)).all()}
        added = 0
        for eid in eids:
            if eid in existing:
                continue
            name = names.get(eid) or ""
            db.add(IgnoredItem(emby_id=eid, name=name, mode=r.mode, scope_type='item', scope_key=''))
            added += 1
            sys_log(f"[IGNORE] 🚫 已忽略 [{r.mode}]: {name if name else eid}")
        db.commit()
        return {"status": "ok", "scope": "item", "added": added}
    finally:
        db.close()

# 🚀 修改：基于 ID 删除
@app.delete("/api/ignore/{row_id}")
def ignore_del(row_id: int):
    db = SessionLocal()
    item = db.query(IgnoredItem).filter(IgnoredItem.id == row_id).first()
    if item:
        name = item.name
        mode = item.mode
        db.delete(item)
        db.commit()
        sys_log(f"[IGNORE] ♻️ 恢复白名单 [{mode}]: {name}") 
    db.close(); return ok(status="ok")

async def _delete_one(client, base, eid, token, sem):
    """删单条，返回 (emby_id, None) 表示成功，(emby_id, 原因) 表示失败。5xx/网络抖动重试一次。"""
    async with sem:
        await asyncio.sleep(0.1)  # 轻微错峰：Emby 每条删除都会触发库监视，别同时涌进去
        last_reason = "unknown"
        for attempt in (1, 2):
            try:
                r = await client.delete(f"{base}/Items/{eid}", headers=emby_headers(token), timeout=20.0)
                if r.status_code in (200, 204):
                    return eid, None
                last_reason = f"HTTP {r.status_code}"
                # 403/404 不会因为重试变好，直接返回；5xx 才重试
                if r.status_code < 500:
                    return eid, last_reason
                try:
                    body = (r.text or "").strip().replace("\n", " ")[:120]
                    if body:
                        last_reason = f"HTTP {r.status_code} {body}"
                except Exception:
                    pass
            except Exception as e:
                last_reason = f"{type(e).__name__}: {e}"
            if attempt == 1:
                await asyncio.sleep(0.8)
        return eid, last_reason


async def perform_delete(ids, host, token, job=None):
    """真正执行删除并把结果回给调用方，而不是默默吞掉。

    旧实现用 BackgroundTasks 异步删，接口永远返回 started，前端拿不到成败，
    删失败（Emby 没给删除权限、条目已被移走、403/404）在页面上完全看不出来，
    表现就是“点了删除没反应/删了又回来”。
    """
    ids = [str(x).strip() for x in (ids or []) if str(x).strip()]
    if len(ids) > DELETE_MAX_BATCH:
        ids = ids[:DELETE_MAX_BATCH]
    result = {"requested": len(ids), "deleted": 0, "failed": [], "saved_bytes": 0, "removed_rows": 0}
    if not ids:
        return result

    db = SessionLocal()
    try:
        client = emby_client(db)
        base = host.rstrip('/')
        sem = asyncio.Semaphore(DELETE_CONCURRENCY)

        async def one(eid):
            outcome = await _delete_one(client, base, eid, token, sem)
            if job is not None:
                job["done"] = job.get("done", 0) + 1
                if outcome[1] is None:
                    job["deleted"] = job.get("deleted", 0) + 1
                else:
                    job["failed"].append({"id": outcome[0], "reason": outcome[1]})
            return outcome

        outcomes = await asyncio.gather(*[one(eid) for eid in ids])

        ok_ids = [eid for eid, reason in outcomes if reason is None]
        failed = [(eid, reason) for eid, reason in outcomes if reason is not None]
        result["deleted"] = len(ok_ids)
        result["failed"] = [{"id": eid, "reason": reason} for eid, reason in failed]

        # 批量处理本地缓存与统计，避免逐条查询
        rows = db.query(MediaItem).filter(MediaItem.emby_id.in_(ok_ids)).all() if ok_ids else []
        saved = 0
        for row in rows:
            saved += row.size or 0
            db.delete(row)
        result["removed_rows"] = len(rows)
        result["saved_bytes"] = saved

        if ok_ids:
            cc = int(get_conf(db, "cleaned_count") or 0) + len(ok_ids)
            ss = int(get_conf(db, "saved_space") or 0) + saved
            for key, value in (("cleaned_count", cc), ("saved_space", ss)):
                cfg = db.query(Config).filter(Config.key == key).first()
                if cfg:
                    cfg.value = str(value)
                else:
                    db.add(Config(key=key, value=str(value)))
            db.commit()

        if failed:
            preview = ', '.join(f"{eid}:{reason}" for eid, reason in failed[:5])
            sys_log(f"[DELETE] ⚠️ 成功 {result['deleted']} 个，失败 {len(failed)} 个 -> {preview}")
        else:
            sys_log(f"[DELETE] ✅ 删除完成，成功 {result['deleted']} 个")
        return result
    finally:
        db.close()


async def background_silent_delete(ids, host, token):
    """兼容旧调用点：后台执行并单独发 webhook 汇总。"""
    res = await perform_delete(ids, host, token)
    if res["deleted"] and res["saved_bytes"] > 0:
        await wb_buffer.add(res["saved_bytes"])
    return res


def _gc_delete_jobs():
    """清掉过期/超量的任务记录，防止内存无限增长。"""
    now = time.time()
    for jid, job in list(DELETE_JOBS.items()):
        if job["state"] != "running" and now - job.get("finished_at", now) > DELETE_JOB_TTL:
            DELETE_JOBS.pop(jid, None)
    while len(DELETE_JOBS) > DELETE_JOB_KEEP:
        oldest = min(DELETE_JOBS.items(), key=lambda kv: kv[1]["started_at"])[0]
        DELETE_JOBS.pop(oldest, None)


async def run_delete_job(job_id, ids, host, token):
    job = DELETE_JOBS[job_id]
    try:
        result = await asyncio.wait_for(perform_delete(ids, host, token, job), timeout=DELETE_TIMEOUT)
        job.update({
            "state": "done",
            "deleted": result["deleted"],
            "failed": result["failed"],
            "saved_bytes": result["saved_bytes"],
            "removed_rows": result["removed_rows"],
            "done": result["requested"],
        })
        if result["deleted"] > 0:
            await wb_buffer.add(result["saved_bytes"])
    except asyncio.TimeoutError:
        job.update({"state": "timeout", "message": f"删除超时（>{DELETE_TIMEOUT}s），请重新扫描核对结果"})
        sys_log(f"[DELETE] ⚠️ 任务 {job_id} 超时，已发出的请求可能仍在 Emby 侧执行")
    except Exception as e:
        job.update({"state": "error", "message": f"{type(e).__name__}: {e}"})
        sys_log(f"[DELETE] ❌ 任务 {job_id} 异常: {e}")
    finally:
        job["finished_at"] = time.time()


@app.post("/api/delete")
async def dele_post(r: DeleteRequest):
    ids = [str(x).strip() for x in (r.ids or []) if str(x).strip()][:DELETE_MAX_BATCH]
    if not ids:
        return err(status="error", message="没有要删除的条目")
    db = SessionLocal()
    try:
        h, t = get_conf(db, "host"), await get_token(db)
    finally:
        db.close()
    if not h or not t:
        raise HTTPException(status_code=400, detail="Emby 未配置或认证失败")
    _gc_delete_jobs()
    job_id = uuid.uuid4().hex[:12]
    DELETE_JOBS[job_id] = {
        "job_id": job_id, "kind": "delete", "state": "running", "total": len(ids), "done": 0,
        "deleted": 0, "failed": [], "saved_bytes": 0, "removed_rows": 0,
        "started_at": time.time(), "finished_at": None, "message": "",
    }
    asyncio.create_task(run_delete_job(job_id, ids, h, t))
    return ok(status="started", job_id=job_id, total=len(ids))


@app.get("/api/ops/status")
@app.get("/api/delete/status")
def ops_status(job_id: str):
    job = DELETE_JOBS.get(job_id)
    if not job:
        return err(status="error", message="任务不存在或已过期")
    return {"status": "ok", "job": job}

@app.get("/api/logs")
def logs_g(since: Optional[int] = None):
    # since 缺省时保持老行为（返回全量数组）；带 since 时只回增量，
    # 日志页 5s 拉一次 2 万行既浪费带宽也让浏览器卡。
    total = log_base + len(log_buffer)
    if since is None:
        return log_buffer
    start = max(0, since - log_base)
    return {"total": total, "base": log_base, "lines": log_buffer[start:]}
@app.post("/api/logs/clear")
def logs_c():
    global log_buffer, log_base
    log_buffer.clear(); log_base = 0
    return ok(status="ok")

@app.get("/", response_class=HTMLResponse)
async def idx_p(r: Request): return templates.TemplateResponse("index.html", {"request": r})
@app.get("/api/libraries")
async def libs_g():
    db = SessionLocal(); h, t = get_conf(db, "host"), await get_token(db)
    try:
        r = await emby_client(db).get(f"{h}/Library/MediaFolders", headers=emby_headers(t))
        return r.json().get("Items", [])
    except Exception as e:
        sys_log(f"[LIBS] ❌ 获取媒体库失败: {e}")
        return []
    finally:
        db.close()
@app.post("/api/sync")
def sync_p(b: BackgroundTasks):
    if sync_lock.locked():
        return ok(status="already_running")
    b.add_task(do_sync)
    return ok(status="started")
@app.post("/api/test_webhook")
async def tw_p():
    db = SessionLocal(); await send_webhook(db, "测试", "链路正常。"); db.close(); return ok(status="ok")
async def perform_refresh(ids, host, token, full_refresh=False, use_secure=True, job=None):
    """执行刷新并返回真实成败；job 传入时顺带推进度。"""
    # 默认只做「按需刷新」：Recursive=false、Default 模式、不替换图片/元数据。
    # 旧实现固定 Recursive=true + FullRefresh + ReplaceAllImages/Metadata：
    # 每选一个条目就让 Emby 全量重刮并重新下载图片，几十个一起点会把 Emby 的
    # 元数据队列和 SQLite 压得很重。
    if full_refresh:
        query = "Recursive=true&ImageRefreshMode=FullRefresh&MetadataRefreshMode=FullRefresh&ReplaceAllImages=true&ReplaceAllMetadata=true"
    else:
        query = "Recursive=false&ImageRefreshMode=Default&MetadataRefreshMode=Default&ReplaceAllImages=false&ReplaceAllMetadata=false"

    http_client = client if use_secure else insecure_client
    sem = asyncio.Semaphore(REFRESH_CONCURRENCY)
    base = host.rstrip('/')

    async def refresh_one(eid):
        async with sem:
            await asyncio.sleep(0.2)  # 轻微错峰，别让 Emby 同时处理一堆刷新
            try:
                resp = await http_client.post(f"{base}/Items/{eid}/Refresh?{query}",
                                              headers=emby_headers(token), timeout=30.0)
                reason = None if resp.status_code in (200, 204) else f"HTTP {resp.status_code}"
            except Exception as e:
                reason = f"{type(e).__name__}: {e}"
            if job is not None:
                job["done"] = job.get("done", 0) + 1
                if reason is None:
                    job["deleted"] = job.get("deleted", 0) + 1  # 复用同一字段表示“成功数”
                else:
                    job["failed"].append({"id": eid, "reason": reason})
            return eid, reason

    outcomes = await asyncio.gather(*[refresh_one(x) for x in ids])
    ok_ids = [eid for eid, reason in outcomes if reason is None]
    failed = [(eid, reason) for eid, reason in outcomes if reason is not None]
    if failed:
        preview = ', '.join(f"{eid}:{reason}" for eid, reason in failed[:5])
        sys_log(f"[REFRESH] ⚠️ 成功 {len(ok_ids)} 个，失败 {len(failed)} 个 -> {preview}")
    else:
        sys_log(f"[REFRESH] 🔄 刷新指令已下发 {len(ok_ids)} 个（{'全量重刮' if full_refresh else '按需刷新'}）")
    return {"success": len(ok_ids), "failed": [{"id": eid, "reason": reason} for eid, reason in failed],
            "ok_ids": ok_ids, "mode": "full" if full_refresh else "default"}


async def run_refresh_job(job_id, ids, host, token, full_refresh, use_secure):
    job = DELETE_JOBS[job_id]
    try:
        result = await perform_refresh(ids, host, token, full_refresh, use_secure, job)
        job.update({
            "state": "done",
            "deleted": result["success"],
            "failed": result["failed"],
            "done": len(ids),
            "mode": result["mode"],
        })
        if result["ok_ids"]:
            # 刷新后延迟校准本地缓存（后台，不阻塞进度条）
            asyncio.create_task(delayed_single_update(result["ok_ids"], host, token))
    except Exception as e:
        job.update({"state": "error", "message": f"{type(e).__name__}: {e}"})
        sys_log(f"[REFRESH] ❌ 任务 {job_id} 异常: {e}")
    finally:
        job["finished_at"] = time.time()


@app.post("/api/refresh")
async def refresh_p(r: RefreshRequest):
    ids = [str(x).strip() for x in (r.ids or []) if str(x).strip()]
    db = SessionLocal()
    try:
        h, t = get_conf(db, "host"), await get_token(db)
        full_refresh = str(get_conf(db, "pref.refresh.full") or "false").lower() in {"1", "true", "yes", "on"}
        use_secure = ssl_verify_enabled(db)
    finally:
        db.close()
    if not h or not t:
        return err(status="error", message="Emby 未配置或认证失败")
    if not ids:
        return err(status="error", message="没有选中条目")
    if len(ids) > REFRESH_MAX_BATCH:
        return err(status="error", message=f"一次最多刷新 {REFRESH_MAX_BATCH} 个，请分批操作")

    _gc_delete_jobs()
    job_id = uuid.uuid4().hex[:12]
    DELETE_JOBS[job_id] = {
        "job_id": job_id, "kind": "refresh", "state": "running", "total": len(ids), "done": 0,
        "deleted": 0, "failed": [], "saved_bytes": 0, "removed_rows": 0,
        "started_at": time.time(), "finished_at": None, "message": "",
    }
    asyncio.create_task(run_refresh_job(job_id, ids, h, t, full_refresh, use_secure))
    return ok(status="started", job_id=job_id, total=len(ids), mode="full" if full_refresh else "default")