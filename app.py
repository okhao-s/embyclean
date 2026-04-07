import os, re, logging, asyncio, httpx, time
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse
from contextlib import suppress
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from core.db import init_db, SessionLocal, Config, IgnoredItem, AuditTask, MediaItem, get_conf, set_conf
from core.schemas import DeleteRequest, IgnoreRequest, RefreshRequest, TaskReq, ConfigRequest
from core.responses import ok, err
import services.scanner as scanner_service
from services.scanner import MODE_MAP
from services.scheduler import cron_matches, is_valid_cron

app = FastAPI()
init_db()
templates = Jinja2Templates(directory="templates")

# --- 日志系统 ---
log_buffer = []
class MemoryHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record); ts = datetime.now().strftime("%H:%M:%S")
            log_buffer.append(f"[{ts}] {msg}")
            if len(log_buffer) > 20000: log_buffer.pop(0)
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
    async def add(self, size_bytes, db):
        async with self.lock:
            self.count += 1; self.size += size_bytes
            if not self.timer_task: self.timer_task = asyncio.create_task(self._flush_later(db))
    async def _flush_later(self, db):
        await asyncio.sleep(5)
        async with self.lock:
            if self.count > 0: await self._send(db)
            self.count = 0; self.size = 0; self.timer_task = None
    async def _send(self, db):
        sz = f"{self.size/1048576:.2f} MB" if self.size < 1073741824 else f"{self.size/1073741824:.2f} GB"
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = f"🛰️ **EmbyCleaner 任务报告**\n━━━━━━━━━━━━━━\n⏱️ 时间: {ts}\n📦 数量: {self.count} 个项目\n💾 空间: {sz}\n━━━━━━━━━━━━━━\n✅ 清理执行完毕"
        await send_webhook(db, "清理完成", msg, raw=True)

wb_buffer = WebhookBuffer()

sync_lock = asyncio.Lock(); global_token = ""; current_sync_lib = ""; last_sync_trigger_slot = ""
DELETE_CONCURRENCY = 8
STATUS_LOG_INTERVAL = 300
last_status_log_at = {}
client = httpx.AsyncClient(timeout=120.0, verify=True)
insecure_client = httpx.AsyncClient(timeout=120.0, verify=False)
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


def utc_now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def invalidate_runtime_state(reset_sync_progress: bool = False):
    global global_token, current_sync_lib
    global_token = ""
    if reset_sync_progress:
        current_sync_lib = ""


def _perform_scan(mode: str, lib: str = "", param_s: str = "100", param_d: str = "0"):
    db = SessionLocal()
    try:
        return scanner_service.perform_internal_scan(db, mode, lib, param_s, param_d)
    finally:
        db.close()


async def perform_scan_async(mode: str, lib: str = "", param_s: str = "100", param_d: str = "0"):
    return await asyncio.to_thread(_perform_scan, mode, lib, param_s, param_d)


async def execute_audit_task(db, task: AuditTask, trigger: str = "计划"):
    started_at = time.time()
    task.last_run = str(utc_now_ts())
    db.commit()
    findings = await perform_scan_async(task.mode, task.libraries)
    count = sum(len(f["items"]) for f in findings) if findings else 0
    task.last_found = count
    task.last_duration_ms = int((time.time() - started_at) * 1000)
    if findings:
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
            row = {c.name: getattr(x, c.name) for c in x.__table__.columns}
            row.update({
                'display_path': os.path.dirname(x.path) + "/" if x.path else "",
                'recommend_action': getattr(x, 'recommend_action', ''),
                'recommend_reason': getattr(x, 'recommend_reason', ''),
            })
            items.append(row)
        res.append({
            "title": f.get("title", ""),
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

async def send_webhook(db, command, detail, raw=False):
    url = get_conf(db, "webhook_url")
    if not url: return
    text_content = detail if raw else f"🛰️ **EmbyCleaner 通知**\n```\n[任务] : {command}\n[详情] : {detail}\n```"
    try:
        await client.post(url, json={"title": f"EmbyCleaner: {command}", "text": text_content})
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
            db.query(MediaItem).delete(); db.commit()
            tot = 0; seen_ids = set()
            for l in libs:
                lib_id = l['Id']; lib_name = l.get('Name', 'Unknown'); current_sync_lib = lib_name; start_index = 0
                while True:
                    params = {"ParentId": lib_id, "Recursive": "true", "IncludeItemTypes": "Movie,Video,Series", "Fields": "Path,MediaSources,ImageTags,DateCreated", "StartIndex": start_index, "Limit": 1000}
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
                        buf.append(MediaItem(emby_id=i["Id"], name=i.get("Name", ""), path=path, resolution=w, size=s, duration=d, has_poster="Primary" in i.get("ImageTags", {}), library_id=lib_id, date_created=date_created, tag_c=c, tag_uc=uc, tag_u=u))
                    if buf:
                        db.bulk_save_objects(buf); db.commit(); tot += len(buf)
                    sys_log(f"[SYNC] ⏳ 索引 [{lib_name}]: {min(start_index + len(items), total_count)} / {total_count}")
                    start_index += len(items)
                    if start_index >= total_count:
                        break
            set_conf(db, "last_sync_ts", str(utc_now_ts()))
            sys_log(f"[SYNC] ✅ 同步完成 (共 {tot} 条)")
            await send_webhook(db, "全量同步", f"入库 {tot} 条。")
            return True
        except Exception as e:
            sys_log(f"[SYNC] ❌ 异常: {e}")
            return False
        finally:
            db.close(); current_sync_lib = ""

async def delayed_single_update(ids: List[str], host: str, token: str):
    await asyncio.sleep(8)
    db = SessionLocal(); updated = 0
    try:
        for eid in ids:
            try:
                res = await emby_client(db).get(f"{host.rstrip('/')}/emby/Items", params={"Ids": eid, "Fields": "MediaSources,ImageTags,DateCreated"}, headers=emby_headers(token))
                if res.status_code == 200:
                    items = res.json().get("Items", [])
                    if items:
                        item_data = items[0]; local_item = db.query(MediaItem).filter(MediaItem.emby_id == eid).first()
                        if local_item:
                            local_item.has_poster = "Primary" in item_data.get("ImageTags", {})
                            local_item.date_created = item_data.get("DateCreated", "") or local_item.date_created or ""
                            if item_data.get("MediaSources"):
                                local_item.size = item_data["MediaSources"][0].get("Size", 0)
                            updated += 1
            except Exception as e:
                sys_log(f"[UPDATE] ❌ 热更新失败 [{eid}]: {e}")
        db.commit()
        if updated > 0:
            sys_log(f"[UPDATE] 🔥 热更新完成: 校准 {updated} 个项目")
    finally:
        db.close()

async def scheduler_loop():
    global last_sync_trigger_slot
    while True:
        now_ts = utc_now_ts()
        sleep_for = 60 - (now_ts % 60)
        if sleep_for <= 0 or sleep_for > 60:
            sleep_for = 60
        await asyncio.sleep(sleep_for + 0.2)
        db = SessionLocal()
        try:
            now = datetime.now().replace(second=0, microsecond=0)
            now_ts = utc_now_ts()
            cs = get_conf(db, "cron_sync")
            current_slot = now.strftime("%Y-%m-%d %H:%M")
            if cs and cron_matches(cs, now):
                if sync_lock.locked():
                    sys_log(f"[SCHED] ⚠️ 命中系统同步 cron，但当前已有同步进行中，跳过本分钟触发 [{current_slot}]")
                    last_sync_trigger_slot = current_slot
                elif last_sync_trigger_slot != current_slot:
                    last_sync_trigger_slot = current_slot
                    asyncio.create_task(do_sync("计划"))
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

@app.get("/api/health")
def health_api():
    return {"status": "ok", "sync_running": sync_lock.locked()}

@app.get("/api/status")
async def st_api():
    db = SessionLocal()
    sn, sv, sid, con = "离线", "", "", False
    h = get_conf(db, "host")
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
    res = {"local_cache": db.query(MediaItem).count(), "cleaned_count": get_conf(db, "cleaned_count") or "0", "saved_space": get_conf(db, "saved_space") or "0", "is_syncing": sync_lock.locked(), "sync_lib": current_sync_lib, "connected": con, "server_name": sn, "server_id": sid, "server_ver": sv, "user_name": get_conf(db, "user"), "sync_cron": get_conf(db, "cron_sync"), "last_log": log_buffer[-1] if log_buffer else "就绪", "status_checked_at": int(time.time())}
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
            set_conf(db, "pref.smart.keep", prefs.get("smart_keep", "reso_max"))
            set_conf(db, "pref.batch.confirm", prefs.get("confirm_batch", "true"))
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
            "smart_keep": get_conf(db, "pref.smart.keep") or "reso_max",
            "confirm_batch": get_conf(db, "pref.batch.confirm") or "true",
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
async def scan_api(mode: str, lib: str = "", param_s: str = "100", param_d: str = "0"):
    findings = await perform_scan_async(mode, lib, param_s, param_d)
    return serialize_findings(findings)

# 🚀 修改：黑名单 API 返回 mode 和 id
@app.get("/api/ignore")
def ignore_get(limit: int = 500, offset: int = 0):
    db = SessionLocal()
    q = db.query(IgnoredItem).order_by(IgnoredItem.id.desc())
    items = q.offset(max(offset, 0)).limit(min(max(limit, 1), 1000)).all()
    res = [{"id": i.id, "emby_id": i.emby_id, "name": i.name if i.name else i.emby_id, "mode": i.mode} for i in items]
    db.close()
    return res

# 🚀 修改：接收 mode 并检查是否重复
@app.post("/api/ignore")
async def ignore_post(r: IgnoreRequest):
    db = SessionLocal()
    for eid in r.ids:
        # 检查是否已存在于该模式
        exists = db.query(IgnoredItem).filter(IgnoredItem.emby_id == eid, IgnoredItem.mode == r.mode).first()
        if not exists: 
            name = ""
            media = db.query(MediaItem).filter(MediaItem.emby_id == eid).first()
            if media: name = media.name
            db.add(IgnoredItem(emby_id=eid, name=name, mode=r.mode))
            sys_log(f"[IGNORE] 🚫 已忽略 [{r.mode}]: {name if name else eid}")
    db.commit(); db.close(); return {"status": "ok"}

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

async def background_silent_delete(ids, host, token):
    db = SessionLocal(); c, s = 0, 0
    sem = asyncio.Semaphore(DELETE_CONCURRENCY)
    failed = []

    async def fast_del(eid):
        async with sem:
            try:
                r = await emby_client(db).delete(f"{host.rstrip('/')}/Items/{eid}", headers=emby_headers(token))
                if r.status_code in [200, 204]:
                    return eid
                failed.append((eid, f"HTTP {r.status_code}"))
                return None
            except Exception as e:
                failed.append((eid, str(e)))
                return None

    try:
        results = await asyncio.gather(*[fast_del(x) for x in ids])
        for eid in [x for x in results if x]:
            i = db.query(MediaItem).filter(MediaItem.emby_id == eid).first()
            if i:
                c += 1; s += i.size; db.delete(i); await wb_buffer.add(i.size)
        db.commit(); set_conf(db, "cleaned_count", str(int(get_conf(db, "cleaned_count") or 0) + c)); set_conf(db, "saved_space", str(int(get_conf(db, "saved_space") or 0) + s))
        if failed:
            preview = ', '.join([f"{eid}:{reason}" for eid, reason in failed[:5]])
            sys_log(f"[DELETE] ⚠️ 删除完成，成功 {c} 个，失败 {len(failed)} 个 -> {preview}")
        else:
            sys_log(f"[DELETE] ✅ 删除完成，成功 {c} 个")
    finally:
        db.close()

@app.post("/api/delete")
async def dele_post(r: DeleteRequest, b: BackgroundTasks):
    db = SessionLocal(); h, t = get_conf(db, "host"), await get_token(db); db.close()
    if not h or not t:
        raise HTTPException(status_code=400, detail="Emby 未配置或认证失败")
    b.add_task(background_silent_delete, r.ids, h, t); return ok(status="started", queued=len(r.ids))

@app.get("/api/logs")
def logs_g(): return log_buffer
@app.post("/api/logs/clear")
def logs_c(): global log_buffer; log_buffer.clear(); return ok(status="ok")

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
@app.post("/api/refresh")
async def refresh_p(r: RefreshRequest, b: BackgroundTasks):
    db = SessionLocal(); h, t = get_conf(db, "host"), await get_token(db)
    if not h or not t: db.close(); return err(status="error", message="Emby 未配置或认证失败")
    sc = 0; fc = 0
    for eid in r.ids:
        try:
            u = f"{h.rstrip('/')}/Items/{eid}/Refresh?Recursive=true&ImageRefreshMode=FullRefresh&MetadataRefreshMode=FullRefresh&ReplaceAllImages=true&ReplaceAllMetadata=true"
            resp = await emby_client(db).post(u, headers=emby_headers(t))
            if resp.status_code in [200, 204]: sc += 1
            else: fc += 1
        except Exception as e:
            fc += 1
            sys_log(f"[REFRESH] ❌ 刷新失败 [{eid}]: {e}")
    if sc > 0: b.add_task(delayed_single_update, r.ids, h, t)
    db.close(); return {"status": "ok", "success": sc, "fail": fc}