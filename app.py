import os, re, logging, asyncio, httpx, time
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, Column, Integer, String, Boolean, BigInteger, Float, func, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker
from pydantic import BaseModel

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --- 核心配置 ---
MODE_MAP = {
    "av": "番号查重",
    "smart": "智能洗版",
    "size": "同大查重",
    "duration": "时长查重",
    "noposter": "缺失封面",
    "tiny": "极小文件"
}

# --- 数据模型 ---
class DeleteRequest(BaseModel): ids: List[str]
class IgnoreRequest(BaseModel): ids: List[str]
class RefreshRequest(BaseModel): ids: List[str]
class TaskReq(BaseModel): name: str; mode: str; cron: str; libraries: str; enabled: bool
class ConfigRequest(BaseModel): host: Optional[str]=""; user: Optional[str]=""; pwd: Optional[str]=""; webhook: Optional[str]=""; cron_sync: Optional[str]=""

# --- 日志系统 ---
log_buffer = []
class MemoryHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record); ts = datetime.now().strftime("%H:%M:%S")
            log_buffer.append(f"[{ts}] {msg}")
            if len(log_buffer) > 20000: log_buffer.pop(0)
        except: pass

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

# --- Database ---
DATA_DIR = "data"; os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = f"sqlite:///{DATA_DIR}/emby.db"
Base = declarative_base()

class Config(Base): __tablename__ = "configs"; key = Column(String, primary_key=True); value = Column(String)
class IgnoredItem(Base): 
    __tablename__ = "ignored_items"
    id = Column(Integer, primary_key=True)
    emby_id = Column(String, unique=True)
    name = Column(String, default="")
class AuditTask(Base): __tablename__ = "audit_tasks"; id = Column(Integer, primary_key=True); name = Column(String); mode = Column(String); cron = Column(String); libraries = Column(String, default=""); enabled = Column(Boolean, default=True); last_run = Column(String, default="0")
class MediaItem(Base): 
    __tablename__ = "media_items"
    id = Column(Integer, primary_key=True)
    emby_id = Column(String, unique=True)
    name = Column(String)
    path = Column(String)
    resolution = Column(Integer)
    size = Column(BigInteger)
    duration = Column(Float, default=0.0) # 🚀 改为浮点数，存储精确秒数
    has_poster = Column(Boolean)
    library_id = Column(String)
    tag_c = Column(Boolean, default=False)
    tag_uc = Column(Boolean, default=False)
    tag_u = Column(Boolean, default=False)

engine = create_engine(DB_PATH, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)
    inspector = inspect(engine)
    with engine.connect() as con:
        # 自动迁移：确保 duration 是 Float 类型 (SQLite不用改类型，但要确保列存在)
        if "media_items" in inspector.get_table_names():
            cols = [c["name"] for c in inspector.get_columns("media_items")]
            if "duration" not in cols: con.execute(text("ALTER TABLE media_items ADD COLUMN duration FLOAT DEFAULT 0"))
        if "ignored_items" in inspector.get_table_names():
            cols = [c["name"] for c in inspector.get_columns("ignored_items")]
            if "name" not in cols: con.execute(text("ALTER TABLE ignored_items ADD COLUMN name VARCHAR DEFAULT ''"))
        con.exec_driver_sql("PRAGMA journal_mode=WAL;")
        con.commit()
init_db()

def get_conf(db, k): r = db.query(Config).filter(Config.key == k).first(); return r.value if r else ""
def set_conf(db, k, v):
    r = db.query(Config).filter(Config.key == k).first()
    if r: r.value = str(v)
    else: db.add(Config(key=k, value=str(v)))
    db.commit()

RE_UC = re.compile(r'[-_. ]uc$', re.I); RE_U = re.compile(r'[-_. ]u$', re.I); RE_C = re.compile(r'([-_. ]c|[-_. ]ch|chinese|中字|sub|字幕)$', re.I); RE_AV = re.compile(r'([a-zA-Z]{2,5})[-_]?(\d{3,5})')

sync_lock = False; global_token = ""; current_sync_lib = ""
client = httpx.AsyncClient(timeout=120.0, verify=False)
EMBY_HEADERS = {"X-Emby-Client": "Cleaner", "X-Emby-Device-Name": "Server", "X-Emby-Device-Id": "v1.2-Precision", "X-Emby-Client-Version": "4.9"}

async def get_token(db, force=False):
    global global_token
    if global_token and not force: return global_token
    h, u, p = get_conf(db, "host"), get_conf(db, "user"), get_conf(db, "pwd")
    if not h or not u: return ""
    try:
        r = await client.post(f"{h.rstrip('/')}/Users/AuthenticateByName", json={"Username": u, "Pw": p}, headers=EMBY_HEADERS)
        if r.status_code == 200: global_token = r.json().get("AccessToken"); return global_token
    except: pass
    return ""

async def send_webhook(db, command, detail, raw=False):
    url = get_conf(db, "webhook_url")
    if not url: return
    text_content = detail if raw else f"🛰️ **EmbyCleaner 通知**\n```\n[任务] : {command}\n[详情] : {detail}\n```"
    try: await client.post(url, json={"title": f"EmbyCleaner: {command}", "text": text_content})
    except: pass

def perform_internal_scan(db, mode, lib_str="", param_s="100", param_d="0"):
    ignored = [x.emby_id for x in db.query(IgnoredItem).all()]
    q = db.query(MediaItem).filter(~MediaItem.emby_id.in_(ignored))
    if lib_str: q = q.filter(MediaItem.library_id.in_(lib_str.split(',')))
    
    if mode == "av":
        rows = q.all(); grp = {}
        for r in rows:
            m = RE_AV.search(r.name) or RE_AV.search(r.path)
            if m: key = (m.group(1) + "-" + m.group(2)).upper(); grp.setdefault(key, []).append(r)
        return [{"title": k, "items": v} for k, v in grp.items() if len(v)>1]
    elif mode == "size":
        sub = db.query(MediaItem.size).filter(MediaItem.size > 0).group_by(MediaItem.size).having(func.count(MediaItem.id) > 1)
        rows = q.filter(MediaItem.size.in_(sub)).all(); grp = {}
        for r in rows: grp.setdefault(r.size, []).append(r)
        return [{"title": f"{k/1e6:.1f} MB", "items": v} for k, v in grp.items()]
    elif mode == "duration":
        # 🚀 核心优化：按精确时长分组 (保留1位小数)
        # SQLite 默认存的是 Float，直接 group by duration 可能会因为微小浮点误差导致不匹配
        # 这里先拉取所有大于0的时长，在 Python 内存中进行高精度分组
        all_items = q.filter(MediaItem.duration > 0.1).all()
        grp = {}
        for item in all_items:
            # 保留1位小数，例如 12.345 -> 12.3
            key = round(item.duration, 1)
            grp.setdefault(key, []).append(item)
        
        # 过滤掉只有1个文件的组
        final_grp = []
        for k, v in grp.items():
            if len(v) > 1:
                # 排序：按大小
                v.sort(key=lambda x: x.size)
                final_grp.append({"title": f"⏱️ {k} 秒", "items": v})
        return final_grp

    elif mode == "smart":
        sub = db.query(MediaItem.name).group_by(MediaItem.name).having(func.count(MediaItem.id) > 1)
        rows = q.filter(MediaItem.name.in_(sub)).all(); grp = {}
        for r in rows: grp.setdefault(r.name, []).append(r)
        for k, v in grp.items(): v.sort(key=lambda x: (x.resolution, x.size), reverse=True)
        return [{"title": k, "items": v} for k, v in grp.items()]
    elif mode == "tiny": 
        sq = q.filter(MediaItem.size < float(param_s)*1e6, MediaItem.size > 0)
        if float(param_d) > 0: sq = sq.filter(MediaItem.duration < float(param_d))
        items = sq.all()
        return [{"title": f"极小资源", "items": items}] if items else []
    elif mode == "noposter":
        items = q.filter(MediaItem.has_poster == False).all()
        return [{"title": "封面缺失清单", "items": items}] if items else []
    return []

async def do_sync(trigger="手动"):
    global sync_lock, current_sync_lib; sync_lock = True; current_sync_lib = "初始化..."; sys_log(f"[SYNC] >>> {trigger}同步启动..."); db = SessionLocal()
    try:
        h, t = get_conf(db, "host"), await get_token(db)
        if not t: sys_log("[SYNC] ❌ 授权失败"); return
        res = await client.get(f"{h}/Library/MediaFolders", headers=EMBY_HEADERS | {"X-Emby-Token": t})
        libs = res.json().get("Items", [])
        db.query(MediaItem).delete(); db.commit()
        tot = 0; seen_ids = set()
        for l in libs:
            lib_id = l['Id']; lib_name = l.get('Name', 'Unknown'); current_sync_lib = lib_name; start_index = 0
            while True:
                params = {"ParentId": lib_id, "Recursive": "true", "IncludeItemTypes": "Movie,Video,Series", "Fields": "Path,MediaSources,ImageTags", "StartIndex": start_index, "Limit": 1000}
                try:
                    res_items = await client.get(f"{h}/emby/Items", params=params, headers=EMBY_HEADERS | {"X-Emby-Token": t})
                    data = res_items.json(); items = data.get("Items", []); total_count = data.get("TotalRecordCount", 0)
                except: break
                if not items: break
                buf = []
                for i in items:
                    if i["Id"] in seen_ids: continue
                    seen_ids.add(i["Id"]); path = i.get("Path", ""); w, s = 0, 0
                    d = 0.0 # 默认为 float
                    if i.get("MediaSources"):
                        ms = i["MediaSources"][0]; s = ms.get("Size", 0); 
                        # 🚀 核心：获取精确秒数 (Ticks / 10000000)
                        ticks = ms.get("RunTimeTicks", 0)
                        if ticks: d = float(ticks) / 10000000.0
                        if ms.get("MediaStreams"): w = ms["MediaStreams"][0].get("Width", 0)
                    base = os.path.splitext(os.path.basename(path))[0]
                    uc = bool(RE_UC.search(base or i['Name'])); u = False if uc else bool(RE_U.search(base or i['Name'])); c = False if (uc or u) else bool(RE_C.search(base or i['Name']))
                    buf.append(MediaItem(emby_id=i["Id"], name=i.get("Name", ""), path=path, resolution=w, size=s, duration=d, has_poster="Primary" in i.get("ImageTags", {}), library_id=lib_id, tag_c=c, tag_uc=uc, tag_u=u))
                if buf: db.bulk_save_objects(buf); db.commit(); tot += len(buf)
                sys_log(f"[SYNC] ⏳ 索引 [{lib_name}]: {min(start_index + len(items), total_count)} / {total_count}")
                start_index += len(items)
                if start_index >= total_count: break
        sys_log(f"[SYNC] ✅ 同步完成 (共 {tot} 条)"); await send_webhook(db, "全量同步", f"入库 {tot} 条。")
    except Exception as e: sys_log(f"[SYNC] ❌ 异常: {e}")
    finally: db.close(); sync_lock = False; current_sync_lib = ""

# 🚀 延迟热更新逻辑
async def delayed_single_update(ids: List[str], host: str, token: str):
    await asyncio.sleep(8)
    db = SessionLocal(); updated = 0
    try:
        for eid in ids:
            try:
                res = await client.get(f"{host.rstrip('/')}/emby/Items", params={"Ids": eid, "Fields": "MediaSources,ImageTags"}, headers=EMBY_HEADERS | {"X-Emby-Token": token})
                if res.status_code == 200:
                    items = res.json().get("Items", [])
                    if items:
                        item_data = items[0]; local_item = db.query(MediaItem).filter(MediaItem.emby_id == eid).first()
                        if local_item:
                            local_item.has_poster = "Primary" in item_data.get("ImageTags", {})
                            if item_data.get("MediaSources"): local_item.size = item_data["MediaSources"][0].get("Size", 0)
                            updated += 1
            except: pass
        db.commit()
        if updated > 0: sys_log(f"[UPDATE] 🔥 热更新完成: 校准 {updated} 个项目")
    finally: db.close()

async def scheduler_loop():
    while True:
        await asyncio.sleep(60); db = SessionLocal()
        try:
            cs = get_conf(db, "cron_sync"); now = datetime.now()
            if cs and (time.time() - float(get_conf(db, "last_sync_ts") or "0")) > 60:
                p = cs.split()
                if len(p)>=2 and (p[0]=="*" or int(p[0])==now.minute) and (p[1]=="*" or int(p[1])==now.hour):
                    set_conf(db, "last_sync_ts", str(time.time())); asyncio.create_task(do_sync("计划"))
            ts = db.query(AuditTask).filter(AuditTask.enabled == True).all()
            for t in ts:
                ps = t.cron.split()
                if len(ps)>=2 and (ps[0]=="*" or int(ps[0])==now.minute) and (ps[1]=="*" or int(ps[1])==now.hour):
                    if time.time() - float(t.last_run) < 60: continue
                    t.last_run = str(time.time()); db.commit(); findings = perform_internal_scan(db, t.mode, t.libraries)
                    if findings:
                        count = sum(len(f["items"]) for f in findings)
                        await send_webhook(db, f"定时任务: {t.name}", f"模式: {MODE_MAP.get(t.mode, t.mode)}\n发现: {count} 个待处理项")
        except: pass
        finally: db.close()

@app.on_event("startup")
async def startup_event(): sys_log("[SYSTEM] 🚀 服务就绪..."); asyncio.create_task(scheduler_loop())

@app.get("/api/status")
async def st_api():
    db = SessionLocal(); sn, sv, sid, con = "离线", "", "", False; h, t = get_conf(db, "host"), await get_token(db)
    if h and t:
        try:
            r = await client.get(f"{h.rstrip('/')}/System/Info", headers=EMBY_HEADERS | {"X-Emby-Token": t}, timeout=10.0)
            if r.status_code == 200:
                info = r.json(); sid = info.get("Id") or info.get("id") or info.get("ServerId") or ""
                sn, sv, con = info.get("ServerName"), info.get("Version"), True
            elif r.status_code in [401, 403]: await get_token(db, force=True)
        except: pass
    res = {"local_cache": db.query(MediaItem).count(), "cleaned_count": get_conf(db, "cleaned_count") or "0", "saved_space": get_conf(db, "saved_space") or "0", "is_syncing": sync_lock, "sync_lib": current_sync_lib, "connected": con, "server_name": sn, "server_id": sid, "server_ver": sv, "user_name": get_conf(db, "user"), "sync_cron": get_conf(db, "cron_sync"), "last_log": log_buffer[-1] if log_buffer else "就绪"}
    db.close(); return res

@app.post("/api/config")
def cfg_post(c: ConfigRequest):
    db = SessionLocal(); set_conf(db, "host", c.host.rstrip('/')); set_conf(db, "user", c.user)
    if c.pwd: set_conf(db, "pwd", c.pwd)
    set_conf(db, "webhook_url", c.webhook); set_conf(db, "cron_sync", c.cron_sync); db.close(); return {"status": "ok"}

@app.get("/api/config")
def cfg_get():
    db = SessionLocal(); r = {"host": get_conf(db, "host"), "user": get_conf(db, "user"), "webhook": get_conf(db, "webhook_url"), "cron_sync": get_conf(db, "cron_sync")}; db.close(); return r

@app.get("/api/tasks")
def tasks_get(): db = SessionLocal(); r = db.query(AuditTask).all(); db.close(); return r
@app.post("/api/tasks")
def task_post(t: TaskReq):
    db = SessionLocal(); db.add(AuditTask(name=t.name, mode=t.mode, cron=t.cron, libraries=t.libraries, enabled=t.enabled)); db.commit(); db.close(); return {"status": "ok"}
@app.put("/api/tasks/{tid}")
def task_put(tid: int, t: TaskReq):
    db = SessionLocal(); x = db.query(AuditTask).filter(AuditTask.id == tid).first()
    if x: x.name, x.mode, x.cron, x.libraries = t.name, t.mode, t.cron, t.libraries; db.commit()
    db.close(); return {"status": "ok"}
@app.delete("/api/tasks/{id}")
def task_del(id: int):
    db = SessionLocal(); db.query(AuditTask).filter(AuditTask.id == id).delete(); db.commit(); db.close(); return {"status": "ok"}

@app.get("/api/scan")
def scan_api(mode: str, lib: str = "", param_s: str = "100", param_d: str = "0"):
    db = SessionLocal(); findings = perform_internal_scan(db, mode, lib, param_s, param_d)
    def td(lst): return [{c.name: getattr(x, c.name) for c in x.__table__.columns} | {'display_path': os.path.dirname(x.path) + "/"} for x in lst]
    res = [{"title": f["title"], "items": td(f["items"])} for f in findings]; db.close(); return res

@app.get("/api/ignore")
def ignore_get():
    db = SessionLocal()
    items = db.query(IgnoredItem).all()
    res = [{"id": i.id, "emby_id": i.emby_id, "name": i.name if i.name else i.emby_id} for i in items]
    db.close()
    return res

@app.post("/api/ignore")
async def ignore_post(r: IgnoreRequest):
    db = SessionLocal()
    for eid in r.ids:
        if not db.query(IgnoredItem).filter(IgnoredItem.emby_id == eid).first(): 
            name = ""
            media = db.query(MediaItem).filter(MediaItem.emby_id == eid).first()
            if media: name = media.name
            db.add(IgnoredItem(emby_id=eid, name=name))
            sys_log(f"[IGNORE] 🚫 已忽略: {name if name else eid}")
    db.commit(); db.close(); return {"status": "ok"}

@app.delete("/api/ignore/{eid}")
def ignore_del(eid: str):
    db = SessionLocal(); 
    item = db.query(IgnoredItem).filter(IgnoredItem.emby_id == eid).first()
    name = item.name if (item and item.name) else eid
    db.query(IgnoredItem).filter(IgnoredItem.emby_id == eid).delete()
    db.commit()
    sys_log(f"[IGNORE] ♻️ 恢复白名单: {name}") 
    db.close(); return {"status": "ok"}

async def background_silent_delete(ids, host, token):
    db = SessionLocal(); c, s = 0, 0
    async def fast_del(eid):
        try:
            r = await client.delete(f"{host}/Items/{eid}", headers=EMBY_HEADERS | {"X-Emby-Token": token})
            return eid if r.status_code in [200, 204] else None
        except: return None
    results = await asyncio.gather(*[fast_del(x) for x in ids])
    for eid in [x for x in results if x]:
        i = db.query(MediaItem).filter(MediaItem.emby_id == eid).first()
        if i: c += 1; s += i.size; db.delete(i); await wb_buffer.add(i.size, db)
    db.commit(); set_conf(db, "cleaned_count", str(int(get_conf(db, "cleaned_count") or 0) + c)); set_conf(db, "saved_space", str(int(get_conf(db, "saved_space") or 0) + s))
    db.close()

@app.post("/api/delete")
async def dele_post(r: DeleteRequest, b: BackgroundTasks):
    db = SessionLocal(); h, t = get_conf(db, "host"), await get_token(db); db.close()
    b.add_task(background_silent_delete, r.ids, h, t); return {"status": "started"}

@app.get("/api/logs")
def logs_g(): return log_buffer
@app.post("/api/logs/clear")
def logs_c(): global log_buffer; log_buffer.clear(); return {"status": "ok"}

@app.get("/", response_class=HTMLResponse)
async def idx_p(r: Request): return templates.TemplateResponse("index.html", {"request": r})
@app.get("/api/libraries")
async def libs_g():
    db = SessionLocal(); h, t = get_conf(db, "host"), await get_token(db); db.close()
    try: r = await client.get(f"{h}/Library/MediaFolders", headers=EMBY_HEADERS | {"X-Emby-Token": t}); return r.json().get("Items", [])
    except: return []
@app.post("/api/sync")
def sync_p(b: BackgroundTasks): b.add_task(do_sync); return {"status": "started"}
@app.post("/api/test_webhook")
async def tw_p():
    db = SessionLocal(); await send_webhook(db, "测试", "链路正常。"); db.close(); return {"status": "ok"}
@app.post("/api/refresh")
async def refresh_p(r: RefreshRequest, b: BackgroundTasks):
    db = SessionLocal(); h, t = get_conf(db, "host"), await get_token(db)
    if not h or not t: db.close(); return {"status": "error"}
    sc = 0; fc = 0
    for eid in r.ids:
        try:
            u = f"{h.rstrip('/')}/Items/{eid}/Refresh?Recursive=true&ImageRefreshMode=FullRefresh&MetadataRefreshMode=FullRefresh&ReplaceAllImages=true&ReplaceAllMetadata=true"
            resp = await client.post(u, headers=EMBY_HEADERS | {"X-Emby-Token": t})
            if resp.status_code in [200, 204]: sc += 1
            else: fc += 1
        except: fc += 1
    if sc > 0: b.add_task(delayed_single_update, r.ids, h, t)
    db.close(); return {"status": "ok", "success": sc, "fail": fc}
