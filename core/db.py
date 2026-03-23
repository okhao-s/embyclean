import os
from sqlalchemy import create_engine, Column, Integer, String, Boolean, BigInteger, Float, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker

DATA_DIR = os.environ.get("EMBYCLEAN_DATA_DIR", "/app/data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = f"sqlite:///{DATA_DIR}/emby.db"
Base = declarative_base()

class Config(Base):
    __tablename__ = "configs"
    key = Column(String, primary_key=True)
    value = Column(String)

class IgnoredItem(Base):
    __tablename__ = "ignored_items"
    id = Column(Integer, primary_key=True)
    emby_id = Column(String)
    name = Column(String, default="")
    mode = Column(String, default="global")

class AuditTask(Base):
    __tablename__ = "audit_tasks"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    mode = Column(String)
    cron = Column(String)
    libraries = Column(String, default="")
    enabled = Column(Boolean, default=True)
    last_run = Column(String, default="0")
    last_status = Column(String, default="idle")
    last_found = Column(Integer, default=0)
    last_message = Column(String, default="")
    last_duration_ms = Column(Integer, default=0)

class MediaItem(Base):
    __tablename__ = "media_items"
    id = Column(Integer, primary_key=True)
    emby_id = Column(String, unique=True)
    name = Column(String)
    path = Column(String)
    resolution = Column(Integer)
    size = Column(BigInteger)
    duration = Column(Float, default=0.0)
    has_poster = Column(Boolean)
    library_id = Column(String)
    date_created = Column(String, default="")
    tag_c = Column(Boolean, default=False)
    tag_uc = Column(Boolean, default=False)
    tag_u = Column(Boolean, default=False)

class ScanJob(Base):
    __tablename__ = "scan_jobs"
    id = Column(Integer, primary_key=True)
    source = Column(String, default="manual")
    source_ref = Column(String, default="")
    mode = Column(String, default="")
    libraries = Column(String, default="")
    params_json = Column(String, default="{}")
    status = Column(String, default="pending")
    message = Column(String, default="")
    snapshot_id = Column(Integer, default=0)
    total_groups = Column(Integer, default=0)
    total_items = Column(Integer, default=0)
    started_at = Column(String, default="")
    finished_at = Column(String, default="")
    duration_ms = Column(Integer, default=0)

class ScanSnapshot(Base):
    __tablename__ = "scan_snapshots"
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, default=0)
    source = Column(String, default="manual")
    mode = Column(String, default="")
    libraries = Column(String, default="")
    params_json = Column(String, default="{}")
    summary_json = Column(String, default="{}")
    created_at = Column(String, default="")

class ScanSnapshotGroup(Base):
    __tablename__ = "scan_snapshot_groups"
    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, default=0)
    group_index = Column(Integer, default=0)
    title = Column(String, default="")
    summary_json = Column(String, default="{}")
    items_json = Column(String, default="[]")

class ActionLog(Base):
    __tablename__ = "action_logs"
    id = Column(Integer, primary_key=True)
    level = Column(String, default="info")
    category = Column(String, default="system")
    action = Column(String, default="")
    detail = Column(String, default="")
    ref_type = Column(String, default="")
    ref_id = Column(String, default="")
    created_at = Column(String, default="")

engine = create_engine(DB_PATH, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)
    inspector = inspect(engine)
    with engine.connect() as con:
        if "media_items" in inspector.get_table_names():
            cols = [c["name"] for c in inspector.get_columns("media_items")]
            if "duration" not in cols:
                con.execute(text("ALTER TABLE media_items ADD COLUMN duration FLOAT DEFAULT 0"))
            if "date_created" not in cols:
                con.execute(text("ALTER TABLE media_items ADD COLUMN date_created VARCHAR DEFAULT ''"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_media_items_library_id ON media_items(library_id)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_media_items_size ON media_items(size)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_media_items_duration ON media_items(duration)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_media_items_name ON media_items(name)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_media_items_date_created ON media_items(date_created)"))
        if "ignored_items" in inspector.get_table_names():
            cols = [c["name"] for c in inspector.get_columns("ignored_items")]
            if "name" not in cols:
                con.execute(text("ALTER TABLE ignored_items ADD COLUMN name VARCHAR DEFAULT ''"))
            if "mode" not in cols:
                con.execute(text("ALTER TABLE ignored_items ADD COLUMN mode VARCHAR DEFAULT 'global'"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_ignored_items_emby_mode ON ignored_items(emby_id, mode)"))
        if "audit_tasks" in inspector.get_table_names():
            cols = [c["name"] for c in inspector.get_columns("audit_tasks")]
            if "last_status" not in cols:
                con.execute(text("ALTER TABLE audit_tasks ADD COLUMN last_status VARCHAR DEFAULT 'idle'"))
            if "last_found" not in cols:
                con.execute(text("ALTER TABLE audit_tasks ADD COLUMN last_found INTEGER DEFAULT 0"))
            if "last_message" not in cols:
                con.execute(text("ALTER TABLE audit_tasks ADD COLUMN last_message VARCHAR DEFAULT ''"))
            if "last_duration_ms" not in cols:
                con.execute(text("ALTER TABLE audit_tasks ADD COLUMN last_duration_ms INTEGER DEFAULT 0"))
        if "scan_jobs" in inspector.get_table_names():
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_scan_jobs_status_started ON scan_jobs(status, started_at)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_scan_jobs_source_ref ON scan_jobs(source, source_ref)"))
        if "scan_snapshots" in inspector.get_table_names():
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_scan_snapshots_job_created ON scan_snapshots(job_id, created_at)"))
        if "scan_snapshot_groups" in inspector.get_table_names():
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_scan_snapshot_groups_snapshot ON scan_snapshot_groups(snapshot_id, group_index)"))
        if "action_logs" in inspector.get_table_names():
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_action_logs_created ON action_logs(created_at)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_action_logs_category_level ON action_logs(category, level)"))
        con.exec_driver_sql("PRAGMA journal_mode=WAL;")
        con.commit()

def get_conf(db, k):
    r = db.query(Config).filter(Config.key == k).first()
    return r.value if r else ""

def set_conf(db, k, v):
    r = db.query(Config).filter(Config.key == k).first()
    if r:
        r.value = str(v)
    else:
        db.add(Config(key=k, value=str(v)))
    db.commit()
