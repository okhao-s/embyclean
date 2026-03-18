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
    tag_c = Column(Boolean, default=False)
    tag_uc = Column(Boolean, default=False)
    tag_u = Column(Boolean, default=False)

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
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_media_items_library_id ON media_items(library_id)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_media_items_size ON media_items(size)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_media_items_duration ON media_items(duration)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_media_items_name ON media_items(name)"))
        if "ignored_items" in inspector.get_table_names():
            cols = [c["name"] for c in inspector.get_columns("ignored_items")]
            if "name" not in cols:
                con.execute(text("ALTER TABLE ignored_items ADD COLUMN name VARCHAR DEFAULT ''"))
            if "mode" not in cols:
                con.execute(text("ALTER TABLE ignored_items ADD COLUMN mode VARCHAR DEFAULT 'global'"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_ignored_items_emby_mode ON ignored_items(emby_id, mode)"))
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
