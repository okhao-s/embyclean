from core.db import engine


def test_busy_timeout_set():
    """P1(4)：SQLite 连接应带 busy_timeout=5000，避免并发写库直接 database is locked。"""
    with engine.connect() as con:
        val = con.exec_driver_sql("PRAGMA busy_timeout").scalar()
    assert val == 5000
