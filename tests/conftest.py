"""pytest 夹具与测试环境隔离。

关键：必须在导入 app / core.db 之前设置 EMBYCLEAN_DATA_DIR（隔离测试数据库）
和 PRUNE_ENABLED=false（测试里别让 prune 反查假 Emby 误删数据）。
"""
import os
import tempfile

# 独立数据目录，避免污染真实 /app/data
_TEST_DATA_DIR = tempfile.mkdtemp(prefix="embyclean-test-")
os.environ["EMBYCLEAN_DATA_DIR"] = _TEST_DATA_DIR
# 关闭 prune 远端反查：单测用 MockTransport 的假 Emby，prune 会把假数据当孤儿删掉
os.environ["PRUNE_ENABLED"] = "false"

import pytest


@pytest.fixture(autouse=True)
def _reset_state():
    """每个测试前重置模块全局状态与数据表，保证用例相互隔离。"""
    import app as app_mod
    from core.db import SessionLocal, MediaItem, Config, IgnoredItem, AuditTask

    app_mod.global_token = ""
    app_mod.current_sync_lib = ""
    app_mod.DELETE_JOBS.clear()
    app_mod._status_probe_cache.update({"key": "", "at": 0.0, "data": None})

    db = SessionLocal()
    try:
        db.query(MediaItem).delete()
        db.query(IgnoredItem).delete()
        db.query(AuditTask).delete()
        db.query(Config).delete()
        db.commit()
    finally:
        db.close()
    yield


@pytest.fixture
def fake_emby(monkeypatch):
    """安装一个假的 Emby 客户端（httpx MockTransport）。

    返回 install(handler)：handler 是 async def handler(request) -> httpx.Response。
    """
    import app as app_mod
    import httpx

    def install(handler):
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        monkeypatch.setattr(app_mod, "client", client)
        monkeypatch.setattr(app_mod, "insecure_client", client)
        return client

    return install
