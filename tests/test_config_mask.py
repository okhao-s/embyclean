from fastapi.testclient import TestClient

from app import app as fastapi_app
from core.db import SessionLocal, set_conf, get_conf


def test_webhook_masked_and_preserved():
    """P1(5)：GET 脱敏回显 webhook（query 打码），POST 回传脱敏串不覆盖真实值。"""
    db = SessionLocal()
    try:
        set_conf(db, "webhook_url", "https://example.com/hook?token=SECRET&foo=bar")
    finally:
        db.close()

    client = TestClient(fastapi_app)
    data = client.get("/api/config").json()
    assert "SECRET" not in data["webhook"]
    assert data["webhook"] == "https://example.com/hook?token=******&foo=******"

    # 前端把脱敏串原样回传 → 库里真实值保持不变
    client.post("/api/config", json={
        "host": "", "user": "", "pwd": "",
        "webhook": data["webhook"], "cron_sync": "",
    })
    db = SessionLocal()
    try:
        assert get_conf(db, "webhook_url") == "https://example.com/hook?token=SECRET&foo=bar"
    finally:
        db.close()


def test_webhook_update_with_real_url():
    """用户真的改了 webhook（不含脱敏标记）→ 正常保存。"""
    client = TestClient(fastapi_app)
    client.post("/api/config", json={
        "host": "", "user": "", "pwd": "",
        "webhook": "https://example.com/new?x=1", "cron_sync": "",
    })
    db = SessionLocal()
    try:
        assert get_conf(db, "webhook_url") == "https://example.com/new?x=1"
    finally:
        db.close()
