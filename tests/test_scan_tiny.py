from fastapi.testclient import TestClient

from app import app as fastapi_app


def test_tiny_invalid_param_returns_400():
    """P1(2)：tiny 模式非法 param_s 应返回 400 而不是 500。"""
    client = TestClient(fastapi_app)
    r = client.get("/api/scan", params={"mode": "tiny", "param_s": "abc"})
    assert r.status_code == 400


def test_tiny_invalid_param_d_returns_400():
    client = TestClient(fastapi_app)
    r = client.get("/api/scan", params={"mode": "tiny", "param_d": "not-a-number"})
    assert r.status_code == 400


def test_tiny_valid_param_ok():
    client = TestClient(fastapi_app)
    r = client.get("/api/scan", params={"mode": "tiny", "param_s": "100", "param_d": "0"})
    assert r.status_code == 200
