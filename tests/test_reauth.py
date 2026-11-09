import asyncio
import httpx

from core.db import SessionLocal, set_conf


def test_sync_recovers_from_401(fake_emby):
    """P1(3)：收到 401 后强制重新登录换新 token 并重试一次，不递归。"""
    import app as app_mod

    db = SessionLocal()
    try:
        set_conf(db, "host", "http://fake")
        set_conf(db, "user", "u")
        set_conf(db, "pwd", "p")
    finally:
        db.close()

    auth_calls = []

    async def handler(request):
        url = str(request.url)
        if request.method == "POST" and url.endswith("/Users/AuthenticateByName"):
            auth_calls.append(1)
            return httpx.Response(200, json={"AccessToken": f"tok{len(auth_calls)}"})
        if url.endswith("/Library/MediaFolders"):
            token = request.headers.get("X-Emby-Token")
            if token == "tok1":
                return httpx.Response(401, json={})
            return httpx.Response(200, json={"Items": []})
        return httpx.Response(200, json={"Items": []})

    fake_emby(handler)

    ok = asyncio.run(app_mod.do_sync("测试"))
    assert ok is True
    # 第一次登录 + 401 后强制重登，共两次；若再 401 也不会无限重试
    assert len(auth_calls) == 2
