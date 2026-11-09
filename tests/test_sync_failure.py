import asyncio
import httpx

from core.db import SessionLocal, MediaItem, set_conf


def _item(eid, name):
    return {
        "Id": eid,
        "Name": name,
        "Path": f"/x/{name}.mkv",
        "MediaSources": [{"Size": 100, "RunTimeTicks": 0, "MediaStreams": [{"Width": 1920}]}],
        "ImageTags": {},
        "DateCreated": "2024-01-01T00:00:00Z",
        "ProviderIds": {},
    }


def test_sync_mid_failure_preserves_old_generation(fake_emby):
    """P0：同步中途失败不得清掉上一代 sync_gen，/api/scan 仍能扫到旧缓存。"""
    import app as app_mod

    # 预置上一代缓存行
    db = SessionLocal()
    try:
        db.add(MediaItem(
            emby_id="old1", name="OldItem", path="/x/old.mkv",
            resolution=0, size=100, duration=0.0, has_poster=False,
            library_id="lib1", sync_gen=12345,
        ))
        set_conf(db, "host", "http://fake")
        set_conf(db, "user", "u")
        set_conf(db, "pwd", "p")
        db.commit()
    finally:
        db.close()

    async def handler(request):
        url = str(request.url)
        if request.method == "POST" and url.endswith("/Users/AuthenticateByName"):
            return httpx.Response(200, json={"AccessToken": "tok"})
        if url.endswith("/Library/MediaFolders"):
            return httpx.Response(200, json={"Items": [{"Id": "lib1", "Name": "Lib1"}]})
        if "/emby/Items" in url:
            start = int(dict(request.url.params).get("StartIndex", "0"))
            if start == 0:
                # 第一页返回 1 条但声称总数 2 条 → 触发第二页拉取
                return httpx.Response(200, json={"Items": [_item("new1", "NewItem")], "TotalRecordCount": 2})
            # 第二页模拟中途失败
            return httpx.Response(500, json={})
        return httpx.Response(404, json={})

    fake_emby(handler)

    ok = asyncio.run(app_mod.do_sync("测试"))
    assert ok is False

    # 旧代 sync_gen 行数不变（未被清理）
    db = SessionLocal()
    try:
        old_count = db.query(MediaItem).filter(MediaItem.sync_gen == 12345).count()
    finally:
        db.close()
    assert old_count == 1

    # /api/scan 仍有数据
    findings = asyncio.run(app_mod.scan_api("noposter"))
    ids = [it["emby_id"] for f in findings for it in f.get("items", [])]
    assert "old1" in ids
