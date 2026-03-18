import os
import re
from sqlalchemy import func
from core.db import MediaItem, IgnoredItem

MODE_MAP = {
    "av": "番号查重",
    "smart": "智能洗版",
    "size": "同大查重",
    "duration": "时长查重",
    "noposter": "缺失封面",
    "tiny": "极小文件"
}

RE_UC = re.compile(r'[-_. ]uc$', re.I)
RE_U = re.compile(r'[-_. ]u$', re.I)
RE_C = re.compile(r'([-_. ]c|[-_. ]ch|chinese|中字|sub|字幕)$', re.I)
RE_AV = re.compile(r'([a-zA-Z]{2,5})[-_]?(\d{3,5})')

def decorate_media_flags(path: str, name: str):
    base = os.path.splitext(os.path.basename(path))[0]
    uc = bool(RE_UC.search(base or name))
    u = False if uc else bool(RE_U.search(base or name))
    c = False if (uc or u) else bool(RE_C.search(base or name))
    return c, uc, u

def perform_internal_scan(db, mode, lib_str="", param_s="100", param_d="0"):
    ignored = [x.emby_id for x in db.query(IgnoredItem).filter(IgnoredItem.mode == mode).all()]
    q = db.query(MediaItem).filter(~MediaItem.emby_id.in_(ignored))
    if lib_str:
        q = q.filter(MediaItem.library_id.in_(lib_str.split(',')))

    if mode == "av":
        rows = q.all(); grp = {}
        for r in rows:
            m = RE_AV.search(r.name) or RE_AV.search(r.path)
            if m:
                key = (m.group(1) + "-" + m.group(2)).upper()
                grp.setdefault(key, []).append(r)
        return [{"title": k, "items": v} for k, v in grp.items() if len(v) > 1]
    elif mode == "size":
        sub = db.query(MediaItem.size).filter(MediaItem.size > 0).group_by(MediaItem.size).having(func.count(MediaItem.id) > 1)
        rows = q.filter(MediaItem.size.in_(sub)).all(); grp = {}
        for r in rows:
            grp.setdefault(r.size, []).append(r)
        return [{"title": f"{k/1e6:.1f} MB", "items": v} for k, v in grp.items()]
    elif mode == "duration":
        all_items = q.filter(MediaItem.duration > 0.1).all(); grp = {}
        for item in all_items:
            key = round(item.duration, 1)
            grp.setdefault(key, []).append(item)
        final_grp = []
        for k, v in grp.items():
            if len(v) > 1:
                v.sort(key=lambda x: x.size)
                final_grp.append({"title": f"⏱️ {k} 秒", "items": v})
        return final_grp
    elif mode == "smart":
        sub = db.query(MediaItem.name).group_by(MediaItem.name).having(func.count(MediaItem.id) > 1)
        rows = q.filter(MediaItem.name.in_(sub)).all(); grp = {}
        for r in rows:
            grp.setdefault(r.name, []).append(r)
        for _, v in grp.items():
            v.sort(key=lambda x: (x.resolution, x.size), reverse=True)
        return [{"title": k, "items": v} for k, v in grp.items()]
    elif mode == "tiny":
        sq = q.filter(MediaItem.size < float(param_s) * 1e6, MediaItem.size > 0)
        if float(param_d) > 0:
            sq = sq.filter(MediaItem.duration < float(param_d))
        items = sq.all()
        return [{"title": "极小资源", "items": items}] if items else []
    elif mode == "noposter":
        items = q.filter(MediaItem.has_poster == False).all()
        return [{"title": "封面缺失清单", "items": items}] if items else []
    return []
