import json
import os
import re
from sqlalchemy import func
from core.db import MediaItem, IgnoredItem, get_conf

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
        return apply_recommendations(db, mode, [{"title": k, "items": v} for k, v in grp.items() if len(v) > 1])
    elif mode == "size":
        sub = db.query(MediaItem.size).filter(MediaItem.size > 0).group_by(MediaItem.size).having(func.count(MediaItem.id) > 1)
        rows = q.filter(MediaItem.size.in_(sub)).all(); grp = {}
        for r in rows:
            grp.setdefault(r.size, []).append(r)
        return apply_recommendations(db, mode, [{"title": f"{k/1e6:.1f} MB", "items": v} for k, v in grp.items()])
    elif mode == "duration":
        all_items = q.filter(MediaItem.duration > 0.1).all(); grp = {}
        for item in all_items:
            key = round(item.duration, 2)
            grp.setdefault(key, []).append(item)
        final_grp = []
        for k, v in grp.items():
            if len(v) > 1:
                v.sort(key=lambda x: x.size)
                final_grp.append({"title": f"⏱️ {k} 秒", "items": v})
        return apply_recommendations(db, mode, final_grp)
    elif mode == "smart":
        sub = db.query(MediaItem.name).group_by(MediaItem.name).having(func.count(MediaItem.id) > 1)
        rows = q.filter(MediaItem.name.in_(sub)).all(); grp = {}
        for r in rows:
            grp.setdefault(r.name, []).append(r)
        for _, v in grp.items():
            v.sort(key=lambda x: (x.resolution, x.size), reverse=True)
        return apply_recommendations(db, mode, [{"title": k, "items": v} for k, v in grp.items()])
    elif mode == "tiny":
        sq = q.filter(MediaItem.size < float(param_s) * 1e6, MediaItem.size > 0)
        if float(param_d) > 0:
            sq = sq.filter(MediaItem.duration < float(param_d))
        items = sq.all()
        return apply_recommendations(db, mode, [{"title": "极小资源", "items": items}] if items else [])
    elif mode == "noposter":
        items = q.filter(MediaItem.has_poster == False).all()
        return apply_recommendations(db, mode, [{"title": "封面缺失清单", "items": items}] if items else [])
    return []


def _pref(db, key, default):
    raw = get_conf(db, key)
    return raw if raw else default

def _mark(items, keep_ids, reason_keep='推荐保留', reason_del='推荐删除'):
    for item in items:
        if item.emby_id in keep_ids:
            item.recommend_action = 'keep'
            item.recommend_reason = reason_keep
        else:
            item.recommend_action = 'delete'
            item.recommend_reason = reason_del
    return items

def _score_item(item):
    return (
        1 if getattr(item, 'has_poster', False) else 0,
        getattr(item, 'resolution', 0) or 0,
        getattr(item, 'size', 0) or 0,
    )

def _pick_by_size_rule(items, rule):
    ordered = list(items)
    if rule == 'path_long':
        ordered.sort(key=lambda x: ((x.path or ''), len(x.path or '')))
        return ordered[-1], '按批量按钮规则：最长径'
    if rule == 'path_short':
        ordered.sort(key=lambda x: ((x.path or ''), len(x.path or '')))
        return ordered[0], '按批量按钮规则：最短径'
    if rule == 'name_long':
        ordered.sort(key=lambda x: ((x.name or ''), len(x.name or '')))
        return ordered[-1], '按批量按钮规则：最长名'
    if rule == 'name_short':
        ordered.sort(key=lambda x: ((x.name or ''), len(x.name or '')))
        return ordered[0], '按批量按钮规则：最短名'
    ordered.sort(key=lambda x: (getattr(x, 'size', 0) or 0, _score_item(x)))
    return ordered[-1], '保留最大文件'

def _pick_by_smart_rule(items, rule):
    ordered = list(items)
    if rule == 'reso_min':
        ordered.sort(key=lambda x: ((getattr(x, 'resolution', 0) or 0), (getattr(x, 'size', 0) or 0)))
        return ordered[0], '按批量按钮规则：最低分辨率'
    ordered.sort(key=lambda x: ((getattr(x, 'resolution', 0) or 0), (getattr(x, 'size', 0) or 0)), reverse=True)
    return ordered[0], '按批量按钮规则：最高分辨率'

def apply_recommendations(db, mode, grouped):
    av_pref = _pref(db, 'pref.av.keep_priority', 'tag_uc')
    size_pref = _pref(db, 'pref.size.keep', 'path_long')
    duration_pref = _pref(db, 'pref.duration.keep', 'min')
    smart_pref = _pref(db, 'pref.smart.keep', 'reso_max')

    for group in grouped:
        items = group.get('items', [])
        if not items:
            continue
        if mode == 'av':
            tag_rule = av_pref if av_pref in {'tag_uc', 'tag_c', 'tag_raw'} else 'tag_uc'
            matched = []
            if tag_rule == 'tag_uc':
                matched = [x for x in items if getattr(x, 'tag_uc', False)]
                reason = '按批量按钮规则：选 [UC]'
            elif tag_rule == 'tag_c':
                matched = [x for x in items if getattr(x, 'tag_c', False)]
                reason = '按批量按钮规则：选 [C]'
            else:
                matched = [x for x in items if not getattr(x, 'tag_c', False) and not getattr(x, 'tag_uc', False) and not getattr(x, 'tag_u', False)]
                reason = '按批量按钮规则：选原版'
            keep = sorted(matched or items, key=lambda x: (getattr(x, 'size', 0) or 0), reverse=True)[0]
            _mark(items, {keep.emby_id}, reason, f"与推荐保留项重复：{keep.name}")
        elif mode == 'size':
            keep, reason = _pick_by_size_rule(items, size_pref)
            _mark(items, {keep.emby_id}, reason, '同大重复，建议清理其余副本')
        elif mode == 'duration':
            rule = duration_pref if duration_pref in {'min', 'max'} else 'min'
            keep = sorted(items, key=lambda x: getattr(x, 'size', 0) or 0)[0 if rule == 'min' else -1]
            reason = '按批量按钮规则：选最小文件' if rule == 'min' else '按批量按钮规则：选最大文件'
            _mark(items, {keep.emby_id}, reason, '重复候选，建议删除')
        elif mode == 'smart':
            keep, reason = _pick_by_smart_rule(items, smart_pref)
            _mark(items, {keep.emby_id}, reason, '重复候选，建议删除')
        elif mode in ('tiny', 'noposter'):
            for item in items:
                item.recommend_action = 'delete'
                item.recommend_reason = '命中清理规则'
    return grouped
