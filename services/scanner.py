import hashlib
import json
import os
import re
from decimal import Decimal, ROUND_HALF_UP
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

def _created_key(item):
    return getattr(item, 'date_created', '') or ''

def _sort_items_by_created_desc(items):
    items.sort(key=lambda x: (_created_key(x), getattr(x, 'size', 0) or 0, getattr(x, 'id', 0) or 0), reverse=True)
    return items

def _sort_grouped_by_created_desc(grouped):
    def group_key(group):
        items = group.get('items', []) or []
        latest = max((_created_key(x) for x in items), default='')
        return (latest, group.get('title', ''))
    grouped.sort(key=group_key, reverse=True)
    return grouped

def _dir_key(path: str):
    raw = (path or '').replace('\\', '/')
    normalized = os.path.normpath(raw)
    return os.path.dirname(normalized)


def _duration_group_key(duration):
    value = Decimal(str(duration or 0))
    return value.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)


def _format_duration_group_key(duration_key: Decimal):
    return format(duration_key, '.1f')


def _duration_group_signature(dir_key: str, duration_key: Decimal, items):
    member_ids = sorted(str(getattr(item, 'emby_id', '') or '') for item in items if getattr(item, 'emby_id', ''))
    payload = {
        'scope': 'duration-group-v1',
        'dir_key': dir_key or '/',
        'duration_key': _format_duration_group_key(duration_key),
        'member_ids': member_ids,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha1(encoded.encode('utf-8')).hexdigest()
    return f"duration-group-v1:{digest}"


def _group_duration_duplicates(items, ignored_group_keys=None):
    ignored_group_keys = set(ignored_group_keys or [])
    by_dir = {}
    for item in items:
        dir_key = _dir_key(getattr(item, 'path', ''))
        by_dir.setdefault(dir_key, []).append(item)

    grouped = []
    for dir_key, dir_items in by_dir.items():
        by_duration = {}
        for item in dir_items:
            duration = getattr(item, 'duration', 0) or 0
            duration_key = _duration_group_key(duration)
            by_duration.setdefault(duration_key, []).append(item)

        title_dir = dir_key or "/"
        for duration_key, duration_items in by_duration.items():
            if len(duration_items) > 1:
                group_key = _duration_group_signature(dir_key, duration_key, duration_items)
                if group_key in ignored_group_keys:
                    continue
                grouped.append({
                    "title": f"⏱️ {_format_duration_group_key(duration_key)} 秒 · 📁 {title_dir}",
                    "items": duration_items,
                    "group_key": group_key,
                    "ignore_scope": "group",
                    "group_meta": {
                        "dir_key": title_dir,
                        "duration_key": _format_duration_group_key(duration_key),
                        "member_count": len(duration_items),
                    }
                })
    return grouped


def _duration_candidate_duration_keys_query(db, lib_ids=None):
    duration_key_expr = func.round(MediaItem.duration * 10, 0) / 10.0
    q = db.query(duration_key_expr.label('duration_key')).filter(MediaItem.duration > 0.1)
    if lib_ids:
        q = q.filter(MediaItem.library_id.in_(lib_ids))
    return q.group_by(duration_key_expr).having(func.count(MediaItem.id) > 1)


def _duration_candidate_items_query(db, ignored_emby_ids, lib_ids=None):
    candidate_duration_keys = _duration_candidate_duration_keys_query(db, lib_ids).subquery()
    duration_key_expr = func.round(MediaItem.duration * 10, 0) / 10.0
    q = db.query(MediaItem).join(
        candidate_duration_keys,
        candidate_duration_keys.c.duration_key == duration_key_expr
    )
    if ignored_emby_ids:
        q = q.filter(~MediaItem.emby_id.in_(ignored_emby_ids))
    if lib_ids:
        q = q.filter(MediaItem.library_id.in_(lib_ids))
    return q.filter(MediaItem.duration > 0.1)

def perform_internal_scan(db, mode, lib_str="", param_s="100", param_d="0"):
    lib_ids = [x for x in lib_str.split(',') if x] if lib_str else []
    ignored = [x.emby_id for x in db.query(IgnoredItem.emby_id).filter(IgnoredItem.mode == mode).all()]
    q = db.query(MediaItem)
    if ignored:
        q = q.filter(~MediaItem.emby_id.in_(ignored))
    if lib_ids:
        q = q.filter(MediaItem.library_id.in_(lib_ids))

    grouped = []
    if mode == "av":
        rows = q.all(); grp = {}
        for r in rows:
            m = RE_AV.search(r.name) or RE_AV.search(r.path)
            if m:
                key = (m.group(1) + "-" + m.group(2)).upper()
                grp.setdefault(key, []).append(r)
        grouped = [{"title": k, "items": v} for k, v in grp.items() if len(v) > 1]
    elif mode == "size":
        sub = db.query(MediaItem.size).filter(MediaItem.size > 0).group_by(MediaItem.size).having(func.count(MediaItem.id) > 1)
        rows = q.filter(MediaItem.size.in_(sub)).all(); grp = {}
        for r in rows:
            grp.setdefault(r.size, []).append(r)
        grouped = [{"title": f"{k/1e6:.1f} MB", "items": v} for k, v in grp.items()]
    elif mode == "duration":
        ignored_group_keys = [
            x.scope_key for x in db.query(IgnoredItem.scope_key)
            .filter(IgnoredItem.mode == mode, IgnoredItem.scope_type == 'group', IgnoredItem.scope_key != '')
            .all()
        ]
        rows = _duration_candidate_items_query(db, ignored, lib_ids).all()
        grouped = _group_duration_duplicates(rows, ignored_group_keys)
    elif mode == "smart":
        sub = db.query(MediaItem.name).group_by(MediaItem.name).having(func.count(MediaItem.id) > 1)
        rows = q.filter(MediaItem.name.in_(sub)).all(); grp = {}
        for r in rows:
            grp.setdefault(r.name, []).append(r)
        grouped = [{"title": k, "items": v} for k, v in grp.items()]
    elif mode == "tiny":
        sq = q.filter(MediaItem.size < float(param_s) * 1e6, MediaItem.size > 0)
        if float(param_d) > 0:
            sq = sq.filter(MediaItem.duration < float(param_d))
        items = sq.all()
        grouped = [{"title": "极小资源", "items": items}] if items else []
    elif mode == "noposter":
        items = q.filter(MediaItem.has_poster == False).all()
        grouped = [{"title": "封面缺失清单", "items": items}] if items else []
    else:
        return []

    for group in grouped:
        _sort_items_by_created_desc(group.get('items', []))
    _sort_grouped_by_created_desc(grouped)
    return apply_recommendations(db, mode, grouped)


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
