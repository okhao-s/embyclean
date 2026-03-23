import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.db import ActionLog, ScanJob, ScanSnapshot, ScanSnapshotGroup


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)


def summarize_groups(groups: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_groups = len(groups)
    total_items = sum(len(g.get("items", [])) for g in groups)
    total_keep = 0
    total_delete = 0
    for group in groups:
        for item in group.get("items", []):
            action = getattr(item, "recommend_action", "")
            if action == "keep":
                total_keep += 1
            elif action == "delete":
                total_delete += 1
    return {
        "groups": total_groups,
        "items": total_items,
        "keep": total_keep,
        "delete": total_delete,
    }


def serialize_media_item(item: Any) -> Dict[str, Any]:
    data = {c.name: getattr(item, c.name) for c in item.__table__.columns}
    data.update(
        {
            "display_path": (item.path.rsplit("/", 1)[0] + "/") if getattr(item, "path", "") and "/" in item.path else "",
            "recommend_action": getattr(item, "recommend_action", ""),
            "recommend_reason": getattr(item, "recommend_reason", ""),
            "media": {
                "emby_id": getattr(item, "emby_id", ""),
                "title": getattr(item, "name", ""),
                "library_id": getattr(item, "library_id", ""),
                "has_poster": bool(getattr(item, "has_poster", False)),
                "date_created": getattr(item, "date_created", "") or "",
            },
            "file": {
                "path": getattr(item, "path", ""),
                "dirname": (item.path.rsplit("/", 1)[0] if getattr(item, "path", "") and "/" in item.path else ""),
                "basename": (item.path.rsplit("/", 1)[-1] if getattr(item, "path", "") else ""),
                "size": getattr(item, "size", 0) or 0,
                "duration": getattr(item, "duration", 0.0) or 0.0,
                "resolution": getattr(item, "resolution", 0) or 0,
            },
        }
    )
    return data


def persist_snapshot(db, *, groups: List[Dict[str, Any]], mode: str, libraries: str = "", params: Optional[Dict[str, Any]] = None, source: str = "manual", source_ref: str = "") -> Dict[str, Any]:
    params = params or {}
    started_at = now_iso()
    job = ScanJob(
        source=source,
        source_ref=source_ref,
        mode=mode,
        libraries=libraries,
        params_json=json_dumps(params),
        status="done",
        message="扫描完成",
        started_at=started_at,
        finished_at=started_at,
    )
    db.add(job)
    db.flush()

    summary = summarize_groups(groups)
    snapshot = ScanSnapshot(
        job_id=job.id,
        source=source,
        mode=mode,
        libraries=libraries,
        params_json=json_dumps(params),
        summary_json=json_dumps(summary),
        created_at=started_at,
    )
    db.add(snapshot)
    db.flush()

    for idx, group in enumerate(groups):
        items = [serialize_media_item(x) for x in group.get("items", [])]
        group_summary = {
            "keep": sum(1 for i in items if i.get("recommend_action") == "keep"),
            "delete": sum(1 for i in items if i.get("recommend_action") == "delete"),
            "total": len(items),
        }
        db.add(
            ScanSnapshotGroup(
                snapshot_id=snapshot.id,
                group_index=idx,
                title=group.get("title", ""),
                summary_json=json_dumps(group_summary),
                items_json=json_dumps(items),
            )
        )

    job.snapshot_id = snapshot.id
    job.total_groups = summary["groups"]
    job.total_items = summary["items"]
    job.duration_ms = 0
    db.commit()
    return {"job_id": job.id, "snapshot_id": snapshot.id, "summary": summary, "created_at": started_at}


def list_snapshots(db, limit: int = 20) -> List[Dict[str, Any]]:
    rows = db.query(ScanSnapshot).order_by(ScanSnapshot.id.desc()).limit(limit).all()
    result = []
    for row in rows:
        result.append(
            {
                "id": row.id,
                "job_id": row.job_id,
                "source": row.source,
                "mode": row.mode,
                "libraries": row.libraries,
                "params": json.loads(row.params_json or "{}"),
                "summary": json.loads(row.summary_json or "{}"),
                "created_at": row.created_at,
            }
        )
    return result


def get_snapshot_detail(db, snapshot_id: int) -> Optional[Dict[str, Any]]:
    row = db.query(ScanSnapshot).filter(ScanSnapshot.id == snapshot_id).first()
    if not row:
        return None
    groups = db.query(ScanSnapshotGroup).filter(ScanSnapshotGroup.snapshot_id == snapshot_id).order_by(ScanSnapshotGroup.group_index.asc()).all()
    return {
        "id": row.id,
        "job_id": row.job_id,
        "source": row.source,
        "mode": row.mode,
        "libraries": row.libraries,
        "params": json.loads(row.params_json or "{}"),
        "summary": json.loads(row.summary_json or "{}"),
        "created_at": row.created_at,
        "groups": [
            {
                "id": g.id,
                "index": g.group_index,
                "title": g.title,
                "summary": json.loads(g.summary_json or "{}"),
                "items": json.loads(g.items_json or "[]"),
            }
            for g in groups
        ],
    }


def add_action_log(db, *, level: str = "info", category: str = "system", action: str = "", detail: str = "", ref_type: str = "", ref_id: str = "") -> None:
    db.add(
        ActionLog(
            level=level,
            category=category,
            action=action,
            detail=detail,
            ref_type=ref_type,
            ref_id=ref_id,
            created_at=now_iso(),
        )
    )
    db.commit()


def list_action_logs(db, *, limit: int = 200, category: str = "", level: str = "", keyword: str = "") -> List[Dict[str, Any]]:
    q = db.query(ActionLog)
    if category:
        q = q.filter(ActionLog.category == category)
    if level:
        q = q.filter(ActionLog.level == level)
    if keyword:
        like = f"%{keyword}%"
        q = q.filter((ActionLog.detail.like(like)) | (ActionLog.action.like(like)) | (ActionLog.ref_id.like(like)))
    rows = q.order_by(ActionLog.id.desc()).limit(min(max(limit, 1), 1000)).all()
    return [
        {
            "id": r.id,
            "level": r.level,
            "category": r.category,
            "action": r.action,
            "detail": r.detail,
            "ref_type": r.ref_type,
            "ref_id": r.ref_id,
            "created_at": r.created_at,
        }
        for r in rows
    ]
