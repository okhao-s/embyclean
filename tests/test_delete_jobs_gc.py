import time


def test_gc_preserves_running_job():
    """P2(6)：DELETE_JOBS 淘汰只弹已结束的任务，running 任务不能被弹掉。"""
    import app as app_mod

    app_mod.DELETE_JOBS.clear()
    now = time.time()
    # 最旧的 started_at，但仍在 running —— 旧的实现会把它当最旧任务弹掉
    app_mod.DELETE_JOBS["running"] = {"state": "running", "started_at": now - 1000, "finished_at": None}
    for i in range(30):
        app_mod.DELETE_JOBS[f"done{i}"] = {"state": "done", "started_at": now - 500 + i, "finished_at": now}

    app_mod._gc_delete_jobs()

    assert "running" in app_mod.DELETE_JOBS
    assert app_mod.DELETE_JOBS["running"]["state"] == "running"
    assert len(app_mod.DELETE_JOBS) == app_mod.DELETE_JOB_KEEP
