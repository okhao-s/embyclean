def ok(data=None, message="ok", **extra):
    payload = {"ok": True, "message": message}
    if data is not None:
        payload["data"] = data
    payload.update(extra)
    return payload

def err(message="error", **extra):
    payload = {"ok": False, "message": message}
    payload.update(extra)
    return payload
