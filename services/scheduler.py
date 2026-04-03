from datetime import datetime, timedelta
from croniter import croniter


def is_valid_cron(expr: str) -> bool:
    expr = (expr or "").strip()
    if not expr:
        return True
    try:
        croniter(expr, datetime.now().replace(second=0, microsecond=0))
        return True
    except Exception:
        return False


def cron_matches(expr: str, now: datetime) -> bool:
    try:
        base = now.replace(second=0, microsecond=0)
        if hasattr(croniter, "match"):
            return bool(croniter.match(expr, base))
        prev_minute = base - timedelta(minutes=1)
        next_time = croniter(expr, prev_minute).get_next(datetime)
        return next_time == base
    except Exception:
        return False
