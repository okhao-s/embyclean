from datetime import datetime
from croniter import croniter

def cron_matches(expr: str, now: datetime) -> bool:
    try:
        base = now.replace(second=0, microsecond=0)
        prev_time = croniter(expr, base).get_prev(datetime)
        return prev_time == base
    except Exception:
        return False
