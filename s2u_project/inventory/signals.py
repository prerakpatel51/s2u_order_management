from __future__ import annotations

import os
from datetime import timedelta
from django.contrib.auth.signals import user_logged_in
from django.dispatch import receiver
from django.utils import timezone

from .redis_client import r as redis_client
from .redis_client import exists as redis_exists
from .views import _refresh_lock_key, start_global_refresh_async


def _last_completed_ts() -> timezone.datetime | None:
    try:
        data = redis_client.get("refresh:last_completed_at")
        if not data:
            return None
        import json
        obj = json.loads(data)
        ts = obj.get("ts")
        if not ts:
            return None
        # parse ISO timestamp
        try:
            from datetime import datetime
            return datetime.fromisoformat(ts).astimezone(timezone.utc)
        except Exception:
            return None
    except Exception:
        return None


@receiver(user_logged_in)
def trigger_refresh_on_login(sender, user, request, **kwargs):  # noqa: ANN001
    # Only staff can trigger the global refresh automatically
    if not getattr(user, "is_staff", False):
        return

    # Refresh interval (minutes); default 24h
    try:
        interval_min = int(os.environ.get("REFRESH_INTERVAL_MINUTES", "1440"))
    except Exception:
        interval_min = 1440

    # Skip if another refresh is running
    try:
        if redis_exists(_refresh_lock_key()):
            return
    except Exception:
        pass

    last = _last_completed_ts()
    now = timezone.now()
    if not last or (now - last) >= timedelta(minutes=interval_min):
        # Start in background; UI will auto-detect and show progress
        start_global_refresh_async(user.id)

