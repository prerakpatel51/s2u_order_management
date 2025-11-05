from __future__ import annotations

import os
from datetime import timedelta
from django.contrib.auth.signals import user_logged_in
from django.contrib.auth import get_user_model
from django.db.models.signals import pre_save
from django.core.exceptions import ValidationError
import re
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
    # Allow any authenticated user to trigger (employee or admin)
    if not getattr(user, "is_authenticated", False):
        return

    # Refresh interval (minutes); default 12h unless overridden
    try:
        interval_min = int(os.environ.get("REFRESH_INTERVAL_MINUTES", "720"))
    except Exception:
        interval_min = 720

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


# Enforce username rules: case-insensitive + no spaces (single word)
@receiver(pre_save, sender=get_user_model())
def _enforce_username_rules(sender, instance, **kwargs):  # noqa: ANN001
    username = getattr(instance, "username", "") or ""
    norm = username.strip()
    if not norm:
        return
    if re.search(r"\s", norm):
        raise ValidationError("Username cannot contain spaces.")
    norm_lower = norm.lower()
    instance.username = norm_lower

    # Case-insensitive uniqueness check
    User = sender
    qs = User._default_manager.filter(username__iexact=norm_lower)
    if instance.pk:
        qs = qs.exclude(pk=instance.pk)
    if qs.exists():
        raise ValidationError("A user with that username already exists (case-insensitive).")
