import logging
import os
import threading

from django.apps import AppConfig
from django.conf import settings
from django.core.management import call_command

from .redis_client import setnx as redis_setnx


class InventoryConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "inventory"

    def ready(self):
        """On server start, sync stores and products in the background.

        - Skips in dev autoreloader parent (RUN_MAIN!="true") to avoid double-run.
        - Uses a Redis NX lock so only one process triggers the sync.
        - Respects DISABLE_STARTUP_SYNC=1 to opt out.
        """
        logger = logging.getLogger(__name__)

        if os.environ.get("DISABLE_STARTUP_SYNC") == "1":
            logger.info("[startup-sync] Disabled by DISABLE_STARTUP_SYNC=1")
            return

        # Avoid double-run in Django runserver autoreloader
        if settings.DEBUG and os.environ.get("RUN_MAIN") != "true":
            return

        # Skip non-runserver management commands (e.g., migrate, collectstatic)
        import sys
        if sys.argv and sys.argv[0].endswith("manage.py"):
            if len(sys.argv) >= 2 and sys.argv[1] != "runserver":
                return

        # Acquire a lock so only one worker does the bootstrap
        lock_key = "startup_sync:lock"
        got_lock = False
        try:
            got_lock = redis_setnx(lock_key, "1", ex=900)  # 15 minutes
        except Exception as exc:
            logger.warning("[startup-sync] Redis unavailable, skipping lock: %s", exc)
            # Proceed without lock (best-effort)
            got_lock = True

        if not got_lock:
            logger.info("[startup-sync] Another process is handling bootstrap; skipping")
            return

        def _worker():
            try:
                logger.info("[startup-sync] Syncing stores...")
                call_command("sync_stores")
                logger.info("[startup-sync] Syncing products...")
                call_command("load_products", skip_csv=True)
                logger.info("[startup-sync] Completed.")
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("[startup-sync] Failed: %s", exc)

        t = threading.Thread(target=_worker, name="startup-sync", daemon=True)
        t.start()

        # Import signal handlers (login-triggered refresh)
        try:
            from . import signals  # noqa: F401
        except Exception:  # pragma: no cover
            pass
