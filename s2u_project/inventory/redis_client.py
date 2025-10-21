"""Thin Redis wrapper for app-level caching and locks.

Avoids Django's cache framework per project requirements.
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

import redis


def _build_client() -> redis.Redis:
    url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/1")
    return redis.from_url(url, decode_responses=True)  # store strings


# Shared connection (connection pool under the hood)
r: redis.Redis = _build_client()


def get_json(key: str, default: Optional[Any] = None) -> Any:
    raw = r.get(key)
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def set_json(key: str, value: Any, ex: int | None = None) -> None:
    r.set(key, json.dumps(value), ex=ex)


def setnx(key: str, value: str, ex: int | None = None) -> bool:
    # SET with NX + EX is atomic
    # redis-py: r.set(name, value, ex=None, px=None, nx=False, xx=False, keepttl=False)
    return bool(r.set(key, value, ex=ex, nx=True))


def exists(key: str) -> bool:
    return bool(r.exists(key))


def delete(key: str) -> None:
    r.delete(key)


def scan_delete(pattern: str) -> int:
    """Delete keys by pattern using SCAN to avoid blocking the server."""
    total = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=500)
        if keys:
            total += r.delete(*keys)
        if cursor == 0:
            break
    return int(total)

