"""Shared runtime limits for public task APIs."""

from __future__ import annotations

import os


def _read_int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


# Public create-task inflight gate (single source of truth).
PUBLIC_CREATE_TASK_MAX_INFLIGHT = max(1, _read_int_env("PUBLIC_CREATE_TASK_MAX_INFLIGHT", 180))

# Window candidate pool size for task scheduling: 1/3 of create-task inflight.
PUBLIC_BROWSER_POOL_LIMIT = max(1, PUBLIC_CREATE_TASK_MAX_INFLIGHT // 3)

