"""Shared defaults and validators for public task API limits."""

from __future__ import annotations


DEFAULT_PUBLIC_CREATE_TASK_MAX_INFLIGHT = 180


def normalize_public_create_task_max_inflight(value: int | None) -> int:
    """Normalize public create-task inflight limit.

    Rule:
    - must be a positive multiple of 3;
    - fallback to default when input is invalid.
    """
    try:
        num = int(value or 0)
    except Exception:
        num = DEFAULT_PUBLIC_CREATE_TASK_MAX_INFLIGHT
    if num <= 0:
        num = DEFAULT_PUBLIC_CREATE_TASK_MAX_INFLIGHT
    if num % 3 != 0:
        num = DEFAULT_PUBLIC_CREATE_TASK_MAX_INFLIGHT
    return max(3, num)


def calc_public_browser_pool_limit(create_task_max_inflight: int | None) -> int:
    """Window candidate pool size for task scheduling: 1/3 of create-task inflight."""
    limit = normalize_public_create_task_max_inflight(create_task_max_inflight)
    return max(1, limit // 3)

