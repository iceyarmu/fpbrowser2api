"""任务类型动态函数注册表（创建任务 / 刷新窗口额度）。

设计目标：
- 前端可为每个 task_type 选择“创建任务函数(create_task_handler)”与“刷新剩余额度函数(refresh_quota_handler)”
- 后端仅保存函数 key（字符串），运行时通过注册表映射到真实 callable
- 你后续要新增实现：只需在本文件新增函数并注册到字典即可（无需改 DB 结构）
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from ..core.models import TaskType


# -------------------- contexts --------------------
@dataclass(frozen=True)
class CreateTaskContext:
    task_type: TaskType
    prompt: str
    image_path: Optional[str]

    # 延迟导入/传入：避免循环依赖（TaskService 也会 import 其它 services）
    db: Any
    task_service: Any


@dataclass(frozen=True)
class RefreshQuotaContext:
    """刷新窗口剩余额度上下文。

    mapping_row 为 DB join 后的 dict，至少包含：
    - id (mapping_id)
    - task_code (task_type.code)
    - remaining_quota / daily_quota
    - window_pk / window_key
    - vendor / lan_addr / access_key / space_id
    """

    task_type: TaskType
    mapping_row: Dict[str, Any]
    db: Any


CreateTaskHandler = Callable[[CreateTaskContext], Awaitable[str]]
RefreshQuotaHandler = Callable[[RefreshQuotaContext], Awaitable[int]]


def _label(key: str, text: str) -> Dict[str, str]:
    return {"key": key, "label": text}


# -------------------- built-in create task handlers --------------------
async def create_task__default_submit(ctx: CreateTaskContext) -> str:
    """默认：走 TaskService.submit_task（原有行为）。"""

    return await ctx.task_service.submit_task(ctx.task_type.code, ctx.prompt, image_path=ctx.image_path)


async def create_task__force_gen_image(ctx: CreateTaskContext) -> str:
    """示例：无论选择什么任务类型，都强制按 gen_image 创建（便于演示/调试）。"""

    return await ctx.task_service.submit_task("gen_image", ctx.prompt, image_path=ctx.image_path)


CREATE_TASK_HANDLERS: Dict[str, Tuple[str, CreateTaskHandler]] = {
    "default_submit": ("默认：submit_task 调度", create_task__default_submit),
    "force_gen_image": ("示例：强制 gen_image", create_task__force_gen_image),
}


# -------------------- built-in quota refresh handlers --------------------
async def refresh_quota__noop(ctx: RefreshQuotaContext) -> int:
    """默认：不查询外部平台，仅返回当前 remaining_quota。"""

    v = ctx.mapping_row.get("remaining_quota")
    try:
        return int(v or 0)
    except Exception:
        return 0


async def refresh_quota__reset_to_daily(ctx: RefreshQuotaContext) -> int:
    """示例：将 remaining_quota 重置为 daily_quota（适合“每日刷新”场景）。"""

    v = ctx.mapping_row.get("daily_quota")
    try:
        return int(v or 0)
    except Exception:
        return 0


REFRESH_QUOTA_HANDLERS: Dict[str, Tuple[str, RefreshQuotaHandler]] = {
    "noop": ("默认：不刷新（保持当前值）", refresh_quota__noop),
    "reset_to_daily": ("示例：重置为 daily_quota", refresh_quota__reset_to_daily),
}


# -------------------- registry helpers --------------------
def list_create_task_handler_options() -> List[Dict[str, str]]:
    return [_label(k, v[0]) for k, v in CREATE_TASK_HANDLERS.items()]


def list_refresh_quota_handler_options() -> List[Dict[str, str]]:
    return [_label(k, v[0]) for k, v in REFRESH_QUOTA_HANDLERS.items()]


def get_create_task_handler(key: Optional[str]) -> CreateTaskHandler:
    k = (key or "").strip() or "default_submit"
    if k not in CREATE_TASK_HANDLERS:
        raise KeyError(f"未知 create_task_handler: {k}")
    return CREATE_TASK_HANDLERS[k][1]


def get_refresh_quota_handler(key: Optional[str]) -> RefreshQuotaHandler:
    k = (key or "").strip() or "noop"
    if k not in REFRESH_QUOTA_HANDLERS:
        raise KeyError(f"未知 refresh_quota_handler: {k}")
    return REFRESH_QUOTA_HANDLERS[k][1]

