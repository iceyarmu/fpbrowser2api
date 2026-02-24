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
    # 任意参数（由具体 handler 自行解析）
    payload: Dict[str, Any]

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

    return await ctx.task_service.submit_task(ctx.task_type.code, ctx.payload or {})


async def create_task__force_gen_image(ctx: CreateTaskContext) -> str:
    """示例：无论选择什么任务类型，都强制按 gen_image 创建（便于演示/调试）。"""

    return await ctx.task_service.submit_task("gen_image", ctx.payload or {})


async def create_task__sora_gen_video(ctx: CreateTaskContext) -> str:
    """Sora 生视频：同样走 submit_task，执行阶段在 _run_task 中分发到 sora_task_executor.py。"""

    return await ctx.task_service.submit_task(ctx.task_type.code, ctx.payload or {})


CREATE_TASK_HANDLERS: Dict[str, Tuple[str, CreateTaskHandler]] = {
    "default_submit": ("默认：submit_task 调度", create_task__default_submit),
    "force_gen_image": ("示例：强制 gen_image", create_task__force_gen_image),
    "sora_gen_video": ("Sora：生成视频（prompt + first_image_url + duration）", create_task__sora_gen_video),
    "sora_wm_remove": ("Sora：视频去水印（输入 soraUrl 返回 videoUrl）", create_task__sora_gen_video),
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


async def refresh_quota__sora_nf_check(ctx: RefreshQuotaContext) -> int:
    """Sora：通过指纹浏览器读取 /backend/nf/check，并把余额字段写回 mapping。"""
    row = ctx.mapping_row or {}
    vendor = str(row.get("vendor") or "roxy")
    base_url = str(row.get("lan_addr") or "")
    access_key = row.get("access_key")
    space_id = str(row.get("space_id") or "")
    window_key = str(row.get("window_key") or "")
    if not base_url or not space_id or not window_key:
        raise RuntimeError("mapping 缺少 vendor/lan_addr/space_id/window_key，无法刷新 Sora 余额")

    # 复用 SoraSession（避免重复开浏览器）
    from .sora_task_executor import get_or_create_sora_session  # type: ignore

    sora_ctx = get_or_create_sora_session(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
    try:
        sora_ctx.set_access_token(row.get("sora_access_token"), row.get("sora_access_expires"))
    except Exception:
        pass
    # 选一个稳定入口页；只用于触发带 Authorization 的请求头捕获
    target_url = "https://sora.chatgpt.com/drafts"
    info = await sora_ctx.api_nf_check(target_url=target_url)

    remaining = int(info.get("remaining_count") or 0)
    rate_limit_reached = bool(info.get("rate_limit_reached", False))
    resets = int(info.get("access_resets_in_seconds") or 0)
    cooldown_until = info.get("cooldown_until")

    # 写回 DB：remaining_quota 也同步为 remaining_count，保持调度逻辑一致
    kwargs: Dict[str, Any] = {
        "mapping_id": int(row.get("id") or 0),
        "remaining_quota": remaining,
        "sora_remaining_count": remaining,
        "sora_rate_limit_reached": rate_limit_reached,
        "sora_access_resets_in_seconds": resets,
    }
    # api_nf_check 会返回 cooldown_until（当前时间+resets 秒），用于表示“额度重置时间点”
    if cooldown_until:
        kwargs["cooldown_until"] = str(cooldown_until)
    await ctx.db.update_task_type_window(**kwargs)
    return remaining


REFRESH_QUOTA_HANDLERS: Dict[str, Tuple[str, RefreshQuotaHandler]] = {
    "noop": ("默认：不刷新（保持当前值）", refresh_quota__noop),
    "reset_to_daily": ("示例：重置为 daily_quota", refresh_quota__reset_to_daily),
    "sora_nf_check": ("Sora：读取余额 backend/nf/check", refresh_quota__sora_nf_check),
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

