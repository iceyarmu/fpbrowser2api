"""GPT/ChatGPT 图片/视频工作流执行器框架。

只使用 browser_extension 执行真实提交；Python 侧负责：
- 从指纹浏览器/插件读取 ChatGPT 长 access_token 与过期时间；
- 经两层代理刷新余额/会员信息；
- 将图片/视频生成任务转发给插件。
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Dict, Optional

import httpx

from ..core.logger import logger
from .browser_extension_bridge import should_use_extension_executor
from .browser_extension_interaction import (
    submit_extension_task,
    trigger_extension_ws_connection_via_window,
    wait_extension_client,
)
from .playwright_broswer_context import append_log
from .task_executor_types import NonPenalizedTaskError, ProgressCB
from .veo_workflow_executor import get_or_create_veo_session

DEFAULT_GPT_TARGET = "https://chatgpt.com/"


def _int(v: Any, default: int = 0) -> int:
    try:
        return int(v or default)
    except Exception:
        return default


def _is_video_payload(payload: Dict[str, Any]) -> bool:
    raw = str(payload.get("workflow_kind") or payload.get("kind") or payload.get("type") or "").lower()
    if "video" in raw:
        return True
    model = str(payload.get("model") or payload.get("model_code") or "").lower()
    return "video" in model or model.startswith("sora")


async def gpt_fetch_access_token_in_window(
    *,
    browser_vendor: str,
    browser_base_url: str,
    browser_access_key: Optional[str],
    space_id: str,
    window_key: str,
    target_url: str = DEFAULT_GPT_TARGET,
    headless: bool = False,
    pure_mode: bool = True,
    timeout_seconds: float = 60.0,
) -> Dict[str, Any]:
    """通过插件读取 ChatGPT cookie 后调用 /api/auth/session。

    GPT 只保存长 access_token；不做 VEO 那种 short token 兑换。
    """
    sess = get_or_create_veo_session(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )
    try:
        sess.browser_headless = headless
        sess.browser_pure_mode = pure_mode
        await trigger_extension_ws_connection_via_window(
            sess=sess,
            target_url=target_url or DEFAULT_GPT_TARGET,
            space_id=space_id,
            window_key=window_key,
            wait_seconds=10.0,
            headless=headless,
            pure_mode=pure_mode,
            log_file=sess._log_file,
        )
    except Exception as e:
        append_log(sess._log_file, f"[gpt] trigger extension for token failed: {e}")
        await wait_extension_client(space_id, window_key, timeout_seconds=10.0)

    async def _noop(_p: int, _d: Dict[str, Any]) -> None:
        return None

    return await submit_extension_task(
        space_id=space_id,
        window_key=window_key,
        provider="gpt",
        payload={"action": "get_access_token", "target_url": target_url or DEFAULT_GPT_TARGET},
        progress_cb=_noop,
        timeout_seconds=timeout_seconds,
    )


async def gpt_fetch_balance_by_proxy(*, access_token: str, target_url: str = DEFAULT_GPT_TARGET, db: Any = None, picked: Any = None) -> Dict[str, Any]:
    """两层代理刷新余额框架。

    当前先封装 ChatGPT Web 常见账号接口；若上游字段变化，raw 会完整返回供后续适配。
    """
    if not access_token:
        raise NonPenalizedTaskError("缺少 GPT access_token", status_code=401)
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    base = (target_url or DEFAULT_GPT_TARGET).rstrip("/")
    endpoints = [f"{base}/backend-api/me", f"{base}/backend-api/accounts/check/v4-2023-04-27"]
    out: Dict[str, Any] = {"remaining": 0, "raw": {}}
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for url in endpoints:
            try:
                r = await client.get(url, headers=headers)
                text = r.text
                data = r.json() if text else {}
                out["raw"][url] = data
                if r.status_code < 400 and isinstance(data, dict):
                    # ChatGPT 没有稳定公开“余额”字段；尽量抽取 usage/credits，缺省 0。
                    for k in ("remaining", "remaining_quota", "credits", "credit_grants"):
                        if k in data:
                            out["remaining"] = _int(data.get(k), 0)
            except Exception as e:
                out["raw"][url] = {"error": str(e)}
    return out


async def gpt_fetch_membership_by_proxy(*, access_token: str, target_url: str = DEFAULT_GPT_TARGET, db: Any = None, picked: Any = None) -> Dict[str, Any]:
    if not access_token:
        raise NonPenalizedTaskError("缺少 GPT access_token", status_code=401)
    info = await gpt_fetch_balance_by_proxy(access_token=access_token, target_url=target_url, db=db, picked=picked)
    raw = info.get("raw") or {}
    tier = None
    for v in raw.values():
        if isinstance(v, dict):
            tier = v.get("account_plan") or v.get("plan_type") or v.get("subscription") or tier
    return {"membership": tier, "raw": raw}


async def refresh_gpt_balance(ctx: Any) -> int:
    row = ctx.mapping_row or {}
    at = str(row.get("sora_access_token") or "").strip()
    info = await gpt_fetch_balance_by_proxy(access_token=at, target_url=str(row.get("default_target_url") or DEFAULT_GPT_TARGET), db=ctx.db)
    remaining = _int(info.get("remaining"), 0)
    await ctx.db.update_task_type_window(mapping_id=int(row.get("id") or 0), remaining_quota=remaining, sora_remaining_count=remaining)
    return remaining


async def gpt_workflow(
    payload: Dict[str, Any],
    progress_cb: ProgressCB,
    *,
    browser_vendor: str,
    browser_base_url: str,
    browser_access_key: Optional[str],
    space_id: str,
    window_key: str,
    timeout_seconds: float,
    access_token: Optional[str] = None,
    access_expires: Optional[str] = None,
    default_target_url: Optional[str] = None,
    headless: bool = False,
    pure_mode: bool = True,
    db: Any = None,
    task_type_window_id: Optional[int] = None,
) -> Dict[str, Any]:
    payload = dict(payload or {})
    prompt = str(payload.get("prompt") or "").strip()
    if not prompt:
        raise NonPenalizedTaskError("payload.prompt 不能为空", status_code=400)
    if not should_use_extension_executor({**payload, "executor": payload.get("executor") or "extension"}):
        raise NonPenalizedTaskError("GPT workflow only supports browser extension mode", status_code=400)

    target_url = str(payload.get("gpt_url") or payload.get("target_url") or default_target_url or DEFAULT_GPT_TARGET)
    sess = get_or_create_veo_session(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )
    try:
        if await wait_extension_client(space_id, window_key, timeout_seconds=0.2) is None:
            sess.browser_headless = headless
            sess.browser_pure_mode = pure_mode
            await trigger_extension_ws_connection_via_window(
                sess=sess,
                target_url=target_url,
                space_id=space_id,
                window_key=window_key,
                wait_seconds=float(payload.get("extension_connect_wait_seconds") or 10.0),
                headless=headless,
                pure_mode=pure_mode,
                log_file=sess._log_file,
            )
    except Exception as e:
        append_log(sess._log_file, f"[gpt] trigger/wait extension failed: {e}")

    at = str(access_token or "").strip()
    exp = str(access_expires or "").strip() or None
    if db is not None and task_type_window_id:
        try:
            row = await db.get_task_type_window_context(int(task_type_window_id))
            at = str((row or {}).get("sora_access_token") or at).strip()
            exp = str((row or {}).get("sora_access_expires") or exp or "").strip() or None
        except Exception:
            pass
    if not at:
        tok = await gpt_fetch_access_token_in_window(
            browser_vendor=browser_vendor, browser_base_url=browser_base_url, browser_access_key=browser_access_key,
            space_id=space_id, window_key=window_key, target_url=target_url, headless=headless, pure_mode=pure_mode,
        )
        at = str(tok.get("access_token") or "").strip()
        exp = str(tok.get("expires") or "").strip() or None
        if at and db is not None and task_type_window_id:
            await db.update_task_type_window(mapping_id=int(task_type_window_id), sora_access_token=at, sora_access_expires=exp)
    if not at:
        raise NonPenalizedTaskError("缺少 GPT access_token：请确认 ChatGPT 已登录", status_code=401)

    ext_payload = dict(payload)
    ext_payload.update({
        "target_url": target_url,
        "access_token": at,
        "access_expires": exp,
        "workflow_kind": "video" if _is_video_payload(payload) else "image",
        "timeout_seconds": timeout_seconds,
    })
    return await submit_extension_task(
        space_id=space_id, window_key=window_key, provider="gpt", payload=ext_payload,
        progress_cb=progress_cb, timeout_seconds=float(payload.get("max_wait_seconds") or timeout_seconds),
    )
