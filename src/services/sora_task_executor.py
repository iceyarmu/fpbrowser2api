"""Sora 任务执行器（只包含 Sora 相关执行入口）。

说明：
- `_SoraBrowserContext` / `_get_or_create_ctx` 等复用逻辑在 `sora_browser_context.py`
- 其它任务类型（如图片生成）请放到各自的执行器文件中（例如 `image_task_executor.py`）
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .sora_browser_context import _get_or_create_ctx
from .task_executor_types import ProgressCB


def _pick_orientation_from_ratio(ratio: Optional[str]) -> Optional[str]:
    """将尺寸比例（如 19:6 / 6:19）转换为 sora 的 orientation（landscape/portrait）。"""

    if not ratio:
        return None
    s = str(ratio).strip().lower().replace("：", ":")
    if "19:6" in s:
        return "landscape"
    if "6:19" in s:
        return "portrait"
    return None


def _pick_n_frames(v: Any) -> int:
    """将“时长参数”归一化为 n_frames（当前按你的需求仅支持 300/450）。"""

    try:
        iv = int(float(v))
    except Exception:
        iv = 300
    if iv in (300, 450):
        return iv
    # 常见：秒数 10/15
    if iv == 10:
        return 300
    if iv == 15:
        return 450
    return 450


async def sora_gen_video(
    payload: Dict[str, Any],
    progress_cb: ProgressCB,
    *,
    browser_vendor: str,
    browser_base_url: str,
    browser_access_key: Optional[str],
    space_id: str,
    window_key: str,
    timeout_seconds: float,
) -> Dict[str, Any]:
    """Sora 生视频：复用同一指纹浏览器窗口 + Playwright(CDP) 轻量连接，拆分“创建任务”和“进度轮询”。

    参数来源：
    - 运行时浏览器参数来自 TaskService（picked window / browser / space）
    - 业务参数从 payload 读取（prompt / url / regex / 超时等）
    """

    # 延迟导入：避免循环依赖（sora_browser_context 会 import task_executor 中的 helper）
    from .task_executor import _safe_trim  # type: ignore

    payload = payload or {}
    prompt = str(payload.get("prompt") or "").strip()
    if not prompt:
        raise ValueError("payload.prompt 不能为空")

    # 新增：首帧参考图 / 尺寸比例 / 时长（映射到 nf/create 的 inpaint_items/orientation/n_frames）
    first_image_url = str(payload.get("first_image_url") or payload.get("firstImageUrl") or "").strip() or None
    ratio = str(payload.get("size_ratio") or payload.get("aspect_ratio") or payload.get("ratio") or payload.get("尺寸") or "").strip() or None
    orientation = _pick_orientation_from_ratio(ratio) or str(payload.get("orientation") or "").strip() or None
    duration_v = payload.get("n_frames") or payload.get("duration_frames") or payload.get("duration") or payload.get("时长")
    n_frames = _pick_n_frames(duration_v)

    # Playwright 行为配置（可从 payload 覆盖；默认值与旧 task_executor.py 保持一致）
    target_url = str(payload.get("sora_url") or "https://sora.chatgpt.com/drafts").strip()
    create_button_text_regex = str(payload.get("sora_create_video_regex") or r"^\s*Create\s+video\s*$").strip()
    monitor_seconds = float(payload.get("sora_monitor_seconds") or 8.0)
    monitor_url_regex = str(payload.get("sora_monitor_url_regex") or r"https://sora\.chatgpt\.com/backend/nf/create").strip()
    monitor_log_path = (str(payload.get("sora_monitor_log_path") or "").strip() or None)
    pending_url_regex = str(payload.get("sora_pending_url_regex") or r"https://sora\.chatgpt\.com/backend/nf/pending/v2").strip()
    max_wait_seconds = float(payload.get("sora_pending_max_wait_seconds") or max(30.0, min(float(timeout_seconds), 60.0 * 10)))

    ctx = _get_or_create_ctx(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )
    # 轮询与 ctx 回收策略
    poll_interval_seconds = float(payload.get("sora_pending_poll_interval_seconds") or 1.0)
    sniff_timeout_seconds = float(payload.get("sora_pending_sniff_timeout_seconds") or 4.0)
    idle_close_seconds = float(payload.get("ctx_idle_close_seconds") or 30.0)

    try:
        from playwright.async_api import async_playwright  # type: ignore  # noqa: F401
    except Exception as e:
        raise RuntimeError(
            "Playwright 未安装或导入失败，请先安装依赖：pip install playwright；并执行：python -m playwright install chromium；"
            f"错误：{_safe_trim(str(e), 400)}"
        )

    await progress_cb(0, {"stage": "create_task"})
    task_id, _create_tx = await ctx.create_task(
        prompt=prompt,
        target_url=target_url,
        create_button_text_regex=create_button_text_regex,
        monitor_seconds=monitor_seconds,
        monitor_url_regex=monitor_url_regex,
        monitor_log_path=monitor_log_path,
        first_image_url=first_image_url,
        orientation=str(orientation or "portrait"),
        n_frames=int(n_frames),
        browser_open_args=[],
        browser_force_open=False,
        browser_headless=False,
    )

    await progress_cb(1, {"stage": "created", "task_id": task_id})
    await progress_cb(1, {"stage": "monitor_progress", "task_id": task_id})
    progress_result = await ctx.watch_task_progress(
        task_id=task_id,
        progress_cb=progress_cb,
        pending_url_regex=pending_url_regex,
        monitor_log_path=monitor_log_path,
        max_wait_seconds=max_wait_seconds,
        poll_interval_seconds=poll_interval_seconds,
        sniff_timeout_seconds=sniff_timeout_seconds,
        idle_close_seconds=idle_close_seconds,
    )

    await progress_cb(95, {"stage": "drafts_and_publish", "task_id": task_id})
    publish_result = await ctx.finalize_video_and_publish(
        task_id=task_id,
        prompt=prompt,
        target_url=target_url,
        drafts_limit=int(payload.get("sora_drafts_limit") or 100),
    )
    # 成功后顺带读取余额（nf/check）
    nf_check = None
    try:
        nf_check = await ctx.api_nf_check(target_url=target_url)
    except Exception:
        nf_check = None
    await progress_cb(100, {"stage": "done", "task_id": task_id, "post_id": publish_result.get("post_id")})

    # progress_result 目前主要用于调试/诊断：先保留以备未来扩展（如返回更细状态）
    _ = progress_result

    return {
        "type": "video",
        "message": "Sora创建完成",
        "task_id": task_id,
        "post_id": publish_result.get("post_id"),
        "share_url": publish_result.get("share_url"),
        "watermark_free_url": publish_result.get("watermark_free_url"),
        "nf_check": nf_check,
    }

