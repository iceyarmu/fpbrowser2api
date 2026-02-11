"""即梦（https://jimeng.jianying.com/）执行器（预留骨架）。

约定：
- 站点相关逻辑只放在本模块（接口路径、页面元素、鉴权、业务编排等）。
- 指纹浏览器/Playwright 的通用能力统一复用 `playwright_broswer_context.py`。

说明：
- 目前仅提供最小骨架与上下文获取方式，等你确定即梦的具体“创建任务/轮询/下载结果”流程后再补齐实现。
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .playwright_broswer_context import get_or_create_ctx
from .task_executor_types import ProgressCB


async def jimeng_gen_video(
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
    """即梦生视频（待实现）。

建议实现思路（后续你要扩展时照这个结构走）：
- ctx = get_or_create_ctx(...) 获取/复用同一指纹浏览器窗口的 Playwright 上下文
- await ctx.ensure_open(...) 连接 CDP 并拿到 ctx.page
- async with ctx.driver_lock: 在互斥区内完成页面操作/抓包/接口调用
- 持续调用 progress_cb(pct, payload) 汇报进度
"""

    _ = timeout_seconds
    payload = payload or {}

    ctx = get_or_create_ctx(
        vendor=browser_vendor,
        base_url=browser_base_url,
        access_key=browser_access_key,
        space_id=space_id,
        window_key=window_key,
    )
    await ctx.ensure_open(args=[], force_open=False, headless=False)

    await progress_cb(1, {"stage": "open", "url": "https://jimeng.jianying.com/"})
    async with ctx.driver_lock:
        await ctx.page.goto("https://jimeng.jianying.com/", wait_until="domcontentloaded")

    raise NotImplementedError("jimeng_gen_video 尚未实现：请提供即梦的具体交互/接口流程（创建任务、轮询、结果下载）。")

