"""任务执行器（当前先实现“可运行的模拟执行”）。

你后续要接入真实自动化（例如 Selenium/Playwright + 指纹浏览器窗口启动）时，
只需要在这里把 `simulate_*` 替换成真实执行逻辑，并持续调用 `progress_cb` 更新进度即可。
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, Optional


ProgressCB = Callable[[int, Optional[Dict[str, Any]]], Awaitable[None]]


async def simulate_image_task(prompt: str, image_path: Optional[str], progress_cb: ProgressCB) -> Dict[str, Any]:
    # 约 8 秒完成
    for p in (5, 15, 30, 45, 60, 75, 90):
        await asyncio.sleep(0.8)
        await progress_cb(p, None)
    await asyncio.sleep(0.8)
    await progress_cb(100, None)
    return {
        "type": "image",
        "message": "模拟执行完成（请在 task_executor.py 接入真实自动化）",
        "prompt": prompt,
        "image_path": image_path,
        "outputs": [],
    }


async def simulate_video_task(prompt: str, image_path: Optional[str], progress_cb: ProgressCB) -> Dict[str, Any]:
    # 约 25 秒完成
    for p in (3, 8, 15, 25, 35, 45, 55, 65, 75, 85, 92, 96):
        await asyncio.sleep(2.0)
        await progress_cb(p, None)
    await asyncio.sleep(1.5)
    await progress_cb(100, None)
    return {
        "type": "video",
        "message": "模拟执行完成（请在 task_executor.py 接入真实自动化）",
        "prompt": prompt,
        "image_path": image_path,
        "outputs": [],
    }

async def sora_gen_video(payload: Dict[str, Any], progress_cb: ProgressCB) -> Dict[str, Any]:
    """执行 sora 生视频（你稍后实现）。

    期望参数（都从 payload 读取）：
    - prompt: str
    - first_image_url: str (可选)
    - duration: int/float (可选)
    """

    _ = payload  # TODO: 你后续实现具体逻辑
    _cb = progress_cb
    # TODO: 你后续在这里实现（例如调用外部 API / 自动化执行器），并用 progress_cb 更新进度
    raise RuntimeError("sora_gen_video 尚未实现")

