"""任务执行器：通用类型定义。

说明：
- 该文件只放“跨执行器复用”的类型，避免 image/video/sora 等模块互相耦合。
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Optional

# progress_cb(progress_percent, payload)
ProgressCB = Callable[[int, Optional[Dict[str, Any]]], Awaitable[None]]

