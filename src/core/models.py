"""Data models for FPBrowser2API (Pydantic).

说明：
- 这些模型主要用于：DB 行 <-> Python 对象、以及 API 响应结构
- 数据库存储仍以 `aiosqlite` 为主（参考 flow2api）
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class AdminUser(BaseModel):
    id: Optional[int] = None
    username: str
    password_hash: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class SystemConfig(BaseModel):
    id: int = 1
    proxy_enabled: bool = False
    proxy_url: Optional[str] = None
    api_key: str = "fpb123456"
    debug_enabled: bool = False
    log_to_file: bool = False
    # 开关：停止接收新任务（维护模式）
    stop_accepting_tasks: bool = False
    updated_at: Optional[datetime] = None


class Project(BaseModel):
    id: Optional[int] = None
    name: str
    deleted: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class FingerprintBrowser(BaseModel):
    id: Optional[int] = None
    project_id: int
    name: str
    lan_addr: str  # 局域网地址（如 http://192.168.1.10:50000）
    vendor: str = "generic"  # roxy/gologin/adspower/...（预留扩展）
    access_key: Optional[str] = None  # 指纹浏览器侧 API Key（如需要）
    deleted: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class BrowserSpace(BaseModel):
    id: Optional[int] = None
    browser_id: int
    name: str
    space_id: str
    # 可选：RoxyBrowser /browser/list_v3 的 projectIds 过滤参数（格式 "10,11"）
    project_ids: Optional[str] = None
    deleted: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class WindowInfo(BaseModel):
    """空间下窗口信息（从指纹浏览器同步而来，不允许手工编辑）。"""

    id: Optional[int] = None
    space_pk: int  # BrowserSpace.id
    window_key: str  # 指纹浏览器窗口唯一标识（若无则使用 name/url 的 hash）

    window_name: str
    platform_account: Optional[str] = None
    platform_url: Optional[str] = None

    proxy_addr: Optional[str] = None
    proxy_country: Optional[str] = None
    proxy_expire_at: Optional[str] = None

    enabled: bool = True
    deleted: bool = False

    raw: Optional[Dict[str, Any]] = None  # 保存原始窗口信息 JSON（便于排查/扩展）
    synced_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class TaskType(BaseModel):
    id: Optional[int] = None
    name: str
    code: str  # 英文唯一
    concurrency: int = 1
    continuous_error_threshold: int = 3
    timeout_seconds: int = 1800
    # 动态函数 key（来自 services/task_handler_registry.py）
    create_task_handler: Optional[str] = None
    refresh_quota_handler: Optional[str] = None
    enabled: bool = True
    deleted: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class TaskTypePublic(BaseModel):
    id: Optional[int] = None
    name: str
    code: str  # 英文唯一
    # 动态函数 key（来自 services/task_handler_registry.py）
    enabled: bool = True
    created_at: Optional[datetime] = None

class TaskTypeWindow(BaseModel):
    id: Optional[int] = None
    task_type_id: int
    window_pk: int  # WindowInfo.id

    # 运行中/已预占的并发槽位（用于高并发下的“窗口级”并发限制）
    # 说明：必须由 DB 原子 UPDATE 控制增减，避免多进程/多实例下超卖
    inflight_slots: int = 0

    total_errors: int = 0
    consecutive_errors: int = 0

    daily_quota: int = 0
    remaining_quota: int = 0

    cooldown_until: Optional[datetime] = None
    # 连续错误熔断冷却时间（与额度重置时间点 cooldown_until 区分）
    error_cooldown_until: Optional[datetime] = None
    enabled: bool = True
    deleted: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class Task(BaseModel):
    id: Optional[int] = None
    task_id: str
    task_type_code: str

    status: str  # queued/running/completed/failed
    progress: int = 0

    prompt: str
    image_path: Optional[str] = None

    window_pk: Optional[int] = None
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class RequestLog(BaseModel):
    id: Optional[int] = None
    actor: Optional[str] = None  # admin / api / username ...
    method: str
    path: str
    request_body: Optional[str] = None
    response_body: Optional[str] = None
    status_code: int
    duration: float
    created_at: Optional[datetime] = None


class AutoRefreshErrorLog(BaseModel):
    """定时刷新额度失败记录（用于风控/排查与 UI 展示）。"""

    id: Optional[int] = None
    mapping_id: int
    task_type_id: Optional[int] = None
    task_code: Optional[str] = None
    window_pk: Optional[int] = None
    window_name: Optional[str] = None
    platform_account: Optional[str] = None
    error_message: str
    created_at: Optional[datetime] = None


class WindowPickerResult(BaseModel):
    window: WindowInfo
    mapping: TaskTypeWindow


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    progress: int
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

