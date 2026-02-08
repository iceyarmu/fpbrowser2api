"""Admin API routes (management console)."""

from __future__ import annotations

import secrets
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel, Field

from ..core.auth import AuthManager
from ..core.database import Database
from ..core.logger import setup_logging
from ..services.fp_browser_client import FPBrowserClient


router = APIRouter()

# dependency injection (set in src/main.py)
db: Database | None = None

active_admin_tokens: dict[str, str] = {}  # token -> username


def set_dependencies(database: Database) -> None:
    global db
    db = database


# -------------------- models --------------------
class LoginRequest(BaseModel):
    username: str
    password: str


class ChangePasswordRequest(BaseModel):
    username: Optional[str] = None
    old_password: str
    new_password: str = Field(min_length=4)


class UpdateAPIKeyRequest(BaseModel):
    api_key: str = Field(min_length=6)


class UpdateSystemConfigRequest(BaseModel):
    proxy_enabled: bool
    proxy_url: Optional[str] = None
    debug_enabled: bool
    log_to_file: bool


class CreateProjectRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class UpdateProjectRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class CreateBrowserRequest(BaseModel):
    project_id: int
    name: str = Field(min_length=1, max_length=100)
    lan_addr: str = Field(min_length=3, max_length=255)
    vendor: str = Field(default="generic", max_length=50)
    access_key: Optional[str] = Field(default=None, max_length=255)


class UpdateBrowserRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    lan_addr: str = Field(min_length=3, max_length=255)
    vendor: str = Field(default="generic", max_length=50)
    access_key: Optional[str] = Field(default=None, max_length=255)


class CreateSpaceRequest(BaseModel):
    browser_id: int
    name: str = Field(min_length=1, max_length=100)
    space_id: str = Field(min_length=1, max_length=128)


class UpdateSpaceRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    space_id: str = Field(min_length=1, max_length=128)


class CreateTaskTypeRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    code: str = Field(min_length=2, max_length=64, pattern=r"^[a-zA-Z0-9_]+$")
    concurrency: int = Field(default=1, ge=1, le=999)
    continuous_error_threshold: int = Field(default=3, ge=1, le=999)
    timeout_seconds: int = Field(default=1800, ge=10, le=24 * 3600)


class UpdateTaskTypeRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    concurrency: int = Field(default=1, ge=1, le=999)
    continuous_error_threshold: int = Field(default=3, ge=1, le=999)
    timeout_seconds: int = Field(default=1800, ge=10, le=24 * 3600)
    enabled: bool = True


class AddTaskTypeWindowsRequest(BaseModel):
    window_pks: List[int] = Field(min_length=1)
    daily_quota: int = Field(default=0, ge=0, le=100000)
    remaining_quota: int = Field(default=0, ge=0, le=100000)
    max_concurrency: int = Field(default=1, ge=1, le=999)
    enabled: bool = True


class UpdateTaskTypeWindowRequest(BaseModel):
    enabled: Optional[bool] = None
    deleted: Optional[bool] = None
    daily_quota: Optional[int] = Field(default=None, ge=0, le=100000)
    remaining_quota: Optional[int] = Field(default=None, ge=0, le=100000)
    max_concurrency: Optional[int] = Field(default=None, ge=1, le=999)
    cooldown_until: Optional[str] = None  # ISO or empty
    total_errors: Optional[int] = Field(default=None, ge=0, le=1000000)
    consecutive_errors: Optional[int] = Field(default=None, ge=0, le=1000000)


# -------------------- auth helper --------------------
async def verify_admin_token(authorization: str = Header(None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing authorization")
    token = authorization[7:]
    if token not in active_admin_tokens:
        raise HTTPException(status_code=401, detail="Invalid or expired admin token")
    return token


# -------------------- auth endpoints --------------------
@router.post("/api/admin/login")
async def admin_login(req: LoginRequest):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    user = await db.get_admin_user(req.username.strip())
    if not user or not AuthManager.verify_password(req.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    session_token = f"admin-{secrets.token_urlsafe(32)}"
    active_admin_tokens[session_token] = user.username
    return {"success": True, "token": session_token, "username": user.username}


@router.post("/api/login")
async def login_alias(req: LoginRequest):
    """前端兼容：/api/login -> /api/admin/login"""
    return await admin_login(req)


@router.post("/api/admin/logout")
async def admin_logout(token: str = Depends(verify_admin_token)):
    active_admin_tokens.pop(token, None)
    return {"success": True, "message": "退出登录成功"}


@router.post("/api/logout")
async def logout_alias(token: str = Depends(verify_admin_token)):
    """前端兼容：/api/logout -> /api/admin/logout"""
    return await admin_logout(token)


@router.post("/api/admin/change-password")
async def change_password(req: ChangePasswordRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    current_username = active_admin_tokens.get(token)
    if not current_username:
        raise HTTPException(status_code=401, detail="Invalid session")

    user = await db.get_admin_user(current_username)
    if not user:
        raise HTTPException(status_code=400, detail="管理员账号不存在")

    if not AuthManager.verify_password(req.old_password, user.password_hash):
        raise HTTPException(status_code=400, detail="旧密码错误")

    # 更新密码
    await db.update_admin_password(user.username, AuthManager.hash_password(req.new_password))

    # 可选更新用户名
    if req.username and req.username.strip() and req.username.strip() != user.username:
        await db.update_admin_username(user.username, req.username.strip())

    # 安全起见：清空全部会话，强制重登
    active_admin_tokens.clear()
    return {"success": True, "message": "密码修改成功，请重新登录"}


# -------------------- system config --------------------
@router.get("/api/admin/system-config")
async def get_system_config(token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    syscfg = await db.get_system_config()
    admin_user = await db.get_first_admin_user()
    return {
        "success": True,
        "config": {
            "proxy_enabled": syscfg.proxy_enabled,
            "proxy_url": syscfg.proxy_url or "",
            "api_key": syscfg.api_key,
            "debug_enabled": syscfg.debug_enabled,
            "log_to_file": syscfg.log_to_file,
            "admin_username": admin_user.username if admin_user else "admin",
        },
    }


@router.post("/api/admin/system-config")
async def update_system_config(req: UpdateSystemConfigRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    proxy_url = (req.proxy_url or "").strip() or None
    await db.update_system_config(
        proxy_enabled=req.proxy_enabled,
        proxy_url=proxy_url,
        debug_enabled=req.debug_enabled,
        log_to_file=req.log_to_file,
    )
    await db.reload_config_to_memory()
    setup_logging()  # 让日志配置立刻生效
    return {"success": True, "message": "系统配置已更新"}


@router.post("/api/admin/api-key")
async def update_api_key(req: UpdateAPIKeyRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.update_system_config(api_key=req.api_key.strip())
    await db.reload_config_to_memory()
    return {"success": True, "message": "API Key 已更新"}


# -------------------- project management --------------------
@router.get("/api/admin/projects")
async def list_projects(token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "projects": [p.model_dump() for p in await db.list_projects()]}


@router.post("/api/admin/projects")
async def create_project(req: CreateProjectRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    pid = await db.create_project(req.name)
    return {"success": True, "project_id": pid}


@router.put("/api/admin/projects/{project_id}")
async def update_project(project_id: int, req: UpdateProjectRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.update_project(project_id, req.name)
    return {"success": True}


@router.delete("/api/admin/projects/{project_id}")
async def delete_project(project_id: int, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.delete_project(project_id)
    return {"success": True}


# -------------------- browsers & spaces --------------------
@router.get("/api/admin/projects/{project_id}/browsers")
async def list_browsers(project_id: int, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "browsers": [b.model_dump() for b in await db.list_browsers(project_id)]}


@router.post("/api/admin/browsers")
async def create_browser(req: CreateBrowserRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    bid = await db.create_browser(req.project_id, req.name, req.lan_addr, req.vendor, req.access_key)
    return {"success": True, "browser_id": bid}


@router.put("/api/admin/browsers/{browser_id}")
async def update_browser(browser_id: int, req: UpdateBrowserRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.update_browser(browser_id, req.name, req.lan_addr, req.vendor, req.access_key)
    return {"success": True}


@router.delete("/api/admin/browsers/{browser_id}")
async def delete_browser(browser_id: int, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.delete_browser(browser_id)
    return {"success": True}


@router.get("/api/admin/browsers/{browser_id}/spaces")
async def list_spaces(browser_id: int, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "spaces": [s.model_dump() for s in await db.list_spaces(browser_id)]}


@router.post("/api/admin/spaces")
async def create_space(req: CreateSpaceRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    sid = await db.create_space(req.browser_id, req.name, req.space_id)
    return {"success": True, "space_pk": sid}


@router.put("/api/admin/spaces/{space_pk}")
async def update_space(space_pk: int, req: UpdateSpaceRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.update_space(space_pk, req.name, req.space_id)
    return {"success": True}


@router.delete("/api/admin/spaces/{space_pk}")
async def delete_space(space_pk: int, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.delete_space(space_pk)
    return {"success": True}


@router.get("/api/admin/spaces/{space_pk}/windows")
async def list_windows(space_pk: int, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "windows": [w.model_dump() for w in await db.list_windows(space_pk)]}


@router.post("/api/admin/spaces/{space_pk}/sync-windows")
async def sync_windows(space_pk: int, token: str = Depends(verify_admin_token)):
    """同步某个空间的窗口信息（从指纹浏览器拉取后写入本地 DB）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")

    browser = await db.get_browser(space.browser_id)
    if not browser:
        raise HTTPException(status_code=404, detail="browser not found")

    syscfg = await db.get_system_config()
    client = FPBrowserClient(
        proxy_enabled=syscfg.proxy_enabled,
        proxy_url=syscfg.proxy_url,
    )
    windows = await client.list_windows(
        vendor=browser.vendor,
        base_url=browser.lan_addr,
        access_key=browser.access_key,
        space_id=space.space_id,
    )

    affected = await db.upsert_windows(space_pk=space_pk, windows=windows)
    return {"success": True, "message": f"同步完成，写入/更新 {affected} 条窗口记录", "affected": affected}


@router.get("/api/admin/tree")
async def get_project_tree(project_id: Optional[int] = None, token: str = Depends(verify_admin_token)):
    """返回树形结构（项目 -> 浏览器 -> 空间 -> 窗口列表）。

    UI 侧可据此实现：项目切换、浏览器折叠/展开、空间加载窗口、以及“一键展开全部”。
    """
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    projects = await db.list_projects()
    if project_id is not None:
        projects = [p for p in projects if p.id == project_id]

    out: List[Dict[str, Any]] = []
    for p in projects:
        browsers = await db.list_browsers(p.id)  # type: ignore[arg-type]
        b_items: List[Dict[str, Any]] = []
        for b in browsers:
            spaces = await db.list_spaces(b.id)  # type: ignore[arg-type]
            s_items: List[Dict[str, Any]] = []
            for s in spaces:
                win_items = await db.list_windows(s.id)  # type: ignore[arg-type]
                s_items.append(
                    {
                        **s.model_dump(),
                        "windows": [w.model_dump(exclude={"raw"}) for w in win_items],
                    }
                )
            b_items.append({**b.model_dump(), "spaces": s_items})
        out.append({**p.model_dump(), "browsers": b_items})

    return {"success": True, "tree": out}


@router.get("/api/admin/windows/all")
async def list_all_windows(project_id: Optional[int] = None, token: str = Depends(verify_admin_token)):
    """用于“选择窗口绑定任务类型”的弹窗数据源。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    tree = (await get_project_tree(project_id=project_id, token=token))["tree"]
    flat: List[Dict[str, Any]] = []
    for p in tree:
        for b in p.get("browsers", []):
            for s in b.get("spaces", []):
                for w in s.get("windows", []):
                    flat.append(
                        {
                            "project_id": p.get("id"),
                            "project_name": p.get("name"),
                            "browser_id": b.get("id"),
                            "browser_name": b.get("name"),
                            "space_pk": s.get("id"),
                            "space_id": s.get("space_id"),
                            "space_name": s.get("name"),
                            "window_pk": w.get("id"),
                            "window_name": w.get("window_name"),
                            "platform_account": w.get("platform_account"),
                            "platform_url": w.get("platform_url"),
                            "proxy_addr": w.get("proxy_addr"),
                            "proxy_country": w.get("proxy_country"),
                            "proxy_expire_at": w.get("proxy_expire_at"),
                        }
                    )
    return {"success": True, "windows": flat}


# -------------------- task types --------------------
@router.get("/api/admin/task-types")
async def list_task_types(token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "task_types": [t.model_dump() for t in await db.list_task_types()]}


@router.post("/api/admin/task-types")
async def create_task_type(req: CreateTaskTypeRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    try:
        tid = await db.create_task_type(req.name, req.code, req.concurrency, req.continuous_error_threshold, req.timeout_seconds)
        return {"success": True, "task_type_id": tid}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/api/admin/task-types/{task_type_id}")
async def update_task_type(task_type_id: int, req: UpdateTaskTypeRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.update_task_type(
        task_type_id=task_type_id,
        name=req.name,
        concurrency=req.concurrency,
        continuous_error_threshold=req.continuous_error_threshold,
        timeout_seconds=req.timeout_seconds,
        enabled=req.enabled,
    )
    return {"success": True}


@router.delete("/api/admin/task-types/{task_type_id}")
async def delete_task_type(task_type_id: int, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.delete_task_type(task_type_id)
    return {"success": True}


@router.get("/api/admin/task-types/{task_type_id}/windows")
async def list_task_type_windows(task_type_id: int, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "items": await db.list_task_type_windows(task_type_id)}


@router.post("/api/admin/task-types/{task_type_id}/windows")
async def add_task_type_windows(task_type_id: int, req: AddTaskTypeWindowsRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    affected = await db.add_task_type_windows(
        task_type_id=task_type_id,
        window_pks=req.window_pks,
        daily_quota=req.daily_quota,
        remaining_quota=req.remaining_quota,
        max_concurrency=req.max_concurrency,
        enabled=req.enabled,
    )
    return {"success": True, "affected": affected}


@router.patch("/api/admin/task-type-windows/{mapping_id}")
async def update_task_type_window(mapping_id: int, req: UpdateTaskTypeWindowRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.update_task_type_window(
        mapping_id=mapping_id,
        enabled=req.enabled,
        deleted=req.deleted,
        daily_quota=req.daily_quota,
        remaining_quota=req.remaining_quota,
        max_concurrency=req.max_concurrency,
        cooldown_until=req.cooldown_until,
        total_errors=req.total_errors,
        consecutive_errors=req.consecutive_errors,
    )
    return {"success": True}


# -------------------- logs --------------------
@router.get("/api/admin/logs")
async def get_logs(limit: int = 200, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "logs": await db.get_request_logs(limit=limit)}


@router.delete("/api/admin/logs")
async def clear_logs(token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.clear_request_logs()
    return {"success": True}

