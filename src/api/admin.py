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
    stop_accepting_tasks: bool = False


class CreateProjectRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class UpdateProjectRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class CreateBrowserRequest(BaseModel):
    project_id: int
    name: str = Field(min_length=1, max_length=100)
    lan_addr: str = Field(min_length=3, max_length=255)
    vendor: str = Field(default="roxy", max_length=50)
    access_key: Optional[str] = Field(default=None, max_length=255)


class UpdateBrowserRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    lan_addr: str = Field(min_length=3, max_length=255)
    vendor: str = Field(default="roxy", max_length=50)
    access_key: Optional[str] = Field(default=None, max_length=255)


class CreateSpaceRequest(BaseModel):
    browser_id: int
    name: str = Field(min_length=1, max_length=100)
    space_id: str = Field(min_length=1, max_length=128)
    # 可选：RoxyBrowser /browser/list_v3 的 projectIds（格式 "10,11"）
    project_ids: Optional[str] = Field(default=None, max_length=512)


class UpdateSpaceRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    space_id: str = Field(min_length=1, max_length=128)
    project_ids: Optional[str] = Field(default=None, max_length=512)


class CreateTaskTypeRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    code: str = Field(min_length=2, max_length=64, pattern=r"^[a-zA-Z0-9_]+$")
    concurrency: int = Field(default=1, ge=1, le=999)
    continuous_error_threshold: int = Field(default=3, ge=1, le=999)
    timeout_seconds: int = Field(default=1800, ge=10, le=24 * 3600)
    create_task_handler: Optional[str] = None
    refresh_quota_handler: Optional[str] = None


class UpdateTaskTypeRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    code: str = Field(min_length=2, max_length=64, pattern=r"^[a-zA-Z0-9_]+$")
    concurrency: int = Field(default=1, ge=1, le=999)
    continuous_error_threshold: int = Field(default=3, ge=1, le=999)
    timeout_seconds: int = Field(default=1800, ge=10, le=24 * 3600)
    create_task_handler: Optional[str] = None
    refresh_quota_handler: Optional[str] = None
    enabled: bool = True


class AddTaskTypeWindowsRequest(BaseModel):
    window_pks: List[int] = Field(min_length=1)
    daily_quota: int = Field(default=0, ge=0, le=100000)
    remaining_quota: int = Field(default=0, ge=0, le=100000)
    enabled: bool = True


class UpdateTaskTypeWindowRequest(BaseModel):
    enabled: Optional[bool] = None
    deleted: Optional[bool] = None
    daily_quota: Optional[int] = Field(default=None, ge=0, le=100000)
    remaining_quota: Optional[int] = Field(default=None, ge=0, le=100000)
    cooldown_until: Optional[str] = None  # ISO or empty
    error_cooldown_until: Optional[str] = None  # ISO or empty
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
            "stop_accepting_tasks": bool(getattr(syscfg, "stop_accepting_tasks", False)),
            "admin_username": admin_user.username if admin_user else "admin",
        },
    }


@router.get("/api/admin/ui-defaults")
async def get_ui_defaults(token: str = Depends(verify_admin_token)):
    """给管理台前端提供的默认值（避免页面写死 magic number）。"""
    try:
        # dataclass 字段默认值，无需实例化（SoraSession 需要 pw_ctx 参数）
        from ..services.sora_task_executor import SoraSession  # type: ignore

        idle_close_seconds_default = float(SoraSession.__dataclass_fields__["idle_close_seconds"].default)  # type: ignore[attr-defined]
    except Exception:
        idle_close_seconds_default = 30.0

    return {
        "success": True,
        "defaults": {
            # 点击“关闭”后，会触发 _schedule_idle_close；这里的默认倒计时提示应与执行器默认值一致
            "idle_close_seconds": idle_close_seconds_default,
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
        stop_accepting_tasks=req.stop_accepting_tasks,
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
    sid = await db.create_space(req.browser_id, req.name, req.space_id, req.project_ids)
    return {"success": True, "space_pk": sid}


@router.put("/api/admin/spaces/{space_pk}")
async def update_space(space_pk: int, req: UpdateSpaceRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.update_space(space_pk, req.name, req.space_id, req.project_ids)
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
    try:
        windows = await client.list_windows(
            vendor=browser.vendor,
            base_url=browser.lan_addr,
            access_key=browser.access_key,
            space_id=space.space_id,
            project_ids=getattr(space, "project_ids", None),
        )
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

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
        # 校验 handler key（避免保存后运行时报错）
        from ..services.task_handler_registry import get_create_task_handler, get_refresh_quota_handler

        if req.create_task_handler:
            get_create_task_handler(req.create_task_handler)
        if req.refresh_quota_handler:
            get_refresh_quota_handler(req.refresh_quota_handler)

        tid = await db.create_task_type(
            req.name,
            req.code,
            req.concurrency,
            req.continuous_error_threshold,
            req.timeout_seconds,
            create_task_handler=req.create_task_handler,
            refresh_quota_handler=req.refresh_quota_handler,
        )
        return {"success": True, "task_type_id": tid}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/api/admin/task-types/{task_type_id}")
async def update_task_type(task_type_id: int, req: UpdateTaskTypeRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    try:
        # 校验 handler key（避免保存后运行时报错）
        from ..services.task_handler_registry import get_create_task_handler, get_refresh_quota_handler

        if req.create_task_handler:
            get_create_task_handler(req.create_task_handler)
        if req.refresh_quota_handler:
            get_refresh_quota_handler(req.refresh_quota_handler)

        await db.update_task_type(
            task_type_id=task_type_id,
            name=req.name,
            code=req.code,
            concurrency=req.concurrency,
            continuous_error_threshold=req.continuous_error_threshold,
            timeout_seconds=req.timeout_seconds,
            create_task_handler=req.create_task_handler,
            refresh_quota_handler=req.refresh_quota_handler,
            enabled=req.enabled,
        )
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# -------------------- task type dynamic handlers --------------------
@router.get("/api/admin/task-type-handler-options")
async def list_task_type_handler_options(token: str = Depends(verify_admin_token)):
    from ..services.task_handler_registry import list_create_task_handler_options, list_refresh_quota_handler_options

    return {
        "success": True,
        "create_task_handlers": list_create_task_handler_options(),
        "refresh_quota_handlers": list_refresh_quota_handler_options(),
    }


@router.post("/api/admin/task-type-windows/{mapping_id}/refresh-remaining-quota")
async def refresh_mapping_remaining_quota(
    mapping_id: int,
    source: Optional[str] = None,  # auto/manual
    token: str = Depends(verify_admin_token),
):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    ctx_row = await db.get_task_type_window_context(mapping_id)
    if not ctx_row:
        raise HTTPException(status_code=404, detail="mapping not found")

    # 以 task_type 配置为准（避免 join 字段不全）
    task_code = str(ctx_row.get("task_code") or "").strip()
    if not task_code:
        raise HTTPException(status_code=400, detail="task_code missing")

    task_type = await db.get_task_type_by_code(task_code)
    if not task_type:
        raise HTTPException(status_code=404, detail="task_type not found")

    from ..services.task_handler_registry import RefreshQuotaContext, get_refresh_quota_handler

    try:
        fn = get_refresh_quota_handler(task_type.refresh_quota_handler)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        new_remaining = await fn(RefreshQuotaContext(task_type=task_type, mapping_row=ctx_row, db=db))
        new_remaining = max(0, int(new_remaining))
    except Exception as e:
        # 仅当“定时刷新”触发（source=auto）时，写入错误日志并自动禁用
        try:
            from ..core.models import AutoRefreshErrorLog

            await db.add_auto_refresh_error_log(
                AutoRefreshErrorLog(
                    mapping_id=int(mapping_id),
                    task_type_id=int(ctx_row.get("task_type_id") or 0) or None,
                    task_code=str(ctx_row.get("task_code") or "").strip() or None,
                    window_pk=int(ctx_row.get("window_pk") or 0) or None,
                    window_name=str(ctx_row.get("window_name") or "").strip() or None,
                    platform_account=str(ctx_row.get("platform_account") or "").strip() or None,
                    error_message=str(e),
                )
            )
        except Exception:
            pass
        try:
            await db.update_task_type_window(mapping_id=mapping_id, enabled=False)
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"刷新额度失败：{e}（已记录并禁用该窗口）")


    await db.update_task_type_window(mapping_id=mapping_id, remaining_quota=new_remaining)
    return {"success": True, "mapping_id": mapping_id, "remaining_quota": new_remaining, "handler": task_type.refresh_quota_handler}


@router.get("/api/admin/auto-refresh-errors")
async def list_auto_refresh_errors(
    limit: int = 200,
    offset: int = 0,
    task_type_id: Optional[int] = None,
    mapping_id: Optional[int] = None,
    token: str = Depends(verify_admin_token),
):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    items = await db.list_auto_refresh_error_logs(limit=limit, offset=offset, task_type_id=task_type_id, mapping_id=mapping_id)
    return {"success": True, "limit": max(1, min(500, int(limit or 200))), "offset": max(0, int(offset or 0)), "items": items}


@router.post("/api/admin/task-type-windows/{mapping_id}/refresh-invite-code")
async def refresh_mapping_invite_code(mapping_id: int, token: str = Depends(verify_admin_token)):
    """通过指纹浏览器读取 backend/project_y/invite/mine 并写回 mapping.sora_invite_code。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    ctx_row = await db.get_task_type_window_context(mapping_id)
    if not ctx_row:
        raise HTTPException(status_code=404, detail="mapping not found")

    vendor = str(ctx_row.get("vendor") or "roxy")
    base_url = str(ctx_row.get("lan_addr") or "")
    access_key = ctx_row.get("access_key")
    space_id = str(ctx_row.get("space_id") or "")
    window_key = str(ctx_row.get("window_key") or "")
    if not base_url or not space_id or not window_key:
        raise HTTPException(status_code=400, detail="mapping missing vendor/lan_addr/space_id/window_key")

    from ..services.sora_task_executor import get_or_create_sora_session  # type: ignore

    sora_ctx = get_or_create_sora_session(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
    try:
        info = await sora_ctx.api_invite_mine(target_url="https://sora.chatgpt.com/drafts")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"刷新邀请码失败：{e}")

    invite_code = (info or {}).get("invite_code")
    await db.update_task_type_window(mapping_id=mapping_id, sora_invite_code=str(invite_code or "").strip() or None)
    return {
        "success": True,
        "mapping_id": mapping_id,
        "invite_code": invite_code,
        "redeemed_count": info.get("redeemed_count"),
        "total_count": info.get("total_count"),
    }


@router.post("/api/admin/task-type-windows/{mapping_id}/manual-open")
async def manual_open_mapping_window(mapping_id: int, token: str = Depends(verify_admin_token)):
    """手动打开窗口并禁止 _schedule_idle_close 自动关闭（保持窗口常驻）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    ctx_row = await db.get_task_type_window_context(mapping_id)
    if not ctx_row:
        raise HTTPException(status_code=404, detail="mapping not found")

    vendor = str(ctx_row.get("vendor") or "roxy")
    base_url = str(ctx_row.get("lan_addr") or "")
    access_key = ctx_row.get("access_key")
    space_id = str(ctx_row.get("space_id") or "")
    window_key = str(ctx_row.get("window_key") or "")
    if not base_url or not space_id or not window_key:
        raise HTTPException(status_code=400, detail="mapping missing vendor/lan_addr/space_id/window_key")

    from ..services.sora_task_executor import get_or_create_sora_session  # type: ignore

    sora_ctx = get_or_create_sora_session(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
    # 先禁止自动关闭，再确保已打开（避免 ensure_open / 其它调用尾部 schedule 进来）
    sora_ctx.idle_close_disabled = True
    try:
        sora_ctx._cancel_idle_close()
    except Exception:
        pass
    try:
        await sora_ctx.ensure_open(args=[], force_open=False, headless=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"打开窗口失败：{e}")

    return {"success": True, "mapping_id": mapping_id, "idle_close_disabled": True}


@router.post("/api/admin/task-type-windows/{mapping_id}/manual-close")
async def manual_close_mapping_window(mapping_id: int, token: str = Depends(verify_admin_token)):
    """取消“保持打开”并触发一次 _schedule_idle_close（不会立刻关闭，避免影响正在运行的任务）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    ctx_row = await db.get_task_type_window_context(mapping_id)
    if not ctx_row:
        raise HTTPException(status_code=404, detail="mapping not found")

    vendor = str(ctx_row.get("vendor") or "roxy")
    base_url = str(ctx_row.get("lan_addr") or "")
    access_key = ctx_row.get("access_key")
    space_id = str(ctx_row.get("space_id") or "")
    window_key = str(ctx_row.get("window_key") or "")
    if not base_url or not space_id or not window_key:
        raise HTTPException(status_code=400, detail="mapping missing vendor/lan_addr/space_id/window_key")

    from ..services.sora_task_executor import get_or_create_sora_session  # type: ignore

    sora_ctx = get_or_create_sora_session(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
    sora_ctx.idle_close_disabled = False
    try:
        sora_ctx._schedule_idle_close()
    except Exception:
        pass

    return {"success": True, "mapping_id": mapping_id, "idle_close_disabled": False}


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
        cooldown_until=req.cooldown_until,
        error_cooldown_until=req.error_cooldown_until,
        total_errors=req.total_errors,
        consecutive_errors=req.consecutive_errors,
    )
    return {"success": True}


# -------------------- tasks --------------------
@router.get("/api/admin/tasks")
async def list_tasks(
    limit: int = 50,
    offset: int = 0,
    task_type_code: Optional[str] = None,
    status: Optional[str] = None,
    window_pk: Optional[int] = None,
    q: Optional[str] = None,
    token: str = Depends(verify_admin_token),
):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    lim = max(1, min(200, int(limit or 50)))
    off = max(0, int(offset or 0))
    total = await db.count_tasks(task_type_code=task_type_code, status=status, window_pk=window_pk, q=q)
    items = await db.list_tasks(limit=lim, offset=off, task_type_code=task_type_code, status=status, window_pk=window_pk, q=q)
    return {"success": True, "total": total, "limit": lim, "offset": off, "items": items}


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

