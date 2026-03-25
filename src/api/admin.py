"""Admin API routes (management console)."""

from __future__ import annotations

import ipaddress
import re
import secrets
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import httpx
from fastapi import APIRouter, Depends, HTTPException, Header, Query
from pydantic import BaseModel, Field, field_validator

from ..core.auth import AuthManager
from ..core.database import Database
from ..core.logger import setup_logging
from ..core.public_api_limits import normalize_public_create_task_max_inflight
from ..services.fp_browser_client import FPBrowserClient

router = APIRouter()

# dependency injection (set in src/main.py)
db: Database | None = None

active_admin_tokens: dict[str, str] = {}  # token -> username

PAGE_KEYS: Set[str] = {
    "system",
    "projects",
    "task_types",
    "tasks",
    "tasks_gantt",
    "test",
    "card_keys",
    "logs",
    "users",
}


def set_dependencies(database: Database) -> None:
    global db
    db = database


async def _get_user_by_token(token: str):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    username = active_admin_tokens.get(token)
    if not username:
        raise HTTPException(status_code=401, detail="Invalid session")
    user = await db.get_admin_user(username)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user


async def _user_is_admin(user) -> bool:
    return bool(getattr(user, "is_admin", False))


async def _get_allowed_page_keys(user) -> Set[str]:
    if await _user_is_admin(user):
        return set(PAGE_KEYS)
    if not db:
        return set()
    rows = await db.get_user_page_permissions(int(user.id or 0))
    if not rows:
        return set(PAGE_KEYS)
    return {x for x in rows if x in PAGE_KEYS}


async def _ensure_page_access(token: str, page_key: str):
    user = await _get_user_by_token(token)
    pages = await _get_allowed_page_keys(user)
    if page_key not in pages:
        raise HTTPException(status_code=403, detail="无权访问该页面")
    return user


async def _ensure_any_page_access(token: str, page_keys: Set[str]):
    user = await _get_user_by_token(token)
    pages = await _get_allowed_page_keys(user)
    if not any((k in pages) for k in page_keys):
        raise HTTPException(status_code=403, detail="无权访问该页面")
    return user


async def _get_allowed_project_ids(user) -> Optional[List[int]]:
    if await _user_is_admin(user):
        return None
    if not db:
        return []
    rows = await db.get_user_project_permissions(int(user.id or 0))
    # 未设置则默认全可见
    return rows or None


async def _get_allowed_task_type_ids(user) -> Optional[List[int]]:
    if await _user_is_admin(user):
        return None
    if not db:
        return []
    rows = await db.get_user_task_type_permissions(int(user.id or 0))
    # 未设置则默认全可见
    return rows or None


async def _ensure_admin_user(token: str):
    user = await _get_user_by_token(token)
    if not await _user_is_admin(user):
        raise HTTPException(status_code=403, detail="仅管理员可执行此操作")
    return user


# -------------------- models --------------------
class LoginRequest(BaseModel):
    username: str
    password: str


class ChangePasswordRequest(BaseModel):
    username: Optional[str] = None
    old_password: str
    new_password: str = Field(min_length=4)


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=2, max_length=64)
    password: str = Field(min_length=4, max_length=128)
    is_admin: bool = False
    page_permissions: List[str] = Field(default_factory=list)
    project_ids: List[int] = Field(default_factory=list)
    task_type_ids: List[int] = Field(default_factory=list)


class UpdateUserPermissionsRequest(BaseModel):
    is_admin: bool = False
    page_permissions: List[str] = Field(default_factory=list)
    project_ids: List[int] = Field(default_factory=list)
    task_type_ids: List[int] = Field(default_factory=list)


class UpdateUserPasswordRequest(BaseModel):
    new_password: str = Field(min_length=4, max_length=128)


class UpdateAPIKeyRequest(BaseModel):
    api_key: str = Field(min_length=6)


class UpdateSystemConfigRequest(BaseModel):
    proxy_enabled: bool
    proxy_url: Optional[str] = None
    debug_enabled: bool
    log_to_file: bool
    stop_accepting_tasks: bool = False
    public_create_task_max_inflight: int = Field(default=180, ge=3)
    server_count: int = Field(default=1, ge=1)
    browser_open_concurrency: Optional[int] = Field(default=None, ge=1, le=50)
    browser_open_queue_timeout: Optional[float] = Field(default=None, ge=10, le=600)
    task_queue_max_size: Optional[int] = Field(default=None, ge=1, le=100000)
    task_queue_timeout_seconds: Optional[float] = Field(default=None, ge=10, le=3600)

    @field_validator("public_create_task_max_inflight")
    @classmethod
    def _validate_public_create_task_max_inflight(cls, v: int) -> int:
        vv = int(v or 0)
        if vv % 3 != 0:
            raise ValueError("并发上限必须是 3 的整数倍")
        return vv


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
    browser_pool_limit: int = Field(default=0, ge=0)


class UpdateBrowserRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    lan_addr: str = Field(min_length=3, max_length=255)
    vendor: str = Field(default="roxy", max_length=50)
    access_key: Optional[str] = Field(default=None, max_length=255)
    browser_pool_limit: int = Field(default=0, ge=0)


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
    project_id: Optional[int] = Field(default=None, ge=1)
    concurrency: int = Field(default=1, ge=1, le=999)
    continuous_error_threshold: int = Field(default=3, ge=1, le=999)
    continuous_error_close_window_threshold: int = Field(default=3, ge=1, le=999999)
    timeout_seconds: int = Field(default=1800, ge=10, le=24 * 3600)
    create_task_handler: Optional[str] = None
    refresh_quota_handler: Optional[str] = None
    error_retry_count: int = Field(default=0, ge=0, le=10)
    default_target_url: Optional[str] = Field(default=None, max_length=2048)


class UpdateTaskTypeRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    code: str = Field(min_length=2, max_length=64, pattern=r"^[a-zA-Z0-9_]+$")
    project_id: Optional[int] = Field(default=None, ge=1)
    concurrency: int = Field(default=1, ge=1, le=999)
    continuous_error_threshold: int = Field(default=3, ge=1, le=999)
    continuous_error_close_window_threshold: int = Field(default=3, ge=1, le=999999)
    timeout_seconds: int = Field(default=1800, ge=10, le=24 * 3600)
    create_task_handler: Optional[str] = None
    refresh_quota_handler: Optional[str] = None
    error_retry_count: int = Field(default=0, ge=0, le=10)
    default_target_url: Optional[str] = Field(default=None, max_length=2048)
    enabled: bool = True


class AddTaskTypeWindowsRequest(BaseModel):
    window_pks: List[int] = Field(min_length=1)
    daily_quota: int = Field(default=0, ge=0, le=100000)
    remaining_quota: int = Field(default=0, ge=0, le=100000)
    enabled: bool = True


class UpdateTaskTypeWindowRequest(BaseModel):
    enabled: Optional[bool] = None
    headless: Optional[bool] = None
    pure_mode: Optional[bool] = None
    deleted: Optional[bool] = None
    task_type_id: Optional[int] = Field(default=None, ge=1)
    daily_quota: Optional[int] = Field(default=None, ge=0, le=100000)
    remaining_quota: Optional[int] = Field(default=None, ge=0, le=100000)
    cooldown_until: Optional[str] = None  # ISO or empty
    error_cooldown_until: Optional[str] = None  # ISO or empty
    total_errors: Optional[int] = Field(default=None, ge=0, le=1000000)
    consecutive_errors: Optional[int] = Field(default=None, ge=0, le=1000000)


class UpsertSoraAccessTokenRequest(BaseModel):
    access_token: Optional[str] = None
    expires: Optional[str] = None


class UpdateWindowProxyRequest(BaseModel):
    """在指纹浏览器侧修改某个窗口的代理配置（通过本地已保存的 proxy_id 选择）。"""

    proxy_id: int = Field(ge=0, description="本地代理列表中的 proxy_id；0 表示不使用代理")


class UpdateWindowAccountRequest(BaseModel):
    """在指纹浏览器侧修改某个窗口的平台账号（通过本地 account_id 选择）。"""

    account_id: int = Field(ge=0, description="本地账号列表中的 account_id；0 表示清空窗口账号")


class SetPureModeRequest(BaseModel):
    """切换纯净模式：修改窗口的 platformUrl 和 openWorkbench。"""

    pure_mode: bool = Field(description="True=纯净模式（platformUrl 清空），False=恢复（platformUrl 设为 Google 登录页）")
    mapping_id: Optional[int] = Field(default=None, description="task_type_windows 映射 ID，用于持久化 pure_mode 状态")


class DeleteWindowRequest(BaseModel):
    """删除窗口请求。"""

    delete_remote: bool = Field(default=True, description="是否同时删除指纹浏览器远程窗口；False 则仅删除本地数据")


class MoveWindowRequest(BaseModel):
    """把本地窗口转移到另一个空间。"""

    target_space_pk: int = Field(ge=1, description="目标空间主键")


class UpdateWindowRemarkRequest(BaseModel):
    """仅更新本地窗口备注。"""

    window_remark: Optional[str] = Field(default="", description="窗口备注")


class ImportAccountsRequest(BaseModel):
    content: str = Field(min_length=1, description="批量导入文本")


class UpdateAccountRemarkRequest(BaseModel):
    platform_remarks: Optional[str] = Field(default="", description="本地备注")


class SyncAccountsRequest(BaseModel):
    keep_local_deleted: bool = Field(default=True, description="同步时保留本地已删除账号状态")


class UpdateProxyRemarkRequest(BaseModel):
    remark: Optional[str] = Field(default="", description="本地代理备注")


class SyncProxiesRequest(BaseModel):
    keep_local_deleted: bool = Field(default=True, description="同步时保留本地已删除代理状态")
    keep_local_remark: bool = Field(default=True, description="同步时保留本地代理备注")


class ImportCardKeysRequest(BaseModel):
    content: str = Field(min_length=1, description="每行一个卡密")


class UpdateCardKeyRequest(BaseModel):
    card_key: str = Field(min_length=1, max_length=256)


class BatchDeleteCardKeysRequest(BaseModel):
    ids: List[int] = Field(default_factory=list, description="要删除的卡密 ID 列表")


# -------------------- auth helper --------------------
def _parse_batch_account_lines(content: str) -> List[Dict[str, Any]]:
    """解析批量账号文本，支持多种分隔符格式：
    - 邮箱——密码——忽略——EFA（中文长横线）
    - 邮箱----密码----忽略----EFA----忽略（四连字符，取第1/2/4字段）
    """
    out: List[Dict[str, Any]] = []
    lines = [str(x or "").strip() for x in str(content or "").splitlines()]
    for ln in lines:
        if not ln:
            continue
        # 兼容：四连字符 / 中文长横线 / 双连字符 / 单长破折号 / 空格连字符
        parts = [x.strip() for x in re.split(r"(?:----|——|--|—| - )", ln) if str(x or "").strip()]
        if len(parts) < 4:
            continue
        email = str(parts[0] or "").strip()
        password = str(parts[1] or "").strip()
        efa = str(parts[3] or "").strip()
        if not email:
            continue
        out.append(
            {
                "platformUrl": "https://accounts.google.com/",
                "platformUserName": email,
                "platformPassword": password,
                "platformEfa": efa,
                "platformRemarks": "batch-import",
            }
        )
    return out


def _extract_accounts_from_batch_create_response(
    rsp: Dict[str, Any], requested_accounts: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """从 batch_create 返回体中提取可直接入库的账号数据（不再额外回查 account/list）。"""

    req_key_map: Dict[str, Dict[str, Any]] = {}
    for a in (requested_accounts or []):
        if not isinstance(a, dict):
            continue
        k = f"{str(a.get('platformUrl') or '').strip().lower()}||{str(a.get('platformUserName') or '').strip().lower()}"
        if k and k not in req_key_map:
            req_key_map[k] = a

    def _to_int(v: Any) -> Optional[int]:
        try:
            i = int(v)
            return i if i > 0 else None
        except Exception:
            return None

    extracted: List[Dict[str, Any]] = []
    seen_ids: set[int] = set()
    id_keys = {"ids", "accountids", "idlist"}
    id_pool: List[int] = []

    def walk(obj: Any):
        if isinstance(obj, dict):
            # 先尝试“完整账号对象”提取
            aid = _to_int(obj.get("id") if obj.get("id") is not None else (obj.get("accountId") if obj.get("accountId") is not None else obj.get("account_id")))
            if aid:
                url = obj.get("platformUrl") if obj.get("platformUrl") is not None else obj.get("platform_url")
                user = obj.get("platformUserName") if obj.get("platformUserName") is not None else obj.get("platform_username")
                pwd = obj.get("platformPassword") if obj.get("platformPassword") is not None else obj.get("platform_password")
                efa = obj.get("platformEfa") if obj.get("platformEfa") is not None else obj.get("platform_efa")
                remarks = obj.get("platformRemarks") if obj.get("platformRemarks") is not None else obj.get("platform_remarks")
                key = f"{str(url or '').strip().lower()}||{str(user or '').strip().lower()}"
                base = req_key_map.get(key) or {}
                if aid not in seen_ids:
                    extracted.append(
                        {
                            "id": int(aid),
                            "platformUrl": str(url).strip() if url is not None else (str(base.get("platformUrl") or "").strip() or None),
                            "platformUserName": str(user).strip() if user is not None else (str(base.get("platformUserName") or "").strip() or None),
                            "platformPassword": str(pwd).strip() if pwd is not None else (str(base.get("platformPassword") or "").strip() or None),
                            "platformEfa": str(efa).strip() if efa is not None else (str(base.get("platformEfa") or "").strip() or None),
                            "platformRemarks": str(remarks).strip() if remarks is not None else (str(base.get("platformRemarks") or "").strip() or None),
                        }
                    )
                    seen_ids.add(aid)

            # 再提取“仅 ID 列表”
            for k, v in obj.items():
                kl = str(k or "").strip().lower()
                if kl in id_keys and isinstance(v, list):
                    for item in v:
                        iid = _to_int(item)
                        if iid and iid not in seen_ids and iid not in id_pool:
                            id_pool.append(iid)
                walk(v)
            return

        if isinstance(obj, list):
            for it in obj:
                walk(it)

    walk(rsp or {})

    # 如果返回体只给了 ids，则按请求顺序与 ids 对齐回写本地
    if (not extracted) and id_pool:
        for idx, aid in enumerate(id_pool):
            if idx >= len(requested_accounts):
                break
            a = requested_accounts[idx] if isinstance(requested_accounts[idx], dict) else {}
            extracted.append(
                {
                    "id": int(aid),
                    "platformUrl": str(a.get("platformUrl") or "").strip() or None,
                    "platformUserName": str(a.get("platformUserName") or "").strip() or None,
                    "platformPassword": str(a.get("platformPassword") or "").strip() or None,
                    "platformEfa": str(a.get("platformEfa") or "").strip() or None,
                    "platformRemarks": str(a.get("platformRemarks") or "").strip() or None,
                }
            )

    return extracted


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
    return {
        "success": True,
        "token": session_token,
        "username": user.username,
        "is_admin": bool(getattr(user, "is_admin", False)),
    }


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


@router.get("/api/admin/me")
async def admin_me(token: str = Depends(verify_admin_token)):
    user = await _get_user_by_token(token)
    page_permissions = sorted(list(await _get_allowed_page_keys(user)))
    project_ids = await _get_allowed_project_ids(user)
    task_type_ids = await _get_allowed_task_type_ids(user)
    return {
        "success": True,
        "user": {
            "id": user.id,
            "username": user.username,
            "is_admin": bool(getattr(user, "is_admin", False)),
            "page_permissions": page_permissions,
            "project_ids": [] if project_ids is None else project_ids,
            "task_type_ids": [] if task_type_ids is None else task_type_ids,
            "all_projects": project_ids is None,
            "all_task_types": task_type_ids is None,
        },
        "page_options": sorted(list(PAGE_KEYS)),
    }


@router.get("/api/admin/users")
async def list_users(token: str = Depends(verify_admin_token)):
    await _ensure_page_access(token, "users")
    await _ensure_admin_user(token)
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    users = await db.list_admin_users()
    out: List[Dict[str, Any]] = []
    for u in users:
        page_keys = await db.get_user_page_permissions(int(u.id or 0))
        project_ids = await db.get_user_project_permissions(int(u.id or 0))
        task_type_ids = await db.get_user_task_type_permissions(int(u.id or 0))
        out.append(
            {
                "id": u.id,
                "username": u.username,
                "is_admin": bool(getattr(u, "is_admin", False)),
                # 密码不明文展示；保留固定占位，避免泄露 hash
                "password_display": "******",
                "page_permissions": page_keys,
                "project_ids": project_ids,
                "task_type_ids": task_type_ids,
                "all_pages": len(page_keys) == 0 or bool(getattr(u, "is_admin", False)),
                "all_projects": len(project_ids) == 0 or bool(getattr(u, "is_admin", False)),
                "all_task_types": len(task_type_ids) == 0 or bool(getattr(u, "is_admin", False)),
            }
        )
    return {"success": True, "items": out, "page_options": sorted(list(PAGE_KEYS))}


@router.post("/api/admin/users")
async def create_user(req: CreateUserRequest, token: str = Depends(verify_admin_token)):
    await _ensure_admin_user(token)
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    username = req.username.strip()
    if not username:
        raise HTTPException(status_code=400, detail="用户名不能为空")
    if await db.get_admin_user(username):
        raise HTTPException(status_code=400, detail="用户名已存在")

    invalid_pages = [x for x in (req.page_permissions or []) if x not in PAGE_KEYS]
    if invalid_pages:
        raise HTTPException(status_code=400, detail=f"无效页面权限: {', '.join(invalid_pages)}")

    uid = await db.create_admin_user(
        username=username,
        password_hash=AuthManager.hash_password(req.password),
        is_admin=bool(req.is_admin),
    )
    await db.set_user_page_permissions(uid, list(req.page_permissions or []))
    await db.set_user_project_permissions(uid, [int(x) for x in (req.project_ids or [])])
    await db.set_user_task_type_permissions(uid, [int(x) for x in (req.task_type_ids or [])])
    return {"success": True, "user_id": uid}


@router.put("/api/admin/users/{user_id}/password")
async def update_user_password(user_id: int, req: UpdateUserPasswordRequest, token: str = Depends(verify_admin_token)):
    await _ensure_admin_user(token)
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    user = await db.get_admin_user_by_id(int(user_id))
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")
    await db.update_admin_password_by_id(int(user_id), AuthManager.hash_password(req.new_password))
    return {"success": True}


@router.put("/api/admin/users/{user_id}/permissions")
async def update_user_permissions(user_id: int, req: UpdateUserPermissionsRequest, token: str = Depends(verify_admin_token)):
    current = await _ensure_admin_user(token)
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    target = await db.get_admin_user_by_id(int(user_id))
    if not target:
        raise HTTPException(status_code=404, detail="用户不存在")

    # 避免把自己降权导致系统无管理员可维护
    if int(current.id or 0) == int(user_id) and not bool(req.is_admin):
        raise HTTPException(status_code=400, detail="不能取消当前登录管理员的管理员权限")

    invalid_pages = [x for x in (req.page_permissions or []) if x not in PAGE_KEYS]
    if invalid_pages:
        raise HTTPException(status_code=400, detail=f"无效页面权限: {', '.join(invalid_pages)}")

    await db.update_admin_user_role(int(user_id), bool(req.is_admin))
    await db.set_user_page_permissions(int(user_id), list(req.page_permissions or []))
    await db.set_user_project_permissions(int(user_id), [int(x) for x in (req.project_ids or [])])
    await db.set_user_task_type_permissions(int(user_id), [int(x) for x in (req.task_type_ids or [])])
    return {"success": True}


# -------------------- system config --------------------
@router.get("/api/admin/system-config")
async def get_system_config(token: str = Depends(verify_admin_token)):
    await _ensure_any_page_access(token, {"system", "test"})
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
            "public_create_task_max_inflight": normalize_public_create_task_max_inflight(
                getattr(syscfg, "public_create_task_max_inflight", None)
            ),
            "server_count": max(1, int(getattr(syscfg, "server_count", 1) or 1)),
            "browser_open_concurrency": int(getattr(syscfg, "browser_open_concurrency", 3) or 3),
            "browser_open_queue_timeout": float(getattr(syscfg, "browser_open_queue_timeout", 120.0) or 120.0),
            "task_queue_max_size": int(getattr(syscfg, "task_queue_max_size", 1000) or 1000),
            "task_queue_timeout_seconds": float(getattr(syscfg, "task_queue_timeout_seconds", 300.0) or 300.0),
            "admin_username": admin_user.username if admin_user else "admin",
        },
    }


@router.get("/api/admin/ui-defaults")
async def get_ui_defaults(token: str = Depends(verify_admin_token)):
    await _ensure_page_access(token, "task_types")
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
    await _ensure_page_access(token, "system")
    await _ensure_admin_user(token)
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    proxy_url = (req.proxy_url or "").strip() or None
    await db.update_system_config(
        proxy_enabled=req.proxy_enabled,
        proxy_url=proxy_url,
        debug_enabled=req.debug_enabled,
        log_to_file=req.log_to_file,
        stop_accepting_tasks=req.stop_accepting_tasks,
        public_create_task_max_inflight=req.public_create_task_max_inflight,
        server_count=req.server_count,
        browser_open_concurrency=req.browser_open_concurrency,
        browser_open_queue_timeout=req.browser_open_queue_timeout,
        task_queue_max_size=req.task_queue_max_size,
        task_queue_timeout_seconds=req.task_queue_timeout_seconds,
    )
    await db.reload_config_to_memory()
    setup_logging()

    return {"success": True, "message": "系统配置已更新"}


@router.get("/api/admin/queue-info")
async def get_queue_info(token: str = Depends(verify_admin_token)):
    await _ensure_any_page_access(token, {"system", "tasks"})
    from ..api.routes import task_service as _ts
    if not _ts:
        return {"success": True, "queue": {}}
    return {"success": True, "queue": await _ts.get_queue_info()}


@router.post("/api/admin/api-key")
async def update_api_key(req: UpdateAPIKeyRequest, token: str = Depends(verify_admin_token)):
    await _ensure_page_access(token, "system")
    await _ensure_admin_user(token)
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.update_system_config(api_key=req.api_key.strip())
    await db.reload_config_to_memory()
    return {"success": True, "message": "API Key 已更新"}


# -------------------- project management --------------------
@router.get("/api/admin/projects")
async def list_projects(token: str = Depends(verify_admin_token)):
    user = await _ensure_page_access(token, "projects")
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    allowed_ids = await _get_allowed_project_ids(user)
    return {"success": True, "projects": [p.model_dump() for p in await db.list_projects(allowed_project_ids=allowed_ids)]}


@router.post("/api/admin/projects")
async def create_project(req: CreateProjectRequest, token: str = Depends(verify_admin_token)):
    await _ensure_page_access(token, "projects")
    await _ensure_admin_user(token)
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    pid = await db.create_project(req.name)
    return {"success": True, "project_id": pid}


@router.put("/api/admin/projects/{project_id}")
async def update_project(project_id: int, req: UpdateProjectRequest, token: str = Depends(verify_admin_token)):
    await _ensure_page_access(token, "projects")
    await _ensure_admin_user(token)
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.update_project(project_id, req.name)
    return {"success": True}


@router.delete("/api/admin/projects/{project_id}")
async def delete_project(project_id: int, token: str = Depends(verify_admin_token)):
    await _ensure_page_access(token, "projects")
    await _ensure_admin_user(token)
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
    bid = await db.create_browser(req.project_id, req.name, req.lan_addr, req.vendor, req.access_key, req.browser_pool_limit)
    return {"success": True, "browser_id": bid}


@router.put("/api/admin/browsers/{browser_id}")
async def update_browser(browser_id: int, req: UpdateBrowserRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.update_browser(browser_id, req.name, req.lan_addr, req.vendor, req.access_key, req.browser_pool_limit)
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


@router.get("/api/admin/spaces/{space_pk}/proxies")
async def list_local_proxies(
    space_pk: int,
    include_deleted: bool = False,
    token: str = Depends(verify_admin_token),
):
    """查看本地 DB 保存的代理列表（按空间/工作空间维度）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {
        "success": True,
        "proxies": [p.model_dump(exclude={"raw"}) for p in await db.list_proxies(space_pk, include_deleted=include_deleted)],
    }


@router.get("/api/admin/proxies")
async def list_all_proxies(
    include_deleted: bool = False,
    token: str = Depends(verify_admin_token),
):
    """返回所有空间的代理列表（按 proxy_id 去重），代理是跨空间共用的。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {
        "success": True,
        "proxies": [p.model_dump(exclude={"raw"}) for p in await db.list_all_proxies(include_deleted=include_deleted)],
    }


@router.get("/api/admin/spaces/{space_pk}/proxy-bindings")
async def list_proxy_bindings(space_pk: int, token: str = Depends(verify_admin_token)):
    """返回代理绑定数：proxy_id -> 被多少个本地未删除窗口绑定。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "counts": await db.count_proxy_bindings(space_pk)}


@router.get("/api/admin/spaces/{space_pk}/proxy-success-counts")
async def list_proxy_success_counts(space_pk: int, token: str = Depends(verify_admin_token)):
    """返回代理成功任务数：proxy_id -> tasks 表中该 IP 的 completed 数。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "counts": await db.count_proxy_success_tasks(space_pk)}


@router.get("/api/admin/spaces/{space_pk}/proxy-task-stats")
async def list_proxy_task_stats(space_pk: int, token: str = Depends(verify_admin_token)):
    """返回代理任务统计：proxy_id -> {completed, failed}（按 tasks.window_ip 聚合）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "stats": await db.count_proxy_task_stats(space_pk)}


@router.post("/api/admin/spaces/{space_pk}/proxies/{proxy_id}/delete")
async def delete_local_proxy(space_pk: int, proxy_id: int, token: str = Depends(verify_admin_token)):
    """本地删除代理（仅标记 deleted=1，不影响指纹浏览器侧）。

    说明：
    - 后续“同步该空间代理到本地”默认会保留本地删除标记，不会自动恢复。
    """
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    affected = await db.delete_proxy(space_pk=int(space_pk), proxy_id=int(proxy_id))
    if affected <= 0:
        raise HTTPException(status_code=404, detail="proxy not found")
    return {"success": True, "message": "已在本地删除该代理", "affected": affected}


@router.post("/api/admin/spaces/{space_pk}/proxies/{proxy_id}/remark")
async def update_local_proxy_remark(
    space_pk: int,
    proxy_id: int,
    req: UpdateProxyRemarkRequest,
    token: str = Depends(verify_admin_token),
):
    """仅更新本地代理备注，不影响指纹浏览器侧。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    affected = await db.update_proxy_remark(space_pk=int(space_pk), proxy_id=int(proxy_id), remark=str(req.remark or "").strip())
    if affected <= 0:
        raise HTTPException(status_code=404, detail="proxy not found")
    return {"success": True, "message": "代理备注已更新", "affected": affected}


@router.post("/api/admin/spaces/{space_pk}/proxies/{proxy_id}/analyze-ip")
async def analyze_local_proxy_ip(space_pk: int, proxy_id: int, token: str = Depends(verify_admin_token)):
    """检测代理 IP 风险并回写到本地 proxies 表。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    proxy = await db.get_proxy(space_pk=int(space_pk), proxy_id=int(proxy_id))
    if not proxy:
        raise HTTPException(status_code=404, detail="proxy not found")

    def _pick_ip(*candidates: Optional[str]) -> Optional[str]:
        for c in candidates:
            v = str(c or "").strip()
            if not v:
                continue
            try:
                ipaddress.ip_address(v)
                return v
            except Exception:
                continue
        return None

    ip = _pick_ip(proxy.last_ip, proxy.host)
    if not ip:
        raise HTTPException(status_code=400, detail="该代理缺少可检测的 IP（last_ip/host）")

    syscfg = await db.get_system_config()
    url = "https://getgpt.pro/api/analyze-ip"
    timeout = httpx.Timeout(connect=10.0, read=20.0, write=20.0, pool=10.0)
    proxy_url = str(syscfg.proxy_url or "").strip()
    use_proxy = bool(syscfg.proxy_enabled) and bool(proxy_url)

    def _build_client_kwargs(enable_proxy: bool) -> Dict[str, Any]:
        # 禁用环境变量代理，避免其他机器上 HTTP(S)_PROXY 导致意外失败。
        kwargs: Dict[str, Any] = {"timeout": timeout, "trust_env": False}
        if enable_proxy:
            try:
                kwargs["proxy"] = proxy_url
            except TypeError:
                kwargs["proxies"] = proxy_url
        return kwargs

    try:
        async with httpx.AsyncClient(**_build_client_kwargs(use_proxy)) as client:
            rsp = await client.get(url, params={"ip": ip})
    except Exception as first_err:
        if use_proxy:
            # 配了代理但请求失败时，回退直连重试一次。
            try:
                async with httpx.AsyncClient(**_build_client_kwargs(False)) as client:
                    rsp = await client.get(url, params={"ip": ip})
            except Exception as second_err:
                raise HTTPException(
                    status_code=502,
                    detail=(
                        "调用 IP 检测接口失败（代理+直连均失败）："
                        f"proxy_err={first_err.__class__.__name__}: {first_err}; "
                        f"direct_err={second_err.__class__.__name__}: {second_err}"
                    ),
                )
        else:
            raise HTTPException(
                status_code=502,
                detail=f"调用 IP 检测接口失败（直连）：{first_err.__class__.__name__}: {first_err}",
            )

    try:
        payload = rsp.json()
    except Exception:
        payload = None
    if rsp.status_code >= 400 or not isinstance(payload, dict):
        body_preview = ""
        try:
            body_preview = (rsp.text or "")[:200]
        except Exception:
            body_preview = ""
        raise HTTPException(
            status_code=502,
            detail=f"IP 检测接口返回异常（status={rsp.status_code}, body={body_preview}）",
        )

    if not payload.get("success"):
        raise HTTPException(status_code=400, detail=str(payload.get("message") or "IP 检测失败"))

    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    risk_level = str(data.get("riskLevel") or "").strip() or None
    asn_type = str(data.get("asnType") or "").strip() or None

    await db.update_proxy_ip_profile(
        space_pk=int(space_pk),
        proxy_id=int(proxy_id),
        risk_level=risk_level,
        asn_type=asn_type,
    )

    return {
        "success": True,
        "space_pk": int(space_pk),
        "proxy_id": int(proxy_id),
        "ip": ip,
        "risk_level": risk_level,
        "asn_type": asn_type,
        "data": data,
    }


@router.post("/api/admin/spaces/{space_pk}/sync-proxies")
async def sync_space_proxies(
    space_pk: int,
    req: Optional[SyncProxiesRequest] = None,
    token: str = Depends(verify_admin_token),
):
    """同步某个空间的代理列表（从指纹浏览器拉取后写入本地 DB）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")
    browser = await db.get_browser(space.browser_id)
    if not browser:
        raise HTTPException(status_code=404, detail="browser not found")

    syscfg = await db.get_system_config()
    client = FPBrowserClient(proxy_enabled=syscfg.proxy_enabled, proxy_url=syscfg.proxy_url)
    try:
        proxies = await client.list_proxies(
            vendor=browser.vendor,
            base_url=browser.lan_addr,
            access_key=browser.access_key,
            space_id=space.space_id,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

    keep_local_deleted = True if req is None else bool(req.keep_local_deleted)
    keep_local_remark = True if req is None else bool(req.keep_local_remark)
    affected = await db.upsert_proxies(
        space_pk=space_pk,
        proxies=proxies,
        restore_deleted=(not keep_local_deleted),
        overwrite_remark=(not keep_local_remark),
    )
    return {"success": True, "message": f"同步完成，写入/更新 {affected} 条代理记录", "affected": affected}


@router.get("/api/admin/accounts")
async def list_all_accounts_global(
    include_deleted: bool = True,
    token: str = Depends(verify_admin_token),
):
    """返回所有空间的平台账号列表（按 account_id 去重），账号是跨空间共用的。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    rows = [a.model_dump(exclude={"raw"}) for a in await db.list_all_platform_accounts(include_deleted=include_deleted)]
    bindings = await db.list_account_bindings()
    merged: List[Dict[str, Any]] = []
    for x in rows:
        aid = int(x.get("account_id") or 0)
        b = bindings.get(aid) or {}
        merged.append(
            {
                **x,
                "binding_count": int(b.get("count") or 0),
                "binding_windows": str(b.get("windows") or "").strip(),
            }
        )
    return {"success": True, "accounts": merged}


@router.get("/api/admin/spaces/{space_pk}/accounts")
async def list_local_accounts(space_pk: int, token: str = Depends(verify_admin_token)):
    """查看本地 DB 保存的平台账号列表（按空间维度）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    rows = [a.model_dump(exclude={"raw"}) for a in await db.list_platform_accounts(space_pk, include_deleted=True)]
    bindings = await db.list_account_bindings(space_pk)
    merged: List[Dict[str, Any]] = []
    for x in rows:
        aid = int(x.get("account_id") or 0)
        b = bindings.get(aid) or {}
        merged.append(
            {
                **x,
                "binding_count": int(b.get("count") or 0),
                "binding_windows": str(b.get("windows") or "").strip(),
            }
        )
    return {"success": True, "accounts": merged}


@router.post("/api/admin/spaces/{space_pk}/sync-accounts")
async def sync_space_accounts(
    space_pk: int,
    req: Optional[SyncAccountsRequest] = None,
    token: str = Depends(verify_admin_token),
):
    """同步某个空间的平台账号列表（从指纹浏览器拉取后写入本地 DB）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")
    browser = await db.get_browser(space.browser_id)
    if not browser:
        raise HTTPException(status_code=404, detail="browser not found")

    syscfg = await db.get_system_config()
    client = FPBrowserClient(proxy_enabled=syscfg.proxy_enabled, proxy_url=syscfg.proxy_url)
    try:
        accounts = await client.list_accounts(
            vendor=browser.vendor,
            base_url=browser.lan_addr,
            access_key=browser.access_key,
            space_id=space.space_id,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

    keep_local_deleted = True if req is None else bool(req.keep_local_deleted)
    affected = await db.upsert_platform_accounts(
        space_pk=space_pk,
        accounts=accounts,
        restore_deleted=(not keep_local_deleted),
    )
    return {"success": True, "message": f"同步完成，写入/更新 {affected} 条账号记录", "affected": affected}


@router.post("/api/admin/spaces/{space_pk}/accounts/import")
async def import_space_accounts(space_pk: int, req: ImportAccountsRequest, token: str = Depends(verify_admin_token)):
    """批量导入账号到指纹浏览器，并回写本地 DB。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")
    browser = await db.get_browser(space.browser_id)
    if not browser:
        raise HTTPException(status_code=404, detail="browser not found")

    parsed = _parse_batch_account_lines(req.content)
    if not parsed:
        raise HTTPException(status_code=400, detail="未解析到有效账号，请检查格式：邮箱——密码——忽略——EFA 或 邮箱----密码----忽略----EFA----忽略")

    # 避免覆盖/重复：按 (platformUrl, platformUserName) 去重
    dedup_map: Dict[str, Dict[str, Any]] = {}
    for a in parsed:
        k = f"{str(a.get('platformUrl') or '').strip().lower()}||{str(a.get('platformUserName') or '').strip().lower()}"
        if k and k not in dedup_map:
            dedup_map[k] = a
    parsed_unique = list(dedup_map.values())

    local_accounts = await db.list_platform_accounts(space_pk)
    existing_keys = {
        f"{str(x.platform_url or '').strip().lower()}||{str(x.platform_username or '').strip().lower()}"
        for x in local_accounts
    }
    to_create = []
    for a in parsed_unique:
        k = f"{str(a.get('platformUrl') or '').strip().lower()}||{str(a.get('platformUserName') or '').strip().lower()}"
        if k in existing_keys:
            continue
        to_create.append(a)

    if to_create:
        syscfg = await db.get_system_config()
        client = FPBrowserClient(proxy_enabled=syscfg.proxy_enabled, proxy_url=syscfg.proxy_url)
        try:
            rsp = await client.create_accounts_batch(
                vendor=browser.vendor,
                base_url=browser.lan_addr,
                access_key=browser.access_key,
                space_id=space.space_id,
                account_list=to_create,
            )
        except RuntimeError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if int((rsp or {}).get("code") or -1) != 0:
            raise HTTPException(status_code=400, detail=str((rsp or {}).get("msg") or "批量创建账号失败"))

    # 不再额外回查 account/list，直接使用 batch_create 返回结果落本地。
    affected = 0
    found_count = 0
    if to_create:
        created_rows = _extract_accounts_from_batch_create_response(rsp or {}, to_create)
        found_count = len(created_rows or [])
        # 仅增量写入，不做 full_replace（避免清空本地其他账号）
        affected = await db.upsert_platform_accounts(space_pk=space_pk, accounts=created_rows or [], full_replace=False)

    missing_count = max(0, len(to_create) - int(found_count or 0))
    tip = f"，本地写入 {found_count} 条"
    if missing_count > 0:
        tip += f"（{missing_count} 条未从创建响应中拿到ID，可稍后手动同步补齐）"
    return {
        "success": True,
        "message": f"导入完成：新增 {len(to_create)} 条，跳过 {len(parsed_unique) - len(to_create)} 条（重复账号）{tip}",
        "parsed": len(parsed_unique),
        "created": len(to_create),
        "skipped": len(parsed_unique) - len(to_create),
        "synced": int(affected or 0),
        "found": int(found_count or 0),
    }


@router.post("/api/admin/spaces/{space_pk}/accounts/{account_id}/delete")
async def delete_space_account(space_pk: int, account_id: int, token: str = Depends(verify_admin_token)):
    """删除平台账号（远端 + 本地）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    if int(account_id) <= 0:
        raise HTTPException(status_code=400, detail="invalid account_id")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")
    browser = await db.get_browser(space.browser_id)
    if not browser:
        raise HTTPException(status_code=404, detail="browser not found")

    syscfg = await db.get_system_config()
    client = FPBrowserClient(proxy_enabled=syscfg.proxy_enabled, proxy_url=syscfg.proxy_url)
    remote_err = ""
    rsp: Dict[str, Any] = {}
    try:
        rsp = await client.delete_accounts(
            vendor=browser.vendor,
            base_url=browser.lan_addr,
            access_key=browser.access_key,
            space_id=space.space_id,
            account_ids=[int(account_id)],
        )
    except RuntimeError as e:
        remote_err = str(e)
    if not remote_err and int((rsp or {}).get("code") or -1) != 0:
        remote_err = str((rsp or {}).get("msg") or "删除账号失败")

    await db.delete_platform_account(space_pk=space_pk, account_id=int(account_id))
    await db.clear_window_platform_binding_by_account(space_pk=space_pk, account_id=int(account_id))
    if remote_err:
        return {
            "success": True,
            "message": f"本地账号已删除；远端删除失败：{remote_err}",
            "remote_deleted": False,
        }
    return {"success": True, "message": "账号已删除并同步到指纹浏览器", "remote_deleted": True}


@router.post("/api/admin/spaces/{space_pk}/accounts/{account_id}/remark")
async def update_space_account_remark(
    space_pk: int,
    account_id: int,
    req: UpdateAccountRemarkRequest,
    token: str = Depends(verify_admin_token),
):
    """仅更新本地账号备注，不同步到指纹浏览器。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    if int(account_id) <= 0:
        raise HTTPException(status_code=400, detail="invalid account_id")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")

    row = await db.get_platform_account(space_pk=space_pk, account_id=int(account_id))
    if not row:
        raise HTTPException(status_code=404, detail="account not found")

    remark = str(req.platform_remarks or "").strip()
    affected = await db.update_platform_account_remark(
        space_pk=space_pk,
        account_id=int(account_id),
        remark=remark,
    )
    if affected <= 0:
        raise HTTPException(status_code=404, detail="account not found")
    return {"success": True, "message": "本地备注已更新", "affected": int(affected)}


@router.post("/api/admin/spaces/{space_pk}/windows/{window_key}/set-account")
async def set_window_account(
    space_pk: int,
    window_key: str,
    req: UpdateWindowAccountRequest,
    token: str = Depends(verify_admin_token),
):
    """将窗口账号改为本地账号列表中的某个 account_id（实际调用 /browser/mdf）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")
    browser = await db.get_browser(space.browser_id)
    if not browser:
        raise HTTPException(status_code=404, detail="browser not found")

    wk = str(window_key or "").strip()
    if not wk:
        raise HTTPException(status_code=400, detail="window_key is required")

    account_id = int(req.account_id or 0)
    selected = None
    if account_id > 0:
        selected = await db.get_platform_account(space_pk=space_pk, account_id=account_id)
        if not selected:
            raise HTTPException(status_code=404, detail="account not found")

    # 从本地 DB 读取窗口当前 proxy_id，直接构造 proxyInfo（与 set-proxy 一致），
    # 避免从 detail 重建 proxyInfo 时因字段缺失/异常而丢失代理设置。
    target_win = await db.get_window_by_key(space_pk=space_pk, window_key=wk)
    local_proxy_id = int(getattr(target_win, "proxy_id", 0) or 0) if target_win else 0

    syscfg = await db.get_system_config()
    client = FPBrowserClient(proxy_enabled=syscfg.proxy_enabled, proxy_url=syscfg.proxy_url)
    try:
        if local_proxy_id > 0:
            local_proxy_info: Dict[str, Any] = {"moduleId": local_proxy_id, "proxyMethod": "choose"}
        else:
            local_proxy_info = {"moduleId": 0, "proxyMethod": "custom", "proxyCategory": "noproxy"}

        mdf_payload: Dict[str, Any] = {"proxyInfo": local_proxy_info}
        if selected and account_id > 0:
            mdf_payload["windowPlatformList"] = [
                {
                    "id": int(selected.account_id),
                    "platformUrl": str(selected.platform_url or "").strip(),
                    "platformUserName": str(selected.platform_username or "").strip(),
                    "platformPassword": str(selected.platform_password or "").strip(),
                    "platformEfa": str(selected.platform_efa or "").strip(),
                    "platformRemarks": str(selected.platform_remarks or "").strip(),
                }
            ]
        else:
            mdf_payload["windowPlatformList"] = []

        rsp = await client.browser_mdf(
            vendor=browser.vendor,
            base_url=browser.lan_addr,
            access_key=browser.access_key,
            space_id=space.space_id,
            window_key=wk,
            data=mdf_payload,
        )
    except RuntimeError as e:
        pass

    if int((rsp or {}).get("code") or -1) != 0:
        pass

    await db.update_window_platform_binding(
        space_pk=space_pk,
        window_key=wk,
        platform_account_id=(int(selected.account_id) if selected else None),
        platform_account=(selected.platform_username if selected else None),
        platform_url=(selected.platform_url if selected else None),
    )
    return {"success": True, "message": "已提交修改窗口账号请求", "roxy_response": rsp}


@router.post("/api/admin/spaces/{space_pk}/windows/{window_key}/set-proxy")
async def set_window_proxy(space_pk: int, window_key: str, req: UpdateWindowProxyRequest, token: str = Depends(verify_admin_token)):
    """将某个窗口的代理配置改为本地代理列表中的某个 proxy_id（实际调用 /browser/mdf）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")
    browser = await db.get_browser(space.browser_id)
    if not browser:
        raise HTTPException(status_code=404, detail="browser not found")

    wk = str(window_key or "").strip()
    if not wk:
        raise HTTPException(status_code=400, detail="window_key is required")

    # 代理配置：
    # - proxy_id=0：不使用代理（按 Roxy 约定 moduleId=0 + noproxy）
    # - proxy_id>0：优先使用 choose + moduleId（由指纹浏览器从代理库绑定）
    proxy_id = int(req.proxy_id or 0)
    if proxy_id <= 0:
        proxy_info = {"moduleId": 0, "proxyMethod": "custom", "proxyCategory": "noproxy"}
    else:
        proxy_info = {"moduleId": proxy_id, "proxyMethod": "choose"}

    # 从本地 DB 读取窗口当前绑定的账号，直接构造 windowPlatformList，
    # 避免额外调用 get_browser_detail（省掉一次 Roxy API 往返）。
    target_win = await db.get_window_by_key(space_pk=space_pk, window_key=wk)
    local_account_id = int(getattr(target_win, "platform_account_id", 0) or 0) if target_win else 0
    wpl: list = []
    if local_account_id > 0:
        acct = await db.get_platform_account(space_pk=space_pk, account_id=local_account_id)
        if acct:
            wpl = [{
                "id": int(acct.account_id),
                "platformUrl": str(acct.platform_url or "").strip(),
                "platformUserName": str(acct.platform_username or "").strip(),
                "platformPassword": str(acct.platform_password or "").strip(),
                "platformEfa": str(acct.platform_efa or "").strip(),
                "platformRemarks": str(acct.platform_remarks or "").strip(),
            }]

    syscfg = await db.get_system_config()
    client = FPBrowserClient(proxy_enabled=syscfg.proxy_enabled, proxy_url=syscfg.proxy_url)
    try:
        mdf_payload: Dict[str, Any] = {"proxyInfo": proxy_info}
        if wpl:
            mdf_payload["windowPlatformList"] = wpl

        rsp = await client.browser_mdf(
            vendor=browser.vendor,
            base_url=browser.lan_addr,
            access_key=browser.access_key,
            space_id=space.space_id,
            window_key=wk,
            data=mdf_payload,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 同步更新本地 DB，确保 UI “当前代理/绑定数” 立即生效
    try:
        await db.update_window_proxy_id(space_pk=space_pk, window_key=wk, proxy_id=proxy_id)
    except Exception:
        pass

    return {"success": True, "message": "已提交修改窗口代理请求", "roxy_response": rsp}


@router.post("/api/admin/spaces/{space_pk}/windows/{window_key}/set-pure-mode")
async def set_window_pure_mode(
    space_pk: int,
    window_key: str,
    req: SetPureModeRequest,
    token: str = Depends(verify_admin_token),
):
    """切换纯净模式：设置窗口的 platformUrl 和 openWorkbench（实际调用 /browser/mdf）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")
    browser = await db.get_browser(space.browser_id)
    if not browser:
        raise HTTPException(status_code=404, detail="browser not found")

    wk = str(window_key or "").strip()
    if not wk:
        raise HTTPException(status_code=400, detail="window_key is required")

    target_win = await db.get_window_by_key(space_pk=space_pk, window_key=wk)

    # 构造 proxyInfo
    local_proxy_id = int(getattr(target_win, "proxy_id", 0) or 0) if target_win else 0
    if local_proxy_id > 0:
        proxy_info: Dict[str, Any] = {"moduleId": local_proxy_id, "proxyMethod": "choose"}
    else:
        proxy_info = {"moduleId": 0, "proxyMethod": "custom", "proxyCategory": "noproxy"}

    # 构造 windowPlatformList
    local_account_id = int(getattr(target_win, "platform_account_id", 0) or 0) if target_win else 0
    wpl: list = []
    acct = None
    if local_account_id > 0:
        acct = await db.get_platform_account(space_pk=space_pk, account_id=local_account_id)
    elif target_win:
        pa = str(getattr(target_win, "platform_account", "") or "").strip()
        if pa:
            acct = await db.get_platform_account_by_username(space_pk=space_pk, username=pa)
    if acct and not req.pure_mode:
        wpl = [{
            "id": int(acct.account_id),
            "platformUrl": "https://accounts.google.com/",
            "platformUserName": str(acct.platform_username or "").strip(),
            "platformPassword": str(acct.platform_password or "").strip(),
            "platformEfa": str(acct.platform_efa or "").strip(),
            "platformRemarks": str(acct.platform_remarks or "").strip(),
        }]
    openWorkbench = 0 if req.pure_mode else 1
    mdf_payload: Dict[str, Any] = {
        "proxyInfo": proxy_info,
        "fingerInfo": {"openWorkbench": openWorkbench},
    }
    if wpl:
        mdf_payload["windowPlatformList"] = wpl

    syscfg = await db.get_system_config()
    client = FPBrowserClient(proxy_enabled=syscfg.proxy_enabled, proxy_url=syscfg.proxy_url)
    try:
        rsp = await client.browser_mdf(
            vendor=browser.vendor,
            base_url=browser.lan_addr,
            access_key=browser.access_key,
            space_id=space.space_id,
            window_key=wk,
            data=mdf_payload,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if req.mapping_id and req.mapping_id > 0:
        try:
            await db.update_task_type_window(mapping_id=req.mapping_id, pure_mode=req.pure_mode)
        except Exception:
            pass

    return {"success": True, "message": f"已{'开启' if req.pure_mode else '关闭'}纯净模式", "roxy_response": rsp}


@router.post("/api/admin/spaces/{space_pk}/windows/{window_key}/sync-proxy-addr")
async def sync_window_proxy_addr_local(space_pk: int, window_key: str, token: str = Depends(verify_admin_token)):
    """按窗口当前绑定的 proxy_id 回填本地 proxy_addr/proxy_country。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")

    wk = str(window_key or "").strip()
    if not wk:
        raise HTTPException(status_code=400, detail="window_key is required")

    windows = await db.list_windows(space_pk)
    target = next((w for w in (windows or []) if str(getattr(w, "window_key", "") or "").strip() == wk), None)
    if not target:
        raise HTTPException(status_code=404, detail="window not found")

    proxy_id = int(getattr(target, "proxy_id", 0) or 0)
    affected = await db.update_window_proxy_id(space_pk=int(space_pk), window_key=wk, proxy_id=proxy_id)
    if affected <= 0:
        raise HTTPException(status_code=404, detail="window not found or already deleted")

    updated_windows = await db.list_windows(space_pk)
    updated = next((w for w in (updated_windows or []) if str(getattr(w, "window_key", "") or "").strip() == wk), None)
    return {
        "success": True,
        "message": "窗口代理信息已同步到本地",
        "affected": int(affected or 0),
        "proxy_id": proxy_id,
        "proxy_addr": (str(getattr(updated, "proxy_addr", "") or "").strip() or None) if updated else None,
        "proxy_country": (str(getattr(updated, "proxy_country", "") or "").strip() or None) if updated else None,
    }


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

    result = await db.upsert_windows(space_pk=space_pk, windows=windows)
    affected = result["affected"]
    skipped = result["skipped"]
    skipped_keys = result["skipped_keys"]
    msg = f"同步完成，写入/更新 {affected} 条窗口记录"
    if skipped > 0:
        msg += f"，跳过 {skipped} 个跨项目空间窗口"
    return {
        "success": True,
        "message": msg,
        "affected": affected,
        "skipped": skipped,
        "skipped_keys": skipped_keys,
    }


@router.post("/api/admin/spaces/{space_pk}/sync-window-status")
async def sync_window_status(space_pk: int, token: str = Depends(verify_admin_token)):
    """同步某个空间下本地窗口的“打开状态”（1=打开，0=未打开）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")

    browser = await db.get_browser(space.browser_id)
    if not browser:
        raise HTTPException(status_code=404, detail="browser not found")

    local_windows = await db.list_windows(space_pk)
    window_keys = [str(w.window_key or "").strip() for w in local_windows if str(w.window_key or "").strip()]

    syscfg = await db.get_system_config()
    client = FPBrowserClient(
        proxy_enabled=syscfg.proxy_enabled,
        proxy_url=syscfg.proxy_url,
    )
    try:
        opened = await client.list_open_window_connection_infos(
            vendor=browser.vendor,
            base_url=browser.lan_addr,
            access_key=browser.access_key,
            window_keys=window_keys,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"查询浏览器窗口状态失败: {e}")

    open_keys: List[str] = []
    for row in opened:
        did = str((row or {}).get("dirId") or "").strip()
        if did:
            open_keys.append(did)

    affected = await db.sync_window_statuses(space_pk=space_pk, open_window_keys=open_keys)
    return {
        "success": True,
        "message": f"状态同步完成：{len(open_keys)} 个打开 / {len(window_keys)} 个窗口",
        "affected": affected,
        "open_count": len(open_keys),
        "total": len(window_keys),
    }


@router.post("/api/admin/spaces/{space_pk}/windows/{window_key}/delete")
async def delete_window_local(space_pk: int, window_key: str, req: DeleteWindowRequest = DeleteWindowRequest(), token: str = Depends(verify_admin_token)):
    """删除窗口（可选删指纹浏览器远端，再本地标记 deleted=1）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")

    browser = await db.get_browser(space.browser_id)
    if not browser:
        raise HTTPException(status_code=404, detail="browser not found")

    wk = str(window_key or "").strip()
    if not wk:
        raise HTTPException(status_code=400, detail="window_key is required")

    roxy_rsp = None
    remote_already_missing = False

    if req.delete_remote:
        syscfg = await db.get_system_config()
        client = FPBrowserClient(proxy_enabled=syscfg.proxy_enabled, proxy_url=syscfg.proxy_url)
        try:
            roxy_rsp = await client.delete_windows(
                vendor=browser.vendor,
                base_url=browser.lan_addr,
                access_key=browser.access_key,
                space_id=space.space_id,
                window_keys=[wk],
                is_soft_deleted=False,
            )
        except RuntimeError as e:
            raise HTTPException(status_code=400, detail=str(e))

        roxy_msg = str((roxy_rsp or {}).get("msg") or "").strip()
        try:
            roxy_code = int((roxy_rsp or {}).get("code") or -1)
        except Exception:
            roxy_code = -1

        roxy_not_found_hints = (
            "待删除的窗口不存在",
            "窗口不存在",
            "window not found",
            "not found",
        )
        remote_already_missing = (roxy_code != 0) and any(k in roxy_msg.lower() for k in (s.lower() for s in roxy_not_found_hints))
        if roxy_code != 0 and not remote_already_missing:
            raise HTTPException(status_code=400, detail=roxy_msg or "指纹浏览器删除窗口失败")

    affected = await db.delete_window_by_key(space_pk=space_pk, window_key=wk)
    window_affected = int((affected or {}).get("window_affected") or 0)
    task_type_window_affected = int((affected or {}).get("task_type_window_affected") or 0)
    if window_affected <= 0:
        raise HTTPException(status_code=404, detail="window not found or already deleted")

    if not req.delete_remote:
        final_message = "窗口已删除（仅本地），并级联标记 task_type_window 删除"
    elif remote_already_missing:
        final_message = "远端窗口已不存在，已完成本地删除，并级联标记 task_type_window 删除"
    else:
        final_message = "窗口已删除（远程 + 本地），并级联标记 task_type_window 删除"
    return {
        "success": True,
        "message": final_message,
        "affected": window_affected,
        "task_type_window_affected": task_type_window_affected,
        "roxy_response": roxy_rsp,
    }


@router.post("/api/admin/spaces/{space_pk}/windows/{window_key}/move")
async def move_window_local(space_pk: int, window_key: str, req: MoveWindowRequest, token: str = Depends(verify_admin_token)):
    """仅本地转移窗口到另一个空间（不调用指纹浏览器接口）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    source_space = await db.get_space(space_pk)
    if not source_space:
        raise HTTPException(status_code=404, detail="source space not found")

    target_space_pk = int(req.target_space_pk or 0)
    if target_space_pk <= 0:
        raise HTTPException(status_code=400, detail="target_space_pk is required")
    if target_space_pk == int(space_pk):
        raise HTTPException(status_code=400, detail="目标空间不能与源空间相同")

    target_space = await db.get_space(target_space_pk)
    if not target_space:
        raise HTTPException(status_code=404, detail="target space not found")

    wk = str(window_key or "").strip()
    if not wk:
        raise HTTPException(status_code=400, detail="window_key is required")

    try:
        affected = await db.move_window_to_space(
            source_space_pk=int(space_pk),
            target_space_pk=target_space_pk,
            window_key=wk,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        raise HTTPException(status_code=500, detail="move window failed")

    if affected <= 0:
        raise HTTPException(status_code=404, detail="window not found or already deleted")
    return {
        "success": True,
        "message": f"窗口已转移到目标空间（space_pk={target_space_pk}）",
        "affected": affected,
        "source_space_pk": int(space_pk),
        "target_space_pk": target_space_pk,
    }


@router.post("/api/admin/spaces/{space_pk}/windows/{window_key}/remark")
async def update_window_remark_local(
    space_pk: int,
    window_key: str,
    req: UpdateWindowRemarkRequest,
    token: str = Depends(verify_admin_token),
):
    """仅本地更新窗口备注（不调用指纹浏览器接口）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    space = await db.get_space(space_pk)
    if not space:
        raise HTTPException(status_code=404, detail="space not found")

    wk = str(window_key or "").strip()
    if not wk:
        raise HTTPException(status_code=400, detail="window_key is required")

    remark = str(req.window_remark or "").strip()
    affected = await db.update_window_remark(space_pk=int(space_pk), window_key=wk, remark=remark)
    if affected <= 0:
        raise HTTPException(status_code=404, detail="window not found or already deleted")
    return {"success": True, "message": "窗口备注已更新", "affected": affected, "window_remark": remark}


@router.get("/api/admin/browsers/{browser_id}/workspace-projects")
async def get_browser_workspace_projects(browser_id: int, token: str = Depends(verify_admin_token)):
    """读取指纹浏览器的“空间 + 项目列表”（只读展示，不落库）。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    browser = await db.get_browser(browser_id)
    if not browser:
        raise HTTPException(status_code=404, detail="browser not found")

    syscfg = await db.get_system_config()
    client = FPBrowserClient(
        proxy_enabled=syscfg.proxy_enabled,
        proxy_url=syscfg.proxy_url,
    )

    try:
        rows = await client.list_workspace_projects(
            vendor=browser.vendor,
            base_url=browser.lan_addr,
            access_key=browser.access_key,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"success": True, "browser_id": browser_id, "rows": rows}


@router.get("/api/admin/tree")
async def get_project_tree(project_id: Optional[int] = None, token: str = Depends(verify_admin_token)):
    """返回树形结构（项目 -> 浏览器 -> 空间 -> 窗口列表）。

    UI 侧可据此实现：项目切换、浏览器折叠/展开、空间加载窗口、以及“一键展开全部”。
    """
    # 仅要求已登录；不再强依赖 projects 页面权限。
    # 仍然会按项目权限（allowed_project_ids）过滤可见数据。
    user = await _get_user_by_token(token)
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    tree = await db.get_project_tree(
        project_id=project_id,
        allowed_project_ids=None,
    )

    return {"success": True, "tree": tree}


@router.get("/api/admin/windows/all")
async def list_all_windows(project_id: Optional[int] = None, token: str = Depends(verify_admin_token)):
    """用于“选择窗口绑定任务类型”的弹窗数据源。"""
    user = await _ensure_page_access(token, "task_types")
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    allowed_ids = await _get_allowed_project_ids(user)
    tree = await db.get_project_tree(
        project_id=project_id,
        allowed_project_ids=allowed_ids,
    )

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
                            "window_sort_num": w.get("window_sort_num"),
                            "window_name": w.get("window_name"),
                            "window_status": w.get("window_status", 0),
                            "window_remark": w.get("window_remark"),
                            "platform_account": w.get("platform_account"),
                            "platform_url": w.get("platform_url"),
                            "proxy_addr": w.get("proxy_addr"),
                            "proxy_country": w.get("proxy_country"),
                            "proxy_expire_at": w.get("proxy_expire_at"),
                        }
                    )
    return {"success": True, "windows": flat}


# -------------------- card keys --------------------
@router.get("/api/admin/card-keys")
async def list_card_keys(token: str = Depends(verify_admin_token)):
    await _ensure_any_page_access(token, {"card_keys", "test"})
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "items": [x.model_dump() for x in await db.list_card_keys()]}


@router.post("/api/admin/card-keys/import")
async def import_card_keys(req: ImportCardKeysRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    result = await db.batch_import_card_keys(req.content)
    return {"success": True, **result}


@router.put("/api/admin/card-keys/{card_key_id}")
async def update_card_key(card_key_id: int, req: UpdateCardKeyRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    try:
        affected = await db.update_card_key(card_key_id=card_key_id, card_key=req.card_key)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    if affected <= 0:
        raise HTTPException(status_code=404, detail="card key not found")
    return {"success": True}


@router.delete("/api/admin/card-keys/{card_key_id}")
async def delete_card_key(card_key_id: int, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    affected = await db.delete_card_key(card_key_id=card_key_id)
    if affected <= 0:
        raise HTTPException(status_code=404, detail="card key not found")
    return {"success": True}


@router.post("/api/admin/card-keys/batch-delete")
async def batch_delete_card_keys(req: BatchDeleteCardKeysRequest, token: str = Depends(verify_admin_token)):
    await _ensure_any_page_access(token, {"card_keys", "test"})
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    if not req.ids:
        raise HTTPException(status_code=400, detail="ids 不能为空")
    affected = await db.batch_delete_card_keys(req.ids)
    return {"success": True, "deleted": affected}


# -------------------- task types --------------------
@router.get("/api/admin/task-types")
async def list_task_types(include_all: bool = False, token: str = Depends(verify_admin_token)):
    user = await _ensure_any_page_access(token, {"task_types", "tasks", "tasks_gantt", "test", "users"})
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    allowed_ids = None if include_all else await _get_allowed_task_type_ids(user)
    return {"success": True, "task_types": [t.model_dump() for t in await db.list_task_types(allowed_task_type_ids=allowed_ids)]}


@router.post("/api/admin/task-types")
async def create_task_type(req: CreateTaskTypeRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    try:
        await _ensure_page_access(token, "task_types")
        user = await _ensure_admin_user(token)
        allowed_project_ids = await _get_allowed_project_ids(user)
        if req.project_id is not None:
            if allowed_project_ids is not None and int(req.project_id) not in {int(x) for x in allowed_project_ids}:
                raise HTTPException(status_code=403, detail="无权绑定到该项目")
            projects = await db.list_projects(allowed_project_ids=allowed_project_ids)
            if int(req.project_id) not in {int(p.id or 0) for p in projects}:
                raise HTTPException(status_code=400, detail="绑定项目不存在")

        # 校验 handler key（避免保存后运行时报错）
        from ..services.task_handler_registry import get_create_task_handler, get_refresh_quota_handler

        if req.create_task_handler:
            get_create_task_handler(req.create_task_handler)
        if req.refresh_quota_handler:
            get_refresh_quota_handler(req.refresh_quota_handler)

        tid = await db.create_task_type(
            req.name,
            req.code,
            req.project_id,
            req.concurrency,
            req.continuous_error_threshold,
            req.continuous_error_close_window_threshold,
            req.timeout_seconds,
            create_task_handler=req.create_task_handler,
            refresh_quota_handler=req.refresh_quota_handler,
            error_retry_count=req.error_retry_count,
            default_target_url=req.default_target_url,
        )
        return {"success": True, "task_type_id": tid}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/api/admin/task-types/{task_type_id}")
async def update_task_type(task_type_id: int, req: UpdateTaskTypeRequest, token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    try:
        await _ensure_page_access(token, "task_types")
        user = await _ensure_admin_user(token)
        allowed_project_ids = await _get_allowed_project_ids(user)
        if req.project_id is not None:
            if allowed_project_ids is not None and int(req.project_id) not in {int(x) for x in allowed_project_ids}:
                raise HTTPException(status_code=403, detail="无权绑定到该项目")
            projects = await db.list_projects(allowed_project_ids=allowed_project_ids)
            if int(req.project_id) not in {int(p.id or 0) for p in projects}:
                raise HTTPException(status_code=400, detail="绑定项目不存在")

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
            project_id=req.project_id,
            concurrency=req.concurrency,
            continuous_error_threshold=req.continuous_error_threshold,
            continuous_error_close_window_threshold=req.continuous_error_close_window_threshold,
            timeout_seconds=req.timeout_seconds,
            create_task_handler=req.create_task_handler,
            refresh_quota_handler=req.refresh_quota_handler,
            enabled=req.enabled,
            error_retry_count=req.error_retry_count,
            default_target_url=req.default_target_url,
        )
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# -------------------- task type dynamic handlers --------------------
@router.get("/api/admin/task-type-handler-options")
async def list_task_type_handler_options(token: str = Depends(verify_admin_token)):
    await _ensure_page_access(token, "task_types")
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
    headless: bool = False,
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

    from ..services.task_handler_registry import (
        RefreshQuotaContext,
        get_refresh_quota_handler,
        refresh_quota__veo_flow_credits,
    )

    create_handler = str(ctx_row.get("create_task_handler") or "").strip().lower()
    # veo_workflow：始终走指纹窗口内 fetch credits（与刷新会员信息一致），不依赖任务类型上选的 refresh_quota_handler
    if create_handler == "veo_workflow":
        fn = refresh_quota__veo_flow_credits
        handler_used = "veo_flow_credits"
    else:
        try:
            fn = get_refresh_quota_handler(task_type.refresh_quota_handler)
        except KeyError as e:
            raise HTTPException(status_code=400, detail=str(e))
        handler_used = (task_type.refresh_quota_handler or "").strip() or "noop"

    ctx_row["_headless"] = headless
    try:
        new_remaining = await fn(RefreshQuotaContext(task_type=task_type, mapping_row=ctx_row, db=db))
        new_remaining = max(0, int(new_remaining))
    except Exception as e:
        is_auto = str(source or "").strip().lower() == "auto"
        no_penalty = bool(getattr(e, "no_penalty", False))

        # 手工触发：不做风控/熔断副作用，仅返回错误即可
        if not is_auto:
            raise HTTPException(status_code=400, detail=f"刷新额度失败：{e}")

        # 定时触发：记录错误日志，并累计窗口连续错误；达到阈值才禁用
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

        suffix = "（已记录）"
        if no_penalty:
            suffix = "（已记录；no_penalty：未计入连续错误）"
        else:
            thr = max(1, int(getattr(task_type, "continuous_error_threshold", 3) or 3))
            try:
                await db.mark_mapping_error(mapping_id=mapping_id, threshold=thr, cooldown_seconds=3600)
            except Exception:
                pass

            try:
                st = await db.get_mapping_runtime_state(mapping_id=mapping_id)
                ce = int((st or {}).get("consecutive_errors") or 0)
                if ce >= thr:
                    try:
                        await db.update_task_type_window(mapping_id=mapping_id, enabled=False)
                        suffix = f"（已记录；连续错误 {ce}/{thr} 达到阈值，已自动禁用该窗口）"
                    except Exception:
                        suffix = f"（已记录；连续错误 {ce}/{thr} 达到阈值）"
                else:
                    suffix = f"（已记录；已累计连续错误 {ce}/{thr}）"
            except Exception:
                suffix = f"（已记录；已累计连续错误，阈值 {thr}）"

        raise HTTPException(status_code=400, detail=f"刷新额度失败：{e}{suffix}")


    await db.update_task_type_window(mapping_id=mapping_id, remaining_quota=new_remaining)
    # 一次成功：连续错误清零（手工/定时都可清零）
    try:
        await db.mark_mapping_success(mapping_id=mapping_id)
    except Exception:
        pass
    return {"success": True, "mapping_id": mapping_id, "remaining_quota": new_remaining, "handler": handler_used}


@router.post("/api/admin/task-type-windows/{mapping_id}/refresh-subscription-info")
async def refresh_mapping_subscription_info(mapping_id: int, headless: bool = False, token: str = Depends(verify_admin_token)):
    """通过指纹浏览器读取订阅信息并写回 mapping。"""
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    ctx_row = await db.get_task_type_window_context(mapping_id)
    if not ctx_row:
        raise HTTPException(status_code=404, detail="mapping not found")

    handler = str(ctx_row.get("create_task_handler") or "").strip().lower()

    vendor = str(ctx_row.get("vendor") or "roxy")
    base_url = str(ctx_row.get("lan_addr") or "")
    access_key = ctx_row.get("access_key")
    space_id = str(ctx_row.get("space_id") or "")
    window_key = str(ctx_row.get("window_key") or "")
    if not base_url or not space_id or not window_key:
        raise HTTPException(status_code=400, detail="mapping missing vendor/lan_addr/space_id/window_key")

    if handler == "veo_workflow":
        # 在指纹浏览器页面内请求 credits（与 Sora 的 page_fetch_json 一致，走窗口网络栈）
        from ..services.veo_workflow_executor import (  # type: ignore
            get_or_create_veo_session,
            veo_fetch_credits_in_window,
            veo_format_paygate_tier_label,
        )

        at = str(ctx_row.get("sora_access_token") or "").strip()
        if not at:
            raise HTTPException(status_code=400, detail="缺少 access_token，请先获取并保存")

        default_target_url = str(ctx_row.get("default_target_url") or "").strip()
        target_url = default_target_url or "https://labs.google/fx"

        veo_ctx = get_or_create_veo_session(
            vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key
        )
        veo_ctx.browser_headless = headless

        try:
            info = await veo_fetch_credits_in_window(sess=veo_ctx, target_url=target_url, access_token=at)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"刷新会员信息失败：{e}")

        tier = str((info or {}).get("user_paygate_tier") or "").strip() or None
        plan_title = veo_format_paygate_tier_label(tier)
        credits = int((info or {}).get("credits") or 0)
        await db.update_task_type_window(
            mapping_id=mapping_id,
            sora_plan_title=plan_title,
            remaining_quota=credits,
            sora_remaining_count=credits,
        )
        return {
            "success": True,
            "mapping_id": mapping_id,
            "plan_title": plan_title,
            "subscription_end": None,
            "credits": credits,
            "user_paygate_tier": tier,
        }

    from ..services.sora_task_executor import get_or_create_sora_session  # type: ignore

    sora_ctx = get_or_create_sora_session(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
    sora_ctx.browser_headless = headless
    try:
        sora_ctx.set_access_token(ctx_row.get("sora_access_token"), ctx_row.get("sora_access_expires"))
    except Exception:
        pass

    try:
        info = await sora_ctx.api_subscription_info(target_url="https://sora.chatgpt.com/drafts")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"刷新会员信息失败：{e}")

    plan_title = str((info or {}).get("plan_title") or "").strip() or None
    subscription_end = str((info or {}).get("subscription_end") or "").strip() or None
    await db.update_task_type_window(
        mapping_id=mapping_id,
        sora_plan_title=plan_title,
        sora_subscription_end=subscription_end,
    )
    return {
        "success": True,
        "mapping_id": mapping_id,
        "plan_title": plan_title,
        "subscription_end": subscription_end,
    }


@router.post("/api/admin/task-type-windows/{mapping_id}/clear-drafts")
async def clear_mapping_drafts(mapping_id: int, headless: bool = False, token: str = Depends(verify_admin_token)):
    """通过指纹浏览器获取 drafts/v2 列表并逐个 DELETE 清除所有草稿。"""
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
    sora_ctx.browser_headless = headless
    try:
        sora_ctx.set_access_token(ctx_row.get("sora_access_token"), ctx_row.get("sora_access_expires"))
    except Exception:
        pass

    try:
        result = await sora_ctx.api_clear_all_drafts(target_url="https://sora.chatgpt.com/drafts")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"清除草稿失败：{e}")

    try:
        await db.update_task_type_window(mapping_id=mapping_id, sora_drafts_count=0)
    except Exception:
        pass

    return {
        "success": True,
        "mapping_id": mapping_id,
        "total": result.get("total", 0),
        "deleted": result.get("deleted", []),
        "failed": result.get("failed", []),
    }


@router.get("/api/admin/auto-refresh-errors")
async def list_auto_refresh_errors(
    limit: int = 200,
    offset: int = 0,
    task_type_id: Optional[int] = None,
    mapping_id: Optional[int] = None,
    token: str = Depends(verify_admin_token),
):
    await _ensure_page_access(token, "task_types")
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
        sora_ctx.set_access_token(ctx_row.get("sora_access_token"), ctx_row.get("sora_access_expires"))
    except Exception:
        pass
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


@router.post("/api/admin/task-type-windows/{mapping_id}/convert-access-token")
async def convert_sora_session_token_to_access_token(
    mapping_id: int,
    req: UpsertSoraAccessTokenRequest,
    headless: bool = False,
    token: str = Depends(verify_admin_token),
):
    """写入/自动获取并写入 access_token + expires 到 mapping。

    规则：
    - 若请求携带 access_token（非空）：直接写入 DB（不发起自动获取）。
    - 若 access_token 为空：使用指纹浏览器对应窗口，在页面上下文内请求 `/api/auth/session` 获取并写入。
    """
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    input_token = str(req.access_token or "").strip() or None
    input_expires = str(req.expires or "").strip() or None

    if input_token:
        await db.update_task_type_window(mapping_id=mapping_id, sora_access_token=input_token, sora_access_expires=input_expires)
        return {"success": True, "mapping_id": mapping_id, "access_token": input_token, "expires": input_expires, "source": "manual"}

    ctx_row = await db.get_task_type_window_context(mapping_id)
    if not ctx_row:
        raise HTTPException(status_code=404, detail="mapping not found")

    handler = str(ctx_row.get("create_task_handler") or "").strip().lower()

    vendor = str(ctx_row.get("vendor") or "roxy")
    base_url = str(ctx_row.get("lan_addr") or "")
    access_key = ctx_row.get("access_key")
    space_id = str(ctx_row.get("space_id") or "")
    window_key = str(ctx_row.get("window_key") or "")
    if not base_url or not space_id or not window_key:
        raise HTTPException(status_code=400, detail="mapping missing vendor/lan_addr/space_id/window_key")

    if handler == "veo_workflow":
        default_target_url = str(ctx_row.get("default_target_url") or "").strip()
        target_url = default_target_url or "https://labs.google/fx"

        try:
            from ..services.veo_workflow_executor import get_or_create_veo_session, veo_fetch_access_token_in_window  # type: ignore

            veo_ctx = get_or_create_veo_session(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
            veo_ctx.browser_headless = headless
            info = await veo_fetch_access_token_in_window(sess=veo_ctx, target_url=target_url)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"自动获取失败：{e}")

        access_token = str((info or {}).get("access_token") or "").strip() or None
        expires = str((info or {}).get("expires") or "").strip() or None
        if not access_token:
            raise HTTPException(status_code=400, detail="自动获取失败：返回缺少 access_token / session_token")

        await db.update_task_type_window(mapping_id=mapping_id, sora_access_token=access_token, sora_access_expires=expires)
        return {
            "success": True,
            "mapping_id": mapping_id,
            "access_token": access_token,
            "expires": expires,
            "session_token": str((info or {}).get("session_token") or "").strip() or None,
            "source": "window",
        }
    else:
        try:
            from ..services.sora_task_executor import get_or_create_sora_session, sora_fetch_access_token_in_window  # type: ignore

            sora_ctx = get_or_create_sora_session(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
            sora_ctx.browser_headless = headless
            info = await sora_fetch_access_token_in_window(sess=sora_ctx, target_url="https://sora.chatgpt.com/drafts")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"自动获取失败：{e}")

        access_token = str((info or {}).get("access_token") or "").strip() or None
        expires = str((info or {}).get("expires") or "").strip() or None
        if not access_token:
            raise HTTPException(status_code=400, detail="自动获取失败：返回缺少 access_token")

        await db.update_task_type_window(mapping_id=mapping_id, sora_access_token=access_token, sora_access_expires=expires)
        try:
            sora_ctx.set_access_token(access_token, expires)
        except Exception:
            pass
        return {"success": True, "mapping_id": mapping_id, "access_token": access_token, "expires": expires, "source": "window"}


@router.post("/api/admin/task-type-windows/{mapping_id}/manual-open")
async def manual_open_mapping_window(mapping_id: int, headless: bool = False, token: str = Depends(verify_admin_token)):
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

    handler = str(ctx_row.get("create_task_handler") or "").strip().lower()
    default_target_url = str(ctx_row.get("default_target_url") or "").strip()

    if handler == "veo_workflow":
        from ..services.veo_workflow_executor import get_or_create_veo_session  # type: ignore

        target_url = default_target_url or "https://veo.google.com"
        veo_ctx = get_or_create_veo_session(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
        veo_ctx.browser_headless = headless
        veo_ctx.idle_close_disabled = True
        try:
            veo_ctx._cancel_idle_close()
        except Exception:
            pass
        try:
            await veo_ctx.ensure_open(args=[], force_open=False, headless=headless)
            await veo_ctx._bring_target_page_to_front(refresh_target=False, drafts_url=target_url)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"打开窗口失败：{e}")
    else:
        from ..services.sora_task_executor import get_or_create_sora_session  # type: ignore

        target_url = default_target_url or "https://sora.chatgpt.com/drafts"
        sora_ctx = get_or_create_sora_session(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
        sora_ctx.browser_headless = headless
        sora_ctx.idle_close_disabled = True
        try:
            sora_ctx._cancel_idle_close()
        except Exception:
            pass
        try:
            await sora_ctx.ensure_open(args=[], force_open=False, headless=headless)
            await sora_ctx._bring_sora_drafts_to_front(refresh_target=False, drafts_url=target_url)
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


@router.post("/api/admin/task-type-windows/{mapping_id}/manual-start")
async def manual_start_mapping_window(mapping_id: int, headless: bool = False, token: str = Depends(verify_admin_token)):
    """启用窗口 + 立刻关闭指纹浏览器窗口，然后重新打开并进入 Sora drafts。

    说明：用于管理台“启动”按钮，效果等价于“强制重启窗口并确保 drafts tab 置前”。
    """
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

    handler = str(ctx_row.get("create_task_handler") or "").strip().lower()
    default_target_url = str(ctx_row.get("default_target_url") or "").strip()

    if handler == "veo_workflow":
        from ..services.veo_workflow_executor import get_or_create_veo_session  # type: ignore

        target_url = default_target_url or "https://veo.google.com"
        veo_ctx = get_or_create_veo_session(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
        veo_ctx.browser_headless = headless
        veo_ctx.idle_close_disabled = True
        try:
            veo_ctx._cancel_idle_close()
        except Exception:
            pass
        try:
            await veo_ctx.close_and_drop()
        except Exception:
            pass
        try:
            await veo_ctx.ensure_open(args=[], force_open=True, headless=headless)
            await veo_ctx._bring_target_page_to_front(refresh_target=False, drafts_url=target_url)
            try:
                await db.update_task_type_window(mapping_id=mapping_id, enabled=True)
            except Exception:
                pass
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"启动窗口失败：{e}")
    else:
        from ..services.sora_task_executor import get_or_create_sora_session  # type: ignore

        target_url = default_target_url or "https://sora.chatgpt.com/drafts"
        sora_ctx = get_or_create_sora_session(vendor=vendor, base_url=base_url, access_key=access_key, space_id=space_id, window_key=window_key)
        sora_ctx.browser_headless = headless
        sora_ctx.idle_close_disabled = True
        try:
            sora_ctx._cancel_idle_close()
        except Exception:
            pass
        try:
            await sora_ctx.close_and_drop()
        except Exception:
            pass
        try:
            await sora_ctx.ensure_open(args=[], force_open=True, headless=headless)
            await sora_ctx._bring_sora_drafts_to_front(refresh_target=False, drafts_url=target_url)
            try:
                await db.update_task_type_window(mapping_id=mapping_id, enabled=True)
            except Exception:
                pass
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"启动窗口失败：{e}")

    return {"success": True, "mapping_id": mapping_id, "enabled": True, "idle_close_disabled": True}


@router.delete("/api/admin/task-types/{task_type_id}")
async def delete_task_type(task_type_id: int, token: str = Depends(verify_admin_token)):
    await _ensure_admin_user(token)
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.delete_task_type(task_type_id)
    return {"success": True}


@router.get("/api/admin/task-types/{task_type_id}/windows")
async def list_task_type_windows(task_type_id: int, token: str = Depends(verify_admin_token)):
    user = await _ensure_any_page_access(token, {"task_types", "test"})
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    allowed_ids = await _get_allowed_task_type_ids(user)
    if allowed_ids is not None and int(task_type_id) not in {int(x) for x in allowed_ids}:
        raise HTTPException(status_code=403, detail="无权查看该任务类型")
    return {"success": True, "items": await db.list_task_type_windows(task_type_id)}


@router.post("/api/admin/task-types/{task_type_id}/windows")
async def add_task_type_windows(task_type_id: int, req: AddTaskTypeWindowsRequest, token: str = Depends(verify_admin_token)):
    user = await _ensure_page_access(token, "task_types")
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    allowed_task_type_ids = await _get_allowed_task_type_ids(user)
    if allowed_task_type_ids is not None and int(task_type_id) not in {int(x) for x in allowed_task_type_ids}:
        raise HTTPException(status_code=403, detail="无权操作该任务类型")

    allowed_project_ids = await _get_allowed_project_ids(user)
    if allowed_project_ids is not None:
        pairs = await db.list_window_project_pairs([int(x) for x in (req.window_pks or [])])
        missing = [int(x) for x in (req.window_pks or []) if int(x) not in pairs]
        if missing:
            raise HTTPException(status_code=400, detail=f"窗口不存在或已删除: {missing[:5]}")
        allowed_set = {int(x) for x in allowed_project_ids}
        denied = [wid for wid, pid in pairs.items() if int(pid) not in allowed_set]
        if denied:
            raise HTTPException(status_code=403, detail=f"无权绑定这些窗口: {denied[:5]}")
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
    await _ensure_page_access(token, "task_types")
    if req.task_type_id is not None:
        ctx = await db.get_task_type_window_context(mapping_id)
        if not ctx:
            raise HTTPException(status_code=404, detail="绑定不存在或已删除")

        src_type_id = int(ctx.get("task_type_id") or 0)
        target_type_id = int(req.task_type_id)
        if target_type_id != src_type_id:
            target = await db.get_task_type(target_type_id)
            if not target:
                raise HTTPException(status_code=400, detail="目标任务类型不存在")

            same_type_rows = await db.list_task_type_windows(target_type_id)
            target_has_same_window = any(
                int(row.get("window_pk") or 0) == int(ctx.get("window_pk") or 0)
                and int(row.get("id") or 0) != int(mapping_id)
                for row in (same_type_rows or [])
            )
            if target_has_same_window:
                raise HTTPException(status_code=400, detail="目标任务类型已绑定该窗口")
    try:
        await db.update_task_type_window(
            mapping_id=mapping_id,
            enabled=req.enabled,
            headless=req.headless,
            pure_mode=req.pure_mode,
            deleted=req.deleted,
            task_type_id=req.task_type_id,
            daily_quota=req.daily_quota,
            remaining_quota=req.remaining_quota,
            cooldown_until=req.cooldown_until,
            error_cooldown_until=req.error_cooldown_until,
            total_errors=req.total_errors,
            consecutive_errors=req.consecutive_errors,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"success": True}


def _admin_veo_pool_base_name(raw: Optional[str]) -> str:
    s = (raw or "").strip()
    if s:
        parts = s.rsplit(" ", 1)
        if len(parts) == 2 and parts[1].startswith("P") and parts[1][1:].isdigit():
            return parts[0]
        return s
    return datetime.now().strftime("%b %d - %H:%M")


def _admin_veo_pooled_project_title(pool_index: int, base_name: Optional[str]) -> str:
    return f"{_admin_veo_pool_base_name(base_name)} P{pool_index}"


@router.get("/api/admin/task-type-windows/{mapping_id}/veo-flow-projects")
async def admin_list_veo_flow_projects(mapping_id: int, token: str = Depends(verify_admin_token)):
    user = await _ensure_any_page_access(token, {"task_types", "test"})
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    ctx = await db.get_task_type_window_context(mapping_id)
    if not ctx:
        raise HTTPException(status_code=404, detail="绑定不存在或已删除")
    allowed = await _get_allowed_task_type_ids(user)
    ttid = int(ctx.get("task_type_id") or 0)
    if allowed is not None and ttid not in {int(x) for x in allowed}:
        raise HTTPException(status_code=403, detail="无权查看该绑定")
    if str(ctx.get("create_task_handler") or "").strip().lower() != "veo_workflow":
        raise HTTPException(status_code=400, detail="仅 veo_workflow 任务类型支持 Veo 项目管理")
    items = await db.list_veo_flow_projects(mapping_id)
    return {"success": True, "items": items}


@router.post("/api/admin/task-type-windows/{mapping_id}/veo-flow-projects")
async def admin_create_veo_flow_project(
    mapping_id: int,
    headless: bool = False,
    base_name: Optional[str] = Query(None, max_length=240),
    token: str = Depends(verify_admin_token),
):
    await _ensure_page_access(token, "task_types")
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    ctx = await db.get_task_type_window_context(mapping_id)
    if not ctx:
        raise HTTPException(status_code=404, detail="绑定不存在或已删除")

    user = await _get_user_by_token(token)
    allowed_task_type_ids = await _get_allowed_task_type_ids(user)
    ttid = int(ctx.get("task_type_id") or 0)
    if allowed_task_type_ids is not None and ttid not in {int(x) for x in allowed_task_type_ids}:
        raise HTTPException(status_code=403, detail="无权操作该绑定")

    if str(ctx.get("create_task_handler") or "").strip().lower() != "veo_workflow":
        raise HTTPException(status_code=400, detail="仅 veo_workflow 任务类型可通过此处创建 Flow 项目")

    vendor = str(ctx.get("vendor") or "roxy")
    base_url = str(ctx.get("lan_addr") or "")
    access_key = ctx.get("access_key")
    space_id = str(ctx.get("space_id") or "")
    window_key = str(ctx.get("window_key") or "")
    if not base_url or not space_id or not window_key:
        raise HTTPException(status_code=400, detail="绑定缺少浏览器/空间/窗口信息")

    n_existing = await db.count_veo_flow_projects(mapping_id)
    pool_index = n_existing + 1
    project_title = _admin_veo_pooled_project_title(pool_index, base_name)

    default_target_url = str(ctx.get("default_target_url") or "").strip()
    target_url = default_target_url or "https://labs.google/fx"

    from ..services.veo_workflow_executor import get_or_create_veo_session, veo_create_flow_project_in_window  # type: ignore

    sess = get_or_create_veo_session(
        vendor=vendor,
        base_url=base_url,
        access_key=access_key,
        space_id=space_id,
        window_key=window_key,
    )
    sess.browser_headless = bool(headless)

    try:
        flow_project_id = await veo_create_flow_project_in_window(
            sess=sess,
            target_url=target_url,
            title=project_title,
            tool_name="PINHOLE",
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"创建 Flow 项目失败：{e}")

    row_id = await db.add_veo_flow_project(
        task_type_window_id=mapping_id,
        project_id=flow_project_id,
        project_name=project_title,
        tool_name="PINHOLE",
    )

    return {
        "success": True,
        "id": row_id,
        "project_id": flow_project_id,
        "project_name": project_title,
        "veo_flow_project_count": n_existing + 1,
    }


# -------------------- tasks --------------------
@router.get("/api/admin/tasks")
async def list_tasks(
    limit: int = 50,
    offset: int = 0,
    task_type_code: Optional[str] = None,
    status: Optional[str] = None,
    window_pk: Optional[int] = None,
    window_ip: Optional[str] = None,
    q: Optional[str] = None,
    prompt_tag: Optional[str] = None,
    token: str = Depends(verify_admin_token),
):
    await _ensure_page_access(token, "tasks")
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    lim = max(1, min(200, int(limit or 50)))
    off = max(0, int(offset or 0))
    total = await db.count_tasks(task_type_code=task_type_code, status=status, window_pk=window_pk, window_ip=window_ip, q=q, prompt_tag=prompt_tag)
    items = await db.list_tasks(
        limit=lim,
        offset=off,
        task_type_code=task_type_code,
        status=status,
        window_pk=window_pk,
        window_ip=window_ip,
        q=q,
        prompt_tag=prompt_tag,
    )
    return {"success": True, "total": total, "limit": lim, "offset": off, "items": items}


@router.get("/api/admin/tasks/timeline")
async def list_task_success_fail_timeline(
    group_by: str = "account",  # account | ip
    bucket: str = "day",  # month | week | day | hour | minute
    limit: int = 100,
    offset: int = 0,
    task_type_code: Optional[str] = None,
    q: Optional[str] = None,
    start_at: Optional[str] = None,
    end_at: Optional[str] = None,
    token: str = Depends(verify_admin_token),
):
    await _ensure_page_access(token, "tasks_gantt")
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")

    data = await db.list_task_success_fail_timeline(
        group_by=group_by,
        bucket=bucket,
        limit=limit,
        offset=offset,
        task_type_code=task_type_code,
        q=q,
        start_at=start_at,
        end_at=end_at,
    )
    return {"success": True, **data}


@router.get("/api/admin/tasks/timeline-items")
async def list_task_timeline_items(
    group_by: str = "account",  # account | ip
    bucket: str = "day",  # month | week | day | hour | minute
    group_key: str = "",
    bucket_start: str = "",
    limit: int = 200,
    offset: int = 0,
    task_type_code: Optional[str] = None,
    token: str = Depends(verify_admin_token),
):
    await _ensure_page_access(token, "tasks_gantt")
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    if not str(bucket_start or "").strip():
        raise HTTPException(status_code=400, detail="bucket_start is required")

    data = await db.list_task_timeline_items(
        group_by=group_by,
        bucket=bucket,
        group_key=group_key,
        bucket_start=bucket_start,
        limit=limit,
        offset=offset,
        task_type_code=task_type_code,
    )
    return {"success": True, **data}


# -------------------- logs --------------------
@router.get("/api/admin/logs")
async def get_logs(limit: int = 200, token: str = Depends(verify_admin_token)):
    await _ensure_page_access(token, "logs")
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    return {"success": True, "logs": await db.get_request_logs(limit=limit)}


@router.delete("/api/admin/logs")
async def clear_logs(token: str = Depends(verify_admin_token)):
    if not db:
        raise HTTPException(status_code=500, detail="db not initialized")
    await db.clear_request_logs()
    return {"success": True}

