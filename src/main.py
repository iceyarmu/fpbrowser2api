"""FastAPI application initialization."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from pathlib import Path
import re

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

from .api import admin, routes
from .core.config import config
from .core.database import Database
from .core.logger import logger, setup_logging
from .core.models import RequestLog


db = Database()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1) 先按文件配置初始化日志（DB 可能尚未可用）
    setup_logging()

    logger.info("=" * 60)
    logger.info("FPBrowser2API Starting...")
    logger.info("=" * 60)

    config_dict = config.get_raw_config()
    is_first_startup = not db.db_exists()

    # 2) 初始化/迁移数据库（参考 flow2api：启动即建库/补列）
    await db.init_db()
    if is_first_startup:
        logger.info("🎉 首次启动：初始化数据库并写入默认配置/管理员...")
    else:
        logger.info("🔄 检测到已有数据库：检查缺失表/字段并补齐...")
    await db.check_and_migrate_db(config_dict)

    # 2.5) 启动清理：把上次异常退出遗留的 queued/running 任务统一置为 failed
    #      同时重置窗口映射的 inflight_slots，避免并发槽位“泄漏”导致无法继续派单
    try:
        r = await db.fail_running_and_queued_tasks_on_startup()
        if (r.get("tasks_failed") or 0) > 0 or (r.get("mapping_slots_reset") or 0) > 0:
            logger.warning(
                "启动清理完成：tasks_failed=%s mapping_slots_reset=%s",
                r.get("tasks_failed"),
                r.get("mapping_slots_reset"),
            )
    except Exception as e:
        # 不阻断启动：即便清理失败，也让服务起来方便排查
        logger.exception("启动清理失败（忽略）：%s", e)

    # 3) DB 配置回写到内存 config（API key / proxy / debug / log_to_file）
    syscfg = await db.reload_config_to_memory()
    # 4) 按 DB 配置重置日志（特别是 log_to_file）
    setup_logging()
    logger.info(
        "✓ 配置加载完成：proxy_enabled=%s debug=%s log_to_file=%s",
        syscfg.proxy_enabled,
        syscfg.debug_enabled,
        syscfg.log_to_file,
    )

    # 5) 依赖注入
    admin.set_dependencies(db)
    routes.set_dependencies(db)

    logger.info("✓ Server running on http://%s:%s", config.server_host, config.server_port)
    logger.info("=" * 60)

    yield

    logger.info("FPBrowser2API Shutting down...")


app = FastAPI(
    title="FPBrowser2API",
    description="指纹浏览器管理与任务调用服务",
    version="0.1.0",
    lifespan=lifespan,
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_logger(request: Request, call_next):
    """记录请求日志到数据库（简化版，适合后台排查）。

    注意：为避免影响性能，仅记录小体积 body；大文件上传不记录内容。
    """
    start = time.perf_counter()
    status_code = 500
    req_body = None
    resp_body = None
    try:
        # 仅在 JSON/文本时尝试读取
        content_type = (request.headers.get("content-type") or "").lower()
        if "application/json" in content_type:
            try:
                raw = await request.body()
                if raw and len(raw) <= 64 * 1024:
                    req_body = raw.decode("utf-8", errors="ignore")
            except Exception:
                pass

        # 非管理员只允许读取管理台接口；写操作统一仅管理员可执行
        path = request.url.path
        method = (request.method or "").upper()
        if path.startswith("/api/admin") and method in {"POST", "PUT", "PATCH", "DELETE"}:
            bypass_paths = {
                "/api/admin/login",
                "/api/login",
                "/api/admin/logout",
                "/api/logout",
                "/api/admin/change-password",
            }
            non_admin_write_allow_patterns = [
                r"^/api/admin/task-types/\d+/windows$",
                r"^/api/admin/task-type-windows/\d+$",
                r"^/api/admin/spaces/\d+/windows/[^/]+/set-proxy$",
            ]
            allow_non_admin_write = any(re.match(p, path) for p in non_admin_write_allow_patterns)
            if path not in bypass_paths and not allow_non_admin_write:
                authorization = request.headers.get("authorization") or ""
                if not authorization.startswith("Bearer "):
                    return Response(content='{"detail":"Missing authorization"}', media_type="application/json", status_code=401)
                token = authorization[7:]
                username = admin.active_admin_tokens.get(token)
                if not username:
                    return Response(content='{"detail":"Invalid or expired admin token"}', media_type="application/json", status_code=401)
                user = await db.get_admin_user(username)
                if not user or not bool(getattr(user, "is_admin", False)):
                    return Response(content='{"detail":"仅管理员可执行此操作"}', media_type="application/json", status_code=403)

        response: Response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        duration = time.perf_counter() - start
        try:
            # 不阻断主流程：日志写入失败直接忽略
            actor = "admin" if request.url.path.startswith("/api/admin") else "api"
            await db.add_request_log(
                RequestLog(
                    actor=actor,
                    method=request.method,
                    path=request.url.path,
                    request_body=req_body,
                    response_body=resp_body,
                    status_code=int(status_code),
                    duration=float(duration),
                )
            )
        except Exception:
            pass


app.include_router(routes.router)
app.include_router(admin.router)


# 静态资源（js/css/img）
static_dir = Path(__file__).parent.parent / "static"
assets_dir = static_dir / "assets"
assets_dir.mkdir(parents=True, exist_ok=True)
app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")


def _page(path: Path) -> Response:
    if path.exists():
        return FileResponse(str(path))
    return HTMLResponse(content=f"<h1>页面不存在</h1><p>{path.name}</p>", status_code=404)


@app.get("/", response_class=HTMLResponse)
async def index():
    return _page(static_dir / "login.html")


@app.get("/login", response_class=HTMLResponse)
async def login_page():
    return _page(static_dir / "login.html")


@app.get("/admin/system", response_class=HTMLResponse)
async def admin_system_page():
    return _page(static_dir / "system.html")


@app.get("/admin/projects", response_class=HTMLResponse)
async def admin_projects_page():
    return _page(static_dir / "projects.html")


@app.get("/admin/task-types", response_class=HTMLResponse)
async def admin_task_types_page():
    return _page(static_dir / "task_types.html")


@app.get("/admin/tasks", response_class=HTMLResponse)
async def admin_tasks_page():
    return _page(static_dir / "tasks.html")


@app.get("/admin/tasks-gantt", response_class=HTMLResponse)
async def admin_tasks_gantt_page():
    return _page(static_dir / "tasks_gantt.html")


@app.get("/admin/test", response_class=HTMLResponse)
async def admin_test_page():
    return _page(static_dir / "test.html")


@app.get("/admin/card-keys", response_class=HTMLResponse)
async def admin_card_keys_page():
    return _page(static_dir / "card_keys.html")


@app.get("/admin/logs", response_class=HTMLResponse)
async def admin_logs_page():
    return _page(static_dir / "logs.html")


@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users_page():
    return _page(static_dir / "users.html")

