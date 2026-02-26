"""Database storage layer (SQLite + aiosqlite).

参考 flow2api 的策略：
- init_db(): 建表（CREATE TABLE IF NOT EXISTS）
- check_and_migrate_db(): 启动时检查缺失表/缺失列并补齐（ALTER TABLE ADD COLUMN）
- 配置行/管理员账号：首次启动从 setting.toml 初始化，后续仅补缺不覆盖
"""

from __future__ import annotations

import aiosqlite
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .auth import AuthManager
from .models import (
    AdminUser,
    AutoRefreshErrorLog,
    BrowserSpace,
    FingerprintBrowser,
    Project,
    ProxyInfo,
    RequestLog,
    SystemConfig,
    Task,
    TaskType,
    TaskTypePublic,
    TaskTypeWindow,
    WindowInfo,
)


class Database:
    def __init__(self, db_path: Optional[str] = None) -> None:
        if db_path is None:
            data_dir = Path(__file__).parent.parent.parent / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            db_path = str(data_dir / "fpbrowser.db")
        self.db_path = db_path

    def db_exists(self) -> bool:
        return Path(self.db_path).exists()

    async def _table_exists(self, db: aiosqlite.Connection, table_name: str) -> bool:
        cur = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        row = await cur.fetchone()
        return row is not None

    async def _column_exists(self, db: aiosqlite.Connection, table_name: str, column_name: str) -> bool:
        try:
            cur = await db.execute(f"PRAGMA table_info({table_name})")
            cols = await cur.fetchall()
            return any(c[1] == column_name for c in cols)
        except Exception:
            return False

    async def init_db(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA foreign_keys=ON")

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS system_config (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    proxy_enabled BOOLEAN DEFAULT 0,
                    proxy_url TEXT,
                    api_key TEXT NOT NULL,
                    debug_enabled BOOLEAN DEFAULT 0,
                    log_to_file BOOLEAN DEFAULT 0,
                    stop_accepting_tasks BOOLEAN DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    deleted BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS browsers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    lan_addr TEXT NOT NULL,
                    vendor TEXT DEFAULT 'generic',
                    access_key TEXT,
                    deleted BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (project_id) REFERENCES projects(id)
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS spaces (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    browser_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    space_id TEXT NOT NULL,
                    project_ids TEXT,
                    deleted BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (browser_id, space_id),
                    FOREIGN KEY (browser_id) REFERENCES browsers(id)
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS windows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    space_pk INTEGER NOT NULL,
                    window_key TEXT NOT NULL,
                    window_sort_num INTEGER,
                    window_name TEXT NOT NULL,
                    platform_account TEXT,
                    platform_url TEXT,
                    proxy_id INTEGER,
                    proxy_addr TEXT,
                    proxy_country TEXT,
                    proxy_expire_at TEXT,
                    enabled BOOLEAN DEFAULT 1,
                    deleted BOOLEAN DEFAULT 0,
                    raw_json TEXT,
                    synced_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (space_pk, window_key),
                    FOREIGN KEY (space_pk) REFERENCES spaces(id)
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS proxies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    space_pk INTEGER NOT NULL,
                    proxy_id INTEGER NOT NULL,
                    expire_at TEXT,
                    ip_type TEXT,
                    protocol TEXT,
                    host TEXT,
                    port TEXT,
                    proxy_username TEXT,
                    proxy_password TEXT,
                    refresh_url TEXT,
                    remark TEXT,
                    check_status INTEGER,
                    check_channel TEXT,
                    check_channel_value TEXT,
                    last_ip TEXT,
                    last_country TEXT,
                    last_state TEXT,
                    last_city TEXT,
                    check_time TEXT,
                    create_time TEXT,
                    update_time TEXT,
                    deleted BOOLEAN DEFAULT 0,
                    raw_json TEXT,
                    synced_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (space_pk, proxy_id),
                    FOREIGN KEY (space_pk) REFERENCES spaces(id)
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS task_types (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    code TEXT UNIQUE NOT NULL,
                    concurrency INTEGER DEFAULT 1,
                    continuous_error_threshold INTEGER DEFAULT 3,
                    timeout_seconds INTEGER DEFAULT 1800,
                    create_task_handler TEXT,
                    refresh_quota_handler TEXT,
                    enabled BOOLEAN DEFAULT 1,
                    deleted BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS task_type_windows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_type_id INTEGER NOT NULL,
                    window_pk INTEGER NOT NULL,
                    inflight_slots INTEGER DEFAULT 0,
                    total_errors INTEGER DEFAULT 0,
                    consecutive_errors INTEGER DEFAULT 0,
                    daily_quota INTEGER DEFAULT 0,
                    remaining_quota INTEGER DEFAULT 0,
                    sora_remaining_count INTEGER DEFAULT 0,
                    sora_rate_limit_reached BOOLEAN DEFAULT 0,
                    sora_access_resets_in_seconds INTEGER DEFAULT 0,
                    sora_invite_code TEXT,
                    sora_access_token TEXT,
                    sora_access_expires TEXT,
                    -- 额度重置时间点（来自 nf/check：now + access_resets_in_seconds）
                    cooldown_until TIMESTAMP,
                    -- 连续错误熔断冷却时间（与 cooldown_until 区分）
                    error_cooldown_until TIMESTAMP,
                    enabled BOOLEAN DEFAULT 1,
                    deleted BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (task_type_id, window_pk),
                    FOREIGN KEY (task_type_id) REFERENCES task_types(id),
                    FOREIGN KEY (window_pk) REFERENCES windows(id)
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT UNIQUE NOT NULL,
                    task_type_code TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    progress INTEGER DEFAULT 0,
                    prompt TEXT NOT NULL,
                    image_path TEXT,
                    window_pk INTEGER,
                    result_json TEXT,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS request_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor TEXT,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    request_body TEXT,
                    response_body TEXT,
                    status_code INTEGER NOT NULL,
                    duration FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS auto_refresh_error_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mapping_id INTEGER NOT NULL,
                    task_type_id INTEGER,
                    task_code TEXT,
                    window_pk INTEGER,
                    window_name TEXT,
                    platform_account TEXT,
                    error_message TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_task_id ON tasks(task_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_task_types_code ON task_types(code)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_windows_space_pk ON windows(space_pk)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_proxies_space_pk ON proxies(space_pk)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_req_logs_created_at ON request_logs(created_at)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_auto_refresh_err_created_at ON auto_refresh_error_logs(created_at)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_auto_refresh_err_mapping_id ON auto_refresh_error_logs(mapping_id)")

            await db.commit()

    async def _ensure_default_rows(self, db: aiosqlite.Connection, config_dict: Dict[str, Any]) -> None:
        """确保 system_config 与 admin_users 至少有一条记录（不覆盖已有）。"""
        # system_config (id=1)
        cur = await db.execute("SELECT COUNT(*) FROM system_config WHERE id = 1")
        cnt = (await cur.fetchone())[0]
        if cnt == 0:
            proxy_enabled = bool(config_dict.get("system", {}).get("proxy_enabled", False))
            proxy_url = (config_dict.get("system", {}).get("proxy_url", "") or "").strip() or None
            api_key = str(config_dict.get("global", {}).get("api_key", "fpb123456"))
            debug_enabled = bool(config_dict.get("system", {}).get("debug_enabled", False))
            log_to_file = bool(config_dict.get("system", {}).get("log_to_file", False))
            await db.execute(
                """
                INSERT INTO system_config (id, proxy_enabled, proxy_url, api_key, debug_enabled, log_to_file)
                VALUES (1, ?, ?, ?, ?, ?)
                """,
                (proxy_enabled, proxy_url, api_key, debug_enabled, log_to_file),
            )

        # admin_users: 仅当空表时创建默认管理员
        cur = await db.execute("SELECT COUNT(*) FROM admin_users")
        cnt = (await cur.fetchone())[0]
        if cnt == 0:
            username = str(config_dict.get("global", {}).get("admin_username", "admin"))
            password = str(config_dict.get("global", {}).get("admin_password", "admin"))
            password_hash = AuthManager.hash_password(password)
            await db.execute(
                """
                INSERT INTO admin_users (username, password_hash)
                VALUES (?, ?)
                """,
                (username, password_hash),
            )

        # 默认任务类型（仅补缺）
        defaults: List[Tuple[str, str, int, int, int]] = [
            ("文/图生视频", "gen_video", 1, 3, 1800),
            ("文/图生图", "gen_image", 2, 3, 600),
        ]
        for name, code, conc, thr, timeout in defaults:
            cur = await db.execute("SELECT COUNT(*) FROM task_types WHERE code = ?", (code,))
            if (await cur.fetchone())[0] == 0:
                await db.execute(
                    """
                    INSERT INTO task_types (name, code, concurrency, continuous_error_threshold, timeout_seconds, enabled, deleted)
                    VALUES (?, ?, ?, ?, ?, 1, 0)
                    """,
                    (name, code, conc, thr, timeout),
                )

    async def check_and_migrate_db(self, config_dict: Dict[str, Any]) -> None:
        """升级模式：补齐缺失表/列，并确保默认行存在。"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA foreign_keys=ON")

            # Step 1: 缺失表（直接复用 init_db 的建表语句）
            await self.init_db()

            # Step 2: 缺失列（示例：未来扩展时在这里追加）
            if await self._table_exists(db, "system_config"):
                columns_to_add = [
                    ("log_to_file", "BOOLEAN DEFAULT 0"),
                    ("stop_accepting_tasks", "BOOLEAN DEFAULT 0"),
                ]
                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(db, "system_config", col_name):
                        await db.execute(f"ALTER TABLE system_config ADD COLUMN {col_name} {col_type}")

            # task_types: 动态 handler 字段
            if await self._table_exists(db, "task_types"):
                columns_to_add = [
                    ("create_task_handler", "TEXT"),
                    ("refresh_quota_handler", "TEXT"),
                ]
                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(db, "task_types", col_name):
                        await db.execute(f"ALTER TABLE task_types ADD COLUMN {col_name} {col_type}")

            # spaces: project_ids（用于 RoxyBrowser list_v3 的 projectIds 过滤）
            if await self._table_exists(db, "spaces"):
                columns_to_add = [
                    ("project_ids", "TEXT"),
                ]
                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(db, "spaces", col_name):
                        await db.execute(f"ALTER TABLE spaces ADD COLUMN {col_name} {col_type}")

            # windows: window_sort_num（RoxyBrowser: windowSortNum，用于 UI 展示）
            if await self._table_exists(db, "windows"):
                columns_to_add = [
                    ("window_sort_num", "INTEGER"),
                    ("proxy_id", "INTEGER"),
                ]
                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(db, "windows", col_name):
                        await db.execute(f"ALTER TABLE windows ADD COLUMN {col_name} {col_type}")

                # 仅当新增了 proxy_id 列（或历史数据未回填）时，尝试从 raw_json 回填
                # raw_json 结构：{"raw": {"proxyModuleId": <int>}, ...}
                if await self._column_exists(db, "windows", "proxy_id"):
                    try:
                        await db.execute(
                            """
                            UPDATE windows
                            SET proxy_id = CAST(json_extract(raw_json, '$.raw.proxyModuleId') AS INTEGER)
                            WHERE proxy_id IS NULL
                              AND raw_json IS NOT NULL
                              AND json_extract(raw_json, '$.raw.proxyModuleId') IS NOT NULL
                            """
                        )
                    except Exception:
                        # json_extract 依赖 SQLite JSON1；若不可用则用 Python 手工回填
                        try:
                            cur = await db.execute(
                                "SELECT id, raw_json FROM windows WHERE proxy_id IS NULL AND raw_json IS NOT NULL"
                            )
                            rows = await cur.fetchall()
                            for rid, raw_json in rows:
                                try:
                                    obj = json.loads(raw_json or "{}")
                                except Exception:
                                    continue
                                raw_obj = obj.get("raw")
                                if not isinstance(raw_obj, dict):
                                    continue
                                pid_raw = raw_obj.get("proxyModuleId")
                                if pid_raw in (None, "", "-"):
                                    continue
                                try:
                                    pid = int(pid_raw)
                                except Exception:
                                    continue
                                await db.execute("UPDATE windows SET proxy_id = ? WHERE id = ?", (pid, int(rid)))
                        except Exception:
                            pass

            # proxies: expire_at（代理过期时间，用于 UI 过滤/展示）
            if await self._table_exists(db, "proxies"):
                columns_to_add = [
                    ("expire_at", "TEXT"),
                ]
                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(db, "proxies", col_name):
                        await db.execute(f"ALTER TABLE proxies ADD COLUMN {col_name} {col_type}")

            # task_type_windows: 移除 max_concurrency（窗口层并发不再配置）
            if await self._table_exists(db, "task_type_windows"):
                # 是否需要做一次性迁移：旧版 cooldown_until 曾用于“错误冷却”
                need_cooldown_migration = False
                if await self._column_exists(db, "task_type_windows", "max_concurrency"):
                    # SQLite 不支持 DROP COLUMN：采用“建新表 -> 拷贝 -> 替换”的方式迁移
                    await db.execute("PRAGMA foreign_keys=OFF")
                    await db.execute("BEGIN")
                    await db.execute(
                        """
                        CREATE TABLE IF NOT EXISTS task_type_windows_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            task_type_id INTEGER NOT NULL,
                            window_pk INTEGER NOT NULL,
                            inflight_slots INTEGER DEFAULT 0,
                            total_errors INTEGER DEFAULT 0,
                            consecutive_errors INTEGER DEFAULT 0,
                            daily_quota INTEGER DEFAULT 0,
                            remaining_quota INTEGER DEFAULT 0,
                            cooldown_until TIMESTAMP,
                            error_cooldown_until TIMESTAMP,
                            enabled BOOLEAN DEFAULT 1,
                            deleted BOOLEAN DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE (task_type_id, window_pk),
                            FOREIGN KEY (task_type_id) REFERENCES task_types(id),
                            FOREIGN KEY (window_pk) REFERENCES windows(id)
                        )
                        """
                    )
                    await db.execute(
                        """
                        INSERT INTO task_type_windows_new (
                            id, task_type_id, window_pk,
                            inflight_slots,
                            total_errors, consecutive_errors,
                            daily_quota, remaining_quota,
                            cooldown_until, error_cooldown_until, enabled, deleted,
                            created_at, updated_at
                        )
                        SELECT
                            id, task_type_id, window_pk,
                            0 AS inflight_slots,
                            total_errors, consecutive_errors,
                            daily_quota, remaining_quota,
                            cooldown_until, NULL AS error_cooldown_until, enabled, deleted,
                            created_at, updated_at
                        FROM task_type_windows
                        """
                    )
                    await db.execute("DROP TABLE task_type_windows")
                    await db.execute("ALTER TABLE task_type_windows_new RENAME TO task_type_windows")
                    await db.execute("COMMIT")
                    await db.execute("PRAGMA foreign_keys=ON")
                    # 发生过旧表重建：需要进行一次性 cooldown 字段迁移
                    need_cooldown_migration = True

                # Sora 扩展字段（余额/邀请码）
                columns_to_add = [
                    ("inflight_slots", "INTEGER DEFAULT 0"),
                    ("sora_remaining_count", "INTEGER DEFAULT 0"),
                    ("sora_rate_limit_reached", "BOOLEAN DEFAULT 0"),
                    ("sora_access_resets_in_seconds", "INTEGER DEFAULT 0"),
                    ("sora_invite_code", "TEXT"),
                    ("sora_access_token", "TEXT"),
                    ("sora_access_expires", "TEXT"),
                    ("error_cooldown_until", "TIMESTAMP"),
                ]
                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(db, "task_type_windows", col_name):
                        await db.execute(f"ALTER TABLE task_type_windows ADD COLUMN {col_name} {col_type}")
                        if col_name == "error_cooldown_until":
                            # 新增 error_cooldown_until 列：需要进行一次性 cooldown 字段迁移
                            need_cooldown_migration = True

                # 迁移：旧版 cooldown_until 曾用于“错误冷却”，为避免与“额度重置时间点”混用，
                # 将其复制到 error_cooldown_until，并清空 cooldown_until。
                #
                # 注意：该迁移必须是“一次性的”。新版中 cooldown_until 用于表示“额度重置时间点”
                # （来自 nf/check 的 now + access_resets_in_seconds），不能在每次 ensure_schema 时反复清空。
                if need_cooldown_migration and await self._column_exists(db, "task_type_windows", "error_cooldown_until"):
                    try:
                        await db.execute(
                            """
                            UPDATE task_type_windows
                            SET error_cooldown_until = cooldown_until
                            WHERE error_cooldown_until IS NULL AND cooldown_until IS NOT NULL
                            """
                        )
                        await db.execute("UPDATE task_type_windows SET cooldown_until = NULL WHERE cooldown_until IS NOT NULL")
                    except Exception:
                        pass

            # Step 3: 默认行（不覆盖已有）
            await self._ensure_default_rows(db, config_dict=config_dict)
            await db.commit()

    # ---------- system config ----------
    async def get_system_config(self) -> SystemConfig:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM system_config WHERE id = 1")
            row = await cur.fetchone()
            if row:
                return SystemConfig(**dict(row))
            # 理论上不会走到这里（启动会 ensure 默认行）
            return SystemConfig()

    async def update_system_config(
        self,
        proxy_enabled: Optional[bool] = None,
        proxy_url: Optional[str] = None,
        api_key: Optional[str] = None,
        debug_enabled: Optional[bool] = None,
        log_to_file: Optional[bool] = None,
        stop_accepting_tasks: Optional[bool] = None,
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM system_config WHERE id = 1")
            row = await cur.fetchone()
            current = dict(row) if row else {}

            new_proxy_enabled = proxy_enabled if proxy_enabled is not None else bool(current.get("proxy_enabled", False))
            new_proxy_url = proxy_url if proxy_url is not None else current.get("proxy_url")
            new_api_key = api_key if api_key is not None else str(current.get("api_key", "fpb123456"))
            new_debug_enabled = debug_enabled if debug_enabled is not None else bool(current.get("debug_enabled", False))
            new_log_to_file = log_to_file if log_to_file is not None else bool(current.get("log_to_file", False))
            new_stop_accepting = (
                stop_accepting_tasks if stop_accepting_tasks is not None else bool(current.get("stop_accepting_tasks", False))
            )

            await db.execute(
                """
                INSERT INTO system_config (id, proxy_enabled, proxy_url, api_key, debug_enabled, log_to_file, stop_accepting_tasks, updated_at)
                VALUES (1, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                  proxy_enabled=excluded.proxy_enabled,
                  proxy_url=excluded.proxy_url,
                  api_key=excluded.api_key,
                  debug_enabled=excluded.debug_enabled,
                  log_to_file=excluded.log_to_file,
                  stop_accepting_tasks=excluded.stop_accepting_tasks,
                  updated_at=CURRENT_TIMESTAMP
                """,
                (new_proxy_enabled, new_proxy_url, new_api_key, new_debug_enabled, new_log_to_file, new_stop_accepting),
            )
            await db.commit()

    async def reload_config_to_memory(self) -> SystemConfig:
        """从 DB 读取 system_config，回写到内存 config（用于热更新）。"""
        from .config import config as mem

        syscfg = await self.get_system_config()
        mem.api_key = syscfg.api_key
        mem.set_proxy_enabled_from_db(syscfg.proxy_enabled)
        mem.set_proxy_url_from_db(syscfg.proxy_url)
        mem.set_debug_enabled(syscfg.debug_enabled)
        mem.set_log_to_file_from_db(syscfg.log_to_file)
        try:
            mem.set_stop_accepting_tasks_from_db(syscfg.stop_accepting_tasks)
        except Exception:
            # 兼容旧版 Config（极少数情况下）
            pass
        return syscfg

    # ---------- admin user ----------
    async def get_admin_user(self, username: str) -> Optional[AdminUser]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM admin_users WHERE username = ?", (username,))
            row = await cur.fetchone()
            if row:
                return AdminUser(**dict(row))
            return None

    async def get_first_admin_user(self) -> Optional[AdminUser]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM admin_users ORDER BY id ASC LIMIT 1")
            row = await cur.fetchone()
            if row:
                return AdminUser(**dict(row))
            return None

    async def update_admin_password(self, username: str, new_password_hash: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE admin_users
                SET password_hash = ?, updated_at = CURRENT_TIMESTAMP
                WHERE username = ?
                """,
                (new_password_hash, username),
            )
            await db.commit()

    async def update_admin_username(self, old_username: str, new_username: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE admin_users
                SET username = ?, updated_at = CURRENT_TIMESTAMP
                WHERE username = ?
                """,
                (new_username, old_username),
            )
            await db.commit()

    # ---------- request logs ----------
    async def add_request_log(self, log: RequestLog) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO request_logs (actor, method, path, request_body, response_body, status_code, duration)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (log.actor, log.method, log.path, log.request_body, log.response_body, log.status_code, log.duration),
            )
            await db.commit()

    async def get_request_logs(self, limit: int = 200) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM request_logs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def clear_request_logs(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM request_logs")
            await db.commit()

    # ---------- projects ----------
    async def list_projects(self) -> List[Project]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM projects WHERE deleted = 0 ORDER BY updated_at DESC, id DESC")
            rows = await cur.fetchall()
            return [Project(**dict(r)) for r in rows]

    async def create_project(self, name: str) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "INSERT INTO projects (name, deleted) VALUES (?, 0)",
                (name.strip(),),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def update_project(self, project_id: int, name: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE projects SET name = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (name.strip(), project_id),
            )
            await db.commit()

    async def delete_project(self, project_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE projects SET deleted = 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (project_id,))
            await db.commit()

    # ---------- browsers ----------
    async def list_browsers(self, project_id: int) -> List[FingerprintBrowser]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM browsers WHERE deleted = 0 AND project_id = ? ORDER BY updated_at DESC, id DESC",
                (project_id,),
            )
            rows = await cur.fetchall()
            return [FingerprintBrowser(**dict(r)) for r in rows]

    async def create_browser(
        self,
        project_id: int,
        name: str,
        lan_addr: str,
        vendor: str = "generic",
        access_key: Optional[str] = None,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                INSERT INTO browsers (project_id, name, lan_addr, vendor, access_key, deleted)
                VALUES (?, ?, ?, ?, ?, 0)
                """,
                (project_id, name.strip(), lan_addr.strip(), vendor.strip() or "generic", access_key),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def update_browser(self, browser_id: int, name: str, lan_addr: str, vendor: str, access_key: Optional[str]) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE browsers
                SET name=?, lan_addr=?, vendor=?, access_key=?, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (name.strip(), lan_addr.strip(), vendor.strip() or "generic", access_key, browser_id),
            )
            await db.commit()

    async def delete_browser(self, browser_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE browsers SET deleted = 1, updated_at=CURRENT_TIMESTAMP WHERE id = ?", (browser_id,))
            await db.commit()

    async def get_browser(self, browser_id: int) -> Optional[FingerprintBrowser]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM browsers WHERE id = ?", (browser_id,))
            row = await cur.fetchone()
            return FingerprintBrowser(**dict(row)) if row else None

    # ---------- spaces ----------
    async def list_spaces(self, browser_id: int) -> List[BrowserSpace]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM spaces WHERE deleted = 0 AND browser_id = ? ORDER BY updated_at DESC, id DESC",
                (browser_id,),
            )
            rows = await cur.fetchall()
            return [BrowserSpace(**dict(r)) for r in rows]

    async def create_space(self, browser_id: int, name: str, space_id: str, project_ids: Optional[str] = None) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                INSERT INTO spaces (browser_id, name, space_id, project_ids, deleted)
                VALUES (?, ?, ?, ?, 0)
                """,
                (browser_id, name.strip(), space_id.strip(), (project_ids or "").strip() or None),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def update_space(self, space_pk: int, name: str, space_id: str, project_ids: Optional[str] = None) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE spaces SET name=?, space_id=?, project_ids=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (name.strip(), space_id.strip(), (project_ids or "").strip() or None, space_pk),
            )
            await db.commit()

    async def delete_space(self, space_pk: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE spaces SET deleted = 1, updated_at=CURRENT_TIMESTAMP WHERE id = ?", (space_pk,))
            await db.commit()

    async def get_space(self, space_pk: int) -> Optional[BrowserSpace]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM spaces WHERE id = ?", (space_pk,))
            row = await cur.fetchone()
            return BrowserSpace(**dict(row)) if row else None

    # ---------- windows ----------
    async def list_windows(self, space_pk: int) -> List[WindowInfo]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT * FROM windows
                WHERE deleted = 0 AND space_pk = ?
                ORDER BY (window_sort_num IS NULL) ASC, window_sort_num ASC, window_name ASC, id ASC
                """,
                (space_pk,),
            )
            rows = await cur.fetchall()
            result: List[WindowInfo] = []
            for r in rows:
                d = dict(r)
                if d.get("raw_json"):
                    try:
                        d["raw"] = json.loads(d["raw_json"])
                    except Exception:
                        d["raw"] = None
                d.pop("raw_json", None)
                result.append(WindowInfo(**d))
            return result

    async def upsert_windows(self, space_pk: int, windows: List[Dict[str, Any]]) -> int:
        """把同步到的窗口信息保存到 DB（按 space_pk+window_key 唯一 upsert）。

        返回：本次写入/更新的行数（粗略统计）。
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA foreign_keys=ON")
            affected = 0
            for w in windows:
                window_key = str(w.get("window_key") or w.get("id") or w.get("dirId") or w.get("name") or "").strip()
                if not window_key:
                    continue
                # window_sort_num: 优先取标准 snake_case，其次取 Roxy 的 camelCase，最后从 raw 里兜底
                window_sort_num_raw = (
                    w.get("window_sort_num")
                    if w.get("window_sort_num") is not None
                    else (w.get("windowSortNum") if w.get("windowSortNum") is not None else None)
                )
                if window_sort_num_raw is None:
                    raw_obj = w.get("raw")
                    if isinstance(raw_obj, dict):
                        window_sort_num_raw = raw_obj.get("windowSortNum")
                try:
                    window_sort_num = int(window_sort_num_raw) if window_sort_num_raw not in (None, "", "-") else None
                except Exception:
                    window_sort_num = None

                window_name = str(w.get("window_name") or w.get("name") or window_key).strip()
                platform_account = (w.get("platform_account") or w.get("account") or w.get("username"))
                platform_url = (w.get("platform_url") or w.get("url"))
                # proxy_id: 优先从 raw 中提取（Roxy: proxyInfo.moduleId -> minimal_raw.proxyModuleId）
                proxy_id_raw = None
                raw_obj = w.get("raw")
                if isinstance(raw_obj, dict):
                    proxy_id_raw = (
                        raw_obj.get("proxyModuleId")
                        if raw_obj.get("proxyModuleId") is not None
                        else (raw_obj.get("proxy_module_id") if raw_obj.get("proxy_module_id") is not None else raw_obj.get("moduleId"))
                    )
                    if proxy_id_raw is None:
                        proxy_info = raw_obj.get("proxyInfo")
                        if isinstance(proxy_info, dict):
                            proxy_id_raw = proxy_info.get("moduleId")
                if proxy_id_raw is None:
                    proxy_id_raw = w.get("proxy_id") if w.get("proxy_id") is not None else w.get("proxyModuleId")
                try:
                    proxy_id = int(proxy_id_raw) if proxy_id_raw not in (None, "", "-") else None
                except Exception:
                    proxy_id = None
                proxy_addr = (w.get("proxy_addr") or w.get("proxy") or w.get("proxy_url"))
                proxy_country = (w.get("proxy_country") or w.get("country"))
                proxy_expire_at = (w.get("proxy_expire_at") or w.get("expire_at") or w.get("proxy_expire"))
                enabled = 1 if bool(w.get("enabled", True)) else 0
                deleted = 1 if bool(w.get("deleted", False)) else 0
                raw_json = json.dumps(w, ensure_ascii=False)

                await db.execute(
                    """
                    INSERT INTO windows (
                        space_pk, window_key, window_sort_num, window_name,
                        platform_account, platform_url,
                        proxy_id,
                        proxy_addr, proxy_country, proxy_expire_at,
                        enabled, deleted, raw_json, synced_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(space_pk, window_key) DO UPDATE SET
                        window_sort_num=excluded.window_sort_num,
                        window_name=excluded.window_name,
                        platform_account=excluded.platform_account,
                        platform_url=excluded.platform_url,
                        -- 约定：同步窗口时，若新数据未解析到 proxy_id，则保留已有 proxy_id
                        proxy_id=COALESCE(excluded.proxy_id, windows.proxy_id),
                        proxy_addr=excluded.proxy_addr,
                        proxy_country=excluded.proxy_country,
                        proxy_expire_at=excluded.proxy_expire_at,
                        enabled=excluded.enabled,
                        -- 约定：本地标记删除后，不因“同步窗口”而被覆盖为未删除
                        deleted=CASE WHEN windows.deleted = 1 THEN 1 ELSE excluded.deleted END,
                        raw_json=excluded.raw_json,
                        synced_at=CURRENT_TIMESTAMP,
                        updated_at=CURRENT_TIMESTAMP
                    """,
                    (
                        space_pk,
                        window_key,
                        window_sort_num,
                        window_name,
                        platform_account,
                        platform_url,
                        proxy_id,
                        proxy_addr,
                        proxy_country,
                        proxy_expire_at,
                        enabled,
                        deleted,
                        raw_json,
                    ),
                )
                affected += 1
            await db.commit()
            return affected

    async def count_proxy_bindings(self, space_pk: int) -> Dict[int, int]:
        """统计某个空间下：每个 proxy_id 被多少个“本地未删除窗口”绑定。

        说明：
        - 优先使用 windows.proxy_id 统计（选择代理 moduleId 的场景）。
        - 若 windows.proxy_id 为空/0（常见于“自定义代理”），则尝试用 windows.proxy_addr 匹配本地 proxies：
          - proxies.last_ip == windows.proxy_addr
          - 或 proxies.host:port == windows.proxy_addr
          - 或 proxies.host == windows.proxy_addr
        - 只要能匹配到 1 个本地 proxy_id，就按该 proxy_id 计数；否则归到 0（不计入任何代理）。
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                WITH win AS (
                  SELECT
                    id,
                    space_pk,
                    COALESCE(proxy_id, 0) AS pid,
                    TRIM(COALESCE(proxy_addr, '')) AS proxy_addr
                  FROM windows
                  WHERE deleted = 0 AND space_pk = ?
                ),
                matched AS (
                  SELECT
                    w.id AS window_id,
                    MIN(p.proxy_id) AS matched_proxy_id
                  FROM win w
                  JOIN proxies p
                    ON p.space_pk = w.space_pk
                   AND p.deleted = 0
                   AND w.pid = 0
                   AND w.proxy_addr <> ''
                   AND (
                     (p.last_ip IS NOT NULL AND TRIM(p.last_ip) <> '' AND TRIM(p.last_ip) = w.proxy_addr)
                     OR ((p.host || ':' || p.port) IS NOT NULL AND TRIM(p.host || ':' || p.port) = w.proxy_addr)
                     OR (p.host IS NOT NULL AND TRIM(p.host) <> '' AND TRIM(p.host) = w.proxy_addr)
                   )
                  GROUP BY w.id
                )
                SELECT
                  COALESCE(NULLIF(win.pid, 0), matched.matched_proxy_id, 0) AS proxy_id,
                  COUNT(*) AS cnt
                FROM win
                LEFT JOIN matched ON matched.window_id = win.id
                GROUP BY COALESCE(NULLIF(win.pid, 0), matched.matched_proxy_id, 0)
                """,
                (int(space_pk),),
            )
            rows = await cur.fetchall()
            out: Dict[int, int] = {}
            for r in rows:
                try:
                    pid = int(r["proxy_id"] or 0)
                except Exception:
                    pid = 0
                try:
                    out[pid] = int(r["cnt"] or 0)
                except Exception:
                    out[pid] = 0
            return out

    async def get_window(self, window_pk: int) -> Optional[WindowInfo]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM windows WHERE id = ?", (window_pk,))
            row = await cur.fetchone()
            if not row:
                return None
            d = dict(row)
            if d.get("raw_json"):
                try:
                    d["raw"] = json.loads(d["raw_json"])
                except Exception:
                    d["raw"] = None
            d.pop("raw_json", None)
            return WindowInfo(**d)

    async def delete_window_by_key(self, *, space_pk: int, window_key: str) -> int:
        """本地标记删除窗口（不物理删除）。

        返回：影响行数（0 表示未找到该窗口或已被删除）。
        """
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "UPDATE windows SET deleted = 1, updated_at=CURRENT_TIMESTAMP WHERE space_pk = ? AND window_key = ? AND deleted = 0",
                (int(space_pk), str(window_key).strip()),
            )
            await db.commit()
            try:
                return int(cur.rowcount or 0)
            except Exception:
                return 0

    async def update_window_proxy_id(self, *, space_pk: int, window_key: str, proxy_id: Optional[int]) -> int:
        """仅更新本地窗口记录的 proxy_id（用于 UI 立即生效的“当前代理”显示/统计）。

        返回：影响行数（0 表示未找到或已删除）。
        """
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                UPDATE windows
                SET proxy_id = ?, updated_at=CURRENT_TIMESTAMP
                WHERE space_pk = ? AND window_key = ? AND deleted = 0
                """,
                (proxy_id, int(space_pk), str(window_key).strip()),
            )
            await db.commit()
            try:
                return int(cur.rowcount or 0)
            except Exception:
                return 0

    async def resolve_space_pk_for_window(self, *, space_id: str, window_key: str) -> Optional[int]:
        """根据 (workspaceId=space_id, window_key) 解析本地 space_pk。

        说明：
        - tasks 执行器侧通常只有 space_id(window workspaceId) + window_key(dirId)，没有本地 spaces.id。
        - 本方法优先通过 windows+spaces join 精确定位；兜底仅按 space_id 找最近的 space_pk。
        """
        sid = str(space_id or "").strip()
        wk = str(window_key or "").strip()
        if not sid:
            return None
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if wk:
                cur = await db.execute(
                    """
                    SELECT w.space_pk AS space_pk
                    FROM windows w
                    JOIN spaces s ON s.id = w.space_pk
                    WHERE s.deleted = 0
                      AND w.deleted = 0
                      AND s.space_id = ?
                      AND w.window_key = ?
                    ORDER BY w.id DESC
                    LIMIT 1
                    """,
                    (sid, wk),
                )
                row = await cur.fetchone()
                # aiosqlite.Row（sqlite3.Row）不是 dict，没有 .get()
                if row and dict(row).get("space_pk") is not None:
                    try:
                        return int(row["space_pk"])
                    except Exception:
                        pass

            cur2 = await db.execute(
                "SELECT id FROM spaces WHERE deleted = 0 AND space_id = ? ORDER BY id DESC LIMIT 1",
                (sid,),
            )
            row2 = await cur2.fetchone()
            if row2 and dict(row2).get("id") is not None:
                try:
                    return int(row2["id"])
                except Exception:
                    return None
            return None

    # ---------- proxies ----------
    async def list_proxies(self, space_pk: int) -> List[ProxyInfo]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT * FROM proxies
                WHERE deleted = 0 AND space_pk = ?
                ORDER BY updated_at DESC, id DESC
                """,
                (int(space_pk),),
            )
            rows = await cur.fetchall()
            result: List[ProxyInfo] = []
            for r in rows:
                d = dict(r)
                if d.get("raw_json"):
                    try:
                        d["raw"] = json.loads(d["raw_json"])
                    except Exception:
                        d["raw"] = None
                d.pop("raw_json", None)
                result.append(ProxyInfo(**d))
            return result

    async def upsert_proxies(self, space_pk: int, proxies: List[Dict[str, Any]]) -> int:
        """把同步到的代理列表保存到 DB（按 space_pk+proxy_id 唯一 upsert）。"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA foreign_keys=ON")
            affected = 0
            incoming_ids: List[int] = []
            for p in (proxies or []):
                if not isinstance(p, dict):
                    continue
                proxy_id_raw = p.get("id") if p.get("id") is not None else p.get("proxy_id")
                try:
                    proxy_id = int(proxy_id_raw)
                except Exception:
                    continue
                incoming_ids.append(int(proxy_id))

                # 过期时间字段：不同版本/不同代理源字段名可能不同，尽量兼容
                expire_at = (
                    p.get("expireAt")
                    or p.get("expire_at")
                    or p.get("expireTime")
                    or p.get("expire_time")
                    or p.get("expirationTime")
                    or p.get("expiration_time")
                    or p.get("endTime")
                    or p.get("end_time")
                    or p.get("dueTime")
                    or p.get("due_time")
                )

                ip_type = (p.get("ipType") or p.get("ip_type"))
                protocol = (p.get("protocol") or p.get("proxyCategory") or p.get("proxy_category"))
                host = p.get("host")
                port = p.get("port")
                proxy_username = p.get("proxyUserName") or p.get("proxy_username")
                proxy_password = p.get("proxyPassword") or p.get("proxy_password")
                refresh_url = p.get("refreshUrl") or p.get("refresh_url")
                remark = p.get("remark") or p.get("remarks") or p.get("proxyRemarks")

                check_status = p.get("checkStatus")
                check_channel = p.get("checkChannel")
                check_channel_value = p.get("checkChannelValue")
                last_ip = p.get("lastIp")
                last_country = p.get("lastCountry")
                last_state = p.get("lastState")
                last_city = p.get("lastCity")
                check_time = p.get("checkTime")
                create_time = p.get("createTime")
                update_time = p.get("updateTime")

                raw_json = json.dumps(p, ensure_ascii=False)
                await db.execute(
                    """
                    INSERT INTO proxies (
                        space_pk, proxy_id,
                        expire_at,
                        ip_type, protocol, host, port,
                        proxy_username, proxy_password, refresh_url, remark,
                        check_status, check_channel, check_channel_value,
                        last_ip, last_country, last_state, last_city,
                        check_time, create_time, update_time,
                        deleted, raw_json, synced_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(space_pk, proxy_id) DO UPDATE SET
                        expire_at=excluded.expire_at,
                        ip_type=excluded.ip_type,
                        protocol=excluded.protocol,
                        host=excluded.host,
                        port=excluded.port,
                        proxy_username=excluded.proxy_username,
                        proxy_password=excluded.proxy_password,
                        refresh_url=excluded.refresh_url,
                        remark=excluded.remark,
                        check_status=excluded.check_status,
                        check_channel=excluded.check_channel,
                        check_channel_value=excluded.check_channel_value,
                        last_ip=excluded.last_ip,
                        last_country=excluded.last_country,
                        last_state=excluded.last_state,
                        last_city=excluded.last_city,
                        check_time=excluded.check_time,
                        create_time=excluded.create_time,
                        update_time=excluded.update_time,
                        deleted=excluded.deleted,
                        raw_json=excluded.raw_json,
                        synced_at=CURRENT_TIMESTAMP,
                        updated_at=CURRENT_TIMESTAMP
                    """,
                    (
                        int(space_pk),
                        int(proxy_id),
                        str(expire_at).strip() if expire_at is not None else None,
                        str(ip_type).strip() if ip_type is not None else None,
                        str(protocol).strip() if protocol is not None else None,
                        str(host).strip() if host is not None else None,
                        str(port).strip() if port is not None else None,
                        str(proxy_username).strip() if proxy_username is not None else None,
                        str(proxy_password).strip() if proxy_password is not None else None,
                        str(refresh_url).strip() if refresh_url is not None else None,
                        str(remark).strip() if remark is not None else None,
                        int(check_status) if str(check_status or "").strip().isdigit() else None,
                        str(check_channel).strip() if check_channel is not None else None,
                        str(check_channel_value).strip() if check_channel_value is not None else None,
                        str(last_ip).strip() if last_ip is not None else None,
                        str(last_country).strip() if last_country is not None else None,
                        str(last_state).strip() if last_state is not None else None,
                        str(last_city).strip() if last_city is not None else None,
                        str(check_time).strip() if check_time is not None else None,
                        str(create_time).strip() if create_time is not None else None,
                        str(update_time).strip() if update_time is not None else None,
                        raw_json,
                    ),
                )
                affected += 1

            # 同步策略：以同步结果为准（全量覆盖本地可见代理列表）
            # - 同步返回存在的代理：deleted=0（由 upsert 写入/更新）
            # - 同步未返回的代理：本地标记 deleted=1（让 UI 不再展示）
            #
            # 注意：如果同步结果为空列表，也认为该空间当前无代理，清空本地展示。
            try:
                uniq = sorted(set(int(x) for x in (incoming_ids or []) if isinstance(x, int)))
                if uniq:
                    placeholders = ",".join(["?"] * len(uniq))
                    await db.execute(
                        f"""
                        UPDATE proxies
                        SET deleted = 1, updated_at = CURRENT_TIMESTAMP
                        WHERE space_pk = ?
                          AND deleted = 0
                          AND proxy_id NOT IN ({placeholders})
                        """,
                        (int(space_pk), *uniq),
                    )
                else:
                    await db.execute(
                        """
                        UPDATE proxies
                        SET deleted = 1, updated_at = CURRENT_TIMESTAMP
                        WHERE space_pk = ? AND deleted = 0
                        """,
                        (int(space_pk),),
                    )
            except Exception:
                pass
            await db.commit()
            return affected

    async def delete_proxy(self, *, space_pk: int, proxy_id: int) -> int:
        """本地删除某个代理（仅标记 deleted=1，不影响指纹浏览器侧）。"""
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                UPDATE proxies
                SET deleted = 1, updated_at = CURRENT_TIMESTAMP
                WHERE space_pk = ? AND proxy_id = ?
                """,
                (int(space_pk), int(proxy_id)),
            )
            await db.commit()
            return int(cur.rowcount or 0)

    # ---------- task types ----------
    async def list_task_types(self) -> List[TaskType]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM task_types WHERE deleted = 0 ORDER BY updated_at DESC, id DESC")
            rows = await cur.fetchall()
            return [TaskType(**dict(r)) for r in rows]

    
    async def list_task_types_public(self) -> List[TaskTypePublic]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT id, name, code,timeout_seconds, created_at,enabled FROM task_types WHERE deleted = 0 ORDER BY updated_at DESC, id DESC")
            rows = await cur.fetchall()
            return [TaskTypePublic(**dict(r)) for r in rows]

    async def create_task_type(
        self,
        name: str,
        code: str,
        concurrency: int,
        continuous_error_threshold: int,
        timeout_seconds: int,
        create_task_handler: Optional[str] = None,
        refresh_quota_handler: Optional[str] = None,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                INSERT INTO task_types (
                  name, code, concurrency, continuous_error_threshold, timeout_seconds,
                  create_task_handler, refresh_quota_handler,
                  enabled, deleted
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0)
                """,
                (
                    name.strip(),
                    code.strip(),
                    int(concurrency),
                    int(continuous_error_threshold),
                    int(timeout_seconds),
                    (create_task_handler or "").strip() or None,
                    (refresh_quota_handler or "").strip() or None,
                ),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def update_task_type(
        self,
        task_type_id: int,
        name: str,
        code: str,
        concurrency: int,
        continuous_error_threshold: int,
        timeout_seconds: int,
        create_task_handler: Optional[str],
        refresh_quota_handler: Optional[str],
        enabled: bool,
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT code FROM task_types WHERE id=? AND deleted=0", (task_type_id,))
            row = await cur.fetchone()
            if not row:
                raise ValueError("任务类型不存在")

            old_code = str(row["code"] or "").strip()
            new_code = (code or "").strip()
            if not new_code:
                raise ValueError("code 不能为空")

            if new_code != old_code:
                cur = await db.execute(
                    "SELECT COUNT(*) AS c FROM task_types WHERE code=? AND id<>?",
                    (new_code, task_type_id),
                )
                if int((await cur.fetchone())["c"]) > 0:
                    raise ValueError("英文名称（code）已存在")

            await db.execute(
                """
                UPDATE task_types
                SET name=?, code=?, concurrency=?, continuous_error_threshold=?, timeout_seconds=?,
                    create_task_handler=?, refresh_quota_handler=?,
                    enabled=?, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (
                    name.strip(),
                    new_code,
                    int(concurrency),
                    int(continuous_error_threshold),
                    int(timeout_seconds),
                    (create_task_handler or "").strip() or None,
                    (refresh_quota_handler or "").strip() or None,
                    1 if enabled else 0,
                    task_type_id,
                ),
            )

            # 同步历史任务的 task_type_code，避免改 code 后历史记录“断链”
            if new_code != old_code and old_code:
                await db.execute("UPDATE tasks SET task_type_code=? WHERE task_type_code=?", (new_code, old_code))
            await db.commit()

    async def get_task_type_window_context(self, mapping_id: int) -> Optional[Dict[str, Any]]:
        """按 mapping_id 取“刷新额度/调度”所需的上下文（join 后 dict）。"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT
                  m.*,
                  t.id AS task_type_id,
                  t.code AS task_code,
                  t.create_task_handler,
                  t.refresh_quota_handler,
                  w.window_key,
                  w.window_name,
                  w.platform_account,
                  s.space_id AS space_id,
                  b.vendor,
                  b.lan_addr,
                  b.access_key
                FROM task_type_windows m
                JOIN task_types t ON m.task_type_id = t.id
                JOIN windows w ON m.window_pk = w.id
                JOIN spaces s ON w.space_pk = s.id
                JOIN browsers b ON s.browser_id = b.id
                WHERE m.id = ? AND m.deleted = 0
                LIMIT 1
                """,
                (int(mapping_id),),
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    # ---------- auto refresh error logs ----------
    async def add_auto_refresh_error_log(self, log: AutoRefreshErrorLog) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                INSERT INTO auto_refresh_error_logs (
                  mapping_id, task_type_id, task_code,
                  window_pk, window_name, platform_account,
                  error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(log.mapping_id),
                    int(log.task_type_id) if log.task_type_id is not None else None,
                    (log.task_code or "").strip() or None,
                    int(log.window_pk) if log.window_pk is not None else None,
                    (log.window_name or "").strip() or None,
                    (log.platform_account or "").strip() or None,
                    str(log.error_message or "").strip(),
                ),
            )
            await db.commit()
            return int(cur.lastrowid or 0)

    async def list_auto_refresh_error_logs(
        self,
        limit: int = 200,
        offset: int = 0,
        task_type_id: Optional[int] = None,
        mapping_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        lim = max(1, min(500, int(limit or 200)))
        off = max(0, int(offset or 0))
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if mapping_id is not None:
                cur = await db.execute(
                    """
                    SELECT * FROM auto_refresh_error_logs
                    WHERE mapping_id = ?
                    ORDER BY id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (int(mapping_id), lim, off),
                )
            elif task_type_id is None:
                cur = await db.execute(
                    """
                    SELECT * FROM auto_refresh_error_logs
                    ORDER BY id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (lim, off),
                )
            else:
                cur = await db.execute(
                    """
                    SELECT * FROM auto_refresh_error_logs
                    WHERE task_type_id = ?
                    ORDER BY id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (int(task_type_id), lim, off),
                )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def delete_task_type(self, task_type_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE task_types SET deleted=1, updated_at=CURRENT_TIMESTAMP WHERE id=?", (task_type_id,))
            await db.commit()

    async def get_task_type_by_code(self, code: str) -> Optional[TaskType]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM task_types WHERE code=? AND deleted=0", (code.strip(),))
            row = await cur.fetchone()
            return TaskType(**dict(row)) if row else None

    async def get_task_type(self, task_type_id: int) -> Optional[TaskType]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM task_types WHERE id=? AND deleted=0", (task_type_id,))
            row = await cur.fetchone()
            return TaskType(**dict(row)) if row else None

    # ---------- task type windows mapping ----------
    async def list_task_type_windows(self, task_type_id: int) -> List[Dict[str, Any]]:
        """返回映射 + 窗口基本信息（便于 UI 展示）。"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT
                  m.*,
                  w.window_name,
                  w.window_key,
                  w.window_sort_num,
                  w.platform_account,
                  w.platform_url,
                  w.space_pk,
                  w.proxy_id,
                  w.proxy_addr,
                  w.proxy_country,
                  w.proxy_expire_at,
                  w.enabled AS window_enabled,
                  s.space_id AS space_id,
                  s.name AS space_name,
                  b.name AS browser_name
                FROM task_type_windows m
                JOIN windows w ON m.window_pk = w.id
                JOIN spaces s ON w.space_pk = s.id
                JOIN browsers b ON s.browser_id = b.id
                WHERE m.deleted = 0 AND m.task_type_id = ?
                -- 绑定窗口排序：按 id 倒序，避免刷新额度/邀请码后因 updated_at 变化导致列表重排
                ORDER BY m.id DESC
                """,
                (task_type_id,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def add_task_type_windows(
        self,
        task_type_id: int,
        window_pks: List[int],
        daily_quota: int,
        remaining_quota: int,
        enabled: bool = True,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            affected = 0
            for wid in window_pks:
                await db.execute(
                    """
                    INSERT INTO task_type_windows (
                      task_type_id, window_pk,
                      daily_quota, remaining_quota,
                      enabled, deleted, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
                    ON CONFLICT(task_type_id, window_pk) DO UPDATE SET
                      daily_quota=excluded.daily_quota,
                      remaining_quota=excluded.remaining_quota,
                      enabled=excluded.enabled,
                      deleted=0,
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    (
                        task_type_id,
                        int(wid),
                        int(daily_quota),
                        int(remaining_quota),
                        1 if enabled else 0,
                    ),
                )
                affected += 1
            await db.commit()
            return affected

    async def update_task_type_window(
        self,
        mapping_id: int,
        enabled: Optional[bool] = None,
        deleted: Optional[bool] = None,
        daily_quota: Optional[int] = None,
        remaining_quota: Optional[int] = None,
        sora_remaining_count: Optional[int] = None,
        sora_rate_limit_reached: Optional[bool] = None,
        sora_access_resets_in_seconds: Optional[int] = None,
        sora_invite_code: Optional[str] = None,
        sora_access_token: Optional[str] = None,
        sora_access_expires: Optional[str] = None,
        cooldown_until: Optional[str] = None,  # ISO string or None
        error_cooldown_until: Optional[str] = None,  # ISO string or None
        total_errors: Optional[int] = None,
        consecutive_errors: Optional[int] = None,
    ) -> None:
        updates: List[str] = []
        params: List[Any] = []

        def _set(col: str, val: Any) -> None:
            updates.append(f"{col}=?")
            params.append(val)

        if enabled is not None:
            _set("enabled", 1 if enabled else 0)
        if deleted is not None:
            _set("deleted", 1 if deleted else 0)
        if daily_quota is not None:
            _set("daily_quota", int(daily_quota))
        if remaining_quota is not None:
            _set("remaining_quota", int(remaining_quota))
        if sora_remaining_count is not None:
            _set("sora_remaining_count", int(sora_remaining_count))
        if sora_rate_limit_reached is not None:
            _set("sora_rate_limit_reached", 1 if bool(sora_rate_limit_reached) else 0)
        if sora_access_resets_in_seconds is not None:
            _set("sora_access_resets_in_seconds", int(sora_access_resets_in_seconds))
        if sora_invite_code is not None:
            _set("sora_invite_code", (sora_invite_code or "").strip() or None)
        if sora_access_token is not None:
            _set("sora_access_token", (sora_access_token or "").strip() or None)
        if sora_access_expires is not None:
            _set("sora_access_expires", (sora_access_expires or "").strip() or None)
        if cooldown_until is not None:
            _set("cooldown_until", cooldown_until if cooldown_until else None)
        if error_cooldown_until is not None:
            _set("error_cooldown_until", error_cooldown_until if error_cooldown_until else None)
        if total_errors is not None:
            _set("total_errors", int(total_errors))
        if consecutive_errors is not None:
            _set("consecutive_errors", int(consecutive_errors))

        if not updates:
            return

        params.append(mapping_id)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                f"UPDATE task_type_windows SET {', '.join(updates)}, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                params,
            )
            await db.commit()

    async def pick_and_reserve_window_for_task(self, task_type_code: str) -> Optional[Dict[str, Any]]:
        """挑选 1 个窗口并原子预占 1 个并发槽位（一步完成）。

        目标：
        - 避免 TaskService “先查一批候选 -> 再循环 try_reserve” 的高并发抖动
        - 把“排序挑选 + 并发预占”压缩为单次 DB 写入（单条 SQL）
        - remaining_quota 只代表额度：remaining_quota == 3 表示不可用；并发限制以 task_types.concurrency 为准

        返回：
        - 成功：返回 join 后的窗口信息 dict（与 list_available_windows_for_pick 字段一致）
        - 失败：返回 None
        """
        code = (task_type_code or "").strip()
        if not code:
            return None

        # SQLite 高并发下可能出现 "database is locked"；做少量快速重试
        for _ in range(3):
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    db.row_factory = aiosqlite.Row
                    await db.execute("PRAGMA foreign_keys=ON")
                    # busy_timeout 可以显著减少高并发瞬时锁冲突失败
                    try:
                        await db.execute("PRAGMA busy_timeout=3000")
                    except Exception:
                        pass
                    # 兼容旧版 SQLite：部分版本不支持「CTE 内含 UPDATE/RETURNING」语法；
                    # 这里改为同一事务中：先挑选 mapping，再条件 UPDATE 预占并发槽位，最后 SELECT 详情。
                    await db.execute("BEGIN IMMEDIATE")

                    # Step 1: 按排序挑选 1 个 mapping_id，并取出本次更新需要的并发/阈值参数
                    cur = await db.execute(
                        """
                        SELECT
                          m.id AS mapping_id,
                          t.concurrency AS task_concurrency,
                          t.continuous_error_threshold
                        FROM task_types t
                        JOIN task_type_windows m ON m.task_type_id = t.id
                        JOIN windows w ON m.window_pk = w.id
                        JOIN spaces s ON w.space_pk = s.id
                        JOIN browsers b ON s.browser_id = b.id
                        WHERE t.deleted=0 AND t.enabled=1
                          AND t.code=?
                          AND m.deleted=0 AND m.enabled=1
                          AND w.deleted=0 AND w.enabled=1
                          AND (
                            (m.remaining_quota > 2)
                            OR (m.cooldown_until IS NOT NULL AND m.cooldown_until <= datetime('now', '+5 minutes'))
                          )
                          AND (m.error_cooldown_until IS NULL OR m.error_cooldown_until <= CURRENT_TIMESTAMP)
                          AND (m.consecutive_errors < t.continuous_error_threshold)
                          AND (COALESCE(m.inflight_slots, 0) < t.concurrency)
                        ORDER BY m.consecutive_errors ASC, m.updated_at ASC, m.remaining_quota DESC
                        LIMIT 1
                        """,
                        (code,),
                    )
                    picked = await cur.fetchone()
                    if not picked:
                        await db.execute("ROLLBACK")
                        return None

                    mapping_id = int(picked["mapping_id"])
                    task_concurrency = max(1, int(picked["task_concurrency"] or 1))
                    threshold = max(1, int(picked["continuous_error_threshold"] or 1))

                    # Step 2: 条件 UPDATE，确保并发/健康度/额度约束仍成立（在同一事务内保证原子性）
                    cur2 = await db.execute(
                        """
                        UPDATE task_type_windows
                        SET inflight_slots = COALESCE(inflight_slots, 0) + 1,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                          AND deleted = 0 AND enabled = 1
                          AND (consecutive_errors < ?)
                          AND (COALESCE(inflight_slots, 0) < ?)
                          AND (
                            (remaining_quota > 2)
                            OR (cooldown_until IS NOT NULL AND cooldown_until <= datetime('now', '+5 minutes'))
                          )
                          AND (error_cooldown_until IS NULL OR error_cooldown_until <= CURRENT_TIMESTAMP)
                        """,
                        (mapping_id, threshold, task_concurrency),
                    )
                    if int(cur2.rowcount or 0) <= 0:
                        # 理论上在 IMMEDIATE 事务内不太会发生，但为了稳健性（以及未来条件调整）保留兜底
                        await db.execute("ROLLBACK")
                        continue

                    # Step 3: 返回 join 后的上下文字段（与旧实现一致）
                    cur3 = await db.execute(
                        """
                        SELECT
                          m.*,
                          t.code AS task_code,
                          t.concurrency AS task_concurrency,
                          t.continuous_error_threshold,
                          t.timeout_seconds,
                          t.create_task_handler,
                          w.window_key,
                          w.window_name,
                          w.platform_account,
                          w.platform_url,
                          s.id AS space_pk,
                          s.space_id AS space_id,
                          b.id AS browser_pk,
                          b.lan_addr,
                          b.vendor,
                          b.access_key
                        FROM task_type_windows m
                        JOIN task_types t ON m.task_type_id = t.id
                        JOIN windows w ON m.window_pk = w.id
                        JOIN spaces s ON w.space_pk = s.id
                        JOIN browsers b ON s.browser_id = b.id
                        WHERE m.id = ?
                        LIMIT 1
                        """,
                        (mapping_id,),
                    )
                    row = await cur3.fetchone()
                    await db.commit()
                    return dict(row) if row else None
            except Exception as e:
                # 仅对锁竞争做轻量重试，其他异常直接抛出便于定位
                if "database is locked" in str(e).lower():
                    import asyncio

                    await asyncio.sleep(0.01)
                    continue
                raise
        return None

    async def reserve_mapping_for_task(self, task_type_code: str, mapping_id: int) -> Optional[Dict[str, Any]]:
        """按指定 mapping_id（task_type_windows.id）预占 1 个并发槽位，并返回窗口上下文。

        说明：
        - 用于“指定窗口运行任务”的调试/测试场景（管理端页面）。
        - 约束与 pick_and_reserve_window_for_task 保持一致（额度/冷却/错误熔断/并发上限/启用状态）。
        """
        code = (task_type_code or "").strip()
        mid = int(mapping_id)
        if not code or mid <= 0:
            return None

        for _ in range(3):
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    db.row_factory = aiosqlite.Row
                    await db.execute("PRAGMA foreign_keys=ON")
                    try:
                        await db.execute("PRAGMA busy_timeout=3000")
                    except Exception:
                        pass
                    await db.execute("BEGIN IMMEDIATE")

                    # Step 1: 校验该 mapping 属于 task_type 且当前可用，并取并发/阈值参数
                    cur = await db.execute(
                        """
                        SELECT
                          m.id AS mapping_id,
                          t.concurrency AS task_concurrency,
                          t.continuous_error_threshold
                        FROM task_types t
                        JOIN task_type_windows m ON m.task_type_id = t.id
                        JOIN windows w ON m.window_pk = w.id
                        WHERE t.deleted=0 AND t.enabled=1
                          AND t.code=?
                          AND m.id=?
                          AND m.deleted=0 AND m.enabled=1
                          AND w.deleted=0 AND w.enabled=1
                          AND (
                            (m.remaining_quota > 2)
                            OR (m.cooldown_until IS NOT NULL AND m.cooldown_until <= datetime('now', '+5 minutes'))
                          )
                          AND (m.error_cooldown_until IS NULL OR m.error_cooldown_until <= CURRENT_TIMESTAMP)
                          AND (m.consecutive_errors < t.continuous_error_threshold)
                          AND (COALESCE(m.inflight_slots, 0) < t.concurrency)
                        LIMIT 1
                        """,
                        (code, mid),
                    )
                    picked = await cur.fetchone()
                    if not picked:
                        await db.execute("ROLLBACK")
                        return None

                    task_concurrency = max(1, int(picked["task_concurrency"] or 1))
                    threshold = max(1, int(picked["continuous_error_threshold"] or 1))

                    # Step 2: 预占并发槽位（同事务原子保证）
                    cur2 = await db.execute(
                        """
                        UPDATE task_type_windows
                        SET inflight_slots = COALESCE(inflight_slots, 0) + 1,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                          AND deleted = 0 AND enabled = 1
                          AND (consecutive_errors < ?)
                          AND (COALESCE(inflight_slots, 0) < ?)
                          AND (
                            (remaining_quota > 2)
                            OR (cooldown_until IS NOT NULL AND cooldown_until <= datetime('now', '+5 minutes'))
                          )
                          AND (error_cooldown_until IS NULL OR error_cooldown_until <= CURRENT_TIMESTAMP)
                        """,
                        (mid, threshold, task_concurrency),
                    )
                    if int(cur2.rowcount or 0) <= 0:
                        await db.execute("ROLLBACK")
                        continue

                    # Step 3: 返回上下文（字段与 pick_and_reserve_window_for_task 一致）
                    cur3 = await db.execute(
                        """
                        SELECT
                          m.*,
                          t.code AS task_code,
                          t.concurrency AS task_concurrency,
                          t.continuous_error_threshold,
                          t.timeout_seconds,
                          t.create_task_handler,
                          w.window_key,
                          w.window_name,
                          w.platform_account,
                          w.platform_url,
                          s.id AS space_pk,
                          s.space_id AS space_id,
                          b.id AS browser_pk,
                          b.lan_addr,
                          b.vendor,
                          b.access_key
                        FROM task_type_windows m
                        JOIN task_types t ON m.task_type_id = t.id
                        JOIN windows w ON m.window_pk = w.id
                        JOIN spaces s ON w.space_pk = s.id
                        JOIN browsers b ON s.browser_id = b.id
                        WHERE m.id = ?
                        LIMIT 1
                        """,
                        (mid,),
                    )
                    row = await cur3.fetchone()
                    await db.commit()
                    return dict(row) if row else None
            except Exception as e:
                if "database is locked" in str(e).lower():
                    import asyncio

                    await asyncio.sleep(0.01)
                    continue
                raise
        return None

    async def reserve_window_for_task(self, task_type_code: str, window_pk: int) -> Optional[Dict[str, Any]]:
        """按指定 window_pk 预占 1 个并发槽位，并返回窗口上下文。"""
        code = (task_type_code or "").strip()
        wid = int(window_pk)
        if not code or wid <= 0:
            return None

        for _ in range(3):
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    db.row_factory = aiosqlite.Row
                    await db.execute("PRAGMA foreign_keys=ON")
                    try:
                        await db.execute("PRAGMA busy_timeout=3000")
                    except Exception:
                        pass
                    await db.execute("BEGIN IMMEDIATE")

                    # Step 1: 找到该 task_type+window 的 mapping，并取并发/阈值参数
                    cur = await db.execute(
                        """
                        SELECT
                          m.id AS mapping_id,
                          t.concurrency AS task_concurrency,
                          t.continuous_error_threshold
                        FROM task_types t
                        JOIN task_type_windows m ON m.task_type_id = t.id
                        JOIN windows w ON m.window_pk = w.id
                        WHERE t.deleted=0 AND t.enabled=1
                          AND t.code=?
                          AND m.window_pk=?
                          AND m.deleted=0 AND m.enabled=1
                          AND w.deleted=0 AND w.enabled=1
                          AND (
                            (m.remaining_quota > 2)
                            OR (m.cooldown_until IS NOT NULL AND m.cooldown_until <= datetime('now', '+5 minutes'))
                          )
                          AND (m.error_cooldown_until IS NULL OR m.error_cooldown_until <= CURRENT_TIMESTAMP)
                          AND (m.consecutive_errors < t.continuous_error_threshold)
                          AND (COALESCE(m.inflight_slots, 0) < t.concurrency)
                        LIMIT 1
                        """,
                        (code, wid),
                    )
                    picked = await cur.fetchone()
                    if not picked:
                        await db.execute("ROLLBACK")
                        return None

                    mid = int(picked["mapping_id"])
                    task_concurrency = max(1, int(picked["task_concurrency"] or 1))
                    threshold = max(1, int(picked["continuous_error_threshold"] or 1))

                    # Step 2: 预占并发槽位
                    cur2 = await db.execute(
                        """
                        UPDATE task_type_windows
                        SET inflight_slots = COALESCE(inflight_slots, 0) + 1,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                          AND deleted = 0 AND enabled = 1
                          AND (consecutive_errors < ?)
                          AND (COALESCE(inflight_slots, 0) < ?)
                          AND (
                            (remaining_quota > 2)
                            OR (cooldown_until IS NOT NULL AND cooldown_until <= datetime('now', '+5 minutes'))
                          )
                          AND (error_cooldown_until IS NULL OR error_cooldown_until <= CURRENT_TIMESTAMP)
                        """,
                        (mid, threshold, task_concurrency),
                    )
                    if int(cur2.rowcount or 0) <= 0:
                        await db.execute("ROLLBACK")
                        continue

                    # Step 3: 返回上下文
                    cur3 = await db.execute(
                        """
                        SELECT
                          m.*,
                          t.code AS task_code,
                          t.concurrency AS task_concurrency,
                          t.continuous_error_threshold,
                          t.timeout_seconds,
                          t.create_task_handler,
                          w.window_key,
                          w.window_name,
                          w.platform_account,
                          w.platform_url,
                          s.id AS space_pk,
                          s.space_id AS space_id,
                          b.id AS browser_pk,
                          b.lan_addr,
                          b.vendor,
                          b.access_key
                        FROM task_type_windows m
                        JOIN task_types t ON m.task_type_id = t.id
                        JOIN windows w ON m.window_pk = w.id
                        JOIN spaces s ON w.space_pk = s.id
                        JOIN browsers b ON s.browser_id = b.id
                        WHERE m.id = ?
                        LIMIT 1
                        """,
                        (mid,),
                    )
                    row = await cur3.fetchone()
                    await db.commit()
                    return dict(row) if row else None
            except Exception as e:
                if "database is locked" in str(e).lower():
                    import asyncio

                    await asyncio.sleep(0.01)
                    continue
                raise
        return None

    async def release_mapping_slot(self, mapping_id: int) -> None:
        """释放 1 个预占并发槽位（下限到 0，避免异常时减成负数）。"""
        mid = int(mapping_id)
        for _ in range(3):
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute("PRAGMA foreign_keys=ON")
                    await db.execute(
                        """
                        UPDATE task_type_windows
                        SET inflight_slots = CASE
                              WHEN COALESCE(inflight_slots, 0) >= 1 THEN COALESCE(inflight_slots, 0) - 1
                              ELSE 0
                            END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (mid,),
                    )
                    await db.commit()
                    return
            except Exception as e:
                if "database is locked" in str(e).lower():
                    import asyncio

                    await asyncio.sleep(0.01)
                    continue
                raise

    # ---------- tasks ----------
    async def fail_running_and_queued_tasks_on_startup(self) -> Dict[str, int]:
        """启动时清理遗留任务状态。

        目的：
        - 进程异常退出/重启后，DB 里可能残留 status=running/queued 的任务
        - 这些任务在当前实例不可能再继续执行，需要统一置为 failed，避免 UI/调度误判

        同时：
        - 重置 `task_type_windows.inflight_slots`，避免预占并发槽位在异常退出后“泄漏”
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE tasks
                SET status = 'failed',
                    error_message = CASE
                      WHEN error_message IS NULL OR TRIM(error_message) = '' THEN 'server restarted'
                      ELSE error_message
                    END,
                    completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP)
                WHERE status IN ('running', 'queued')
                """
            )
            cur = await db.execute("SELECT changes()")
            tasks_failed = int(((await cur.fetchone()) or [0])[0] or 0)

            await db.execute(
                """
                UPDATE task_type_windows
                SET inflight_slots = 0,
                    updated_at = CURRENT_TIMESTAMP
                WHERE COALESCE(inflight_slots, 0) != 0
                """
            )
            cur = await db.execute("SELECT changes()")
            mapping_slots_reset = int(((await cur.fetchone()) or [0])[0] or 0)

            await db.commit()
            return {"tasks_failed": tasks_failed, "mapping_slots_reset": mapping_slots_reset}

    async def create_task(self, task: Task) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                INSERT INTO tasks (task_id, task_type_code, status, progress, prompt, image_path, window_pk)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (task.task_id, task.task_type_code, task.status, int(task.progress or 0), task.prompt, task.image_path, task.window_pk),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def get_task(self, task_id: str) -> Optional[Task]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id.strip(),))
            row = await cur.fetchone()
            if not row:
                return None
            d = dict(row)
            if d.get("result_json"):
                try:
                    d["result"] = json.loads(d["result_json"])
                except Exception:
                    d["result"] = None
            d.pop("result_json", None)
            return Task(**d)

    async def count_tasks(
        self,
        task_type_code: Optional[str] = None,
        status: Optional[str] = None,
        window_pk: Optional[int] = None,
        q: Optional[str] = None,
    ) -> int:
        where: List[str] = ["1=1"]
        params: List[Any] = []

        if task_type_code:
            where.append("task_type_code = ?")
            params.append(task_type_code.strip())
        if status:
            where.append("status = ?")
            params.append(status.strip())
        if window_pk is not None:
            where.append("window_pk = ?")
            params.append(int(window_pk))
        if q:
            qq = f"%{q.strip()}%"
            where.append("(task_id LIKE ? OR prompt LIKE ? OR error_message LIKE ?)")
            params.extend([qq, qq, qq])

        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(f"SELECT COUNT(*) AS c FROM tasks WHERE {' AND '.join(where)}", params)
            row = await cur.fetchone()
            try:
                return int((row[0] if row else 0) or 0)
            except Exception:
                return 0

    async def list_tasks(
        self,
        limit: int = 50,
        offset: int = 0,
        task_type_code: Optional[str] = None,
        status: Optional[str] = None,
        window_pk: Optional[int] = None,
        q: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        lim = max(1, min(200, int(limit or 50)))
        off = max(0, int(offset or 0))

        where: List[str] = ["1=1"]
        params: List[Any] = []

        if task_type_code:
            where.append("t.task_type_code = ?")
            params.append(task_type_code.strip())
        if status:
            where.append("t.status = ?")
            params.append(status.strip())
        if window_pk is not None:
            where.append("t.window_pk = ?")
            params.append(int(window_pk))
        if q:
            qq = f"%{q.strip()}%"
            where.append("(t.task_id LIKE ? OR t.prompt LIKE ? OR t.error_message LIKE ?)")
            params.extend([qq, qq, qq])

        params.extend([lim, off])

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                f"""
                SELECT
                  t.task_id,
                  t.task_type_code,
                  t.status,
                  t.prompt,
                  t.window_pk,
                  w.platform_account AS window_account,
                  w.window_sort_num AS window_sort_num,
                  t.error_message,
                  t.result_json,
                  t.created_at
                FROM tasks t
                LEFT JOIN windows w ON w.id = t.window_pk
                WHERE {' AND '.join(where)}
                ORDER BY t.id DESC
                LIMIT ? OFFSET ?
                """,
                params,
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def update_task(
        self,
        task_id: str,
        status: Optional[str] = None,
        progress: Optional[int] = None,
        window_pk: Optional[int] = None,
        result: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        set_started: bool = False,
        set_completed: bool = False,
    ) -> None:
        updates: List[str] = []
        params: List[Any] = []

        if status is not None:
            updates.append("status=?")
            params.append(status)
        if progress is not None:
            updates.append("progress=?")
            params.append(int(progress))
        if window_pk is not None:
            updates.append("window_pk=?")
            params.append(int(window_pk))
        if result is not None:
            updates.append("result_json=?")
            params.append(json.dumps(result, ensure_ascii=False))
        if error_message is not None:
            updates.append("error_message=?")
            params.append(error_message)
        if set_started:
            updates.append("started_at=CURRENT_TIMESTAMP")
        if set_completed:
            updates.append("completed_at=CURRENT_TIMESTAMP")

        if not updates:
            return

        params.append(task_id.strip())
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                f"UPDATE tasks SET {', '.join(updates)} WHERE task_id=?",
                params,
            )
            await db.commit()

    # ---------- mapping runtime updates (quota/errors/cooldown) ----------
    async def consume_mapping_quota(self, mapping_id: int, amount: int = 1) -> None:
        """扣减剩余额度（最低到 0）。"""
        amt = max(1, int(amount))
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE task_type_windows
                SET remaining_quota = CASE
                      WHEN remaining_quota >= ? THEN remaining_quota - ?
                      ELSE 0
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (amt, amt, int(mapping_id)),
            )
            await db.commit()

    async def mark_mapping_success(self, mapping_id: int) -> None:
        """一次成功：连续错误清零。"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE task_type_windows
                SET consecutive_errors = 0,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (int(mapping_id),),
            )
            await db.commit()

    async def mark_mapping_error(self, mapping_id: int, threshold: int, cooldown_seconds: int = 3600, cooldown_seconds_short: int = 900) -> None:
        """一次失败：累计错误 + 连续错误，并在达到阈值时写入冷却时间。"""
        thr = max(1, int(threshold))
        cd = max(10, int(cooldown_seconds))
        cd_short = max(10, int(cooldown_seconds_short))
        modifier = f"+{cd} seconds"
        modifier_short = f"+{cd_short} seconds"
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE task_type_windows
                SET total_errors = total_errors + 1,
                    consecutive_errors = consecutive_errors + 1,
                    error_cooldown_until = CASE
                      WHEN (consecutive_errors + 1) >= ? THEN datetime('now', ?)
                      ELSE datetime('now', ?)
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (thr, modifier, modifier_short, int(mapping_id)),
            )
            await db.commit()

    async def get_mapping_runtime_state(self, mapping_id: int) -> Dict[str, Any]:
        """读取窗口映射运行态字段（连续错误/错误冷却等）。"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT
                  id,
                  consecutive_errors,
                  error_cooldown_until,
                  enabled,
                  remaining_quota,
                  cooldown_until,
                  updated_at
                FROM task_type_windows
                WHERE id = ?
                """,
                (int(mapping_id),),
            )
            row = await cur.fetchone()
            await cur.close()
            return dict(row) if row else {}

