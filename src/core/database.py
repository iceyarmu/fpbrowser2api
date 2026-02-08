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
    BrowserSpace,
    FingerprintBrowser,
    Project,
    RequestLog,
    SystemConfig,
    Task,
    TaskType,
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
                    window_name TEXT NOT NULL,
                    platform_account TEXT,
                    platform_url TEXT,
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
                CREATE TABLE IF NOT EXISTS task_types (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    code TEXT UNIQUE NOT NULL,
                    concurrency INTEGER DEFAULT 1,
                    continuous_error_threshold INTEGER DEFAULT 3,
                    timeout_seconds INTEGER DEFAULT 1800,
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
                    total_errors INTEGER DEFAULT 0,
                    consecutive_errors INTEGER DEFAULT 0,
                    daily_quota INTEGER DEFAULT 0,
                    remaining_quota INTEGER DEFAULT 0,
                    max_concurrency INTEGER DEFAULT 1,
                    cooldown_until TIMESTAMP,
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

            await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_task_id ON tasks(task_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_task_types_code ON task_types(code)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_windows_space_pk ON windows(space_pk)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_req_logs_created_at ON request_logs(created_at)")

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
                ]
                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(db, "system_config", col_name):
                        await db.execute(f"ALTER TABLE system_config ADD COLUMN {col_name} {col_type}")

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

            await db.execute(
                """
                INSERT INTO system_config (id, proxy_enabled, proxy_url, api_key, debug_enabled, log_to_file, updated_at)
                VALUES (1, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                  proxy_enabled=excluded.proxy_enabled,
                  proxy_url=excluded.proxy_url,
                  api_key=excluded.api_key,
                  debug_enabled=excluded.debug_enabled,
                  log_to_file=excluded.log_to_file,
                  updated_at=CURRENT_TIMESTAMP
                """,
                (new_proxy_enabled, new_proxy_url, new_api_key, new_debug_enabled, new_log_to_file),
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

    async def create_space(self, browser_id: int, name: str, space_id: str) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                INSERT INTO spaces (browser_id, name, space_id, deleted)
                VALUES (?, ?, ?, 0)
                """,
                (browser_id, name.strip(), space_id.strip()),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def update_space(self, space_pk: int, name: str, space_id: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE spaces SET name=?, space_id=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (name.strip(), space_id.strip(), space_pk),
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
                "SELECT * FROM windows WHERE deleted = 0 AND space_pk = ? ORDER BY window_name ASC, id ASC",
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
                window_name = str(w.get("window_name") or w.get("name") or window_key).strip()
                platform_account = (w.get("platform_account") or w.get("account") or w.get("username"))
                platform_url = (w.get("platform_url") or w.get("url"))
                proxy_addr = (w.get("proxy_addr") or w.get("proxy") or w.get("proxy_url"))
                proxy_country = (w.get("proxy_country") or w.get("country"))
                proxy_expire_at = (w.get("proxy_expire_at") or w.get("expire_at") or w.get("proxy_expire"))
                enabled = 1 if bool(w.get("enabled", True)) else 0
                deleted = 1 if bool(w.get("deleted", False)) else 0
                raw_json = json.dumps(w, ensure_ascii=False)

                await db.execute(
                    """
                    INSERT INTO windows (
                        space_pk, window_key, window_name,
                        platform_account, platform_url,
                        proxy_addr, proxy_country, proxy_expire_at,
                        enabled, deleted, raw_json, synced_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(space_pk, window_key) DO UPDATE SET
                        window_name=excluded.window_name,
                        platform_account=excluded.platform_account,
                        platform_url=excluded.platform_url,
                        proxy_addr=excluded.proxy_addr,
                        proxy_country=excluded.proxy_country,
                        proxy_expire_at=excluded.proxy_expire_at,
                        enabled=excluded.enabled,
                        deleted=excluded.deleted,
                        raw_json=excluded.raw_json,
                        synced_at=CURRENT_TIMESTAMP,
                        updated_at=CURRENT_TIMESTAMP
                    """,
                    (
                        space_pk,
                        window_key,
                        window_name,
                        platform_account,
                        platform_url,
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

    # ---------- task types ----------
    async def list_task_types(self) -> List[TaskType]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM task_types WHERE deleted = 0 ORDER BY updated_at DESC, id DESC")
            rows = await cur.fetchall()
            return [TaskType(**dict(r)) for r in rows]

    async def create_task_type(
        self,
        name: str,
        code: str,
        concurrency: int,
        continuous_error_threshold: int,
        timeout_seconds: int,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                INSERT INTO task_types (name, code, concurrency, continuous_error_threshold, timeout_seconds, enabled, deleted)
                VALUES (?, ?, ?, ?, ?, 1, 0)
                """,
                (name.strip(), code.strip(), int(concurrency), int(continuous_error_threshold), int(timeout_seconds)),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def update_task_type(
        self,
        task_type_id: int,
        name: str,
        concurrency: int,
        continuous_error_threshold: int,
        timeout_seconds: int,
        enabled: bool,
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE task_types
                SET name=?, concurrency=?, continuous_error_threshold=?, timeout_seconds=?, enabled=?, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (name.strip(), int(concurrency), int(continuous_error_threshold), int(timeout_seconds), 1 if enabled else 0, task_type_id),
            )
            await db.commit()

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
                  w.platform_account,
                  w.platform_url,
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
                ORDER BY m.updated_at DESC, m.id DESC
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
        max_concurrency: int,
        enabled: bool = True,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            affected = 0
            for wid in window_pks:
                await db.execute(
                    """
                    INSERT INTO task_type_windows (
                      task_type_id, window_pk,
                      daily_quota, remaining_quota, max_concurrency,
                      enabled, deleted, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
                    ON CONFLICT(task_type_id, window_pk) DO UPDATE SET
                      daily_quota=excluded.daily_quota,
                      remaining_quota=excluded.remaining_quota,
                      max_concurrency=excluded.max_concurrency,
                      enabled=excluded.enabled,
                      deleted=0,
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    (
                        task_type_id,
                        int(wid),
                        int(daily_quota),
                        int(remaining_quota),
                        int(max_concurrency),
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
        max_concurrency: Optional[int] = None,
        cooldown_until: Optional[str] = None,  # ISO string or None
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
        if max_concurrency is not None:
            _set("max_concurrency", int(max_concurrency))
        if cooldown_until is not None:
            _set("cooldown_until", cooldown_until if cooldown_until else None)
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

    async def pick_available_window(self, task_type_code: str) -> Optional[Dict[str, Any]]:
        """选择一个可用窗口（额度>0、启用、未冷却、未删除）。

        注意：并发控制在调度器层完成；这里仅做 DB 条件筛选 + 简单排序。
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT
                  m.*,
                  t.code AS task_code,
                  t.concurrency AS task_concurrency,
                  t.continuous_error_threshold,
                  t.timeout_seconds,
                  w.window_name,
                  w.platform_account,
                  w.platform_url,
                  s.id AS space_pk,
                  s.space_id AS space_id,
                  b.id AS browser_pk,
                  b.lan_addr,
                  b.vendor,
                  b.access_key
                FROM task_types t
                JOIN task_type_windows m ON m.task_type_id = t.id
                JOIN windows w ON m.window_pk = w.id
                JOIN spaces s ON w.space_pk = s.id
                JOIN browsers b ON s.browser_id = b.id
                WHERE t.deleted=0 AND t.enabled=1
                  AND t.code=?
                  AND m.deleted=0 AND m.enabled=1
                  AND w.deleted=0 AND w.enabled=1
                  AND (m.remaining_quota > 0)
                  AND (m.cooldown_until IS NULL OR m.cooldown_until <= CURRENT_TIMESTAMP)
                ORDER BY m.consecutive_errors ASC, m.remaining_quota DESC, m.updated_at DESC
                LIMIT 1
                """,
                (task_type_code.strip(),),
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    # ---------- tasks ----------
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

    async def mark_mapping_error(self, mapping_id: int, threshold: int, cooldown_seconds: int = 1800) -> None:
        """一次失败：累计错误 + 连续错误，并在达到阈值时写入冷却时间。"""
        thr = max(1, int(threshold))
        cd = max(10, int(cooldown_seconds))
        modifier = f"+{cd} seconds"
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE task_type_windows
                SET total_errors = total_errors + 1,
                    consecutive_errors = consecutive_errors + 1,
                    cooldown_until = CASE
                      WHEN (consecutive_errors + 1) >= ? THEN datetime('now', ?)
                      ELSE cooldown_until
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (thr, modifier, int(mapping_id)),
            )
            await db.commit()

