"""Configuration management for FPBrowser2API.

设计目标（参考 flow2api）：
- 基础值从 `config/setting.toml` 读取
- 关键值允许在运行时由数据库覆盖（管理后台修改后重启仍生效）
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional


def _load_toml(path: Path) -> Dict[str, Any]:
    try:
        import tomllib  # py>=3.11
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception:
        import tomli  # type: ignore
        with path.open("rb") as f:
            return tomli.load(f)


class Config:
    def __init__(self) -> None:
        self._config = self._load_config()

        # DB 覆盖项（None 表示仍使用文件配置）
        self._admin_username: Optional[str] = None
        self._admin_password: Optional[str] = None
        self._api_key: Optional[str] = None
        self._proxy_enabled: Optional[bool] = None
        self._proxy_url: Optional[str] = None
        self._debug_enabled: Optional[bool] = None
        self._log_to_file: Optional[bool] = None

    def _load_config(self) -> Dict[str, Any]:
        config_path = Path(__file__).parent.parent.parent / "config" / "setting.toml"
        if not config_path.exists():
            example_path = Path(__file__).parent.parent.parent / "config" / "setting_example.toml"
            raise FileNotFoundError(f"缺少配置文件：{config_path}（可参考 {example_path}）")
        return _load_toml(config_path)

    def reload_config(self) -> None:
        self._config = self._load_config()

    def get_raw_config(self) -> Dict[str, Any]:
        return self._config

    # -------- server --------
    @property
    def server_host(self) -> str:
        return str(self._config.get("server", {}).get("host", "0.0.0.0"))

    @property
    def server_port(self) -> int:
        return int(self._config.get("server", {}).get("port", 8002))

    # -------- global/security --------
    @property
    def admin_username(self) -> str:
        if self._admin_username is not None:
            return self._admin_username
        return str(self._config.get("global", {}).get("admin_username", "admin"))

    def set_admin_username_from_db(self, username: str) -> None:
        self._admin_username = username

    @property
    def admin_password(self) -> str:
        if self._admin_password is not None:
            return self._admin_password
        return str(self._config.get("global", {}).get("admin_password", "admin"))

    def set_admin_password_from_db(self, password: str) -> None:
        self._admin_password = password

    @property
    def api_key(self) -> str:
        if self._api_key is not None:
            return self._api_key
        return str(self._config.get("global", {}).get("api_key", "fpb123456"))

    @api_key.setter
    def api_key(self, value: str) -> None:
        self._api_key = value
        self._config.setdefault("global", {})["api_key"] = value

    # -------- system --------
    @property
    def proxy_enabled(self) -> bool:
        if self._proxy_enabled is not None:
            return bool(self._proxy_enabled)
        return bool(self._config.get("system", {}).get("proxy_enabled", False))

    def set_proxy_enabled_from_db(self, enabled: bool) -> None:
        self._proxy_enabled = bool(enabled)

    @property
    def proxy_url(self) -> str:
        if self._proxy_url is not None:
            return str(self._proxy_url or "")
        return str(self._config.get("system", {}).get("proxy_url", "") or "")

    def set_proxy_url_from_db(self, url: Optional[str]) -> None:
        self._proxy_url = (url or "").strip()

    @property
    def debug_enabled(self) -> bool:
        if self._debug_enabled is not None:
            return bool(self._debug_enabled)
        return bool(self._config.get("system", {}).get("debug_enabled", False))

    def set_debug_enabled(self, enabled: bool) -> None:
        self._debug_enabled = bool(enabled)
        self._config.setdefault("system", {})["debug_enabled"] = bool(enabled)

    @property
    def log_to_file(self) -> bool:
        if self._log_to_file is not None:
            return bool(self._log_to_file)
        return bool(self._config.get("system", {}).get("log_to_file", False))

    def set_log_to_file_from_db(self, enabled: bool) -> None:
        self._log_to_file = bool(enabled)


config = Config()

