"""Fingerprint browser client (用于同步空间窗口信息).

由于不同指纹浏览器的局域网 API 不同，本模块采取“尽量兼容”的策略：
- 按 vendor 选择优先路径
- 同时提供多组常见路径回退
- 尽量从响应中解析出窗口列表

你后续只需要在这里补充某个 vendor 的具体协议即可（例如 roxy/adspower/gologin...）。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx


class FPBrowserClient:
    def __init__(self, proxy_enabled: bool = False, proxy_url: Optional[str] = None) -> None:
        self.proxy_enabled = proxy_enabled
        self.proxy_url = (proxy_url or "").strip() or None

    def _client(self) -> httpx.AsyncClient:
        proxies = None
        if self.proxy_enabled and self.proxy_url:
            proxies = self.proxy_url
        return httpx.AsyncClient(proxies=proxies, timeout=httpx.Timeout(30.0))

    async def list_windows(
        self,
        vendor: str,
        base_url: str,
        access_key: Optional[str],
        space_id: str,
    ) -> List[Dict[str, Any]]:
        vendor = (vendor or "generic").strip().lower()
        base_url = (base_url or "").strip().rstrip("/")
        space_id = (space_id or "").strip()
        if not base_url or not space_id:
            return []

        headers: Dict[str, str] = {}
        if access_key:
            # 常见的两种：Authorization Bearer / token
            headers["Authorization"] = f"Bearer {access_key}"
            headers["token"] = access_key

        # 兼容路径（按 vendor 调整优先顺序）
        candidates: List[str] = []
        if vendor == "roxy":
            candidates.extend(
                [
                    f"{base_url}/api/spaces/{space_id}/windows",
                    f"{base_url}/api/windows?space_id={space_id}",
                ]
            )
        else:
            candidates.extend(
                [
                    f"{base_url}/api/spaces/{space_id}/windows",
                    f"{base_url}/spaces/{space_id}/windows",
                    f"{base_url}/api/windows?space_id={space_id}",
                ]
            )

        last_err: Optional[str] = None
        async with self._client() as client:
            for url in candidates:
                try:
                    resp = await client.get(url, headers=headers)
                    if resp.status_code >= 400:
                        last_err = f"{url} -> HTTP {resp.status_code}"
                        continue
                    data = resp.json()
                    items = self._extract_window_list(data)
                    if items is not None:
                        return [self._normalize_item(x) for x in items]
                    last_err = f"{url} -> 无法识别返回结构"
                except Exception as e:
                    last_err = f"{url} -> {e}"
                    continue

        # 同步失败时返回空列表；由上层提示错误
        if last_err:
            raise RuntimeError(f"同步窗口失败：{last_err}")
        return []

    def _extract_window_list(self, data: Any) -> Optional[List[Dict[str, Any]]]:
        """尽量从不同风格的响应里提取窗口数组。"""
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if not isinstance(data, dict):
            return None

        # 常见：{data: [...]}
        if isinstance(data.get("data"), list):
            return [x for x in data["data"] if isinstance(x, dict)]
        # 常见：{windows: [...]}
        if isinstance(data.get("windows"), list):
            return [x for x in data["windows"] if isinstance(x, dict)]
        # 常见：{result: {list: [...]}}
        result = data.get("result")
        if isinstance(result, dict):
            for k in ("list", "items", "windows"):
                if isinstance(result.get(k), list):
                    return [x for x in result[k] if isinstance(x, dict)]
        return None

    def _normalize_item(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """把不同字段名归一成 DB 层能识别的 key，同时保留 raw。"""
        window_key = raw.get("window_key") or raw.get("id") or raw.get("dirId") or raw.get("profileId") or raw.get("uuid")
        name = raw.get("window_name") or raw.get("name") or raw.get("title") or str(window_key or "")

        account = raw.get("platform_account") or raw.get("account") or raw.get("username") or raw.get("email")
        url = raw.get("platform_url") or raw.get("url") or raw.get("site") or raw.get("homepage")

        proxy_addr = raw.get("proxy_addr") or raw.get("proxy") or raw.get("proxy_url") or raw.get("proxyUrl")
        proxy_country = raw.get("proxy_country") or raw.get("country") or raw.get("proxyCountry")
        proxy_expire_at = raw.get("proxy_expire_at") or raw.get("expire_at") or raw.get("proxy_expire") or raw.get("expireAt")

        enabled = raw.get("enabled", True)
        deleted = raw.get("deleted", False)

        return {
            "window_key": str(window_key or name),
            "window_name": str(name or "").strip() or str(window_key or ""),
            "platform_account": str(account).strip() if account is not None else None,
            "platform_url": str(url).strip() if url is not None else None,
            "proxy_addr": str(proxy_addr).strip() if proxy_addr is not None else None,
            "proxy_country": str(proxy_country).strip() if proxy_country is not None else None,
            "proxy_expire_at": str(proxy_expire_at).strip() if proxy_expire_at is not None else None,
            "enabled": bool(enabled),
            "deleted": bool(deleted),
            "raw": raw,
        }

