"""
指纹浏览器管理与同步客户端（FPBrowserClient）。

本文件由 `sora2api/src/services/proxyBroserManage.py` 迁移/整理而来，并作为 fpbrowser2api 的统一入口。

当前重点实现：RoxyBrowser 的“空间(=workspaceId) -> 窗口同步”：
- 先 GET /browser/list_v3 分页拿到窗口列表（简要信息）
- 再 GET /browser/detail 逐个窗口拉取明细信息
- 最终仅抽取并返回以下字段（用于写入本地 windows 表）：
  - dirId -> window_key
  - windowName -> window_name
  - windowPlatformList[0].platformUserName -> platform_account
  - windowPlatformList[0].platformUrl -> platform_url
  - proxyInfo.lastIp -> proxy_addr
  - proxyInfo.lastCountry -> proxy_country

RoxyBrowser 官方接口文档：
- 获取窗口列表：`/browser/list_v3`
  https://faq.roxybrowser.com/zh/api-documentation/api-endpoint.html#%E8%8E%B7%E5%8F%96%E6%B5%8F%E8%A7%88%E5%99%A8%E7%AA%97%E5%8F%A3%E5%88%97%E8%A1%A8
- 获取窗口明细：`/browser/detail`
  https://faq.roxybrowser.com/zh/api-documentation/api-endpoint.html#%E8%8E%B7%E5%8F%96%E6%B5%8F%E8%A7%88%E5%99%A8%E7%AA%97%E5%8F%A3%E6%98%8E%E7%BB%86
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import httpx


class FPBrowserClient:
    def __init__(self, proxy_enabled: bool = False, proxy_url: Optional[str] = None) -> None:
        self.proxy_enabled = proxy_enabled
        self.proxy_url = (proxy_url or "").strip() or None

    def _client(self) -> httpx.AsyncClient:
        """
        httpx 版本兼容：
        - 新版本常用参数名为 proxy
        - 老版本常用参数名为 proxies
        """
        timeout = httpx.Timeout(30.0)
        if self.proxy_enabled and self.proxy_url:
            try:
                return httpx.AsyncClient(proxy=self.proxy_url, timeout=timeout)  # type: ignore[call-arg]
            except TypeError:
                return httpx.AsyncClient(proxies=self.proxy_url, timeout=timeout)  # type: ignore[call-arg]
        return httpx.AsyncClient(timeout=timeout)

    async def list_windows(
        self,
        vendor: str,
        base_url: str,
        access_key: Optional[str],
        space_id: str,
    ) -> List[Dict[str, Any]]:
        # 说明：本项目“同步窗口”当前按 RoxyBrowser 官方接口实现，
        # 以 `_roxy_get_browser_list_v3` + `_roxy_get_browser_detail` 为准。
        vendor = (vendor or "roxy").strip().lower()
        base_url = (base_url or "").strip().rstrip("/")
        space_id = (space_id or "").strip()
        if not base_url or not space_id:
            return []

        # RoxyBrowser：space_id 对应 workspaceId（int）
        if vendor not in ("roxy", "roxybrowser", "generic"):
            # 未来你要接入其他厂商时，再在此处扩展分支
            raise RuntimeError(f"暂不支持 vendor={vendor} 的同步，请设置为 roxy")

        try:
            workspace_id = int(space_id)
        except Exception:
            raise RuntimeError("RoxyBrowser 的 space_id 请填写 workspaceId（纯数字）")

        return await self._roxy_list_windows(
            base_url=base_url,
            token=access_key,
            workspace_id=workspace_id,
        )

    # -------------------- RoxyBrowser --------------------
    def _roxy_headers(self, token: Optional[str]) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if token:
            # RoxyBrowser 文档要求请求头 token
            h["token"] = token
        return h

    async def _roxy_get(self, base_url: str, token: Optional[str], path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = base_url.rstrip("/") + "/" + path.lstrip("/")
        async with self._client() as client:
            resp = await client.get(url, headers=self._roxy_headers(token), params={k: v for k, v in (params or {}).items() if v is not None and v != ""})
            resp.raise_for_status()
            return resp.json()

    async def _roxy_get_browser_list_v3(
        self,
        *,
        base_url: str,
        token: Optional[str],
        workspace_id: int,
        page_index: int,
        page_size: int,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        rsp = await self._roxy_get(
            base_url,
            token,
            "/browser/list_v3",
            {
                "workspaceId": int(workspace_id),
                "page_index": int(page_index),
                "page_size": int(page_size),
            },
        )
        if (rsp or {}).get("code") != 0:
            raise RuntimeError(f"Roxy list_v3 失败：{(rsp or {}).get('msg')}")
        data = (rsp or {}).get("data") or {}
        total = int(data.get("total") or 0)
        rows = data.get("rows") or []
        rows = [x for x in rows if isinstance(x, dict)]
        return total, rows

    async def _roxy_get_browser_detail(
        self,
        *,
        base_url: str,
        token: Optional[str],
        workspace_id: int,
        dir_id: str,
    ) -> Dict[str, Any]:
        rsp = await self._roxy_get(
            base_url,
            token,
            "/browser/detail",
            {
                "workspaceId": int(workspace_id),
                "dirId": str(dir_id),
            },
        )
        if (rsp or {}).get("code") != 0:
            raise RuntimeError(f"Roxy detail 失败(dirId={dir_id})：{(rsp or {}).get('msg')}")
        data = (rsp or {}).get("data") or {}
        rows = data.get("rows") or []
        if isinstance(rows, list) and rows:
            if isinstance(rows[0], dict):
                return rows[0]
        # 兼容：有的实现可能直接返回 data 为 dict
        if isinstance(data, dict):
            return data
        return {}

    def _pick_platform(self, detail: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        lst = detail.get("windowPlatformList") or []
        if isinstance(lst, list):
            for it in lst:
                if not isinstance(it, dict):
                    continue
                u = it.get("platformUserName")
                url = it.get("platformUrl")
                if u or url:
                    return (str(u).strip() if u is not None else None, str(url).strip() if url is not None else None)
        return (None, None)

    async def _roxy_list_windows(
        self,
        *,
        base_url: str,
        token: Optional[str],
        workspace_id: int,
    ) -> List[Dict[str, Any]]:
        """
        按你的需求“完整同步”：
        1) list_v3 分页拉全量窗口 dirId 列表
        2) detail 逐个 dirId 拉取明细
        3) 仅返回需要入库的字段（避免把 cookies 等超大字段写入 raw_json）
        """
        base_url = base_url.rstrip("/")
        page_size = 100
        page_index = 1
        total = 0
        all_rows: List[Dict[str, Any]] = []

        while True:
            t, rows = await self._roxy_get_browser_list_v3(
                base_url=base_url,
                token=token,
                workspace_id=workspace_id,
                page_index=page_index,
                page_size=page_size,
            )
            if total <= 0:
                total = t
            if not rows:
                break
            all_rows.extend(rows)
            if total > 0 and len(all_rows) >= total:
                break
            page_index += 1
            if page_index > 500:  # 极端兜底
                break

        # 逐个 detail 补全
        result: List[Dict[str, Any]] = []
        for r in all_rows:
            dir_id = (r.get("dirId") or "").strip()
            if not dir_id:
                continue
            detail = await self._roxy_get_browser_detail(
                base_url=base_url,
                token=token,
                workspace_id=workspace_id,
                dir_id=dir_id,
            )
            window_name = (detail.get("windowName") or r.get("windowName") or "").strip() or dir_id

            platform_user, platform_url = self._pick_platform(detail)
            proxy_info = detail.get("proxyInfo") or {}
            if not isinstance(proxy_info, dict):
                proxy_info = {}
            last_ip = proxy_info.get("lastIp")
            last_country = proxy_info.get("lastCountry")

            minimal_raw = {
                "dirId": dir_id,
                "windowName": window_name,
                "platformUserName": platform_user,
                "platformUrl": platform_url,
                "lastIp": str(last_ip).strip() if last_ip is not None else None,
                "lastCountry": str(last_country).strip() if last_country is not None else None,
            }

            result.append(
                {
                    "window_key": dir_id,
                    "window_name": window_name,
                    "platform_account": platform_user,
                    "platform_url": platform_url,
                    "proxy_addr": str(last_ip).strip() if last_ip is not None else None,
                    "proxy_country": str(last_country).strip() if last_country is not None else None,
                    "enabled": True,
                    "deleted": False,
                    "raw": minimal_raw,
                }
            )
        return result

