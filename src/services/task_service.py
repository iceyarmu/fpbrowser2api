"""Task scheduling + dispatch service."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..core.database import Database
from ..core.logger import logger
from ..core.models import Task
from .image_task_executor import simulate_image_task
from .video_task_executor import simulate_video_task
from .sora_task_executor import get_or_create_sora_session, sora_gen_video
from .sora_wm_remove_executor import sora_wm_remove


@dataclass
class PickedWindow:
    mapping_id: int
    window_pk: int
    window_key: str
    task_code: str
    task_concurrency: int
    threshold: int
    timeout_seconds: int
    create_task_handler: Optional[str]

    browser_vendor: str
    browser_base_url: str
    browser_access_key: Optional[str]
    space_id: str
    sora_access_token: Optional[str] = None
    sora_access_expires: Optional[str] = None


class TaskService:
    def __init__(self, db: Database) -> None:
        self.db = db
        # 仅内存保存 payload（不落库，节省 DB）
        self._task_payloads: dict[str, Dict[str, Any]] = {}

    async def submit_task(
        self,
        task_type_code: str,
        payload: Dict[str, Any],
        *,
        mapping_id: Optional[int] = None,
        window_pk: Optional[int] = None,
    ) -> str:
        task_type_code = (task_type_code or "").strip()
        if not task_type_code:
            raise ValueError("task_type_code 不能为空")
        payload = payload or {}

        picked: Optional[PickedWindow] = None
        # 指定窗口优先级：mapping_id > window_pk > 默认自动挑选
        if mapping_id is not None:
            picked = await self._pick_window_by_mapping(task_type_code, mapping_id=int(mapping_id))
        elif window_pk is not None:
            picked = await self._pick_window_by_window_pk(task_type_code, window_pk=int(window_pk))
        else:
            picked = await self._pick_window(task_type_code)
        if not picked:
            if mapping_id is not None or window_pk is not None:
                raise RuntimeError("指定窗口不可用：请确认该窗口已绑定该任务类型且额度>0、未冷却、已启用、未超并发")
            raise RuntimeError("没有可用窗口：请确认该任务类型已绑定窗口且额度>0、未冷却、已启用")

        task_id = uuid.uuid4().hex
        try:
            # 不把 payload 写入 DB；仅保存最小字段（prompt 存空字符串满足 NOT NULL）
            await self.db.create_task(
                Task(
                    task_id=task_id,
                    task_type_code=task_type_code,
                    status="queued",
                    progress=0,
                    prompt="",
                    image_path=None,
                    window_pk=picked.window_pk,
                )
            )
            self._task_payloads[task_id] = payload
            asyncio.create_task(self._run_task(task_id, picked))
            return task_id
        except Exception:
            # 兜底：若创建任务失败，释放预占槽位避免泄漏
            try:
                await self.db.release_mapping_slot(picked.mapping_id)
            except Exception:
                pass
            raise

    async def _pick_window(self, task_type_code: str) -> Optional[PickedWindow]:
        """从 DB 候选中挑选窗口，并在 DB 中原子预占并发槽位。

        说明：
        - 预占由 DB 字段 inflight_slots 完成（支持多进程/多实例，避免超卖）
        - 挑选排序由 DB 决定（consecutive_errors 最低优先，其次 remaining_quota 最少优先）
        """
        r = await self.db.pick_and_reserve_window_for_task(task_type_code=task_type_code)
        if not r:
            return None

        mid = int(r["id"])
        picked = PickedWindow(
            mapping_id=mid,
            window_pk=int(r["window_pk"]),
            window_key=str(r.get("window_key") or "").strip(),
            task_code=str(r["task_code"]),
            task_concurrency=int(r.get("task_concurrency") or 1),
            threshold=int(r.get("continuous_error_threshold") or 3),
            timeout_seconds=int(r.get("timeout_seconds") or 600),
            create_task_handler=(str(r.get("create_task_handler") or "").strip() or None),
            browser_vendor=str(r.get("vendor") or "generic"),
            browser_base_url=str(r.get("lan_addr") or ""),
            browser_access_key=r.get("access_key"),
            space_id=str(r.get("space_id") or ""),
            sora_access_token=(str(r.get("sora_access_token") or "").strip() or None),
            sora_access_expires=(str(r.get("sora_access_expires") or "").strip() or None),
        )
        if not picked.window_key:
            try:
                await self.db.release_mapping_slot(mid)
            except Exception:
                pass
            return None
        return picked

    async def _pick_window_by_mapping(self, task_type_code: str, mapping_id: int) -> Optional[PickedWindow]:
        """指定 mapping_id（task_type_windows.id）预占并发槽位并返回窗口上下文。"""
        r = await self.db.reserve_mapping_for_task(task_type_code=task_type_code, mapping_id=int(mapping_id))
        if not r:
            return None
        # 复用字段解析逻辑：与 _pick_window 保持一致
        mid = int(r["id"])
        picked = PickedWindow(
            mapping_id=mid,
            window_pk=int(r["window_pk"]),
            window_key=str(r.get("window_key") or "").strip(),
            task_code=str(r["task_code"]),
            task_concurrency=int(r.get("task_concurrency") or 1),
            threshold=int(r.get("continuous_error_threshold") or 3),
            timeout_seconds=int(r.get("timeout_seconds") or 600),
            create_task_handler=(str(r.get("create_task_handler") or "").strip() or None),
            browser_vendor=str(r.get("vendor") or "generic"),
            browser_base_url=str(r.get("lan_addr") or ""),
            browser_access_key=r.get("access_key"),
            space_id=str(r.get("space_id") or ""),
            sora_access_token=(str(r.get("sora_access_token") or "").strip() or None),
            sora_access_expires=(str(r.get("sora_access_expires") or "").strip() or None),
        )
        if not picked.window_key:
            try:
                await self.db.release_mapping_slot(mid)
            except Exception:
                pass
            return None
        return picked

    async def _pick_window_by_window_pk(self, task_type_code: str, window_pk: int) -> Optional[PickedWindow]:
        """指定 window_pk 预占并发槽位并返回窗口上下文。"""
        r = await self.db.reserve_window_for_task(task_type_code=task_type_code, window_pk=int(window_pk))
        if not r:
            return None
        mid = int(r["id"])
        picked = PickedWindow(
            mapping_id=mid,
            window_pk=int(r["window_pk"]),
            window_key=str(r.get("window_key") or "").strip(),
            task_code=str(r["task_code"]),
            task_concurrency=int(r.get("task_concurrency") or 1),
            threshold=int(r.get("continuous_error_threshold") or 3),
            timeout_seconds=int(r.get("timeout_seconds") or 600),
            create_task_handler=(str(r.get("create_task_handler") or "").strip() or None),
            browser_vendor=str(r.get("vendor") or "generic"),
            browser_base_url=str(r.get("lan_addr") or ""),
            browser_access_key=r.get("access_key"),
            space_id=str(r.get("space_id") or ""),
            sora_access_token=(str(r.get("sora_access_token") or "").strip() or None),
            sora_access_expires=(str(r.get("sora_access_expires") or "").strip() or None),
        )
        if not picked.window_key:
            try:
                await self.db.release_mapping_slot(mid)
            except Exception:
                pass
            return None
        return picked

    async def _run_task(self, task_id: str, picked: PickedWindow) -> None:
        try:
            await self.db.update_task(task_id, status="running", progress=1, set_started=True)
            logger.info("task started: %s type=%s window=%s mapping=%s", task_id, picked.task_code, picked.window_pk, picked.mapping_id)

            async def progress_cb(p: int, _payload: Optional[Dict[str, Any]]):
                try:
                    await self.db.update_task(task_id, progress=int(p))
                except Exception:
                    pass

            try:
                payload = self._task_payloads.get(task_id) or {}
                prompt = str(payload.get("prompt") or "").strip()

                # 执行分发：优先按 task_type 配置的 create_task_handler 决定执行器
                if picked.create_task_handler == "sora_gen_video":
                    result = await asyncio.wait_for(
                        sora_gen_video(
                            payload,
                            progress_cb,
                            browser_vendor=picked.browser_vendor,
                            browser_base_url=picked.browser_base_url,
                            browser_access_key=picked.browser_access_key,
                            space_id=picked.space_id,
                            window_key=picked.window_key,
                            timeout_seconds=float(picked.timeout_seconds),
                            access_token=picked.sora_access_token,
                            access_expires=picked.sora_access_expires,
                        ),
                        timeout=float(picked.timeout_seconds),
                    )
                elif picked.create_task_handler == "sora_wm_remove":
                    result = await asyncio.wait_for(
                        sora_wm_remove(
                            payload,
                            progress_cb,
                            browser_vendor=picked.browser_vendor,
                            browser_base_url=picked.browser_base_url,
                            browser_access_key=picked.browser_access_key,
                            space_id=picked.space_id,
                            window_key=picked.window_key,
                            timeout_seconds=float(picked.timeout_seconds),
                        ),
                        timeout=float(picked.timeout_seconds),
                    )
                elif picked.task_code == "gen_video":
                    result = await asyncio.wait_for(simulate_video_task(prompt, None, progress_cb), timeout=float(picked.timeout_seconds))
                else:
                    # 默认按图片模拟（包括 gen_image 以及其它未实现类型）
                    result = await asyncio.wait_for(simulate_image_task(prompt, None, progress_cb), timeout=float(picked.timeout_seconds))

                # Sora：若执行器返回了 nf_check，则用其回写余额/限流信息（覆盖本地扣减，更贴近真实剩余）
                try:
                    nf = (result or {}).get("nf_check") if isinstance(result, dict) else None
                    rate = (nf or {}) if isinstance(nf, dict) else None
                    if rate and rate.get("remaining_count") is not None:
                        await self.db.update_task_type_window(
                            mapping_id=picked.mapping_id,
                            remaining_quota=int(rate.get("remaining_count") or 0),
                            sora_remaining_count=int(rate.get("remaining_count") or 0),
                            sora_rate_limit_reached=bool(rate.get("rate_limit_reached", False)),
                            sora_access_resets_in_seconds=int(rate.get("access_resets_in_seconds") or 0),
                            cooldown_until=(str(rate.get("cooldown_until")) if rate.get("cooldown_until") else None),
                        )
                except Exception:
                    pass
                # 清空一下result中的nf_check，避免敏感信息泄露
                result["nf_check"] = None
                await self.db.update_task(task_id, status="completed", progress=100, result=result, set_completed=True)
                #await self.db.consume_mapping_quota(picked.mapping_id, amount=1)
                await self.db.mark_mapping_success(picked.mapping_id)
                logger.info("task completed: %s", task_id)
            except Exception as e:
                # 失败：尽量把“是否不扣罚(no_penalty)”等信息写入 result_json，便于上游做退款/分类。
                no_penalty = bool(getattr(e, "no_penalty", False))
                status_code = getattr(e, "status_code", None)
                err_result: Dict[str, Any] = {
                    "error_type": e.__class__.__name__,
                    "no_penalty": no_penalty,
                }
                if status_code is not None:
                    try:
                        err_result["status_code"] = int(status_code)
                    except Exception:
                        err_result["status_code"] = str(status_code)
                await self.db.update_task(
                    task_id,
                    status="failed",
                    error_message=str(e),
                    result=err_result,
                    set_completed=True,
                )
                # 某些错误不应计入“窗口连续错误”（例如：Sora create 400 invalid_request、未抓到 POST 等环境/请求错误）
                # 执行器侧会抛出带 no_penalty=true 的异常（或同名属性），这里做兼容判断。
                if not no_penalty:
                    await self.db.mark_mapping_error(picked.mapping_id, threshold=picked.threshold, cooldown_seconds=3600)
                    # 若连续错误达到/超过阈值（熔断进入错误冷却），则启动倒计时关闭窗口（释放前端状态）
                    # 注意：仅对 Sora 窗口做处理；其它模拟执行器没有需要维护的浏览器会话
                    try:
                        st = await self.db.get_mapping_runtime_state(picked.mapping_id)
                        ce = int((st or {}).get("consecutive_errors") or 0)
                        if ce >= int(picked.threshold):
                            sess = get_or_create_sora_session(
                                vendor=picked.browser_vendor,
                                base_url=picked.browser_base_url,
                                access_key=picked.browser_access_key,
                                space_id=picked.space_id,
                                window_key=picked.window_key,
                            )
                            # 连续错误达到阈值：切换 IP（更换代理），降低后续继续被风控/封禁概率
                            try:
                                await sess.switch_window_ip_by_proxy_pool()
                            except Exception:
                                pass
                            sess._schedule_idle_close()
                    except Exception:
                        pass
                logger.exception("task failed: %s err=%s", task_id, e)
            finally:
                # 清理内存 payload（避免堆积）
                self._task_payloads.pop(task_id, None)
        finally:
            try:
                await self.db.release_mapping_slot(picked.mapping_id)
            except Exception:
                pass

