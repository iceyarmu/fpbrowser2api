"""Task scheduling + dispatch service."""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..core.database import Database
from ..core.logger import logger
from ..core.models import Task
from .image_task_executor import simulate_image_task
from .video_task_executor import simulate_video_task
from .sora_task_executor import get_or_create_sora_session, sora_fetch_access_token_in_window, sora_gen_video
from .sora_wm_remove_executor import sora_wm_remove
from .sora_plus_register_executor import sora_plus_register


@dataclass
class PickedWindow:
    mapping_id: int
    window_pk: int
    window_key: str
    task_code: str
    task_concurrency: int
    threshold: int
    close_window_threshold: int
    timeout_seconds: int
    create_task_handler: Optional[str]
    browser_vendor: str
    browser_base_url: str
    browser_access_key: Optional[str]
    space_id: str
    sora_access_token: Optional[str] = None
    sora_access_expires: Optional[str] = None
    window_ip: Optional[str] = None  # 窗口绑定的 IP/代理地址，落库到任务


class TaskService:
    def __init__(self, db: Database) -> None:
        self.db = db
        # 任务 payload 仍保留一份内存副本供执行器使用；DB 侧仅保存一个“可查看/可检索”的 prompt 字符串
        self._task_payloads: dict[str, Dict[str, Any]] = {}
        # 1) payload["prompt"] 本身的长度上限（便于查看，也避免超长文本撑爆 DB）
        self._payload_prompt_max_chars: int = 1000
        # 2) 最终落库到 tasks.prompt 的总长度上限（兼容某些历史/自定义 schema 的较短字段）
        self._prompt_max_chars: int = 2000

    def _truncate_text(self, s: str, max_chars: int, *, label: str) -> str:
        s = str(s or "")
        max_chars = int(max_chars or 0)
        if max_chars <= 0:
            return ""
        if len(s) <= max_chars:
            return s
        suffix = f"…({label} truncated, orig_chars={len(s)}, max_chars={max_chars})"
        keep = max(0, max_chars - len(suffix))
        if keep <= 0:
            return suffix[:max_chars]
        return s[:keep] + suffix

    def _payload_to_prompt_text(self, payload: Dict[str, Any]) -> str:
        """把 payload 序列化成可落库的 prompt 文本（尽量是 JSON，且控制长度）。"""

        def _dumps(obj: Any) -> str:
            return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)

        total_max = max(64, int(self._prompt_max_chars or 0))
        prompt_max = max(0, int(self._payload_prompt_max_chars or 0))

        base_payload: Dict[str, Any]
        if isinstance(payload, dict):
            base_payload = dict(payload or {})
        else:
            base_payload = {"payload": payload}

        # 先对 payload["prompt"] 做“字段级”限长（<=1000）
        orig_prompt = str(base_payload.get("prompt") or "")
        if "prompt" in base_payload or orig_prompt:
            base_payload["prompt"] = self._truncate_text(orig_prompt, prompt_max, label="prompt")

        try:
            s = _dumps(base_payload)
        except Exception:
            # 极端兜底：保证永远能落库
            s = self._truncate_text(str(payload or {}), total_max, label="payload")

        if len(s) <= total_max:
            return s

        # 若整段 JSON 仍超长：降级为最小可查看 JSON（保证总长度 <= 2000 且尽量保持可解析）
        minimal_flag_key = "_payload_trimmed"
        prompt_text = str(base_payload.get("prompt") or "")

        def _minimal_json(prompt_val: str) -> str:
            return _dumps({"prompt": prompt_val, minimal_flag_key: True})

        # 二分裁剪 prompt（在不超过字段级上限的前提下），直到 minimal JSON 满足 total_max
        hi = len(prompt_text)
        lo = 0
        best = ""
        while lo <= hi:
            mid = (lo + hi) // 2
            cand_prompt = self._truncate_text(prompt_text, mid, label="prompt_db")
            cand = _minimal_json(cand_prompt)
            if len(cand) <= total_max:
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1

        if best:
            return best

        # 最后兜底：即使 prompt 为空也要可落库
        empty = _minimal_json("")
        if len(empty) <= total_max:
            return empty
        return empty[:total_max]

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

        # Sora 角色创建分支：payload.generation_id + payload.head_url
        # 需求：若能走该分支，则优先复用 generation_id 对应历史任务的窗口
        payload_generation_id = str(payload.get("generation_id") or "").strip() or None
        payload_head_url = str(payload.get("head_url") or "").strip() or None

        picked: Optional[PickedWindow] = None
        # 指定窗口优先级：mapping_id > window_pk > 默认自动挑选
        if mapping_id is not None:
            picked = await self._pick_window_by_mapping(task_type_code, mapping_id=int(mapping_id))
        elif window_pk is not None:
            picked = await self._pick_window_by_window_pk(task_type_code, window_pk=int(window_pk))
        else:
            # 若 payload 满足“基于 generation_id 创建角色”分支，则尝试按 generation_id 绑定窗口
            if payload_generation_id and payload_head_url:
                try:
                    win_pk = await self.db.get_task_window_pk_by_generation_id(payload_generation_id)
                except Exception:
                    win_pk = None
                
                if win_pk is None:
                    raise RuntimeError("该视频不属于我们的账号，请先生成视频再使用返回的generation_id创建角色")
                else:
                    picked = await self._pick_window_by_window_pk(task_type_code, win_pk)
                    if not picked:
                        raise RuntimeError("该视频不属于我们的账号，请先生成视频再使用返回的generation_id创建角色")
            if not picked:
                picked = await self._pick_window(task_type_code)
        if not picked:
            if mapping_id is not None or window_pk is not None:
                raise RuntimeError("指定窗口不可用：请确认该窗口已绑定该任务类型、未删除、已启用")
            raise RuntimeError("账号池负载已满没有，请稍后重试")

        task_id = uuid.uuid4().hex
        try:
            # 把 payload 序列化落库到 prompt 里，便于管理台查看/检索（控制长度，避免字段溢出）
            prompt_text = self._payload_to_prompt_text(payload)
            await self.db.create_task(
                Task(
                    task_id=task_id,
                    task_type_code=task_type_code,
                    generation_id=None,
                    status="queued",
                    progress=0,
                    prompt=prompt_text,
                    image_path=None,
                    window_pk=picked.window_pk,
                    window_ip=picked.window_ip,
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
        r = await self.db.pick_and_reserve_window_for_task(task_type_code=task_type_code, browser_pool_limit=80)
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
            close_window_threshold=int(r.get("continuous_error_close_window_threshold") or 3),
            timeout_seconds=int(r.get("timeout_seconds") or 600),
            create_task_handler=(str(r.get("create_task_handler") or "").strip() or None),
            window_ip=(str(r.get("window_ip") or "").strip() or None),
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
        # 显式指定窗口：不按“额度/冷却/熔断/并发上限”等资源约束拒绝，直接选中该窗口
        r = await self.db.force_reserve_mapping_for_task(task_type_code=task_type_code, mapping_id=int(mapping_id))
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
            close_window_threshold=int(r.get("continuous_error_close_window_threshold") or 3),
            timeout_seconds=int(r.get("timeout_seconds") or 600),
            create_task_handler=(str(r.get("create_task_handler") or "").strip() or None),
            window_ip=(str(r.get("window_ip") or "").strip() or None),
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
        # 显式指定窗口：不按“额度/冷却/熔断/并发上限”等资源约束拒绝，直接选中该窗口
        r = await self.db.force_reserve_window_for_task(task_type_code=task_type_code, window_pk=int(window_pk))
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
            close_window_threshold=int(r.get("continuous_error_close_window_threshold") or 3),
            timeout_seconds=int(r.get("timeout_seconds") or 600),
            create_task_handler=(str(r.get("create_task_handler") or "").strip() or None),
            window_ip=(str(r.get("window_ip") or "").strip() or None),
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

            payload = self._task_payloads.get(task_id) or {}
            prompt = str(payload.get("prompt") or "").strip()
            target_url = str(payload.get("sora_url") or "https://sora.chatgpt.com/drafts").strip()

            async def _refresh_sora_balance() -> Optional[Dict[str, Any]]:
                handler = str(picked.create_task_handler or "").strip().lower()
                if not handler.startswith("sora_gen_video"):
                    return None

                try:
                    sess = get_or_create_sora_session(
                        vendor=picked.browser_vendor,
                        base_url=picked.browser_base_url,
                        access_key=picked.browser_access_key,
                        space_id=picked.space_id,
                        window_key=picked.window_key,
                    )
                except Exception:
                    return None

                nf_check: Optional[Dict[str, Any]] = None
                nf_check_err: Optional[Exception] = None
                try:
                    checked = await sess.api_nf_check(target_url=target_url)
                    nf_check = checked if isinstance(checked, dict) else None
                except Exception as e:
                    nf_check_err = e
                    nf_check = None

                try:
                    if nf_check and nf_check.get("remaining_count") is not None:
                        await self.db.update_task_type_window(
                            mapping_id=picked.mapping_id,
                            remaining_quota=int(nf_check.get("remaining_count") or 0),
                            sora_remaining_count=int(nf_check.get("remaining_count") or 0),
                            sora_purchased_remaining_count=int(nf_check.get("purchased_remaining_count") or 0),
                            sora_rate_limit_reached=bool(nf_check.get("rate_limit_reached", False)),
                            sora_access_resets_in_seconds=int(nf_check.get("access_resets_in_seconds") or 0),
                            cooldown_until=(str(nf_check.get("cooldown_until")) if nf_check.get("cooldown_until") else None),
                        )
                except Exception:
                    pass

                # 当 remaining_count=0 时，参考 admin 接口逻辑尝试在窗口内刷新一次 access_token
                try:
                    if nf_check and int(nf_check.get("remaining_count") or 0) == 0:
                        info = await sora_fetch_access_token_in_window(sess=sess, target_url=target_url)
                        access_token = str((info or {}).get("access_token") or "").strip() or None
                        expires = str((info or {}).get("expires") or "").strip() or None
                        if access_token:
                            await self.db.update_task_type_window(
                                mapping_id=picked.mapping_id,
                                sora_access_token=access_token,
                                sora_access_expires=expires,
                            )
                            picked.sora_access_token = access_token
                            picked.sora_access_expires = expires
                            try:
                                sess.set_access_token(access_token, expires)
                            except Exception:
                                pass

                            try:
                                checked = await sess.api_nf_check(target_url=target_url)
                                nf_check = checked if isinstance(checked, dict) else None
                            except Exception as e:
                                nf_check_err = e
                                nf_check = None

                            try:
                                if nf_check and nf_check.get("remaining_count") is not None:
                                    await self.db.update_task_type_window(
                                        mapping_id=picked.mapping_id,
                                        remaining_quota=int(nf_check.get("remaining_count") or 0),
                                        sora_remaining_count=int(nf_check.get("remaining_count") or 0),
                                        sora_purchased_remaining_count=int(nf_check.get("purchased_remaining_count") or 0),
                                        sora_rate_limit_reached=bool(nf_check.get("rate_limit_reached", False)),
                                        sora_access_resets_in_seconds=int(nf_check.get("access_resets_in_seconds") or 0),
                                        cooldown_until=(str(nf_check.get("cooldown_until")) if nf_check.get("cooldown_until") else None),
                                    )
                            except Exception:
                                pass

                except Exception as e:
                    print("refresh access token error", e)
                    pass

                # 余额低/查询异常时倾向于回收会话，余额充足时保持会话热态
                try:
                    if nf_check_err is not None:
                        print("余额更新失败:", nf_check_err)
                        sess._schedule_idle_close()
                    else:
                        remaining = int((nf_check or {}).get("remaining_count") or 0)
                        if remaining <= 2:
                            sess._schedule_idle_close()
                        else:
                            sess._cancel_idle_close()
                except Exception:
                    pass

                return nf_check

            try:
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
                elif picked.create_task_handler == "sora_plus_register":
                    result = await asyncio.wait_for(
                        sora_plus_register(
                            payload,
                            progress_cb,
                            db=self.db,
                            window_pk=picked.window_pk,
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

                # Sora：单独把 generation_id 落库（用于后续按 generation_id 绑定窗口）
                try:
                    if isinstance(result, dict):
                        gid = str(result.get("generation_id") or "").strip() or None
                        if gid:
                            await self.db.update_task(task_id, generation_id=gid)
                except Exception:
                    pass

                await _refresh_sora_balance()
                # 清空一下result中的nf_check，避免敏感信息泄露
                if isinstance(result, dict):
                    result["nf_check"] = None
                await self.db.update_task(task_id, status="completed", progress=100, result=result, set_completed=True)
                #await self.db.consume_mapping_quota(picked.mapping_id, amount=1)
                await self.db.mark_mapping_success(picked.mapping_id)
                logger.info("task completed: %s", task_id)
            except Exception as e:
                await _refresh_sora_balance()
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
                    await self.db.mark_mapping_error(
                        picked.mapping_id,
                        threshold=picked.threshold,
                        cooldown_seconds=3600,
                        reset_on_threshold=False,
                    )
                    # 连续错误达到“关闭窗口阈值”的整数倍时，启动倒计时关闭窗口（不重置连续错误）
                    try:
                        st = await self.db.get_mapping_runtime_state(mapping_id=picked.mapping_id)
                        ce = int((st or {}).get("consecutive_errors") or 0)
                    except Exception:
                        ce = 0
                    close_thr = max(1, int(getattr(picked, "close_window_threshold", 1) or 1))
                    should_close = ce > 0 and (ce % close_thr == 0)
                    # 注意：仅对 Sora 窗口做处理；其它模拟执行器没有需要维护的浏览器会话
                    if should_close:
                        try:
                            sess = get_or_create_sora_session(
                                vendor=picked.browser_vendor,
                                base_url=picked.browser_base_url,
                                access_key=picked.browser_access_key,
                                space_id=picked.space_id,
                                window_key=picked.window_key,
                            )
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

