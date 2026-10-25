"""Generic asynchronous polling image provider for task-based endpoints."""

import asyncio
import base64
import json
import math
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlparse

import aiohttp
from astrbot.api import logger

from .base import (
    BaseProvider,
    extract_error_message,
    extract_image_url_from_response,
    is_complete_endpoint_url,
    read_response_text_limited,
    summarize_payload_json_for_log,
    summarize_response_text_for_log,
    summarize_text_for_log,
    summarize_url_for_log,
)

DEFAULT_ASYNC_POLL_INTERVAL_SECONDS = 3.0


def extract_task_id_from_response(payload: Any) -> Optional[str]:
    """Recursively search for a task/job ID in common asynchronous response structures."""
    if not isinstance(payload, dict):
        return None

    # Priority keys for task identification
    id_keys = [
        "id",
        "task_id",
        "taskId",
        "job_id",
        "jobId",
        "request_id",
        "requestId",
    ]

    # Check top-level
    for key in id_keys:
        val = payload.get(key)
        if isinstance(val, (str, int)) and str(val).strip():
            return str(val).strip()

    # Check nested common containers (data, task, result, response)
    for container_key in ["data", "task", "result", "response"]:
        sub = payload.get(container_key)
        if isinstance(sub, dict):
            sub_id = extract_task_id_from_response(sub)
            if sub_id:
                return sub_id
        elif isinstance(sub, list) and sub and isinstance(sub[0], dict):
            sub_id = extract_task_id_from_response(sub[0])
            if sub_id:
                return sub_id

    return None


def _origin_of(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    return f"{parsed.scheme}://{parsed.netloc}"


_DEFAULT_ASYNC_SUCCESS_STATUSES = {"SUCCESS", "SUCCEEDED", "COMPLETED"}
_LIST_IMAGE_FIELDS = {"image_urls", "images", "urls"}
_PATH_TOKEN = re.compile(r"([^\.\[]+)|\[(\d*)\]")


def _render_async_template(template: str, endpoint: str, task_id: str = "") -> str:
    return (
        template.replace("{origin}", _origin_of(endpoint))
        .replace("{endpoint}", endpoint.rstrip("/"))
        .replace("{task_id}", task_id)
    )


def _is_list_image_field(field: str) -> bool:
    return field in _LIST_IMAGE_FIELDS or field.endswith("_urls")


def _lookup_json_path(payload: Any, path: str) -> Any:
    current = payload
    for match in _PATH_TOKEN.finditer(path.strip()):
        key, index_text = match.group(1), match.group(2)
        if key is not None:
            key = key.strip()
            if not key:
                continue
            if isinstance(current, dict) and key in current:
                current = current[key]
                continue
            if isinstance(current, list) and key.isdigit():
                index = int(key)
                if index < 0 or index >= len(current):
                    return None
                current = current[index]
                continue
            return None
        if not isinstance(current, list) or not current:
            return None
        index = 0 if index_text == "" else int(index_text)
        if index < 0 or index >= len(current):
            return None
        current = current[index]
    return current


def _coerce_configured_image(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith(("http://", "https://", "data:image")):
            return text
        return ""
    if isinstance(value, list):
        for item in value:
            found = _coerce_configured_image(item)
            if found:
                return found
        return ""
    if isinstance(value, dict):
        for key in ("url", "image_url", "imageUrl"):
            found = _coerce_configured_image(value.get(key))
            if found:
                return found
    return ""


class CustomAsyncEndpointProvider(BaseProvider):
    """Generic asynchronous endpoint provider.
    Submits generation tasks, extracts task ID, and polls until completion.
    """

    async def _encode_reference_images(self, ref_images: List[str]) -> List[str]:
        loaded_refs = await self.load_reference_images(ref_images)
        return [
            f"data:{mime_type};base64," + base64.b64encode(image_bytes).decode("utf-8")
            for image_bytes, mime_type in loaded_refs
        ]

    def _endpoint(self) -> str:
        endpoint = str(self.config.base_url or "").strip()
        if not is_complete_endpoint_url(endpoint):
            raise ValueError(
                "异步轮询节点必须填写完整请求路径，例如 "
                "https://api.example.com/v1/images/generations，不能只填域名或 /v1。"
            )
        return endpoint

    def _configured_text(self, name: str) -> str:
        return str(getattr(self.config, name, "") or "").strip()

    def _resolved_upload_url(self) -> str:
        """Reference upload URL, only when the node provides a template."""
        template = self._configured_text("async_upload_template")
        if template:
            return _render_async_template(template, self._endpoint())
        return ""

    def _resolved_image_field(self) -> str:
        return self._configured_text("async_image_field")

    def _success_statuses(self) -> set:
        raw = self._configured_text("async_success_status")
        if not raw:
            return set(_DEFAULT_ASYNC_SUCCESS_STATUSES)
        return {part.strip().upper() for part in re.split(r"[\s,，;；]+", raw) if part.strip()}

    def _extract_result_image(self, data: Any, base_url: str) -> str:
        path = self._configured_text("async_image_path")
        if path:
            return _coerce_configured_image(_lookup_json_path(data, path))
        return extract_image_url_from_response(data, base_url)

    def _build_poll_url(self, endpoint: str, task_id: str) -> str:
        """Derive status/poll endpoint URL from submission URL and task_id.

        Without a template the task id is appended to the submit path. A vendor
        that serves tasks from another path must set async_poll_template.
        """
        safe_task_id = quote(str(task_id).strip(), safe="")
        template = self._configured_text("async_poll_template")
        if template:
            return _render_async_template(template, endpoint, safe_task_id)
        return f"{endpoint.rstrip('/')}/{safe_task_id}"

    def _build_payload(self, prompt: str, encoded_images: List[str], api_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"model": self.config.model, "prompt": prompt}
        if self.config.default_size:
            payload["size"] = self.config.default_size
        for index, image_url in enumerate(encoded_images[:3]):
            payload["image" if index == 0 else f"image{index + 1}"] = image_url
        payload.update(api_kwargs)
        return payload

    def _build_reference_payload(
        self,
        prompt: str,
        images: List[str],
        api_kwargs: Dict[str, Any],
        field: str,
        as_list: bool,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"model": self.config.model, "prompt": prompt}
        if self.config.default_size:
            payload["size"] = self.config.default_size
        if images:
            if as_list:
                payload[field] = list(images)
            else:
                payload[field] = images[0]
                for index, image in enumerate(images[1:], start=2):
                    payload[f"{field}{index}"] = image
        payload.update(api_kwargs)
        return payload

    async def _upload_reference_image(
        self,
        upload_url: str,
        authorization: str,
        image_bytes: bytes,
        mime_type: str,
        index: int,
    ) -> str:
        media_type = (mime_type or "image/png").split(";", 1)[0].strip().lower() or "image/png"
        extension = {
            "image/jpeg": "jpg",
            "image/png": "png",
            "image/webp": "webp",
            "image/gif": "gif",
        }.get(media_type, "png")
        form = aiohttp.FormData()
        form.add_field(
            "file",
            image_bytes,
            filename=f"ref-{index}.{extension}",
            content_type=media_type,
        )
        timeout_obj = aiohttp.ClientTimeout(total=min(60.0, float(self.config.timeout)))
        logger.info("📤 [异步轮询通道] 上传参考图: " + summarize_url_for_log(upload_url))
        async with self.session.post(
            upload_url,
            data=form,
            headers={"Authorization": authorization},
            timeout=timeout_obj,
        ) as response:
            text_body = await read_response_text_limited(response)
            if response.status >= 400:
                logger.error("💥 参考图上传失败: " + summarize_response_text_for_log(text_body, max_string_length=500))
                raise RuntimeError(
                    f"参考图上传失败 HTTP {response.status}: {extract_error_message(text_body)}"
                )
            try:
                uploaded = json.loads(text_body)
            except Exception as exc:
                raise RuntimeError("参考图上传未返回 JSON") from exc
            image_url = uploaded.get("url") if isinstance(uploaded, dict) else ""
            if not isinstance(image_url, str) or not image_url.strip():
                raise RuntimeError("参考图上传响应缺少 url")
            return image_url.strip()

    async def _reference_image_urls(self, upload_url: str, authorization: str, ref_images: List[str]) -> List[str]:
        image_urls: List[str] = []
        for index, ref in enumerate(ref_images, start=1):
            parsed = urlparse(str(ref).strip())
            if parsed.scheme.lower() in {"http", "https"}:
                image_urls.append(str(ref).strip())
                continue
            loaded = await self.load_reference_images([ref], max_images=1)
            image_bytes, mime_type = loaded[0]
            image_urls.append(
                await self._upload_reference_image(upload_url, authorization, image_bytes, mime_type, index)
            )
        return image_urls

    async def _submit_task(self, endpoint: str, headers: Dict[str, str], payload: Dict[str, Any]) -> Any:
        timeout_obj = aiohttp.ClientTimeout(total=min(60.0, self.config.timeout))
        logger.info(f"📤 [异步轮询通道] 提交任务完整路径: {summarize_url_for_log(endpoint)}")
        logger.info(f"📤 [异步轮询通道] 请求体摘要: {summarize_payload_json_for_log(payload)}")
        async with self.session.post(endpoint, json=payload, headers=headers, timeout=timeout_obj) as response:
            text = await read_response_text_limited(response)
            if response.status >= 400:
                logger.error("💥 异步轮询通道提交任务失败: " + summarize_response_text_for_log(text, max_string_length=500))
                raise RuntimeError(f"HTTP {response.status}: {extract_error_message(text)}")
            try:
                return json.loads(text)
            except Exception:
                return text

    async def _poll_task(self, poll_url: str, headers: Dict[str, str], deadline: float) -> str:
        attempt = 0
        consecutive_errors = 0
        interval = DEFAULT_ASYNC_POLL_INTERVAL_SECONDS

        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError(f"异步任务在 {self.config.timeout} 秒内未完成，已超时。")

            if attempt > 0:
                await asyncio.sleep(min(interval, remaining))
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError(f"异步任务在 {self.config.timeout} 秒内未完成，已超时。")

            attempt += 1
            logger.info(f"⏳ [异步轮询通道] 正在轮询任务状态 (尝试 {attempt}): {summarize_url_for_log(poll_url)}")

            try:
                req_timeout = aiohttp.ClientTimeout(total=min(20.0, remaining))
                async with self.session.get(poll_url, headers=headers, timeout=req_timeout) as response:
                    text = await read_response_text_limited(response)
                    if response.status >= 400:
                        consecutive_errors += 1
                        logger.warning(
                            f"⚠️ 轮询返回 HTTP {response.status}: {summarize_response_text_for_log(text, max_string_length=200)}"
                        )
                        if consecutive_errors >= 4:
                            raise RuntimeError(f"轮询连续失败 (HTTP {response.status}): {extract_error_message(text)}")
                        continue

                    consecutive_errors = 0
                    try:
                        data = json.loads(text)
                    except Exception:
                        data = text

            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                consecutive_errors += 1
                logger.warning(f"⚠️ 轮询网络异常: {err}")
                if consecutive_errors >= 4:
                    raise RuntimeError(f"轮询连续发生网络异常: {err}") from err
                continue

            # Check if image is directly present
            image_url = self._extract_result_image(data, poll_url)
            if image_url:
                logger.info(f"✅ [异步轮询通道] 轮询成功提取到图像 URL")
                return image_url

            if isinstance(data, dict):
                raw_status = str(
                    data.get("status")
                    or data.get("task_status")
                    or data.get("state")
                    or (data.get("data") if isinstance(data.get("data"), dict) else {}).get("status", "")
                ).upper()

                if raw_status in {"FAILURE", "FAILED", "ERROR", "CANCELLED"}:
                    error_payload: Any = data
                    nested = data.get("data") if isinstance(data, dict) else None
                    if isinstance(nested, dict) and nested.get("error"):
                        error_payload = nested
                    error_msg = extract_error_message(error_payload)
                    raise RuntimeError(f"异步任务执行失败: {error_msg}")

                if raw_status in self._success_statuses():
                    # Already checked extract_image_url_from_response above, but maybe nested
                    if image_url:
                        return image_url
                    raise ValueError("异步任务显示已完成，但未找到可用的图像数据: " + summarize_payload_json_for_log(data))

        raise TimeoutError("异步轮询超时。")

    async def generate_image(self, prompt: str, **kwargs: Any) -> str:
        current_key = self.get_current_key()
        if not current_key:
            raise ValueError("节点未配置 API Key！")
        if not self.config.model:
            raise ValueError("异步轮询节点未配置模型名！")

        endpoint = self._endpoint()
        ref_images = self.get_reference_images(**kwargs)
        image_field = self._resolved_image_field()
        upload_url = self._resolved_upload_url()
        list_mode = _is_list_image_field(image_field) if image_field else False
        max_refs = 16 if list_mode else 3
        if ref_images and len(ref_images) > max_refs:
            raise ValueError(f"异步轮询接口最多支持 {max_refs} 张参考图。")

        api_kwargs = self.filter_api_kwargs(kwargs)
        headers = {
            "Authorization": f"Bearer {current_key}",
            "Content-Type": "application/json",
        }

        logger.info(f"📝 [异步轮询通道] 提交提示词:\n{prompt}")
        if upload_url:
            if not image_field:
                raise ValueError(
                    "已配置参考图上传地址，但未配置参考图字段名，无法提交参考图。请填写 async_image_field。"
                )
            image_urls = await self._reference_image_urls(upload_url, headers["Authorization"], ref_images)
            field = image_field
            payload = self._build_reference_payload(
                prompt,
                image_urls,
                api_kwargs,
                field,
                as_list=_is_list_image_field(field),
            )
        elif image_field:
            encoded_images = await self._encode_reference_images(ref_images)
            payload = self._build_reference_payload(
                prompt,
                encoded_images,
                api_kwargs,
                image_field,
                as_list=list_mode,
            )
        else:
            encoded_images = await self._encode_reference_images(ref_images)
            payload = self._build_payload(prompt, encoded_images, api_kwargs)

        submission_resp = await self._submit_task(endpoint, headers, payload)

        # 1. First check if response already contains final image (synchronous or instant result)
        direct_image = self._extract_result_image(submission_resp, endpoint)
        if direct_image:
            logger.info("✅ [异步轮询通道] 提交后直接返回图像")
            return direct_image

        # 2. Extract task ID for polling
        task_id = extract_task_id_from_response(submission_resp)
        if not task_id:
            summary = (
                summarize_payload_json_for_log(submission_resp, max_string_length=500)
                if isinstance(submission_resp, (dict, list))
                else summarize_text_for_log(str(submission_resp), max_string_length=500)
            )
            raise ValueError(f"异步提交未返回有效的 task_id，响应内容: {summary}")

        logger.info(f"⏳ [异步轮询通道] 成功获取任务 ID: {task_id}")
        poll_url = self._build_poll_url(endpoint, task_id)
        deadline = asyncio.get_running_loop().time() + float(self.config.timeout)

        return await self._poll_task(poll_url, headers, deadline)
