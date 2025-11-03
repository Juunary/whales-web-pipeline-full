# -*- coding: utf-8 -*-
import asyncio, os
from typing import Optional, Dict, Any
import httpx

HTTP_MAX_CONCURRENCY = int(os.getenv("HTTP_MAX_CONCURRENCY", "8"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "5"))
RETRY_STATUS = {429, 500, 502, 503, 504}  # backoff 대상
FATAL_STATUS = {400, 401, 403}             # 즉시 중단 (의미 없는 재시도 방지)

_sema = asyncio.Semaphore(HTTP_MAX_CONCURRENCY)

async def get_json(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    method: str = "GET",
    data: Optional[Dict[str, Any]] = None,
    retries: int = HTTP_RETRIES,
) -> Dict[str, Any]:
    async with _sema:
        for attempt in range(1, retries + 1):
            try:
                if method.upper() == "GET":
                    r = await client.get(url, params=params, headers=headers, timeout=60)
                else:
                    r = await client.post(url, params=params, headers=headers, json=data, timeout=60)
                # 치명 상태코드는 즉시 중단 (더 이상 재시도하지 않음)
                if r.status_code in FATAL_STATUS:
                    r.raise_for_status()
                # 정상/기타
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                code = e.response.status_code if e.response is not None else None
                # 재시도 가능한 상태코드만 백오프
                if code in RETRY_STATUS and attempt < retries:
                    await asyncio.sleep(1.25 * attempt)
                    continue
                raise
            except Exception:
                if attempt < retries:
                    await asyncio.sleep(1.25 * attempt)
                    continue
                raise
