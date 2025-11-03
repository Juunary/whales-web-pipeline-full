# -*- coding: utf-8 -*-
import os, asyncio, time, json
from typing import List, Dict, Optional
import httpx 
import pandas as pd 

# --- 레이트리밋 및 동시성 제어 클래스 (New) ---
class Throttle:
    """단일 호출당 최소 간격을 적용하여 전역 RPS(초당 요청 수)를 제한합니다."""
    def __init__(self, rps: float = 8.0):
        self.min_interval = 1.0/float(rps)
        self._last = 0.0
    async def wait(self):
        now = time.perf_counter()
        d = self.min_interval - (now - self._last)
        if d > 0:
            await asyncio.sleep(d)
        self._last = time.perf_counter()

# --- 전역 제어 객체 ---
# 동시에 최대 40개의 요청을 허용합니다. (Morallis/Etherscan 등을 병렬 처리)
sema = asyncio.Semaphore(40) 
# 초당 최대 8회 요청으로 제한합니다. (안정적인 API 사용을 위해)
throttle = Throttle(8.0) 

# --- API Key 및 헤더 정의 ---
# API Keys를 환경 변수에서 가져옵니다.
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
MORALIS_API_KEY = os.getenv("MORALIS_API_KEY")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")

# Moralis 키를 가져옵니다. 
MORALIS_KEY = os.getenv("MORALIS_API_KEY","")
HDR_MORALIS = {"X-API-Key": MORALIS_KEY, "accept":"application/json"}
# Etherscan 헤더 정의
# CoinGecko 헤더 정의: 키가 있으면 Pro API 헤더를 추가합니다.
HDR_CG = {"accept":"application/json"}
if COINGECKO_API_KEY:
    HDR_CG["x-cg-pro-api-key"] = COINGECKO_API_KEY


def make_chunk(lst: List, size: int) -> List[List]:
    """리스트를 지정된 크기로 청크 분할합니다."""
    return [lst[i:i + size] for i in range(0, len(lst), size)]

async def get_json(client: httpx.AsyncClient, url: str, params=None, headers=None, method="GET", data=None, retries=5) -> dict:
    """
    HTTP GET/POST 요청을 수행하고 JSON 응답을 반환합니다. 
    Semaphore와 Throttle을 사용하여 동시성 및 레이트리밋을 제어합니다.
    """
    async with sema:
        await throttle.wait() # 전역 RPS 제한 적용
        for attempt in range(retries):
            try:
                if method == "GET":
                    # 타임아웃 60초 적용
                    r = await client.get(url, params=params, headers=headers, timeout=60)
                else:
                    # POST 요청 (e.g. Alchemy RPC)
                    r = await client.post(url, params=params, headers=headers, json=data, timeout=60)
                
                # HTTP 상태 코드가 4xx/5xx인 경우 예외 발생
                r.raise_for_status() 
                return r.json()
            except Exception as e:
                # 에러 발생 시 로그 출력 (선택 사항)
                print(f"[HTTP] Failed attempt {attempt+1}/{retries} for {url}. Error: {e}")
                
                if attempt < retries - 1:
                    # 지수적 백오프(Exponential Backoff)를 적용하여 재시도 전에 대기
                    await asyncio.sleep(1.1 * (attempt + 1))
                    continue
                
                # 최종 재시도까지 실패
                raise RuntimeError(f"HTTP failed after {retries} attempts: {url}")
