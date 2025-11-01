# -*- coding: utf-8 -*-
import asyncio, time, httpx

class Throttle:
    def __init__(self, rps: float = 8.0):
        self.min_interval = 1.0/float(rps)
        self._last = 0.0
    async def wait(self):
        now = time.perf_counter()
        d = self.min_interval - (now - self._last)
        if d > 0:
            await asyncio.sleep(d)
        self._last = time.perf_counter()

sema = asyncio.Semaphore(40)
throttle = Throttle(8.0)

async def get_json(client: httpx.AsyncClient, url: str, params=None, headers=None, method="GET", data=None, retries=5):
    async with sema:
        await throttle.wait()
        for attempt in range(retries):
            try:
                if method == "GET":
                    r = await client.get(url, params=params, headers=headers, timeout=60)
                else:
                    r = await client.post(url, params=params, headers=headers, json=data, timeout=60)
                r.raise_for_status()
                return r.json()
            except Exception:
                await asyncio.sleep(1.1*(attempt+1))
        raise RuntimeError(f"HTTP failed: {url}")
