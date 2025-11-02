# scraper/scrape_etherscan.py
# -*- coding: utf-8 -*-
import asyncio, csv, re, os, random, time
from pathlib import Path
from typing import List, Tuple
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

ADDRESS_RE = re.compile(r"0x[a-fA-F0-9]{40}")
EXCH = {k.lower() for k in [
    "binance","coinbase","kraken","okx","okex","huobi","htx","bitfinex","bybit",
    "kucoin","gate.io","crypto.com","gemini","upbit","bithumb","mexc","bitstamp","poloniex"
]}

def looks_exchange(n):
    n = (n or "").lower()
    return any(k in n for k in EXCH)

async def accept_cookies(page):
    # Etherscan 쿠키 배너(OneTrust) 처리
    for sel in [
        "#onetrust-accept-btn-handler",
        "button:has-text('I agree')",
        "text=Accept All",
        "text=Allow all"
    ]:
        try:
            await page.locator(sel).click(timeout=2000)
            await asyncio.sleep(0.2)
            break
        except:
            pass

def parse_rows_from_html(html: str) -> List[Tuple[str, str]]:
    rows = []
    # 느슨하게 tr/td를 파지 말고, 페이지 전체에서 주소를 긁고 인접 텍스트를 name tag로 추정
    # (Etherscan 구조 변경 대비)
    for m in ADDRESS_RE.finditer(html):
        addr = m.group(0)
        # 근방 문자열에서 name tag 비스무리한 부분 추출(보조적)
        start = max(m.start() - 200, 0)
        end   = min(m.end() + 200, len(html))
        ctx = html[start:end]
        # 간단히 >Name Tag</a> 같은 패턴 힌트
        nt = ""
        mm = re.search(r"Name\s*Tag.*?>\s*([^<]{1,64})<", ctx, flags=re.I)
        if mm:
            nt = mm.group(1).strip()
        rows.append((addr, nt))
    # 주소 중복 제거(뒤쪽 name tag 우선)
    seen = {}
    for a, nt in rows:
        seen[a] = nt or seen.get(a, "")
    return [(a, nt) for a, nt in seen.items()]

async def parse_page(page, url: str, wait_timeout_ms: int) -> List[Tuple[str, str]]:
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await accept_cookies(page)

    # 자동화 탐지 회피 스크립트(약함)
    try:
        await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
    except:
        pass

    # 1차: DOM에 붙었는지(attached)만 확인 (visible은 배너/레이어에 막힐 수 있음)
    try:
        await page.wait_for_selector("table tbody tr", state="attached", timeout=wait_timeout_ms)
    except PWTimeout:
        # 폴백: HTML 통째로 파싱 시도 (반복적으로 UI 블락되면 이 경로가 살길)
        html = await page.content()
        # 봇 차단/인증 페이지 감지
        lower = html.lower()
        if any(s in lower for s in ["verify you are human", "cf-browser-verification", "just a moment"]):
            raise RuntimeError("Blocked by anti-bot challenge")
        return parse_rows_from_html(html)

    # DOM 파싱
    rows = []
    trs = await page.query_selector_all("table tbody tr")
    for tr in trs:
        txt = (await tr.inner_text()).strip()
        m = ADDRESS_RE.search(txt)
        if not m:
            continue
        addr = m.group(0)
        tds = await tr.query_selector_all("td")
        nametag = ""
        if len(tds) >= 2:
            try:
                nametag = (await tds[1].inner_text()).strip()
            except:
                nametag = ""
        rows.append((addr, nametag))
    return rows

async def scrape(pages: int, ps: int, outpath: str, retries: int, sleep_ms: int, wait_timeout_ms: int):
    base = "https://etherscan.io/accounts/{page}?ps={ps}"
    out = Path(outpath); out.parent.mkdir(parents=True, exist_ok=True)
    seen = set(); results = []

    ua = os.getenv("SCRAPER_UA",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"
        ])
        context = await browser.new_context(
            user_agent=ua,
            viewport={"width": 1366, "height": 900},
            locale="en-US",
            timezone_id="UTC"
        )
        # 리소스 절약(속도↑)
        await context.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image","font","media"] else route.continue_())
        page = await context.new_page()

        for i in range(1, pages+1):
            url = base.format(page=i, ps=ps)
            ok = False
            for attempt in range(1, retries+1):
                try:
                    rows = await parse_page(page, url, wait_timeout_ms)
                    # 누적/필터
                    added = 0
                    for addr, nt in rows:
                        if addr not in seen:
                            seen.add(addr)
                            if looks_exchange(nt):  # CEX 제거
                                continue
                            results.append((addr, nt))
                            added += 1
                    print(f"[scrape] page {i}/{pages} attempt {attempt}: +{added} (total={len(results)})")
                    ok = True
                    break
                except Exception as e:
                    print(f"[warn] page {i} attempt {attempt} error: {e}")
                    await asyncio.sleep(0.4 * attempt)
            # 페이지 간 랜덤 슬립(봇탐지 우회 도움)
            await asyncio.sleep((sleep_ms + random.randint(0, 300)) / 1000.0)
            if not ok:
                # 완전 실패한 페이지는 건너뛰되, 전체는 계속
                continue

        await context.close()
        await browser.close()

    # 저장
    with out.open("w", newline="") as f:
        w = csv.writer(f); w.writerow(["address","nametag"]); w.writerows(results)
    print(f"[done] {out} ({len(results)} addresses, after CEX keyword filter)")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=int(os.getenv("SCRAPER_PAGES", "400")))
    ap.add_argument("--ps", type=int, default=int(os.getenv("SCRAPER_PS", "100")))
    ap.add_argument("--out", type=str, default="data/addresses.csv")
    ap.add_argument("--retries", type=int, default=int(os.getenv("SCRAPER_RETRIES", "3")))
    ap.add_argument("--sleep-ms", type=int, default=int(os.getenv("SCRAPER_SLEEP_MS", "600")))
    ap.add_argument("--wait-timeout-ms", type=int, default=int(os.getenv("SCRAPER_WAIT_MS", "20000")))
    args = ap.parse_args()
    asyncio.run(scrape(args.pages, args.ps, args.out, args.retries, args.sleep_ms, args.wait_timeout_ms))
