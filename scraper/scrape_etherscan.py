# -*- coding: utf-8 -*-
import asyncio, csv, re
from pathlib import Path
from playwright.async_api import async_playwright

ADDRESS_RE = re.compile(r"0x[a-fA-F0-9]{40}")
EXCH = {k.lower() for k in [
    "binance","coinbase","kraken","okx","okex","huobi","htx","bitfinex","bybit",
    "kucoin","gate.io","crypto.com","gemini","upbit","bithumb","mexc","bitstamp","poloniex"
]}

def looks_exchange(n):
    n = (n or "").lower()
    return any(k in n for k in EXCH)

async def parse_page(page, url: str):
    await page.goto(url, wait_until="networkidle")
    await page.wait_for_selector("table")
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

async def scrape(pages: int, ps: int, outpath: str):
    base = "https://etherscan.io/accounts/{page}?ps={ps}"
    out = Path(outpath); out.parent.mkdir(parents=True, exist_ok=True)
    seen = set(); results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        for i in range(1, pages+1):
            url = base.format(page=i, ps=ps)
            try:
                rows = await parse_page(page, url)
                for a, nt in rows:
                    if a in seen: continue
                    seen.add(a)
                    if looks_exchange(nt):  # CEX 바로 제외
                        continue
                    results.append((a, nt))
                print(f"[scrape] page {i}/{pages} → cumulative {len(results)}")
            except Exception as e:
                print(f"[warn] page {i} error: {e}")
        await browser.close()
    with out.open("w", newline="") as f:
        w = csv.writer(f); w.writerow(["address","nametag"]); w.writerows(results)
    print(f"[done] {out} ({len(results)} rows)")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=400)
    ap.add_argument("--ps", type=int, default=100)
    ap.add_argument("--out", type=str, default="data/addresses.csv")
    args = ap.parse_args()
    asyncio.run(scrape(args.pages, args.ps, args.out))
