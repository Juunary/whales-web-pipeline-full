# -*- coding: utf-8 -*-
import os, asyncio, time, json
from typing import List, Dict, Optional
import pandas as pd
import numpy as np
import httpx
import logging
from collections import Counter
# common/utils의 수정된 버전 임포트
from common.utils import get_json, HDR_CG, ETHERSCAN_API_KEY, MORALIS_API_KEY, ALCHEMY_API_KEY 
from common.classify import classify_token
# CoinGecko 안정화를 위해 token_lists.py에서 STABLES, BTC_PEGS 임포트가 필요합니다.
# 해당 파일이 없다면 이 라인에서 에러가 날 수 있으므로, 임시로 기본 리스트를 사용합니다.
try:
    from common.token_lists import STABLES, BTC_PEGS
except ImportError:
    # Fallback for STABLES and BTC_PEGS if token_lists.py is missing
    STABLES = ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "0xdac17f958d2ee523a2206206994597c13d831ec7"] # USDC, USDT
    BTC_PEGS = ["0x2260fac5e5542a773aa44fbcfedf7e1ee42ad2b8"] # WBTC

logger = logging.getLogger(__name__)

# --- API 엔드포인트 설정 (COINGECKO_BASE 수정) ---
ETHERSCAN_API = "https://api.etherscan.io/api"
MORALIS_BASE  = "https://deep-index.moralis.io/api/v2.2"
# CoinGecko Base URL: If key exists, use Pro API domain; otherwise, use Public API domain.
COINGECKO_BASE= "https://pro-api.coingecko.com/api/v3" if os.getenv("COINGECKO_API_KEY") else "https://api.coingecko.com/api/v3"
ALCHEMY_RPC   = f"https://eth-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY','')}"

# --- API Key 가져오기 ---
ETHERSCAN_KEY = os.getenv("ETHERSCAN_API_KEY","")
MORALIS_KEY   = os.getenv("MORALIS_API_KEY","")
COINGECKO_KEY = os.getenv("COINGECKO_API_KEY","")

def now_ts() -> int:
    return int(time.time())

async def block_by_time(client: httpx.AsyncClient, ts: int) -> int:
    params = {"module":"block","action":"getblocknobytime","timestamp":ts,"closest":"before","apikey":ETHERSCAN_KEY}
    j = await get_json(client, ETHERSCAN_API, params=params)
    if j.get("status") == "1":
        return int(j["result"])
    raise RuntimeError(f"Etherscan getblocknobytime error: {j}")

async def is_contract(client: httpx.AsyncClient, address: str, block_hex: str) -> bool:
    if not ALCHEMY_RPC:
        return False
    data = {"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":[address, block_hex]}
    j = await get_json(client, ALCHEMY_RPC, method="POST", data=data)
    code = j.get("result","0x")
    return code != "0x"

async def moralis_wallet_tokens(client: httpx.AsyncClient, address: str, block: Optional[int]):
    url = f"{MORALIS_BASE}/wallets/{address}/tokens"
    params = {"chain":"eth","exclude_spam":"true","exclude_unverified_contracts":"true"}
    if block is not None:
        params["to_block"] = block
    j = await get_json(client, url, params=params, headers=HDR_MORALIS)
    return j.get("result", [])

async def coingecko_token_24h_change_by_contracts(client: httpx.AsyncClient, contracts: List[str]) -> Dict[str,float]:
    """
    Retrieves 24-hour price change for observed tokens using CoinGecko API.
    Uses Pro domain if API key is present. Includes retry and graceful failure logic for stability.
    Returns: {contract_address: change_percentage}
    """
    out: Dict[str, float] = {}
    if not contracts:
        return out
    
    # Set chunk size from env var or default to 60 (to manage rate limits)
    chunk_size = int(os.getenv("CG_CHUNK", "60"))
    
    for i in range(0, len(contracts), chunk_size):
        chunk = contracts[i:i+chunk_size]
        url = f"{COINGECKO_BASE}/simple/token_price/ethereum"
        params = {"contract_addresses": ",".join(chunk), "vs_currencies": "usd", "include_24hr_change": "true"}
        
        # 5 attempts per chunk
        for attempt in range(5):
            try:
                # Use raw client get for granular control over 429/403 errors
                r = await client.get(url, params=params, headers=HDR_CG, timeout=60)
                
                if r.status_code in (429, 403):
                    # Rate Limit or Forbidden: wait and retry
                    ra = r.headers.get("retry-after")
                    delay = float(ra) if ra and ra.isdigit() else 1.5*(attempt+1)
                    print(f"[CoinGecko] Rate limit/Forbidden. Retrying chunk {i//chunk_size} in {delay:.1f}s...")
                    await asyncio.sleep(delay)
                    continue
                
                r.raise_for_status() # Raise exception for other non-2xx status codes
                j = r.json()
                
                for k, v in j.items():
                    out[k.lower()] = float(v.get("usd_24h_change", 0.0))
                
                break # Success, move to next chunk
            
            except Exception as e:
                if attempt == 4:
                    # Final failure: log warning and move to next chunk (graceful fail)
                    logger.warning(f"[CoinGecko] Chunk {i//chunk_size} failed after 5 attempts: {e}")
                else:
                    # Intermediate failure: wait with exponential backoff
                    await asyncio.sleep(1.5*(attempt+1))
                    
    return out

def weights_from_rows(rows: List[dict]):
    by_cat = {"BTC":0.0,"STABLE":0.0,"ALT":0.0}
    total = 0.0
    for r in rows:
        usd_value = float(r.get("usd_value") or 0.0)
        if usd_value <= 0: continue
        contract = r.get("token_address") or None
        cat = classify_token(contract)
        by_cat[cat] += usd_value
        total += usd_value
    if total <= 0:
        return 0.0, {"BTC":0.0,"STABLE":0.0,"ALT":0.0}
    return total, {k: (v/total*100.0) for k,v in by_cat.items()}

async def analyze_addresses(addresses: List[str], exclude_contracts: bool = True) -> Dict:
    T = now_ts()
    T_1M, T_1W, T_24H = T-30*86400, T-7*86400, T-86400
    # Use httpx.AsyncClient for concurrent API calls
    async with httpx.AsyncClient(http2=True, follow_redirects=True) as client:
        # Get blocks for historical snapshots
        b_now = await block_by_time(client, T)
        b_1m = await block_by_time(client, T_1M)
        b_1w = await block_by_time(client, T_1W)
        b_24h = await block_by_time(client, T_24H)
        hex_now = hex(b_now)

        # Optional: Exclude contract addresses using Alchemy
        if exclude_contracts:
            kept = []
            for i in range(0, len(addresses), 50):
                batch = addresses[i:i+50]
                res = await asyncio.gather(*[is_contract(client, a, hex_now) for a in batch], return_exceptions=True)
                for a, isctr in zip(batch, res):
                    if isinstance(isctr, Exception): kept.append(a) # Keep address if Alchemy fails
                    else:
                        if not isctr: kept.append(a) # Keep if not a contract
            addresses = kept

        # Function to fetch token balances for a given block
        async def snapshot(block: Optional[int]):
            out = {}
            for i in range(0, len(addresses), 25):
                batch = addresses[i:i+25]
                # Concurrently fetch token balances using Moralis
                res = await asyncio.gather(*[moralis_wallet_tokens(client, a, block) for a in batch], return_exceptions=True)
                for a, r in zip(batch, res):
                    if isinstance(r, Exception): continue
                    out[a] = r
            return out

        # Get snapshots for T, T-1M, T-1W, T-24H
        S_T = await snapshot(b_now)
        S_1m = await snapshot(b_1m)
        S_1w = await snapshot(b_1w)
        S_24h = await snapshot(b_24h)

        # Function to compute allocation weights (BTC, STABLE, ALT) from snapshots
        def compute_w(snp: Dict[str, List[dict]]):
            rows = []
            for a, toks in snp.items():
                total, w = weights_from_rows(toks)
                rows.append({"address":a,"total_usd":total,"BTC":w["BTC"],"STABLE":w["STABLE"],"ALT":w["ALT"]})
            df = pd.DataFrame(rows).fillna(0.0)
            return df

        W_T = compute_w(S_T).sort_values("total_usd", ascending=False).reset_index(drop=True)
        W_1m = compute_w(S_1m)
        W_1w = compute_w(S_1w)
        W_24h = compute_w(S_24h)

        addrs = set(W_T["address"].tolist())

        # Function to compute delta (change in allocation)
        def align(A, B):
            a = A[A["address"].isin(addrs)].set_index("address")
            b = B[B["address"].isin(addrs)].set_index("address")
            both = a.join(b, lsuffix="_T", rsuffix="_P", how="inner")
            D = pd.DataFrame({
                "BTC": both["BTC_T"] - both["BTC_P"],
                "STABLE": both["STABLE_T"] - both["STABLE_P"],
                "ALT": both["ALT_T"] - both["ALT_P"]
            }, index=both.index)
            return D

        D_1m, D_1w, D_24h = align(W_T, W_1m), align(W_T, W_1w), align(W_T, W_24h)

        # Function to compute average portfolio weights
        def averages(df: pd.DataFrame):
            ew = {"BTC": round(df["BTC"].mean(), 4), "STABLE": round(df["STABLE"].mean(), 4), "ALT": round(df["ALT"].mean(), 4)}
            v = df["total_usd"] / max(df["total_usd"].sum(), 1.0)
            vw = {c: round((df[c] * v).sum(), 4) for c in ["BTC","STABLE","ALT"]}
            return {"equal_weight": ew, "value_weight": vw}

        avg_T = averages(W_T)

        # --- CoinGecko 24h Price Change Fetch ---
        # Limit contracts to the top N frequent ones + key stable/pegs to respect rate limits
        freq = Counter()
        for toks in S_T.values():
            for r in toks:
                c = (r.get("token_address") or "").lower()
                if c: freq[c] += 1
        
        # Max contracts to query (default 200)
        top_n = int(os.getenv("CG_MAX_CONTRACTS_PER_RUN", "200"))
        observed = [c for c, _ in freq.most_common(top_n)]
        
        # Combine observed top tokens with core Stablecoins and BTC pegs
        seed = set(observed) | set(STABLES) | set(BTC_PEGS)
        contracts = sorted(seed)
        
        try:
            cg_24h = await coingecko_token_24h_change_by_contracts(client, contracts)
        except Exception as e:
            # Non-fatal failure: if the function fails, it returns an empty dict and logging warning
            logger.warning(f"[CoinGecko] 24h change fetch failed (non-fatal): {e}")
            cg_24h = {}
        
        # Final Top 10 sample for display
        sample = W_T.head(10).copy()
        sample = sample[["total_usd","BTC","STABLE","ALT"]]
        sample.insert(0, "address", W_T.head(10)["address"].tolist())

        # Final result structure
        return {
            "blocks": {"now": b_now, "m1": b_1m, "w1": b_1w, "h24": b_24h},
            "averages_T": avg_T,
            "sample_top10": sample,
            "W_T": W_T,
            "D_1m": D_1m,
            "D_1w": D_1w,
            "D_24h": D_24h,
            "token_24h_change": cg_24h
        }
