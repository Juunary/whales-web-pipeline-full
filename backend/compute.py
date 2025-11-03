# -*- coding: utf-8 -*-
"""
Compute whale portfolio weights for 'today' (now, latest) and deltas vs T-1M/1W/24H.
- Primary "timestamp -> block" resolution via Moralis dateToBlock
- Fallback to Etherscan V2 getblocknobytime
- Last-resort fallback to Alchemy RPC binary search
- CoinGecko 24h changes with robust retry/backoff and Pro domain auto switch
"""

import os, asyncio, time, json
from typing import List, Dict, Optional
from datetime import datetime, timezone

import pandas as pd
import numpy as np
import httpx

from common.utils import get_json
from common.classify import classify_token
from common.token_lists import STABLES, BTC_PEGS

# -------------------------
# Endpoints & API Keys
# -------------------------
ETHERSCAN_API = "https://api.etherscan.io/v2/api"  # V2
MORALIS_BASE  = "https://deep-index.moralis.io/api/v2.2"
# CoinGecko: Pro 키면 pro-api, 아니면 public
COINGECKO_BASE= "https://pro-api.coingecko.com/api/v3" if os.getenv("COINGECKO_API_KEY") else "https://api.coingecko.com/api/v3"
ALCHEMY_RPC   = f"https://eth-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY','')}"

ETHERSCAN_KEY = os.getenv("ETHERSCAN_API_KEY","")
MORALIS_KEY   = os.getenv("MORALIS_API_KEY","")
COINGECKO_KEY = os.getenv("COINGECKO_API_KEY","")

HDR_MORALIS = {"X-API-Key": MORALIS_KEY, "accept":"application/json"}
HDR_CG = {"accept":"application/json"}
if COINGECKO_KEY:
    HDR_CG["x-cg-pro-api-key"] = COINGECKO_KEY

# -------------------------
# Time helpers
# -------------------------
def now_ts() -> int:
    return int(time.time())

# -------------------------
# Timestamp -> Block
# Priority: Moralis -> Etherscan V2 -> RPC
# -------------------------
async def moralis_block_by_time(client: httpx.AsyncClient, ts: int) -> int:
    """Moralis dateToBlock (primary)."""
    iso = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{MORALIS_BASE}/dateToBlock"
    params = {"chain": "eth", "date": iso}
    j = await get_json(client, url, params=params, headers=HDR_MORALIS)
    block = j.get("block")
    if block is None:
        raise RuntimeError(f"Moralis dateToBlock missing block: {j}")
    return int(block)

async def block_by_time_etherscan_v2(client: httpx.AsyncClient, ts: int) -> int:
    """Etherscan V2 getblocknobytime (secondary)."""
    params = {
        "chainid": "1",
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": ts,
        "closest": "before",
        "apikey": ETHERSCAN_KEY,
    }
    j = await get_json(client, ETHERSCAN_API, params=params)
    if j.get("status") == "1" and str(j.get("result","")).isdigit():
        return int(j["result"])
    raise RuntimeError(f"Etherscan NOTOK or V1 Deprecated: {j}")

async def rpc_block_number(client: httpx.AsyncClient) -> int:
    if not ALCHEMY_RPC:
        raise RuntimeError("ALCHEMY_API_KEY required for RPC fallback.")
    data = {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}
    r = await client.post(ALCHEMY_RPC, json=data, timeout=60)
    r.raise_for_status()
    return int(r.json()["result"], 16)

async def rpc_block_timestamp(client: httpx.AsyncClient, n: int) -> int:
    tag = hex(n)
    data = {"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":[tag, False]}
    r = await client.post(ALCHEMY_RPC, json=data, timeout=60)
    r.raise_for_status()
    j = r.json()
    ts_hex = j["result"]["timestamp"]
    return int(ts_hex, 16)

async def block_by_time_rpc(client: httpx.AsyncClient, ts: int) -> int:
    """Last-resort: binary search on Alchemy RPC."""
    try:
        latest = await rpc_block_number(client)
        ts_latest = await rpc_block_timestamp(client, latest)
        if ts >= ts_latest:
            return latest
        # Rough guess by 12s per block
        lo, hi = 0, latest
        lo_ts = await rpc_block_timestamp(client, lo)
        if ts <= lo_ts:
            return lo
        # Binary search
        for _ in range(64):
            mid = (lo + hi) // 2
            tsm = await rpc_block_timestamp(client, mid)
            if tsm == ts:
                return mid
            if tsm < ts:
                lo = mid + 1
            else:
                hi = mid - 1
            if lo > hi:
                break
        return hi if hi >= 0 else 0
    except httpx.HTTPStatusError as e:
        # 401 등 권한 문제일 때: 최신 블록으로 안전 탈출 (정밀도는 낮아지지만 파이프라인 지속)
        print(f"[rpc] block search failed ({e}), fallback to latest block approximation")
        r = await client.post(ALCHEMY_RPC, json={"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}, timeout=60)
        r.raise_for_status()
        return int(r.json()["result"], 16)

async def block_by_time(client: httpx.AsyncClient, ts: int) -> int:
    """Resolve block number for given unix ts with robust fallback chain."""
    # 1) Moralis
    try:
        return await moralis_block_by_time(client, ts)
    except Exception as e1:
        print(f"[dateToBlock] Moralis failed: {e1} → try Etherscan V2")
    # 2) Etherscan V2
    try:
        return await block_by_time_etherscan_v2(client, ts)
    except Exception as e2:
        print(f"[etherscan] getblocknobytime failed: {e2} → fallback to RPC")
    # 3) RPC
    return await block_by_time_rpc(client, ts)

# -------------------------
# On-chain helpers
# -------------------------
async def is_contract(client: httpx.AsyncClient, address: str, block_hex: str) -> bool:
    """EOA/Contract 판별 (eth_getCode != '0x'이면 컨트랙트)."""
    if not ALCHEMY_RPC:
        return False  # no RPC → don't exclude aggressively
    data = {"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":[address, block_hex]}
    j = await get_json(client, ALCHEMY_RPC, method="POST", data=data)
    code = j.get("result","0x")
    return code != "0x"

async def moralis_wallet_tokens(client: httpx.AsyncClient, address: str, block: Optional[int]):
    """
    Moralis wallets/:address/tokens (with to_block)
    Returns a list of tokens w/ usd_value if available.
    """
    url = f"{MORALIS_BASE}/wallets/{address}/tokens"
    params = {"chain":"eth","exclude_spam":"true","exclude_unverified_contracts":"true"}
    if block is not None:
        params["to_block"] = block
    j = await get_json(client, url, params=params, headers=HDR_MORALIS)
    return j.get("result", [])

# -------------------------
# CoinGecko 24h change (robust)
# -------------------------
async def coingecko_token_24h_change_by_contracts(client: httpx.AsyncClient, contracts: List[str]) -> Dict[str,float]:
    """
    CoinGecko 24h change with Pro domain if COINGECKO_API_KEY is set.
    Chunked, with 429/403 backoff; failures are logged but not fatal.
    """
    out: Dict[str, float] = {}
    if not contracts:
        return out
    chunk_size = int(os.getenv("CG_CHUNK", "60"))
    for i in range(0, len(contracts), chunk_size):
        chunk = contracts[i:i+chunk_size]
        url = f"{COINGECKO_BASE}/simple/token_price/ethereum"
        params = {"contract_addresses": ",".join(chunk), "vs_currencies": "usd", "include_24hr_change": "true"}
        for attempt in range(5):
            try:
                r = await client.get(url, params=params, headers=HDR_CG, timeout=60)
                if r.status_code in (429, 403):
                    ra = r.headers.get("retry-after")
                    delay = float(ra) if ra else 1.5*(attempt+1)
                    await asyncio.sleep(delay)
                    continue
                r.raise_for_status()
                j = r.json()
                for k, v in j.items():
                    out[k.lower()] = float(v.get("usd_24h_change", 0.0))
                break
            except Exception as e:
                if attempt == 4:
                    print(f"[coingecko] chunk {i//chunk_size} failed: {e}")
                else:
                    await asyncio.sleep(1.5*(attempt+1))
    return out

# -------------------------
# Portfolio math
# -------------------------
def weights_from_rows(rows: List[dict]):
    """
    rows: Moralis token objects (usd_value / token_address present)
    Returns: (total_usd, {'BTC':%, 'STABLE':%, 'ALT':%})
    """
    by_cat = {"BTC":0.0,"STABLE":0.0,"ALT":0.0}
    total = 0.0
    for r in rows:
        usd_value = float(r.get("usd_value") or 0.0)
        if usd_value <= 0:
            continue
        contract = r.get("token_address") or None
        cat = classify_token(contract)
        by_cat[cat] += usd_value
        total += usd_value
    if total <= 0:
        return 0.0, {"BTC":0.0,"STABLE":0.0,"ALT":0.0}
    return total, {k: (v/total*100.0) for k,v in by_cat.items()}

# -------------------------
# Main analysis entry
# -------------------------
async def analyze_addresses(addresses: List[str], exclude_contracts: bool = True) -> Dict:
    """
    Compute now(T) and T-1M/1W/24H snapshots, average weights, deltas and token 24h changes.
    """
    T = now_ts()
    T_1M, T_1W, T_24H = T - 30*86400, T - 7*86400, T - 86400

    async with httpx.AsyncClient(http2=True) as client:
        # Resolve blocks
        b_now = await block_by_time(client, T)
        b_1m  = await block_by_time(client, T_1M)
        b_1w  = await block_by_time(client, T_1W)
        b_24h = await block_by_time(client, T_24H)
        hex_now = hex(b_now)

        # Optionally exclude contract accounts
        if exclude_contracts:
            kept = []
            for i in range(0, len(addresses), 50):
                batch = addresses[i:i+50]
                res = await asyncio.gather(*[is_contract(client, a, hex_now) for a in batch], return_exceptions=True)
                for a, isctr in zip(batch, res):
                    if isinstance(isctr, Exception):
                        kept.append(a)  # keep if unknown
                    else:
                        if not isctr:
                            kept.append(a)
            addresses = kept

        # Snapshots at blocks
        async def snapshot(block: Optional[int]):
            out = {}
            for i in range(0, len(addresses), 25):
                batch = addresses[i:i+25]
                res = await asyncio.gather(*[moralis_wallet_tokens(client, a, block) for a in batch], return_exceptions=True)
                for a, r in zip(batch, res):
                    if isinstance(r, Exception):
                        continue
                    out[a] = r
            return out

        S_T   = await snapshot(b_now)
        S_1m  = await snapshot(b_1m)
        S_1w  = await snapshot(b_1w)
        S_24h = await snapshot(b_24h)

        # Compute per-wallet weights
        def compute_w(snp: Dict[str, List[dict]]):
            rows = []
            for a, toks in snp.items():
                total, w = weights_from_rows(toks)
                rows.append({"address":a, "total_usd": total, "BTC": w["BTC"], "STABLE": w["STABLE"], "ALT": w["ALT"]})
            df = pd.DataFrame(rows).fillna(0.0)
            return df

        W_T   = compute_w(S_T).sort_values("total_usd", ascending=False).reset_index(drop=True)
        W_1m  = compute_w(S_1m)
        W_1w  = compute_w(S_1w)
        W_24h = compute_w(S_24h)

        # Align & deltas (T - past)
        addrs = set(W_T["address"].tolist())

        def align(A: pd.DataFrame, B: pd.DataFrame) -> pd.DataFrame:
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

        # Averages (EW/VW)
        def averages(df: pd.DataFrame):
            ew = {"BTC": round(df["BTC"].mean(), 4), "STABLE": round(df["STABLE"].mean(), 4), "ALT": round(df["ALT"].mean(), 4)}
            v = df["total_usd"] / max(df["total_usd"].sum(), 1.0)
            vw = {c: round((df[c] * v).sum(), 4) for c in ["BTC","STABLE","ALT"]}
            return {"equal_weight": ew, "value_weight": vw}

        avg_T = averages(W_T)

        # Token 24h header: limit to frequent contracts + must-have sets
        from collections import Counter
        freq = Counter()
        for toks in S_T.values():
            for r in toks:
                c = (r.get("token_address") or "").lower()
                if c:
                    freq[c] += 1
        top_n = int(os.getenv("CG_MAX_CONTRACTS_PER_RUN", "200"))
        contracts = sorted(set([c for c, _ in freq.most_common(top_n)]) | set(STABLES) | set(BTC_PEGS))
        try:
            cg_24h = await coingecko_token_24h_change_by_contracts(client, contracts)
        except Exception as e:
            print(f"[coingecko] 24h change fetch failed (non-fatal): {e}")
            cg_24h = {}

        # Top-10 sample
        sample = W_T.head(10).copy()
        sample = sample[["total_usd","BTC","STABLE","ALT"]]
        sample.insert(0, "address", W_T.head(10)["address"].tolist())

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
