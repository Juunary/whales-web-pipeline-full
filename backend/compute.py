# -*- coding: utf-8 -*-
"""
Compute whale portfolio weights for 'today' (now) and deltas vs T-1M/1W/24H.

Hardenings:
- Timestamp->block: Moralis dateToBlock (primary) -> Etherscan V2 (secondary) -> degrade to latest (None)
- Contract exclusion: RPC(Alchemy) unhealthy/401 => auto-skip (don't block pipeline)
- Moralis tokens: if to_block call returns 400/500 => fallback to latest call (limit applied)
- DataFrames are empty-safe (always have required columns)
- CoinGecko: Pro auto-use, 429/403 backoff, top-N contracts only
"""

import os, asyncio, time
from typing import List, Dict, Optional
from datetime import datetime, timezone

import pandas as pd
import numpy as np
import httpx

from common.utils import get_json
from common.classify import classify_token
from common.token_lists import STABLES, BTC_PEGS

# -------------------------
# Endpoints & Keys
# -------------------------
ETHERSCAN_API = "https://api.etherscan.io/v2/api"  # V2
MORALIS_BASE  = "https://deep-index.moralis.io/api/v2.2"
COINGECKO_BASE= "https://pro-api.coingecko.com/api/v3" if os.getenv("COINGECKO_API_KEY") else "https://api.coingecko.com/api/v3"
ALCHEMY_RPC   = f"https://eth-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY','')}"

ETHERSCAN_KEY = os.getenv("ETHERSCAN_API_KEY","")
MORALIS_KEY   = os.getenv("MORALIS_API_KEY","")
COINGECKO_KEY = os.getenv("COINGECKO_API_KEY","")

HDR_MORALIS = {"X-API-Key": MORALIS_KEY, "accept":"application/json"}
HDR_CG = {"accept":"application/json"}
if COINGECKO_KEY:
    HDR_CG["x-cg-pro-api-key"] = COINGECKO_KEY

def now_ts() -> int:
    return int(time.time())

# -------------------------
# Timestamp -> Block (robust)
# -------------------------
async def _moralis_block_by_time(client: httpx.AsyncClient, ts: int) -> int:
    iso = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{MORALIS_BASE}/dateToBlock"
    params = {"chain": "eth", "date": iso}
    j = await get_json(client, url, params=params, headers=HDR_MORALIS)
    blk = j.get("block")
    if blk is None:
        raise RuntimeError(f"Moralis dateToBlock missing block: {j}")
    return int(blk)

async def _etherscan_block_by_time(client: httpx.AsyncClient, ts: int) -> int:
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
    raise RuntimeError(f"Etherscan NOTOK/V1: {j}")

async def block_by_time(client: httpx.AsyncClient, ts: int) -> Optional[int]:
    # 1) Moralis
    try:
        return await _moralis_block_by_time(client, ts)
    except Exception as e1:
        print(f"[dateToBlock] Moralis failed: {e1} → try Etherscan V2")
    # 2) Etherscan
    try:
        return await _etherscan_block_by_time(client, ts)
    except Exception as e2:
        print(f"[etherscan] getblocknobytime failed: {e2} → degrade to latest")
    # 3) degrade to latest
    return None

# -------------------------
# RPC health & EOA check (optional)
# -------------------------
async def rpc_healthy(client: httpx.AsyncClient) -> bool:
    if not ALCHEMY_RPC or ALCHEMY_RPC.endswith("/"):
        return False
    try:
        data = {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}
        r = await client.post(ALCHEMY_RPC, json=data, timeout=15)
        if r.status_code == 401:
            return False
        r.raise_for_status()
        return True
    except Exception:
        return False

async def is_contract_rpc(client: httpx.AsyncClient, address: str, block_hex_or_tag: str) -> bool:
    data = {"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":[address, block_hex_or_tag]}
    j = await get_json(client, ALCHEMY_RPC, method="POST", data=data)
    code = j.get("result","0x")
    return code != "0x"

# -------------------------
# Moralis wallet tokens (with fallback)
# -------------------------
async def moralis_wallet_tokens(client: httpx.AsyncClient, address: str, block: Optional[int]):
    """
    GET /wallets/:address/tokens
    - primary: with to_block (if provided)
    - fallback: without to_block (latest), to bypass occasional 400/500
    Docs show 'to_block' is supported. We fallback only when provider errors.  :contentReference[oaicite:1]{index=1}
    """
    base_params = {
        "chain":"eth",
        "exclude_spam":"true",
        "exclude_unverified_contracts":"true",
        "limit": 200,  # keep it reasonable; we don't paginate here
    }
    url = f"{MORALIS_BASE}/wallets/{address}/tokens"

    # Attempt 1: with to_block (if any)
    p1 = dict(base_params)
    if block is not None:
        p1["to_block"] = block
    try:
        j = await get_json(client, url, params=p1, headers=HDR_MORALIS)
        return j.get("result", [])
    except Exception as e:
        # Typical: 400/500 on certain block/address combos → fallback to latest
        print(f"[moralis] tokens (to_block={block}) failed for {address}: {e} → fallback latest")

    # Attempt 2: latest (no to_block)
    try:
        j = await get_json(client, url, params=base_params, headers=HDR_MORALIS)
        return j.get("result", [])
    except Exception as e2:
        print(f"[moralis] tokens latest failed for {address}: {e2}")
        return []

# -------------------------
# CoinGecko 24h change
# -------------------------
async def coingecko_token_24h_change_by_contracts(client: httpx.AsyncClient, contracts: List[str]) -> Dict[str,float]:
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
                    await asyncio.sleep(delay); continue
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
# Portfolio utils
# -------------------------
REQ_COLS = ["address","total_usd","BTC","STABLE","ALT"]

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

def _safe_df(rows: List[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=REQ_COLS)
    df = pd.DataFrame(rows)
    for c in REQ_COLS:
        if c not in df.columns:
            df[c] = 0.0 if c != "address" else ""
    return df[REQ_COLS].fillna(0.0)

def _safe_averages(df: pd.DataFrame) -> Dict[str, Dict[str,float]]:
    if df is None or df.empty:
        z = {"BTC":0.0,"STABLE":0.0,"ALT":0.0}
        return {"equal_weight": z, "value_weight": z}
    ew = {
        "BTC": round(float(np.nan_to_num(df["BTC"].mean(), nan=0.0)), 4),
        "STABLE": round(float(np.nan_to_num(df["STABLE"].mean(), nan=0.0)), 4),
        "ALT": round(float(np.nan_to_num(df["ALT"].mean(), nan=0.0)), 4),
    }
    tot_sum = float(df["total_usd"].sum())
    if tot_sum <= 0:
        vw = {"BTC":0.0,"STABLE":0.0,"ALT":0.0}
    else:
        v = df["total_usd"] / tot_sum
        vw = {c: round(float((df[c] * v).sum()), 4) for c in ["BTC","STABLE","ALT"]}
    return {"equal_weight": ew, "value_weight": vw}

# -------------------------
# Main analysis
# -------------------------
async def analyze_addresses(addresses: List[str], exclude_contracts: Optional[bool] = None) -> Dict:
    """
    exclude_contracts: True | False | None(auto by RPC health)
    """
    T = now_ts()
    T_1M, T_1W, T_24H = T - 30*86400, T - 7*86400, T - 86400

    async with httpx.AsyncClient(http2=True) as client:
        # Resolve blocks (None => latest)
        b_now = await block_by_time(client, T)
        b_1m  = await block_by_time(client, T_1M)
        b_1w  = await block_by_time(client, T_1W)
        b_24h = await block_by_time(client, T_24H)

        # Decide contract filtering
        do_exclude = exclude_contracts
        if do_exclude is None:
            do_exclude = await rpc_healthy(client)
            if not do_exclude:
                print("[analyze] RPC unhealthy or missing → skip contract exclusion")
        elif do_exclude is True:
            if not await rpc_healthy(client):
                print("[analyze] RPC unhealthy (401/timeout) → skip contract exclusion")
                do_exclude = False

        # Optional contract exclusion
        if do_exclude:
            hex_or_tag = hex(b_now) if isinstance(b_now, int) else "latest"
            kept = []
            for i in range(0, len(addresses), 25):
                batch = addresses[i:i+25]
                res = await asyncio.gather(
                    *[is_contract_rpc(client, a, hex_or_tag) for a in batch],
                    return_exceptions=True
                )
                for a, r in zip(batch, res):
                    if isinstance(r, Exception): kept.append(a)
                    else:
                        if not r: kept.append(a)   # keep EOAs
            addresses = kept

        # Snapshot helper (block=None -> latest)
        async def snapshot(block: Optional[int]):
            out = {}
            for i in range(0, len(addresses), 25):
                batch = addresses[i:i+25]
                res = await asyncio.gather(
                    *[moralis_wallet_tokens(client, a, block) for a in batch],
                    return_exceptions=True
                )
                for a, r in zip(batch, res):
                    if isinstance(r, Exception): continue
                    out[a] = r
            return out

        S_T   = await snapshot(b_now)
        S_1m  = await snapshot(b_1m)
        S_1w  = await snapshot(b_1w)
        S_24h = await snapshot(b_24h)

        def compute_w(snp: Dict[str, List[dict]]) -> pd.DataFrame:
            rows = []
            for a, toks in (snp or {}).items():
                total, w = weights_from_rows(toks)
                rows.append({"address":a, "total_usd": total, "BTC": w["BTC"], "STABLE": w["STABLE"], "ALT": w["ALT"]})
            return _safe_df(rows)

        W_T   = compute_w(S_T)
        if not W_T.empty:
            W_T = W_T.sort_values("total_usd", ascending=False).reset_index(drop=True)
        W_1m  = compute_w(S_1m)
        W_1w  = compute_w(S_1w)
        W_24h = compute_w(S_24h)

        addrs = set(W_T["address"].tolist())

        def align(A: pd.DataFrame, B: pd.DataFrame) -> pd.DataFrame:
            if A is None or B is None or A.empty or B.empty:
                return pd.DataFrame(columns=["BTC","STABLE","ALT"])
            a = A[A["address"].isin(addrs)].set_index("address")
            b = B[B["address"].isin(addrs)].set_index("address")
            both = a.join(b, lsuffix="_T", rsuffix="_P", how="inner")
            if both.empty:
                return pd.DataFrame(columns=["BTC","STABLE","ALT"])
            D = pd.DataFrame({
                "BTC": both["BTC_T"] - both["BTC_P"],
                "STABLE": both["STABLE_T"] - both["STABLE_P"],
                "ALT": both["ALT_T"] - both["ALT_P"]
            }, index=both.index)
            return D

        D_1m, D_1w, D_24h = align(W_T, W_1m), align(W_T, W_1w), align(W_T, W_24h)
        avg_T = _safe_averages(W_T)

        # Token 24h header: frequent contracts + must-have sets
        from collections import Counter
        freq = Counter()
        for toks in (S_T or {}).values():
            for r in toks:
                c = (r.get("token_address") or "").lower()
                if c: freq[c] += 1
        top_n = int(os.getenv("CG_MAX_CONTRACTS_PER_RUN", "200"))
        contracts = sorted(set([c for c, _ in freq.most_common(top_n)]) | set(STABLES) | set(BTC_PEGS))
        try:
            cg_24h = await coingecko_token_24h_change_by_contracts(client, contracts)
        except Exception as e:
            print(f"[coingecko] 24h change fetch failed (non-fatal): {e}")
            cg_24h = {}

        # Top-10 sample
        sample = (W_T.head(10) if not W_T.empty else pd.DataFrame(columns=REQ_COLS)).copy()
        if not sample.empty:
            sample = sample[["address","total_usd","BTC","STABLE","ALT"]]

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
