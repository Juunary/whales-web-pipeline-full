# -*- coding: utf-8 -*-
import os, asyncio, time, json
from typing import List, Dict, Optional
import pandas as pd
import numpy as np
import httpx
from common.utils import get_json
from common.classify import classify_token

ETHERSCAN_API = "https://api.etherscan.io/v2/api"
MORALIS_BASE  = "https://deep-index.moralis.io/api/v2.2"
COINGECKO_BASE= "https://api.coingecko.com/api/v3"
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

async def block_by_time(client: httpx.AsyncClient, ts: int) -> int:
    params = {"chainid":"1","module":"block","action":"getblocknobytime","timestamp":ts,"closest":"before","apikey":ETHERSCAN_KEY}
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
    out = {}
    if not contracts: return out
    for i in range(0, len(contracts), 120):
        chunk = contracts[i:i+120]
        url = f"{COINGECKO_BASE}/simple/token_price/ethereum"
        params = {"contract_addresses":",".join(chunk),"vs_currencies":"usd","include_24hr_change":"true"}
        j = await get_json(client, url, params=params, headers=HDR_CG)
        for k, v in j.items():
            out[k.lower()] = float(v.get("usd_24h_change", 0.0))
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
    async with httpx.AsyncClient(http2=True) as client:
        b_now = await block_by_time(client, T)
        b_1m  = await block_by_time(client, T_1M)
        b_1w  = await block_by_time(client, T_1W)
        b_24h = await block_by_time(client, T_24H)
        hex_now = hex(b_now)

        if exclude_contracts:
            kept = []
            for i in range(0, len(addresses), 50):
                batch = addresses[i:i+50]
                res = await asyncio.gather(*[is_contract(client, a, hex_now) for a in batch], return_exceptions=True)
                for a, isctr in zip(batch, res):
                    if isinstance(isctr, Exception): kept.append(a)
                    else:
                        if not isctr: kept.append(a)
            addresses = kept

        async def snapshot(block: Optional[int]):
            out = {}
            for i in range(0, len(addresses), 25):
                batch = addresses[i:i+25]
                res = await asyncio.gather(*[moralis_wallet_tokens(client, a, block) for a in batch], return_exceptions=True)
                for a, r in zip(batch, res):
                    if isinstance(r, Exception): continue
                    out[a] = r
            return out

        S_T   = await snapshot(b_now)
        S_1m  = await snapshot(b_1m)
        S_1w  = await snapshot(b_1w)
        S_24h = await snapshot(b_24h)

        def compute_w(snp: Dict[str, List[dict]]):
            rows = []
            for a, toks in snp.items():
                total, w = weights_from_rows(toks)
                rows.append({"address":a,"total_usd":total,"BTC":w["BTC"],"STABLE":w["STABLE"],"ALT":w["ALT"]})
            df = pd.DataFrame(rows).fillna(0.0)
            return df

        W_T   = compute_w(S_T).sort_values("total_usd", ascending=False).reset_index(drop=True)
        W_1m  = compute_w(S_1m)
        W_1w  = compute_w(S_1w)
        W_24h = compute_w(S_24h)

        addrs = set(W_T["address"].tolist())

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

        def averages(df: pd.DataFrame):
            ew = {"BTC": round(df["BTC"].mean(), 4), "STABLE": round(df["STABLE"].mean(), 4), "ALT": round(df["ALT"].mean(), 4)}
            v = df["total_usd"] / max(df["total_usd"].sum(), 1.0)
            vw = {c: round((df[c] * v).sum(), 4) for c in ["BTC","STABLE","ALT"]}
            return {"equal_weight": ew, "value_weight": vw}

        avg_T = averages(W_T)

        observed_contracts = set()
        for toks in S_T.values():
            for r in toks:
                c = (r.get("token_address") or "").lower()
                if c: observed_contracts.add(c)
        cg_24h = await coingecko_token_24h_change_by_contracts(client, sorted(list(observed_contracts)))

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
