# -*- coding: utf-8 -*-
import os, asyncio, time, json
from typing import List, Dict, Optional
import pandas as pd
import numpy as np
import httpx
from common.utils import get_json # get_json은 common/utils.py에서 정의됨
from common.classify import classify_token
from common.token_lists import STABLES, BTC_PEGS
from collections import Counter 

# --- API Endpoints ---
# ETHERSCAN_API는 V1/V2 관계없이 기본 도메인 사용 (V2 마이그레이션 권고에 따른 변경)
ETHERSCAN_API = "https://api.etherscan.io/api"
MORALIS_BASE  = "https://deep-index.moralis.io/api/v2.2"
# CoinGecko: Pro 키 있으면 Pro 도메인으로 자동 전환 (whales.yml의 시크릿에 의존)
COINGECKO_BASE= "https://pro-api.coingecko.com/api/v3" if os.getenv("COINGECKO_API_KEY") else "https://api.coingecko.com/api/v3"
ALCHEMY_RPC   = f"https://eth-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY','')}"

ETHERSCAN_KEY = os.getenv("ETHERSCAN_API_KEY","")
MORALIS_KEY   = os.getenv("MORALIS_API_KEY","")
COINGECKO_KEY = os.getenv("COINGECKO_API_KEY","")

HDR_MORALIS = {"X-API-Key": MORALIS_KEY, "accept":"application/json"}
# HDR_CG는 common/utils.py에서 정의되므로 여기서는 필요 없음

def now_ts() -> int:
    """현재 타임스탬프를 반환합니다."""
    return int(time.time())

# --- Etherscan / Alchemy 블록 조회 로직 ---

async def rpc_block_number(client: httpx.AsyncClient) -> int:
    """Alchemy RPC를 사용하여 현재 블록 번호를 조회합니다."""
    if not ALCHEMY_RPC:
        raise RuntimeError("ALCHEMY_API_KEY required for RPC fallback.")
    data = {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}
    r = await client.post(ALCHEMY_RPC, json=data, timeout=60)
    r.raise_for_status()
    return int(r.json()["result"], 16)

async def rpc_block_timestamp(client: httpx.AsyncClient, n: int) -> int:
    """Alchemy RPC를 사용하여 특정 블록 번호의 타임스탬프를 조회합니다."""
    tag = hex(n)
    data = {"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":[tag, False]}
    r = await client.post(ALCHEMY_RPC, json=data, timeout=60)
    r.raise_for_status()
    j = r.json()
    if not j.get("result"):
        raise RuntimeError(f"RPC getBlockByNumber failed for block {n}")
    ts_hex = j["result"]["timestamp"]
    return int(ts_hex, 16)

async def block_by_time_rpc(client: httpx.AsyncClient, ts: int) -> int:
    """Alchemy RPC만 사용하여 타임스탬프에 맞는 블록을 이진 탐색합니다."""
    latest = await rpc_block_number(client)
    ts_latest = await rpc_block_timestamp(client, latest)
    
    if ts >= ts_latest:
        return latest
    
    # 이진 탐색 초기화
    lo, hi = 0, latest 
    
    # 이진 탐색 (최대 60회 반복)
    for _ in range(60):
        mid = (lo + hi) // 2
        # Alchemy 호출 (RPC 블록 정보 함수는 get_json을 사용하지 않고 직접 httpx.AsyncClient를 사용하여 제어)
        tsm = await rpc_block_timestamp(client, mid) 
        
        if tsm == ts:
            return mid
        if tsm < ts:
            lo = mid + 1
        else:
            hi = mid - 1
        if lo > hi:
            break
    
    # hi가 ts 이하의 최댓값이 됩니다.
    if hi >= 0:
        return hi
    return 0

async def block_by_time(client: httpx.AsyncClient, ts: int) -> int:
    """
    Etherscan API로 블록을 조회하고, 실패 시 Alchemy RPC 폴백을 사용합니다.
    """
    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": ts,
        "closest": "before",
        "apikey": ETHERSCAN_KEY,
        "chainid": "1" # V1/V2 호환성을 위해 유지
    }
    try:
        j = await get_json(client, ETHERSCAN_API, params=params)
        if j.get("status") == "1" and str(j.get("result","")).isdigit():
            return int(j["result"])
        
        # V1 Deprecated 또는 NOTOK 에러 발생 시
        raise RuntimeError(f"Etherscan NOTOK or V1 Deprecated: {j}")
    
    except Exception as e:
        print(f"[etherscan] getblocknobytime failed: {e} → fallback to RPC search")
        # 실패 시 Alchemy RPC 폴백
        return await block_by_time_rpc(client, ts)

# --- 기타 API 로직 ---

async def is_contract(client: httpx.AsyncClient, address: str, block_hex: str) -> bool:
    if not ALCHEMY_RPC:
        return False
    data = {"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":[address, block_hex]}
    # Alchemy RPC 호출
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
    CoinGecko 24h 변동률: Pro 도메인 사용, 429/403 백오프/재시도.
    """
    out: Dict[str,float] = {}
    if not contracts: return out
    
    # 환경 변수에서 청크 크기를 가져옵니다.
    chunk = int(os.getenv("CG_CHUNK","60"))
    
    for i in range(0, len(contracts), chunk):
        chunk_data = contracts[i:i+chunk]
        url = f"{COINGECKO_BASE}/simple/token_price/ethereum"
        params = {"contract_addresses":",".join(chunk_data),"vs_currencies":"usd","include_24hr_change":"true"}
        
        # common/utils.py의 HDR_CG 대신 여기에서 직접 헤더를 생성하여 의존성 제거 및 명시적 제어
        headers = {'accept': 'application/json'}
        if os.getenv("COINGECKO_API_KEY"):
            headers["x-cg-pro-api-key"] = os.getenv("COINGECKO_API_KEY")

        # 429/403 백오프 + 5회 재시도
        for attempt in range(5):
            try:
                r = await client.get(url, params=params, headers=headers, timeout=60)
                
                if r.status_code in (429,403):
                    ra = r.headers.get("retry-after")
                    delay = float(ra) if ra and ra.isdigit() else 1.5*(attempt+1)
                    await asyncio.sleep(delay); continue
                    
                r.raise_for_status()
                j = r.json()
                for k,v in j.items():
                    out[k.lower()] = float(v.get("usd_24h_change",0.0))
                break # 성공 시 루프 종료
            
            except Exception as e:
                if attempt == 4:
                    print(f"[coingecko] chunk {i//chunk} failed after 5 attempts: {e}")
                else:
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
    
    if not addresses:
        print("[Analysis] Address list is empty. Exiting analysis.")
        return { "blocks": {}, "averages_T": {}, "sample_top10": pd.DataFrame(), "W_T": pd.DataFrame(), "D_1m": pd.DataFrame(), "D_1w": pd.DataFrame(), "D_24h": pd.DataFrame(), "token_24h_change": {} }

    async with httpx.AsyncClient(http2=True) as client:
        # Etherscan/Alchemy 블록 조회
        b_now = await block_by_time(client, T)
        b_1m  = await block_by_time(client, T_1M)
        b_1w  = await block_by_time(client, T_1W)
        b_24h = await block_by_time(client, T_24H)
        hex_now = hex(b_now)
        print(f"[Etherscan/Alchemy] Blocks found: Now={b_now}, 1M={b_1m}, 1W={b_1w}, 24H={b_24h}")

        if exclude_contracts:
            kept = []
            # ... (contract filter logic) ...
            for i in range(0, len(addresses), 50):
                batch = addresses[i:i+50]
                res = await asyncio.gather(*[is_contract(client, a, hex_now) for a in batch], return_exceptions=True)
                for a, isctr in zip(batch, res):
                    if isinstance(isctr, Exception): 
                        print(f"[Alchemy] Error checking contract for {a}: {isctr}")
                        kept.append(a)
                    else:
                        if not isctr: kept.append(a)
            addresses = kept
            print(f"[Analysis] Filtered down to {len(addresses)} non-contract addresses.")

        async def snapshot(block: Optional[int]):
            out = {}
            # ... (Moralis snapshot logic) ...
            for i in range(0, len(addresses), 25):
                batch = addresses[i:i+25]
                res = await asyncio.gather(*[moralis_wallet_tokens(client, a, block) for a in batch], return_exceptions=True)
                for a, r in zip(batch, res):
                    if isinstance(r, Exception):
                        print(f"[Moralis] Failed to fetch tokens for {a} at block {block}: {r}") 
                        continue
                    out[a] = r
            return out

        S_T   = await snapshot(b_now)
        S_1m  = await snapshot(b_1m)
        S_1w  = await snapshot(b_1w)
        S_24h = await snapshot(b_24h)
        print(f"[Analysis] Moralis snapshots completed. ({len(S_T)} addresses analyzed at T)")

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
        print("[Analysis] Weights computed.")

        addrs = set(W_T["address"].tolist())
        if not addrs:
            print("[Analysis] No valid addresses found after Moralis snapshot. Exiting.")
            return { "blocks": {}, "averages_T": {}, "sample_top10": pd.DataFrame(), "W_T": pd.DataFrame(), "D_1m": pd.DataFrame(), "D_1w": pd.DataFrame(), "D_24h": pd.DataFrame(), "token_24h_change": {} }

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

        # 24h 변동률 조회를 위한 계약 주소 수집 및 제한 (CoinGecko 안정화 로직)
        freq = Counter()
        for toks in S_T.values():
            for r in toks:
                c = (r.get("token_address") or "").lower()
                if c: freq[c] += 1
        
        top_n = int(os.getenv("CG_MAX_CONTRACTS_PER_RUN", "200"))
        contracts = sorted(set([c for c,_ in freq.most_common(top_n)]) | set(STABLES) | set(BTC_PEGS))

        try:
            cg_24h = await coingecko_token_24h_change_by_contracts(client, contracts)
        except Exception as e:
            print(f"[coingecko] 24h change fetch failed (non-fatal): {e}")
            cg_24h = {}
        print("[Analysis] Token 24h change data fetched.")

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