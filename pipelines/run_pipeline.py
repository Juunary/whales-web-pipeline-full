# -*- coding: utf-8 -*-
import os, asyncio, json
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from backend.compute import analyze_addresses

def save_heatmap(matrix, title, outfile: Path):
    plt.figure(figsize=(9,6))
    plt.imshow(matrix, aspect='auto')
    plt.title(title)
    plt.xlabel("Categories (BTC, STABLE, ALT)")
    plt.ylabel("Wallet rank index")
    plt.colorbar(label="Δ percentage points")
    plt.tight_layout()
    outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outfile, dpi=150); plt.close()

def main(address_csv: str, out_dir: str):
    out = Path(out_dir)
    (out/"data").mkdir(parents=True, exist_ok=True)
    (out/"images").mkdir(parents=True, exist_ok=True)

    addrs = pd.read_csv(address_csv)["address"].dropna().astype(str).tolist()
    if not addrs:
        raise SystemExit("addresses.csv is empty — scraper blocked or DOM changed. Check scraper logs.")

    # EXCLUDE_CONTRACTS: 'auto' (default) | 'true' | 'false'
    excl_env = os.getenv("EXCLUDE_CONTRACTS", "auto").strip().lower()
    if excl_env in ("true","1","yes"):
        excl = True
    elif excl_env in ("false","0","no"):
        excl = False
    else:
        excl = None  # auto by RPC health

    res = asyncio.run(analyze_addresses(addrs, exclude_contracts=excl))

    avg = {
        "timestamp_utc": int(pd.Timestamp.utcnow().timestamp()),
        "blocks": res["blocks"],
        "averages_T": res["averages_T"],
        "wallet_count": int(len(res["W_T"])) if res.get("W_T") is not None else 0,
    }
    (out/"data"/"averages.json").write_text(json.dumps(avg, indent=2), encoding="utf-8")

    # sample top10
    sample = res.get("sample_top10")
    if sample is not None and not sample.empty:
        sample.to_csv(out/"data"/"samples_top10.csv", index=False)
    else:
        pd.DataFrame(columns=["address","total_usd","BTC","STABLE","ALT"]).to_csv(out/"data"/"samples_top10.csv", index=False)

    # token 24h change
    tok = res.get("token_24h_change") or {}
    pd.DataFrame([{"contract":k,"usd_24h_change":v} for k,v in tok.items()]).to_csv(out/"data"/"token_24h_change.csv", index=False)

    # heatmaps (empty-safe)
    def hm(df, w_t):
        if df is None or df.empty or w_t is None or w_t.empty:
            return np.zeros((1,3), dtype=float)
        idx = w_t.set_index("address").index
        M = df.loc[idx][["BTC","STABLE","ALT"]].to_numpy()
        if M.size == 0:
            return np.zeros((1,3), dtype=float)
        return M

    W_T = res.get("W_T")
    for key, ttl, fn in [
        ("D_1m","Δ Weights: T - 1M","heatmap_1m.png"),
        ("D_1w","Δ Weights: T - 1W","heatmap_1w.png"),
        ("D_24h","Δ Weights: T - 24H","heatmap_24h.png"),
    ]:
        H = hm(res.get(key), W_T)
        save_heatmap(H, ttl, out/"images"/fn)

    print("[pipeline] done →", out.resolve())

if __name__ == "__main__":
    import argparse, sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parents[1]))  # import robustness
    ap = argparse.ArgumentParser()
    ap.add_argument("--addresses", required=True)
    ap.add_argument("--out", default="site")
    args = ap.parse_args()
    main(args.addresses, args.out)
