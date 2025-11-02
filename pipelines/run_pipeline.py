# -*- coding: utf-8 -*-
import asyncio, json
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
    out = Path(out_dir); (out/"data").mkdir(parents=True, exist_ok=True); (out/"images").mkdir(parents=True, exist_ok=True)
    addrs = pd.read_csv(address_csv)["address"].dropna().astype(str).tolist()
    if not addrs:
        raise SystemExit("addresses.csv is empty — scraper failed (bot protection or DOM change). Check scraper logs.")
    res = asyncio.run(analyze_addresses(addrs, exclude_contracts=True))

    avg = {"timestamp_utc": int(pd.Timestamp.utcnow().timestamp()), "blocks": res["blocks"], "averages_T": res["averages_T"], "wallet_count": int(len(res["W_T"]))}
    (out/"data"/"averages.json").write_text(json.dumps(avg, indent=2), encoding="utf-8")
    res["sample_top10"].to_csv(out/"data"/"samples_top10.csv", index=False)
    pd.DataFrame([{"contract":k,"usd_24h_change":v} for k,v in res["token_24h_change"].items()]).to_csv(out/"data"/"token_24h_change.csv", index=False)

    for key, ttl, fn in [("D_1m","Δ Weights: T - 1M","heatmap_1m.png"),("D_1w","Δ Weights: T - 1W","heatmap_1w.png"),("D_24h","Δ Weights: T - 24H","heatmap_24h.png")]:
        df = res[key]
        H = df.loc[res["W_T"].set_index("address").index][["BTC","STABLE","ALT"]].to_numpy()
        save_heatmap(H, ttl, out/"images"/fn)

    print("[pipeline] done →", out.resolve())

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--addresses", required=True)
    ap.add_argument("--out", default="site")
    args = ap.parse_args()
    main(args.addresses, args.out)
