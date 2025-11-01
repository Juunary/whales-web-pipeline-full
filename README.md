# Ethereum Whale Portfolio — Dynamic Web Pipeline (Etherscan + Moralis + CoinGecko)

**핵심 목표 (동적/웹 기반)**  
- Etherscan **Top Accounts** 페이지를 **Playwright**로 동적으로 스크래핑해 상위 주소(최대 10,000 계정)를 확보합니다.  
- 확보한 주소에 대해 **오늘(now)** 시점과 **T-30일 / T-7일 / T-1일** 시점의 포트폴리오 비중을 계산합니다.  
- 결과(JSON/PNG/CSV)를 `site/`에 생성하여 **GitHub Pages**로 정적 대시보드를 배포합니다.  
- **GitHub Actions**로 일정 주기(기본 6시간) 또는 수동으로 파이프라인을 실행합니다.

## 필요 시크릿
- `ETHERSCAN_API_KEY` — getblocknobytime(시점→블록)  
- `MORALIS_API_KEY` — `/wallets/:address/tokens?to_block=` (네이티브+ERC-20+USD)  
- `COINGECKO_API_KEY` — `simple/token_price`(24h), `market_chart/range`(히스토리)  
- `ALCHEMY_API_KEY` — `eth_getCode`(EOA/컨트랙트 판별), `eth_getBalance` 등 (백업)

## 로컬 실행
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install --with-deps

# 1) 스크레이프
python scraper/scrape_etherscan.py --pages 400 --ps 100 --out data/addresses.csv

# 2) 파이프라인
export ETHERSCAN_API_KEY=...
export MORALIS_API_KEY=...
export COINGECKO_API_KEY=...
export ALCHEMY_API_KEY=...
python pipelines/run_pipeline.py --addresses data/addresses.csv --out site

# 3) 정적 서버
python -m http.server 8000 --directory site
```

## 산출물
- `site/data/averages.json` — 평균 비중(EW/VW), 블록넘버, 시각, 집계 지갑수  
- `site/data/samples_top10.csv` — 상위 10개 지갑 샘플(총액/비중)  
- `site/data/token_24h_change.csv` — 관측 토큰(계약)들의 24h 변화율  
- `site/images/heatmap_*.png` — Δ비중 히트맵 (T - 1M/1W/24H)  
- `site/index.html` — 대시보드(도넛/테이블/히트맵)

## 분류 규칙
- **BTC**: wBTC/renBTC/HBTC/tBTC/imBTC/cbBTC 등 (ETH 메인넷)  
- **Stable**: USDT/USDC/DAI/BUSD/FRAX/TUSD/USDP/GUSD/LUSD/sUSD/crvUSD/FDUSD/PYUSD/USDe  
- **Alt**: 위 둘을 제외한 모든 ERC-20 + **네이티브 ETH**

## 참고 문서
- Etherscan Top Accounts(10,000 한도): https://etherscan.io/accounts  
- Etherscan getblocknobytime: https://docs.etherscan.io/api-reference/endpoint/getblocknobytime  
- Moralis wallets/:address/tokens (to_block): https://docs.moralis.com/web3-data-api/evm/reference/get-wallet-token-balances-price  
- CoinGecko simple/token_price, market_chart/range: https://docs.coingecko.com/reference/simple-token-price & https://docs.coingecko.com/reference/contract-address-market-chart-range  
- EOA 판별: `eth_getCode` 결과가 `0x`이면 EOA
