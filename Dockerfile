FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && python -m playwright install --with-deps
COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["bash","-lc","python scraper/scrape_etherscan.py --pages 400 --ps 100 --out data/addresses.csv && python pipelines/run_pipeline.py --addresses data/addresses.csv --out site && python -m http.server 8000 --directory site"]
