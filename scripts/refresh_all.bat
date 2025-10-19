@echo off
REM ==== Go to project root ====
cd /d C:\Users\leino\finnews_sentiment

REM ==== Activate venv ====
call .venv\Scripts\activate.bat

REM ==== Step 1: Ingest new RSS articles ====
echo Ingesting new RSS articles...
python -m finnews_sentiment.etl.ingest_rss

REM ==== Step 2: Enrich articles with tickers ====
echo Enriching articles...
python -m finnews_sentiment.etl.enrich_articles

REM ==== Step 3: Fetch latest prices ====
echo Fetching latest prices...
python -m finnews_sentiment.etl.fetch_prices

REM ==== Step 4: Build dataset ====
echo Building dataset...
python -m finnews_sentiment.features.build_dataset

echo  Compute sentiment...
python -m finnews_sentiment.features.compute_sentiment

echo  Join sentiment + returns...
python scripts\join_sentiment_returns.py

echo Refresh completed!
pause
