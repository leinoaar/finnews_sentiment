# Finnews_sentiment
A Python-based pipeline for analyzing financial news sentiment and its relationship with stock returns.
The project is structured as an end-to-end ETL workflow from fetching news and market data to enriching, processing, and preparing datasets for sentiment and correlation analysis.

Currently, the project is in its data collection phase. Once sufficient data has been gathered, deeper sentiment–return analysis will follow.

## Overview

| Stage                        | Description                                                                                                |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------- |
| ETL (Extract–Transform–Load) | Fetches RSS news feeds and financial price data, normalizes articles, and links them to tickers.           |
| Feature engineering          | Builds a combined dataset of text sentiment scores and market returns.                                     |
| Analysis (coming soon)       | Will explore correlations and predictive relationships between sentiment and returns using various models. |

## Structure
finnews_sentiment/

│

├── configs/                # YAML configs for tickers and sources
│   ├── tickers.example.yaml
│   └── sources.example.yaml
│
├── finnews_sentiment/
│   ├── etl/                # Data ingestion and enrichment scripts
│   ├── features/           # Feature building and sentiment computation
│   ├── db.py               # SQLAlchemy database connection
│   └── settings.py
│
├── data/                   # (ignored) local data storage
├── figures/                # (ignored) plots and outputs
├── notebooks/              # exploratory notebooks
│
├── requirements.txt
├── pyproject.toml
├── Makefile
└── README.md

## Installation

**Clone repository**
git clone https://github.com/<your-username>/finnews_sentiment.git
cd finnews_sentiment

**Create virtual environment**
python -m venv .venv
.\.venv\Scripts\Activate.ps1  # (Windows PowerShell)

**Install dependencies**
pip install -r requirements.txt

**Usage**
copy configs\tickers.example.yaml configs\tickers.yaml
copy configs\sources.example.yaml configs\sources.yaml

**ETL**
python -m finnews_sentiment.etl.ingest_rss
python -m finnews_sentiment.etl.fetch_prices
python -m finnews_sentiment.features.build_dataset

## Next steps

Collecting more data and performing larger statistical analysis on it.

### Licence
This project is licensed under the MIT License.




