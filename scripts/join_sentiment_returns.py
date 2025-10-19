# scripts/join_sentiment_returns.py
import pandas as pd
from pathlib import Path

RET_PATH = Path("data/dataset.parquet")
SENT_PATH = Path("data/dataset_with_sentiment.parquet")
OUT_PATH = Path("data/model_dataset.parquet")

def run():
    if not RET_PATH.exists():
        print(f"Missing returns file: {RET_PATH}")
        return
    if not SENT_PATH.exists():
        print(f"Missing sentiment file: {SENT_PATH}")
        return

    print("Loading datasets")
    returns = pd.read_parquet(RET_PATH)
    sent = pd.read_parquet(SENT_PATH)

    # Check required columns
    if "article_id" not in returns.columns or "article_id" not in sent.columns:
        print("Both files must contain 'article_id'")
        return

    print("Joining sentiment + returns...")
    df = returns.merge(sent[["article_id", "sentiment"]], on="article_id", how="inner")

 
    before = len(df)
    df = df.dropna(subset=["sentiment", "ret_1d"])
    print(f" Kept {len(df)} rows (dropped {before - len(df)})")

   
    df.to_parquet(OUT_PATH, index=False)
    print(f" Saved merged dataset -> {OUT_PATH}")

    
    print(df.head(10))

if __name__ == "__main__":
    run()
