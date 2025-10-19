import sqlite3, pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer    

DB = "data/finnews.db"
OUT = "data/dataset_with_sentiment.parquet"

def run():
    conn = sqlite3.connect(DB)
    df = pd.read_sql(
        """SELECT id as article_id, title, summary, tickers, published_at
        FROM articles
        WHERE tickers != '' AND published_at IS NOT NULL
        """, conn, parse_dates=["published_at"])
    conn.close()
    
    if df.empty:
        print("No articles to score.")

    analyzer = SentimentIntensityAnalyzer()
    df["text"] = (df["title"].fillna("") + " " + df["summary"].fillna("")).str.strip()
    df["sentiment"] = df["text"].apply(lambda t: analyzer.polarity_scores(t)["compound"])

    df.to_parquet(OUT, index=False)
    print(f"Saved {len(df)} articles with sentiment to {OUT}")

if __name__ == "__main__":
    run()