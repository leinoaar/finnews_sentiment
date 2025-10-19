# finnews_sentiment/features/build_dataset.py
import sqlite3
import pandas as pd

DB_PATH = "data/finnews.db"

def _ret_forward(df_t, pub_date, days_ahead):
    """
    Compute forward return N trading days after pub_date for one ticker dataframe.
    Assumes df_t has index = DatetimeIndex of trading days and column 'close'.
    p0 = last close on or before pub_date
    pN = first close on or after (pub_date + N days)
    """
    # last close on/before pub_date
    df_on_or_before = df_t.loc[:pub_date]
    if df_on_or_before.empty:
        return None, None, None  # (ret, p0_date, pN_date)

    p0_row = df_on_or_before.iloc[-1]
    p0 = float(p0_row["close"])
    p0_date = pd.to_datetime(p0_row.name)

    if p0 == 0:
        return None, p0_date, None

    target_date = pub_date + pd.Timedelta(days=days_ahead)
    df_after = df_t.loc[target_date:]
    if df_after.empty:
        return None, p0_date, None

    pN_row = df_after.iloc[0]
    pN = float(pN_row["close"])
    pN_date = pd.to_datetime(pN_row.name)

    return (pN - p0) / p0, p0_date, pN_date


def build_dataset():
    conn = sqlite3.connect(DB_PATH)

    articles = pd.read_sql(
        "SELECT id, title, summary, tickers, published_at FROM articles",
        conn,
        parse_dates=["published_at"],
    )
    prices = pd.read_sql(
        "SELECT ticker, date, close FROM prices",
        conn,
        parse_dates=["date"],
    )
    conn.close()

    if articles.empty or prices.empty:
        print("No data: 'articles' or 'prices' table is empty")
        return pd.DataFrame()

    # Drop articles with missing timestamp
    missing_ts = articles["published_at"].isna().sum()
    if missing_ts:
        print(f"Dropping {missing_ts} articles with missing published_at")
        articles = articles.dropna(subset=["published_at"])

    # Sort, then we'll make per-ticker views with DatetimeIndex
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)

    rows = []

    for _, art in articles.iterrows():
        tickers_str = (art.get("tickers") or "").strip()
        if not tickers_str:
            continue

        pub_dt = pd.to_datetime(art["published_at"], errors="coerce")
        if pd.isna(pub_dt):
            continue
        pub_date = pub_dt.normalize()

        for t in [x.strip() for x in tickers_str.split(",") if x.strip()]:
            df_t = prices.loc[prices["ticker"] == t].copy()
            if df_t.empty:
                continue

            # IMPORTANT: set index to date for label-based slicing
            df_t = df_t.set_index("date").sort_index()

            ret_1d, p0d, p1d = _ret_forward(df_t, pub_date, 1)
            ret_2d, _p0d2, p2d = _ret_forward(df_t, pub_date, 2)
            ret_5d, _p0d5, p5d = _ret_forward(df_t, pub_date, 5)

            # If nothing could be computed, skip row (e.g., too fresh article)
            if ret_1d is None and ret_2d is None and ret_5d is None:
                continue

            rows.append({
                "article_id": int(art["id"]),
                "ticker": t,
                "title": art["title"],
                "summary": art["summary"],
                "published_at": pub_dt,
                "p0_date": p0d,
                "p1_date": p1d,
                "p2_date": p2d,
                "p5_date": p5d,
                "ret_1d": ret_1d,
                "ret_2d": ret_2d,
                "ret_5d": ret_5d,
            })

    if not rows:
        print("No matches produced. Quick diagnostics:")
        has_tickers = (articles["tickers"].fillna("") != "").sum()
        print(f" - Articles with tickers: {has_tickers} / {len(articles)}")
        print(f" - Prices date range: {prices['date'].min().date()} → {prices['date'].max().date()}")
        print(f" - Articles published range: {articles['published_at'].min().date()} → {articles['published_at'].max().date()}")
        ex = (articles.loc[articles["tickers"].fillna("") != ""]
                    .sort_values("published_at", ascending=False).head(3))
        if not ex.empty:
            print(" - Example articles with tickers (latest 3):")
            for _, r in ex.iterrows():
                print(f"   • id={r['id']}  date={r['published_at'].date()}  tickers={r['tickers']}  title={(r['title'] or '')[:60]}")
        return pd.DataFrame()

    dataset = (pd.DataFrame(rows)
                 .sort_values(["ticker", "published_at"])
                 .reset_index(drop=True))
    print(f"Built dataset with {len(dataset)} rows")
    return dataset


if __name__ == "__main__":
    df = build_dataset()
    print(df.head(15))

    if not df.empty:
        df.to_parquet("data/dataset.parquet", index=False)
        
