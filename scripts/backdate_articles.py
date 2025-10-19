# scripts/backdate_articles.py
import sqlite3
from datetime import datetime, timedelta

DB = "data/finnews.db"

def backdate_latest_with_tickers(days_offset=1, limit=5):
    """
    Move the latest N (limit) articles that HAVE tickers backward by 'days_offset' days.
    This is only for testing to make next-day returns (ret_1d) available.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    
    rows = conn.execute("""
        SELECT id, published_at
        FROM articles
        WHERE tickers != '' AND published_at IS NOT NULL
        ORDER BY published_at DESC
        LIMIT ?
    """, (limit,)).fetchall()

    if not rows:
        print("No articles with tickers found.")
        conn.close()
        return

   
    updated = 0
    for r in rows:
        old_dt = datetime.fromisoformat(r["published_at"].replace("Z","")) if isinstance(r["published_at"], str) else r["published_at"]
        new_dt = old_dt - timedelta(days=days_offset)
        conn.execute("UPDATE articles SET published_at=? WHERE id=?", (new_dt.isoformat(sep=" "), r["id"]))
        updated += 1

    conn.commit()
    conn.close()
    print(f"✅ Backdated {updated} article(s) by {days_offset} day(s).")

if __name__ == "__main__":
    # shift the latest 5 by 1 day
    backdate_latest_with_tickers(days_offset=1, limit=5)
