import sqlite3
from datetime import datetime, timedelta

DB = "data/finnews.db"

def main(days_offset=1, limit=5):
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT id, published_at, tickers, title
        FROM articles
        WHERE tickers != '' AND published_at IS NOT NULL
        ORDER BY published_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    if not rows:
        print("No eligible articles (need non-empty tickers and non-null published_at).")
        conn.close()
        return

    print("Before:")
    for r in rows:
        print(f"  id={r['id']}  published_at={r['published_at']}  tickers={r['tickers']}  title={(r['title'] or '')[:50]}")

    updated = 0
    for r in rows:
        old_dt = datetime.fromisoformat(str(r["published_at"]))
        new_dt = old_dt - timedelta(days=days_offset)
        conn.execute(
            "UPDATE articles SET published_at=? WHERE id=?",
            (new_dt.strftime("%Y-%m-%d %H:%M:%S"), r["id"]),
        )
        updated += 1

    conn.commit()

    print(f"\n✅ Backdated {updated} article(s) by {days_offset} day(s). After:")
    rows2 = conn.execute(
        f"SELECT id, published_at, tickers, title FROM articles WHERE id IN ({','.join(str(r['id']) for r in rows)})"
    ).fetchall()

    for r in rows2:
        print(f"  id={r['id']}  published_at={r['published_at']}  tickers={r['tickers']}  title={(r['title'] or '')[:50]}")

    conn.close()

if __name__ == "__main__":
    main(days_offset=1, limit=5)
