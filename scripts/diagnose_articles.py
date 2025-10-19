
import sqlite3

DB = "data/finnews.db"
conn = sqlite3.connect(DB)

with_tickers = conn.execute("SELECT COUNT(*) FROM articles WHERE tickers != ''").fetchone()[0]
print(f"✅ Articles with tickers: {with_tickers}")

print("\n📋 Latest 10 articles:")
rows = conn.execute("""
    SELECT id, published_at, tickers, title
    FROM articles
    ORDER BY id DESC
    LIMIT 10
""").fetchall()

for r in rows:
    print(r)

conn.close()
