import sqlite3
import pandas as pd

conn = sqlite3.connect("data/finnews.db")
df = pd.read_sql(
    "SELECT * FROM prices WHERE ticker='TSLA' ORDER BY date DESC LIMIT 5",
    conn,
    parse_dates=["date"]
)
print(df)
conn.close()
