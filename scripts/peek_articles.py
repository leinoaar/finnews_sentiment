import sqlite3
import pandas as pd

conn = sqlite3.connect("data/finnews.db")

df = pd.read_sql("SELECT COUNT(*) AS count, source FROM articles GROUP BY source ORDER BY count DESC", conn)

print(df)