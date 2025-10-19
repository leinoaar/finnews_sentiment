# update_date.py
import sqlite3

conn = sqlite3.connect("data/finnews.db")
conn.execute(
    "UPDATE articles SET published_at='2025-10-02 12:00:00' WHERE id=10;"
)
conn.commit()
conn.close()
print("Updated article id=10 to 2025-10-02 12:00:00")
