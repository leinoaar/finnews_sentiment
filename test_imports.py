import pandas as pd
import numpy as np
import requests
import feedparser

print("pandas version:", pd.__version__)
print("numpy version:", np.__version__)

df = pd.DataFrame({"x": np.arange(6), "y":np.arange(6)**2})
print(df)

# test also RSS

url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=AAPL,MSFT"
feed = feedparser.parse(url)
print("titles:", len(feed.entries))
print("first title:", feed.entries[0].title)

