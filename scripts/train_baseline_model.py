import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score

df = pd.read_parquet("data/dataset_with_sentiment.parquet")
returns = pd.read_parquet("data/dataset.parquet")
data = returns.merge(df[["article_id", "sentiment"]], on="article_id", how="inner")

data = data.dropna(subset=["ret_1d", "sentiment"])
data["target"] = (data["ret_1d"] > 0).astype(int)  # 1 = positive return

X = data[["sentiment"]]
y = data["target"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

model = LogisticRegression()
model.fit(X_train, y_train)

preds = model.predict(X_test)
probs = model.predict_proba(X_test)[:, 1]

acc = accuracy_score(y_test, preds)
auc = roc_auc_score(y_test, probs)

print(f" Accuracy: {acc:.3f} | AUC: {auc:.3f}")
