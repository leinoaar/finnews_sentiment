import os
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

DATA_DIR = Path("data")
FIG_DIR = Path("figures")
RET_PATH = DATA_DIR / "dataset.parquet"
SENT_PATH = DATA_DIR / "dataset_with_sentiment.parquet"
MODEL_PATH = DATA_DIR / "model_dataset.parquet"

FIG_DIR.mkdir(parents=True, exist_ok=True)


def load_or_join() -> pd.DataFrame:
    """Load model_dataset if available, otherwise join returns + sentiment by article_id."""
    if MODEL_PATH.exists():
        print(f"Loading {MODEL_PATH}")
        return pd.read_parquet(MODEL_PATH)

    if not RET_PATH.exists():
        raise FileNotFoundError(f"Missing returns file: {RET_PATH}")
    if not SENT_PATH.exists():
        raise FileNotFoundError(f"Missing sentiment file: {SENT_PATH}")

    print(f"Joining {RET_PATH.name} + {SENT_PATH.name}")
    returns = pd.read_parquet(RET_PATH)
    sent = pd.read_parquet(SENT_PATH)

    if "article_id" not in returns.columns or "article_id" not in sent.columns:
        raise ValueError("Both datasets must contain 'article_id' column.")

    df = returns.merge(sent[["article_id", "sentiment"]], on="article_id", how="inner")

    df = df.dropna(subset=["sentiment"]).copy()

    
    df.to_parquet(MODEL_PATH, index=False)
    print(f"Saved merged dataset -> {MODEL_PATH} ({len(df)} rows)")
    return df


def basic_report(df: pd.DataFrame):
    print("\n===== BASIC REPORT =====")
    cols_ret = [c for c in ["ret_1d", "ret_2d", "ret_5d"] if c in df.columns]
    print(f"Rows: {len(df)} | Tickers: {df['ticker'].nunique() if 'ticker' in df else 'n/a'}")
    print(f"Date range: {df['published_at'].min()} → {df['published_at'].max()}" if "published_at" in df else "Date col missing")


    for c in cols_ret:
        sub = df[["sentiment", c]].dropna()
        if len(sub) > 2:
            corr = sub.corr().iloc[0, 1]
            print(f"Correlation(sentiment, {c}): {corr:.3f}  (n={len(sub)})")
        else:
            print(f"Correlation(sentiment, {c}): n<3")

    df["sentiment_label"] = np.where(df["sentiment"] > 0, "Positive",
                              np.where(df["sentiment"] < 0, "Negative", "Neutral"))
    counts = df["sentiment_label"].value_counts()
    print("\nCounts by sentiment label:")
    print(counts.to_string())


def scatter_sent_vs_return(df: pd.DataFrame, ret_col: str):
    sub = df[["sentiment", ret_col]].dropna()
    if sub.empty:
        print(f"No data for scatter {ret_col}")
        return
    plt.figure(figsize=(7, 5))
    plt.scatter(sub["sentiment"], sub[ret_col], alpha=0.6)
  
    try:
        m, b = np.polyfit(sub["sentiment"], sub[ret_col], 1)
        xs = np.linspace(sub["sentiment"].min(), sub["sentiment"].max(), 100)
        plt.plot(xs, m * xs + b)
    except Exception:
        pass
    plt.axhline(0, linewidth=0.8)
    plt.axvline(0, linewidth=0.8)
    plt.xlabel("Sentiment (compound)")
    plt.ylabel(f"{ret_col}")
    plt.title(f"Sentiment vs. {ret_col}")
    out = FIG_DIR / f"scatter_sent_{ret_col}.png"
    plt.tight_layout()
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out}")


def boxplot_by_label(df: pd.DataFrame, ret_col: str):
    if ret_col not in df.columns:
        return
    sub = df[[ret_col, "sentiment"]].copy()
    sub["label"] = np.where(sub["sentiment"] > 0, "Positive",
                     np.where(sub["sentiment"] < 0, "Negative", "Neutral"))
    sub = sub.dropna(subset=[ret_col])
    if sub.empty:
        print(f"No data for boxplot {ret_col}")
        return

    data = [sub.loc[sub["label"] == g, ret_col].values for g in ["Negative", "Neutral", "Positive"]]
    plt.figure(figsize=(7, 5))
    plt.boxplot(data, labels=["Negative", "Neutral", "Positive"], showfliers=False)
    plt.axhline(0, linewidth=0.8)
    plt.ylabel(ret_col)
    plt.title(f"{ret_col} distribution by sentiment label")
    out = FIG_DIR / f"box_{ret_col}_by_label.png"
    plt.tight_layout()
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out}")


def bar_means_with_ci(df: pd.DataFrame, ret_col: str):
    if ret_col not in df.columns:
        return
    sub = df[[ret_col, "sentiment"]].dropna()
    if sub.empty:
        print(f"No data for bar means {ret_col}")
        return
    sub["label"] = np.where(sub["sentiment"] > 0, "Positive",
                     np.where(sub["sentiment"] < 0, "Negative", "Neutral"))

    groups = sub.groupby("label")[ret_col]
    order = ["Negative", "Neutral", "Positive"]
    means = []
    errs = []
    ns = []
    for g in order:
        vals = groups.get_group(g) if g in groups.groups else pd.Series(dtype=float)
        means.append(vals.mean() if len(vals) else np.nan)
        ns.append(len(vals))
        
        if len(vals) > 1:
            errs.append(1.96 * (vals.std(ddof=1) / np.sqrt(len(vals))))
        else:
            errs.append(np.nan)

    plt.figure(figsize=(7, 5))
    x = np.arange(len(order))
    plt.bar(x, means, yerr=errs, capsize=4)
    plt.axhline(0, linewidth=0.8)
    plt.xticks(x, order)
    plt.ylabel(f"Mean {ret_col}")
    plt.title(f"Mean {ret_col} by sentiment label (error bars ~95% CI)\nN={dict(zip(order, ns))}")
    out = FIG_DIR / f"means_{ret_col}_by_label.png"
    plt.tight_layout()
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out}")


def main():
    
    df = load_or_join()
   
    basic_report(df)

   
    for ret_col in [c for c in ["ret_1d", "ret_2d", "ret_5d"] if c in df.columns]:
        scatter_sent_vs_return(df, ret_col)
        boxplot_by_label(df, ret_col)
        bar_means_with_ci(df, ret_col)

    print("\nAnalysis done. See figures/ for PNGs.")


if __name__ == "__main__":
    main()