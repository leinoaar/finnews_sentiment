import yaml
import yfinance as yf
import pandas as pd
import math
from datetime import datetime, timedelta
from ..db import SessionLocal, Price


def load_tickers(cfg_path: str = "configs/tickers.yaml"):
    """Load ticker configuration from a YAML file"""
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _to_float(x, default=None):
    """
    Safely convert a value (numpy scalar, pandas element, etc.) to float.
    If the value is NaN or None, return the provided default.
    """
    try:
        val = x.item() if hasattr(x, "item") else x
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return default
        return float(val)
    except Exception:
        return default


def _to_int(x, default=0):
    """
    Safely convert a value to int, with fallback.
    Useful because Pandas may return NaN or numpy.int64 objects.
    """
    f = _to_float(x, default=None)
    if f is None:
        return default
    try:
        return int(f)
    except Exception:
        return default


def _normalize_df(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Normalize Yahoo Finance dataframe structure.

    yfinance sometimes returns MultiIndex columns:
        - (field, ticker) or (ticker, field)
    This function ensures we end up with flat columns:
        ['Open','High','Low','Close','Adj Close','Volume']
    """
    if not isinstance(df.columns, pd.MultiIndex):
        # Already flat → nothing to do
        return df

    lv0 = list(df.columns.get_level_values(0))
    lv1 = list(df.columns.get_level_values(1))
    fields = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}

    has_fields_lv0 = fields.issubset(set(lv0))
    has_fields_lv1 = fields.issubset(set(lv1))

    if has_fields_lv0 and ticker in set(lv1):
        # Case: (field, ticker)
        return df.xs(ticker, axis=1, level=1)
    if has_fields_lv1 and ticker in set(lv0):
        # Case: (ticker, field)
        return df.xs(ticker, axis=1, level=0)
    if has_fields_lv0:
        # Drop extra level with ticker names
        return df.droplevel(1, axis=1)
    if has_fields_lv1:
        return df.droplevel(0, axis=1)

    # If structure is unexpected, raise error
    raise ValueError(f"Unexpected columns for {ticker}: {df.columns}")


def run(cfg_path: str = "configs/tickers.yaml", lookback_days: int = 365):
    """
    Fetch historical prices for tickers listed in configs/tickers.yaml
    and insert them into the 'prices' table.
    """
    sess = SessionLocal()

    # Load ticker configuration (universe and mappings)
    tickers_cfg = load_tickers(cfg_path)
    tickers = tickers_cfg.get("universe", [])

    # Define date range: lookback_days into the past up to today
    start = datetime.today() - timedelta(days=lookback_days)
    end = datetime.today()

    total_inserted = 0

    for t in tickers:
        print(f"Fetching {t} from Yahoo Finance...")

        # Download daily OHLCV data from Yahoo
        df = yf.download(
            t,
            start=start,
            end=end,
            auto_adjust=False,   # keep Adj Close column
            actions=False,       # skip dividends/splits
            group_by="column",   # return flat columns if possible
            progress=False,
        )
        if df.empty:
            print(f"No data for {t}, skipping.")
            continue

        # Normalize structure if dataframe has MultiIndex columns
        try:
            df = _normalize_df(df, t)
        except Exception as e:
            print(f"Column shape issue for {t}: {e}")
            continue

        # Ensure required OHLCV columns exist
        needed = {"Open", "High", "Low", "Close", "Volume"}
        if not needed.issubset(set(df.columns)):
            print(f"Missing expected columns for {t}: have {list(df.columns)}")
            continue

        has_adj = "Adj Close" in df.columns
        first_error_printed = False

        # Iterate through each row (daily data)
        for date, row in df.iterrows():
            open_ = _to_float(row.get("Open"), default=None)
            high_ = _to_float(row.get("High"), default=None)
            low_  = _to_float(row.get("Low"), default=None)
            close_ = _to_float(row.get("Close"), default=None)
            adj_close_ = _to_float(row.get("Adj Close"), default=None) if has_adj else None
            volume_ = _to_int(row.get("Volume"), default=0)

            # If critical fields are missing, skip this row
            if open_ is None or high_ is None or low_ is None or close_ is None:
                if not first_error_printed:
                    print(f"Skipping {t} {date.date()} due to missing OHLC values")
                    first_error_printed = True
                continue

            # If Adj Close is missing, use Close as fallback
            if adj_close_ is None:
                adj_close_ = close_

            # insert price record into DB
            price = Price(
                ticker=t,
                date=date.to_pydatetime(),
                open=open_,
                high=high_,
                low=low_,
                close=close_,
                adj_close=adj_close_,
                volume=volume_,
            )
            try:
                sess.add(price)
                sess.commit()
                total_inserted += 1
            except Exception as e:
                # Roll back if unique constraint or other DB error
                sess.rollback()
                if not first_error_printed:
                    print(f"Insert failed for {t} {date.date()}: {e}")
                    first_error_printed = True

    print(f"fetch_prices: inserted {total_inserted} rows into prices")


if __name__ == "__main__":
    run()