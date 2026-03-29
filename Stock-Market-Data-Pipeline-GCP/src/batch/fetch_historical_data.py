"""
Fetch Historical Stock Data from Alpha Vantage.

Downloads daily adjusted stock data for configured symbols
and saves as CSV files for batch processing.

Usage:
    python -m src.batch.fetch_historical_data
    python -m src.batch.fetch_historical_data --env qa
    python -m src.batch.fetch_historical_data --env pd --symbols AAPL GOOGL
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from src.utils.config_loader import load_config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s — %(message)s",
)
logger = logging.getLogger("fetch_historical")



def fetch_daily_adjusted(api_key: str, symbol: str) -> pd.DataFrame | None:
    """
    Fetch full-length daily adjusted time series from Alpha Vantage.

    Returns a DataFrame with columns:
        date, open, high, low, close, adjusted_close, volume,
        dividend_amount, split_coefficient, symbol
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "outputsize": "full",         # full history (20+ years)
        "apikey": api_key,
        "datatype": "json",
    }

    try:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if "Note" in data:
            logger.warning("Rate limit hit: %s", data["Note"])
            return None
        if "Error Message" in data:
            logger.error("API error for %s: %s", symbol, data["Error Message"])
            return None

        ts_key = "Time Series (Daily)"
        if ts_key not in data:
            logger.warning("No daily data returned for %s", symbol)
            return None

        rows = []
        for date_str, values in data[ts_key].items():
            rows.append({
                "date": date_str,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "adjusted_close": float(values["5. adjusted close"]),
                "volume": int(values["6. volume"]),
                "dividend_amount": float(values["7. dividend amount"]),
                "split_coefficient": float(values["8. split coefficient"]),
                "symbol": symbol,
            })

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

        logger.info("Fetched %d daily rows for %s (from %s to %s)",
                     len(df), symbol, df["date"].min().date(), df["date"].max().date())
        return df

    except requests.RequestException as e:
        logger.error("Request failed for %s: %s", symbol, e)
        return None


def main():
    parser = argparse.ArgumentParser(description="Fetch Historical Stock Data")
    parser.add_argument("--env", default=None, choices=["dv", "qa", "pd"],
                        help="Environment (dv/qa/pd)")
    parser.add_argument("--symbols", nargs="+", default=None,
                        help="Override symbols from config")
    parser.add_argument("--output", default="data/historical",
                        help="Output directory for CSV files")
    args = parser.parse_args()

    cfg = load_config(env=args.env)
    env_name = cfg.get("environment", "unknown")
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        logger.error("Set ALPHA_VANTAGE_API_KEY environment variable.")
        sys.exit(1)

    symbols = args.symbols or cfg["api"]["symbols"]
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_dfs = []
    for i, symbol in enumerate(symbols):
        logger.info("Fetching %s (%d/%d)…", symbol, i + 1, len(symbols))
        df = fetch_daily_adjusted(api_key, symbol)

        if df is not None:
            csv_path = output_dir / f"{symbol}_daily.csv"
            df.to_csv(csv_path, index=False)
            logger.info("Saved → %s (%d rows)", csv_path, len(df))
            all_dfs.append(df)

        # Respect rate limits (5 calls/min on free tier)
        if i < len(symbols) - 1:
            logger.info("Waiting 12s for rate limit…")
            time.sleep(12)

    # Combined CSV for batch processing
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        combined_path = output_dir / "all_stocks_daily.csv"
        combined.to_csv(combined_path, index=False)
        logger.info("Combined CSV → %s (%d total rows)", combined_path, len(combined))

    logger.info("Done. Fetched data for %d/%d symbols.", len(all_dfs), len(symbols))


if __name__ == "__main__":
    main()
