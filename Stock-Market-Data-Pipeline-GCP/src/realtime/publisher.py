"""
Stock Market Data Publisher — Real-time Pub/Sub Ingestion.

Fetches intraday stock prices from Alpha Vantage API and publishes
each tick as a JSON message to a Google Cloud Pub/Sub topic.

Usage:
    # Dev environment (default)
    python -m src.realtime.publisher

    # Specific environment
    python -m src.realtime.publisher --env qa
    python -m src.realtime.publisher --env pd
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1

from src.utils.config_loader import load_config

# ── Load environment variables ──────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s — %(message)s",
)
logger = logging.getLogger("stock_publisher")


# ============================================================
# Alpha Vantage API Client
# ============================================================

class StockDataFetcher:
    """Fetches real-time / intraday stock data from Alpha Vantage."""

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()

    def get_intraday(self, symbol: str, interval: str = "1min") -> list[dict]:
        """
        Fetch intraday time-series data for a stock symbol.

        Args:
            symbol:   Ticker symbol (e.g. "AAPL").
            interval: Time interval between data points (1min, 5min, 15min, 30min, 60min).

        Returns:
            List of tick dicts with keys: symbol, timestamp, open, high, low, close, volume, price.
        """
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": interval,
            "apikey": self.api_key,
            "outputsize": "compact",       # last 100 data points
            "datatype": "json",
        }

        try:
            resp = self.session.get(self.BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            # Alpha Vantage rate-limit or error check
            if "Note" in data:
                logger.warning("API rate limit reached: %s", data["Note"])
                return []
            if "Error Message" in data:
                logger.error("API error for %s: %s", symbol, data["Error Message"])
                return []

            time_series_key = f"Time Series ({interval})"
            if time_series_key not in data:
                logger.warning("No time-series data returned for %s", symbol)
                return []

            ticks = []
            for ts_str, values in data[time_series_key].items():
                ticks.append({
                    "symbol": symbol,
                    "timestamp": ts_str,
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"]),
                    "price": float(values["4. close"]),   # primary price = close
                })

            logger.info("Fetched %d ticks for %s", len(ticks), symbol)
            return ticks

        except requests.RequestException as e:
            logger.error("HTTP request failed for %s: %s", symbol, e)
            return []

    def get_quote(self, symbol: str) -> dict | None:
        """
        Fetch the latest global quote for a symbol (single real-time price).
        """
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key,
            "datatype": "json",
        }

        try:
            resp = self.session.get(self.BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if "Global Quote" not in data or not data["Global Quote"]:
                logger.warning("No quote data for %s", symbol)
                return None

            q = data["Global Quote"]
            return {
                "symbol": symbol,
                "timestamp": q.get("07. latest trading day", ""),
                "open": float(q.get("02. open", 0)),
                "high": float(q.get("03. high", 0)),
                "low": float(q.get("04. low", 0)),
                "close": float(q.get("08. previous close", 0)),
                "price": float(q.get("05. price", 0)),
                "volume": int(q.get("06. volume", 0)),
            }

        except requests.RequestException as e:
            logger.error("Quote request failed for %s: %s", symbol, e)
            return None


# ============================================================
# Pub/Sub Publisher
# ============================================================

class StockPublisher:
    """Publishes stock tick messages to a Google Cloud Pub/Sub topic."""

    def __init__(self, project_id: str, topic_name: str):
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        logger.info("Publisher initialized → %s", self.topic_path)

    def publish_tick(self, tick: dict) -> None:
        """
        Publish a single tick message.

        The message payload is a UTF-8 JSON string. Additional metadata
        is attached as Pub/Sub message attributes.
        """
        tick["ingestion_time"] = datetime.now(timezone.utc).isoformat()

        message_data = json.dumps(tick).encode("utf-8")
        future = self.publisher.publish(
            self.topic_path,
            data=message_data,
            symbol=tick["symbol"],          # attribute for filtering
            source="alpha_vantage",
        )
        msg_id = future.result()
        logger.debug("Published %s tick → msg_id=%s", tick["symbol"], msg_id)

    def publish_batch(self, ticks: list[dict]) -> int:
        """Publish a list of ticks. Returns count of successfully published messages."""
        count = 0
        for tick in ticks:
            try:
                self.publish_tick(tick)
                count += 1
            except Exception as e:
                logger.error("Failed to publish tick: %s — %s", tick, e)
        return count


# ============================================================
# Main Loop
# ============================================================

def run_publisher(config: dict) -> None:
    """
    Continuous loop: fetch stock data → publish to Pub/Sub.
    """
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        logger.error("ALPHA_VANTAGE_API_KEY environment variable is not set.")
        sys.exit(1)

    project_id = os.getenv("GCP_PROJECT_ID", config["gcp"]["project_id"])
    topic_name = config["pubsub"]["topic_name"]
    symbols = config["api"]["symbols"]
    poll_interval = config["api"].get("polling_interval_seconds", 60)
    env_name = config.get("environment", "unknown")

    fetcher = StockDataFetcher(api_key)
    publisher = StockPublisher(project_id, topic_name)

    logger.info(
        "Starting publisher [%s] — symbols=%s, interval=%ds",
        env_name, symbols, poll_interval,
    )

    while True:
        for symbol in symbols:
            # Fetch latest intraday ticks
            ticks = fetcher.get_intraday(symbol, interval="1min")

            if ticks:
                published = publisher.publish_batch(ticks)
                logger.info(
                    "Published %d/%d ticks for %s",
                    published, len(ticks), symbol,
                )
            else:
                # Fallback: try single quote
                quote = fetcher.get_quote(symbol)
                if quote:
                    publisher.publish_tick(quote)
                    logger.info("Published single quote for %s", symbol)

            # Alpha Vantage free tier: max 5 calls/min – pace requests
            time.sleep(12)

        logger.info("Sleeping %d seconds before next poll cycle…", poll_interval)
        time.sleep(poll_interval)


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Market Pub/Sub Publisher")
    parser.add_argument(
        "--env", default=None,
        choices=["dv", "qa", "pd"],
        help="Environment (dv/qa/pd). Defaults to ENV variable or 'dv'.",
    )
    args = parser.parse_args()

    cfg = load_config(env=args.env)
    run_publisher(cfg)
