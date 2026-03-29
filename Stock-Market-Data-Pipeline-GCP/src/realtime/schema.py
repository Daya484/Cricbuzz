"""
BigQuery Schema Definitions for Stock Market Data Pipeline.

Defines table schemas for:
  - Raw stock ticks (real-time from Pub/Sub)
  - Aggregated 1-minute OHLCV data
  - Historical daily stock data (batch)
  - Technical indicators (batch-computed)
"""

from google.cloud import bigquery


# ============================================================
# Raw Stock Ticks — streamed via Dataflow from Pub/Sub
# ============================================================
RAW_TICKS_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED",
                         description="Stock ticker symbol (e.g. AAPL)"),
    bigquery.SchemaField("price", "FLOAT64", mode="REQUIRED",
                         description="Last trade price"),
    bigquery.SchemaField("volume", "INTEGER", mode="NULLABLE",
                         description="Trade volume"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED",
                         description="Tick timestamp from the API"),
    bigquery.SchemaField("open", "FLOAT64", mode="NULLABLE",
                         description="Opening price for the interval"),
    bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE",
                         description="Highest price in the interval"),
    bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE",
                         description="Lowest price in the interval"),
    bigquery.SchemaField("close", "FLOAT64", mode="NULLABLE",
                         description="Closing price for the interval"),
    bigquery.SchemaField("ingestion_time", "TIMESTAMP", mode="REQUIRED",
                         description="Pipeline ingestion timestamp"),
]

# Table spec string for Beam pipeline
RAW_TICKS_TABLE_SPEC = {
    "fields": [
        {"name": "symbol",         "type": "STRING",    "mode": "REQUIRED"},
        {"name": "price",          "type": "FLOAT64",   "mode": "REQUIRED"},
        {"name": "volume",         "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "timestamp",      "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "open",           "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "high",           "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "low",            "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "close",          "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


# ============================================================
# Aggregated 1-Minute OHLCV — computed by Dataflow windowing
# ============================================================
AGGREGATED_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("window_start", "TIMESTAMP", mode="REQUIRED",
                         description="Start of the 1-minute window"),
    bigquery.SchemaField("window_end", "TIMESTAMP", mode="REQUIRED",
                         description="End of the 1-minute window"),
    bigquery.SchemaField("open", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("close", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("vwap", "FLOAT64", mode="NULLABLE",
                         description="Volume-weighted average price"),
    bigquery.SchemaField("total_volume", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("tick_count", "INTEGER", mode="NULLABLE"),
]

AGGREGATED_TABLE_SPEC = {
    "fields": [
        {"name": "symbol",       "type": "STRING",    "mode": "REQUIRED"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end",   "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "open",         "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "high",         "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "low",          "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "close",        "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "vwap",         "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "total_volume", "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "tick_count",   "type": "INTEGER",   "mode": "NULLABLE"},
    ]
}


# ============================================================
# Historical Daily Stock Data — loaded via batch (Dataproc)
# ============================================================
HISTORICAL_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("open", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("close", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("adjusted_close", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("volume", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("dividend_amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("split_coefficient", "FLOAT64", mode="NULLABLE"),
]


# ============================================================
# Technical Indicators — computed by Spark batch job
# ============================================================
TECHNICAL_INDICATORS_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("close", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("sma_20", "FLOAT64", mode="NULLABLE",
                         description="20-day Simple Moving Average"),
    bigquery.SchemaField("sma_50", "FLOAT64", mode="NULLABLE",
                         description="50-day Simple Moving Average"),
    bigquery.SchemaField("ema_12", "FLOAT64", mode="NULLABLE",
                         description="12-day Exponential Moving Average"),
    bigquery.SchemaField("ema_26", "FLOAT64", mode="NULLABLE",
                         description="26-day Exponential Moving Average"),
    bigquery.SchemaField("rsi_14", "FLOAT64", mode="NULLABLE",
                         description="14-day Relative Strength Index"),
    bigquery.SchemaField("macd", "FLOAT64", mode="NULLABLE",
                         description="MACD (EMA12 - EMA26)"),
    bigquery.SchemaField("macd_signal", "FLOAT64", mode="NULLABLE",
                         description="9-day EMA of MACD"),
    bigquery.SchemaField("bollinger_upper", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("bollinger_lower", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("daily_return_pct", "FLOAT64", mode="NULLABLE",
                         description="Daily return percentage"),
]
