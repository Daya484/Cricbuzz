"""
Dataflow Streaming Pipeline — Pub/Sub → BigQuery.

Apache Beam pipeline that:
  1. Reads stock tick messages from Pub/Sub.
  2. Parses and validates JSON payloads.
  3. Writes raw ticks to BigQuery (streaming inserts).
  4. Applies 1-minute fixed windowing to compute OHLCV aggregates.
  5. Writes aggregated data to a separate BigQuery table.

Usage:
    # Local testing with DirectRunner
    python -m src.realtime.dataflow_pipeline \\
        --runner DirectRunner \\
        --project your-project-id \\
        --input_subscription projects/your-project-id/subscriptions/stock-market-ticks-sub

    # Deploy to Dataflow
    python -m src.realtime.dataflow_pipeline \\
        --runner DataflowRunner \\
        --project your-project-id \\
        --region us-central1 \\
        --input_subscription projects/your-project-id/subscriptions/stock-market-ticks-sub \\
        --temp_location gs://your-bucket/temp/ \\
        --staging_location gs://your-bucket/staging/
"""

import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode

logger = logging.getLogger(__name__)


# ============================================================
# Schema (inline for Beam — mirrors src/realtime/schema.py)
# ============================================================

RAW_TICKS_TABLE_SCHEMA = {
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

AGGREGATED_TABLE_SCHEMA = {
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
# Transform Functions
# ============================================================

class ParseStockTick(beam.DoFn):
    """Parse raw Pub/Sub JSON message into a validated dict."""

    def process(self, element):
        try:
            message = json.loads(element.decode("utf-8"))

            # Validate required fields
            if "symbol" not in message or "price" not in message:
                logger.warning("Missing required fields: %s", message)
                return

            # Normalize timestamp
            ts = message.get("timestamp", "")
            if ts and "T" not in ts:
                # Alpha Vantage format: "2024-01-15 10:30:00"
                ts = ts.replace(" ", "T") + "Z"
            elif not ts:
                ts = datetime.now(timezone.utc).isoformat()

            ingestion = message.get(
                "ingestion_time",
                datetime.now(timezone.utc).isoformat(),
            )

            yield {
                "symbol": str(message["symbol"]),
                "price": float(message["price"]),
                "volume": int(message.get("volume", 0)),
                "timestamp": ts,
                "open": float(message.get("open", message["price"])),
                "high": float(message.get("high", message["price"])),
                "low": float(message.get("low", message["price"])),
                "close": float(message.get("close", message["price"])),
                "ingestion_time": ingestion,
            }

        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logger.error("Failed to parse message: %s — %s", element, e)


class ComputeOHLCV(beam.CombineFn):
    """
    Combine function that aggregates ticks within a window
    into a single OHLCV + VWAP record.
    """

    def create_accumulator(self):
        return {
            "ticks": [],
            "total_volume": 0,
            "price_volume_sum": 0.0,
            "count": 0,
        }

    def add_input(self, accumulator, tick):
        accumulator["ticks"].append(tick)
        vol = tick.get("volume", 0)
        accumulator["total_volume"] += vol
        accumulator["price_volume_sum"] += tick["price"] * vol
        accumulator["count"] += 1
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["ticks"].extend(acc["ticks"])
            merged["total_volume"] += acc["total_volume"]
            merged["price_volume_sum"] += acc["price_volume_sum"]
            merged["count"] += acc["count"]
        return merged

    def extract_output(self, accumulator):
        ticks = accumulator["ticks"]
        if not ticks:
            return None

        # Sort by timestamp to determine open/close
        sorted_ticks = sorted(ticks, key=lambda t: t.get("timestamp", ""))
        total_vol = accumulator["total_volume"]

        return {
            "open": sorted_ticks[0].get("open", sorted_ticks[0]["price"]),
            "high": max(t.get("high", t["price"]) for t in ticks),
            "low": min(t.get("low", t["price"]) for t in ticks),
            "close": sorted_ticks[-1].get("close", sorted_ticks[-1]["price"]),
            "vwap": (
                accumulator["price_volume_sum"] / total_vol
                if total_vol > 0 else sorted_ticks[-1]["price"]
            ),
            "total_volume": total_vol,
            "tick_count": accumulator["count"],
        }


class FormatAggregatedRow(beam.DoFn):
    """Format (key, aggregated_data) + window info into a BigQuery row."""

    def process(self, element, window=beam.DoFn.WindowParam):
        symbol, agg = element
        if agg is None:
            return

        yield {
            "symbol": symbol,
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "open": agg["open"],
            "high": agg["high"],
            "low": agg["low"],
            "close": agg["close"],
            "vwap": agg["vwap"],
            "total_volume": agg["total_volume"],
            "tick_count": agg["tick_count"],
        }


# ============================================================
# Pipeline Definition
# ============================================================

def run_pipeline(argv=None):
    """Build and run the streaming Beam pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        required=True,
        help="Pub/Sub subscription path (projects/<proj>/subscriptions/<sub>)",
    )
    parser.add_argument(
        "--raw_table",
        default="stock_market_data.raw_stock_ticks",
        help="BigQuery table for raw ticks (dataset.table)",
    )
    parser.add_argument(
        "--agg_table",
        default="stock_market_data.stock_aggregated_1min",
        help="BigQuery table for aggregated data (dataset.table)",
    )
    parser.add_argument(
        "--window_size_sec",
        type=int,
        default=60,
        help="Fixed window size in seconds (default: 60)",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:

        # ── Read from Pub/Sub ───────────────────────────────
        raw_messages = (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription
            )
        )

        # ── Parse & Validate ───────────────────────────────
        parsed_ticks = (
            raw_messages
            | "ParseTicks" >> beam.ParDo(ParseStockTick())
        )

        # ── Branch 1: Write raw ticks to BigQuery ──────────
        (
            parsed_ticks
            | "WriteRawToBQ" >> beam.io.WriteToBigQuery(
                table=known_args.raw_table,
                schema=RAW_TICKS_TABLE_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )

        # ── Branch 2: Windowed aggregation → BigQuery ──────
        (
            parsed_ticks
            | "KeyBySymbol" >> beam.Map(lambda tick: (tick["symbol"], tick))
            | "Window" >> beam.WindowInto(
                FixedWindows(known_args.window_size_sec),
                trigger=AfterWatermark(
                    early=AfterProcessingTime(30)   # emit early results every 30s
                ),
                accumulation_mode=AccumulationMode.DISCARDING,
            )
            | "AggregateOHLCV" >> beam.CombinePerKey(ComputeOHLCV())
            | "FormatAggRow" >> beam.ParDo(FormatAggregatedRow())
            | "WriteAggToBQ" >> beam.io.WriteToBigQuery(
                table=known_args.agg_table,
                schema=AGGREGATED_TABLE_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )

    logger.info("Pipeline completed.")


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_pipeline()
