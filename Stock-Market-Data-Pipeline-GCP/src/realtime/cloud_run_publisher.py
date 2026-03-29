"""
Cloud Run Publisher — Health Check Server + Stock Data Publisher.

Cloud Run requires an HTTP server responding on PORT (default 8080).
This module runs:
  1. A lightweight health-check HTTP server (for Cloud Run probes)
  2. The stock publisher loop in a background thread

Cloud Run will keep this service alive as long as it responds to
health checks. The publisher continuously fetches stock data and
publishes to Pub/Sub.

Environment Variables (set in Cloud Run):
  - ALPHA_VANTAGE_API_KEY  — API key for stock data
  - GCP_PROJECT_ID         — GCP project ID
  - PORT                   — HTTP port (Cloud Run sets this automatically)
"""

import logging
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s — %(message)s",
)
logger = logging.getLogger("cloud_run_publisher")


# ============================================================
# Health Check HTTP Server
# ============================================================

class HealthCheckHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler for Cloud Run health/readiness probes."""

    def do_GET(self):
        if self.path == "/health" or self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "healthy", "service": "stock-publisher"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress noisy health-check logs
        if "/health" not in str(args):
            logger.debug(format, *args)


def start_health_server(port: int):
    """Start the HTTP health-check server."""
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    logger.info("Health check server listening on port %d", port)
    server.serve_forever()


# ============================================================
# Main Entry Point
# ============================================================

def main():
    port = int(os.environ.get("PORT", 8080))

    # Start health check server in a daemon thread
    health_thread = threading.Thread(
        target=start_health_server,
        args=(port,),
        daemon=True,
    )
    health_thread.start()
    logger.info("Health check server started on port %d", port)

    # Load config and start the publisher
    from src.realtime.publisher import load_config, run_publisher

    config_path = os.environ.get("CONFIG_PATH", "config/config.yaml")

    try:
        config = load_config(config_path)
    except FileNotFoundError:
        logger.warning("Config file not found at %s, using defaults", config_path)
        config = {
            "gcp": {"project_id": os.environ.get("GCP_PROJECT_ID", "")},
            "pubsub": {"topic_name": os.environ.get("PUBSUB_TOPIC", "stock-market-ticks")},
            "api": {
                "symbols": os.environ.get("STOCK_SYMBOLS", "AAPL,GOOGL,MSFT,AMZN,TSLA").split(","),
                "polling_interval_seconds": int(os.environ.get("POLL_INTERVAL", "60")),
            },
        }

    logger.info("Starting stock publisher on Cloud Run…")
    run_publisher(config)


if __name__ == "__main__":
    main()
