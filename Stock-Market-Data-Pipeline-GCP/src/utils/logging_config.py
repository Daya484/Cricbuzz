"""
Centralized Logging Configuration.

Configures Python logging with optional Cloud Logging (Stackdriver)
integration for GCP environments.
"""

import logging
import os
import sys


def setup_logging(
    level: str = "INFO",
    enable_cloud_logging: bool = False,
    log_name: str = "stock-market-pipeline",
) -> logging.Logger:
    """
    Configure the root logger with console output and optional Cloud Logging.

    Args:
        level:                 Logging level (DEBUG, INFO, WARNING, ERROR).
        enable_cloud_logging:  If True, also send logs to Google Cloud Logging.
        log_name:              Name of the Cloud Logging logger.

    Returns:
        Configured root logger.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # ── Console Handler ─────────────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(
        "%(asctime)s  [%(levelname)-8s]  %(name)-30s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_formatter)

    # ── Root Logger ─────────────────────────────────────────
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)

    # ── Cloud Logging (optional) ────────────────────────────
    if enable_cloud_logging:
        try:
            import google.cloud.logging as cloud_logging

            client = cloud_logging.Client()
            cloud_handler = cloud_logging.handlers.CloudLoggingHandler(
                client, name=log_name,
            )
            cloud_handler.setLevel(log_level)
            root_logger.addHandler(cloud_handler)
            root_logger.info("Cloud Logging enabled for '%s'.", log_name)

        except ImportError:
            root_logger.warning(
                "google-cloud-logging not installed. Skipping Cloud Logging."
            )
        except Exception as e:
            root_logger.warning("Failed to initialize Cloud Logging: %s", e)

    return root_logger
