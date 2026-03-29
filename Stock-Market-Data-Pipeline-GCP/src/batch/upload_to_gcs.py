"""
Upload Local Files to Google Cloud Storage.

Uploads historical CSV files (or any local files) to
a GCS bucket for downstream batch processing via Dataproc.

Usage:
    python -m src.batch.upload_to_gcs
    python -m src.batch.upload_to_gcs --env qa --source data/historical
    python -m src.batch.upload_to_gcs --env pd --prefix raw/
"""

import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage

from src.utils.config_loader import load_config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s — %(message)s",
)
logger = logging.getLogger("upload_gcs")


def upload_file(bucket: storage.Bucket, local_path: str, gcs_prefix: str) -> str:
    """Upload a single file to GCS. Returns the GCS URI."""
    filename = os.path.basename(local_path)
    blob_name = f"{gcs_prefix}{filename}"
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(local_path)
    gcs_uri = f"gs://{bucket.name}/{blob_name}"
    logger.info("Uploaded %s → %s", local_path, gcs_uri)
    return gcs_uri


def upload_directory(
    bucket: storage.Bucket,
    local_dir: str,
    gcs_prefix: str,
    pattern: str = "*.csv",
) -> list[str]:
    """Upload all matching files in a directory to GCS."""
    local_path = Path(local_dir)
    if not local_path.exists():
        logger.error("Directory not found: %s", local_dir)
        return []

    uploaded = []
    for file_path in sorted(local_path.glob(pattern)):
        uri = upload_file(bucket, str(file_path), gcs_prefix)
        uploaded.append(uri)

    logger.info("Uploaded %d files to gs://%s/%s", len(uploaded), bucket.name, gcs_prefix)
    return uploaded


def main():
    parser = argparse.ArgumentParser(description="Upload files to GCS")
    parser.add_argument("--env", default=None, choices=["dv", "qa", "pd"],
                        help="Environment (dv/qa/pd)")
    parser.add_argument("--source", default="data/historical",
                        help="Local directory or file to upload")
    parser.add_argument("--prefix", default=None,
                        help="GCS prefix (overrides config)")
    parser.add_argument("--pattern", default="*.csv",
                        help="File glob pattern (default: *.csv)")
    args = parser.parse_args()

    cfg = load_config(env=args.env)
    project_id = os.getenv("GCP_PROJECT_ID", cfg["gcp"]["project_id"])
    bucket_name = cfg["gcs"]["bucket_name"]
    gcs_prefix = args.prefix or cfg["gcs"]["raw_data_prefix"]

    # Ensure prefix ends with /
    if not gcs_prefix.endswith("/"):
        gcs_prefix += "/"

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    source = Path(args.source)
    if source.is_file():
        upload_file(bucket, str(source), gcs_prefix)
    elif source.is_dir():
        upload_directory(bucket, str(source), gcs_prefix, pattern=args.pattern)
    else:
        logger.error("Source not found: %s", args.source)


if __name__ == "__main__":
    main()
