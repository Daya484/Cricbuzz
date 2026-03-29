"""
Google Cloud Storage Helper Functions.

Upload, download, list, and manage blobs in GCS.
"""

import logging
import os
from pathlib import Path

from google.cloud import storage

logger = logging.getLogger(__name__)


def get_client(project_id: str) -> storage.Client:
    """Get a GCS client."""
    return storage.Client(project=project_id)


def create_bucket(
    client: storage.Client,
    bucket_name: str,
    location: str = "US",
) -> storage.Bucket:
    """Create a GCS bucket if it doesn't exist."""
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket = client.create_bucket(bucket_name, location=location)
        logger.info("Created bucket: gs://%s", bucket_name)
    else:
        logger.info("Bucket gs://%s already exists.", bucket_name)
    return bucket


def upload_file(
    client: storage.Client,
    bucket_name: str,
    local_path: str,
    gcs_blob_name: str,
) -> str:
    """Upload a local file to GCS. Returns the GCS URI."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(local_path)
    uri = f"gs://{bucket_name}/{gcs_blob_name}"
    logger.info("Uploaded %s → %s", local_path, uri)
    return uri


def download_file(
    client: storage.Client,
    bucket_name: str,
    gcs_blob_name: str,
    local_path: str,
) -> str:
    """Download a GCS blob to a local file."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_blob_name)
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(local_path)
    logger.info("Downloaded gs://%s/%s → %s", bucket_name, gcs_blob_name, local_path)
    return local_path


def list_blobs(
    client: storage.Client,
    bucket_name: str,
    prefix: str = "",
    delimiter: str | None = None,
) -> list[str]:
    """List blob names in a bucket with optional prefix filter."""
    blobs = client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)
    names = [blob.name for blob in blobs]
    logger.info("Found %d blobs in gs://%s/%s", len(names), bucket_name, prefix)
    return names


def delete_blob(
    client: storage.Client,
    bucket_name: str,
    gcs_blob_name: str,
) -> None:
    """Delete a blob from GCS."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_blob_name)
    blob.delete()
    logger.info("Deleted gs://%s/%s", bucket_name, gcs_blob_name)


def get_blob_size(
    client: storage.Client,
    bucket_name: str,
    gcs_blob_name: str,
) -> int:
    """Get the size of a blob in bytes."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_blob_name)
    blob.reload()
    return blob.size or 0
