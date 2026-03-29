"""
BigQuery Helper Functions.

Create datasets, tables, run queries, and check data freshness.
"""

import logging
from datetime import datetime, timezone

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict

logger = logging.getLogger(__name__)


def get_client(project_id: str) -> bigquery.Client:
    """Get a BigQuery client."""
    return bigquery.Client(project=project_id)


def create_dataset(client: bigquery.Client, dataset_id: str, location: str = "US") -> None:
    """Create a BigQuery dataset if it doesn't exist."""
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location

    try:
        client.create_dataset(dataset, exists_ok=True)
        logger.info("Dataset %s.%s ready.", client.project, dataset_id)
    except Conflict:
        logger.info("Dataset %s already exists.", dataset_id)


def create_table(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    schema: list,
    partition_field: str | None = None,
    clustering_fields: list[str] | None = None,
) -> None:
    """Create a BigQuery table with optional partitioning and clustering."""
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)

    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )

    if clustering_fields:
        table.clustering_fields = clustering_fields

    try:
        client.create_table(table, exists_ok=True)
        logger.info("Table %s.%s.%s ready.", client.project, dataset_id, table_id)
    except Conflict:
        logger.info("Table %s.%s already exists.", dataset_id, table_id)


def run_query(client: bigquery.Client, query: str) -> list[dict]:
    """Execute a SQL query and return results as list of dicts."""
    job = client.query(query)
    results = job.result()
    return [dict(row) for row in results]


def get_row_count(client: bigquery.Client, dataset_id: str, table_id: str) -> int:
    """Get the number of rows in a table."""
    query = f"SELECT COUNT(*) as cnt FROM `{client.project}.{dataset_id}.{table_id}`"
    results = run_query(client, query)
    return results[0]["cnt"] if results else 0


def get_latest_timestamp(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    timestamp_col: str = "timestamp",
) -> datetime | None:
    """Get the latest timestamp from a table to check data freshness."""
    query = (
        f"SELECT MAX({timestamp_col}) as latest "
        f"FROM `{client.project}.{dataset_id}.{table_id}`"
    )
    try:
        results = run_query(client, query)
        if results and results[0]["latest"]:
            return results[0]["latest"]
    except NotFound:
        logger.warning("Table %s.%s not found.", dataset_id, table_id)
    return None


def check_data_freshness(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    max_age_hours: int = 24,
    timestamp_col: str = "timestamp",
) -> bool:
    """
    Check if the latest data is within max_age_hours.
    Returns True if data is fresh, False otherwise.
    """
    latest = get_latest_timestamp(client, dataset_id, table_id, timestamp_col)
    if latest is None:
        logger.warning("No data found in %s.%s", dataset_id, table_id)
        return False

    age = datetime.now(timezone.utc) - latest.replace(tzinfo=timezone.utc)
    age_hours = age.total_seconds() / 3600

    if age_hours <= max_age_hours:
        logger.info("Data is fresh (%.1f hours old).", age_hours)
        return True
    else:
        logger.warning("Data is stale (%.1f hours old, max=%d).", age_hours, max_age_hours)
        return False
