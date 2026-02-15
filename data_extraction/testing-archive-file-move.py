from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError, NotFound
import sys
import time

SOURCE_BUCKET = "testing-daya"
DEST_BUCKET = "archive-daya"
SOURCE_PREFIX = ""  # keep empty "" to move everything
DEST_PREFIX = ""


def move_blob(storage_client, source_bucket_name, dest_bucket_name, blob_name, dest_blob_name=None, retries=3):
    """Copy blob to destination bucket and delete source blob. Returns (True, dest_name) or (False, exception)."""
    if dest_blob_name is None:
        dest_blob_name = blob_name

    src_bucket = storage_client.bucket(source_bucket_name)
    dst_bucket = storage_client.bucket(dest_bucket_name)
    src_blob = src_bucket.blob(blob_name)

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            new_blob = src_bucket.copy_blob(src_blob, dst_bucket, new_name=dest_blob_name)
            src_blob.delete()
            return True, new_blob.name
        except (GoogleAPICallError, NotFound) as e:
            last_err = e
            time.sleep(2 * attempt)

    return False, last_err


def main():
    storage_client = storage.Client()

    try:
        # Validate buckets exist
        storage_client.get_bucket(DEST_BUCKET)
        storage_client.get_bucket(SOURCE_BUCKET)
    except NotFound:
        print("FAIL", file=sys.stderr)
        sys.exit(1)

    moved = 0
    failed = 0

    try:
        blobs = storage_client.list_blobs(SOURCE_BUCKET, prefix=SOURCE_PREFIX)
        for blob in blobs:
            # Skip directory markers
            if blob.name.endswith("/") and blob.size == 0:
                continue

            dest_name = f"{DEST_PREFIX}{blob.name}" if DEST_PREFIX else blob.name

            ok, _ = move_blob(
                storage_client,
                SOURCE_BUCKET,
                DEST_BUCKET,
                blob.name,
                dest_blob_name=dest_name
            )

            if ok:
                moved += 1
            else:
                failed += 1

    except Exception:
        # Any unexpected error during listing/iteration is treated as failure
        print("FAIL", file=sys.stderr)
        sys.exit(2)

    # Only output SUCCESS or FAIL, and set exit code
    if failed > 0:
        print("FAIL", file=sys.stderr)
        sys.exit(2)
    else:
        print("SUCCESS")
        sys.exit(0)


if __name__ == "__main__":
    main()