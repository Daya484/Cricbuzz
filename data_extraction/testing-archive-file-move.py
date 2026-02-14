from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError, NotFound
import sys
import time


SOURCE_BUCKET = "testing-daya"
DEST_BUCKET = "archive-daya"

# Optional: move only objects under a prefix (folder-like path). Example: "incoming/"
SOURCE_PREFIX = ""  # keep empty "" to move everything

# Optional: preserve the same object name in destination (recommended)
# You can also add a prefix in destination, e.g., DEST_PREFIX="archive/"
DEST_PREFIX = ""


def move_blob(storage_client, source_bucket_name, dest_bucket_name, blob_name, dest_blob_name=None, retries=3):
    """Copy blob to destination bucket and delete source blob."""
    if dest_blob_name is None:
        dest_blob_name = blob_name

    src_bucket = storage_client.bucket(source_bucket_name)
    dst_bucket = storage_client.bucket(dest_bucket_name)

    src_blob = src_bucket.blob(blob_name)

    # Copy (with retries)
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            new_blob = src_bucket.copy_blob(src_blob, dst_bucket, new_name=dest_blob_name)
            # Optional: you can verify size/CRC here if desired
            src_blob.delete()
            return True, new_blob.name
        except (GoogleAPICallError, NotFound) as e:
            last_err = e
            time.sleep(2 * attempt)

    return False, last_err


def main():
    storage_client = storage.Client()

    try:
        source_bucket = storage_client.bucket(SOURCE_BUCKET)
        dest_bucket = storage_client.bucket(DEST_BUCKET)

        # Ensure destination bucket exists (will throw if not)
        storage_client.get_bucket(DEST_BUCKET)
        storage_client.get_bucket(SOURCE_BUCKET)

    except NotFound as e:
        print(f"Bucket not found: {e}", file=sys.stderr)
        sys.exit(1)

    moved = 0
    failed = 0

    print(f"Listing objects in gs://{SOURCE_BUCKET}/{SOURCE_PREFIX} ...")

    # List all blobs (handles pagination internally)
    blobs = storage_client.list_blobs(SOURCE_BUCKET, prefix=SOURCE_PREFIX)

    for blob in blobs:
        # Skip "directory markers" if any (rare but can exist as zero-size objects ending with '/')
        if blob.name.endswith("/") and blob.size == 0:
            continue

        dest_name = f"{DEST_PREFIX}{blob.name}" if DEST_PREFIX else blob.name

        ok, info = move_blob(
            storage_client,
            SOURCE_BUCKET,
            DEST_BUCKET,
            blob.name,
            dest_blob_name=dest_name
        )

        if ok:
            moved += 1
            print(f"✅ Moved: gs://{SOURCE_BUCKET}/{blob.name} -> gs://{DEST_BUCKET}/{dest_name}")
        else:
            failed += 1
            print(f"❌ Failed: {blob.name} | Error: {info}", file=sys.stderr)

    print("\n--- Summary ---")
    print(f"Moved  : {moved}")
    print(f"Failed : {failed}")

    # Exit non-zero if failures occurred (useful for CI)
    if failed > 0:
        sys.exit(2)


if __name__ == "__main__":
    main()