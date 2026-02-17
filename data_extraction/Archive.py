from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError, NotFound
import sys
import time
import datetime

SOURCE_BUCKETS = ['testing-daya', 'team-team_result', 'team-team_players', 'photo-photos', 
                  'photo-photo_galary', 'icc_rank_bolwer', 'icc-rank_teams', 'icc-rank_batsmen', 
                  'icc-rank_allrounder']
DEST_BUCKET = "archive-daya"
SOURCE_PREFIX = ""  # keep empty "" to move everything


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
    
    # Get current date in YYYY-MM-DD format
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    date_prefix = f"{current_date}/"

    try:
        # Validate destination bucket exists
        storage_client.get_bucket(DEST_BUCKET)
    except NotFound:
        print(f"ERROR: Destination bucket '{DEST_BUCKET}' not found", file=sys.stderr)
        print("FAIL", file=sys.stderr)
        sys.exit(1)

    total_moved = 0
    total_failed = 0

    # Process each source bucket
    for source_bucket_name in SOURCE_BUCKETS:
        print(f"Processing bucket: {source_bucket_name}")
        
        try:
            # Validate source bucket exists
            storage_client.get_bucket(source_bucket_name)
        except NotFound:
            print(f"WARNING: Source bucket '{source_bucket_name}' not found, skipping...", file=sys.stderr)
            continue

        moved = 0
        failed = 0

        try:
            blobs = storage_client.list_blobs(source_bucket_name, prefix=SOURCE_PREFIX)
            for blob in blobs:
                # Skip directory markers
                if blob.name.endswith("/") and blob.size == 0:
                    continue

                # Add date prefix to destination path
                dest_name = f"{date_prefix}{blob.name}"

                ok, result = move_blob(
                    storage_client,
                    source_bucket_name,
                    DEST_BUCKET,
                    blob.name,
                    dest_blob_name=dest_name
                )

                if ok:
                    moved += 1
                    print(f"  ✓ Moved: {blob.name} -> {dest_name}")
                else:
                    failed += 1
                    print(f"  ✗ Failed: {blob.name} - {result}", file=sys.stderr)

        except Exception as e:
            print(f"ERROR processing bucket '{source_bucket_name}': {e}", file=sys.stderr)
            failed += 1

        print(f"Bucket '{source_bucket_name}': {moved} moved, {failed} failed\n")
        total_moved += moved
        total_failed += failed

    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY:")
    print(f"Total files moved: {total_moved}")
    print(f"Total files failed: {total_failed}")
    print(f"Destination: {DEST_BUCKET}/{current_date}/")
    print(f"{'='*60}")

    # Only output SUCCESS or FAIL, and set exit code
    if total_failed > 0:
        print("FAIL", file=sys.stderr)
        sys.exit(2)
    else:
        print("SUCCESS")
        sys.exit(0)


if __name__ == "__main__":
    main()