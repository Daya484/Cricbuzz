import requests
import csv
import json
import datetime
import sys
import time
from io import StringIO
from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError, NotFound

# ─────────────────────────────────────────────
# Load config from requirements.json
# ─────────────────────────────────────────────
import os

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "requirements.json")

with open(CONFIG_PATH, "r") as f:
    CONFIG = json.load(f)

API_KEY      = CONFIG["api"]["key"]
HOST_V1      = CONFIG["api"]["host_v1"]
HOST_V2      = CONFIG["api"]["host_v2"]
BASE_URL_V1  = CONFIG["api"]["base_url_v1"]
BASE_URL_V2  = CONFIG["api"]["base_url_v2"]
ENDPOINTS    = CONFIG["endpoints"]
BUCKETS      = CONFIG["gcs_buckets"]
ARCHIVE_DEST = CONFIG["archive"]["dest_bucket"]
ARCHIVE_SRCS = CONFIG["archive"]["source_buckets"]

# ─────────────────────────────────────────────
# Helper: Build API headers
# ─────────────────────────────────────────────
def get_headers(host):
    return {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": host
    }

# ─────────────────────────────────────────────
# Helper: Upload CSV string to GCS bucket
# ─────────────────────────────────────────────
def upload_to_gcs(bucket_name, filename, csv_content):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(csv_content, content_type="text/csv")
    print(f"  ✅ Uploaded '{filename}' → GCS bucket: '{bucket_name}'")

# ─────────────────────────────────────────────
# Helper: Convert list of dicts to CSV string
# ─────────────────────────────────────────────
def to_csv_string(records):
    if not records:
        return None
    # Collect all unique field names across all records (safe for varying schemas)
    field_names = list(dict.fromkeys(
        key for record in records for key in record.keys()
    ))
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=field_names)
    writer.writeheader()
    writer.writerows(records)
    return buffer.getvalue()

# ─────────────────────────────────────────────
# ICC Rankings: Allrounders
# ─────────────────────────────────────────────
def extract_icc_rankings_allrounders():
    print("\n[ICC Rankings] Extracting Allrounders...")
    url = BASE_URL_V2 + ENDPOINTS["icc_rankings_allrounders"]
    response = requests.get(url, headers=get_headers(HOST_V2), params={"isWomen": "0", "formatType": "t20"})

    if response.status_code != 200:
        print(f"  ❌ Failed. Status: {response.status_code}")
        return

    data = response.json()
    records = []
    for player in data.get("rank", []):
        records.append({
            "rank": player.get("rank"),
            "id": player.get("id"),
            "name": player.get("name"),
            "country": player.get("country"),
            "countryId": player.get("countryId"),
            "rating": player.get("rating"),
            "points": player.get("points"),
            "difference": player.get("difference", ""),
            "trend": player.get("trend"),
            "lastUpdatedOn": player.get("lastUpdatedOn"),
            "faceImageId": player.get("faceImageId")
        })

    if records:
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_file  = f"icc_t20_allrounders_rankings_{timestamp}.csv"
        upload_to_gcs(BUCKETS["icc_ranking_allrounders"], csv_file, to_csv_string(records))
    else:
        print("  ⚠️  No allrounders ranking data found.")

# ─────────────────────────────────────────────
# ICC Rankings: Batsmen
# ─────────────────────────────────────────────
def extract_icc_rankings_batsmen():
    print("\n[ICC Rankings] Extracting Batsmen...")
    url = BASE_URL_V2 + ENDPOINTS["icc_rankings_batsmen"]
    response = requests.get(url, headers=get_headers(HOST_V2), params={"isWomen": "0", "formatType": "t20"})

    if response.status_code != 200:
        print(f"  ❌ Failed. Status: {response.status_code}")
        return

    data = response.json()
    records = []
    for player in data.get("rank", []):
        records.append({
            "rank": player.get("rank"),
            "id": player.get("id"),
            "name": player.get("name"),
            "country": player.get("country"),
            "countryId": player.get("countryId"),
            "rating": player.get("rating"),
            "points": player.get("points"),
            "difference": player.get("difference", ""),
            "trend": player.get("trend"),
            "lastUpdatedOn": player.get("lastUpdatedOn"),
            "faceImageId": player.get("faceImageId")
        })

    if records:
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_file  = f"icc_t20_batsmen_rankings_{timestamp}.csv"
        upload_to_gcs(BUCKETS["icc_ranking_batsmen"], csv_file, to_csv_string(records))
    else:
        print("  ⚠️  No batsmen ranking data found.")

# ─────────────────────────────────────────────
# ICC Rankings: Bowlers
# ─────────────────────────────────────────────
def extract_icc_rankings_bowlers():
    print("\n[ICC Rankings] Extracting Bowlers...")
    url = BASE_URL_V2 + ENDPOINTS["icc_rankings_bowlers"]
    response = requests.get(url, headers=get_headers(HOST_V2), params={"isWomen": "0", "formatType": "t20"})

    if response.status_code != 200:
        print(f"  ❌ Failed. Status: {response.status_code}")
        return

    data = response.json()
    records = []
    for player in data.get("rank", []):
        records.append({
            "rank": player.get("rank"),
            "id": player.get("id"),
            "name": player.get("name"),
            "country": player.get("country"),
            "countryId": player.get("countryId"),
            "rating": player.get("rating"),
            "points": player.get("points"),
            "difference": player.get("difference", ""),
            "trend": player.get("trend"),
            "lastUpdatedOn": player.get("lastUpdatedOn"),
            "faceImageId": player.get("faceImageId")
        })

    if records:
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_file  = f"icc_t20_bowlers_rankings_{timestamp}.csv"
        upload_to_gcs(BUCKETS["icc_ranking_bowlers"], csv_file, to_csv_string(records))
    else:
        print("  ⚠️  No bowlers ranking data found.")

# ─────────────────────────────────────────────
# ICC Rankings: Teams
# ─────────────────────────────────────────────
def extract_icc_rankings_teams():
    print("\n[ICC Rankings] Extracting Teams...")
    url = BASE_URL_V2 + ENDPOINTS["icc_rankings_teams"]
    response = requests.get(url, headers=get_headers(HOST_V2), params={"isWomen": "0", "formatType": "t20"})

    if response.status_code != 200:
        print(f"  ❌ Failed. Status: {response.status_code}")
        return

    data = response.json()
    records = []
    for team in data.get("rank", []):
        records.append({
            "rank": team.get("rank"),
            "name": team.get("name"),
            "rating": team.get("rating"),
            "points": team.get("points"),
            "difference": team.get("difference", ""),
            "trend": team.get("trend"),
            "lastUpdatedOn": team.get("lastUpdatedOn")
        })

    if records:
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_file  = f"icc_t20_teams_rankings_{timestamp}.csv"
        upload_to_gcs(BUCKETS["icc_ranking_teams"], csv_file, to_csv_string(records))
    else:
        print("  ⚠️  No team ranking data found.")

# ─────────────────────────────────────────────
# Photo: Gallery (Photo Gallery)
# ─────────────────────────────────────────────
def extract_photo_gallery():
    print("\n[Photo] Extracting Photo Gallery...")

    # Step 1: Get all gallery IDs
    url_list = BASE_URL_V1 + ENDPOINTS["photo_gallery_index"]
    response_list = requests.get(url_list, headers=get_headers(HOST_V1))

    if response_list.status_code != 200:
        print(f"  ❌ Failed to fetch gallery list. Status: {response_list.status_code}")
        return

    data = response_list.json()
    gallery_ids = []
    for item in data.get("photoGalleryInfoList", []):
        gallery_info = item.get("photoGalleryInfo", {})
        gid = gallery_info.get("galleryId")
        if gid:
            gallery_ids.append(gid)

    print(f"  Found {len(gallery_ids)} galleries. Fetching details...")

    # Step 2: Fetch details for each gallery
    all_records = []
    for idx, gallery_id in enumerate(gallery_ids, 1):
        url_detail = BASE_URL_V2 + ENDPOINTS["photo_gallery_detail"].format(gallery_id=gallery_id)
        try:
            response_detail = requests.get(url_detail, headers=get_headers(HOST_V2))
            if response_detail.status_code == 200:
                detail_data = response_detail.json()
                for photo in detail_data.get("photoGalleryDetails", []):
                    photo_data = {
                        "gallery_id": gallery_id,
                        "gallery_headline": detail_data.get("headline", ""),
                        "gallery_intro": detail_data.get("intro", ""),
                        "gallery_published_time": detail_data.get("publishedTime", ""),
                        "gallery_state": detail_data.get("state", "")
                    }
                    if isinstance(photo, dict):
                        photo_data.update(photo)
                    all_records.append(photo_data)
                print(f"  Processed gallery {idx}/{len(gallery_ids)}: {gallery_id}")
            else:
                print(f"  ⚠️  Gallery {gallery_id} detail failed: Status {response_detail.status_code}")
        except Exception as e:
            print(f"  ❌ Error processing gallery {gallery_id}: {e}")

    if all_records:
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_file  = f"photo_gallery_details_{timestamp}.csv"
        upload_to_gcs(BUCKETS["photo_gallery"], csv_file, to_csv_string(all_records))
    else:
        print("  ⚠️  No gallery detail data available.")

# ─────────────────────────────────────────────
# Photo: Photos (Timestamped Filename Variant)
# ─────────────────────────────────────────────
def extract_photo_photos():
    print("\n[Photo] Extracting Photos (timestamped)...")

    # Step 1: Get all gallery IDs
    url_list = BASE_URL_V1 + ENDPOINTS["photo_gallery_index"]
    response_list = requests.get(url_list, headers=get_headers(HOST_V1))

    if response_list.status_code != 200:
        print(f"  ❌ Failed to fetch photo list. Status: {response_list.status_code}")
        return

    data = response_list.json()
    gallery_ids = []
    for item in data.get("photoGalleryInfoList", []):
        gallery_info = item.get("photoGalleryInfo", {})
        gid = gallery_info.get("galleryId")
        if gid:
            gallery_ids.append(gid)

    print(f"  Found {len(gallery_ids)} galleries. Fetching details...")

    # Step 2: Fetch details for each gallery
    all_records = []
    for idx, gallery_id in enumerate(gallery_ids, 1):
        url_detail = BASE_URL_V2 + ENDPOINTS["photo_gallery_detail"].format(gallery_id=gallery_id)
        try:
            response_detail = requests.get(url_detail, headers=get_headers(HOST_V2))
            if response_detail.status_code == 200:
                detail_data = response_detail.json()
                for photo in detail_data.get("photoGalleryDetails", []):
                    photo_data = {
                        "gallery_id": gallery_id,
                        "gallery_headline": detail_data.get("headline", ""),
                        "gallery_intro": detail_data.get("intro", ""),
                        "gallery_published_time": detail_data.get("publishedTime", ""),
                        "gallery_state": detail_data.get("state", "")
                    }
                    if isinstance(photo, dict):
                        photo_data.update(photo)
                    all_records.append(photo_data)
                print(f"  Processed gallery {idx}/{len(gallery_ids)}: {gallery_id}")
            else:
                print(f"  ⚠️  Gallery {gallery_id} detail failed: Status {response_detail.status_code}")
        except Exception as e:
            print(f"  ❌ Error processing gallery {gallery_id}: {e}")

    if all_records:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file  = f"gallery_details_{timestamp}.csv"
        upload_to_gcs(BUCKETS["photo_photos"], csv_file, to_csv_string(all_records))
    else:
        print("  ⚠️  No photo detail data available.")

# ─────────────────────────────────────────────
# Team: International Teams
# ─────────────────────────────────────────────
def extract_team_international():
    print("\n[Team] Extracting International Teams...")
    url = BASE_URL_V2 + ENDPOINTS["team_international"]
    response = requests.get(url, headers=get_headers(HOST_V2), params={"formatType": "TeamInternational"})

    if response.status_code != 200:
        print(f"  ❌ Failed. Status: {response.status_code}")
        return

    data = response.json()
    records = []
    for team in data.get("list", []):
        records.append({
            "teamId": team.get("teamId"),
            "imageId": team.get("imageId"),
            "countryName": team.get("countryName"),
            "teamSName": team.get("teamSName"),
            "teamName": team.get("teamName")
        })

    if records:
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_file  = f"team_international_{timestamp}.csv"
        upload_to_gcs(BUCKETS["team_international"], csv_file, to_csv_string(records))
    else:
        print("  ⚠️  No international team data found.")

# ─────────────────────────────────────────────
# Team: Players
# ─────────────────────────────────────────────
def extract_team_players():
    print("\n[Team] Extracting Team Players...")
    url = BASE_URL_V2 + ENDPOINTS["team_players"]
    response = requests.get(url, headers=get_headers(HOST_V2))

    if response.status_code != 200:
        print(f"  ❌ Failed. Status: {response.status_code}")
        return

    data = response.json()

    # Handle varying response structures
    if isinstance(data, dict):
        if "player" in data:
            players_data = data["player"]
        elif "players" in data:
            players_data = data["players"]
        elif "list" in data:
            players_data = data["list"]
        else:
            players_data = []
            for key, value in data.items():
                if isinstance(value, list):
                    players_data = value
                    break
    else:
        players_data = data if isinstance(data, list) else []

    records = []
    for player in players_data:
        if isinstance(player, dict):
            records.append({
                "name": player.get("name"),
                "battingStyle": player.get("battingStyle"),
                "id": player.get("id"),
                "bowlingStyle": player.get("bowlingStyle"),
                "imageId": player.get("imageId")
            })

    if records:
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_file  = f"team_team_players_{timestamp}.csv"
        upload_to_gcs(BUCKETS["team_players"], csv_file, to_csv_string(records))
    else:
        print("  ⚠️  No player data found.")
        print("  API Response keys:", list(data.keys()) if isinstance(data, dict) else type(data))

# ─────────────────────────────────────────────
# Team: Match Results
# ─────────────────────────────────────────────
def extract_team_results():
    print("\n[Team] Extracting Team Results...")
    url = BASE_URL_V2 + ENDPOINTS["team_results"]
    response = requests.get(url, headers=get_headers(HOST_V2))

    if response.status_code != 200:
        print(f"  ❌ Failed. Status: {response.status_code}")
        return

    data = response.json()
    records = []

    for item in data.get("teamMatchesData", []):
        match_map   = item.get("matchDetailsMap", {})
        series_name = match_map.get("key", "")
        for match in match_map.get("match", []):
            match_info  = match.get("matchInfo", {})
            match_score = match.get("matchScore", {})

            team1_score = match_score.get("team1Score", {}).get("inngs1", {})
            team2_score = match_score.get("team2Score", {}).get("inngs1", {})

            records.append({
                "series_name": series_name,
                "match_id": match_info.get("matchId"),
                "match_desc": match_info.get("matchDesc"),
                "match_format": match_info.get("matchFormat"),
                "match_status": match_info.get("status"),
                "state": match_info.get("state"),
                "team1_name": match_info.get("team1", {}).get("teamName"),
                "team1_short": match_info.get("team1", {}).get("teamSName"),
                "team2_name": match_info.get("team2", {}).get("teamName"),
                "team2_short": match_info.get("team2", {}).get("teamSName"),
                "venue_ground": match_info.get("venueInfo", {}).get("ground"),
                "venue_city": match_info.get("venueInfo", {}).get("city"),
                "start_date": match_info.get("startDate"),
                "end_date": match_info.get("endDate"),
                "team1_runs": team1_score.get("runs"),
                "team1_wickets": team1_score.get("wickets"),
                "team1_overs": team1_score.get("overs"),
                "team2_runs": team2_score.get("runs"),
                "team2_wickets": team2_score.get("wickets"),
                "team2_overs": team2_score.get("overs")
            })

    if records:
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_file  = f"Team-team_results{timestamp}.csv"
        upload_to_gcs(BUCKETS["team_results"], csv_file, to_csv_string(records))
    else:
        print("  ⚠️  No match result data found.")

# ─────────────────────────────────────────────
# Archive: Move all source bucket files to archive bucket
# ─────────────────────────────────────────────
def _move_blob(storage_client, source_bucket_name, dest_bucket_name, blob_name, dest_blob_name=None, retries=3):
    """Copy blob to destination bucket and delete the source blob.
    Returns (True, dest_name) on success or (False, exception) on failure."""
    if dest_blob_name is None:
        dest_blob_name = blob_name

    src_bucket = storage_client.bucket(source_bucket_name)
    dst_bucket = storage_client.bucket(dest_bucket_name)
    src_blob   = src_bucket.blob(blob_name)

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


def archive_to_gcs():
    """Move all files from every source bucket into the archive bucket
    under a YYYY-MM-DD/ date prefix folder."""
    print("\n[Archive] Moving files to archive bucket...")
    storage_client = storage.Client()
    current_date   = datetime.datetime.now().strftime("%Y-%m-%d")
    date_prefix    = f"{current_date}/"

    # Validate the destination archive bucket exists
    try:
        storage_client.get_bucket(ARCHIVE_DEST)
    except NotFound:
        print(f"  ❌ ERROR: Archive destination bucket '{ARCHIVE_DEST}' not found.", file=sys.stderr)
        return

    total_moved  = 0
    total_failed = 0

    for source_bucket_name in ARCHIVE_SRCS:
        print(f"  Processing bucket: {source_bucket_name}")

        try:
            storage_client.get_bucket(source_bucket_name)
        except NotFound:
            print(f"  ⚠️  Source bucket '{source_bucket_name}' not found, skipping...", file=sys.stderr)
            continue

        moved  = 0
        failed = 0

        try:
            blobs = storage_client.list_blobs(source_bucket_name, prefix="")
            for blob in blobs:
                # Skip directory markers
                if blob.name.endswith("/") and blob.size == 0:
                    continue

                dest_name = f"{date_prefix}{blob.name}"
                ok, result = _move_blob(
                    storage_client,
                    source_bucket_name,
                    ARCHIVE_DEST,
                    blob.name,
                    dest_blob_name=dest_name
                )

                if ok:
                    moved += 1
                    print(f"    ✓ Moved: {blob.name} → {dest_name}")
                else:
                    failed += 1
                    print(f"    ✗ Failed: {blob.name} — {result}", file=sys.stderr)

        except Exception as e:
            print(f"  ❌ ERROR processing bucket '{source_bucket_name}': {e}", file=sys.stderr)
            failed += 1

        print(f"  Bucket '{source_bucket_name}': {moved} moved, {failed} failed")
        total_moved  += moved
        total_failed += failed

    print(f"\n  {'='*50}")
    print(f"  Archive Summary:")
    print(f"  Total moved : {total_moved}")
    print(f"  Total failed: {total_failed}")
    print(f"  Destination : {ARCHIVE_DEST}/{current_date}/")
    print(f"  {'='*50}")

    if total_failed > 0:
        print("  ⚠️  Some files failed to archive.", file=sys.stderr)
    else:
        print("  ✅ Archive complete.")


# ─────────────────────────────────────────────
# Main: Run all extractions
# ─────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  Cricbuzz Data Extraction — Starting All Jobs")
    print("=" * 55)

    # ICC Rankings
    extract_icc_rankings_allrounders()
    extract_icc_rankings_batsmen()
    extract_icc_rankings_bowlers()
    extract_icc_rankings_teams()

    # Photo
    extract_photo_gallery()
    extract_photo_photos()

    # Team
    extract_team_international()
    extract_team_players()
    extract_team_results()

    # Archive — move everything to archive bucket after extraction
    archive_to_gcs()

    print("\n" + "=" * 55)
    print("  All extractions complete.")
    print("=" * 55)


if __name__ == "__main__":
    import sys

    # Map of CLI argument name → function to call
    DISPATCH = {
        "extract_icc_rankings_allrounders": extract_icc_rankings_allrounders,
        "extract_icc_rankings_batsmen":     extract_icc_rankings_batsmen,
        "extract_icc_rankings_bowlers":     extract_icc_rankings_bowlers,
        "extract_icc_rankings_teams":       extract_icc_rankings_teams,
        "extract_photo_gallery":            extract_photo_gallery,
        "extract_photo_photos":             extract_photo_photos,
        "extract_team_international":       extract_team_international,
        "extract_team_players":             extract_team_players,
        "extract_team_results":             extract_team_results,
        "archive_to_gcs":                   archive_to_gcs,
    }

    if len(sys.argv) > 1:
        # Called from Airflow BashOperator with a specific function name
        func_name = sys.argv[1]
        if func_name in DISPATCH:
            print(f"Running: {func_name}")
            DISPATCH[func_name]()
        else:
            print(f"ERROR: Unknown function '{func_name}'", file=sys.stderr)
            print(f"Available: {list(DISPATCH.keys())}", file=sys.stderr)
            sys.exit(1)
    else:
        # No argument — run all extractions + archive
        main()

