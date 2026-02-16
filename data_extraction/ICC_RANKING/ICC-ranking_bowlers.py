import requests
import csv
from google.cloud import storage
import datetime
from io import StringIO

url = "https://cricbuzz-cricket2.p.rapidapi.com/stats/v1/rankings/bowlers"

querystring = {"isWomen": "0", "formatType": "t20"}

headers = {
    "x-rapidapi-key": "90afb8345bmsh4d4300b9e2bd9ddp114c1ajsnda1a354a12a4",
    "x-rapidapi-host": "cricbuzz-cricket2.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

if response.status_code == 200:
    data = response.json()
    
    # Extract rankings data
    all_rankings = []
    
    if 'rank' in data:
        rankings = data['rank']
        
        for player in rankings:
            # Flatten the player data
            flattened_player = {
                'rank': player.get('rank'),
                'id': player.get('id'),
                'name': player.get('name'),
                'country': player.get('country'),
                'countryId': player.get('countryId'),
                'rating': player.get('rating'),
                'points': player.get('points'),
                'difference': player.get('difference', ''),
                'trend': player.get('trend'),
                'lastUpdatedOn': player.get('lastUpdatedOn'),
                'faceImageId': player.get('faceImageId')
            }
            
            all_rankings.append(flattened_player)
    
    if all_rankings:
        # Generate timestamped filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_filename = f'icc_t20_bowlers_rankings_{timestamp}.csv'
        
        # Create CSV in memory using StringIO
        field_names = all_rankings[0].keys()
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(all_rankings)
         
        # Upload directly to GCS without saving locally
        bucket_name = 'icc_rank_bolwer'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'  # The path to store in GCS

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

        # print(f"Successfully extracted {len(all_rankings)} bowlers rankings and uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
        # print(f"No local file created - data uploaded directly to cloud storage.")
    else:
        print("No ranking data found in the response")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")