import requests
import csv
from google.cloud import storage
import datetime
from io import StringIO

url = "https://cricbuzz-cricket2.p.rapidapi.com/stats/v1/rankings/teams"

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
        
        for team in rankings:
            # Flatten the team data
            flattened_team = {
                'rank': team.get('rank'),
                'name': team.get('name'),
                'rating': team.get('rating'),
                'points': team.get('points'),
                'difference': team.get('difference', ''),
                'trend': team.get('trend'),
                'lastUpdatedOn': team.get('lastUpdatedOn')
            }
            
            all_rankings.append(flattened_team)
    
    if all_rankings:
        # Generate timestamped filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_filename = f'icc_t20_teams_rankings_{timestamp}.csv'
        
        # Create CSV in memory using StringIO
        field_names = all_rankings[0].keys()
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(all_rankings)
        
        # Upload directly to GCS without saving locally
        bucket_name = 'icc-rank_teams'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'  # The path to store in GCS

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

        # print(f"Successfully extracted {len(all_rankings)} teams rankings and uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
        # print(f"No local file created - data uploaded directly to cloud storage.")
    else:
        print("No ranking data found in the response")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
