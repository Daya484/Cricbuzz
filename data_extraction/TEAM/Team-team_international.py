import requests
import csv
from google.cloud import storage
import datetime
from io import StringIO

url = "https://cricbuzz-cricket2.p.rapidapi.com/teams/v1/international"

querystring = {"formatType": "TeamInternational"}

headers = {
    "x-rapidapi-key": "90afb8345bmsh4d4300b9e2bd9ddp114c1ajsnda1a354a12a4",
    "x-rapidapi-host": "cricbuzz-cricket2.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

if response.status_code == 200:
    data = response.json()
    
    # Extract team data
    all_teams = []
    
    if 'list' in data:
        teams = data['list']
        
        for team in teams:
            # Flatten the team data
            flattened_team = {
                'teamId': team.get('teamId'),
                'imageId': team.get('imageId'),
                'countryName': team.get('countryName'),
                'teamSName': team.get('teamSName'),
                'teamName': team.get('teamName')
            }
            
            all_teams.append(flattened_team)
    
    if all_teams:
        # Generate timestamped filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_filename = f'team_international_{timestamp}.csv'
        
        # Create CSV in memory using StringIO
        field_names = all_teams[0].keys()
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(all_teams)
        
        # Upload directly to GCS without saving locally
        bucket_name = 'testing-daya'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'  # The path to store in GCS

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

        # print(f"Successfully extracted {len(all_teams)} international teams and uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
        # print(f"No local file created - data uploaded directly to cloud storage.")
    else:
        print("No team data found in the response")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")