import requests
import csv
from google.cloud import storage
import datetime
from io import StringIO

url = "https://cricbuzz-cricket2.p.rapidapi.com/teams/v1/2/players"

headers = {
	"x-rapidapi-key": "90afb8345bmsh4d4300b9e2bd9ddp114c1ajsnda1a354a12a4",
	"x-rapidapi-host": "cricbuzz-cricket2.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

#print(response.json())

if response.status_code == 200:
    data = response.json()
    timestamp = datetime.datetime.now().strftime("%Y%m%d")
    csv_filename = f'team_team_players_{timestamp}.csv'
    
    # Extract player data - adjust based on actual API response structure
    players_list = []
    
    # Check if the response has a 'player' key with list of players
    if isinstance(data, dict):
        # Try to find players in common response structures
        if 'player' in data:
            players_data = data['player']
        elif 'players' in data:
            players_data = data['players']
        elif 'list' in data:
            players_data = data['list']
        else:
            # If it's a dict with nested structure, try to flatten it
            players_data = data
    else:
        players_data = data
    
    # Flatten the data structure
    if isinstance(players_data, list):
        for player in players_data:
            if isinstance(player, dict):
                players_list.append(player)
    elif isinstance(players_data, dict):
        # If it's a nested dict, extract all player entries
        for key, value in players_data.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        players_list.append(item)
            elif isinstance(value, dict):
                players_list.append(value)
    
    if players_list:
        # Collect all unique keys from all entries
        field_names = set()
        for entry in players_list:
            if isinstance(entry, dict):
                field_names.update(entry.keys())
        
        field_names = list(field_names)
        
        # Create CSV in memory using StringIO
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=field_names)
        writer.writeheader()
        for entry in players_list:
            writer.writerow(entry)
        
        # Upload directly to GCS without saving locally
        bucket_name = 'team-team_players'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'
        
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        
        #print(f"Data fetched successfully and uploaded directly to GCS bucket {bucket_name} as {destination_blob_name}")
       # print(f"No local file created - data uploaded directly to cloud storage.")
    else:
        print("No player data available from the API.")
        print("API Response:", data)
else:
    print("Failed to fetch data:", response.status_code)