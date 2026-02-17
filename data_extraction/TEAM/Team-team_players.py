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

if response.status_code == 200:
    data = response.json()
    
    # Extract player data
    all_players = []
    
    # Navigate through the API response structure to find players
    if isinstance(data, dict):
        # Try common response structures
        if 'player' in data:
            players_data = data['player']
        elif 'players' in data:
            players_data = data['players']
        elif 'list' in data:
            players_data = data['list']
        else:
            players_data = []
            # Check for nested structures
            for key, value in data.items():
                if isinstance(value, list):
                    players_data = value
                    break
    else:
        players_data = data if isinstance(data, list) else []
    
    # Extract only the specified fields from each player
    if isinstance(players_data, list):
        for player in players_data:
            if isinstance(player, dict):
                # Flatten the player data with only required fields
                flattened_player = {
                    'name': player.get('name'),
                    'battingStyle': player.get('battingStyle'),
                    'id': player.get('id'),
                    'bowlingStyle': player.get('bowlingStyle'),
                    'imageId': player.get('imageId')
                }
                
                all_players.append(flattened_player)
    
    if all_players:
        # Generate timestamped filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_filename = f'team_team_players_{timestamp}.csv'
        
        # Create CSV in memory using StringIO
        field_names = all_players[0].keys()
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(all_players)
        
        # Upload directly to GCS without saving locally
        bucket_name = 'team-team_players'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'
        
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        
        # print(f"Successfully extracted {len(all_players)} players and uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
        # print(f"No local file created - data uploaded directly to cloud storage.")
    else:
        print("No player data found in the response")
        print("API Response structure:", list(data.keys()) if isinstance(data, dict) else type(data))
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")