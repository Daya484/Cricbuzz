import requests
import csv
from google.cloud import storage
import datetime
from io import StringIO


url = "https://cricbuzz-cricket2.p.rapidapi.com/teams/v1/2/results"

headers = {
	"x-rapidapi-key": "90afb8345bmsh4d4300b9e2bd9ddp114c1ajsnda1a354a12a4",
	"x-rapidapi-host": "cricbuzz-cricket2.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    
    # Extract all matches from the nested structure
    all_matches = []
    
    if 'teamMatchesData' in data:
        for item in data['teamMatchesData']:
            if 'matchDetailsMap' in item:
                match_map = item['matchDetailsMap']
                series_name = match_map.get('key', '')
                matches = match_map.get('match', [])
                
                for match in matches:
                    match_info = match.get('matchInfo', {})
                    match_score = match.get('matchScore', {})
                    
                    # Flatten the match data
                    flattened_match = {
                        'series_name': series_name,
                        'match_id': match_info.get('matchId'),
                        'match_desc': match_info.get('matchDesc'),
                        'match_format': match_info.get('matchFormat'),
                        'match_status': match_info.get('status'),
                        'state': match_info.get('state'),
                        'team1_name': match_info.get('team1', {}).get('teamName'),
                        'team1_short': match_info.get('team1', {}).get('teamSName'),
                        'team2_name': match_info.get('team2', {}).get('teamName'),
                        'team2_short': match_info.get('team2', {}).get('teamSName'),
                        'venue_ground': match_info.get('venueInfo', {}).get('ground'),
                        'venue_city': match_info.get('venueInfo', {}).get('city'),
                        'start_date': match_info.get('startDate'),
                        'end_date': match_info.get('endDate'),
                    }
                    
                    # Add team scores
                    team1_score = match_score.get('team1Score', {}).get('inngs1', {})
                    team2_score = match_score.get('team2Score', {}).get('inngs1', {})
                    
                    flattened_match['team1_runs'] = team1_score.get('runs')
                    flattened_match['team1_wickets'] = team1_score.get('wickets')
                    flattened_match['team1_overs'] = team1_score.get('overs')
                    flattened_match['team2_runs'] = team2_score.get('runs')
                    flattened_match['team2_wickets'] = team2_score.get('wickets')
                    flattened_match['team2_overs'] = team2_score.get('overs')
                    
                    all_matches.append(flattened_match)
    
    if all_matches:
        # Generate timestamped filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        csv_filename = f'Team-team_results{timestamp}.csv'
        
        # Create CSV in memory using StringIO
        field_names = all_matches[0].keys()
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(all_matches)
        
        # Upload directly to GCS without saving locally
        bucket_name = 'team-team_result' 
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'  # The path to store in GCS

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

        # print(f"Successfully extracted {len(all_matches)} matches and uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
        # print(f"No local file created - data uploaded directly to cloud storage.")
    else:
        print("No match data found in the response")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")