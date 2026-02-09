import requests
import csv

url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"

headers = {
	"x-rapidapi-key": "90afb8345bmsh4d4300b9e2bd9ddp114c1ajsnda1a354a12a4",
	"x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

#print(response.json())

if response.status_code == 200:
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f'matches_{timestamp}.csv'
    
    # Extract matches from the nested structure
    matches_list = []
    response_data = response.json()
    
    # Navigate through the nested structure
    type_matches = response_data.get('typeMatches', [])
    for type_match in type_matches:
        series_matches = type_match.get('seriesMatches', [])
        for series_match in series_matches:
            series_ad_wrapper = series_match.get('seriesAdWrapper', {})
            matches = series_ad_wrapper.get('matches', [])
            for match in matches:
                match_info = match.get('matchInfo', {})
                if match_info:  # Only add if matchInfo exists
                    # Flatten the nested structure
                    flattened_match = {
                        'matchId': match_info.get('matchId'),
                        'seriesId': match_info.get('seriesId'),
                        'seriesName': match_info.get('seriesName'),
                        'matchDesc': match_info.get('matchDesc'),
                        'matchFormat': match_info.get('matchFormat'),
                        'startDate': match_info.get('startDate'),
                        'endDate': match_info.get('endDate'),
                        'state': match_info.get('state'),
                        'status': match_info.get('status'),
                        'team1_id': match_info.get('team1', {}).get('teamId'),
                        'team1_name': match_info.get('team1', {}).get('teamName'),
                        'team1_sname': match_info.get('team1', {}).get('teamSName'),
                        'team2_id': match_info.get('team2', {}).get('teamId'),
                        'team2_name': match_info.get('team2', {}).get('teamName'),
                        'team2_sname': match_info.get('team2', {}).get('teamSName'),
                        'venueInfo': match_info.get('venueInfo', {}).get('ground', ''),
                        'matchType': type_match.get('matchType', '')
                    }
                    matches_list.append(flattened_match)

    if matches_list:
        # Collect all unique keys from all entries
        field_names = set()
        for entry in matches_list:
            if isinstance(entry, dict):
                 field_names.update(entry.keys())
        
        field_names = list(field_names)

        # Write data to CSV file
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for entry in matches_list:
                writer.writerow(entry)

        print(f"Data fetched successfully and written to '{csv_filename}'")
    else:
        print("No data available from the API.")

else:
    print("Failed to fetch data:", response.status_code)

