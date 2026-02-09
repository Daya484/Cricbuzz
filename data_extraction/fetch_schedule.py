import requests
import json
import csv
import datetime

url = "https://cricbuzz-cricket2.p.rapidapi.com/teams/v1/2/schedule"

headers = {
	"x-rapidapi-key": "90afb8345bmsh4d4300b9e2bd9ddp114c1ajsnda1a354a12a4",
	"x-rapidapi-host": "cricbuzz-cricket2.p.rapidapi.com"
}

params = {
    'formatType': 'schedule'
}

def flatten_dict(d, parent_key='', sep='.'):
    """
    recursively flattens a nested dictionary.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

print(f"Fetching data from {url}...")
response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f'schedule_full_{timestamp}.csv'
    
    all_rows = []
    
    if "teamMatchesData" in data:
        for item in data["teamMatchesData"]:
            if "matchDetailsMap" in item:
                match_map = item["matchDetailsMap"]
                match_date = match_map.get("key", "")
                
                if "match" in match_map:
                    for match_entry in match_map["match"]:
                        # Flatten the match entry (which usually contains matchInfo)
                        flat_entry = flatten_dict(match_entry)
                        
                        # Add the separated date key context
                        flat_entry['matchDate'] = match_date
                        
                        all_rows.append(flat_entry)

    if all_rows:
        # Collect all unique headers
        headers_set = set()
        for row in all_rows:
            headers_set.update(row.keys())
        
        # Sort headers for better readability, putting matchDate first if possible
        sorted_headers = sorted(list(headers_set))
        if 'matchDate' in sorted_headers:
            sorted_headers.insert(0, sorted_headers.pop(sorted_headers.index('matchDate')))

        try:
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=sorted_headers)
                writer.writeheader()
                writer.writerows(all_rows)
            print(f"Data successfully saved to '{csv_filename}' with {len(all_rows)} rows and {len(sorted_headers)} columns.")
        except IOError as e:
            print(f"Error writing to CSV: {e}")
    else:
        print("No match data found or parsed.")

else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
    print(response.text)