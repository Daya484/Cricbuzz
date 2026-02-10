import requests
import csv
from google.cloud import storage
url = "https://cricbuzz-cricket2.p.rapidapi.com/teams/v1/international"

headers = {
	"x-rapidapi-key": "90afb8345bmsh4d4300b9e2bd9ddp114c1ajsnda1a354a12a4",
	"x-rapidapi-host": "cricbuzz-cricket2.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

#print(response.json())

params = {
    'formatType': 'TeamInternational'
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json().get('list', [])  # Extracting the 'list' data
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f'international_match_{timestamp}.csv'

    if data:
        #field_names = ['teamId', 'countryName', 'teamSName', 'imageId', 'teamName']  # Specify requ
        # Collect all unique keys from all entries to ensure we have all columns
        field_names = set()
        for entry in data:
            if isinstance(entry, dict):
                 field_names.update(entry.keys())
        
        field_names = list(field_names) # Convert to list for DictWriter

        # Write data to CSV file with only specified field names
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for entry in data:
                if isinstance(entry, dict) and 'teamId' in entry: # Keep the filter for valid teams if desired, or just all dicts
                     writer.writerow({field: entry.get(field) for field in field_names})

        print(f"Data fetched successfully and written to '{csv_filename}'")
        # Upload the CSV file to GCS
        bucket_name = 'testing-daya'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'  # The path to store in GCS

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(csv_filename)

        print(f"File {csv_filename} uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
    else:
        print("No data available from the API.")
else:
    print("Failed to fetch data:", response.status_code)