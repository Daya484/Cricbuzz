import requests
import csv

url = "https://cricbuzz-cricket.p.rapidapi.com/photos/v1/index"

headers = {
	"x-rapidapi-key": "90afb8345bmsh4d4300b9e2bd9ddp114c1ajsnda1a354a12a4",
	"x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

#print(response.json())

if response.status_code == 200:
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d")
    csv_filename = f'photolist_{timestamp}.csv'
    
    # Extract photo galleries from the nested structure
    photo_list = []
    response_data = response.json()
    
    # Navigate through the nested structure
    photo_gallery_info_list = response_data.get('photoGalleryInfoList', [])
    for item in photo_gallery_info_list:
        # Each item can be either a photoGalleryInfo or an ad
        if 'photoGalleryInfo' in item:
            photo_info = item['photoGalleryInfo']
            photo_list.append(photo_info)

    if photo_list:
        # Collect all unique keys from all entries
        field_names = set()
        for entry in photo_list:
            if isinstance(entry, dict):
                 field_names.update(entry.keys())
        
        field_names = list(field_names)

        # Write data to CSV file
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for entry in photo_list:
                writer.writerow(entry)

        print(f"Data fetched successfully and written to '{csv_filename}'")
    else:
        print("No data available from the API.")

else:
    print("Failed to fetch data:", response.status_code)