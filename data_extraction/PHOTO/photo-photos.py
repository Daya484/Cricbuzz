import requests
import csv
from google.cloud import storage
import datetime
from io import StringIO

# Step 1: Get all gallery IDs from the photo list
url_list = "https://cricbuzz-cricket.p.rapidapi.com/photos/v1/index"

headers_list = {
	"x-rapidapi-key": "90afb8345bmsh4d4300b9e2bd9ddp114c1ajsnda1a354a12a4",
	"x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

print("Fetching photo gallery list...")
response_list = requests.get(url_list, headers=headers_list)

all_gallery_details = []

if response_list.status_code == 200:
    data = response_list.json()
    photo_gallery_info_list = data.get('photoGalleryInfoList', [])
    
    # Extract gallery IDs
    gallery_ids = []
    for item in photo_gallery_info_list:
        if 'photoGalleryInfo' in item:
            gallery_info = item['photoGalleryInfo']
            gallery_id = gallery_info.get('galleryId')  # Changed from 'id' to 'galleryId'
            if gallery_id:
                gallery_ids.append(gallery_id)
    
    print(f"Found {len(gallery_ids)} galleries. Fetching details...")
    
    # Step 2: Fetch details for each gallery
    headers_detail = {
        "x-rapidapi-key": "90afb8345bmsh4d4300b9e2bd9ddp114c1ajsnda1a354a12a4",
        "x-rapidapi-host": "cricbuzz-cricket2.p.rapidapi.com"
    }
    
    for idx, gallery_id in enumerate(gallery_ids, 1):
        url_detail = f"https://cricbuzz-cricket2.p.rapidapi.com/photos/v1/detail/{gallery_id}"
        
        try:
            response_detail = requests.get(url_detail, headers=headers_detail)
            
            if response_detail.status_code == 200:
                detail_data = response_detail.json()
                
                # Extract photo details from the gallery
                if 'photoGalleryDetails' in detail_data:
                    for photo in detail_data['photoGalleryDetails']:
                        photo_data = {
                            'gallery_id': gallery_id,
                            'gallery_headline': detail_data.get('headline', ''),
                            'gallery_intro': detail_data.get('intro', ''),
                            'gallery_published_time': detail_data.get('publishedTime', ''),
                            'gallery_state': detail_data.get('state', ''),
                        }
                        
                        # Add photo-specific fields
                        if isinstance(photo, dict):
                            photo_data.update(photo)
                        
                        all_gallery_details.append(photo_data)
                
                print(f"Processed gallery {idx}/{len(gallery_ids)}: {gallery_id}")
            else:
                print(f"Failed to fetch details for gallery {gallery_id}: Status {response_detail.status_code}")
        
        except Exception as e:
            print(f"Error processing gallery {gallery_id}: {str(e)}")
    
    # Step 3: Upload to GCS if we have data
    if all_gallery_details:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f'gallery_details_{timestamp}.csv'
        
        # Collect all unique keys from all entries
        field_names = set()
        for entry in all_gallery_details:
            if isinstance(entry, dict):
                field_names.update(entry.keys())
        
        field_names = list(field_names)
        
        # Create CSV in memory using StringIO
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=field_names)
        writer.writeheader()
        for entry in all_gallery_details:
            writer.writerow(entry)
        
        # Upload directly to GCS without saving locally
        bucket_name = 'photo-photos'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'
        
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        
        # print(f"\nSuccessfully extracted {len(all_gallery_details)} photo records from {len(gallery_ids)} galleries")
        # print(f"Data uploaded directly to GCS bucket {bucket_name} as {destination_blob_name}")
        # print(f"No local file created - data uploaded directly to cloud storage.")
    else:
        print("No gallery detail data available from the API.")
else:
    print(f"Failed to fetch photo list: {response_list.status_code}")
