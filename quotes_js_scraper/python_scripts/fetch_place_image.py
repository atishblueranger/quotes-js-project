import os
import json
import time
import requests
import threading
import concurrent.futures
import googlemaps
import firebase_admin
from firebase_admin import credentials, firestore, storage
from googlemaps.exceptions import ApiError
from google.api_core.exceptions import NotFound

# Initialize Firebase Admin SDK
def initialize_firebase():
    firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
    cred = credentials.Certificate(firebase_credentials_path)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred, {
            'storageBucket': 'mycasavsc.appspot.com'
        })
    db = firestore.client()
    return db

# Initialize Google Maps Client
def initialize_google_maps():
    gmaps = googlemaps.Client(key='AIzaSyBaIgAs4ZNgi5chk9kwh2Az57wyje67KZA')
    return gmaps

# Get place details from Google Places API
def get_place_details(gmaps, place_id):
    try:
        response = gmaps.place(place_id=place_id, fields=['geometry', 'photo'])
        result = response.get('result', {})
        photos = result.get('photos', [])
        lat_lng = result.get('geometry', {}).get('location', {})
        latitude = lat_lng.get('lat')
        longitude = lat_lng.get('lng')
        photo_references = [photo.get('photo_reference') for photo in photos[:4]]
        return photo_references, latitude, longitude
    except ApiError as e:
        print(f"Error fetching place details for {place_id}: {e}")
        return [], None, None

# Download the photo using the photo_reference
def download_photo(photo_reference, session, api_key):
    photo_url = "https://maps.googleapis.com/maps/api/place/photo"
    params = {
        'maxwidth': 800,
        'photoreference': photo_reference,
        'key': api_key,
    }
    try:
        with session.get(photo_url, params=params, stream=True, timeout=10) as response:
            response.raise_for_status()
            return response.content
    except requests.exceptions.RequestException as e:
        print(f"Error downloading photo: {e}")
        return None

# Upload photo to Firebase Storage
def upload_photo_to_firebase_storage(photo_data, place_id, photo_num):
    try:
        bucket = storage.bucket()
        storage_path = f"places/{place_id}_{photo_num}.jpg"
        blob = bucket.blob(storage_path)
        blob.upload_from_string(photo_data, content_type="image/jpeg")
        blob.make_public()
        return blob.public_url
    except Exception as e:
        print(f"Error uploading photo to Firebase Storage: {e}")
        return None

# Update Firestore document with image URLs and latitude/longitude
def update_firestore_document(db, city_id, place_id, photo_urls, latitude, longitude):
    try:
        doc_ref = db.collection('exploreData').document(city_id).collection('TouristAttractions').document(place_id)
        doc = doc_ref.get()
        if doc.exists:
            update_data = {}
            if photo_urls:
                update_data['g_image_urls'] = firestore.ArrayUnion(photo_urls)
            if latitude is not None and longitude is not None:
                update_data['latitude'] = latitude
                update_data['longitude'] = longitude
            doc_ref.update(update_data)
            print(f"Updated Firestore document for place_id {place_id}")
        else:
            print(f"Document for place_id {place_id} not found in city {city_id}.")
    except Exception as e:
        print(f"Error updating Firestore document for place_id {place_id}: {e}")

# Process each place
def process_place(db, gmaps, place_entry, session, api_key):
    try:
        city_id = place_entry.get('city_id')
        place_id = place_entry.get('placeId')
        if not place_id or not city_id:
            print(f"Missing place_id or city_id for place: {place_entry}")
            return

        photo_references, latitude, longitude = get_place_details(gmaps, place_id)
        if not photo_references:
            print(f"No photos found for place_id {place_id}")
            return

        photo_urls = []
        # Download and upload photos concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_photo_num = {
                executor.submit(
                    download_and_upload_photo,
                    photo_reference,
                    place_id,
                    idx + 1,
                    session,
                    api_key
                ): idx + 1
                for idx, photo_reference in enumerate(photo_references)
            }
            for future in concurrent.futures.as_completed(future_to_photo_num):
                photo_num = future_to_photo_num[future]
                photo_url = future.result()
                if photo_url:
                    photo_urls.append(photo_url)

        if photo_urls or latitude is not None or longitude is not None:
            update_firestore_document(db, city_id, place_id, photo_urls, latitude, longitude)

    except Exception as e:
        print(f"Error processing place {place_entry.get('placeId')}: {e}")

def download_and_upload_photo(photo_reference, place_id, photo_num, session, api_key):
    photo_data = download_photo(photo_reference, session, api_key)
    if photo_data:
        photo_url = upload_photo_to_firebase_storage(photo_data, place_id, photo_num)
        return photo_url
    else:
        print(f"Failed to download or upload photo {photo_num} for place_id {place_id}")
        return None

# Main function to process all places from JSON file
def process_places_from_json(db, gmaps, json_file_path, max_workers=10):
    # Load the JSON data
    with open(json_file_path, 'r', encoding='utf-8') as f:
        places_data = json.load(f)

    total_places = len(places_data)
    print(f"Total places to process: {total_places}")

    api_key = 'AIzaSyBaIgAs4ZNgi5chk9kwh2Az57wyje67KZA'  # Replace with your API key

    # Use a single session for all requests
    with requests.Session() as session:
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        # Process places concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_place, db, gmaps, place_entry, session, api_key): place_entry
                for place_entry in places_data
            }
            for idx, future in enumerate(concurrent.futures.as_completed(futures)):
                place_entry = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing place {place_entry.get('placeId')}: {e}")
                if (idx + 1) % 100 == 0:
                    print(f"Processed {idx + 1}/{total_places} places.")

    print("Processing completed.")

# Entry point of the script
if __name__ == '__main__':
    db = initialize_firebase()
    gmaps = initialize_google_maps()
    json_file_path = 'pending_tourist_attraction_places2.json'  # Path to your JSON file
    process_places_from_json(db, gmaps, json_file_path, max_workers=10)


## tourist attraction
# import os
# import json
# import time
# import requests
# import googlemaps
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# from googlemaps.exceptions import ApiError
# from google.api_core.exceptions import NotFound

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     if not firebase_admin._apps:
#         firebase_admin.initialize_app(cred, {
#             'storageBucket': 'mycasavsc.appspot.com'
#         })
#     db = firestore.client()
#     return db

# # Initialize Google Maps Client
# def initialize_google_maps():
#     gmaps = googlemaps.Client(key='AIzaSyBaIgAs4ZNgi5chk9kwh2Az57wyje67KZA')
#     return gmaps

# # Get place details from Google Places API
# def get_place_details(gmaps, place_id):
#     try:
#         response = gmaps.place(place_id=place_id, fields=['geometry', 'photo'])
#         result = response.get('result', {})
#         photos = result.get('photos', [])
#         lat_lng = result.get('geometry', {}).get('location', {})
#         latitude = lat_lng.get('lat')
#         longitude = lat_lng.get('lng')
#         photo_references = [photo.get('photo_reference') for photo in photos[:4]]
#         return photo_references, latitude, longitude
#     except ApiError as e:
#         print(f"Error fetching place details for {place_id}: {e}")
#         return [], None, None

# # Download the photo using the photo_reference
# def download_photo(photo_reference):
#     photo_url = "https://maps.googleapis.com/maps/api/place/photo"
#     params = {
#         'maxwidth': 800,
#         'photoreference': photo_reference,
#         'key': 'AIzaSyBaIgAs4ZNgi5chk9kwh2Az57wyje67KZA',
#     }
#     try:
#         response = requests.get(photo_url, params=params, stream=True, timeout=10)
#         response.raise_for_status()
#         return response.content
#     except requests.exceptions.RequestException as e:
#         print(f"Error downloading photo: {e}")
#         return None

# # Upload photo to Firebase Storage
# def upload_photo_to_firebase_storage(photo_data, place_id, photo_num):
#     try:
#         bucket = storage.bucket()
#         storage_path = f"places/{place_id}_{photo_num}.jpg"
#         blob = bucket.blob(storage_path)
#         blob.upload_from_string(photo_data, content_type="image/jpeg")
#         blob.make_public()
#         return blob.public_url
#     except Exception as e:
#         print(f"Error uploading photo to Firebase Storage: {e}")
#         return None

# # Update Firestore document with image URLs and latitude/longitude
# def update_firestore_document(db, city_id, place_id, photo_urls, latitude, longitude):
#     try:
#         doc_ref = db.collection('exploreData').document(city_id).collection('TouristAttractions').document(place_id)
#         doc = doc_ref.get()
#         if doc.exists:
#             update_data = {}
#             if photo_urls:
#                 update_data['g_image_urls'] = firestore.ArrayUnion(photo_urls)
#             if latitude is not None and longitude is not None:
#                 update_data['latitude'] = latitude
#                 update_data['longitude'] = longitude
#             doc_ref.update(update_data)
#             print(f"Updated Firestore document for place_id {place_id}")
#         else:
#             print(f"Document for place_id {place_id} not found in city {city_id}.")
#     except Exception as e:
#         print(f"Error updating Firestore document for place_id {place_id}: {e}")

# # Process each place
# def process_place(db, gmaps, place_entry):
#     city_id = place_entry.get('city_id')
#     place_id = place_entry.get('placeId')
#     if not place_id or not city_id:
#         print(f"Missing place_id or city_id for place: {place_entry}")
#         return

#     photo_references, latitude, longitude = get_place_details(gmaps, place_id)
#     if not photo_references:
#         print(f"No photos found for place_id {place_id}")
#         return

#     photo_urls = []
#     for idx, photo_reference in enumerate(photo_references):
#         photo_data = download_photo(photo_reference)
#         if photo_data:
#             photo_url = upload_photo_to_firebase_storage(photo_data, place_id, idx + 1)
#             if photo_url:
#                 photo_urls.append(photo_url)

#     if photo_urls or latitude is not None or longitude is not None:
#         update_firestore_document(db, city_id, place_id, photo_urls, latitude, longitude)

# # Main function to process all places from JSON file
# def process_places_from_json(db, gmaps, json_file_path, batch_size=100, delay=1):
#     # Load the JSON data
#     with open(json_file_path, 'r', encoding='utf-8') as f:
#         places_data = json.load(f)

#     total_places = len(places_data)
#     print(f"Total places to process: {total_places}")

#     api_calls = 0
#     max_api_calls = 20000  # Adjust as per your quota
#     for idx, place_entry in enumerate(places_data):
#         if api_calls >= max_api_calls:
#             print(f"Reached API call limit of {max_api_calls}. Stopping...")
#             break

#         process_place(db, gmaps, place_entry)
#         api_calls += 1

#         if (idx + 1) % batch_size == 0:
#             print(f"Processed {idx + 1}/{total_places} places. Pausing to avoid API limits.")
#             time.sleep(30)  # Adjust sleep time as needed

#         time.sleep(delay)  # Delay between API calls

#     print("Processing completed.")

# # Entry point of the script
# if __name__ == '__main__':
#     db = initialize_firebase()
#     gmaps = initialize_google_maps()
#     json_file_path = 'tourist_attractions_places.json'  # Path to your JSON file
#     process_places_from_json(db, gmaps, json_file_path, batch_size=100, delay=1)

# import os
# import time
# import requests
# import googlemaps
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# from googlemaps.exceptions import ApiError
# from google.api_core.exceptions import NotFound

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'
#     })
#     db = firestore.client()
#     return db

# # Initialize Google Maps Client
# def initialize_google_maps():
#     gmaps = googlemaps.Client(key='AIzaSyBaIgAs4ZNgi5chk9kwh2Az57wyje67KZA')
#     return gmaps

# # Get place details from Google Places API
# def get_place_details(gmaps, place_id):
#     try:
#         response = gmaps.place(place_id=place_id, fields=['geometry', 'photo'])
#         result = response.get('result', {})
#         photos = result.get('photos', [])
#         lat_lng = result.get('geometry', {}).get('location', {})
#         latitude = lat_lng.get('lat')
#         longitude = lat_lng.get('lng')
#         photo_references = [photo.get('photo_reference') for photo in photos[:4]]
#         return photo_references, latitude, longitude
#     except ApiError as e:
#         print(f"Error fetching place details for {place_id}: {e}")
#         return [], None, None

# # Download the photo using the photo_reference
# def download_photo(photo_reference):
#     photo_url = "https://maps.googleapis.com/maps/api/place/photo"
#     params = {
#         'maxwidth': 800,
#         'photoreference': photo_reference,
#         'key': 'AIzaSyBaIgAs4ZNgi5chk9kwh2Az57wyje67KZA',
#     }
#     try:
#         response = requests.get(photo_url, params=params, stream=True, timeout=10)
#         response.raise_for_status()
#         return response.content
#     except requests.exceptions.RequestException as e:
#         print(f"Error downloading photo: {e}")
#         return None

# # Upload photo to Firebase Storage
# def upload_photo_to_firebase_storage(photo_data, place_id, photo_num):
#     bucket = storage.bucket()
#     storage_path = f"places/{place_id}_{photo_num}.jpg"
#     blob = bucket.blob(storage_path)
#     blob.upload_from_string(photo_data, content_type="image/jpeg")
#     blob.make_public()
#     return blob.public_url

# # Update Firestore document with image URLs and latitude/longitude
# def update_firestore_document(db, city_id, place_id, photo_urls, latitude, longitude):
#     try:
#         doc_ref = db.collection('exploreData').document(city_id).collection('TouristAttractions').document(place_id)
#         doc = doc_ref.get()
#         if doc.exists:
#             update_data = {
#                 'g_image_urls': firestore.ArrayUnion(photo_urls),
#                 'latitude': latitude,
#                 'longitude': longitude
#             }
#             doc_ref.update(update_data)
#             print(f"Updated Firestore document for place_id {place_id}")
#         else:
#             print(f"Document for place_id {place_id} not found.")
#     except Exception as e:
#         print(f"Error updating Firestore document for place_id {place_id}: {e}")

# # Process each place
# def process_place(db, gmaps, city_id, place_data):
#     place_id = place_data.get('placeId')
#     if not place_id:
#         print(f"Missing place_id for place: {place_data}")
#         return

#     photo_references, latitude, longitude = get_place_details(gmaps, place_id)
#     if not photo_references:
#         return

#     photo_urls = []
#     for idx, photo_reference in enumerate(photo_references):
#         photo_data = download_photo(photo_reference)
#         if photo_data:
#             photo_url = upload_photo_to_firebase_storage(photo_data, place_id, idx + 1)
#             photo_urls.append(photo_url)

#     update_firestore_document(db, city_id, place_id, photo_urls, latitude, longitude)

# # Main function to process all places with pagination
# def process_places(db, gmaps, batch_size=100, delay=1, max_api_calls=20000):
#     total_processed = 0
#     api_calls = 0
#     last_document = None

#     while api_calls < max_api_calls:
#         # Use the last_document for pagination (start_after)
#         query = db.collection('exploreData').limit(batch_size)
#         if last_document:
#             query = query.start_after(last_document)

#         national_parks = query.stream()

#         last_fetched_doc = None
#         for idx, park in enumerate(national_parks):
#             last_fetched_doc = park
#             city_id = park.id

#             # Fetch TouristAttractions and TouristRestaurants subcollections
#             for subcollection in ['TouristAttractions', 'TouristRestaurants']:
#                 places = db.collection('exploreData').document(city_id).collection(subcollection).stream()

#                 for place in places:
#                     place_data = place.to_dict()
#                     process_place(db, gmaps, city_id, place_data)
#                     api_calls += 1

#                     if api_calls >= max_api_calls:
#                         print(f"Reached API call limit of {max_api_calls}. Stopping...")
#                         return

#             total_processed += 1
#             if total_processed % batch_size == 0:
#                 print(f"Processed {total_processed} places. Pausing to avoid API limits.")
#                 time.sleep(30)

#             time.sleep(delay)

#         if last_fetched_doc is None:
#             # No more documents to process
#             break
#         else:
#             last_document = last_fetched_doc

# # Entry point of the script
# if __name__ == '__main__':
#     db = initialize_firebase()
#     gmaps = initialize_google_maps()
#     process_places(db, gmaps, batch_size=100, delay=1, max_api_calls=20000)



# import os
# import time
# import requests
# import googlemaps
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# from googlemaps.exceptions import ApiError

# # Replace with your actual API key
# GOOGLE_PLACES_API_KEY = 'AIzaSyBaIgAs4ZNgi5chk9kwh2Az57wyje67KZA'

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     # Replace with the path to your Firebase service account JSON file
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'
#     })
#     db = firestore.client()
#     return db

# # Initialize Google Maps Client
# def initialize_google_maps():
#     gmaps = googlemaps.Client(key=GOOGLE_PLACES_API_KEY)
#     return gmaps

# # Get details for a place_id from Google Places API
# def get_place_details(gmaps, place_id):
#     try:
#         response = gmaps.place(place_id=place_id, fields=['geometry', 'photo'])
#         result = response.get('result', {})
#         photos = result.get('photos', [])
#         lat_lng = result.get('geometry', {}).get('location', {})

#         # Extract latitude and longitude if available
#         latitude = lat_lng.get('lat')
#         longitude = lat_lng.get('lng')

#         # Extract up to 4 photo references if available
#         photo_references = [photo.get('photo_reference') for photo in photos[:4]]

#         return photo_references, latitude, longitude

#     except ApiError as e:
#         print(f"Error fetching place details for {place_id}: {e}")
#         return [], None, None

# # Download the photo using the photo_reference
# def download_photo(photo_reference):
#     photo_url = "https://maps.googleapis.com/maps/api/place/photo"
#     params = {
#         'maxwidth': 800,
#         'photoreference': photo_reference,
#         'key': GOOGLE_PLACES_API_KEY,
#     }
#     try:
#         response = requests.get(photo_url, params=params, stream=True, timeout=10)
#         response.raise_for_status()
#         return response.content
#     except requests.exceptions.RequestException as e:
#         print(f"Error downloading photo: {e}")
#         return None

# # Upload photo to Firebase Storage
# def upload_photo_to_firebase_storage(photo_data, place_id, photo_num):
#     bucket = storage.bucket()
#     storage_path = f"places/{place_id}_{photo_num}.jpg"
#     blob = bucket.blob(storage_path)

#     # Upload the photo data
#     blob.upload_from_string(photo_data, content_type="image/jpeg")
#     blob.make_public()  # Make the file public

#     return blob.public_url

# # Update Firestore document with image URLs and latitude/longitude
# def update_firestore_document(db, city_id, place_id, photo_urls, latitude, longitude):
#     try:
#         # Reference to the document in the subcollection
#         doc_ref = db.collection('exploreData').document(city_id).collection('TouristAttractions').document(place_id)

#         # Check if the document exists
#         doc = doc_ref.get()
#         if doc.exists:
#             # Prepare update data
#             update_data = {
#                 'g_image_urls': firestore.ArrayUnion(photo_urls),
#                 'latitude': latitude,
#                 'longitude': longitude
#             }
#             doc_ref.update(update_data)
#             print(f"Updated Firestore document for place_id {place_id}")
#         else:
#             print(f"Document for place_id {place_id} not found.")
#     except Exception as e:
#         print(f"Error updating Firestore document for place_id {place_id}: {e}")

# # Process each place
# def process_place(db, gmaps, city_id, place_data):
#     place_id = place_data.get('placeId')

#     if not place_id:
#         print(f"Missing place_id for place: {place_data}")
#         return

#     # Get Google Place details
#     photo_references, latitude, longitude = get_place_details(gmaps, place_id)
#     if not photo_references:
#         return

#     # Download and upload photos
#     photo_urls = []
#     for idx, photo_reference in enumerate(photo_references):
#         photo_data = download_photo(photo_reference)
#         if photo_data:
#             photo_url = upload_photo_to_firebase_storage(photo_data, place_id, idx + 1)
#             photo_urls.append(photo_url)

#     # Update Firestore with image URLs and lat/lng
#     update_firestore_document(db, city_id, place_id, photo_urls, latitude, longitude)

# # Main function to process all places
# def process_places(db, gmaps, batch_size=100, delay=1, max_api_calls=20000):
#     total_processed = 0
#     api_calls = 0

#     while api_calls < max_api_calls:
#         # Fetch documents from Firestore
#         national_parks = db.collection('exploreData').limit(batch_size).stream()

#         for idx, park in enumerate(national_parks):
#             city_id = park.id

#             # Fetch TouristAttractions and TouristRestaurants subcollections
#             for subcollection in ['TouristAttractions']:
#                 places = db.collection('exploreData').document(city_id).collection(subcollection).stream()

#                 for place in places:
#                     place_data = place.to_dict()
#                     process_place(db, gmaps, city_id, place_data)
#                     api_calls += 1

#                     if api_calls >= max_api_calls:
#                         print(f"Reached API call limit of {max_api_calls}. Stopping...")
#                         return

#             total_processed += 1
#             if total_processed % batch_size == 0:
#                 print(f"Processed {total_processed} places. Pausing to avoid API limits.")
#                 time.sleep(30)

#             time.sleep(delay)

# # Entry point of the script
# if __name__ == '__main__':
#     db = initialize_firebase()
#     gmaps = initialize_google_maps()
#     process_places(db, gmaps, batch_size=100, delay=1, max_api_calls=20000)


# import os
# import json
# import requests
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# import googlemaps
# from googlemaps.exceptions import ApiError
# from google.api_core.exceptions import NotFound
# import time

# # Replace with your actual API key
# GOOGLE_PLACES_API_KEY = 'AIzaSyBaIgAs4ZNgi5chk9kwh2Az57wyje67KZA'

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'
#     })
#     db = firestore.client()
#     return db

# # Initialize Google Maps Client
# def initialize_google_maps():
#     gmaps = googlemaps.Client(key=GOOGLE_PLACES_API_KEY)
#     return gmaps

# # Get up to 4 photo references and other details for a place
# def get_place_details(gmaps, place_id):
#     try:
#         response = gmaps.place(place_id=place_id, fields=['photo', 'geometry'])
#         result = response.get('result', {})
#         photos = result.get('photos', [])
#         lat_lng = result.get('geometry', {}).get('location', {})

#         # Extract latitude and longitude if available
#         latitude = lat_lng.get('lat')
#         longitude = lat_lng.get('lng')

#         # Get up to 4 photo references
#         photo_references = []
#         for photo in photos[:4]:  # Limit to a maximum of 4 photos
#             photo_reference = photo.get('photo_reference')
#             if photo_reference:
#                 photo_references.append(photo_reference)

#         return photo_references, latitude, longitude

#     except ApiError as e:
#         print(f"Error fetching place details for {place_id}: {e}")
#         return [], None, None

# # Download and return photos using the photo references
# def download_photos(photo_references):
#     downloaded_photos = []
#     photo_url = "https://maps.googleapis.com/maps/api/place/photo"
    
#     for idx, photo_reference in enumerate(photo_references):
#         params = {
#             'maxwidth': 800,
#             'photoreference': photo_reference,
#             'key': GOOGLE_PLACES_API_KEY,
#         }
#         try:
#             response = requests.get(photo_url, params=params, stream=True, timeout=10)
#             response.raise_for_status()
#             downloaded_photos.append(response.content)  # Append photo content to the list
#             print(f"Downloaded photo {idx + 1} of {len(photo_references)}")

#         except requests.exceptions.RequestException as e:
#             print(f"Error downloading photo {idx + 1}: {e}")

#     return downloaded_photos

# # Upload photos to Firebase Storage and return public URLs
# def upload_photos_to_firebase_storage(downloaded_photos, place_id):
#     photo_urls = []
#     for idx, photo_data in enumerate(downloaded_photos):
#         storage_path = f"places/{place_id}_photo_{idx + 1}.jpg"
#         try:
#             bucket = storage.bucket()
#             blob = bucket.blob(storage_path)
#             blob.upload_from_string(photo_data, content_type='image/jpeg')
#             blob.make_public()  # Make the uploaded photo publicly accessible
#             photo_urls.append(blob.public_url)
#             print(f"Uploaded photo {idx + 1} for place_id {place_id} to Firebase Storage")

#         except Exception as e:
#             print(f"Error uploading photo {idx + 1} for place_id {place_id}: {e}")

#     return photo_urls

# # Update Firestore document with image URLs and lat/lng
# def update_firestore_document(db, place_id, photo_urls, latitude, longitude):
#     try:
#         doc_ref = db.collection('exploreData').document(str(place_id))
#         update_data = {}

#         if photo_urls:
#             update_data['g_image_urls'] = firestore.ArrayUnion(photo_urls)  # Append multiple URLs
#         if latitude and longitude:
#             update_data['latitude'] = latitude
#             update_data['longitude'] = longitude

#         doc_ref.update(update_data)
#         print(f"Updated Firestore document for place_id {place_id}")
#     except NotFound:
#         print(f"Firestore document for place_id {place_id} not found")
#     except Exception as e:
#         print(f"Error updating Firestore document for place_id {place_id}: {e}")

# # Main function to process places
# def process_places(db, gmaps, batch_size=100, delay=1, max_api_calls=20000):
#     total_processed = 0
#     api_calls = 0

#     # Fetch all documents from 'exploreData' collection
#     national_parks_ref = db.collection('exploreData')
#     national_parks = national_parks_ref.stream()

#     for idx, park in enumerate(national_parks):
#         if api_calls >= max_api_calls:
#             print(f"Reached API call limit of {max_api_calls}. Stopping further requests.")
#             break

#         park_id = park.id

#         # Fetch subcollections 'TouristAttractions' and 'TouristRestaurants'
#         for subcollection in ['TouristAttractions']:
#             subcollection_ref = national_parks_ref.document(park_id).collection(subcollection)
#             places = subcollection_ref.stream()

#             for place in places:
#                 if api_calls >= max_api_calls:
#                     print(f"Reached API call limit of {max_api_calls}. Stopping further requests.")
#                     break

#                 place_data = place.to_dict()
#                 place_id = place_data.get('placeId')

#                 if place_id:
#                     # Fetch Google Places data
#                     photo_references, latitude, longitude = get_place_details(gmaps, place_id)
#                     api_calls += 1  # Increment API call count after each Google Places API request

#                     if photo_references:
#                         # Download photos (up to 4)
#                         downloaded_photos = download_photos(photo_references)

#                         # Upload photos to Firebase and get URLs
#                         if downloaded_photos:
#                             photo_urls = upload_photos_to_firebase_storage(downloaded_photos, place_id)

#                             # Update Firestore with photo URLs and lat/lng
#                             update_firestore_document(db, place_id, photo_urls, latitude, longitude)

#                 # Add delay between API calls to respect quotas
#                 time.sleep(delay)
#                 total_processed += 1

#                 # If batch size is reached, pause to prevent Firestore quota issues
#                 if total_processed % batch_size == 0:
#                     print(f"Processed {total_processed} places. Pausing to prevent Firestore quota issues.")
#                     time.sleep(30)  # Pause between batches

# if __name__ == "__main__":
#     # Initialize Firebase and Google Maps client
#     db = initialize_firebase()
#     gmaps = initialize_google_maps()

#     # Process places from Firestore with a limit of 20,000 API calls
#     process_places(db, gmaps, batch_size=100, delay=1, max_api_calls=20000)



# import os
# import requests
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# import googlemaps
# from googlemaps.exceptions import ApiError
# from google.api_core.exceptions import NotFound
# import time

# # Replace with your actual API key
# GOOGLE_PLACES_API_KEY = 'AIzaSyBaIgAs4ZNgi5chk9kwh2Az57wyje67KZA'

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'
#     })
#     db = firestore.client()
#     return db

# # Initialize Google Maps Client
# def initialize_google_maps():
#     gmaps = googlemaps.Client(key=GOOGLE_PLACES_API_KEY)
#     return gmaps

# # Get photo references and other details for a place
# def get_place_details(gmaps, place_id):
#     try:
#         # Fetch place details including photos and geometry (latitude and longitude)
#         response = gmaps.place(place_id=place_id, fields=['photos', 'geometry'])
#         result = response.get('result', {})
#         photos = result.get('photos', [])
#         lat_lng = result.get('geometry', {}).get('location', {})

#         # Extract latitude and longitude if available
#         latitude = lat_lng.get('lat')
#         longitude = lat_lng.get('lng')

#         # Get up to 4 photo references
#         photo_references = []
#         for photo in photos[:4]:  # Limit to a maximum of 4 photos
#             photo_reference = photo.get('photo_reference')
#             if photo_reference:
#                 photo_references.append(photo_reference)

#         return photo_references, latitude, longitude

#     except ApiError as e:
#         print(f"Error fetching place details for {place_id}: {e}")
#         return [], None, None


# # Download the photos using the photo_reference
# def download_photos(photo_references):
#     downloaded_photos = []

#     photo_url = "https://maps.googleapis.com/maps/api/place/photo"
#     for idx, photo_reference in enumerate(photo_references):
#         params = {
#             'maxwidth': 800,
#             'photoreference': photo_reference,
#             'key': GOOGLE_PLACES_API_KEY,
#         }
#         try:
#             response = requests.get(photo_url, params=params, stream=True, timeout=10)
#             response.raise_for_status()
#             downloaded_photos.append(response.content)  # Append photo content to the list
#             print(f"Downloaded photo {idx + 1} of {len(photo_references)}")

#         except requests.exceptions.RequestException as e:
#             print(f"Error downloading photo {idx + 1}: {e}")

#     return downloaded_photos


# # Upload photos to Firebase Storage and return public URLs
# def upload_photos_to_firebase_storage(downloaded_photos, place_id):
#     photo_urls = []
#     for idx, photo_data in enumerate(downloaded_photos):
#         storage_path = f"places/{place_id}_photo_{idx + 1}.jpg"
#         try:
#             bucket = storage.bucket()
#             blob = bucket.blob(storage_path)
#             blob.upload_from_string(photo_data, content_type='image/jpeg')
#             blob.make_public()  # Make the uploaded photo publicly accessible
#             photo_urls.append(blob.public_url)
#             print(f"Uploaded photo {idx + 1} for place_id {place_id} to Firebase Storage")

#         except Exception as e:
#             print(f"Error uploading photo {idx + 1} for place_id {place_id}: {e}")

#     return photo_urls

# # Update Firestore document with image URLs and lat/lng
# def update_firestore_document(db, place_id, photo_urls, latitude, longitude):
#     try:
#         doc_ref = db.collection('exploreData').document(str(place_id))
#         update_data = {}

#         if photo_urls:
#             update_data['g_image_urls'] = firestore.ArrayUnion(photo_urls)  # Append multiple URLs
#         if latitude and longitude:
#             update_data['latitude'] = latitude
#             update_data['longitude'] = longitude

#         doc_ref.update(update_data)
#         print(f"Updated Firestore document for place_id {place_id}")
#     except NotFound:
#         print(f"Firestore document for place_id {place_id} not found")
#     except Exception as e:
#         print(f"Error updating Firestore document for place_id {place_id}: {e}")

# # Main function to process places
# def process_places(db, gmaps, batch_size=100, delay=1):
#     total_processed = 0

#     # Fetch all documents from 'exploreData' collection
#     national_parks_ref = db.collection('exploreData')
#     national_parks = national_parks_ref.stream()

#     for idx, park in enumerate(national_parks):
#         park_id = park.id

#         # Fetch subcollections 'TouristAttractions' and 'TouristRestaurants'
#         for subcollection in ['TouristAttractions']:
#             subcollection_ref = national_parks_ref.document(park_id).collection(subcollection)
#             places = subcollection_ref.stream()

#             for place in places:
#                 place_data = place.to_dict()
#                 place_id = place_data.get('placeId')

#                 if place_id:
#                     # Fetch Google Places data
#                     photo_references, latitude, longitude = get_place_details(gmaps, place_id)

#                     if photo_references:
#                         # Download photos (up to 4)
#                         downloaded_photos = download_photos(photo_references)

#                         # Upload photos to Firebase and get URLs
#                         if downloaded_photos:
#                             photo_urls = upload_photos_to_firebase_storage(downloaded_photos, place_id)

#                             # Update Firestore with photo URLs and lat/lng
#                             update_firestore_document(db, place_id, photo_urls, latitude, longitude)

#                 # Add delay between API calls to respect quotas
#                 time.sleep(delay)
#                 total_processed += 1

#                 # If batch size is reached, pause to prevent Firestore quota issues
#                 if total_processed % batch_size == 0:
#                     print(f"Processed {total_processed} places. Pausing to prevent Firestore quota issues.")
#                     time.sleep(30)  # Pause between batches

# if __name__ == "__main__":
#     # Initialize Firebase and Google Maps client
#     db = initialize_firebase()
#     gmaps = initialize_google_maps()

#     # Process places from Firestore
#     process_places(db, gmaps, batch_size=100, delay=1)



# # explore cities
# import os
# import json
# import requests
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# import googlemaps
# from googlemaps.exceptions import ApiError
# from google.api_core.exceptions import NotFound
# import time

# # Replace with your actual API key
# GOOGLE_PLACES_API_KEY = 'AIzaSyBe-suda095R60l0G1NrdYeIVKjUCxwK_8'

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     # Replace with the path to your Firebase service account JSON file
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'
#     })
#     db = firestore.client()
#     return db

# # Initialize Google Maps Client
# def initialize_google_maps():
#     gmaps = googlemaps.Client(key=GOOGLE_PLACES_API_KEY)
#     return gmaps

# # Search for a place and get its place_id
# def get_google_place_id(gmaps, city_name, latitude=None, longitude=None):
#     try:
#         location_bias = f'point:{latitude},{longitude}' if latitude and longitude else None
#         response = gmaps.find_place(
#             input=city_name,
#             input_type='textquery',
#             fields=['place_id'],
#             location_bias=location_bias
#         )

#         candidates = response.get('candidates')
#         if candidates:
#             place_id = candidates[0]['place_id']
#             return place_id
#         else:
#             print(f"No place found for {city_name}")
#             return None
#     except ApiError as e:
#         print(f"Error searching for place {city_name}: {e}")
#         return None

# # Get photo references for a place_id
# def get_photo_reference(gmaps, place_id):
#     try:
#         # The correct field is 'photo', not 'photos'
#         response = gmaps.place(place_id=place_id, fields=['photo'])
#         result = response.get('result')
#         photos = result.get('photos')
#         if photos:
#             photo_reference = photos[0]['photo_reference']
#             return photo_reference
#         else:
#             print(f"No photos available for place_id {place_id}")
#             return None
#     except ApiError as e:
#         print(f"Error getting photo reference for place_id {place_id}: {e}")
#         return None


# # Download the photo using the photo_reference
# def download_photo(photo_reference, filename):
#     photo_url = "https://maps.googleapis.com/maps/api/place/photo"
#     params = {
#         'maxwidth': 800,
#         'photoreference': photo_reference,
#         'key': GOOGLE_PLACES_API_KEY,
#     }
#     try:
#         response = requests.get(photo_url, params=params, stream=True, timeout=10)
#         response.raise_for_status()
#         with open(filename, 'wb') as f:
#             for chunk in response.iter_content(1024):
#                 f.write(chunk)
#         print(f"Photo downloaded: {filename}")
#         return True
#     except requests.exceptions.RequestException as e:
#         print(f"Error downloading photo: {e}")
#         return False

# # Upload the image to Firebase Storage
# def upload_image_to_firebase_storage(local_filename, storage_path):
#     try:
#         bucket = storage.bucket()
#         blob = bucket.blob(storage_path)
#         blob.upload_from_filename(local_filename)
#         blob.make_public()
#         print(f"Uploaded {local_filename} to Firebase Storage at {storage_path}")
#         return blob.public_url
#     except Exception as e:
#         print(f"Error uploading image to Firebase Storage: {e}")
#         return None

# # Update Firestore document with image URL
# def update_firestore_document_with_image_url(db, place_id, image_url,g_place_id):
#     try:
#         doc_ref = db.collection('explore').document(str(place_id))
#         doc_ref.update({'image_url': image_url,'g_place_id': g_place_id,})
#         print(f"Updated Firestore document for place_id {place_id} with image_url")
#     except NotFound:
#         print(f"Firestore document for place_id {place_id} not found")
#     except Exception as e:
#         print(f"Error updating Firestore document for place_id {place_id}: {e}")

# # Process each place
# def process_place(db, gmaps, place_data):
#     place_id = place_data.get('place_id')
#     city_name = place_data.get('city_name')
#     latitude = place_data.get('latitude')
#     longitude = place_data.get('longitude')

#     if not place_id or not city_name:
#         print(f"Missing place_id or city_name for place: {place_data}")
#         return

#     # Get Google Place ID
#     google_place_id = get_google_place_id(gmaps, city_name, latitude, longitude)
#     if not google_place_id:
#         return

#     # Get photo reference & g_place_id
#     g_place_id = google_place_id
#     photo_reference = get_photo_reference(gmaps, google_place_id)
#     if not photo_reference:
#         return

#     # Download the photo
#     local_filename = f"{place_id}.jpg"
#     if not download_photo(photo_reference, local_filename):
#         return

#     # Upload to Firebase Storage
#     storage_path = f"places/{place_id}.jpg"
#     image_url = upload_image_to_firebase_storage(local_filename, storage_path)
#     if not image_url:
#         os.remove(local_filename)
#         return

#     # Update Firestore document
#     update_firestore_document_with_image_url(db, place_id, image_url,g_place_id)

#     # Clean up local file
#     os.remove(local_filename)

# # Main function
# def main():
#     # Initialize Firebase and Google Maps client
#     db = initialize_firebase()
#     gmaps = initialize_google_maps()

#     # Load place data from explore_places_data.json
#     with open('explore_places_data.json', 'r', encoding='utf-8') as f:
#         places = json.load(f)

#     # Process each place
#     for idx, place_data in enumerate(places):
#         print(f"Processing place {idx + 1}/{len(places)}: {place_data.get('city_name')}")
#         process_place(db, gmaps, place_data)
#         # Respect rate limits
#         time.sleep(1)  # Adjust sleep time as needed to stay within quota limits

# if __name__ == '__main__':
#     main()



# import os
# import json
# import requests
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# import googlemaps
# from googlemaps.exceptions import ApiError
# from google.api_core.exceptions import NotFound
# import time

# # Replace with your actual API key
# GOOGLE_PLACES_API_KEY = 'AIzaSyBe-suda095R60l0G1NrdYeIVKjUCxwK_8'

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     # Replace with the path to your Firebase service account JSON file
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'your-firebase-storage-bucket-url'
#     })
#     db = firestore.client()
#     return db

# # Initialize Google Maps Client
# def initialize_google_maps():
#     gmaps = googlemaps.Client(key=GOOGLE_PLACES_API_KEY)
#     return gmaps

# # Search for a place and get its place_id
# def get_google_place_id(gmaps, city_name, latitude=None, longitude=None):
#     try:
#         if latitude and longitude:
#             # Bias the search to the location
#             location_bias = f'point:{latitude},{longitude}'
#         else:
#             location_bias = None

#         response = gmaps.find_place(
#             input=city_name,
#             input_type='textquery',
#             fields=['place_id'],
#             location_bias=location_bias
#         )

#         candidates = response.get('candidates')
#         if candidates:
#             place_id = candidates[0]['place_id']
#             return place_id
#         else:
#             print(f"No place found for {city_name}")
#             return None
#     except ApiError as e:
#         print(f"Error searching for place {city_name}: {e}")
#         return None

# # Get photo references for a place_id
# def get_photo_reference(gmaps, place_id):
#     try:
#         response = gmaps.place(place_id=place_id, fields=['photos'])
#         result = response.get('result')
#         photos = result.get('photos')
#         if photos:
#             photo_reference = photos[0]['photo_reference']
#             return photo_reference
#         else:
#             print(f"No photos available for place_id {place_id}")
#             return None
#     except ApiError as e:
#         print(f"Error getting photo reference for place_id {place_id}: {e}")
#         return None

# # Download the photo using the photo_reference
# def download_photo(photo_reference, filename):
#     photo_url = "https://maps.googleapis.com/maps/api/place/photo"
#     params = {
#         'maxwidth': 800,
#         'photoreference': photo_reference,
#         'key': GOOGLE_PLACES_API_KEY,
#     }
#     try:
#         response = requests.get(photo_url, params=params, stream=True, timeout=10)
#         response.raise_for_status()
#         with open(filename, 'wb') as f:
#             for chunk in response.iter_content(1024):
#                 f.write(chunk)
#         print(f"Photo downloaded: {filename}")
#         return True
#     except requests.exceptions.RequestException as e:
#         print(f"Error downloading photo: {e}")
#         return False

# # Upload the image to Firebase Storage
# def upload_image_to_firebase_storage(local_filename, storage_path):
#     try:
#         bucket = storage.bucket()
#         blob = bucket.blob(storage_path)
#         blob.upload_from_filename(local_filename)
#         # Make the blob publicly viewable
#         blob.make_public()
#         print(f"Uploaded {local_filename} to Firebase Storage at {storage_path}")
#         return blob.public_url
#     except Exception as e:
#         print(f"Error uploading image to Firebase Storage: {e}")
#         return None

# # Update Firestore document with image URL
# def update_firestore_document_with_image_url(db, place_id, image_url):
#     try:
#         doc_ref = db.collection('explore').document(str(place_id))
#         doc_ref.update({'image_url': image_url})
#         print(f"Updated Firestore document for place_id {place_id} with image_url")
#     except NotFound:
#         print(f"Firestore document for place_id {place_id} not found")
#     except Exception as e:
#         print(f"Error updating Firestore document for place_id {place_id}: {e}")

# # Process each place
# def process_place(db, gmaps, place_data):
#     place_id = place_data.get('place_id')
#     city_name = place_data.get('city_name')
#     latitude = place_data.get('latitude')
#     longitude = place_data.get('longitude')

#     if not place_id or not city_name:
#         print(f"Missing place_id or city_name for place: {place_data}")
#         return

#     # Get Google Place ID (if needed)
#     google_place_id = get_google_place_id(gmaps, city_name, latitude, longitude)
#     if not google_place_id:
#         return

#     # Get photo reference
#     photo_reference = get_photo_reference(gmaps, google_place_id)
#     if not photo_reference:
#         return

#     # Download the photo
#     local_filename = f"{place_id}.jpg"
#     if not download_photo(photo_reference, local_filename):
#         return

#     # Upload to Firebase Storage
#     storage_path = f"places/{place_id}.jpg"
#     image_url = upload_image_to_firebase_storage(local_filename, storage_path)
#     if not image_url:
#         # Clean up local file
#         os.remove(local_filename)
#         return

#     # Update Firestore document
#     update_firestore_document_with_image_url(db, place_id, image_url)

#     # Clean up local file
#     os.remove(local_filename)

# # Main function
# def main():
#     # Initialize Firebase and Google Maps client
#     db = initialize_firebase()
#     gmaps = initialize_google_maps()

#     # Load place data
#     with open('explore_places_data.json', 'r', encoding='utf-8') as f:
#         places = json.load(f)

#     # Process each place
#     for idx, place_data in enumerate(places):
#         print(f"Processing place {idx + 1}/{len(places)}: {place_data.get('city_name')}")
#         process_place(db, gmaps, place_data)
#         # Respect rate limits
#         time.sleep(1)  # Adjust sleep time as needed to stay within quota limits

# if __name__ == '__main__':
#     main()
