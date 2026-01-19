import os
import json
import requests
import firebase_admin
from firebase_admin import credentials, firestore, storage

def download_and_upload_image(original_url, place_id, index):
    """
    Downloads an image from the original URL and uploads it to Firebase Storage.
    The new image is stored at the path:
        places/{place_id}_{index+1}.jpg
    Returns the public URL of the uploaded image.
    """
    try:
        # Download the image content.
        response = requests.get(original_url, stream=True)
        if response.status_code != 200:
            print(f"Error downloading image from {original_url}: status {response.status_code}")
            return None
        image_content = response.content
    except Exception as e:
        print(f"Exception while downloading image from {original_url}: {e}")
        return None

    # Build the filename for the image in storage.
    filename = f"places/{place_id}_{index+1}.jpg"
    try:
        bucket = storage.bucket()
        blob = bucket.blob(filename)
        blob.upload_from_string(image_content, content_type="image/jpeg")
        # Optionally make the file public or generate a download URL.
        public_url = blob.public_url
        return public_url
    except Exception as e:
        print(f"Error uploading image {filename} to storage: {e}")
        return None

def transform_place(place):
    """
    Transforms a single place record before uploading to Firestore.
    
    1. Removes the 'place_name' field so that only the 'name' field remains.
    2. Downloads each image from the original 'g_image_urls' list,
       uploads it to Firebase Storage, and replaces the list with the new URLs.
    3. Leaves other fields (including openingPeriods) unchanged.
    """
    # Remove the redundant "place_name" field.
    if "place_name" in place:
        del place["place_name"]

    # Process image URLs.
    if "g_image_urls" in place and isinstance(place["g_image_urls"], list) and place["g_image_urls"]:
        place_id = place.get("placeId")
        if not place_id:
            print("Missing placeId; skipping image processing for this record.")
            return place

        new_image_urls = []
        for idx, original_url in enumerate(place["g_image_urls"]):
            print(f"Processing image {idx+1} for place {place_id}")
            new_url = download_and_upload_image(original_url, place_id, idx)
            if new_url:
                new_image_urls.append(new_url)
            else:
                print(f"Skipping image {idx+1} for place {place_id} due to an error.")
        place["g_image_urls"] = new_image_urls

    return place

def upload_places_to_firestore():
    """
    Reads the JSON file with place data, transforms each record (including downloading
    and uploading images), and uploads each record to Firestore under:
        explore/{city_id}/atlas/{placeId}
    """
    # --- UPDATE THESE PARAMETERS AS NEEDED --- #
    # Path to the JSON file containing the place data.
    json_file = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\atlasobscura_india_cities_places\pune_places.json"  
    # City document ID in the 'explore' collection.
    city_id = "57"  
    # Path to your Firebase service account key file.
    service_account_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
    # Firebase Storage bucket name.
    storage_bucket = "mycasavsc.appspot.com"  
    # ----------------------------------------- #

    # Initialize Firebase Admin SDK.
    cred = credentials.Certificate(service_account_path)
    firebase_admin.initialize_app(cred, {'storageBucket': storage_bucket})
    db = firestore.client()

    # Reference to the atlas subcollection for this city.
    collection_ref = db.collection("explore").document(city_id).collection("atlas")

    # Read the JSON file.
    with open(json_file, "r", encoding="utf-8") as f:
        places = json.load(f)

    # Transform and upload each place record.
    for place in places:
        transformed = transform_place(place)
        doc_id = transformed.get("placeId")
        if not doc_id:
            print("Skipping a record due to missing placeId.")
            continue

        collection_ref.document(doc_id).set(transformed)
        print(f"Uploaded place with ID: {doc_id}")

if __name__ == "__main__":
    upload_places_to_firestore()




# Script store_atlas_obscura_data_to_my_firebase_project
# import json
# import argparse
# import firebase_admin
# from firebase_admin import credentials, firestore

# def transform_place(place, storage_base_url):
#     """
#     Transform the place record before uploading to Firestore.
    
#     - Remove the 'place_name' field so that Firestore uses the 'name' field.
#     - Rebuild the g_image_urls list:
#       If there are 4 or more images, apply the fixed permutation [2, 1, 4, 3] (as shown in the example).
#       Otherwise, simply number sequentially starting at 1.
#     - (Other fields such as openingPeriods remain as in the original JSON,
#       provided they are already structured as an array of maps.)
#     """
#     # Remove the "place_name" field if present.
#     if "place_name" in place:
#         del place["place_name"]
    
#     # Transform g_image_urls: build new URLs using the placeId and a fixed image index order.
#     if "g_image_urls" in place and isinstance(place["g_image_urls"], list) and place["g_image_urls"]:
#         place_id = place.get("placeId")
#         if not place_id:
#             # If there's no placeId, skip image URL transformation.
#             return place

#         # Use a fixed permutation if there are at least 4 images.
#         desired_order = [2, 1, 4, 3]
#         if len(place["g_image_urls"]) >= 4:
#             new_urls = [f"{storage_base_url}/places/{place_id}_{num}.jpg" for num in desired_order]
#         else:
#             # If there are fewer than 4 images, use sequential numbering.
#             new_urls = [f"{storage_base_url}/places/{place_id}_{i+1}.jpg" for i in range(len(place["g_image_urls"]))]
#         place["g_image_urls"] = new_urls

#     # (Optionally, you can perform additional type conversion or data cleaning here.)
#     return place

# def upload_places_to_firestore(json_file, city, storage_base_url, service_account_path):
#     """
#     Reads the JSON file of place records, applies transformation, and uploads
#     each record to Firestore under the structure:
#       explore/{city}/atlas/{placeId}
#     """
#     # Initialize Firebase Admin SDK with the service account key.
#     cred = credentials.Certificate(service_account_path)
#     firebase_admin.initialize_app(cred)
#     db = firestore.client()

#     # Create a reference to the subcollection "atlas" under document for the city.
#     # This assumes a Firestore structure like: explore -> (city document) -> atlas (subcollection)
#     collection_ref = db.collection("explore").document(city).collection("atlas")

#     # Read the JSON file.
#     with open(json_file, "r", encoding="utf-8") as f:
#         places = json.load(f)

#     # Process and upload each place.
#     for place in places:
#         transformed_place = transform_place(place, storage_base_url)
#         doc_id = transformed_place.get("placeId")
#         if not doc_id:
#             print("Skipping place record because it has no 'placeId'.")
#             continue

#         # Upload the transformed record (document id set to placeId)
#         collection_ref.document(doc_id).set(transformed_place)
#         print(f"Uploaded place with ID: {doc_id}")

# def main():
#     parser = argparse.ArgumentParser(description="Upload places JSON to Firestore under the 'atlas' subcollection.")
#     parser.add_argument("json_file", help="Path to the JSON file containing places data.")
#     parser.add_argument("--city", required=True, help="City name (used as the document ID under 'explore').")
#     parser.add_argument("--storage_base_url", default="https://storage.googleapis.com/mycasavsc.appspot.com",
#                         help="Base URL for Firebase Storage where images are hosted.")
#     parser.add_argument("--service_account", required=True, help="Path to your Firebase service account JSON key file.")
    
#     args = parser.parse_args()
    
#     upload_places_to_firestore(args.json_file, args.city, args.storage_base_url, args.service_account)

# if __name__ == "__main__":
#     main()
