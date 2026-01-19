#!/usr/bin/env python
"""
Unsplash City Image Fetcher
---------------------------
This script fetches 4 images from Unsplash for each city based on a search query,
and then updates the Firestore document (in the "explore" collection, using the city ID as the document ID)
by appending the new image URLs to the existing "image_url" field.
It skips any document that already has more than 3 image URLs.
"""

import os
import firebase_admin
from firebase_admin import credentials, firestore
import requests

# Unsplash API credentials
UNSPLASH_ACCESS_KEY = "iLLthDJadkWlTiqrO6dypqNh6wKU_rZ3WJUpRw1ZuWo"
# (The Secret Key and Application ID are provided but not required for basic API requests.)

def initialize_firebase():
    """
    Initialize Firebase Admin SDK using a service account JSON file.
    Returns:
        db: Firestore client.
    """
    print("Initializing Firebase Admin SDK...")
    # Replace with the path to your Firebase service account JSON file
    firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
    cred = credentials.Certificate(firebase_credentials_path)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    return db

def fetch_images_from_unsplash(city_query, per_page=4):
    """
    Fetch images from Unsplash based on the search query.
    
    Args:
        city_query (str): The search query (e.g., city name and country name).
        per_page (int): Number of images to fetch.
        
    Returns:
        List of image URLs (using the 'regular' size from Unsplash).
    """
    url = "https://api.unsplash.com/search/photos"
    headers = {"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"}
    params = {"query": city_query, "per_page": per_page}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        image_urls = [photo["urls"]["regular"] for photo in results]
        return image_urls
    except Exception as e:
        print(f"Error fetching images for query '{city_query}': {e}")
        return []

def main():
    print("Starting Unsplash City Image Fetcher script...")
    db = initialize_firebase()

    # List of city IDs (Firestore document IDs in the "explore" collection)
    city_ids = [
         "10006", "10007", 
    ]
    
    for city_id in city_ids:
        doc_ref = db.collection('explore').document(city_id)
        doc = doc_ref.get()
        if doc.exists:
            doc_data = doc.to_dict()
            print(f"\nProcessing city document with ID: {city_id}")
            existing_image_url = doc_data.get('image_url')
            
            # Constraint: Skip if image_url exists as a list with more than 3 URLs.
            if existing_image_url and isinstance(existing_image_url, list) and len(existing_image_url) > 3:
                print(f"Skipping city {city_id} as it already has more than 3 image URLs.")
                continue
            
            # Build a more specific search query using city and country names if available.
            city_name = doc_data.get('city_name', '')
            country_name = doc_data.get('country_name', '')
            if city_name and country_name:
                city_query = f"{city_name} {country_name}"
            elif city_name:
                city_query = city_name
            else:
                city_query = city_id
            
            print(f"Fetching images for query: {city_query}")
            new_image_urls = fetch_images_from_unsplash(city_query, per_page=4)
            
            # Merge new image URLs with any existing ones (if present).
            image_urls = new_image_urls[:]  # copy new images list
            if existing_image_url:
                if isinstance(existing_image_url, list):
                    image_urls.extend(existing_image_url)
                elif isinstance(existing_image_url, str):
                    image_urls.append(existing_image_url)
            
            if image_urls:
                try:
                    # Update only the "image_url" field, leaving other fields unchanged.
                    doc_ref.update({'image_url': image_urls})
                    print(f"Updated city {city_id} with image URLs:\n{image_urls}")
                except Exception as e:
                    print(f"Error updating city {city_id}: {e}")
            else:
                print(f"No images fetched for city {city_id}.")
        else:
            print(f"No document found for city {city_id}.")

if __name__ == '__main__':
    main()



# #!/usr/bin/env python
# """
# Pexels City Image Fetcher
# --------------------------
# This script fetches 4 images from Pexels for each city based on a search query,
# and then updates the Firestore document (in the "explore" collection, using the city ID as the document ID)
# by appending the new image URLs to the existing "image_url" field.
# It skips any document that already has more than 3 image URLs.
# """

# import os
# import firebase_admin
# from firebase_admin import credentials, firestore
# import requests

# # Replace with your actual Pexels API key
# PEXELS_API_KEY = "spNoycDS7KttYK8Yr9kk2E9CZQY8XtI1NsXbKlU4oyg6X9ga9ITRFEOR"

# def initialize_firebase():
#     """
#     Initialize Firebase Admin SDK using a service account JSON file.
#     Returns:
#         db: Firestore client
#     """
#     print("Initializing Firebase Admin SDK...")
#     # Replace with the path to your Firebase service account JSON file
#     firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         # You can add additional configuration here if needed
#     })
#     db = firestore.client()
#     return db

# def fetch_images_from_pexels(city_query, per_page=4):
#     """
#     Fetch images from Pexels based on the search query.
    
#     Args:
#         city_query (str): The search query (e.g., city name and country name).
#         per_page (int): Number of images to fetch.
        
#     Returns:
#         List of image URLs (using the 'original' size from Pexels).
#     """
#     url = "https://api.pexels.com/v1/search"
#     headers = {"Authorization": PEXELS_API_KEY}
#     params = {"query": city_query, "per_page": per_page}
#     try:
#         response = requests.get(url, headers=headers, params=params)
#         response.raise_for_status()  # Raises an error for non-200 responses
#         data = response.json()
#         photos = data.get("photos", [])
#         image_urls = [photo["src"]["original"] for photo in photos]
#         return image_urls
#     except Exception as e:
#         print(f"Error fetching images for query '{city_query}': {e}")
#         return []

# def main():
#     print("Starting Pexels City Image Fetcher script...")
#     db = initialize_firebase()

#     # List of city IDs (which are also the Firestore document IDs in the "explore" collection)
#     city_ids = [
#         "10002", "10005",
#     ]
    
#     for city_id in city_ids:
#         doc_ref = db.collection('explore').document(city_id)
#         doc = doc_ref.get()
#         if doc.exists:
#             doc_data = doc.to_dict()
#             print(f"\nProcessing city document with ID: {city_id}")
#             existing_image_url = doc_data.get('image_url')
            
#             # Constraint: Skip if the existing image_url is a list with more than 3 URLs.
#             if existing_image_url and isinstance(existing_image_url, list) and len(existing_image_url) > 3:
#                 print(f"Skipping city {city_id} as it already has more than 3 image URLs.")
#                 continue

#             # Enhance query by combining city_name and country_name, if available.
#             city_name = doc_data.get('city_name', '')
#             country_name = doc_data.get('country_name', '')
#             if city_name and country_name:
#                 city_query = f"{city_name} {country_name}"
#             elif city_name:
#                 city_query = city_name
#             else:
#                 city_query = city_id

#             print(f"Fetching images for query: {city_query}")
#             new_image_urls = fetch_images_from_pexels(city_query, per_page=4)
            
#             # Merge new image URLs with any existing ones.
#             image_urls = new_image_urls[:]  # make a copy of the new images list
#             if existing_image_url:
#                 if isinstance(existing_image_url, list):
#                     image_urls.extend(existing_image_url)
#                 elif isinstance(existing_image_url, str):
#                     image_urls.append(existing_image_url)
            
#             if image_urls:
#                 try:
#                     doc_ref.update({'image_url': image_urls})
#                     print(f"Updated city {city_id} with image URLs:\n{image_urls}")
#                 except Exception as e:
#                     print(f"Error updating city {city_id}: {e}")
#             else:
#                 print(f"No images fetched for city {city_id}.")
#         else:
#             print(f"No document found for city {city_id}.")

# if __name__ == '__main__':
#     main()









