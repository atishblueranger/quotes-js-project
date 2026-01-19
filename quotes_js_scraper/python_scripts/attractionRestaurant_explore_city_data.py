"""
Script to collect pending places from TouristAttractions subcollection in Firestore and save to a JSON file.
Pending places are those that are missing required fields (g_image_urls, latitude, longitude).
The output JSON file will contain the city_id, placeId and index of the pending places.
"""


import os
import firebase_admin
from firebase_admin import credentials, firestore
import json

# Initialize Firebase Admin SDK
def initialize_firebase():
    firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
    cred = credentials.Certificate(firebase_credentials_path)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    return db

# Function to collect pending place IDs from Firestore
def collect_pending_places(db):
    pending_tourist_attractions = []
    
    try:
        # Reference to the main collection 'exploreData'
        cities_ref = db.collection('exploreData')
        cities = cities_ref.stream()  # Fetch all city documents

        pending_index = 0

        for city in cities:
            city_id = city.id
            print(f"Processing city: {city_id}")

            # Process 'TouristAttractions' subcollection
            attractions_ref = cities_ref.document(city_id).collection('TouristAttractions')
            attractions = attractions_ref.stream()

            for place in attractions:
                place_data = place.to_dict()
                place_id = place.id  # Assuming the document ID is the placeId

                # Check if required fields are missing
                required_fields = ['g_image_urls', 'latitude', 'longitude']
                missing_fields = [field for field in required_fields if field not in place_data]

                if missing_fields:
                    pending_tourist_attractions.append({
                        'index': pending_index,
                        'city_id': city_id,
                        'placeId': place_id
                    })
                    pending_index += 1

        print(f"Collected {len(pending_tourist_attractions)} pending places from TouristRestaurants.")

    except Exception as e:
        print(f"Error collecting pending place IDs: {e}")

    return pending_tourist_attractions

# Function to save the collected data into a JSON file
def save_to_json(collected_data, output_filename):
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(collected_data, f, ensure_ascii=False, indent=4)
        print(f"Data saved to {output_filename}")
    except Exception as e:
        print(f"Error saving data to JSON file: {e}")

if __name__ == "__main__":
    # Initialize Firebase
    db = initialize_firebase()

    # Collect pending place IDs from TouristAttractions
    pending_tourist_attractions = collect_pending_places(db)

    # Save pending places data to a JSON file
    save_to_json(pending_tourist_attractions, output_filename='pending_tourist_attraction_places2.json')






# # Updated Script to Check for Duplicates

# import json

# # Function to check for duplicate place IDs in a given list of places
# def check_duplicates(data):
#     seen = set()
#     duplicates = []
#     unique_data = []

#     for place in data:
#         place_id = place.get('placeId')
#         if place_id in seen:
#             duplicates.append(place)
#         else:
#             seen.add(place_id)
#             unique_data.append(place)

#     return unique_data, duplicates

# # Function to load JSON data from a file
# def load_json_file(file_path):
#     try:
#         with open(file_path, 'r', encoding='utf-8') as f:
#             data = json.load(f)
#         return data
#     except Exception as e:
#         print(f"Error loading JSON file {file_path}: {e}")
#         return []

# # Function to save JSON data to a file
# def save_to_json(data, output_filename):
#     try:
#         with open(output_filename, 'w', encoding='utf-8') as f:
#             json.dump(data, f, ensure_ascii=False, indent=4)
#         print(f"Data saved to {output_filename}")
#     except Exception as e:
#         print(f"Error saving data to JSON file: {e}")

# if __name__ == "__main__":
#     # Paths to the existing JSON files
#     tourist_attractions_file = 'tourist_attractions_places.json'
#     tourist_restaurants_file = 'tourist_restaurants_places.json'

#     # Load the existing JSON data
#     tourist_attractions_data = load_json_file(tourist_attractions_file)
#     tourist_restaurants_data = load_json_file(tourist_restaurants_file)

#     # Check for duplicates in TouristAttractions
#     unique_tourist_attractions_data, tourist_attractions_duplicates = check_duplicates(tourist_attractions_data)
#     print(f"Found {len(tourist_attractions_duplicates)} duplicate places in TouristAttractions.")

#     # Check for duplicates in TouristRestaurants
#     unique_tourist_restaurants_data, tourist_restaurants_duplicates = check_duplicates(tourist_restaurants_data)
#     print(f"Found {len(tourist_restaurants_duplicates)} duplicate places in TouristRestaurants.")

#     # Save the unique data and duplicates for both TouristAttractions and TouristRestaurants
#     save_to_json(unique_tourist_attractions_data, output_filename='unique_tourist_attractions_places.json')
#     save_to_json(tourist_attractions_duplicates, output_filename='duplicate_tourist_attractions_places.json')

#     save_to_json(unique_tourist_restaurants_data, output_filename='unique_tourist_restaurants_places.json')
#     save_to_json(tourist_restaurants_duplicates, output_filename='duplicate_tourist_restaurants_places.json')





# import os
# import firebase_admin
# from firebase_admin import credentials, firestore
# import json

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     db = firestore.client()
#     return db

# # Function to collect place IDs from Firestore
# def collect_place_ids(db):
#     tourist_attractions_data = []
#     tourist_restaurants_data = []
    
#     try:
#         # Reference to the main collection 'exploreData'
#         cities_ref = db.collection('exploreData')
#         cities = cities_ref.stream()  # Fetch all city documents

#         attractions_index = 0
#         restaurants_index = 0

#         for city in cities:
#             city_id = city.id
#             print(f"Processing city: {city_id}")

#             # Process 'TouristAttractions' subcollection
#             attractions_ref = cities_ref.document(city_id).collection('TouristAttractions')
#             attractions = attractions_ref.stream()

#             for place in attractions:
#                 place_data = place.to_dict()
#                 place_id = place_data.get('placeId', None)
#                 if place_id:
#                     tourist_attractions_data.append({
#                         'index': attractions_index,
#                         'city_id': city_id,
#                         'placeId': place_id
#                     })
#                     attractions_index += 1

#             # Process 'TouristRestaurants' subcollection
#             restaurants_ref = cities_ref.document(city_id).collection('TouristRestaurants')
#             restaurants = restaurants_ref.stream()

#             for place in restaurants:
#                 place_data = place.to_dict()
#                 place_id = place_data.get('placeId', None)
#                 if place_id:
#                     tourist_restaurants_data.append({
#                         'index': restaurants_index,
#                         'city_id': city_id,
#                         'placeId': place_id
#                     })
#                     restaurants_index += 1

#         print(f"Collected {len(tourist_attractions_data)} places from TouristAttractions.")
#         print(f"Collected {len(tourist_restaurants_data)} places from TouristRestaurants.")
    
#     except Exception as e:
#         print(f"Error collecting place IDs: {e}")

#     return tourist_attractions_data, tourist_restaurants_data

# # Function to save the collected data into a JSON file
# def save_to_json(collected_data, output_filename):
#     try:
#         with open(output_filename, 'w', encoding='utf-8') as f:
#             json.dump(collected_data, f, ensure_ascii=False, indent=4)
#         print(f"Data saved to {output_filename}")
#     except Exception as e:
#         print(f"Error saving data to JSON file: {e}")

# if __name__ == "__main__":
#     # Initialize Firebase
#     db = initialize_firebase()

#     # Collect place IDs for both TouristAttractions and TouristRestaurants
#     tourist_attractions_data, tourist_restaurants_data = collect_place_ids(db)

#     # Save TouristAttractions data to a separate JSON file
#     save_to_json(tourist_attractions_data, output_filename='tourist_attractions_places.json')

#     # Save TouristRestaurants data to a separate JSON file
#     save_to_json(tourist_restaurants_data, output_filename='tourist_restaurants_places.json')
