import os
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase Admin SDK
firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
cred = credentials.Certificate(firebase_credentials_path)
firebase_admin.initialize_app(cred)

# Initialize Firestore client
db = firestore.client()

# Batch size for fetching data
BATCH_SIZE = 100

def copy_place_data_to_placeDetail():
    # Reference to the 'exploreData' collection
    explore_data_ref = db.collection('exploreData')

    # Fetch all national parks
    national_parks = explore_data_ref.stream()

    for park in national_parks:
        park_id = park.id
        print(f"Processing exploreData: {park_id}")

        # Fetch 'TouristAttractions' and 'TouristRestaurants' subcollections in batches
        for subcollection in ['TouristAttractions', 'TouristRestaurants']:
            process_subcollection_in_batches(explore_data_ref, park_id, subcollection)

def process_subcollection_in_batches(explore_data_ref, park_id, subcollection_name):
    subcollection_ref = explore_data_ref.document(park_id).collection(subcollection_name)
    last_doc = None
    while True:
        query = subcollection_ref.limit(BATCH_SIZE)

        # If there's a last document, start the next batch from that document
        if last_doc:
            query = query.start_after(last_doc)

        places = list(query.stream())

        # If there are no more documents, exit the loop
        if not places:
            break

        for place in places:
            place_data = place.to_dict()

            # Extract the necessary fields for 'placeDetail' collection
            place_detail_data = {
                'id': place_data.get('id'),
                'name': place_data.get('name'),
                'description': place_data.get('description'),
                'categories': place_data.get('categories', []),
                'address': place_data.get('address'),
                'rating': place_data.get('rating'),
                'numRatings': place_data.get('numRatings'),
                'website': place_data.get('website'),
                'internationalPhoneNumber': place_data.get('internationalPhoneNumber'),
                'priceLevel': place_data.get('priceLevel'),
                'imageKeys': place_data.get('imageKeys', []),
                'placeId': place_data.get('placeId'),
                'permanentlyClosed': place_data.get('permanentlyClosed'),
                'ratingDistribution': place_data.get('ratingDistribution', {}),
                'utcOffset': place_data.get('utcOffset'),
                'openingPeriods': place_data.get('openingPeriods', [])
            }

            # Reference to the 'placeDetail' collection with document ID as the 'id' field
            place_detail_ref = db.collection('specificPlaceDetail').document(str(place_data.get('id')))
            
            # Save the data to 'placeDetail' collection
            place_detail_ref.set(place_detail_data)
            print(f"Saved place detail for place ID: {place_data.get('id')}")

        # Set the last document for the next batch
        last_doc = places[-1] if places else None

if __name__ == '__main__':
    copy_place_data_to_placeDetail()

# import os
# import firebase_admin
# from firebase_admin import credentials, firestore
# import time

# # Initialize Firebase Admin SDK
# firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
# cred = credentials.Certificate(firebase_credentials_path)
# firebase_admin.initialize_app(cred)

# # Initialize Firestore client
# db = firestore.client()

# # Retry logic for Firestore operations
# MAX_RETRIES = 5
# RETRY_DELAY = 5  # seconds

# def fetch_data_with_retry(ref):
#     """Fetch Firestore documents with retry logic."""
#     for attempt in range(MAX_RETRIES):
#         try:
#             return ref.stream()  # Attempt to fetch Firestore data
#         except Exception as e:
#             print(f"Error fetching data: {e}. Retrying {attempt + 1}/{MAX_RETRIES}...")
#             if attempt < MAX_RETRIES - 1:
#                 time.sleep(RETRY_DELAY)
#             else:
#                 raise e

# def copy_place_data_to_placeDetail():
#     # Reference to the 'exploreData' collection
#     national_parks_ref = db.collection('exploreData')

#     # Fetch all national parks with retry logic
#     national_parks = fetch_data_with_retry(national_parks_ref)

#     for park in national_parks:
#         park_id = park.id
#         print(f"Processing exploreData: {park_id}")

#         # Fetch 'TouristAttractions' and 'TouristRestaurants' subcollections
#         for subcollection in ['TouristAttractions', 'TouristRestaurants']:
#             subcollection_ref = national_parks_ref.document(park_id).collection(subcollection)
#             places = fetch_data_with_retry(subcollection_ref)

#             for place in places:
#                 place_data = place.to_dict()

#                 # Extract the necessary fields for 'placeDetail' collection
#                 place_detail_data = {
#                     'id': place_data.get('id'),
#                     'name': place_data.get('name'),
#                     'description': place_data.get('description'),
#                     'categories': place_data.get('categories', []),
#                     'address': place_data.get('address'),
#                     'rating': place_data.get('rating'),
#                     'numRatings': place_data.get('numRatings'),
#                     'website': place_data.get('website'),
#                     'internationalPhoneNumber': place_data.get('internationalPhoneNumber'),
#                     'priceLevel': place_data.get('priceLevel'),
#                     'imageKeys': place_data.get('imageKeys', []),
#                     'placeId': place_data.get('placeId'),
#                     'permanentlyClosed': place_data.get('permanentlyClosed'),
#                     'ratingDistribution': place_data.get('ratingDistribution', {}),
#                     'utcOffset': place_data.get('utcOffset'),
#                     'openingPeriods': place_data.get('openingPeriods', [])
#                 }

#                 # Reference to the 'placeDetail' collection with document ID as the 'id' field
#                 place_detail_ref = db.collection('specificPlaceDetail').document(str(place_data.get('id')))
                
#                 # Save the data to 'placeDetail' collection
#                 place_detail_ref.set(place_detail_data)
#                 print(f"Saved place detail for place ID: {place_data.get('id')}")

# if __name__ == '__main__':
#     copy_place_data_to_placeDetail()


# import os
# import firebase_admin
# from firebase_admin import credentials, firestore

# # Initialize Firebase Admin SDK
# firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
# cred = credentials.Certificate(firebase_credentials_path)
# firebase_admin.initialize_app(cred)

# # Initialize Firestore client
# db = firestore.client()

# def copy_place_data_to_placeDetail():
#     # Reference to the 'nationalParks' collection
#     national_parks_ref = db.collection('exploreData')

#     # Fetch all national parks
#     national_parks = national_parks_ref.stream()

#     for park in national_parks:
#         park_id = park.id
#         print(f"Processing exploreData: {park_id}")

#         # Fetch 'TouristAttractions' and 'TouristRestaurants' subcollections
#         for subcollection in ['TouristAttractions', 'TouristRestaurants']:
#             subcollection_ref = national_parks_ref.document(park_id).collection(subcollection)
#             places = subcollection_ref.stream()

#             for place in places:
#                 place_data = place.to_dict()
                
#                 # Extract the necessary fields for 'placeDetail' collection
#                 place_detail_data = {
#                     'id': place_data.get('id'),
#                     'name': place_data.get('name'),
#                     'description': place_data.get('description'),
#                     'categories': place_data.get('categories', []),
#                     'address': place_data.get('address'),
#                     'rating': place_data.get('rating'),
#                     'numRatings': place_data.get('numRatings'),
#                     'website': place_data.get('website'),
#                     'internationalPhoneNumber': place_data.get('internationalPhoneNumber'),
#                     'priceLevel': place_data.get('priceLevel'),
#                     'imageKeys': place_data.get('imageKeys', []),
#                     'placeId': place_data.get('placeId'),
#                     'permanentlyClosed': place_data.get('permanentlyClosed'),
#                     'ratingDistribution': place_data.get('ratingDistribution', {}),
#                     'utcOffset': place_data.get('utcOffset'),
#                     'openingPeriods': place_data.get('openingPeriods', [])
#                 }

#                 # Reference to the 'placeDetail' collection with document ID as the 'id' field
#                 place_detail_ref = db.collection('specificPlaceDetail').document(str(place_data.get('id')))
                
#                 # Save the data to 'placeDetail' collection
#                 place_detail_ref.set(place_detail_data)
#                 print(f"Saved place detail for place ID: {place_data.get('id')}")

# if __name__ == '__main__':
#     copy_place_data_to_placeDetail()
