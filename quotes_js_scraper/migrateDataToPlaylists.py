

import os 
import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.retry import Retry
from google.api_core import exceptions

# Initialize the app with a service account, granting admin privileges
firebase_credentials_path = os.path.join(
    os.getcwd(),
    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
)
cred = credentials.Certificate(firebase_credentials_path)
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)

# Get a reference to the Firestore client
db = firestore.client()

def migrate_data_for_city(city_id):
    # Reference to the old collection
    old_collection_ref = db.collection('searchedCategories2')

    # Reference to the new collection
    new_collection_ref = db.collection('playlistsNew')

    # Determine the starting value for playlist_counter
    existing_playlists = new_collection_ref.stream()
    max_list_id = 0
    for doc in existing_playlists:
        try:
            list_id = int(doc.id)
            if list_id > max_list_id:
                max_list_id = list_id
        except ValueError:
            continue  # Skip non-integer document IDs

    # Initialize the playlist counter
    playlist_counter = max_list_id + 1

    # Fetch the specific city document
    city_doc_ref = old_collection_ref.document(city_id)
    city_doc = city_doc_ref.get()

    if not city_doc.exists:
        print(f"City with ID {city_id} does not exist in 'searchedCategories2' collection.")
        return

    city_data = city_doc.to_dict()
    city_name = city_data.get('city_name', 'Unknown City')

    print(f"Processing city: {city_name} (ID: {city_id})")

    # Get all playlists (subcollections) under the city
    try:
        playlists = city_doc_ref.collections()
    except Exception as e:
        print(f"Error fetching playlists for city {city_name}: {e}")
        return

    for playlist in playlists:
        playlist_title = playlist.id  # The subcollection name is the title
        print(f'Migrating playlist: {playlist_title} in {city_name}')

        # Assign a sequential list_id
        list_id = str(playlist_counter)

        # Prepare the new playlist document
        new_playlist_data = {
            'title': playlist_title,
            'city': city_name,
            'city_id': city_id,
            'category': '',     # To be set manually later
            'imageUrl': '',     # To be set manually later
            'list_id': list_id,
        }

        # Create the new playlist document
        new_playlist_ref = new_collection_ref.document(list_id)
        new_playlist_ref.set(new_playlist_data)

        # Migrate places under the playlist
        try:
            places = playlist.stream()
        except Exception as e:
            print(f"Error fetching places for playlist {playlist_title}: {e}")
            continue

        for place_doc in places:
            place_data = place_doc.to_dict()
            place_id = place_doc.id

            # Write place data to the new 'places' subcollection
            new_place_ref = new_playlist_ref.collection('places').document(place_id)
            new_place_ref.set(place_data)

        print(f'Playlist "{playlist_title}" migrated successfully.')

        # Increment the playlist counter
        playlist_counter += 1

if __name__ == '__main__':
    # Manual list of cities (adjust or extend with all 100 cities as needed)
    cities_manual = [
        {"name": "Srinagar", "id": "256"},
        {"name": "Jaipur", "id": "24"},
        {"name": "Varanasi", "id": "122"},
        {"name": "lakshadweep", "id": "88144"},
        {"name": "Kodagu (Coorg)", "id": "87325"},
        {"name": "Havelock Island", "id": "1483"},
        {"name": "Ooty", "id": "480"},
        {"name": "Rishikesh", "id": "194"},
        {"name": "kodaikanal", "id": "842"},
        {"name": "Mcleodganj", "id": "146282"},
        {"name": "Nanital", "id": "895"},
        {"name": "Shimla", "id": "428"},
        {"name": "New Delhi", "id": "13"},
        {"name": "Kolkata", "id": "69"},
        {"name": "Mussoorie", "id": "687"},
        {"name": "Pondicherry", "id": "334"},
        {"name": "Mumbai", "id": "25"},
        {"name": "Lonavala", "id": "701"},
        {"name": "Gokarna", "id": "2371"},
        {"name": "Bir Billing (Bir)", "id": "1622"},
        {"name": "Varkala", "id": "662"},
        {"name": "Dalhousie", "id": "1784"},
        {"name": "Mount Abu", "id": "877"},
        {"name": "Wayanad", "id": "87128"},
        {"name": "Pachmarhi", "id": "1215"},
        {"name": "Bangalore (Bengaluru)", "id": "35"},
        {"name": "Jodhpur", "id": "143"},
        {"name": "Jaisalmer", "id": "183"},
        {"name": "Ujjain", "id": "1025"},
        {"name": "Nubra Valley", "id": "88231"},
        {"name": "Hyderabad", "id": "78"},
        {"name": "Khajuraho", "id": "850"},
        {"name": "Chennai (Madras)", "id": "40"},
        {"name": "katra", "id": "2976"},
        {"name": "Haridwar", "id": "783"},
        {"name": "Kanyakumari", "id": "926"},
        {"name": "Hampi", "id": "696"},
        {"name": "Kochi", "id": "157"},
        {"name": "Tirupati", "id": "914"},
        {"name": "Alibaug", "id": "1480"},
        {"name": "Ahmedabad", "id": "161"},
        {"name": "Kanha National Park", "id": "7807"},
        {"name": "Kasol (Kullu)", "id": "86862"},
        {"name": "Mysore", "id": "280"},
        {"name": "Almora", "id": "1765"},
        {"name": "Shirdi", "id": "1316"},
        {"name": "Madurai", "id": "615"},
        {"name": "Bodh Gaya", "id": "1365"},
        {"name": "Mahabaleshwar", "id": "978"},
        {"name": "Jim Corbett NationalPark", "id": "1635"},
        {"name": "Nashik", "id": "449"},
        {"name": "Chandigarh", "id": "403"},
        {"name": "Mathura", "id": "1002"},
        {"name": "Shimoga (Shivamogga)", "id": "1942"},
        {"name": "Rameshwaram", "id": "1325"},
        {"name": "Visakhapatnam", "id": "518"},
        {"name": "Pune", "id": "57"},
        {"name": "Vrindavan", "id": "1874"},
        {"name": "Ranthambore National Park", "id": "3825"},
        {"name": "Coimbatore", "id": "371"},
        {"name": "Lucknow", "id": "375"},
        {"name": "Dharamshala", "id": "405"},
        {"name": "Pahalgam", "id": "2253"},
        {"name": "Gwailor", "id": "1291"},
        {"name": "Khandala", "id": "146301"},
        {"name": "Kovalam", "id": "1966"},
        {"name": "Madikeri", "id": "1886"},
        {"name": "Matheran", "id": "1667"},
        {"name": "Kamlimpong", "id": "1407"},
        {"name": "Thanjavur", "id": "738"},
        {"name": "Bhubaneswar", "id": "444"},
        {"name": "kasauli", "id": "2397"},
        {"name": "Ajmer", "id": "956"},
        {"name": "Aurangabad", "id": "607"},
        {"name": "Jammu", "id": "981"}
    ]

    # Loop through each city and call the migration function
    for city in cities_manual:
        city_id = city.get("id")
        # Skip cities with missing or invalid id
        if not city_id or city_id.upper() == "NA":
            print(f"Skipping city {city.get('name')} due to missing or invalid id.")
            continue
        print(f"Migrating data for city: {city.get('name')} (ID: {city_id})")
        migrate_data_for_city(city_id)




# Last used script
# import os
# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.api_core.retry import Retry
# from google.api_core import exceptions

# # Initialize the app with a service account, granting admin privileges
# firebase_credentials_path = os.path.join(
#     os.getcwd(),
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )
# cred = credentials.Certificate(firebase_credentials_path)
# firebase_admin.initialize_app(cred)

# # Get a reference to the Firestore client
# db = firestore.client()

# def migrate_data_for_city(city_id):
#     # Reference to the old collection
#     old_collection_ref = db.collection('searchedCategoriesNew')

#     # Reference to the new collection
#     new_collection_ref = db.collection('playlistsNew')

#     # Determine the starting value for playlist_counter
#     existing_playlists = new_collection_ref.stream()
#     max_list_id = 0
#     for doc in existing_playlists:
#         try:
#             list_id = int(doc.id)
#             if list_id > max_list_id:
#                 max_list_id = list_id
#         except ValueError:
#             continue  # Skip non-integer document IDs

#     # Initialize the playlist counter
#     playlist_counter = max_list_id + 1

#     # Fetch the specific city document
#     city_doc_ref = old_collection_ref.document(city_id)
#     city_doc = city_doc_ref.get()

#     if not city_doc.exists:
#         print(f"City with ID {city_id} does not exist in 'searchedCategories_backup' collection.")
#         return

#     city_data = city_doc.to_dict()
#     city_name = city_data.get('city_name', 'Unknown City')

#     print(f"Processing city: {city_name} (ID: {city_id})")

#     # Get all playlists (subcollections) under the city
#     try:
#         playlists = city_doc_ref.collections()
#     except Exception as e:
#         print(f"Error fetching playlists for city {city_name}: {e}")
#         return

#     for playlist in playlists:
#         playlist_title = playlist.id  # The subcollection name is the title
#         print(f'Migrating playlist: {playlist_title} in {city_name}')

#         # Assign a sequential list_id
#         list_id = str(playlist_counter)

#         # Prepare the new playlist document
#         new_playlist_data = {
#             'title': playlist_title,
#             'city': city_name,
#             'city_id': city_id,
#             'category': '',     # To be set manually later
#             'imageUrl': '',     # To be set manually later
#             'list_id': list_id,
#         }

#         # Create the new playlist document
#         new_playlist_ref = new_collection_ref.document(list_id)
#         new_playlist_ref.set(new_playlist_data)

#         # Migrate places under the playlist
#         try:
#             places = playlist.stream()
#         except Exception as e:
#             print(f"Error fetching places for playlist {playlist_title}: {e}")
#             continue

#         for place_doc in places:
#             place_data = place_doc.to_dict()
#             place_id = place_doc.id

#             # Write place data to the new 'places' subcollection
#             new_place_ref = new_playlist_ref.collection('places').document(place_id)
#             new_place_ref.set(place_data)

#         print(f'Playlist "{playlist_title}" migrated successfully.')

#         # Increment the playlist counter
#         playlist_counter += 1

# if __name__ == '__main__':
#     # Run the migration for Tokyo (city_id = '1')
#     migrate_data_for_city('1')



# Firestore writes exceeded error 
# import os
# import firebase_admin
# from firebase_admin import credentials, firestore

# # Initialize the app with a service account, granting admin privileges
# firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
# cred = credentials.Certificate(firebase_credentials_path)
# firebase_admin.initialize_app(cred)

# # Get a reference to the Firestore client
# db = firestore.client()

# def migrate_data():
#     # Reference to the old collection
#     old_collection_ref = db.collection('searchedCategories_backup')

#     # Reference to the new collection
#     new_collection_ref = db.collection('playlists')

#     # Determine the starting value for playlist_counter
#     existing_playlists = new_collection_ref.stream()
#     max_list_id = 0
#     for doc in existing_playlists:
#         try:
#             list_id = int(doc.id)
#             if list_id > max_list_id:
#                 max_list_id = list_id
#         except ValueError:
#             continue  # Skip non-integer document IDs

#     # Initialize the playlist counter
#     playlist_counter = max_list_id + 1

#     # Get all city documents
#     cities = old_collection_ref.stream()

#     for city_doc in cities:
#         city_data = city_doc.to_dict()
#         city_name = city_data.get('city_name', 'Unknown City')
#         city_id = city_doc.id  # Assuming city_doc.id is the city_id

#         city_doc_ref = old_collection_ref.document(city_doc.id)

#         # Get all playlists (subcollections) under the city
#         playlists = city_doc_ref.collections()

#         for playlist in playlists:
#             playlist_title = playlist.id  # The subcollection name is the title
#             print(f'Migrating playlist: {playlist_title} in {city_name}')

#             # Assign a sequential list_id
#             list_id = str(playlist_counter)

#             # Prepare the new playlist document
#             new_playlist_data = {
#                 'title': playlist_title,
#                 'city': city_name,
#                 'city_id': city_id,
#                 'category': '',     # To be set manually later
#                 'imageUrl': '',     # To be set manually later
#                 'list_id': list_id,
#             }

#             # Create the new playlist document
#             new_playlist_ref = new_collection_ref.document(list_id)
#             new_playlist_ref.set(new_playlist_data)

#             # Migrate places under the playlist
#             places = playlist.stream()

#             for place_doc in places:
#                 place_data = place_doc.to_dict()
#                 place_id = place_doc.id

#                 # Write place data to the new 'places' subcollection
#                 new_place_ref = new_playlist_ref.collection('places').document(place_id)
#                 new_place_ref.set(place_data)

#             print(f'Playlist "{playlist_title}" migrated successfully.')

#             # Increment the playlist counter
#             playlist_counter += 1

# if __name__ == '__main__':
#     migrate_data()





# Backup collection code
# import firebase_admin
# from firebase_admin import credentials, firestore
# import sys
# import time
# from google.api_core.exceptions import DeadlineExceeded

# def initialize_firebase():
#     """
#     Initialize Firebase Admin SDK with the service account key.
#     """
#     # Replace with the path to your Firebase service account key JSON file
#     service_account_key_path = r'C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json'

#     try:
#         if not firebase_admin._apps:
#             cred = credentials.Certificate(service_account_key_path)
#             firebase_admin.initialize_app(cred)
#             print("Firebase Admin SDK initialized successfully.")
#     except Exception as e:
#         print(f"Error initializing Firebase Admin SDK: {e}")
#         sys.exit(1)

# def copy_collection(source_collection_path, dest_collection_path):
#     """
#     Recursively copy documents and subcollections from source to destination with pagination.
#     """
#     db = firestore.client()
#     source_collection = db.collection(source_collection_path)
#     dest_collection = db.collection(dest_collection_path)

#     batch_size = 500  # Adjust as needed
#     last_doc = None
#     total_copied = 0

#     while True:
#         try:
#             query = source_collection.limit(batch_size)
#             if last_doc:
#                 query = query.start_after(last_doc)
#             docs = list(query.stream())
#             if not docs:
#                 print("No more documents to copy.")
#                 break

#             for doc in docs:
#                 doc_id = doc.id
#                 source_doc_ref = source_collection.document(doc_id)
#                 dest_doc_ref = dest_collection.document(doc_id)

#                 try:
#                     # Get document data
#                     doc_dict = doc.to_dict()
#                     # Copy document data to destination
#                     dest_doc_ref.set(doc_dict)
#                     print(f"Copied document: {source_collection_path}/{doc_id}")
#                     total_copied += 1

#                     # Recursively copy subcollections
#                     copy_subcollections(source_doc_ref, dest_doc_ref)

#                 except Exception as e:
#                     print(f"Error copying document {doc_id}: {e}")

#                 last_doc = doc  # Update last_doc for pagination

#         except DeadlineExceeded as e:
#             print(f"Deadline exceeded during main query. Retrying after a short pause... {e}")
#             time.sleep(5)
#             continue
#         except Exception as e:
#             print(f"An error occurred: {e}")
#             break

#     print(f"Total documents copied: {total_copied}")

# def copy_subcollections(source_doc_ref, dest_doc_ref):
#     """
#     Recursively copy all subcollections from source document to destination document.
#     """
#     try:
#         subcollections = source_doc_ref.collections()
#         for subcol in subcollections:
#             subcol_name = subcol.id
#             source_subcol_ref = source_doc_ref.collection(subcol_name)
#             dest_subcol_ref = dest_doc_ref.collection(subcol_name)

#             # Pagination for subcollections
#             batch_size = 500
#             last_subdoc = None
#             while True:
#                 try:
#                     subquery = source_subcol_ref.limit(batch_size)
#                     if last_subdoc:
#                         subquery = subquery.start_after(last_subdoc)
#                     docs = list(subquery.stream())
#                     if not docs:
#                         break

#                     for doc in docs:
#                         doc_id = doc.id
#                         doc_dict = doc.to_dict()
#                         dest_subcol_ref.document(doc_id).set(doc_dict)
#                         print(f"Copied subcollection document: {source_doc_ref.path}/{subcol_name}/{doc_id}")

#                         # Recursively copy nested subcollections
#                         copy_subcollections(source_subcol_ref.document(doc_id), dest_subcol_ref.document(doc_id))

#                         last_subdoc = doc  # Update last_subdoc for pagination

#                 except DeadlineExceeded as e:
#                     print(f"Deadline exceeded while copying subcollection '{subcol_name}'. Retrying after a short pause... {e}")
#                     time.sleep(5)
#                     continue
#                 except Exception as e:
#                     print(f"Error copying subcollection '{subcol_name}': {e}")
#                     break

#     except Exception as e:
#         print(f"Error copying subcollections for document {source_doc_ref.id}: {e}")

# def main():
#     initialize_firebase()

#     # Define source and destination collection paths
#     source_collection_path = 'searchedCategories'
#     dest_collection_path = 'searchedCategories_backup'  # Change to your desired destination

#     print(f"Starting replication from '{source_collection_path}' to '{dest_collection_path}'...")
#     copy_collection(source_collection_path, dest_collection_path)
#     print("Replication completed successfully.")

# if __name__ == '__main__':
#     main()


# import firebase_admin
# from firebase_admin import credentials, firestore
# import sys

# def initialize_firebase():
#     """
#     Initialize Firebase Admin SDK with the service account key.
#     """
#     # Replace with the path to your Firebase service account key JSON file
#     service_account_key_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

#     try:
#         if not firebase_admin._apps:
#             cred = credentials.Certificate(service_account_key_path)
#             firebase_admin.initialize_app(cred)
#             print("Firebase Admin SDK initialized successfully.")
#     except Exception as e:
#         print(f"Error initializing Firebase Admin SDK: {e}")
#         sys.exit(1)

# def copy_collection(source_collection_path, dest_collection_path):
#     """
#     Recursively copy documents and subcollections from source to destination.

#     :param source_collection_path: Path to the source collection.
#     :param dest_collection_path: Path to the destination collection.
#     """
#     db = firestore.client()
#     source_collection = db.collection(source_collection_path)
#     docs = source_collection.stream()

#     for doc in docs:
#         doc_id = doc.id
#         source_doc_ref = source_collection.document(doc_id)
#         dest_doc_ref = db.collection(dest_collection_path).document(doc_id)

#         try:
#             # Get document data
#             doc_dict = doc.to_dict()
#             # Copy document data to destination
#             dest_doc_ref.set(doc_dict)
#             print(f"Copied document: {source_collection_path}/{doc_id}")

#             # Recursively copy subcollections
#             copy_subcollections(source_doc_ref, dest_doc_ref)

#         except Exception as e:
#             print(f"Error copying document {doc_id}: {e}")

# def copy_subcollections(source_doc_ref, dest_doc_ref):
#     """
#     Recursively copy all subcollections from source document to destination document.

#     :param source_doc_ref: Reference to the source document.
#     :param dest_doc_ref: Reference to the destination document.
#     """
#     try:
#         subcollections = source_doc_ref.collections()
#         for subcol in subcollections:
#             subcol_name = subcol.id
#             source_subcol_ref = source_doc_ref.collection(subcol_name)
#             dest_subcol_ref = dest_doc_ref.collection(subcol_name)

#             docs = source_subcol_ref.stream()
#             for doc in docs:
#                 doc_id = doc.id
#                 doc_dict = doc.to_dict()
#                 dest_subcol_ref.document(doc_id).set(doc_dict)
#                 print(f"Copied subcollection document: {source_doc_ref.path}/{subcol_name}/{doc_id}")

#                 # Recursively copy nested subcollections
#                 copy_subcollections(source_subcol_ref.document(doc_id), dest_subcol_ref.document(doc_id))
#     except Exception as e:
#         print(f"Error copying subcollections for document {source_doc_ref.id}: {e}")

# def main():
#     initialize_firebase()

#     # Define source and destination collection paths
#     source_collection_path = 'searchedCategories'
#     dest_collection_path = 'searchedCategories_backup'  # Change to your desired destination

#     print(f"Starting replication from '{source_collection_path}' to '{dest_collection_path}'...")
#     copy_collection(source_collection_path, dest_collection_path)
#     print("Replication completed successfully.")

# if __name__ == '__main__':
#     main()







# import os
# import json
# import firebase_admin
# from firebase_admin import credentials, firestore
# import sys
# import time
# from google.api_core.exceptions import DeadlineExceeded

# def initialize_firestore():
#     """
#     Initialize Firebase Admin SDK.
#     """
#     # Replace with the path to your Firebase service account key JSON file
#     firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     cred = credentials.Certificate(firebase_credentials_path)
#     if not firebase_admin._apps:
#         firebase_admin.initialize_app(cred)
#     return firestore.client()

# def copy_subcollections(source_doc_ref, dest_doc_ref, subcollection_names):
#     """
#     Copies specified subcollections from source document to destination document.

#     :param source_doc_ref: Reference to the source document.
#     :param dest_doc_ref: Reference to the destination document.
#     :param subcollection_names: List of subcollection names to copy.
#     """
#     for subcol_name in subcollection_names:
#         source_subcol_ref = source_doc_ref.collection(subcol_name)
#         dest_subcol_ref = dest_doc_ref.collection(subcol_name)

#         try:
#             # Stream documents from the source subcollection
#             subcol_docs = list(source_subcol_ref.stream())
#             if not subcol_docs:
#                 print(f"No documents found in subcollection '{subcol_name}' for document '{source_doc_ref.id}'.")
#                 continue

#             for doc in subcol_docs:
#                 doc_id = doc.id
#                 doc_dict = doc.to_dict()
#                 # Copy document to destination subcollection without overwriting existing data
#                 dest_subcol_doc_ref = dest_subcol_ref.document(doc_id)
#                 dest_subcol_doc_ref.set(doc_dict, merge=True)  # Use merge=True to avoid overwriting
#                 print(f"Copied document '{doc_id}' in subcollection '{subcol_name}' for place_id '{source_doc_ref.id}'.")

#             print(f"Successfully copied subcollection '{subcol_name}' for place_id '{source_doc_ref.id}'.")
#         except Exception as e:
#             print(f"Error copying subcollection '{subcol_name}' for place_id '{source_doc_ref.id}': {e}")

# def process_missing_subcollections():
#     """
#     Processes documents with missing subcollections by copying data from 'exploreData' to 'explore'.
#     """
#     db = initialize_firestore()

#     # Load the list of place_ids with missing subcollections
#     input_file_path = os.path.join(os.getcwd(), 'missing_subcollections copy.json')
#     if not os.path.exists(input_file_path):
#         print(f"Input file '{input_file_path}' not found.")
#         sys.exit(1)

#     with open(input_file_path, 'r') as json_file:
#         missing_subcollections = json.load(json_file)

#     total_places = len(missing_subcollections)
#     print(f"Total places to process: {total_places}")

#     for idx, entry in enumerate(missing_subcollections):
#         place_id = entry.get('place_id')
#         if not place_id:
#             print(f"Missing 'place_id' in entry: {entry}")
#             continue

#         print(f"\nProcessing place {idx + 1}/{total_places}: place_id '{place_id}'")

#         # References to the documents in both collections
#         source_doc_ref = db.collection('exploreData').document(place_id)
#         dest_doc_ref = db.collection('explore').document(place_id)

#         try:
#             # Check if the source document exists
#             source_doc = source_doc_ref.get()
#             if not source_doc.exists:
#                 print(f"Source document '{place_id}' does not exist in 'exploreData'. Skipping.")
#                 continue

#             # Check if the destination document exists
#             dest_doc = dest_doc_ref.get()
#             if not dest_doc.exists:
#                 print(f"Destination document '{place_id}' does not exist in 'explore'. Skipping.")
#                 continue

#             # Determine which subcollections are missing
#             subcollections_to_copy = []
#             if entry.get('missing_TouristAttractions'):
#                 subcollections_to_copy.append('TouristAttractions')
#             if entry.get('missing_TouristRestaurants'):
#                 subcollections_to_copy.append('TouristRestaurants')

#             if not subcollections_to_copy:
#                 print(f"No subcollections to copy for place_id '{place_id}'.")
#                 continue

#             # Copy the missing subcollections
#             copy_subcollections(source_doc_ref, dest_doc_ref, subcollections_to_copy)

#         except DeadlineExceeded as e:
#             print(f"Deadline exceeded while processing place_id '{place_id}'. Retrying after a short pause...")
#             time.sleep(5)
#             continue
#         except Exception as e:
#             print(f"An error occurred while processing place_id '{place_id}': {e}")
#             continue

#     print("\nProcessing completed.")

# def main():
#     process_missing_subcollections()

# if __name__ == '__main__':
#     main()




# import os
# import json
# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.api_core.exceptions import DeadlineExceeded
# import time

# def initialize_firestore():
#     """
#     Initialize Firebase Admin SDK.
#     """
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     if not firebase_admin._apps:
#         firebase_admin.initialize_app(cred)
#     return firestore.client()

# def find_documents_with_subcollections():
#     """
#     Find documents in the 'exploreData' collection that have the subcollections
#     'TouristAttractions' and 'TouristRestaurants'. Create a JSON file with their place_id.
#     """
#     db = initialize_firestore()
#     explore_collection = db.collection('exploreData')

#     total_documents = 0
#     matching_documents = []

#     batch_size = 500  # Adjust batch size if necessary
#     last_doc = None

#     while True:
#         try:
#             query = explore_collection.limit(batch_size)
#             if last_doc:
#                 query = query.start_after(last_doc)
#             docs = list(query.stream())
#             if not docs:
#                 break

#             for doc in docs:
#                 total_documents += 1
#                 place_id = doc.id
#                 last_doc = doc  # Update last_doc for pagination

#                 # Get the subcollection references for TouristAttractions and TouristRestaurants
#                 parent_doc_ref = explore_collection.document(place_id)
#                 attractions_subcol_ref = parent_doc_ref.collection('TouristAttractions')
#                 restaurants_subcol_ref = parent_doc_ref.collection('TouristRestaurants')

#                 # Initialize exists flags
#                 attractions_exists = False
#                 restaurants_exists = False

#                 # Check if the subcollections exist by trying to stream their contents with a timeout
#                 try:
#                     attractions_docs = attractions_subcol_ref.limit(1).stream(timeout=10)
#                     attractions_exists = any(attractions_docs)
#                 except DeadlineExceeded:
#                     print(f"Deadline exceeded while accessing TouristAttractions for {place_id}")
#                     attractions_exists = False
#                 except Exception as e:
#                     print(f"Error accessing TouristAttractions for {place_id}: {e}")
#                     attractions_exists = False

#                 try:
#                     restaurants_docs = restaurants_subcol_ref.limit(1).stream(timeout=10)
#                     restaurants_exists = any(restaurants_docs)
#                 except DeadlineExceeded:
#                     print(f"Deadline exceeded while accessing TouristRestaurants for {place_id}")
#                     restaurants_exists = False
#                 except Exception as e:
#                     print(f"Error accessing TouristRestaurants for {place_id}: {e}")
#                     restaurants_exists = False

#                 # If both subcollections exist, add the place_id to the list
#                 if attractions_exists and restaurants_exists:
#                     matching_entry = {
#                         "place_id": place_id
#                     }
#                     matching_documents.append(matching_entry)
#                     print(f"Document {place_id} has both subcollections.")

#         except DeadlineExceeded as e:
#             print(f"Deadline exceeded during main query. Retrying after a short pause... {e}")
#             time.sleep(5)
#             continue
#         except Exception as e:
#             print(f"An error occurred: {e}")
#             break

#     # Save the list of matching documents as a JSON file
#     output_file_path = os.path.join(os.getcwd(), 'documents_with_subcollections.json')
#     with open(output_file_path, 'w') as json_file:
#         json.dump(matching_documents, json_file, indent=4)

#     print(f"\nTotal documents processed: {total_documents}")
#     print(f"Documents with both subcollections: {len(matching_documents)}")
#     print(f"JSON file with matching documents created at: {output_file_path}")

# if __name__ == '__main__':
#     find_documents_with_subcollections()


# import os
# import json
# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.api_core.exceptions import DeadlineExceeded
# import time

# def initialize_firestore():
#     """
#     Initialize Firebase Admin SDK.
#     """
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     if not firebase_admin._apps:
#         firebase_admin.initialize_app(cred)
#     return firestore.client()

# def find_documents_missing_subcollections():
#     """
#     Find documents in the 'explore' collection that are missing the subcollections
#     'TouristAttractions' or 'TouristRestaurants'. Create a JSON file with their place_id.
#     """
#     db = initialize_firestore()
#     explore_collection = db.collection('explore')

#     total_documents = 0
#     missing_subcollections = []

#     batch_size = 500  # Adjust batch size if necessary
#     last_doc = None

#     while True:
#         try:
#             query = explore_collection.limit(batch_size)
#             if last_doc:
#                 query = query.start_after(last_doc)
#             docs = list(query.stream())
#             if not docs:
#                 break

#             for doc in docs:
#                 total_documents += 1
#                 place_id = doc.id
#                 last_doc = doc  # Update last_doc for pagination

#                 # Get the subcollection references for TouristAttractions and TouristRestaurants
#                 parent_doc_ref = explore_collection.document(place_id)
#                 attractions_subcol_ref = parent_doc_ref.collection('TouristAttractions')
#                 restaurants_subcol_ref = parent_doc_ref.collection('TouristRestaurants')

#                 # Initialize missing flags
#                 missing_TouristAttractions = False
#                 missing_TouristRestaurants = False

#                 # Check if the subcollections exist by trying to stream their contents with a timeout
#                 try:
#                     attractions_docs = attractions_subcol_ref.limit(1).stream(timeout=10)
#                     attractions_exists = any(attractions_docs)
#                 except DeadlineExceeded:
#                     print(f"Deadline exceeded while accessing TouristAttractions for {place_id}")
#                     attractions_exists = False
#                 except Exception as e:
#                     print(f"Error accessing TouristAttractions for {place_id}: {e}")
#                     attractions_exists = False

#                 try:
#                     restaurants_docs = restaurants_subcol_ref.limit(1).stream(timeout=10)
#                     restaurants_exists = any(restaurants_docs)
#                 except DeadlineExceeded:
#                     print(f"Deadline exceeded while accessing TouristRestaurants for {place_id}")
#                     restaurants_exists = False
#                 except Exception as e:
#                     print(f"Error accessing TouristRestaurants for {place_id}: {e}")
#                     restaurants_exists = False

#                 # Update missing flags
#                 if not attractions_exists:
#                     missing_TouristAttractions = True
#                 if not restaurants_exists:
#                     missing_TouristRestaurants = True

#                 # If either of the subcollections is missing, add the place_id to the list
#                 if missing_TouristAttractions or missing_TouristRestaurants:
#                     missing_entry = {
#                         "place_id": place_id,
#                         "missing_TouristAttractions": missing_TouristAttractions,
#                         "missing_TouristRestaurants": missing_TouristRestaurants
#                     }
#                     missing_subcollections.append(missing_entry)
#                     print(f"Document {place_id} is missing subcollections: {missing_entry}")

#         except DeadlineExceeded as e:
#             print(f"Deadline exceeded during main query. Retrying after a short pause... {e}")
#             time.sleep(5)
#             continue
#         except Exception as e:
#             print(f"An error occurred: {e}")
#             break

#     # Save the list of missing subcollections as a JSON file
#     output_file_path = os.path.join(os.getcwd(), 'missing_subcollections.json')
#     with open(output_file_path, 'w') as json_file:
#         json.dump(missing_subcollections, json_file, indent=4)

#     print(f"\nTotal documents processed: {total_documents}")
#     print(f"Documents with missing subcollections: {len(missing_subcollections)}")
#     print(f"JSON file with missing subcollections created at: {output_file_path}")

# if __name__ == '__main__':
#     find_documents_missing_subcollections()


# # Python Script to Find Missing Subcollections
# import os
# import json
# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore():
#     """
#     Initialize Firebase Admin SDK.
#     """
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     return firestore.client()

# def find_documents_missing_subcollections():
#     """
#     Find documents in the 'explore' collection that are missing the subcollections
#     'TouristAttractions' or 'TouristRestaurants'. Create a JSON file with their place_id.
#     """
#     db = initialize_firestore()
#     explore_collection = db.collection('explore')
#     docs = explore_collection.stream()

#     total_documents = 0
#     missing_subcollections = []

#     for doc in docs:
#         total_documents += 1
#         place_id = doc.id

#         # Get the subcollection references for TouristAttractions and TouristRestaurants
#         attractions_subcol_ref = explore_collection.document(place_id).collection('TouristAttractions')
#         restaurants_subcol_ref = explore_collection.document(place_id).collection('TouristRestaurants')

#         # Check if the subcollections exist by trying to stream their contents
#         attractions_docs = attractions_subcol_ref.limit(1).stream()
#         restaurants_docs = restaurants_subcol_ref.limit(1).stream()

#         attractions_exists = any(attractions_docs)
#         restaurants_exists = any(restaurants_docs)

#         # If either of the subcollections is missing, add the place_id to the list
#         if not attractions_exists or not restaurants_exists:
#             missing_entry = {
#                 "place_id": place_id,
#                 "missing_TouristAttractions": not attractions_exists,
#                 "missing_TouristRestaurants": not restaurants_exists
#             }
#             missing_subcollections.append(missing_entry)
#             print(f"Document {place_id} is missing subcollections: {missing_entry}")

#     # Save the list of missing subcollections as a JSON file
#     output_file_path = os.path.join(os.getcwd(), 'missing_subcollections.json')
#     with open(output_file_path, 'w') as json_file:
#         json.dump(missing_subcollections, json_file, indent=4)

#     print(f"\nTotal documents processed: {total_documents}")
#     print(f"Documents with missing subcollections: {len(missing_subcollections)}")
#     print(f"JSON file with missing subcollections created at: {output_file_path}")

# if __name__ == '__main__':
#     find_documents_missing_subcollections()


# import firebase_admin
# from firebase_admin import credentials, firestore
# import sys
# import time
# from google.api_core.exceptions import DeadlineExceeded

# def initialize_firebase():
#     """
#     Initialize Firebase Admin SDK with the service account key.
#     """
#     # Replace with the path to your Firebase service account key JSON file
#     service_account_key_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

#     try:
#         if not firebase_admin._apps:
#             cred = credentials.Certificate(service_account_key_path)
#             firebase_admin.initialize_app(cred)
#             print("Firebase Admin SDK initialized successfully.")
#     except Exception as e:
#         print(f"Error initializing Firebase Admin SDK: {e}")
#         sys.exit(1)

# def get_top_image_url(subcollection_ref):
#     """
#     Retrieves the top image URL from the first document in the subcollection.

#     :param subcollection_ref: Reference to the subcollection.
#     :return: The top image URL or None if not found.
#     """
#     try:
#         # Get the first document in the subcollection
#         docs = subcollection_ref.limit(1).stream()
#         for doc in docs:
#             data = doc.to_dict()
#             g_image_urls = data.get('g_image_urls')
#             if g_image_urls and isinstance(g_image_urls, list) and len(g_image_urls) > 0:
#                 return g_image_urls[0]  # Return the first image URL
#         return None  # Return None if no image URLs found
#     except Exception as e:
#         print(f"Error retrieving top image URL from subcollection {subcollection_ref.id}: {e}")
#         return None

# def update_parent_document(parent_doc_ref, top_attraction_image_url, top_restaurant_image_url):
#     """
#     Updates the parent document with the top image URLs.

#     :param parent_doc_ref: Reference to the parent document.
#     :param top_attraction_image_url: URL string or None.
#     :param top_restaurant_image_url: URL string or None.
#     """
#     update_data = {}
#     if top_attraction_image_url:
#         update_data['topAttractionImageUrl'] = top_attraction_image_url
#     if top_restaurant_image_url:
#         update_data['topRestaurantImageUrl'] = top_restaurant_image_url

#     if update_data:
#         try:
#             parent_doc_ref.update(update_data)
#             print(f"Updated parent document {parent_doc_ref.id} with top image URLs.")
#         except Exception as e:
#             print(f"Error updating parent document {parent_doc_ref.id}: {e}")
#     else:
#         print(f"No top image URLs found for parent document {parent_doc_ref.id}. No update performed.")

# def process_parent_documents():
#     db = firestore.client()
#     explore_collection = db.collection('explore')

#     total_docs = 0
#     updated_docs = 0
#     batch_size = 500  # Adjust as necessary
#     last_doc = None

#     while True:
#         try:
#             query = explore_collection.limit(batch_size)
#             if last_doc:
#                 query = query.start_after(last_doc)
#             docs = list(query.stream())

#             if not docs:
#                 break

#             for doc in docs:
#                 total_docs += 1
#                 parent_doc_ref = explore_collection.document(doc.id)
#                 print(f"\nProcessing parent document: {doc.id}")
#                 last_doc = doc  # Update last_doc for pagination

#                 # References to subcollections
#                 attractions_subcol_ref = parent_doc_ref.collection('TouristAttractions')
#                 restaurants_subcol_ref = parent_doc_ref.collection('TouristRestaurants')

#                 # Get top image URLs from subcollections
#                 top_attraction_image_url = get_top_image_url(attractions_subcol_ref)
#                 top_restaurant_image_url = get_top_image_url(restaurants_subcol_ref)

#                 # Update parent document
#                 update_parent_document(parent_doc_ref, top_attraction_image_url, top_restaurant_image_url)

#                 if top_attraction_image_url or top_restaurant_image_url:
#                     updated_docs += 1

#         except DeadlineExceeded as e:
#             print(f"Deadline exceeded during main query. Retrying after a short pause... {e}")
#             time.sleep(5)
#             continue
#         except Exception as e:
#             print(f"An error occurred: {e}")
#             break

#     print(f"\nProcessed {total_docs} parent documents.")
#     print(f"Updated {updated_docs} parent documents with top image URLs.")

# def main():
#     initialize_firebase()
#     process_parent_documents()
#     print("Script execution completed.")

# if __name__ == '__main__':
#     main()






# # topattractionurl &toprestauranturl
# import firebase_admin
# from firebase_admin import credentials, firestore
# import sys

# def initialize_firebase():
#     """
#     Initialize Firebase Admin SDK with the service account key.
#     """
#     # Replace with the path to your Firebase service account key JSON file
#     service_account_key_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

#     try:
#         cred = credentials.Certificate(service_account_key_path)
#         firebase_admin.initialize_app(cred)
#         print("Firebase Admin SDK initialized successfully.")
#     except Exception as e:
#         print(f"Error initializing Firebase Admin SDK: {e}")
#         sys.exit(1)

# def get_top_image_url(subcollection_ref):
#     """
#     Retrieves the top image URL from the first document in the subcollection.

#     :param subcollection_ref: Reference to the subcollection.
#     :return: The top image URL or None if not found.
#     """
#     try:
#         # Get the first document in the subcollection
#         docs = subcollection_ref.limit(1).stream()
#         for doc in docs:
#             data = doc.to_dict()
#             g_image_urls = data.get('g_image_urls')
#             if g_image_urls and isinstance(g_image_urls, list) and len(g_image_urls) > 0:
#                 return g_image_urls[0]  # Return the first image URL
#         return None  # Return None if no image URLs found
#     except Exception as e:
#         print(f"Error retrieving top image URL from subcollection {subcollection_ref.id}: {e}")
#         return None

# def update_parent_document(parent_doc_ref, top_attraction_image_url, top_restaurant_image_url):
#     """
#     Updates the parent document with the top image URLs.

#     :param parent_doc_ref: Reference to the parent document.
#     :param top_attraction_image_url: URL string or None.
#     :param top_restaurant_image_url: URL string or None.
#     """
#     update_data = {}
#     if top_attraction_image_url:
#         update_data['topAttractionImageUrl'] = top_attraction_image_url
#     if top_restaurant_image_url:
#         update_data['topRestaurantImageUrl'] = top_restaurant_image_url

#     if update_data:
#         try:
#             parent_doc_ref.update(update_data)
#             print(f"Updated parent document {parent_doc_ref.id} with top image URLs.")
#         except Exception as e:
#             print(f"Error updating parent document {parent_doc_ref.id}: {e}")
#     else:
#         print(f"No top image URLs found for parent document {parent_doc_ref.id}. No update performed.")

# def process_parent_documents():
#     db = firestore.client()
#     explore_collection = db.collection('explore')

#     # Retrieve all documents in the 'explore' collection
#     docs = explore_collection.stream()
#     total_docs = 0
#     updated_docs = 0

#     for doc in docs:
#         total_docs += 1
#         parent_doc_ref = explore_collection.document(doc.id)
#         print(f"\nProcessing parent document: {doc.id}")

#         # References to subcollections
#         attractions_subcol_ref = parent_doc_ref.collection('TouristAttractions')
#         restaurants_subcol_ref = parent_doc_ref.collection('TouristRestaurants')

#         # Get top image URLs from subcollections
#         top_attraction_image_url = get_top_image_url(attractions_subcol_ref)
#         top_restaurant_image_url = get_top_image_url(restaurants_subcol_ref)

#         # Update parent document
#         update_parent_document(parent_doc_ref, top_attraction_image_url, top_restaurant_image_url)

#         if top_attraction_image_url or top_restaurant_image_url:
#             updated_docs += 1

#     print(f"\nProcessed {total_docs} parent documents.")
#     print(f"Updated {updated_docs} parent documents with top image URLs.")

# def main():
#     initialize_firebase()
#     process_parent_documents()
#     print("Script execution completed.")

# if __name__ == '__main__':
#     main()

# import firebase_admin
# from firebase_admin import credentials, firestore
# import sys

# def initialize_firebase():
#     """
#     Initialize Firebase Admin SDK with the service account key.
#     """
#     # Replace with the path to your Firebase service account key JSON file
#     service_account_key_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

#     try:
#         cred = credentials.Certificate(service_account_key_path)
#         firebase_admin.initialize_app(cred)
#         print("Firebase Admin SDK initialized successfully.")
#     except Exception as e:
#         print(f"Error initializing Firebase Admin SDK: {e}")
#         sys.exit(1)

# def copy_collection(source_collection_path, dest_collection_path):
#     """
#     Recursively copy documents and subcollections from source to destination.

#     :param source_collection_path: Path to the source collection.
#     :param dest_collection_path: Path to the destination collection.
#     """
#     db = firestore.client()
#     source_collection = db.collection(source_collection_path)
#     docs = source_collection.stream()

#     for doc in docs:
#         doc_id = doc.id
#         source_doc_ref = source_collection.document(doc_id)
#         dest_doc_ref = db.collection(dest_collection_path).document(doc_id)

#         try:
#             # Get document data
#             doc_dict = doc.to_dict()
#             # Copy document data to destination
#             dest_doc_ref.set(doc_dict)
#             print(f"Copied document: {source_collection_path}/{doc_id}")

#             # Recursively copy subcollections
#             copy_subcollections(source_doc_ref, dest_doc_ref)

#         except Exception as e:
#             print(f"Error copying document {doc_id}: {e}")

# def copy_subcollections(source_doc_ref, dest_doc_ref):
#     """
#     Recursively copy all subcollections from source document to destination document.

#     :param source_doc_ref: Reference to the source document.
#     :param dest_doc_ref: Reference to the destination document.
#     """
#     try:
#         subcollections = source_doc_ref.collections()
#         for subcol in subcollections:
#             subcol_name = subcol.id
#             source_subcol_ref = source_doc_ref.collection(subcol_name)
#             dest_subcol_ref = dest_doc_ref.collection(subcol_name)

#             docs = source_subcol_ref.stream()
#             for doc in docs:
#                 doc_id = doc.id
#                 doc_dict = doc.to_dict()
#                 dest_subcol_ref.document(doc_id).set(doc_dict)
#                 print(f"Copied subcollection document: {source_doc_ref.path}/{subcol_name}/{doc_id}")

#                 # Recursively copy nested subcollections
#                 copy_subcollections(source_subcol_ref.document(doc_id), dest_subcol_ref.document(doc_id))
#     except Exception as e:
#         print(f"Error copying subcollections for document {source_doc_ref.id}: {e}")

# def main():
#     initialize_firebase()

#     # Define source and destination collection paths
#     source_collection_path = 'explore'
#     dest_collection_path = 'explore_backup'  # Change to your desired destination

#     print(f"Starting replication from '{source_collection_path}' to '{dest_collection_path}'...")
#     copy_collection(source_collection_path, dest_collection_path)
#     print("Replication completed successfully.")

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
# import logging
# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util.retry import Retry

# # Replace with your actual API key
# GOOGLE_PLACES_API_KEY = 'AIzaSyBe-suda095R60l0G1NrdYeIVKjUCxwK_8'

# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger()

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     try:
#         logger.info("Initializing Firebase Admin SDK...")
#         firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred, {
#             'storageBucket': 'mycasavsc.appspot.com'
#         })
#         db = firestore.client()
#         return db
#     except Exception as e:
#         logger.error(f"Failed to initialize Firebase: {e}")
#         raise

# # Initialize Google Maps Client
# def initialize_google_maps():
#     try:
#         logger.info("Initializing Google Maps API client...")
#         gmaps = googlemaps.Client(key=GOOGLE_PLACES_API_KEY)
#         return gmaps
#     except Exception as e:
#         logger.error(f"Failed to initialize Google Maps client: {e}")
#         raise

# # Search for a place and get its Google Place ID
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
#             g_place_id = candidates[0]['place_id']
#             logger.info(f"Found Google Place ID: {g_place_id} for {city_name}")
#             return g_place_id
#         else:
#             logger.warning(f"No place found for {city_name}")
#             return None
#     except ApiError as e:
#         logger.error(f"API error while searching for place {city_name}: {e}")
#         return None
#     except Exception as e:
#         logger.error(f"Unexpected error while searching for place {city_name}: {e}")
#         return None

# # Get photo reference for a Google Place ID
# def get_photo_reference(gmaps, g_place_id):
#     try:
#         response = gmaps.place(place_id=g_place_id, fields=['photo'])
#         result = response.get('result')
#         photos = result.get('photos')
#         if photos:
#             photo_reference = photos[0]['photo_reference']
#             logger.info(f"Found photo reference for Google Place ID {g_place_id}")
#             return photo_reference
#         else:
#             logger.warning(f"No photos available for Google Place ID {g_place_id}")
#             return None
#     except ApiError as e:
#         logger.error(f"API error while getting photo reference for Google Place ID {g_place_id}: {e}")
#         return None
#     except Exception as e:
#         logger.error(f"Unexpected error while getting photo reference for Google Place ID {g_place_id}: {e}")
#         return None

# # Download the photo using the photo_reference
# def download_photo(photo_reference, filename):
#     photo_url = "https://maps.googleapis.com/maps/api/place/photo"
#     params = {
#         'maxwidth': 400,
#         'photoreference': photo_reference,
#         'key': GOOGLE_PLACES_API_KEY,
#     }
#     try:
#         session = requests.Session()
#         retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
#         session.mount('https://', HTTPAdapter(max_retries=retries))
#         response = session.get(photo_url, params=params, stream=True, timeout=10)
#         response.raise_for_status()
#         with open(filename, 'wb') as f:
#             for chunk in response.iter_content(1024):
#                 f.write(chunk)
#         logger.info(f"Photo downloaded: {filename}")
#         return True
#     except requests.exceptions.RequestException as e:
#         logger.error(f"Error downloading photo: {e}")
#         return False

# # Upload the image to Firebase Storage
# def upload_image_to_firebase_storage(local_filename, storage_path):
#     try:
#         bucket = storage.bucket()
#         blob = bucket.blob(storage_path)
#         blob.upload_from_filename(local_filename)
#         blob.make_public()
#         image_url = blob.public_url
#         logger.info(f"Uploaded {local_filename} to Firebase Storage at {storage_path}")
#         return image_url
#     except Exception as e:
#         logger.error(f"Error uploading image to Firebase Storage: {e}")
#         return None

# # Update Firestore document with image URL and g_place_id
# def update_firestore_document(db, place_id, image_url, g_place_id):
#     try:
#         doc_ref = db.collection('explore').document(place_id)
#         doc_ref.update({
#             'image_url': firestore.ArrayUnion([image_url]),
#             'g_place_id': g_place_id
#         })
#         logger.info(f"Updated Firestore document for place_id {place_id} with image_url and g_place_id")
#     except NotFound:
#         logger.warning(f"Firestore document for place_id {place_id} not found")
#     except Exception as e:
#         logger.error(f"Error updating Firestore document for place_id {place_id}: {e}")

# # Process each place
# def process_place(db, gmaps, place_id):
#     try:
#         doc_ref = db.collection('explore').document(place_id)
#         doc = doc_ref.get()
#         if not doc.exists:
#             logger.warning(f"Document {place_id} does not exist in Firestore.")
#             return
#         data = doc.to_dict()

#         city_name = data.get('city_name')
#         latitude = data.get('latitude')
#         longitude = data.get('longitude')

#         if not city_name:
#             logger.warning(f"city_name is missing for document {place_id}")
#             return

#         g_place_id = get_google_place_id(gmaps, city_name, latitude, longitude)
#         if not g_place_id:
#             return

#         photo_reference = get_photo_reference(gmaps, g_place_id)
#         if not photo_reference:
#             return

#         local_filename = f"{place_id}.jpg"
#         if not download_photo(photo_reference, local_filename):
#             return

#         storage_path = f"places/{place_id}.jpg"
#         image_url = upload_image_to_firebase_storage(local_filename, storage_path)
#         if not image_url:
#             os.remove(local_filename)
#             return

#         update_firestore_document(db, place_id, image_url, g_place_id)

#         # Clean up local file
#         os.remove(local_filename)

#     except Exception as e:
#         logger.error(f"Error processing place_id {place_id}: {e}")

# # Main function
# def main():
#     db = initialize_firebase()
#     gmaps = initialize_google_maps()

#     with open('missing_image_urls.json', 'r', encoding='utf-8') as f:
#         places = json.load(f)

#     logger.info(f"Total places to process: {len(places)}")

#     for idx, place in enumerate(places):
#         place_id = place.get('place_id')
#         if not place_id:
#             logger.warning(f"Invalid place data: {place}")
#             continue
#         logger.info(f"\nProcessing place {idx + 1}/{len(places)}: place_id {place_id}")
#         process_place(db, gmaps, place_id)
#         time.sleep(1)

# if __name__ == '__main__':
#     main()



# working but slow
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
#     print("Initializing Firebase Admin SDK...")
#     # Replace with the path to your Firebase service account JSON file
#     firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'
#     })
#     db = firestore.client()
#     return db

# # Initialize Google Maps Client
# def initialize_google_maps():
#     print("Initializing Google Maps API client...")
#     gmaps = googlemaps.Client(key=GOOGLE_PLACES_API_KEY)
#     return gmaps

# # Search for a place and get its Google Place ID
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
#             g_place_id = candidates[0]['place_id']
#             print(f"Found Google Place ID: {g_place_id} for {city_name}")
#             return g_place_id
#         else:
#             print(f"No place found for {city_name}")
#             return None
#     except ApiError as e:
#         print(f"Error searching for place {city_name}: {e}")
#         return None

# # Get photo reference for a Google Place ID
# def get_photo_reference(gmaps, g_place_id):
#     try:
#         response = gmaps.place(place_id=g_place_id, fields=['photo'])
#         result = response.get('result')
#         photos = result.get('photos')
#         if photos:
#             photo_reference = photos[0]['photo_reference']
#             print(f"Found photo reference for Google Place ID {g_place_id}")
#             return photo_reference
#         else:
#             print(f"No photos available for Google Place ID {g_place_id}")
#             return None
#     except ApiError as e:
#         print(f"Error getting photo reference for Google Place ID {g_place_id}: {e}")
#         return None

# # Download the photo using the photo_reference
# def download_photo(photo_reference, filename):
#     photo_url = "https://maps.googleapis.com/maps/api/place/photo"
#     params = {
#         'maxwidth': 400,
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
#         image_url = blob.public_url
#         print(f"Uploaded {local_filename} to Firebase Storage at {storage_path}")
#         return image_url
#     except Exception as e:
#         print(f"Error uploading image to Firebase Storage: {e}")
#         return None

# # Update Firestore document with image URL and g_place_id
# def update_firestore_document(db, place_id, image_url, g_place_id):
#     try:
#         doc_ref = db.collection('explore').document(place_id)
#         # Use update() to update specific fields without overwriting the entire document
#         doc_ref.update({
#             'image_url': firestore.ArrayUnion([image_url]),
#             'g_place_id': g_place_id
#         })
#         print(f"Updated Firestore document for place_id {place_id} with image_url and g_place_id")
#     except NotFound:
#         print(f"Firestore document for place_id {place_id} not found")
#     except Exception as e:
#         print(f"Error updating Firestore document for place_id {place_id}: {e}")

# # Process each place
# def process_place(db, gmaps, place_id):
#     try:
#         # Retrieve the document from Firestore
#         doc_ref = db.collection('explore').document(place_id)
#         doc = doc_ref.get()
#         if not doc.exists:
#             print(f"Document {place_id} does not exist in Firestore.")
#             return
#         data = doc.to_dict()

#         city_name = data.get('city_name')
#         latitude = data.get('latitude')
#         longitude = data.get('longitude')

#         if not city_name:
#             print(f"city_name is missing for document {place_id}")
#             return

#         # Get Google Place ID
#         g_place_id = get_google_place_id(gmaps, city_name, latitude, longitude)
#         if not g_place_id:
#             return

#         # Get photo reference
#         photo_reference = get_photo_reference(gmaps, g_place_id)
#         if not photo_reference:
#             return

#         # Download the photo
#         local_filename = f"{place_id}.jpg"
#         if not download_photo(photo_reference, local_filename):
#             return

#         # Upload to Firebase Storage
#         storage_path = f"places/{place_id}.jpg"
#         image_url = upload_image_to_firebase_storage(local_filename, storage_path)
#         if not image_url:
#             os.remove(local_filename)
#             return

#         # Update Firestore document
#         update_firestore_document(db, place_id, image_url, g_place_id)

#         # Clean up local file
#         # os.remove(local_filename)

#     except Exception as e:
#         print(f"Error processing place_id {place_id}: {e}")

# # Main function
# def main():
#     # Initialize Firebase and Google Maps client
#     db = initialize_firebase()
#     gmaps = initialize_google_maps()

#     # Load place_ids from missing_image_urls.json
#     with open('missing_image_urls.json', 'r', encoding='utf-8') as f:
#         places = json.load(f)

#     print(f"Total places to process: {len(places)}")

#     # Process each place_id
#     for idx, place in enumerate(places):
#         place_id = place.get('place_id')
#         if not place_id:
#             print(f"Invalid place data: {place}")
#             continue
#         print(f"\nProcessing place {idx + 1}/{len(places)}: place_id {place_id}")
#         process_place(db, gmaps, place_id)
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
#     print("Initializing Firebase Admin SDK...")
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'
#     })
#     db = firestore.client()
#     return db

# # Initialize Google Maps Client
# def initialize_google_maps():
#     print("Initializing Google Maps API client...")
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
#             print(f"Found Google Place ID: {place_id} for {city_name}")
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
#         response = gmaps.place(place_id=place_id, fields=['photo'])
#         result = response.get('result')
#         photos = result.get('photos')
#         if photos:
#             photo_reference = photos[0]['photo_reference']
#             print(f"Found photo reference for place_id {place_id}")
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
# def update_firestore_document_with_image_url(db, place_id, image_url, g_place_id):
#     try:
#         doc_ref = db.collection('explore').document(str(place_id))
#         existing_data = doc_ref.get().to_dict()
        
#         if existing_data:
#             # Ensure image_url is a list and append new URLs
#             if 'image_url' in existing_data and isinstance(existing_data['image_url'], list):
#                 image_url_list = existing_data['image_url']
#                 image_url_list.append(image_url)
#             else:
#                 image_url_list = [image_url]
            
#             # Update Firestore document with image_url list and g_place_id
#             doc_ref.update({'image_url': image_url_list, 'g_place_id': g_place_id})
#             print(f"Updated Firestore document for place_id {place_id} with new image_url and g_place_id")
#         else:
#             print(f"Document {place_id} does not exist")
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
#     update_firestore_document_with_image_url(db, place_id, image_url, g_place_id)

#     # Clean up local file
#     os.remove(local_filename)

# # Main function
# def main():
#     # Initialize Firebase and Google Maps client
#     db = initialize_firebase()
#     gmaps = initialize_google_maps()

#     # Get documents from the 'explore' collection where 'image_url' is missing
#     docs = db.collection('explore').where('image_url', '==', None).stream()

#     for idx, doc in enumerate(docs):
#         place_data = doc.to_dict()
#         print(f"Processing place {idx + 1}: {place_data.get('city_name')}")
#         process_place(db, gmaps, place_data)
#         # Respect rate limits
#         time.sleep(1)  # Adjust sleep time as needed to stay within quota limits

# if __name__ == '__main__':
#     main()


# import os
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# from google.api_core.exceptions import NotFound
# import mimetypes

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     print("Initializing Firebase Admin SDK...")
#     # Replace with the path to your Firebase service account JSON file
#     firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'
#     })
#     db = firestore.client()
#     bucket = storage.bucket()
#     return db, bucket

# def main():
#     print("Starting the script...")
#     # Initialize Firebase
#     db, bucket = initialize_firebase()
    
#     # Path to the 'cities' folder
#     cities_folder_path = r'C:\dev\python_runs\scrapy_selenium\quotes-js-project\Cities'
    
#     # Get all documents in 'explore' collection
#     docs = db.collection('explore').get()
#     print(f"Found {len(docs)} documents in the 'explore' collection.")

#     for doc in docs:
#         doc_id = doc.id
#         doc_ref = db.collection('explore').document(doc_id)
#         doc_data = doc.to_dict()
#         print(f"Processing document ID: {doc_id}")
        
#         # Build path to local folder for this document
#         local_folder_path = os.path.join(cities_folder_path, doc_id)
        
#         if os.path.exists(local_folder_path) and os.path.isdir(local_folder_path):
#             print(f"Found local folder for document ID: {doc_id}")
#             # Get list of image files in the folder
#             image_files = [f for f in os.listdir(local_folder_path) if os.path.isfile(os.path.join(local_folder_path, f))]
#             image_urls = []
            
#             for image_file in image_files:
#                 local_image_path = os.path.join(local_folder_path, image_file)
#                 # Upload the image to Firebase Storage
#                 storage_path = f"places/{doc_id}/{image_file}"
#                 try:
#                     blob = bucket.blob(storage_path)
#                     # Set the content type
#                     content_type, _ = mimetypes.guess_type(local_image_path)
#                     if content_type is None:
#                         content_type = 'application/octet-stream'
#                     # Upload the file with the content type
#                     blob.upload_from_filename(local_image_path, content_type=content_type)
#                     # Make the blob publicly accessible
#                     blob.make_public()
#                     image_url = blob.public_url
#                     image_urls.append(image_url)
#                     print(f"Uploaded {local_image_path} to {storage_path}, URL: {image_url}")
#                 except Exception as e:
#                     print(f"Error uploading {local_image_path}: {e}")
            
#             # Retrieve existing 'image_url' field and ensure it's a list
#             existing_image_url = doc_data.get('image_url')
#             if existing_image_url:
#                 if isinstance(existing_image_url, list):
#                     # Append the existing image URLs to the end of the new list
#                     image_urls.extend(existing_image_url)
#                 elif isinstance(existing_image_url, str):
#                     # Append the existing image URL string to the end of the new list
#                     image_urls.append(existing_image_url)
#                 else:
#                     print(f"Unexpected 'image_url' type in document {doc_id}: {type(existing_image_url)}")
#             else:
#                 print(f"No existing 'image_url' field in document {doc_id}")

#             # Update the document with the combined list of image URLs
#             if image_urls:
#                 try:
#                     doc_ref.update({'image_url': image_urls})
#                     print(f"Updated document {doc_id} with image URLs.")
#                 except Exception as e:
#                     print(f"Error updating document {doc_id}: {e}")
#             else:
#                 print(f"No images found to update in document {doc_id}")
#         else:
#             print(f"No folder found for document ID {doc_id} at {local_folder_path}")

# if __name__ == '__main__':
#     main()

# File created of missing places id
# import os
# import json
# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore():
#     # Initialize Firebase Admin SDK
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     return firestore.client()

# def find_documents_missing_image_url():
#     db = initialize_firestore()
#     explore_collection = db.collection('explore')
#     docs = explore_collection.stream()

#     missing_image_url_count = 0
#     total_documents = 0
#     missing_image_urls = []

#     for doc in docs:
#         total_documents += 1
#         data = doc.to_dict()
        
#         # Check if 'image_url' field is missing or empty list
#         if 'image_url' not in data or not data['image_url']:
#             missing_image_url_count += 1
#             missing_image_urls.append({"place_id": doc.id})  # Collect the place_id
#             print(f"Document {doc.id} is missing 'image_url' or it's empty.")

#     # Save the list of missing image URLs as a JSON file
#     output_file_path = os.path.join(os.getcwd(), 'missing_image_urls.json')
#     with open(output_file_path, 'w') as json_file:
#         json.dump(missing_image_urls, json_file, indent=4)

#     print(f"\nTotal documents processed: {total_documents}")
#     print(f"Documents missing 'image_url' or with an empty list: {missing_image_url_count}")
#     print(f"JSON file with missing image URLs created at: {output_file_path}")

# if __name__ == '__main__':
#     find_documents_missing_image_url()



# Script for missing field length


# import os
# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore():
#     # Initialize Firebase Admin SDK
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     return firestore.client()

# def count_documents_missing_image_url():
#     db = initialize_firestore()
#     explore_collection = db.collection('explore')
#     docs = explore_collection.stream()

#     missing_image_url_count = 0
#     total_documents = 0

#     for doc in docs:
#         total_documents += 1
#         data = doc.to_dict()
        
#         # Check if 'image_url' field is missing or empty list
#         if 'image_url' not in data or not data['image_url']:
#             missing_image_url_count += 1
#             print(f"Document {doc.id} is missing 'image_url' or it's empty.")

#     print(f"\nTotal documents processed: {total_documents}")
#     print(f"Documents missing 'image_url' or with an empty list: {missing_image_url_count}")

# if __name__ == '__main__':
#     print("Starting script...")
#     count_documents_missing_image_url()
# import os
# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore():
#     # Initialize Firebase Admin SDK
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     return firestore.client()

# def count_documents_missing_g_place_id():
#     db = initialize_firestore()
#     explore_collection = db.collection('explore')
#     docs = explore_collection.stream()

#     missing_g_place_id_count = 0
#     total_documents = 0

#     for doc in docs:
#         total_documents += 1
#         data = doc.to_dict()
        
#         # Check if 'g_place_id' field is missing or empty string
#         if 'g_place_id' not in data or not data['g_place_id']:
#             missing_g_place_id_count += 1
#             print(f"Document {doc.id} is missing 'g_place_id' or it's empty.")

#     print(f"\nTotal documents processed: {total_documents}")
#     print(f"Documents missing 'g_place_id' or with an empty string: {missing_g_place_id_count}")

# if __name__ == '__main__':
#     print("Starting script...")
#     count_documents_missing_g_place_id()

# def count_documents_missing_image_url():
#     db = initialize_firestore()
#     explore_collection = db.collection('explore')
#     docs = explore_collection.stream()

#     missing_image_url_count = 0
#     total_documents = 0

#     for doc in docs:
#         total_documents += 1
#         data = doc.to_dict()
        
#         # Check if 'image_url' field is missing or empty list
#         if 'image_url' not in data or not data['image_url']:
#             missing_image_url_count += 1
#             print(f"Document {doc.id} is missing 'image_url' or it's empty.")

#     print(f"\nTotal documents processed: {total_documents}")
#     print(f"Documents missing 'image_url' or with an empty list: {missing_image_url_count}")

# if __name__ == '__main__':
#     print("Starting script...")
#     count_documents_missing_image_url()





# import os
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# import time
# import logging

# # Enable debug logging
# logging.basicConfig(level=logging.DEBUG)

# def initialize_firebase():
#     print("Initializing Firebase Admin SDK...")
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'  # Replace with your bucket name
#     })
#     print("Firebase Admin SDK initialized.")

# def get_all_files():
#     print("Accessing Firebase Storage bucket...")
#     bucket = storage.bucket()
#     print("Listing blobs in 'places/' directory...")
#     blobs = bucket.list_blobs(prefix='places/')
#     blobs = list(blobs)
#     print(f"Total blobs found: {len(blobs)}")
#     return blobs

# def make_files_public_and_get_urls(blobs):
#     place_images = {}
#     total_blobs = len(blobs)
#     processed_blobs = 0
#     print("Processing blobs to make them public and generate URLs...")
#     for blob in blobs:
#         # Skip if it's a folder (in Firebase Storage, folders are just prefixes)
#         if blob.name.endswith('/'):
#             continue

#         # Extract the place_id from the file path
#         # The place_id is always the second element after splitting by '/'
#         path_parts = blob.name.split('/')

#         if len(path_parts) >= 2:
#             place_id = path_parts[1]
#         else:
#             print(f"Unexpected file path format: {blob.name}")
#             continue

#         # Make the file publicly accessible
#         try:
#             blob.make_public()
#             download_url = blob.public_url
#         except Exception as e:
#             print(f"Error making blob public: {blob.name}, error: {e}")
#             continue

#         # Add the URL to the list for this place_id
#         if place_id in place_images:
#             place_images[place_id].append(download_url)
#         else:
#             place_images[place_id] = [download_url]

#         processed_blobs += 1
#         if processed_blobs % 100 == 0:
#             print(f"Processed {processed_blobs}/{total_blobs} blobs.")

#     print(f"Finished processing all blobs. Total blobs processed: {processed_blobs}")
#     return place_images

# def update_firestore(place_images):
#     db = firestore.client()
#     batch = db.batch()
#     batch_size = 250  # Firestore allows up to 500 operations per batch
#     counter = 0
#     total_places = len(place_images)
#     processed_places = 0

#     print("Updating Firestore documents...")
#     for place_id, image_urls in place_images.items():
#         doc_ref = db.collection('explore').document(place_id)
#         try:
#             # Print statement to show processing
#             print(f"Updating document {place_id} with {len(image_urls)} image URLs.")

#             # Update the 'image_url' field with the list of image URLs
#             batch.update(doc_ref, {'image_url': image_urls})
#             counter += 1
#             processed_places += 1

#             if counter >= batch_size:
#                 batch.commit()
#                 print(f'Committed batch of {counter} documents.')
#                 batch = db.batch()
#                 counter = 0
#                 time.sleep(1)  # Optional delay to prevent rate limiting

#             # Optional: Print progress
#             if processed_places % 100 == 0:
#                 print(f"Processed {processed_places}/{total_places} places.")

#         except firestore.NotFound:
#             print(f"Document {place_id} not found in Firestore. Skipping.")
#         except Exception as e:
#             print(f"Error updating document {place_id}: {e}")

#     if counter > 0:
#         batch.commit()
#         print(f'Committed final batch of {counter} documents.')

#     print(f'Firestore update complete. Total documents updated: {processed_places}')

# def main():
#     try:
#         print("Starting script...")
#         initialize_firebase()
#         print("Fetching blobs from Firebase Storage...")
#         blobs = get_all_files()
#         print(f"Blobs retrieved: {len(blobs)}")
#         place_images = make_files_public_and_get_urls(blobs)
#         print(f"Total places to update: {len(place_images)}")
#         print("Updating Firestore documents...")
#         update_firestore(place_images)
#         print("Script completed successfully.")
#     except Exception as e:
#         print(f"An error occurred: {e}")

# if __name__ == '__main__':
#     main()



# import os
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# import time

# def initialize_firebase():
#     # Replace with the path to your service account JSON file
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'  # Replace with your bucket name
#     })

# def get_all_files():
#     bucket = storage.bucket()
#     blobs = bucket.list_blobs(prefix='places/')
#     return blobs

# def make_files_public_and_get_urls(blobs):
#     place_images = {}
#     for blob in blobs:
#         # Skip if it's a folder (in Firebase Storage, folders are just prefixes)
#         if blob.name.endswith('/'):
#             continue

#         # Extract the place_id from the file path
#         # Assuming the structure is 'places/{place_id}/filename.jpg' or 'places/{place_id}.jpg'
#         path_parts = blob.name.split('/')
#         if len(path_parts) == 2:
#             # Format: 'places/{place_id}.jpg'
#             place_id_with_ext = path_parts[1]
#             place_id = place_id_with_ext.split('.')[0]
#         elif len(path_parts) == 3:
#             # Format: 'places/{place_id}/filename.jpg'
#             place_id = path_parts[1]
#         else:
#             print(f"Unexpected file path format: {blob.name}")
#             continue

#         # Make the file publicly accessible
#         blob.make_public()
#         download_url = blob.public_url

#         # Add the URL to the list for this place_id
#         if place_id in place_images:
#             place_images[place_id].append(download_url)
#         else:
#             place_images[place_id] = [download_url]

#     return place_images

# def update_firestore(place_images):
#     db = firestore.client()
#     batch = db.batch()
#     batch_size = 250  # Firestore allows up to 500 operations per batch
#     counter = 0
#     total_places = len(place_images)
#     processed_places = 0

#     for place_id, image_urls in place_images.items():
#         doc_ref = db.collection('explore').document(place_id)
#         try:
#             # Print statement to show processing
#             print(f"Updating document {place_id} with {len(image_urls)} image URLs.")

#             # Update the 'image_url' field with the list of image URLs
#             batch.update(doc_ref, {'image_url': image_urls})
#             counter += 1
#             processed_places += 1

#             if counter >= batch_size:
#                 batch.commit()
#                 print(f'Committed batch of {counter} documents.')
#                 batch = db.batch()
#                 counter = 0
#                 time.sleep(1)  # Optional delay to prevent rate limiting

#             # Optional: Print progress
#             if processed_places % 100 == 0:
#                 print(f"Processed {processed_places}/{total_places} places.")

#         except Exception as e:
#             print(f"Error updating document {place_id}: {e}")

#     if counter > 0:
#         batch.commit()
#         print(f'Committed final batch of {counter} documents.')

#     print(f'Firestore update complete. Total documents updated: {processed_places}')

# def main():
#     initialize_firebase()
#     blobs = get_all_files()
#     place_images = make_files_public_and_get_urls(blobs)
#     print(f"Total places to update: {len(place_images)}")
#     update_firestore(place_images)

# if __name__ == '__main__':
#     main()





# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.api_core import retry

# def initialize_firestore():
#     # Replace with the path to your service account JSON file
#     firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     return firestore.client()

# def merge_documents(source_doc_ref, dest_doc_ref):
#     try:
#         source_doc = source_doc_ref.get()
#         if not source_doc.exists:
#             print(f"Source document {source_doc_ref.id} does not exist. Skipping.")
#             return

#         source_data = source_doc.to_dict()

#         # Attempt to update the destination document
#         try:
#             dest_doc_ref.update(source_data)
#             print(f"Updated document {dest_doc_ref.id} with missing fields.")
#         except firestore.NotFound:
#             # Destination document does not exist; create it
#             dest_doc_ref.set(source_data)
#             print(f"Destination document {dest_doc_ref.id} did not exist. Created new document.")

#     except Exception as e:
#         print(f"Error processing document {source_doc_ref.id}: {e}")

# def main():
#     db = initialize_firestore()
#     source_collection = db.collection('places')
#     dest_collection = db.collection('explore')

#     batch_size = 500  # Adjust as needed
#     last_doc = None

#     # Set up retry and timeout
#     retry_policy = retry.Retry(
#         initial=1.0,  # seconds (initial retry delay)
#         maximum=60.0,  # seconds (maximum retry delay)
#         multiplier=2.0,
#         deadline=300.0  # seconds (total time to retry)
#     )
#     timeout = 60.0  # seconds

#     while True:
#         # Build the query with pagination
#         query = source_collection.limit(batch_size)
#         if last_doc:
#             query = query.start_after(last_doc)

#         # Fetch documents with retry and timeout
#         try:
#             docs = list(query.stream(retry=retry_policy, timeout=timeout))
#         except Exception as e:
#             print(f"Error fetching documents: {e}")
#             break

#         if not docs:
#             break

#         for doc in docs:
#             doc_id = doc.id
#             source_doc_ref = source_collection.document(doc_id)
#             dest_doc_ref = dest_collection.document(doc_id)
#             merge_documents(source_doc_ref, dest_doc_ref)

#         # Set the last_doc to the last document in the current batch
#         last_doc = docs[-1]

# if __name__ == '__main__':
#     main()


# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore():
#     # Replace 'path/to/serviceAccountKey.json' with the path to your service account JSON file
#     firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     return firestore.client()

# def merge_documents(source_doc_ref, dest_doc_ref):
#     try:
#         source_doc = source_doc_ref.get()
#         if not source_doc.exists:
#             print(f"Source document {source_doc_ref.id} does not exist. Skipping.")
#             return

#         source_data = source_doc.to_dict()

#         # Attempt to update the destination document
#         dest_doc_ref.update(source_data)
#         print(f"Updated document {dest_doc_ref.id} with missing fields.")

#     except firestore.NotFound:
#         # Destination document does not exist; create it
#         dest_doc_ref.set(source_data)
#         print(f"Destination document {dest_doc_ref.id} did not exist. Created new document.")

#     except Exception as e:
#         print(f"Error updating document {dest_doc_ref.id}: {e}")

# def main():
#     db = initialize_firestore()
#     source_collection = db.collection('places')
#     dest_collection = db.collection('explore')

#     # Optional: Limit the number of documents for testing
#     # docs = source_collection.limit(5).stream()
#     docs = source_collection.stream()

#     for doc in docs:
#         doc_id = doc.id
#         source_doc_ref = source_collection.document(doc_id)
#         dest_doc_ref = dest_collection.document(doc_id)
#         merge_documents(source_doc_ref, dest_doc_ref)

# if __name__ == '__main__':
#     main()



# Script which deleted my explore collection data
# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.api_core import retry

# def initialize_firestore():
#     firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     return firestore.client()

# def copy_collection(source_ref, dest_ref):
#     timeout = 120  # Timeout in seconds
#     retry_policy = retry.Retry(deadline=300)

#     batch_size = 500
#     docs = source_ref.limit(batch_size).stream(retry=retry_policy, timeout=timeout)

#     while True:
#         doc_list = list(docs)
#         if not doc_list:
#             break

#         for doc in doc_list:
#             # Copy the document data
#             doc_ref = dest_ref.document(doc.id)
#             doc_ref.set(doc.to_dict())
#             print(f'Copied document {doc.id}')

#             # Get all subcollections of the document
#             subcollections = doc.reference.collections()
#             for subcol in subcollections:
#                 subcol_name = subcol.id
#                 print(f'Found subcollection {subcol_name} in document {doc.id}')

#                 # Recursively copy the subcollection
#                 copy_collection(
#                     source_ref=doc.reference.collection(subcol_name),
#                     dest_ref=doc_ref.collection(subcol_name)
#                 )

#         # Get the last document and prepare for the next batch
#         last_doc = doc_list[-1]
#         docs = source_ref.limit(batch_size).start_after(last_doc).stream(retry=retry_policy, timeout=timeout)

# def main():
#     db = initialize_firestore()

#     source_collection = db.collection('exploreData')
#     dest_collection = db.collection('explore')

#     # Specify the subcollections to copy
#     subcollections_to_copy = ['TouristAttractions', 'TouristRestaurants']

#     timeout = 120  # Timeout in seconds
#     retry_policy = retry.Retry(deadline=300)

#     batch_size = 500
#     docs = source_collection.limit(batch_size).stream(retry=retry_policy, timeout=timeout)

#     while True:
#         doc_list = list(docs)
#         if not doc_list:
#             break

#         for doc in doc_list:
#             doc_id = doc.id
#             print(f'Processing document {doc_id}')

#             # Copy the document data
#             dest_doc_ref = dest_collection.document(doc_id)
#             dest_doc_ref.set(doc.to_dict())
#             print(f'Copied document {doc_id} to explore')

#             for subcol_name in subcollections_to_copy:
#                 source_subcol_ref = doc.reference.collection(subcol_name)
#                 dest_subcol_ref = dest_doc_ref.collection(subcol_name)

#                 subcol_docs = source_subcol_ref.limit(batch_size).stream(retry=retry_policy, timeout=timeout)
#                 subcol_doc_list = list(subcol_docs)
#                 if not subcol_doc_list:
#                     print(f'No documents found in subcollection {subcol_name} for document {doc_id}')
#                     continue

#                 print(f'Copying subcollection {subcol_name} for document {doc_id}')
#                 copy_collection(source_subcol_ref, dest_subcol_ref)

#         # Get the last document and prepare for the next batch
#         last_doc = doc_list[-1]
#         docs = source_collection.limit(batch_size).start_after(last_doc).stream(retry=retry_policy, timeout=timeout)

# if __name__ == '__main__':
#     main()



# import os
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# from google.api_core.exceptions import NotFound
# import mimetypes

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     # Replace with the path to your Firebase service account JSON file
#     firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'
#     })
#     db = firestore.client()
#     bucket = storage.bucket()
#     return db, bucket

# def main():
#     # Initialize Firebase
#     db, bucket = initialize_firebase()
    
#     # Path to the 'cities' folder
#     cities_folder_path = r'C:\dev\python_runs\scrapy_selenium\quotes-js-project\Cities'
    
#     # Get all documents in 'explore' collection
#     docs = db.collection('explore').get()
    
#     for doc in docs:
#         doc_id = doc.id
#         doc_ref = db.collection('explore').document(doc_id)
#         doc_data = doc.to_dict()
#         print(f"Processing document ID: {doc_id}")
        
#         # Build path to local folder for this document
#         local_folder_path = os.path.join(cities_folder_path, doc_id)
        
#         if os.path.exists(local_folder_path) and os.path.isdir(local_folder_path):
#             # Get list of image files in the folder
#             image_files = [f for f in os.listdir(local_folder_path) if os.path.isfile(os.path.join(local_folder_path, f))]
#             image_urls = []
            
#             for image_file in image_files:
#                 local_image_path = os.path.join(local_folder_path, image_file)
#                 # Upload the image to Firebase Storage
#                 storage_path = f"places/{doc_id}/{image_file}"
#                 try:
#                     blob = bucket.blob(storage_path)
#                     # Set the content type
#                     content_type, _ = mimetypes.guess_type(local_image_path)
#                     if content_type is None:
#                         content_type = 'application/octet-stream'
#                     # Upload the file with the content type
#                     blob.upload_from_filename(local_image_path, content_type=content_type)
#                     # Make the blob publicly accessible
#                     blob.make_public()
#                     image_url = blob.public_url
#                     image_urls.append(image_url)
#                     print(f"Uploaded {local_image_path} to {storage_path}, URL: {image_url}")
#                 except Exception as e:
#                     print(f"Error uploading {local_image_path}: {e}")
#             # Retrieve existing 'image_url' field and ensure it's a list
#             existing_image_url = doc_data.get('image_url')
#             if existing_image_url:
#                 if isinstance(existing_image_url, list):
#                     # Append the existing image URLs to the end of the new list
#                     image_urls.extend(existing_image_url)
#                 elif isinstance(existing_image_url, str):
#                     # Append the existing image URL string to the end of the new list
#                     image_urls.append(existing_image_url)
#                 else:
#                     print(f"Unexpected 'image_url' type in document {doc_id}: {type(existing_image_url)}")
#             else:
#                 print(f"No existing 'image_url' field in document {doc_id}")

#             # Update the document with the combined list of image URLs
#             if image_urls:
#                 try:
#                     doc_ref.update({'image_url': image_urls})
#                     print(f"Updated document {doc_id} with image URLs")
#                 except Exception as e:
#                     print(f"Error updating document {doc_id}: {e}")
#             else:
#                 print(f"No images found to update in document {doc_id}")
#         else:
#             print(f"No folder found for document ID {doc_id} at {local_folder_path}")

# if __name__ == '__main__':
#     main()

# import os
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# from google.api_core.exceptions import NotFound
# import mimetypes

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     # Replace with the path to your Firebase service account JSON file
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     # Replace 'your-project-id' with your actual Firebase project ID
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'
#     })
#     db = firestore.client()
#     bucket = storage.bucket()
#     return db, bucket

# def main():
#     # Initialize Firebase
#     db, bucket = initialize_firebase()
    
#     # Path to the 'cities' folder
#     cities_folder_path = r'C:\dev\python_runs\scrapy_selenium\quotes-js-project\Cities'
    
#     # Get all documents in 'explore' collection
#     docs = db.collection('explore').get()
    
#     for doc in docs:
#         doc_id = doc.id
#         doc_ref = db.collection('explore').document(doc_id)
#         print(f"Processing document ID: {doc_id}")
        
#         # Build path to local folder for this document
#         local_folder_path = os.path.join(cities_folder_path, doc_id)
        
#         if os.path.exists(local_folder_path) and os.path.isdir(local_folder_path):
#             # Get list of image files in the folder
#             image_files = [f for f in os.listdir(local_folder_path) if os.path.isfile(os.path.join(local_folder_path, f))]
#             image_urls = []
            
#             for image_file in image_files:
#                 local_image_path = os.path.join(local_folder_path, image_file)
#                 # Upload the image to Firebase Storage
#                 storage_path = f"places/{doc_id}/{image_file}"
#                 try:
#                     blob = bucket.blob(storage_path)
#                     # Set the content type
#                     content_type, _ = mimetypes.guess_type(local_image_path)
#                     if content_type is None:
#                         content_type = 'application/octet-stream'
#                     # Upload the file with the content type
#                     blob.upload_from_filename(local_image_path, content_type=content_type)
#                     # Make the blob publicly accessible
#                     blob.make_public()
#                     image_url = blob.public_url
#                     image_urls.append(image_url)
#                     print(f"Uploaded {local_image_path} to {storage_path}, URL: {image_url}")
#                 except Exception as e:
#                     print(f"Error uploading {local_image_path}: {e}")
#             # Update the document with the list of image URLs
#             if image_urls:
#                 try:
#                     doc_ref.update({'image_url': image_urls})
#                     print(f"Updated document {doc_id} with image URLs")
#                 except Exception as e:
#                     print(f"Error updating document {doc_id}: {e}")
#             else:
#                 print(f"No images found in folder {local_folder_path}")
#         else:
#             print(f"No folder found for document ID {doc_id} at {local_folder_path}")

# if __name__ == '__main__':
#     main()





# Changing the structure of image_url field to list
# import os
# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.api_core.exceptions import NotFound

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     # Replace with the path to your Firebase service account JSON file
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     db = firestore.client()
#     return db

# def main():
#     # Initialize Firebase
#     db = initialize_firebase()

#     # Get all documents in 'explore' collection
#     docs = db.collection('explore').get()

#     for doc in docs:
#         doc_id = doc.id
#         doc_ref = db.collection('explore').document(doc_id)
#         doc_data = doc.to_dict()

#         # Check if 'image_url' field exists and is a string
#         image_url = doc_data.get('image_url')
#         if isinstance(image_url, str):
#             # Update 'image_url' field to be a list containing the string
#             try:
#                 doc_ref.update({'image_url': [image_url]})
#                 print(f"Updated document {doc_id}: 'image_url' field is now a list")
#             except Exception as e:
#                 print(f"Error updating document {doc_id}: {e}")
#         else:
#             print(f"Document {doc_id}: 'image_url' field is already a list or not present")

# if __name__ == '__main__':
#     main()



# updating structure of content of plans
# import os
# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.api_core.exceptions import NotFound

# # Initialize Firebase Admin SDK
# def initialize_firebase():
#     # Replace with the path to your Firebase service account JSON file
#     firebase_credentials_path = os.path.join(
#         os.getcwd(),
#         r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     )
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     db = firestore.client()
#     return db

# def main():
#     # Initialize Firebase
#     db = initialize_firebase()

#     # Get all documents in 'plans' collection
#     docs = db.collection('plans').get()

#     for doc in docs:
#         doc_id = doc.id
#         doc_ref = db.collection('plans').document(doc_id)
#         doc_data = doc.to_dict()

#         # Check if 'content' field exists and is a string
#         content = doc_data.get('content')
#         if isinstance(content, str):
#             # Update 'content' field to be a list containing the string
#             try:
#                 doc_ref.update({'content': [content]})
#                 print(f"Updated document {doc_id}: 'content' field is now a list")
#             except Exception as e:
#                 print(f"Error updating document {doc_id}: {e}")
#         else:
#             print(f"Document {doc_id}: 'content' field is already a list or not present")

# if __name__ == '__main__':
#     main()
