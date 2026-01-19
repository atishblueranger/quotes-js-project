import firebase_admin
from firebase_admin import credentials, firestore
import geohash2  # Make sure geohash2 is installed
import os

def initialize_firestore():
    # Replace with the path to your service account JSON file
    firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
    cred = credentials.Certificate(firebase_credentials_path)
    firebase_admin.initialize_app(cred)
    return firestore.client()

def update_documents_with_geohash_and_geopoint():
    db = initialize_firestore()

    # Reference your collection
    places_collection = db.collection('explore')

    # Set batch settings
    batch_size = 500  # Firestore batch limit is 500
    batch = db.batch()
    batch_counter = 0

    docs = places_collection.stream()

    for doc in docs:
        data = doc.to_dict()
        latitude = data.get('latitude')
        longitude = data.get('longitude')

        # Check if latitude and longitude are numeric
        if isinstance(latitude, (int, float)) and isinstance(longitude, (int, float)):
            try:
                # Generate geohash using geohash2 library
                geohash = geohash2.encode(latitude, longitude, precision=9)  # Higher precision

                # Prepare geopoint and geohash to add to Firestore
                position = {
                    'geopoint': firestore.GeoPoint(latitude, longitude),
                    'geohash': geohash
                }

                doc_ref = doc.reference
                batch.update(doc_ref, {'position': position})
                batch_counter += 1

                # Commit the batch when it reaches the batch size limit
                if batch_counter == batch_size:
                    batch.commit()
                    print(f'Committed a batch of {batch_size} updates.')
                    batch = db.batch()
                    batch_counter = 0
            except Exception as e:
                print(f"Error processing document {doc.id}: {e}")
        else:
            print(f"Skipping document {doc.id}: 'latitude' or 'longitude' is not numeric.")

    # Commit any remaining updates
    if batch_counter > 0:
        batch.commit()
        print(f'Committed the final batch of {batch_counter} updates.')

    print('All documents updated with position and geohash fields.')

if __name__ == '__main__':
    update_documents_with_geohash_and_geopoint()


# import os
# import firebase_admin
# from firebase_admin import credentials, firestore, _apps
# import geohash2  # Assuming you install geohash2

# def update_documents_with_geohash_and_geopoint():
#     # Initialize Firebase Admin SDK if not already initialized
#     if not firebase_admin._apps:
#         firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#     else:
#         print("Firebase app already initialized.")
    
#     db = firestore.client()

#     # Reference your collection (replace 'explore' with your collection name if different)
#     places_collection = db.collection('explore')
#     docs = places_collection.stream()

#     batch = db.batch()
#     batch_counter = 0
#     batch_size = 500

#     for doc in docs:
#         data = doc.to_dict()
#         latitude = data.get('latitude')
#         longitude = data.get('longitude')

#         # Check if latitude and longitude are numeric
#         if isinstance(latitude, (int, float)) and isinstance(longitude, (int, float)):
#             # Generate geohash using geohash2 library
#             geohash = geohash2.encode(latitude, longitude, precision=9)  # precision=9 for higher accuracy
            
#             # Prepare geopoint and geohash to add to Firestore
#             position = {
#                 'geopoint': firestore.GeoPoint(latitude, longitude),
#                 'geohash': geohash
#             }
#             doc_ref = doc.reference
#             batch.update(doc_ref, {'position': position})
#             batch_counter += 1

#             if batch_counter == batch_size:
#                 batch.commit()
#                 batch = db.batch()
#                 batch_counter = 0
#         else:
#             print(f"Skipping document {doc.id}: latitude or longitude is not numeric.")

#     if batch_counter > 0:
#         batch.commit()

#     print('All documents updated with position and geohash fields.')

# if __name__ == '__main__':
#     update_documents_with_geohash_and_geopoint()


# import os
# import firebase_admin
# from firebase_admin import credentials, firestore, _apps

# def update_documents_with_position():
#     # Initialize Firebase Admin SDK if not already initialized
#     if not firebase_admin._apps:
#         firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#     else:
#         print("Firebase app already initialized.")
    
#     db = firestore.client()

#     # Reference your collection (replace 'explore' with your collection name if different)
#     places_collection = db.collection('explore')
#     docs = places_collection.stream()

#     batch = db.batch()
#     batch_counter = 0
#     batch_size = 500

#     for doc in docs:
#         data = doc.to_dict()
#         latitude = data.get('latitude')
#         longitude = data.get('longitude')

#         # Check if latitude and longitude are numeric
#         if isinstance(latitude, (int, float)) and isinstance(longitude, (int, float)):
#             position = {'_latitude':latitude,'_longitude':longitude}
#             doc_ref = doc.reference
#             batch.update(doc_ref, {'position': position})
#             batch_counter += 1

#             if batch_counter == batch_size:
#                 batch.commit()
#                 batch = db.batch()
#                 batch_counter = 0
#         else:
#             print(f"Skipping document {doc.id}: latitude or longitude is not numeric.")

#     if batch_counter > 0:
#         batch.commit()

#     print('All documents updated with position field.')

# if __name__ == '__main__':
#     update_documents_with_position()


# import os
# import firebase_admin
# from firebase_admin import credentials, firestore

# def parse_coordinate(coord_str):
#     """
#     Parses a coordinate string like '35.680565° N' and returns a float.
#     """
#     try:
#         coord_str = coord_str.strip()
#         # Remove the degree symbol and any whitespace
#         coord_str = coord_str.replace('°', '').strip()
#         # Split into value and direction
#         parts = coord_str.split()
#         if len(parts) != 2:
#             return None
#         value_str, direction = parts
#         value = float(value_str)
#         if direction.upper() in ['S', 'W']:
#             value = -value
#         return value
#     except Exception as e:
#         print(f"Error parsing coordinate '{coord_str}': {e}")
#         return None

# def update_documents_with_position():
#     # Initialize the Firebase Admin SDK
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     db = firestore.client()

#     places_collection = db.collection('explore')
#     docs = places_collection.stream()

#     batch = db.batch()
#     batch_counter = 0
#     batch_size = 500

#     for doc in docs:
#         data = doc.to_dict()
#         latitude = data.get('latitude')
#         longitude = data.get('longitude')

#         # Parse the coordinate strings
#         if isinstance(latitude, str):
#             latitude = parse_coordinate(latitude)
#         if isinstance(longitude, str):
#             longitude = parse_coordinate(longitude)

#         if latitude is not None and longitude is not None:
#             position = firestore.GeoPoint(latitude, longitude)
#             doc_ref = doc.reference
#             batch.update(doc_ref, {'position': position})
#             batch_counter += 1

#             if batch_counter == batch_size:
#                 batch.commit()
#                 batch = db.batch()
#                 batch_counter = 0

#     if batch_counter > 0:
#         batch.commit()

#     print('All documents updated with position field.')

# if __name__ == '__main__':
    # update_documents_with_position()



# import os
# import firebase_admin
# from firebase_admin import credentials, firestore

# def parse_coordinate(coord_str):
#     """
#     Parses a coordinate string like '35.680565° N' and returns a float.
#     """
#     if not coord_str:
#         return None
#     coord_str = coord_str.strip().replace('°', '')  # Remove the degree symbol
#     parts = coord_str.split()
#     if len(parts) != 2:
#         return None
#     value_str, direction = parts
#     try:
#         value = float(value_str)
#     except ValueError:
#         return None
#     direction = direction.upper()
#     if direction in ['S', 'W']:
#         value = -value
#     elif direction not in ['N', 'E']:
#         return None  # Invalid direction
#     return value

# def update_documents_with_position():
#     # Initialize Firebase Admin SDK
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     db = firestore.client()

#     places_collection = db.collection('explore')
#     docs = places_collection.stream()

#     batch = db.batch()
#     batch_counter = 0
#     batch_size = 500

#     for doc in docs:
#         data = doc.to_dict()
#         latitude_str = data.get('latitude')
#         longitude_str = data.get('longitude')

#         latitude = parse_coordinate(latitude_str)
#         longitude = parse_coordinate(longitude_str)

#         if latitude is not None and longitude is not None:
#             position = firestore.GeoPoint(latitude, longitude)
#             doc_ref = doc.reference
#             batch.update(doc_ref, {'position': position})
#             batch_counter += 1

#             if batch_counter == batch_size:
#                 batch.commit()
#                 batch = db.batch()
#                 batch_counter = 0
#         else:
#             print(f"Invalid or missing coordinates in document {doc.id}")

#     if batch_counter > 0:
#         batch.commit()

#     print('All documents updated with position field.')

# if __name__ == '__main__':
#     update_documents_with_position()
# # import os



# import os
# import firebase_admin
# from firebase_admin import credentials, firestore

# def update_documents_with_position():
#     # Initialize Firebase Admin SDK
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     db = firestore.client()

#     places_collection = db.collection('explore')
#     docs = places_collection.stream()

#     batch = db.batch()
#     batch_counter = 0
#     batch_size = 500

#     for doc in docs:
#         data = doc.to_dict()
#         latitude = data.get('latitude')
#         longitude = data.get('longitude')

#         if latitude and longitude:
#             position = firestore.GeoPoint(latitude, longitude)
#             doc_ref = doc.reference
#             batch.update(doc_ref, {'position': position})
#             batch_counter += 1

#             if batch_counter == batch_size:
#                 batch.commit()
#                 batch = db.batch()
#                 batch_counter = 0

#     if batch_counter > 0:
#         batch.commit()

#     print('All documents updated with position field.')

# if __name__ == '__main__':
#     update_documents_with_position()
