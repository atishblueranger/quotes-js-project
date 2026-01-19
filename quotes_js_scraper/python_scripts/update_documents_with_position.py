



# Script for collection "allplaces" becoz of large size
import firebase_admin
from firebase_admin import credentials, firestore
import geohash2  # Make sure geohash2 is installed
# import os # os module was not used - removed

def initialize_firestore():
    """Initializes the Firestore client."""
    # Check if Firebase app is already initialized to prevent re-initialization error
    if not firebase_admin._apps:
        # Replace with the actual path to your service account JSON file
        firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
        cred = credentials.Certificate(firebase_credentials_path)
        firebase_admin.initialize_app(cred)
        print("Firebase Admin SDK initialized.")
    else:
        print("Firebase Admin SDK already initialized.")
    return firestore.client()

def update_documents_with_geohash_and_geopoint():
    """Fetches documents in pages, updates them with geohash/geopoint using batches."""
    db = initialize_firestore()
    places_collection = db.collection('allplaces') # Reference your collection

    write_batch_size = 500  # Firestore batch write limit (max 500)
    fetch_page_size = 1000 # How many documents to fetch from Firestore at a time (adjust as needed)

    write_batch = db.batch()
    write_batch_counter = 0
    total_processed = 0
    total_updated = 0
    last_doc_snapshot = None # To keep track of the last document for pagination

    while True:
        print("-" * 20)
        print(f"Fetching next page of up to {fetch_page_size} documents...")
        
        # Construct the query for the current page
        if last_doc_snapshot:
            # Query starting after the last document of the previous page
            # Use "__name__" to order by document ID for reliable pagination
            query = places_collection.order_by("__name__").start_after(last_doc_snapshot).limit(fetch_page_size)
        else:
            # Initial query for the first page
            # Use "__name__" to order by document ID
            query = places_collection.order_by("__name__").limit(fetch_page_size)

        # Execute the query and get the documents for this page
        try:
            docs_in_page = list(query.stream())
        except Exception as e:
            print(f"\n!!! Error fetching documents: {e}")
            print("This might be a transient network issue or permissions problem.")
            print("Stopping script. Consider retrying later.")
            # Optionally, commit any pending writes before exiting
            if write_batch_counter > 0:
                 print(f"Committing pending batch of {write_batch_counter} updates before exiting due to fetch error...")
                 try:
                     write_batch.commit()
                     print("Pending batch committed successfully.")
                 except Exception as commit_e:
                     print(f"!!! Error committing pending batch: {commit_e}")
            break # Exit the loop on fetch error


        if not docs_in_page:
            print("No more documents found. Exiting loop.")
            break # Exit the loop if no documents are returned

        current_page_fetched = len(docs_in_page)
        print(f"Fetched {current_page_fetched} documents for this page.")

        page_processed_count = 0
        for doc in docs_in_page:
            page_processed_count += 1
            total_processed += 1
            data = doc.to_dict()
            latitude = data.get('latitude')
            longitude = data.get('longitude')

            # Optional: Check if the 'position' field *with valid subfields* already exists to avoid reprocessing
            # position_data = data.get('position')
            # if isinstance(position_data, dict) and 'geohash' in position_data and 'geopoint' in position_data:
            #     # print(f"Skipping document {doc.id}: 'position' field already exists and seems valid.")
            #     continue # Skip to the next document

            if isinstance(latitude, (int, float)) and isinstance(longitude, (int, float)):
                try:
                    # Generate geohash using geohash2 library
                    geohash = geohash2.encode(latitude, longitude, precision=9) # Using precision 9

                    # Prepare geopoint and geohash to add to Firestore
                    position = {
                        'geopoint': firestore.GeoPoint(latitude, longitude),
                        'geohash': geohash
                    }

                    doc_ref = doc.reference
                    write_batch.update(doc_ref, {'position': position})
                    write_batch_counter += 1
                    total_updated += 1

                    # Commit the write batch when it reaches the size limit
                    if write_batch_counter == write_batch_size:
                        try:
                            write_batch.commit()
                            print(f'---> Committed write batch of {write_batch_size} updates. Total updated so far: {total_updated}')
                            write_batch = db.batch() # Start a new batch
                            write_batch_counter = 0
                        except Exception as commit_e:
                             print(f"\n!!! Error committing batch: {commit_e}")
                             print("Stopping script. Some updates may not have been saved.")
                             # Decide if you want to break the loop here or try to continue
                             break # Exit the outer loop on commit error


                except Exception as e:
                    print(f"Error processing document {doc.id} (Lat: {latitude}, Lon: {longitude}): {e}")
            else:
                # Only print if you really need to know about every skipped doc
                # print(f"Skipping document {doc.id}: 'latitude' or 'longitude' is not numeric or missing.")
                pass # Or handle non-numeric cases if needed

            # Update the cursor for the next iteration's start_after()
            last_doc_snapshot = doc
        
        # Break from outer loop if a commit error happened inside the inner loop
        if write_batch_counter == write_batch_size: # This condition checks if the loop exited due to commit error
            pass # Error message was already printed
        elif page_processed_count < current_page_fetched: # This implies an error during processing stopped the inner loop
             print("Exited page processing loop early due to an error.")


        print(f"Finished processing page. Documents processed in page: {page_processed_count}. Total processed overall: {total_processed}")
        # Optional: Add a small delay between pages if needed, though usually not necessary for Firestore
        # import time
        # time.sleep(0.5)

    # --- End of while loop ---

    # Commit any remaining updates in the final batch
    if write_batch_counter > 0:
        print(f'Committing the final write batch of {write_batch_counter} updates...')
        try:
            write_batch.commit()
            print("Final batch committed successfully.")
        except Exception as final_commit_e:
            print(f"!!! Error committing final batch: {final_commit_e}")


    print('=' * 20)
    print(f'Script finished.')
    print(f'Total documents processed (read): {total_processed}.')
    print(f'Total documents updated (written): {total_updated}.')


if __name__ == '__main__':
    update_documents_with_geohash_and_geopoint()
# import firebase_admin
# from firebase_admin import credentials, firestore
# import geohash2  # Make sure geohash2 is installed
# import os

# def initialize_firestore():
#     # Replace with the path to your service account JSON file
#     firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     return firestore.client()

# def update_documents_with_geohash_and_geopoint():
#     db = initialize_firestore()

#     # Reference your collection
#     places_collection = db.collection('allplaces')

#     # Set batch settings
#     batch_size = 500  # Firestore batch limit is 500
#     batch = db.batch()
#     batch_counter = 0

#     docs = places_collection.stream()

#     for doc in docs:
#         data = doc.to_dict()
#         latitude = data.get('latitude')
#         longitude = data.get('longitude')

#         # Check if latitude and longitude are numeric
#         if isinstance(latitude, (int, float)) and isinstance(longitude, (int, float)):
#             try:
#                 # Generate geohash using geohash2 library
#                 geohash = geohash2.encode(latitude, longitude, precision=9)  # Higher precision

#                 # Prepare geopoint and geohash to add to Firestore
#                 position = {
#                     'geopoint': firestore.GeoPoint(latitude, longitude),
#                     'geohash': geohash
#                 }

#                 doc_ref = doc.reference
#                 batch.update(doc_ref, {'position': position})
#                 batch_counter += 1

#                 # Commit the batch when it reaches the batch size limit
#                 if batch_counter == batch_size:
#                     batch.commit()
#                     print(f'Committed a batch of {batch_size} updates.')
#                     batch = db.batch()
#                     batch_counter = 0
#             except Exception as e:
#                 print(f"Error processing document {doc.id}: {e}")
#         else:
#             print(f"Skipping document {doc.id}: 'latitude' or 'longitude' is not numeric.")

#     # Commit any remaining updates
#     if batch_counter > 0:
#         batch.commit()
#         print(f'Committed the final batch of {batch_counter} updates.')

#     print('All documents updated with position and geohash fields.')

# if __name__ == '__main__':
#     update_documents_with_geohash_and_geopoint()


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
