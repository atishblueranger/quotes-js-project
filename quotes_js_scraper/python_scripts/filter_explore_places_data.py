import json
import os
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase Admin SDK
def initialize_firebase():
    firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
    cred = credentials.Certificate(firebase_credentials_path)  # Replace with your Firebase service account JSON file
    firebase_admin.initialize_app(cred)
    db = firestore.client()  # Firestore client
    return db

# Get all places from Firestore that don't have image_url or g_place_id
def get_unprocessed_places(db):
    unprocessed_place_ids = []

    # Query all documents in the 'explore' collection
    docs = db.collection('explore').stream()

    for doc in docs:
        data = doc.to_dict()

        # Check if either 'image_url' or 'g_place_id' is missing
        if 'image_url' not in data or 'g_place_id' not in data:
            unprocessed_place_ids.append(data.get('place_id'))

    return unprocessed_place_ids

# Filter the local explore_places_data.json to only include unprocessed places
def filter_unprocessed_places(input_file, output_file, unprocessed_place_ids):
    # Read the local places file
    with open(input_file, 'r', encoding='utf-8') as f:
        places = json.load(f)

    # Filter the places that are in the unprocessed list
    filtered_places = [place for place in places if place['place_id'] in unprocessed_place_ids]

    # Write the filtered places to a new file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_places, f, indent=4)

    print(f"Filtered places without image_url and g_place_id. New file created: {output_file}")

def main():
    # Initialize Firestore
    db = initialize_firebase()

    # Get list of unprocessed places (those missing 'image_url' or 'g_place_id')
    unprocessed_place_ids = get_unprocessed_places(db)

    print(f"Found {len(unprocessed_place_ids)} unprocessed places.")

    # Filter the local explore_places_data.json file based on unprocessed place_ids
    input_file = 'explore_places_data.json'
    output_file = 'filtered_unprocessed_places.json'
    filter_unprocessed_places(input_file, output_file, unprocessed_place_ids)

if __name__ == "__main__":
    main()






# import json

# # Load the explore_places_data.json file
# input_file = 'explore_places_data.json'
# output_file = 'filtered_explore_places_data.json'

# # Number of places to skip
# skip_count = 174

# # Read the existing data
# with open(input_file, 'r', encoding='utf-8') as f:
#     places = json.load(f)

# # Filter out the first 174 places
# filtered_places = places[skip_count:]

# # Write the remaining places to a new JSON file
# with open(output_file, 'w', encoding='utf-8') as f:
#     json.dump(filtered_places, f, indent=4)

# print(f"Filtered out the first {skip_count} places. New file created: {output_file}")
