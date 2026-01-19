import firebase_admin
from firebase_admin import credentials, firestore
import json
import os

# Initialize Firebase Admin SDK
firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
cred = credentials.Certificate(firebase_credentials_path)
firebase_admin.initialize_app(cred)

# Firestore database instance
db = firestore.client()

def fetch_selected_fields_from_firestore():
    # Reference to your Firestore collection
    collection_ref = db.collection('explore')

    # Fetch all documents in the collection
    docs = collection_ref.stream()

    places_data = []
    for doc in docs:
        place = doc.to_dict()

        # Extract specific fields: city_name, place_id, latitude, and longitude
        place_data = {
            'city_name': place.get('city_name'),
            'place_id': place.get('place_id'),
            'latitude': place.get('latitude'),
            'longitude': place.get('longitude')
        }

        # Append the selected fields to the list
        places_data.append(place_data)

    return places_data

def save_to_json(data, filename='explore_places_data.json'):
    """Save data to a JSON file."""
    output_file = os.path.join(os.getcwd(), filename)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Saved {len(data)} places to {filename}")

if __name__ == "__main__":
    # Fetch selected fields from Firestore
    places_data = fetch_selected_fields_from_firestore()

    # Save selected fields to a JSON file
    save_to_json(places_data)
