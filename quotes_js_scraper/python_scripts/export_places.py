import firebase_admin
from firebase_admin import credentials, firestore
import json
import os

def initialize_firestore():
    # Replace this path with the path to your Firebase service account JSON file
    firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
    cred = credentials.Certificate(firebase_credentials_path)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    return db

def get_all_places(db):
    places_ref = db.collection('allplaces')
    docs = places_ref.stream()
    places_list = []

    for doc in docs:
        data = doc.to_dict()
        place_id = data.get('place_id') 
        place_name = data.get('city_name') 
        if place_id and place_name:
            places_list.append({
                'placeId': place_id,
                'placeName': place_name
            })
        else:
            print(f"Missing place_id or place_name in document {doc.id}")

    return places_list

def save_to_json(data, filename='places_list.json'):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Data saved to {filename}")

def main():
    db = initialize_firestore()
    places_list = get_all_places(db)
    save_to_json(places_list)

if __name__ == '__main__':
    main()
