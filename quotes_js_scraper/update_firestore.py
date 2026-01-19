import firebase_admin
from firebase_admin import credentials, firestore
import os

def update_all_documents():
    # 1. Initialize the Firebase Admin SDK once.
    firebase_credentials_path = os.path.join(
        os.getcwd(), 
        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
    )
    cred = credentials.Certificate(firebase_credentials_path)
    firebase_admin.initialize_app(cred)

    db = firestore.client()
    places_collection = db.collection('allplaces')
    
    # 2. Pagination settings
    page_size = 500  # How many docs to fetch in each batch
    last_doc = None  # We'll store the last doc reference here to do startAfter
    updated_count = 0

    while True:
        # 3. Build the query for the next page
        query = places_collection.order_by('__name__').limit(page_size)
        
        # If we have a last_doc from the previous iteration, start after it
        if last_doc:
            query = query.start_after({'__name__': last_doc.id})

        # 4. Fetch docs in the current page
        docs = list(query.stream())  # Convert the generator to a list
        if not docs:
            # No more docs -> break
            break
        
        # 5. Create a Firestore batch to update documents
        batch = db.batch()
        for doc in docs:
            data = doc.to_dict()
            city_name = data.get('city_name')
            if city_name:
                batch.update(doc.reference, {
                    'city_name_search': city_name.lower()
                })
                updated_count += 1

        # 6. Commit the batch for this page
        batch.commit()

        # 7. Update last_doc to the last doc in this page for pagination
        last_doc = docs[-1]

    print(f'All documents updated successfully. Updated {updated_count} docs.')

if __name__ == '__main__':
    update_all_documents()



# working for explore collection
# def update_all_documents():
#     # Initialize the Firebase Admin SDK
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)

#     db = firestore.client()

#     # Reference to the 'allplaces' collection
#     places_collection = db.collection('allplaces')

#     # Get all documents in the collection
#     docs = places_collection.stream()

#     # Batch write to Firestore (max 500 operations per batch)
#     batch = db.batch()
#     batch_counter = 0
#     batch_size = 500  # Firestore batch limit

#     for doc in docs:
#         doc_dict = doc.to_dict()
#         city_name = doc_dict.get('city_name')
#         if city_name:
#             doc_ref = doc.reference
#             batch.update(doc_ref, {
#                 'city_name_search': city_name.lower()
#             })
#             batch_counter += 1

#             if batch_counter == batch_size:
#                 # Commit the batch and reset
#                 batch.commit()
#                 batch = db.batch()
#                 batch_counter = 0

#     # Commit any remaining operations
#     if batch_counter > 0:
#         batch.commit()

#     print('All documents updated successfully.')

# if __name__ == '__main__':
#     update_all_documents()
