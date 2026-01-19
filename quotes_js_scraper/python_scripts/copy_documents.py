import firebase_admin
from firebase_admin import credentials, firestore
import os

def copy_documents():
    # Initialize the Firebase Admin SDK
    firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
    cred = credentials.Certificate(firebase_credentials_path)
    firebase_admin.initialize_app(cred)

    db = firestore.client()

    # References to the collections
    source_collection = db.collection('explore')
    destination_collection = db.collection('places')

    # Get all documents in the source collection
    docs = source_collection.stream()

    # Batch write to Firestore (max 500 operations per batch)
    batch = db.batch()
    batch_counter = 0
    batch_size = 500  # Firestore batch limit

    for doc in docs:
        doc_id = doc.id
        doc_dict = doc.to_dict()

        # Reference to the document in the destination collection
        dest_doc_ref = destination_collection.document(doc_id)

        # Check if the document exists in the destination collection
        dest_doc = dest_doc_ref.get()

        if not dest_doc.exists:
            # Document does not exist in destination; copy it
            batch.set(dest_doc_ref, doc_dict)
            batch_counter += 1
        else:
            # Document exists; skip or update
            print(f'Document {doc_id} already exists in destination, skipping.')
            # If you want to update instead of skipping, uncomment the next two lines:
            # batch.set(dest_doc_ref, doc_dict, merge=True)
            # batch_counter += 1

        if batch_counter == batch_size:
            # Commit the batch and reset
            batch.commit()
            batch = db.batch()
            batch_counter = 0
            print('Batch committed.')

    # Commit any remaining operations
    if batch_counter > 0:
        batch.commit()
        print('Final batch committed.')

    print('All documents copied successfully.')

if __name__ == '__main__':
    copy_documents()
