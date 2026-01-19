import os
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize the Firestore client
firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
cred = credentials.Certificate(firebase_credentials_path)
# cred = credentials.Certificate('path/to/your/serviceAccountKey.json')  # Replace with the path to your service account key file
firebase_admin.initialize_app(cred)

# Get Firestore instance
db = firestore.client()

# Define the collection name
collection_name = 'searchedCategoriesNew'  # Replace with your collection name

# Query to get all documents in the collection
docs = db.collection(collection_name).stream()

# Initialize counter
doc_count = 0

# Count the documents
for doc in docs:
    doc_count += 1

print(f'Total number of documents in {collection_name} collection: {doc_count}')
