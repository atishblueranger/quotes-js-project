# import os
# import json
# import firebase_admin
# from firebase_admin import credentials, firestore

# # Initialize Firebase Admin SDK using your service account key
# firebase_credentials_path = os.path.join(
#     os.getcwd(),
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )
# cred = credentials.Certificate(firebase_credentials_path)
# firebase_admin.initialize_app(cred)

# # Get Firestore client
# db = firestore.client()

# # Define collection name
# collection_name = "nationalParks"
# missing_document_ids = []    # To store docs missing one or both subcollections
# both_missing_ids = []        # To store docs missing both subcollections

# # Iterate over each document in the 'nationalParks' collection
# docs = db.collection(collection_name).stream()

# for doc in docs:
#     doc_ref = db.collection(collection_name).document(doc.id)
    
#     # List all subcollections for the document
#     subcollections = [subcol.id for subcol in doc_ref.collections()]
    
#     # Determine if each subcollection exists
#     missing_attractions = "TouristAttractions" not in subcollections
#     missing_restaurants = "TouristRestaurants" not in subcollections
    
#     # Check if the document is missing either subcollection
#     if missing_attractions or missing_restaurants:
#         missing_document_ids.append(doc.id)
        
#     # Check if the document is missing both subcollections
#     if missing_attractions and missing_restaurants:
#         both_missing_ids.append(doc.id)

# # Prepare the output as a JSON object including total counts
# output = {
#     "total_missing": len(missing_document_ids),
#     "missing_document_ids": missing_document_ids,
#     "total_both_missing": len(both_missing_ids),
#     "both_missing_document_ids": both_missing_ids
# }

# # Write JSON output to a file
# with open("missing_document_ids.json", "w") as outfile:
#     json.dump(output, outfile, indent=4)

# print("JSON file created with document ids missing one or both subcollections.")
# print(f"Total number of documents missing either subcollection: {len(missing_document_ids)}")
# print(f"Total number of documents missing both subcollections: {len(both_missing_ids)}")




import os
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize the Firestore client
firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
cred = credentials.Certificate(firebase_credentials_path)
firebase_admin.initialize_app(cred)

# Get Firestore instance
db = firestore.client()

# Define the collection name
collection_name = 'localspots'  # Replace with your collection name

try:
    # Use the count() method to efficiently count documents
    collection_ref = db.collection(collection_name)
    count_query = collection_ref.count()
    count = count_query.get()
    
    print(f'Total number of documents in {collection_name} collection: {count}')
    
except Exception as e:
    print(f"Error counting documents: {e}")
    
    # If count() fails, fall back to pagination approach
    print("Falling back to pagination method...")
    
    batch_size = 1000  # Adjust based on your needs
    doc_count = 0
    
    # Get documents in batches
    query = db.collection(collection_name).limit(batch_size)
    docs = list(query.stream())
    doc_count = len(docs)
    
    # Continue pagination if needed
    while len(docs) == batch_size:
        # Get the last document from the previous batch
        last_doc = docs[-1]
        
        # Get the next batch starting after the last document
        query = db.collection(collection_name).start_after(last_doc).limit(batch_size)
        docs = list(query.stream())
        
        # Update the count
        doc_count += len(docs)
    
    print(f'Total number of documents (pagination method): {doc_count}')


# import os
# import firebase_admin
# from firebase_admin import credentials, firestore

# # Initialize the Firestore client
# firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
# cred = credentials.Certificate(firebase_credentials_path)
# # cred = credentials.Certificate('path/to/your/serviceAccountKey.json')  # Replace with the path to your service account key file
# firebase_admin.initialize_app(cred)

# # Get Firestore instance
# db = firestore.client()

# # Define the collection name
# collection_name = 'allplaces'  # Replace with your collection name

# # Query to get all documents in the collection
# docs = db.collection(collection_name).stream()

# # Initialize counter
# doc_count = 0

# # Count the documents
# for doc in docs:
#     doc_count += 1

# print(f'Total number of documents in {collection_name} collection: {doc_count}')
