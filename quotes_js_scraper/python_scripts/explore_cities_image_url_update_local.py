#!/usr/bin/env python
"""
Firebase Image Syncer
----------------------
This script synchronizes local image files with Firebase Storage and updates Firestore documents in the "explore" collection.
It now includes a constraint: if a document's "image_url" field is a list with more than 3 URLs, the script skips updating that document.
"""

import os
import firebase_admin
from firebase_admin import credentials, firestore, storage
import mimetypes

def initialize_firebase():
    """
    Initialize Firebase Admin SDK using a service account JSON file.
    Returns:
        db: Firestore client
        bucket: Firebase Storage bucket
    """
    print("Initializing Firebase Admin SDK...")
    # Replace with the path to your Firebase service account JSON file
    firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
    cred = credentials.Certificate(firebase_credentials_path)
    firebase_admin.initialize_app(cred, {
        'storageBucket': 'mycasavsc.appspot.com'
    })
    db = firestore.client()
    bucket = storage.bucket()
    return db, bucket

def main():
    """
    Main function that processes Firestore documents and synchronizes local images to Firebase Storage.
    """
    print("Starting the Firebase Image Syncer script...")
    
    # Initialize Firebase
    db, bucket = initialize_firebase()
    
    # Path to the local 'Cities' folder
    cities_folder_path = r'C:\Users\Atish\Desktop\Cities'
    
    # Retrieve all documents from the 'explore' collection
    docs = db.collection('explore').get()
    print(f"Found {len(docs)} documents in the 'explore' collection.")
    
    for doc in docs:
        doc_id = doc.id
        doc_ref = db.collection('explore').document(doc_id)
        doc_data = doc.to_dict()
        print(f"Processing document ID: {doc_id}")
        
        # Retrieve existing "image_url" field
        existing_image_url = doc_data.get('image_url')
        
        # Constraint: if image_url exists as a list and its length is greater than 3, skip updating this document.
        if existing_image_url and isinstance(existing_image_url, list) and len(existing_image_url) > 3:
            print(f"Skipping update for document {doc_id} because image_url list length is greater than 3.")
            continue
        
        # Build path to local folder corresponding to the document ID
        local_folder_path = os.path.join(cities_folder_path, doc_id)
        
        if os.path.exists(local_folder_path) and os.path.isdir(local_folder_path):
            print(f"Found local folder for document ID: {doc_id}")
            # Get list of image files in the folder
            image_files = [f for f in os.listdir(local_folder_path) if os.path.isfile(os.path.join(local_folder_path, f))]
            image_urls = []
            
            for image_file in image_files:
                local_image_path = os.path.join(local_folder_path, image_file)
                # Define the storage path in Firebase Storage
                storage_path = f"places/{doc_id}/{image_file}"
                try:
                    blob = bucket.blob(storage_path)
                    # Determine the content type for the file
                    content_type, _ = mimetypes.guess_type(local_image_path)
                    if content_type is None:
                        content_type = 'application/octet-stream'
                    # Upload the file with the determined content type
                    blob.upload_from_filename(local_image_path, content_type=content_type)
                    # Make the uploaded file publicly accessible
                    blob.make_public()
                    image_url = blob.public_url
                    image_urls.append(image_url)
                    print(f"Uploaded {local_image_path} to {storage_path}, URL: {image_url}")
                except Exception as e:
                    print(f"Error uploading {local_image_path}: {e}")
            
            # Merge existing image URLs with the new ones without deleting previous ones
            if existing_image_url:
                if isinstance(existing_image_url, list):
                    image_urls.extend(existing_image_url)
                elif isinstance(existing_image_url, str):
                    image_urls.append(existing_image_url)
                else:
                    print(f"Unexpected 'image_url' type in document {doc_id}: {type(existing_image_url)}")
            else:
                print(f"No existing 'image_url' field in document {doc_id}")
            
            # Update the Firestore document if there are any image URLs
            if image_urls:
                try:
                    doc_ref.update({'image_url': image_urls})
                    print(f"Updated document {doc_id} with image URLs.")
                except Exception as e:
                    print(f"Error updating document {doc_id}: {e}")
            else:
                print(f"No images found to update in document {doc_id}")
        else:
            print(f"No folder found for document ID {doc_id} at {local_folder_path}")

if __name__ == '__main__':
    main()


# import os
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# import mimetypes

# def initialize_firebase():
#     """
#     Initialize Firebase Admin SDK using a service account JSON file.
#     Returns:
#         db: Firestore client
#         bucket: Firebase Storage bucket
#     """
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
#     """
#     Main function that processes Firestore documents and synchronizes local images to Firebase Storage.
#     """
#     print("Starting the Firebase Image Syncer script...")
    
#     # Initialize Firebase
#     db, bucket = initialize_firebase()
    
#     # Path to the local 'Cities' folder
#     cities_folder_path = r'C:\Users\Atish\Desktop\Cities'
    
#     # Retrieve all documents from the 'explore' collection
#     docs = db.collection('explore').get()
#     print(f"Found {len(docs)} documents in the 'explore' collection.")
    
#     for doc in docs:
#         doc_id = doc.id
#         doc_ref = db.collection('explore').document(doc_id)
#         doc_data = doc.to_dict()
#         print(f"Processing document ID: {doc_id}")
        
#         # Build the local folder path corresponding to this document
#         local_folder_path = os.path.join(cities_folder_path, doc_id)
        
#         if os.path.exists(local_folder_path) and os.path.isdir(local_folder_path):
#             print(f"Found local folder for document ID: {doc_id}")
#             # List all image files in the folder
#             image_files = [f for f in os.listdir(local_folder_path) if os.path.isfile(os.path.join(local_folder_path, f))]
#             image_urls = []
            
#             for image_file in image_files:
#                 local_image_path = os.path.join(local_folder_path, image_file)
#                 # Define the storage path in Firebase Storage
#                 storage_path = f"places/{doc_id}/{image_file}"
#                 try:
#                     blob = bucket.blob(storage_path)
#                     # Determine the content type for the file
#                     content_type, _ = mimetypes.guess_type(local_image_path)
#                     if content_type is None:
#                         content_type = 'application/octet-stream'
#                     # Upload the file with the determined content type
#                     blob.upload_from_filename(local_image_path, content_type=content_type)
#                     # Make the uploaded file publicly accessible
#                     blob.make_public()
#                     image_url = blob.public_url
#                     image_urls.append(image_url)
#                     print(f"Uploaded {local_image_path} to {storage_path}, URL: {image_url}")
#                 except Exception as e:
#                     print(f"Error uploading {local_image_path}: {e}")
            
#             # Retrieve existing 'image_url' field and merge with new URLs without deleting previous ones
#             existing_image_url = doc_data.get('image_url')
#             if existing_image_url:
#                 if isinstance(existing_image_url, list):
#                     image_urls.extend(existing_image_url)
#                 elif isinstance(existing_image_url, str):
#                     image_urls.append(existing_image_url)
#                 else:
#                     print(f"Unexpected 'image_url' type in document {doc_id}: {type(existing_image_url)}")
#             else:
#                 print(f"No existing 'image_url' field in document {doc_id}")
            
#             # Update the Firestore document if there are any image URLs
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