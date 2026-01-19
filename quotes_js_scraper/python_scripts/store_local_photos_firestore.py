

"""
Script to upload images from local 'Cities' folder to Firebase Storage and update Firestore 'explore' documents with image URLs.
It processes all folders in the 'Cities' folder, uploads the image files in each folder to Firebase Storage, and updates
the 'imageUrl' field in the matching Firestore document.
"""

import os
import re
import firebase_admin
from firebase_admin import credentials, firestore, storage

# Initialize Firebase Admin SDK with storage bucket options
firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
cred = credentials.Certificate(firebase_credentials_path)
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred, {
        'storageBucket': 'mycasavsc.appspot.com'  # Replace with your actual bucket name
    })

# Initialize Firestore and Storage clients
db = firestore.client()
bucket = storage.bucket()

# Define the local root folder path on your D: drive
ROOT_FOLDER = r"D:\playlistsNew"

# Allowed image extensions
ALLOWED_EXTENSIONS = {'.png', '.jpg', '.jpeg'}

def find_image_file(folder_path):
    """
    Look for an image file in the given folder.
    Returns the first file found with an allowed image extension.
    """
    for filename in os.listdir(folder_path):
        ext = os.path.splitext(filename)[1].lower()
        if ext in ALLOWED_EXTENSIONS:
            return os.path.join(folder_path, filename)
    return None

def upload_image_file(doc_id, image_filepath):
    """
    Uploads the image file to Firebase Storage.
    The destination path is: playlistsNew_images/<doc_id>/<filename>
    Returns the public URL of the uploaded image.
    """
    filename = os.path.basename(image_filepath)
    destination_path = f"playlistsNew_images/{doc_id}/{filename}"
    blob = bucket.blob(destination_path)
    try:
        with open(image_filepath, "rb") as image_file:
            blob.upload_from_file(image_file, content_type=f"image/{os.path.splitext(filename)[1][1:]}")
        blob.make_public()
        return blob.public_url
    except Exception as e:
        print(f"Error uploading file '{image_filepath}' for document '{doc_id}': {e}")
        return None

def process_local_images():
    """
    Walks through the local folder structure at ROOT_FOLDER.
    For each playlist folder (i.e. each first-level folder represents a document ID,
    and its subfolder is named after the playlist title), attempts to find an image file.
    If an image is found, it uploads the image to Firebase Storage and updates the corresponding
    Firestore document (in collection 'playlistsNew') with the public URL in the 'imageUrl' field.
    """
    # Iterate through each document folder in ROOT_FOLDER
    for doc_id in os.listdir(ROOT_FOLDER):
        doc_folder = os.path.join(ROOT_FOLDER, doc_id)
        if not os.path.isdir(doc_folder):
            continue

        # Inside each document folder, assume there's a subfolder for the playlist title.
        # (If there are multiple subfolders, we'll process each one.)
        for subfolder in os.listdir(doc_folder):
            playlist_folder = os.path.join(doc_folder, subfolder)
            if not os.path.isdir(playlist_folder):
                continue

            print(f"Processing document '{doc_id}' - folder '{subfolder}'")
            image_filepath = find_image_file(playlist_folder)
            if not image_filepath:
                print(f"  No image file found in '{playlist_folder}'. Skipping...")
                continue

            print(f"  Found image file: {image_filepath}")
            public_url = upload_image_file(doc_id, image_filepath)
            if public_url:
                try:
                    # Update the Firestore document in collection 'playlistsNew'
                    doc_ref = db.collection('playlistsNew').document(doc_id)
                    doc_ref.update({'imageUrl': public_url})
                    print(f"  Updated document '{doc_id}' with imageUrl: {public_url}")
                except Exception as e:
                    print(f"  Error updating Firestore for document '{doc_id}': {e}")
            else:
                print(f"  Failed to upload image for document '{doc_id}'.")

if __name__ == "__main__":
    process_local_images()
