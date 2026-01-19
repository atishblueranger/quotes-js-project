
"""
Script to create folders for each playlist in the 'playlistsNew' collection.
For each document in the 'playlistsNew' collection, it creates a folder on D:\playlistsNew
with the following structure:
  D:\playlistsNew\<document_id>\<sanitized_title>
"""

import os
import re
import firebase_admin
from firebase_admin import credentials, firestore

# Replace with your Firebase service account JSON path and bucket if needed
firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
cred = credentials.Certificate(firebase_credentials_path)
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)

# Initialize Firestore client
db = firestore.client()

# Define the local root folder path on your D: drive
ROOT_FOLDER = r"D:\playlistsNew"

# Create the root folder if it doesn't exist
os.makedirs(ROOT_FOLDER, exist_ok=True)

def sanitize_folder_name(name):
    """
    Sanitize a folder name by removing or replacing invalid characters.
    Invalid Windows folder characters: < > : " / \ | ? *
    We'll replace them with an underscore.
    """
    return re.sub(r'[<>:"/\\|?*]', '_', name)

def create_playlist_folders():
    """
    Retrieves all documents from the 'playlists' collection in Firestore
    and creates a folder structure on D:\playlistsNew such that for each playlist:
      D:\playlistsNew\<document_id>\<sanitized_title>
    """
    playlists_ref = db.collection('playlistsNew')
    playlists = playlists_ref.stream()

    for playlist in playlists:
        playlist_data = playlist.to_dict()
        doc_id = playlist.id
        title = playlist_data.get('title', 'Untitled')
        
        # Sanitize the title for folder name
        safe_title = sanitize_folder_name(title.strip())
        
        # Define folder path: D:\playlistsNew\<doc_id>\<safe_title>
        folder_path = os.path.join(ROOT_FOLDER, doc_id, safe_title)
        
        try:
            os.makedirs(folder_path, exist_ok=True)
            print(f"Created folder: {folder_path}")
        except Exception as e:
            print(f"Error creating folder for document {doc_id} with title '{title}': {e}")

if __name__ == "__main__":
    create_playlist_folders()
