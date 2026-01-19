from google.cloud import storage
import os

# Path to your Firebase Admin SDK credentials JSON file
firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# Replace with your Bucket Name and the desired destination folder
bucket_name = "mycasavsc.appspot.com"
source_prefix = "places/"  # The prefix of the folder you want to download
destination_folder = "downloaded_places"  # The local folder to download to

def download_folder_with_credentials(credentials_path, bucket_name, source_prefix, destination_folder):
    """Downloads all files with the given prefix from a GCS bucket to a local folder using provided credentials."""

    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=source_prefix)

    # Create the destination folder if it doesn't exist
    os.makedirs(destination_folder, exist_ok=True)

    print(f"Downloading files from gs://{bucket_name}/{source_prefix} to {destination_folder}/")

    for blob in blobs:
        if blob.name == source_prefix:
            continue  # Skip the folder prefix itself

        relative_path = os.path.relpath(blob.name, source_prefix)
        destination_path = os.path.join(destination_folder, relative_path)
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        print(f"Downloading: {blob.name} -> {destination_path}")
        blob.download_to_filename(destination_path)

    print("Download complete!")

if __name__ == "__main__":
    download_folder_with_credentials(firebase_credentials_path, bucket_name, source_prefix, destination_folder)