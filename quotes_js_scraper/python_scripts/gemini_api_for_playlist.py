"""
Script to generate images for playlist documents using Vertex AI.

This script:

1. Iterates over all playlist documents in Firestore.
2. Checks if the image field is empty.
3. Uses Vertex AI to generate an image based on the description field.
4. Uploads the image to Firebase Storage.
5. Updates the image field on the Firestore document.

"""


import os
import logging
import time
import io
from io import BytesIO
import random  # for retry jitter
import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.api_core.exceptions import DeadlineExceeded
from PIL import Image

# Import Vertex AI packages
import vertexai
from vertexai.preview.vision_models import ImageGenerationModel

# Additional imports for service account credentials
import google.cloud.aiplatform as aiplatform
from google.oauth2 import service_account

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load sensitive info (adjust paths/keys as needed)
firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# Vertex AI configuration: update these with your project ID and location.
VERTEX_PROJECT = "mycasavsc"
VERTEX_LOCATION = "us-central1"

# Initialize Firebase Admin SDK
cred = credentials.Certificate(firebase_credentials_path)
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred, {
        'storageBucket': 'mycasavsc.appspot.com'
    })
db = firestore.client()
bucket = storage.bucket()

# Load service account credentials for Vertex AI
credentials = service_account.Credentials.from_service_account_file(
    firebase_credentials_path,
    scopes=['https://www.googleapis.com/auth/cloud-platform']
)

# Initialize Vertex AI with explicit credentials
vertexai.init(
    project=VERTEX_PROJECT,
    location=VERTEX_LOCATION,
    credentials=credentials
)

# Load the Imagen model
generation_model = ImageGenerationModel.from_pretrained("imagen-3.0-generate-002")

def vertexai_generate_image(prompt, image_data=None):
    """
    Generates an image using Vertex AI's Imagen model based on a text prompt.
    Returns a PIL.Image object if successful.
    """
    try:
        response = generation_model.generate_images(
            prompt=prompt,
            number_of_images=1,
            aspect_ratio="1:1",
            negative_prompt="",
            person_generation="",
            safety_filter_level="",
            add_watermark=False,
        )
        # Instead of checking len(response), we assume response[0] is valid.
        pil_image = response[0]._pil_image
        return pil_image
    except Exception as e:
        logger.error(f"Error generating image with Vertex AI: {e}")
        return None

def vertexai_generate_image_with_retry(prompt, max_retries=5, initial_delay=5):
    """
    Attempts to generate an image using Vertex AI with retry logic.
    Retries on failure with exponential backoff and random jitter.
    """
    for attempt in range(max_retries):
        image = vertexai_generate_image(prompt)
        if image is not None:
            return image
        else:
            delay = initial_delay * (2 ** attempt) + random.random()
            logger.warning(f"Retrying in {delay:.2f} seconds (Attempt {attempt + 1}/{max_retries}) for prompt: {prompt}")
            time.sleep(delay)
    logger.error(f"Failed to generate image after {max_retries} retries for prompt: {prompt}")
    return None

def generate_image_for_playlist(title):
    """
    Generates an image for a playlist based on its title using Vertex AI Imagen.
    Converts the generated PIL.Image to PNG bytes.
    """
    prompt = f'Generate image which will showcase the main context of title "{title}"'
    try:
        image = vertexai_generate_image_with_retry(prompt)
        if image:
            image_bytes_io = BytesIO()
            image.save(image_bytes_io, format='PNG')
            return image_bytes_io.getvalue()
        else:
            logger.error(f"Failed to generate image for playlist '{title}' using Vertex AI.")
            return None
    except Exception as e:
        logger.error(f"Error generating image for playlist '{title}': {e}")
        return None

def upload_image_to_storage(playlist_id, image_bytes):
    """
    Uploads image bytes to Firebase Storage under a path based on playlist_id.
    Makes the blob public and returns the public URL.
    """
    filename = f"playlistsNew_images/{playlist_id}.png"
    blob = bucket.blob(filename)
    try:
        blob.upload_from_string(image_bytes, content_type="image/png")
        blob.make_public()
        return blob.public_url
    except Exception as e:
        logger.error(f"Error uploading image for playlist '{playlist_id}' to Storage: {e}")
        return None

def get_all_playlists(playlists_ref, max_retries=3, timeout=300):
    """
    Retrieves all documents from the Firestore collection with retry logic.
    """
    for attempt in range(max_retries):
        try:
            return list(playlists_ref.stream(timeout=timeout))
        except DeadlineExceeded as e:
            logger.warning(f"Attempt {attempt+1}: DeadlineExceeded while streaming playlists: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    logger.error("Failed to fetch playlists after several retries.")
    return []

def process_playlist_images():
    """
    Processes the 'playlistsNew' Firestore collection:
      - Only processes documents with numeric IDs between 350 and 430 (inclusive)
      - For each document missing an 'imageUrl', generates an image using Vertex AI Imagen,
        uploads the image to Firebase Storage, and updates the document with the image URL.
    """
    playlists_ref = db.collection('playlistsNew')
    playlists = get_all_playlists(playlists_ref, max_retries=3, timeout=300)
    if not playlists:
        logger.info("No playlists to process.")
        return

    for playlist in playlists:
        doc_id = playlist.id
        try:
            doc_id_int = int(doc_id)
        except ValueError:
            logger.info(f"Skipping document ID {doc_id} as it is not numeric.")
            continue

        if doc_id_int < 350 or doc_id_int > 430:
            logger.info(f"Skipping document ID {doc_id} as it is outside the range 350-430.")
            continue

        playlist_data = playlist.to_dict()
        title = playlist_data.get('title', '')
        if not title:
            logger.info(f"No title found for document ID {doc_id}")
            continue
        if playlist_data.get('imageUrl'):
            logger.info(f"Document ID {doc_id} already has an imageUrl. Skipping...")
            continue

        logger.info(f"Processing playlist '{title}' (Document ID: {doc_id})")
        image_bytes = generate_image_for_playlist(title)
        if not image_bytes:
            logger.error(f"Failed to generate image for playlist '{title}'")
            continue

        public_url = upload_image_to_storage(doc_id, image_bytes)
        if public_url:
            try:
                playlist.reference.update({'imageUrl': public_url})
                logger.info(f"Updated document ID {doc_id} with imageUrl: {public_url}")
            except Exception as e:
                logger.error(f"Error updating document ID {doc_id}: {e}")
        else:
            logger.error(f"Failed to upload image for playlist '{title}' (Document ID: {doc_id})")
        
        time.sleep(1)

if __name__ == "__main__":
    process_playlist_images()



# import os
# import logging
# import time
# import io
# from io import BytesIO
# import random  # for retry jitter
# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# from google.api_core.exceptions import DeadlineExceeded
# from PIL import Image

# # Import Vertex AI packages
# import vertexai
# from vertexai.preview.vision_models import ImageGenerationModel

# # Add these imports
# import google.cloud.aiplatform as aiplatform
# from google.oauth2 import service_account

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Load sensitive info (adjust paths/keys as needed)
# firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# # Vertex AI configuration: update these with your project ID and location.
# VERTEX_PROJECT = "mycasavsc"
# VERTEX_LOCATION = "us-central1"

# # Initialize Firebase Admin SDK
# cred = credentials.Certificate(firebase_credentials_path)
# if not firebase_admin._apps:
#     firebase_admin.initialize_app(cred, {
#         'storageBucket': 'mycasavsc.appspot.com'
#     })
# db = firestore.client()
# bucket = storage.bucket()

# # Update the credentials and initialization section
# # Load service account credentials
# credentials = service_account.Credentials.from_service_account_file(
#     firebase_credentials_path,
#     scopes=['https://www.googleapis.com/auth/cloud-platform']
# )

# # Initialize Vertex AI with explicit credentials
# vertexai.init(
#     project=VERTEX_PROJECT,
#     location=VERTEX_LOCATION,
#     credentials=credentials
# )

# # Load the Imagen model (imagen-3.0-generate-002 is the recommended model for image generation)
# generation_model = ImageGenerationModel.from_pretrained("imagen-3.0-generate-002")

# def vertexai_generate_image(prompt, image_data=None):
#     """
#     Generates an image using Vertex AI's Imagen model based on a text prompt.
#     Returns a PIL.Image object if successful.
#     """
#     try:
#         images = generation_model.generate_images(
#             prompt=prompt,
#             number_of_images=1,
#             aspect_ratio="1:1",
#             negative_prompt="",
#             person_generation="",
#             safety_filter_level="",
#             add_watermark=False,
#         )
#         if images and len(images) > 0:
#             # The returned object includes a _pil_image attribute (per the official sample)
#             pil_image = images[0]._pil_image
#             return pil_image
#         else:
#             logger.error("No images returned from Vertex AI.")
#             return None
#     except Exception as e:
#         logger.error(f"Error generating image with Vertex AI: {e}")
#         return None

# def vertexai_generate_image_with_retry(prompt, max_retries=5, initial_delay=5):
#     """
#     Attempts to generate an image using Vertex AI with retry logic.
#     Retries on failure with exponential backoff and random jitter.
#     """
#     for attempt in range(max_retries):
#         image = vertexai_generate_image(prompt)
#         if image is not None:
#             return image
#         else:
#             delay = initial_delay * (2 ** attempt) + random.random()
#             logger.warning(f"Retrying in {delay:.2f} seconds (Attempt {attempt + 1}/{max_retries}) for prompt: {prompt}")
#             time.sleep(delay)
#     logger.error(f"Failed to generate image after {max_retries} retries for prompt: {prompt}")
#     return None

# def generate_image_for_playlist(title):
#     """
#     Generates an image for a playlist based on its title using Vertex AI Imagen.
#     Converts the generated PIL.Image to PNG bytes.
#     """
#     prompt = f'Generate image which will showcase the main context of title "{title}"'
#     try:
#         image = vertexai_generate_image_with_retry(prompt)
#         if image:
#             image_bytes_io = BytesIO()
#             image.save(image_bytes_io, format='PNG')
#             return image_bytes_io.getvalue()
#         else:
#             logger.error(f"Failed to generate image for playlist '{title}' using Vertex AI.")
#             return None
#     except Exception as e:
#         logger.error(f"Error generating image for playlist '{title}': {e}")
#         return None

# def upload_image_to_storage(playlist_id, image_bytes):
#     """
#     Uploads image bytes to Firebase Storage under a path based on playlist_id.
#     Makes the blob public and returns the public URL.
#     """
#     filename = f"playlistsNew_images/{playlist_id}.png"
#     blob = bucket.blob(filename)
#     try:
#         blob.upload_from_string(image_bytes, content_type="image/png")
#         blob.make_public()
#         return blob.public_url
#     except Exception as e:
#         logger.error(f"Error uploading image for playlist '{playlist_id}' to Storage: {e}")
#         return None

# def get_all_playlists(playlists_ref, max_retries=3, timeout=300):
#     """
#     Retrieves all documents from the Firestore collection with retry logic.
#     """
#     for attempt in range(max_retries):
#         try:
#             return list(playlists_ref.stream(timeout=timeout))
#         except DeadlineExceeded as e:
#             logger.warning(f"Attempt {attempt+1}: DeadlineExceeded while streaming playlists: {e}. Retrying in 5 seconds...")
#             time.sleep(5)
#     logger.error("Failed to fetch playlists after several retries.")
#     return []

# def process_playlist_images():
#     """
#     Processes the 'playlistsNew' Firestore collection:
#       - Only processes documents with numeric IDs between 350 and 430 (inclusive)
#       - For each document missing an 'imageUrl', generates an image using Vertex AI Imagen,
#         uploads the image to Firebase Storage, and updates the document with the image URL.
#     """
#     playlists_ref = db.collection('playlistsNew')
#     playlists = get_all_playlists(playlists_ref, max_retries=3, timeout=300)
#     if not playlists:
#         logger.info("No playlists to process.")
#         return

#     for playlist in playlists:
#         doc_id = playlist.id
#         try:
#             doc_id_int = int(doc_id)
#         except ValueError:
#             logger.info(f"Skipping document ID {doc_id} as it is not numeric.")
#             continue

#         if doc_id_int < 350 or doc_id_int > 430:
#             logger.info(f"Skipping document ID {doc_id} as it is outside the range 350-430.")
#             continue

#         playlist_data = playlist.to_dict()
#         title = playlist_data.get('title', '')
#         if not title:
#             logger.info(f"No title found for document ID {doc_id}")
#             continue
#         if playlist_data.get('imageUrl'):
#             logger.info(f"Document ID {doc_id} already has an imageUrl. Skipping...")
#             continue

#         logger.info(f"Processing playlist '{title}' (Document ID: {doc_id})")
#         image_bytes = generate_image_for_playlist(title)
#         if not image_bytes:
#             logger.error(f"Failed to generate image for playlist '{title}'")
#             continue

#         public_url = upload_image_to_storage(doc_id, image_bytes)
#         if public_url:
#             try:
#                 playlist.reference.update({'imageUrl': public_url})
#                 logger.info(f"Updated document ID {doc_id} with imageUrl: {public_url}")
#             except Exception as e:
#                 logger.error(f"Error updating document ID {doc_id}: {e}")
#         else:
#             logger.error(f"Failed to upload image for playlist '{title}' (Document ID: {doc_id})")
        
#         time.sleep(1)

# if __name__ == "__main__":
#     process_playlist_images()
