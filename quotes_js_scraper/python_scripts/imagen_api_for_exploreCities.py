# Countries in allplaces
#!/usr/bin/env python3
import os
import json
import time
import logging
import argparse
from io import BytesIO

import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.oauth2 import service_account
import vertexai
from vertexai.preview.vision_models import ImageGenerationModel
from PIL import Image

# ── CONFIG & LOGGING ───────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Defaults—can be overridden via CLI flags
FIREBASE_CRED_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
FIRESTORE_COLLECTION = "allplaces"
STORAGE_BUCKET     = "mycasavsc.appspot.com"
VERTEX_PROJECT     = "mycasavsc"
VERTEX_LOCATION    = "us-central1"

# Single prompt template—with `{name}` substituted
PROMPT_TEMPLATE = (
    "Bright and sunny image showcasing the beauty of {name}, mostly in white tones."
)

# ── INITIALIZATION ────────────────────────────────────────────────
def init_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_CRED_PATH)
        firebase_admin.initialize_app(cred, {"storageBucket": STORAGE_BUCKET})
        logger.info("✅ Firebase initialized.")
    else:
        logger.info("Firebase already initialized.")
    return firestore.client(), storage.bucket()

def init_vertexai():
    creds = service_account.Credentials.from_service_account_file(
        FIREBASE_CRED_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    vertexai.init(project=VERTEX_PROJECT, location=VERTEX_LOCATION, credentials=creds)
    return ImageGenerationModel.from_pretrained("imagen-3.0-generate-002")

# ── IMAGE GENERATION ──────────────────────────────────────────────
def generate_image(model, prompt, retries=2, delay=5):
    for attempt in range(retries + 1):
        try:
            resp = model.generate_images(
                prompt=prompt,
                number_of_images=1,
                aspect_ratio="1:1",
                add_watermark=False
            )
            return resp[0]._pil_image
        except Exception as e:
            if attempt < retries:
                wait = delay * (2 ** attempt)
                logger.warning(f"Retry {attempt+1}/{retries} after {wait}s due to: {e}")
                time.sleep(wait)
            else:
                logger.error(f"Failed to generate image for prompt '{prompt}': {e}")
    return None

# ── MAIN WORKFLOW ──────────────────────────────────────────────────
def main(args):
    db, bucket = init_firebase()
    model      = init_vertexai()

    # Load countries JSON
    with open(args.input_file, "r", encoding="utf-8") as f:
        countries = json.load(f)

    for entry in countries:
        place_id = entry.get("id")
        name     = entry.get("name", "")
        if not place_id or not name:
            logger.warning(f"Skipping invalid entry: {entry}")
            continue

        doc_ref = db.collection(args.collection).document(place_id)
        snap    = doc_ref.get()
        if not snap.exists:
            logger.warning(f"No document for ID {place_id}")
            continue

        data        = snap.to_dict() or {}
        existing    = data.get("image_url", [])
        # skip if already has an image_url list
        if isinstance(existing, list) and existing:
            logger.info(f"Skipping {place_id} ({name}): already has images")
            continue

        prompt = PROMPT_TEMPLATE.format(name=name)
        logger.info(f"Generating image for {place_id} — {name}")
        img = generate_image(model, prompt)
        if not img:
            continue

        # Upload
        buf      = BytesIO()
        img.save(buf, format="PNG")
        blobpath = f"regions/{place_id}/imagen_1.png"
        blob     = bucket.blob(blobpath)
        try:
            blob.upload_from_string(buf.getvalue(), content_type="image/png")
            blob.make_public()
            url = blob.public_url
        except Exception as e:
            logger.error(f"Upload failed for {place_id}: {e}")
            continue

        # Update Firestore
        try:
            doc_ref.update({"image_url": [url]})
            logger.info(f"Updated {place_id} with image_url: {url}")
        except Exception as e:
            logger.error(f"Firestore update failed for {place_id}: {e}")

        time.sleep(args.between)

    logger.info("🏁 Done.")

if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Generate one country image and store URL in Firestore"
    )
    p.add_argument(
        "--input_file", required=True,
        help="Path to JSON file of [{id,name,lp_url},…]"
    )
    p.add_argument(
        "--collection", default=FIRESTORE_COLLECTION,
        help="Firestore collection name"
    )
    p.add_argument(
        "--between", type=float, default=15,
        help="Seconds to wait between API calls"
    )
    args = p.parse_args()
    main(args)


# #!/usr/bin/env python
# """
# Vertex AI Imagen City Image Generator with Multiple Prompts
# ------------------------------------------------------------
# This script generates 3 images using Vertex AI Imagen for each city by employing three distinct prompts.
# Each generated image is uploaded to Firebase Storage under the path "places/{city_id}/imagen_{index}.png".
# The Firestore document (in the "explore" collection, using the city ID as the document ID)
# is then updated by appending the new image URLs to the existing "image_url" field.
# It skips any document that already has more than 3 image URLs.
# """

# import os
# import logging
# import time
# import random
# from io import BytesIO

# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# from google.api_core.exceptions import DeadlineExceeded
# from PIL import Image

# # Import Vertex AI packages
# import vertexai
# from vertexai.preview.vision_models import ImageGenerationModel

# # Additional imports for service account credentials for Vertex AI
# import google.cloud.aiplatform as aiplatform
# from google.oauth2 import service_account

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Firebase and Vertex AI configuration
# firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
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

# # Load service account credentials for Vertex AI
# vertex_credentials = service_account.Credentials.from_service_account_file(
#     firebase_credentials_path,
#     scopes=['https://www.googleapis.com/auth/cloud-platform']
# )

# # Initialize Vertex AI with explicit credentials
# vertexai.init(
#     project=VERTEX_PROJECT,
#     location=VERTEX_LOCATION,
#     credentials=vertex_credentials
# )

# # Load the Imagen model
# generation_model = ImageGenerationModel.from_pretrained("imagen-3.0-generate-002")

# def vertexai_generate_images(prompt, number_of_images=1):
#     """
#     Generates images using Vertex AI's Imagen model based on a text prompt.
#     Returns a list of PIL.Image objects if successful.
#     """
#     try:
#         response = generation_model.generate_images(
#             prompt=prompt,
#             number_of_images=number_of_images,
#             aspect_ratio="1:1",
#             negative_prompt="",
#             person_generation="",
#             safety_filter_level="",
#             add_watermark=False,
#         )
#         pil_images = [img_response._pil_image for img_response in response]
#         return pil_images
#     except Exception as e:
#         logger.error(f"Error generating images with Vertex AI for prompt '{prompt}': {e}")
#         return []

# def vertexai_generate_images_with_retry(prompt, number_of_images=1, max_retries=2, initial_delay=5):
#     """
#     Attempts to generate images using Vertex AI with retry logic.
#     Retries on failure with exponential backoff and random jitter.
#     """
#     for attempt in range(max_retries):
#         images = vertexai_generate_images(prompt, number_of_images)
#         if images:
#             return images
#         else:
#             delay = initial_delay * (2 ** attempt) + random.random()
#             logger.warning(f"Retrying in {delay:.2f} seconds (Attempt {attempt + 1}/{max_retries}) for prompt: {prompt}")
#             time.sleep(delay)
#     logger.error(f"Failed to generate images after {max_retries} retries for prompt: {prompt}")
#     return []

# def generate_images_for_city(city_query):
#     """
#     Generates 3 images for a city using 3 distinct prompts via Vertex AI Imagen.
#     Returns a list of PNG bytes for each generated image.
#     """
#     prompts = [
#         f'Generate image of city {city_query}.',
#         (
#             f'Generate a high-resolution, artistic image that showcases the unique culture, '
#             f'iconic architecture, and vibrant atmosphere of {city_query}. '
#             f'The image should be realistic, colorful, and evoke a sense of wonder.'
#         ),     
#         f'Generate image of city {city_query}. Showcasing important landmark.'
#     ]
    
#     image_bytes_list = []
#     for prompt in prompts:
#         logger.info(f"Generating image for prompt: {prompt}")
#         images = vertexai_generate_images_with_retry(prompt, number_of_images=1)
#         if images:
#             image_bytes_io = BytesIO()
#             images[0].save(image_bytes_io, format='PNG')
#             image_bytes_list.append(image_bytes_io.getvalue())
#         else:
#             logger.error(f"Failed to generate image for prompt: {prompt}")
#               # Delay 60 seconds after each image generation to respect quota limits.
#         logger.info("Waiting for 20 seconds to respect API quota limits...")
#         time.sleep(15)
            
#     return image_bytes_list

# def upload_image_to_storage(city_id, image_bytes, image_index):
#     """
#     Uploads image bytes to Firebase Storage under the path "places/{city_id}/imagen_{image_index}.png".
#     Makes the blob public and returns the public URL.
#     """
#     filename = f"places/{city_id}/imagen_{image_index}.png"
#     blob = bucket.blob(filename)
#     try:
#         blob.upload_from_string(image_bytes, content_type="image/png")
#         blob.make_public()
#         return blob.public_url
#     except Exception as e:
#         logger.error(f"Error uploading image for city '{city_id}' index {image_index}: {e}")
#         return None

# def main():
#     logger.info("Starting Vertex AI Imagen City Image Generator script...")
    
#     # List of city IDs (Firestore document IDs in the "explore" collection)
#     city_ids = [
#         "10015",
#     "10046",
#     "10064",
#     "10074",
#     "10143",
#     "10211",
#     "10230",
#     "10235",
#     "10260",
#     "11769",
#     "118",
#     "128",
#     "131076",
#     "131082",
#     "131086",
#     "131088",
#     "131093",
#     "131099",
#     "131108",
#     "131110",
#     "131117",
#     "131122",
#     "131125",
#     "131127",
#     "131146",
#     "131166",
#     "131360",
#     "131395",
#     "131438",
#     "146246",
#     "146468",
#     "147352",
#     "147372",
#     "175",
#     "176",
#     "187",
#     "257",
#     "30",
#     "31",
#     "32",
#     "34",
#     "39",
#     "41",
#     "42",
#     "49",
#     "50",
#     "51",
#     "58059",
#     "58062",
#     "58068",
#     "58075",
#     "58079",
#     "58164",
#     "58177",
#     "58184",
#     "58185",
#     "58189",
#     "58190",
#     "58191",
#     "58192",
#     "58195",
#     "58198",
#     "58201",
#     "58203",
#     "58204",
#     "58205",
#     "58208",
#     "58216",
#     "58218",
#     "58231",
#     "58232",
#     "58244",
#     "58251",
#     "58269",
#     "58286",
#     "58341",
#     "58342",
#     "58391",
#     "58748",
#     "58857",
#     "59",
#     "61",
#     "62",
#     "78750",
#     "78752",
#     "79303",
#     "79306",
#     "79316",
#     "79321",
#     "81194",
#     "81197",
#     "81231",
#     "81907",
#     "81941",
#     "82584",
#     "82586",
#     "82588",
#     "82594",
#     "85958",
#     "92",
#     "93",
#     "9638",
#     "9658",
#     "9669",
#     "9676",
#     "9677",
#     "9687",
#     "9689",
#     "9691",
#     "9694",
#     "9703",
#     "9709",
#     "9713",
#     "9720",
#     "9721",
#     "9722",
#     "9725",
#     "9727",
#     "9734",
#     "9737",
#     "9739",
#     "9744",
#     "9753",
#     "9756",
#     "9759",
#     "9765",
#     "9772",
#     "9799",
#     "98",
#     "9819",
#     "9836",
#     "9838",
#     "9840",
#     "9841",
#     "9849",
#     "9857",
#     "9861",
#     "9875",
#     "9891",
#     "9892",
#     "99",
#     "9903",
#     "9917",
#     "9919",
#     "9930",
#     "9935",
#     "9937",
#     "9942",
#     "9945",
#     "9950",
#     "9995",
#     "9998"
#     ]
    
#     for city_id in city_ids:
#         doc_ref = db.collection('explore').document(city_id)
#         doc = doc_ref.get()
#         if not doc.exists:
#             logger.info(f"No document found for city {city_id}.")
#             continue
        
#         doc_data = doc.to_dict()
#         logger.info(f"\nProcessing city document with ID: {city_id}")
#         existing_image_url = doc_data.get('image_url')
        
#         # Constraint: Skip if image_url exists as a list with more than 3 URLs.
#         if existing_image_url and isinstance(existing_image_url, list) and len(existing_image_url) > 3:
#             logger.info(f"Skipping city {city_id} as it already has more than 3 image URLs.")
#             continue
        
#         # Build search query using city_name and country_name if available, with "in" for clarity.
#         city_name = doc_data.get('city_name', '')
#         country_name = doc_data.get('country_name', '')
#         if city_name and country_name:
#             city_query = f"{city_name} in {country_name}"
#         elif city_name:
#             city_query = city_name
#         else:
#             city_query = city_id
        
#         # Generate 3 images for the city using the different prompts.
#         image_bytes_list = generate_images_for_city(city_query)
#         if not image_bytes_list:
#             logger.error(f"No images generated for city {city_id} with query '{city_query}'.")
#             continue
        
#         # Upload each generated image to Firebase Storage.
#         new_image_urls = []
#         for index, image_bytes in enumerate(image_bytes_list, start=1):
#             public_url = upload_image_to_storage(city_id, image_bytes, index)
#             if public_url:
#                 new_image_urls.append(public_url)
#             else:
#                 logger.error(f"Failed to upload image {index} for city {city_id}.")
        
#         # Merge with any existing image URLs (without deleting them)
#         final_image_urls = new_image_urls[:]
#         if existing_image_url:
#             if isinstance(existing_image_url, list):
#                 final_image_urls.extend(existing_image_url)
#             elif isinstance(existing_image_url, str):
#                 final_image_urls.append(existing_image_url)
        
#         # Update the Firestore document if there are image URLs.
#         if final_image_urls:
#             try:
#                 doc_ref.update({'image_url': final_image_urls})
#                 logger.info(f"Updated city {city_id} with image URLs: {final_image_urls}")
#             except Exception as e:
#                 logger.error(f"Error updating city {city_id}: {e}")
#         else:
#             logger.error(f"No image URLs available to update for city {city_id}.")
        
#         time.sleep(1)  # Optional delay between processing cities

# if __name__ == '__main__':
#     main()


# #!/usr/bin/env python
# """
# Vertex AI Imagen City Image Generator
# ---------------------------------------
# This script generates 4 images using Vertex AI Imagen for each city based on a combined query
# (from the city's name and country), uploads each generated image to Firebase Storage under
# the path "places/{city_id}/imagen_{index}.png", and then updates the Firestore document (in the "explore"
# collection, using the city ID as the document ID) by appending the new image URLs to the existing
# "image_url" field. It skips any document that already has more than 3 image URLs.
# """

# import os
# import logging
# import time
# import random
# from io import BytesIO

# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# from google.api_core.exceptions import DeadlineExceeded
# from PIL import Image

# # Import Vertex AI packages
# import vertexai
# from vertexai.preview.vision_models import ImageGenerationModel

# # Additional imports for service account credentials for Vertex AI
# import google.cloud.aiplatform as aiplatform
# from google.oauth2 import service_account

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Firebase and Vertex AI configuration
# firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
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

# # Load service account credentials for Vertex AI
# vertex_credentials = service_account.Credentials.from_service_account_file(
#     firebase_credentials_path,
#     scopes=['https://www.googleapis.com/auth/cloud-platform']
# )

# # Initialize Vertex AI with explicit credentials
# vertexai.init(
#     project=VERTEX_PROJECT,
#     location=VERTEX_LOCATION,
#     credentials=vertex_credentials
# )

# # Load the Imagen model
# generation_model = ImageGenerationModel.from_pretrained("imagen-3.0-generate-002")

# def vertexai_generate_images(prompt, number_of_images=4):
#     """
#     Generates images using Vertex AI's Imagen model based on a text prompt.
#     Returns a list of PIL.Image objects if successful.
#     """
#     try:
#         response = generation_model.generate_images(
#             prompt=prompt,
#             number_of_images=number_of_images,
#             aspect_ratio="1:1",
#             negative_prompt="",
#             person_generation="",
#             safety_filter_level="",
#             add_watermark=False,
#         )
#         # Extract PIL images from the response
#         pil_images = [img_response._pil_image for img_response in response]
#         return pil_images
#     except Exception as e:
#         logger.error(f"Error generating images with Vertex AI for prompt '{prompt}': {e}")
#         return []

# def vertexai_generate_images_with_retry(prompt, number_of_images=4, max_retries=2, initial_delay=5):
#     """
#     Attempts to generate images using Vertex AI with retry logic.
#     Retries on failure with exponential backoff and random jitter.
#     """
#     for attempt in range(max_retries):
#         images = vertexai_generate_images(prompt, number_of_images)
#         if images:
#             return images
#         else:
#             delay = initial_delay * (2 ** attempt) + random.random()
#             logger.warning(f"Retrying in {delay:.2f} seconds (Attempt {attempt + 1}/{max_retries}) for prompt: {prompt}")
#             time.sleep(delay)
#     logger.error(f"Failed to generate images after {max_retries} retries for prompt: {prompt}")
#     return []

# def generate_images_for_city(city_query, number_of_images=4):
#     """
#     Generates images for a city using Vertex AI Imagen.
#     Returns a list of PNG bytes for each generated image.
#     """
#     prompt = (
#         f'Generate a high-resolution, artistic image that showcases the unique culture, '
#         f'iconic architecture, and vibrant atmosphere of "{city_query}". '
#         f'The image should be realistic, colorful, and evoke a sense of wonder.'
#     )
#     logger.info(f"Generating images for prompt: {prompt}")
#     pil_images = vertexai_generate_images_with_retry(prompt, number_of_images)
#     if not pil_images:
#         logger.error(f"Failed to generate images for city '{city_query}'")
#         return []
#     image_bytes_list = []
#     for img in pil_images:
#         image_bytes_io = BytesIO()
#         img.save(image_bytes_io, format='PNG')
#         image_bytes_list.append(image_bytes_io.getvalue())
#     return image_bytes_list

# def upload_image_to_storage(city_id, image_bytes, image_index):
#     """
#     Uploads image bytes to Firebase Storage under the path "places/{city_id}/imagen_{image_index}.png".
#     Makes the blob public and returns the public URL.
#     """
#     filename = f"places/{city_id}/imagen_{image_index}.png"
#     blob = bucket.blob(filename)
#     try:
#         blob.upload_from_string(image_bytes, content_type="image/png")
#         blob.make_public()
#         return blob.public_url
#     except Exception as e:
#         logger.error(f"Error uploading image for city '{city_id}' index {image_index}: {e}")
#         return None

# def main():
#     logger.info("Starting Vertex AI Imagen City Image Generator script...")
    
#     # List of city IDs (Firestore document IDs in the "explore" collection)
#     city_ids = [
#         "10002", "10005", "10006", "10007", "10008", "10009", "10010",
#     ]
    
#     for city_id in city_ids:
#         doc_ref = db.collection('explore').document(city_id)
#         doc = doc_ref.get()
#         if not doc.exists:
#             logger.info(f"No document found for city {city_id}.")
#             continue
        
#         doc_data = doc.to_dict()
#         logger.info(f"\nProcessing city document with ID: {city_id}")
#         existing_image_url = doc_data.get('image_url')
        
#         # Constraint: Skip if image_url exists as a list with more than 3 URLs.
#         if existing_image_url and isinstance(existing_image_url, list) and len(existing_image_url) > 3:
#             logger.info(f"Skipping city {city_id} as it already has more than 3 image URLs.")
#             continue
        
#         # Build search query using city_name and country_name if available
#         city_name = doc_data.get('city_name', '')
#         country_name = doc_data.get('country_name', '')
#         if city_name and country_name:
#             city_query = f"{city_name} city in {country_name}"
#         elif city_name:
#             city_query = city_name
#         else:
#             city_query = city_id
        
#         # Generate images for the city using Vertex AI Imagen
#         image_bytes_list = generate_images_for_city(city_query, number_of_images=3)
#         if not image_bytes_list:
#             logger.error(f"No images generated for city {city_id} with query '{city_query}'.")
#             continue
        
#         # Upload each generated image to Firebase Storage using the same location as the previous script
#         new_image_urls = []
#         for index, image_bytes in enumerate(image_bytes_list, start=1):
#             public_url = upload_image_to_storage(city_id, image_bytes, index)
#             if public_url:
#                 new_image_urls.append(public_url)
#             else:
#                 logger.error(f"Failed to upload image {index} for city {city_id}.")
        
#         # Merge with any existing image URLs (without deleting them)
#         final_image_urls = new_image_urls[:]
#         if existing_image_url:
#             if isinstance(existing_image_url, list):
#                 final_image_urls.extend(existing_image_url)
#             elif isinstance(existing_image_url, str):
#                 final_image_urls.append(existing_image_url)
        
#         # Update the Firestore document if there are image URLs
#         if final_image_urls:
#             try:
#                 doc_ref.update({'image_url': final_image_urls})
#                 logger.info(f"Updated city {city_id} with image URLs: {final_image_urls}")
#             except Exception as e:
#                 logger.error(f"Error updating city {city_id}: {e}")
#         else:
#             logger.error(f"No image URLs available to update for city {city_id}.")
        
#         time.sleep(1)  # Optional delay between processing cities

# if __name__ == '__main__':
#     main()
