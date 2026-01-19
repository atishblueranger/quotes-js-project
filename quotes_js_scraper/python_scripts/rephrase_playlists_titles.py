import firebase_admin
from firebase_admin import credentials, firestore
import openai
import time
from google.api_core.exceptions import DeadlineExceeded

# Initialize Firebase Admin SDK
firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
cred = credentials.Certificate(firebase_credentials_path)
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)

# Initialize Firestore client
db = firestore.client()

# Set up OpenAI API Key (replace with your actual key)
openai.api_key = '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'

def rephrase_title(original_title):
    """
    Rephrase the original title into a cool, short, and professional 4-word phrase using gpt-4o-mini.
    """
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a creative assistant who rephrases titles into cool, short, and professional 4-word phrases. Ensure to keep main subject intact."
                },
                {
                    "role": "user",
                    "content": f"Rephrase the following title into a cool, short, and professional 4-word phrase:\n\n'{original_title}. Keep the main subject intact.'",
                },
            ],
            temperature=0.7,
            max_tokens=10,
        )
        rephrased_title = response.choices[0].message.content.strip().strip('"').strip("'")
        return rephrased_title
    except Exception as e:
        print(f"Error rephrasing title '{original_title}': {e}")
        return original_title

def generate_description(title):
    """
    Generate a professional brief description for the playlist based on its title (within 30 words) using gpt-4o-mini.
    """
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a creative assistant who writes engaging and professional descriptions for playlists based on their titles."
                },
                {
                    "role": "user",
                    "content": f"Write a professional and enticing brief description for a playlist titled '{title}'. The description should encourage users to explore the playlist. Limit the description to 20 words. Use simple vocabulary."
                },
            ],
            temperature=0.7,
            max_tokens=60,
        )
        description = response.choices[0].message.content.strip()
        return description
    except Exception as e:
        print(f"Error generating description for title '{title}': {e}")
        return ""

def assign_category(title):
    """
    Assign a category for the playlist based on its title.
    Allowed categories:
      "Food & Dining", "Romantic Spots", "Unique Stays & Accommodation",
      "Local Attractions", "NightLife & Entertainment", "Outdoor Activities & Nature",
      "Shopping & Souvenirs"
    Uses gpt-4o-mini to choose one and returns only the category name.
    """
    allowed_categories = {
        "Food & Dining",
        "Romantic Spots",
        "Unique Stays & Accommodation",
        "Local Attractions",
        "NightLife & Entertainment",
        "Outdoor Activities & Nature",
        "Shopping & Souvenirs"
    }
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": ("You are an assistant that categorizes playlist titles into one of these categories: "
                                "Food & Dining, Romantic Spots, Unique Stays & Accommodation, Local Attractions, "
                                "NightLife & Entertainment, Outdoor Activities & Nature, Shopping & Souvenirs. "
                                "Only return the category name.")
                },
                {
                    "role": "user",
                    "content": f"Based on the playlist title '{title}', which category does it best belong to?"
                },
            ],
            temperature=0.5,
            max_tokens=15,
        )
        category = response.choices[0].message.content.strip()
        # Validate against allowed categories; if not matched, do a simple fallback check.
        if category not in allowed_categories:
            for cat in allowed_categories:
                if cat.lower() in title.lower():
                    return cat
            return "Local Attractions"
        return category
    except Exception as e:
        print(f"Error assigning category for title '{title}': {e}")
        return "Local Attractions"

def get_all_playlists(playlists_ref, max_retries=3, timeout=300):
    """
    Attempt to stream all documents from a collection with an increased timeout.
    If a DeadlineExceeded error occurs, retry a few times.
    """
    for attempt in range(max_retries):
        try:
            # Using list() to force fetching all documents at once.
            return list(playlists_ref.stream(timeout=timeout))
        except DeadlineExceeded as e:
            print(f"Attempt {attempt+1}: DeadlineExceeded error while streaming playlists: {e}. Retrying...")
            time.sleep(5)
    print("Failed to fetch playlists after several retries.")
    return []

def process_playlists():
    """
    Process the 'playlistsNew' collection in Firestore:
      - Rephrase the 'title'
      - Generate a 'description'
      - Assign a 'category'
    The update is performed only if the existing document's 'category' field is an empty string.
    Then update each document with these fields.
    """
    playlists_ref = db.collection('playlistsNew')
    
    # Fetch playlists using our helper with extended timeout and retry logic.
    playlists = get_all_playlists(playlists_ref, max_retries=3, timeout=300)
    if not playlists:
        print("No playlists to process.")
        return

    for playlist in playlists:
        playlist_data = playlist.to_dict()
        original_title = playlist_data.get('title', '')
        doc_id = playlist.id

        # Check if the document already has a non-empty category field.
        existing_category = playlist_data.get('category', '').strip()
        if existing_category != "":
            print(f"Skipping document ID {doc_id} as category is already set to '{existing_category}'.")
            continue

        if not original_title:
            print(f"No title found for document ID {doc_id}")
            continue

        print(f"Processing document ID {doc_id}: '{original_title}'")

        # Rephrase the title
        new_title = rephrase_title(original_title)
        # Generate the description using the rephrased title
        description = generate_description(new_title)
        # Assign a category based on the rephrased title
        category = assign_category(new_title)

        # Prepare update data
        updates = {
            'title': new_title,
            'description': description,
            'category': category
        }

        try:
            playlist.reference.update(updates)
            print(f"Updated document ID {doc_id}:")
            print(f"  Title: '{original_title}' => '{new_title}'")
            print(f"  Description: '{description}'")
            print(f"  Category: '{category}'")
        except Exception as e:
            print(f"Error updating document ID {doc_id}: {e}")

        # Optional: Delay to avoid rate limits
        time.sleep(0.5)

if __name__ == "__main__":
    process_playlists()



# import firebase_admin
# from firebase_admin import credentials, firestore
# import openai
# import time
# from google.api_core.exceptions import DeadlineExceeded

# # Initialize Firebase Admin SDK
# firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# cred = credentials.Certificate(firebase_credentials_path)
# if not firebase_admin._apps:
#     firebase_admin.initialize_app(cred)

# # Initialize Firestore client
# db = firestore.client()

# # Set up OpenAI API Key (replace with your actual key)
# openai.api_key = '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'

# def rephrase_title(original_title):
#     """
#     Rephrase the original title into a cool, short, and professional 4-word phrase using gpt-4o-mini.
#     """
#     try:
#         response = openai.ChatCompletion.create(
#             model="gpt-4o-mini",
#             messages=[
#                 {
#                     "role": "system",
#                     "content": "You are a creative assistant who rephrases titles into cool, short, and professional 4-word phrases.Ensure to keep main subject intact."
#                 },
#                 {
#                     "role": "user",
#                     "content": f"Rephrase the following title into a cool, short, and professional 4-word phrase:\n\n'{original_title}. Keep the main subject intact.'",
#                 },
#             ],
#             temperature=0.7,
#             max_tokens=10,
#         )
#         rephrased_title = response.choices[0].message.content.strip().strip('"').strip("'")
#         return rephrased_title
#     except Exception as e:
#         print(f"Error rephrasing title '{original_title}': {e}")
#         return original_title

# def generate_description(title):
#     """
#     Generate a professional brief description for the playlist based on its title (within 30 words) using gpt-4o-mini.
#     """
#     try:
#         response = openai.ChatCompletion.create(
#             model="gpt-4o-mini",
#             messages=[
#                 {
#                     "role": "system",
#                     "content": "You are a creative assistant who writes engaging and professional descriptions for playlists based on their titles."
#                 },
#                 {
#                     "role": "user",
#                     "content": f"Write a professional and enticing brief description for a playlist titled '{title}'. The description should encourage users to explore the playlist. Limit the description to 20 words.Use simple vocabulary."
#                 },
#             ],
#             temperature=0.7,
#             max_tokens=60,
#         )
#         description = response.choices[0].message.content.strip()
#         return description
#     except Exception as e:
#         print(f"Error generating description for title '{title}': {e}")
#         return ""

# def assign_category(title):
#     """
#     Assign a category for the playlist based on its title.
#     Allowed categories:
#       "Food & Dining", "Romantic Spots", "Unique Stays & Accommodation",
#       "Local Attractions", "NightLife & Entertainment", "Outdoor Activities & Nature",
#       "Shopping & Souvenirs"
#     Uses gpt-4o-mini to choose one and returns only the category name.
#     """
#     allowed_categories = {
#         "Food & Dining",
#         "Romantic Spots",
#         "Unique Stays & Accommodation",
#         "Local Attractions",
#         "NightLife & Entertainment",
#         "Outdoor Activities & Nature",
#         "Shopping & Souvenirs"
#     }
#     try:
#         response = openai.ChatCompletion.create(
#             model="gpt-4o-mini",
#             messages=[
#                 {
#                     "role": "system",
#                     "content": ("You are an assistant that categorizes playlist titles into one of these categories: "
#                                 "Food & Dining, Romantic Spots, Unique Stays & Accommodation, Local Attractions, "
#                                 "NightLife & Entertainment, Outdoor Activities & Nature, Shopping & Souvenirs. "
#                                 "Only return the category name.")
#                 },
#                 {
#                     "role": "user",
#                     "content": f"Based on the playlist title '{title}', which category does it best belong to?"
#                 },
#             ],
#             temperature=0.5,
#             max_tokens=15,
#         )
#         category = response.choices[0].message.content.strip()
#         # Validate against allowed categories; if not matched, do a simple fallback check.
#         if category not in allowed_categories:
#             for cat in allowed_categories:
#                 if cat.lower() in title.lower():
#                     return cat
#             return "Local Attractions"
#         return category
#     except Exception as e:
#         print(f"Error assigning category for title '{title}': {e}")
#         return "Local Attractions"

# def get_all_playlists(playlists_ref, max_retries=3, timeout=300):
#     """
#     Attempt to stream all documents from a collection with an increased timeout.
#     If a DeadlineExceeded error occurs, retry a few times.
#     """
#     for attempt in range(max_retries):
#         try:
#             # Using list() to force fetching all documents at once.
#             return list(playlists_ref.stream(timeout=timeout))
#         except DeadlineExceeded as e:
#             print(f"Attempt {attempt+1}: DeadlineExceeded error while streaming playlists: {e}. Retrying...")
#             time.sleep(5)
#     print("Failed to fetch playlists after several retries.")
#     return []

# def process_playlists():
#     """
#     Process the 'playlists' collection in Firestore:
#       - Rephrase the 'title'
#       - Generate a 'description'
#       - Assign a 'category'
#     Then update each document with these fields.
#     """
#     playlists_ref = db.collection('playlistsNew')
    
#     # Fetch playlists using our helper with extended timeout and retry logic.
#     playlists = get_all_playlists(playlists_ref, max_retries=3, timeout=300)
#     if not playlists:
#         print("No playlists to process.")
#         return

#     for playlist in playlists:
#         playlist_data = playlist.to_dict()
#         original_title = playlist_data.get('title', '')
#         doc_id = playlist.id

#         if not original_title:
#             print(f"No title found for document ID {doc_id}")
#             continue

#         print(f"Processing document ID {doc_id}: '{original_title}'")

#         # Rephrase the title
#         new_title = rephrase_title(original_title)
#         # Generate the description using the rephrased title
#         description = generate_description(new_title)
#         # Assign a category based on the rephrased title
#         category = assign_category(new_title)

#         # Prepare update data
#         updates = {
#             'title': new_title,
#             'description': description,
#             'category': category
#         }

#         try:
#             playlist.reference.update(updates)
#             print(f"Updated document ID {doc_id}:")
#             print(f"  Title: '{original_title}' => '{new_title}'")
#             print(f"  Description: '{description}'")
#             print(f"  Category: '{category}'")
#         except Exception as e:
#             print(f"Error updating document ID {doc_id}: {e}")

#         # Optional: Delay to avoid rate limits
#         time.sleep(0.5)

# if __name__ == "__main__":
#     process_playlists()
















# import firebase_admin
# from firebase_admin import credentials, firestore
# import openai
# import time

# # Initialize Firebase Admin SDK
# # Replace 'path/to/serviceAccountKey.json' with the actual path to your service account key file
# firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# cred = credentials.Certificate(firebase_credentials_path)
# firebase_admin.initialize_app(cred)

# # Initialize Firestore client
# db = firestore.client()

# # Set up OpenAI API Key
# openai.api_key = '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'  # Replace with your OpenAI API key

# def rephrase_title(original_title):
#     """
#     Rephrase the original title into a cool, short, and professional 3-word phrase.
#     """
#     try:
#         response = openai.ChatCompletion.create(
#             model="gpt-3.5-turbo",  # or "gpt-4" if available
#             messages=[
#                 {
#                     "role": "system",
#                     "content": "You are a creative assistant who rephrases titles into cool, short, and professional 3-word phrases.",
#                 },
#                 {
#                     "role": "user",
#                     "content": f"Rephrase the following title into a cool, short, and professional 3-word phrase:\n\n'{original_title}'",
#                 },
#             ],
#             temperature=0.7,
#             max_tokens=10,
#         )
#         rephrased_title = response.choices[0].message.content.strip().strip('"').strip("'")
#         return rephrased_title
#     except Exception as e:
#         print(f"Error rephrasing title '{original_title}': {e}")
#         return original_title  # Return the original title if there's an error

# def generate_description(title):
#     """
#     Generate a professional brief description for the playlist based on its title.Within 30 words.
#     """
#     try:
#         response = openai.ChatCompletion.create(
#             model="gpt-3.5-turbo",  # Use "gpt-4" if you have access
#             messages=[
#                 {
#                     "role": "system",
#                     "content": "You are a creative assistant who writes engaging and professional descriptions for playlists based on their titles.",
#                 },
#                 {
#                     "role": "user",
#                     "content": f"Write a professional and enticing brief description for a playlist titled '{title}'. The description should encourage users to explore the playlist.Within 30 words",
#                 },
#             ],
#             temperature=0.7,
#             max_tokens=60,  # Adjust based on desired length
#         )
#         description = response.choices[0].message.content.strip()
#         return description
#     except Exception as e:
#         print(f"Error generating description for title '{title}': {e}")
#         return ""  # Return empty string if there's an error

# def process_playlists():
#     """
#     Process the 'playlists' collection in Firestore to rephrase the 'title' field and add 'description'.
#     """
#     playlists_ref = db.collection('playlists')
#     playlists = playlists_ref.stream()

#     for playlist in playlists:
#         playlist_data = playlist.to_dict()
#         original_title = playlist_data.get('title', '')
#         doc_id = playlist.id

#         if not original_title:
#             print(f"No title found for document ID {doc_id}")
#             continue

#         print(f"Processing document ID {doc_id}: '{original_title}'")

#         # Rephrase the title
#         new_title = rephrase_title(original_title)

#         # Generate the description
#         description = generate_description(new_title)

#         # Update the Firestore document
#         updates = {'title': new_title}
#         if description:
#             updates['description'] = description

#         try:
#             playlist.reference.update(updates)
#             print(f"Updated document ID {doc_id}:")
#             print(f"  Title: '{original_title}' => '{new_title}'")
#             print(f"  Description: '{description}'")
#         except Exception as e:
#             print(f"Error updating document ID {doc_id}: {e}")

#         # Optional: Delay to avoid rate limits
#         time.sleep(0.5)  # Adjust the sleep time as needed

# if __name__ == "__main__":
#     process_playlists()


# Rephrasing title script working
# import firebase_admin
# from firebase_admin import credentials, firestore
# import openai
# import time

# # Initialize Firebase Admin SDK
# # Replace 'path/to/serviceAccountKey.json' with the actual path to your service account key file
# firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# cred = credentials.Certificate(firebase_credentials_path)
# firebase_admin.initialize_app(cred)

# # Initialize Firestore client
# db = firestore.client()

# # Set up OpenAI API Key
# openai.api_key = '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'  # Replace with your OpenAI API key

# def rephrase_title(original_title):
#     """
#     Rephrase the original title into a cool, short, and professional 3-word phrase.
#     """
#     try:
#         # Use OpenAI's ChatCompletion API with the latest syntax
#         response = openai.ChatCompletion.create(
#             model="gpt-3.5-turbo",  # Use "gpt-4" if you have access
#             messages=[
#                 {
#                     "role": "system",
#                     "content": "You are a helpful assistant that rephrases titles into cool, short, and professional 3-word phrases.",
#                 },
#                 {
#                     "role": "user",
#                     "content": f"Rephrase the following title into a cool, short, and professional 3-word phrase:\n\n'{original_title}'",
#                 },
#             ],
#             temperature=0.7,
#             max_tokens=10,
#         )
#         # Extract the assistant's reply
#         rephrased_title = response.choices[0].message.content.strip().strip('"').strip("'")
#         return rephrased_title
#     except Exception as e:
#         print(f"Error rephrasing title '{original_title}': {e}")
#         return original_title  # Return the original title if there's an error

# def process_playlists():
#     """
#     Process the 'playlists' collection in Firestore to rephrase the 'title' field.
#     """
#     playlists_ref = db.collection('playlists')
#     playlists = playlists_ref.stream()

#     for playlist in playlists:
#         playlist_data = playlist.to_dict()
#         original_title = playlist_data.get('title', '')
#         doc_id = playlist.id

#         if not original_title:
#             print(f"No title found for document ID {doc_id}")
#             continue

#         print(f"Processing document ID {doc_id}: '{original_title}'")

#         # Rephrase the title
#         new_title = rephrase_title(original_title)

#         # Update the Firestore document if the title has changed
#         if new_title != original_title:
#             try:
#                 playlist.reference.update({'title': new_title})
#                 print(f"Updated document ID {doc_id}: '{original_title}' => '{new_title}'")
#             except Exception as e:
#                 print(f"Error updating document ID {doc_id}: {e}")
#         else:
#             print(f"No change for document ID {doc_id}: '{original_title}' remains the same.")

#         # Optional: Delay to avoid rate limits
#         time.sleep(0.5)  # Adjust the sleep time as needed

# if __name__ == "__main__":
#     process_playlists()

