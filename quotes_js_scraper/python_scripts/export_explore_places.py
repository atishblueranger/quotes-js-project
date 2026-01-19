import os
import json
import firebase_admin
from firebase_admin import credentials, firestore

# We need to know the current date for time-dependent questions, but this script doesn't have time-dependent logic itself.
# However, if popularity changes frequently, running the script reflects the data state at runtime.
# Current Date: Monday, March 31, 2025

def initialize_firestore():
    """Initializes the Firestore client."""
    # --- IMPORTANT: Path Handling ---
    # Ensure this path is correct for your system.
    firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

    if not os.path.exists(firebase_credentials_path):
        print(f"ERROR: Firebase credentials file not found at: {firebase_credentials_path}")
        return None

    try:
        cred = credentials.Certificate(firebase_credentials_path)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        print("Firebase Admin SDK initialized successfully.")
        return firestore.client()
    except ValueError as ve:
         if 'already exists' in str(ve):
             print("Firebase Admin SDK already initialized.")
             return firestore.client()
         else:
            print(f"ERROR initializing Firebase: {ve}")
            return None
    except Exception as e:
        print(f"ERROR initializing Firebase: {e}")
        return None

# Function name is kept, but docstring and comments updated for AND logic
def find_and_export_flagged_docs(db, output_filename):
    """
    Checks documents in the 'explore' collection based on combined criteria:
    1. 'image_url' field is missing, not a list, or has length <= 2.
    AND
    2. 'popularity' field exists, is numeric, and is > 10000.
    Exports the IDs of documents meeting BOTH criteria to a JSON file.

    Args:
        db: The Firestore client object.
        output_filename (str): The name of the JSON file to save the IDs.
    """
    if not db:
        print("Firestore client is not available. Cannot perform check.")
        return

    collection_ref = db.collection('explore')
    flagged_docs_count = 0
    total_docs_scanned = 0
    flagged_ids = []

    # Update print statement for AND condition
    print(f"\nChecking documents in '{collection_ref.id}' collection for issues:")
    print(" - 'image_url' missing, non-list, or length <= 2")
    print(" AND ") # Emphasize the AND logic
    print(" - 'popularity' is numeric and > 10000")
    print("Scanning...")

    try:
        # Stream documents for memory efficiency
        for doc in collection_ref.stream():
            total_docs_scanned += 1
            doc_data = doc.to_dict() # Get document data as a dictionary

            # --- Check image_url condition ---
            image_url_field = doc_data.get('image_url')
            image_url_is_problematic = False
            if image_url_field is None:
                image_url_is_problematic = True
            elif not isinstance(image_url_field, list):
                image_url_is_problematic = True
            elif len(image_url_field) <= 2:
                image_url_is_problematic = True

            # --- Check popularity condition ---
            popularity_field = doc_data.get('popularity')
            popularity_is_problematic = False
            # IMPORTANT: Check field exists AND is a number (int or float) before comparing
            if popularity_field is not None and isinstance(popularity_field, (int, float)) and popularity_field > 10000:
                popularity_is_problematic = True

            # --- Combine conditions: Flag ONLY if BOTH are true ---
            # *** This is the key change: using 'and' instead of 'or' ***
            if image_url_is_problematic and popularity_is_problematic:
                flagged_docs_count += 1
                flagged_ids.append(doc.id)
                # Optional debug print
                # print(f"  - Flagged Doc ID: {doc.id} (ImageUrl issue AND Popularity>10k)")

    except Exception as e:
        print(f"\nAn error occurred while streaming documents: {e}")
        print("Processing stopped. Results gathered so far will be written.")

    print("\n--- Check Complete ---")
    print(f"Total documents scanned in '{collection_ref.id}': {total_docs_scanned}")
    # Update summary print statement for AND logic
    print(f"Number of documents flagged (image_url issue AND popularity > 10000): {flagged_docs_count}")

    # --- Write the flagged IDs to JSON file ---
    # (This part remains the same logic, just writes the IDs found via AND)
    print(f"\nWriting {len(flagged_ids)} flagged document IDs to {output_filename}...")
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(flagged_ids, f, ensure_ascii=False, indent=4)
        print(f"Successfully saved flagged IDs to {output_filename}")
    except IOError as e:
        print(f"ERROR: Could not write to file {output_filename}. Error: {e}")
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during JSON writing. Error: {e}")


# Main execution block
if __name__ == "__main__":
    print("Starting script...")
    db_client = initialize_firestore()

    if db_client:
        # Suggest a new output filename reflecting the AND condition
        json_output_file = "explore_docs_img_issue_and_high_popularity2.json"
        find_and_export_flagged_docs(db_client, json_output_file)
    else:
        print("Exiting script due to Firestore initialization failure.")


# import os
# import json # Need json for writing the output file
# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore():
#     """Initializes the Firestore client."""
#     # --- IMPORTANT: Path Handling ---
#     # Ensure this path is correct for your system.
#     firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

#     if not os.path.exists(firebase_credentials_path):
#         print(f"ERROR: Firebase credentials file not found at: {firebase_credentials_path}")
#         return None

#     try:
#         cred = credentials.Certificate(firebase_credentials_path)
#         if not firebase_admin._apps:
#             firebase_admin.initialize_app(cred)
#         print("Firebase Admin SDK initialized successfully.")
#         return firestore.client()
#     except ValueError as ve:
#          if 'already exists' in str(ve):
#              print("Firebase Admin SDK already initialized.")
#              return firestore.client()
#          else:
#             print(f"ERROR initializing Firebase: {ve}")
#             return None
#     except Exception as e:
#         print(f"ERROR initializing Firebase: {e}")
#         return None

# def check_and_export_image_urls(db, output_filename):
#     """
#     Checks documents in the 'explore' collection for 'image_url' fields that
#     are missing, not a list, or have length <= 2. Exports the IDs of
#     such documents to a JSON file.

#     Args:
#         db: The Firestore client object.
#         output_filename (str): The name of the JSON file to save the IDs.
#     """
#     if not db:
#         print("Firestore client is not available. Cannot perform check.")
#         return

#     collection_ref = db.collection('explore')
#     problematic_docs_count = 0
#     total_docs_scanned = 0
#     problematic_ids = [] # List to store IDs of documents meeting the criteria

#     print(f"\nChecking documents in '{collection_ref.id}' collection for 'image_url' issues (missing, non-list, or length <= 2)...")

#     try:
#         # Stream documents for memory efficiency
#         for doc in collection_ref.stream():
#             total_docs_scanned += 1
#             doc_data = doc.to_dict() # Get document data as a dictionary
#             image_url_field = doc_data.get('image_url') # Use .get() for safe access

#             is_problematic = False
#             # Condition 1: Field is missing (get returns None)
#             if image_url_field is None:
#                 is_problematic = True
#             # Condition 2: Field exists but is not a list
#             elif not isinstance(image_url_field, list):
#                 is_problematic = True
#                 # print(f"  - Issue found in Doc ID: {doc.id}. 'image_url' is not a list. Value: {type(image_url_field)}") # Optional debug
#             # Condition 3: Field is a list, check its length
#             elif len(image_url_field) <= 2:
#                 is_problematic = True
#                 # print(f"  - Issue found in Doc ID: {doc.id}. 'image_url' list length is {len(image_url_field)}.") # Optional debug

#             # If any condition was met, count it and store the ID
#             if is_problematic:
#                 problematic_docs_count += 1
#                 problematic_ids.append(doc.id)

#     except Exception as e:
#         print(f"\nAn error occurred while streaming documents: {e}")
#         print("Processing stopped. Results gathered so far will be written.")
#         # Decide if you want to stop or continue based on the error

#     print("\n--- Check Complete ---")
#     print(f"Total documents scanned in '{collection_ref.id}': {total_docs_scanned}")
#     print(f"Number of documents with problematic 'image_url' (missing, non-list, or length <= 2): {problematic_docs_count}")

#     # --- Write the problematic IDs to JSON file ---
#     print(f"\nWriting {len(problematic_ids)} problematic document IDs to {output_filename}...")
#     try:
#         with open(output_filename, 'w', encoding='utf-8') as f:
#             # Use json.dump to write the list to the file
#             # ensure_ascii=False preserves non-English characters if IDs have them
#             # indent=4 makes the JSON file readable
#             json.dump(problematic_ids, f, ensure_ascii=False, indent=4)
#         print(f"Successfully saved problematic IDs to {output_filename}")
#     except IOError as e:
#         # Catch errors during file writing (e.g., permissions)
#         print(f"ERROR: Could not write to file {output_filename}. Error: {e}")
#     except Exception as e:
#         # Catch other potential errors during JSON serialization
#         print(f"ERROR: An unexpected error occurred during JSON writing. Error: {e}")


# # Main execution block
# if __name__ == "__main__":
#     print("Starting script...")
#     db_client = initialize_firestore()

#     if db_client:
#         # Define the desired output filename for the JSON
#         json_output_file = "explore_cities_low_or_no_images.json"
#         check_and_export_image_urls(db_client, json_output_file)
#     else:
#         print("Exiting script due to Firestore initialization failure.")




# Export ids of all explore collection cities
# import os
# import json
# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore():
#     # Replace with the path to your Firebase service account key file
#     firebase_credentials_path =os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)
#     return firestore.client()

# def export_place_ids(db, output_file):
#     # Reference to the "explore" collection
#     collection_ref = db.collection('explore')
    
#     # Retrieve all document IDs (place_ids) from the collection
#     place_ids = [doc.id for doc in collection_ref.stream()]
    
#     # Write the list of place_ids to a JSON file
#     with open(output_file, 'w', encoding='utf-8') as f:
#         json.dump(place_ids, f, ensure_ascii=False, indent=4)
    
#     print(f"Place IDs exported to {output_file}")

# if __name__ == "__main__":
#     db = initialize_firestore()
#     output_file = "explore_place_ids.json"
#     export_place_ids(db, output_file)
