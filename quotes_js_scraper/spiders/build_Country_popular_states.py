#!/usr/bin/env python3
"""
Trial run: Populate `popular_states` (now including provinces) for a LIST of SPECIFIC countries
by aggregating the popularity of child cities, include image_url, and output to JSON.
"""

import json
import firebase_admin
from firebase_admin import credentials, firestore

# --- Configuration ---
SERVICE_ACCOUNT_JSON = (
    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
    r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
)

# List of Country IDs to process
TARGET_COUNTRY_IDS = ["10165", "10705", "135383", "135385", "135391", "135393", "135396", "135408", "135438", "135468", "135498", "135642", "135733", "135766", "136048", "145918", "3651", "79304", "81176", "81178", "81179", "81184", "81185", "81188", "81191", "81192", "81193", "81204", "81214", "81238", "81239", "81309", "83438", "84883", "86647", "86651", "86652", "86653", "86654", "86655", "86656", "86659", "86661", "86663", "86667", "86668", "86683", "86693", "86698", "86719", "86729", "86731", "86772", "86776", "86779", "86797", "86819", "86831", "86851", "86865", "86938", "86983", "87077", "87139", "87175", "87389", "88352", "88358", "88359", "88360", "88362", "88366", "88367", "88368", "88373", "88375", "88380", "88384", "88386", "88394", "88402", "88403", "88405", "88407", "88408", "88419", "88420", "88423", "88432", "88434", "88437", "88438", "88445", "88449", "88455", "88464", "88477", "88478", "88486", "88503", "88527", "88597", "88609", "88654", "88723", "88727", "88731", "88978", "89073", "89098", "89102", "89196", "89330", "90374", "90407", "90647", "90648", "90651", "90652", "90656", "90660", "90663", "90759", "90761", "90762", "90763", "90764", "90774", "90776", "90785", "90788", "90790", "90791", "90796", "90797", "90798", "90801", "90806", "90810", "90815", "90816", "90817", "90821", "90823", "90828", "90832", "90840", "90841", "90847", "90848", "90852", "90855", "90857", "90858", "90861", "90862", "90869", "90871", "90872", "90877", "90882", "90883", "90884", "90889", "90899", "90900", "90906", "90917", "90919", "90922", "90934", "90940", "90947", "90948", "90950", "90951", "90962", "90963", "91219", "91220", "91223", "91224", "91227", "91232", "91233", "91235", "91342", "91387", "91394", "91403", "91409", "91413", "91416", "91420", "91427", "91433", "91440", "91453", "91463", "91473", "91474", "91478", "91479", "91491", "91506", "91507", "91516", "91524", "91525", "91526", "91530", "91534", "91535", "91536", "91537", "91540", "91548", "91554", "91555", "91564", "91579"]

OUTPUT_JSON_FILE = "trial_run_popular_states_output_multiple2.json"
TOP_REGIONS = 10
CITY_CATEGORIES = {"city", "municipality"}
REGION_CATEGORIES = {"state", "province"}  # <-- now includes provinces

# --- Firestore init ---
if not firebase_admin._apps:
    firebase_admin.initialize_app(
        credentials.Certificate(SERVICE_ACCOUNT_JSON)
    )
db = firestore.client()

# --- Helpers ---
def slim(doc):
    """Creates a simplified dict from a Firestore document snapshot."""
    d = doc.to_dict() or {}
    pid = d.get("place_id", doc.id)
    try:
        pid = int(pid)
    except (ValueError, TypeError):
        pid = str(pid)

    return {
        "place_id": pid,
        "name":  d.get("city_name") ,
        "latitude": d.get("latitude"),
        "longitude": d.get("longitude"),
        "country_name": d.get("country_name"),
        "popularity": d.get("popularity"),
        "image_url": d.get("image_url", None),
    }

def child_cities_of_region(region_id):
    """Yield all child-city snapshots under a given state or province."""
    return (
        db.collection("allplaces")
          .where("parent_id", "==", region_id)
          .where("subcategory", "in", list(CITY_CATEGORIES))
          .stream()
    )

def score_region(region_doc):
    """Sum popularity of all child cities to build a region score."""
    try:
        rid = int(region_doc.id)
    except ValueError:
        print(f"Warning: Invalid region ID format: {region_doc.id}. Skipping.")
        return 0

    total = 0
    for city in child_cities_of_region(rid):
        c = city.to_dict() or {}
        try:
            total += float(c.get("popularity", 0))
        except (ValueError, TypeError):
            pass
    return total

def get_popular_regions_for_country(country_doc):
    """
    Calculates and returns the top regions (states OR provinces)
    for a given country, based on aggregated city popularity.
    """
    cid = country_doc.id
    print(f"Processing regions for country ID: {cid}…")
    try:
        country_id = int(cid)
    except ValueError:
        print(f"Error: Invalid country ID format: {cid}.")
        return []

    # <-- filter on both "state" and "province" now -->
    region_snaps = (
        db.collection("allplaces")
          .where("parent_id", "==", country_id)
          .where("subcategory", "in", list(REGION_CATEGORIES))
          .stream()
    )

    scored = []
    count = 0
    for reg in region_snaps:
        count += 1
        scored.append((score_region(reg), reg))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:TOP_REGIONS]

    print(f"  Found {count} regions; selecting top {len(top)}.")
    return [slim(reg) for _, reg in top]

# --- Main run ---
def main():
    print(f"--- Trial Run for Countries: {', '.join(TARGET_COUNTRY_IDS)} ---")
    results = {}

    for cid in TARGET_COUNTRY_IDS:
        print(f"\n--- Country ID: {cid} ---")
        doc = db.collection("allplaces").document(cid).get()
        if not doc.exists:
            print(f"  ❌ Country {cid} not found, skipping.")
            continue

        country_info = slim(doc)
        popular_regions = get_popular_regions_for_country(doc)

        results[cid] = {
            "country_info": country_info,
            "popular_states": popular_regions,  # key name kept for backwards-compatibility
        }
        print(f"--- Done with Country ID: {cid} ---")

    if not results:
        print("No results generated; exiting.")
        return

    print(f"\nWriting results to {OUTPUT_JSON_FILE}…")
    with open(OUTPUT_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False, default=str)
    print("✅ Completed successfully.")

if __name__ == "__main__":
    main()

# #!/usr/bin/env python3
# """
# Trial run: Populate `popular_states` for a LIST of SPECIFIC countries
# by aggregating the popularity of child cities, include image_url,
# and output to JSON.
# """

# import json
# import firebase_admin
# from firebase_admin import credentials, firestore

# # --- Configuration ---
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json" # <<< YOUR PATH
# # --- List of Country IDs to process ---
# TARGET_COUNTRY_IDS = ["10165", "10705", "135383", "135385", "135391", "135393", "135396", "135408", "135438", "135468", "135498", "135642", "135733", "135766", "136048", "145918", "3651", "79304", "81176", "81178", "81179", "81184", "81185", "81188", "81191", "81192", "81193", "81204", "81214", "81238", "81239", "81309", "83438", "84883", "86647", "86651", "86652", "86653", "86654", "86655", "86656", "86659", "86661", "86663", "86667", "86668", "86683", "86693", "86698", "86719", "86729", "86731", "86772", "86776", "86779", "86797", "86819", "86831", "86851", "86865", "86938", "86983", "87077", "87139", "87175", "87389", "88352", "88358", "88359", "88360", "88362", "88366", "88367", "88368", "88373", "88375", "88380", "88384", "88386", "88394", "88402", "88403", "88405", "88407", "88408", "88419", "88420", "88423", "88432", "88434", "88437", "88438", "88445", "88449", "88455", "88464", "88477", "88478", "88486", "88503", "88527", "88597", "88609", "88654", "88723", "88727", "88731", "88978", "89073", "89098", "89102", "89196", "89330", "90374", "90407", "90647", "90648", "90651", "90652", "90656", "90660", "90663", "90759", "90761", "90762", "90763", "90764", "90774", "90776", "90785", "90788", "90790", "90791", "90796", "90797", "90798", "90801", "90806", "90810", "90815", "90816", "90817", "90821", "90823", "90828", "90832", "90840", "90841", "90847", "90848", "90852", "90855", "90857", "90858", "90861", "90862", "90869", "90871", "90872", "90877", "90882", "90883", "90884", "90889", "90899", "90900", "90906", "90917", "90919", "90922", "90934", "90940", "90947", "90948", "90950", "90951", "90962", "90963", "91219", "91220", "91223", "91224", "91227", "91232", "91233", "91235", "91342", "91387", "91394", "91403", "91409", "91413", "91416", "91420", "91427", "91433", "91440", "91453", "91463", "91473", "91474", "91478", "91479", "91491", "91506", "91507", "91516", "91524", "91525", "91526", "91530", "91534", "91535", "91536", "91537", "91540", "91548", "91554", "91555", "91564", "91579"]
# # --- ---
# OUTPUT_JSON_FILE = "trial_run_popular_states_output_multiple.json" # Renamed output file
# TOP_STATES = 10
# CITY_CATEGORIES = {"city", "municipality"} # Still needed for scoring states

# # --- Firestore init ---
# if not firebase_admin._apps:
#     firebase_admin.initialize_app(
#         credentials.Certificate(SERVICE_ACCOUNT_JSON))
# db = firestore.client()

# # --- Helpers ---
# def slim(doc):
#     """Creates a simplified dictionary from a Firestore document snapshot."""
#     d = doc.to_dict()
#     if not d:
#         return None

#     place_id_val = d.get("place_id")
#     try:
#         final_place_id = int(place_id_val) if place_id_val is not None else int(doc.id)
#     except (ValueError, TypeError):
#         final_place_id = doc.id if isinstance(doc.id, (int, str)) else None
#         print(f"Warning: Could not determine integer place_id for doc {doc.id}. Using '{final_place_id}'.")

#     return {
#         "place_id": final_place_id,
#         "name":     d.get("name") or d.get("city_name") or d.get("state_name"),
#         "latitude": d.get("latitude"),
#         "longitude": d.get("longitude"),
#         "country_name": d.get("country_name"),
#         "image_url": d.get("image_url", None)
#     }

# def child_cities_of_state(state_id):
#     """Return generator of child-city snapshots under a given state."""
#     return (db.collection("allplaces")
#               .where("parent_id", "==", state_id)
#               .where("subcategory", "in", list(CITY_CATEGORIES))
#               .stream())

# def score_state(state_doc):
#     """Sum popularity of all child cities to build a state score."""
#     total = 0
#     try:
#         state_id = int(state_doc.id)
#     except ValueError:
#         print(f"Warning: Invalid state ID format: {state_doc.id}. Skipping scoring.")
#         return 0

#     for city in child_cities_of_state(state_id):
#         city_data = city.to_dict()
#         if city_data:
#             try:
#                 popularity = float(city_data.get("popularity", 0))
#             except (ValueError, TypeError):
#                 popularity = 0
#             total += popularity
#     return total

# def get_popular_states_for_country(country_doc):
#     """Calculates and returns the data for popular states for a given country."""
#     print(f"Processing states for country ID: {country_doc.id}...")
#     try:
#         country_id = int(country_doc.id)
#     except ValueError:
#         print(f"Error: Invalid country ID format: {country_doc.id}. Cannot find states.")
#         return [] # Return empty list

#     state_snapshots = (db.collection("allplaces")
#                          .where("parent_id", "==", country_id)
#                          .where("subcategory", "==", "state")
#                          .stream())

#     scored_states = []
#     state_count = 0
#     for state in state_snapshots:
#         state_count += 1
#         score = score_state(state)
#         scored_states.append((score, state))

#     scored_states.sort(key=lambda tup: tup[0], reverse=True)
#     top_scored_states = scored_states[:TOP_STATES]

#     top_states_data = [slim(s) for _, s in top_scored_states if slim(s)]

#     print(f"  Found {state_count} states, selected top {len(top_states_data)} based on aggregated city popularity.")
#     return top_states_data

# # --- Main run ---
# def main():
#     print(f"--- Starting Trial Run for Countries: {', '.join(TARGET_COUNTRY_IDS)} ---")
#     results = {} # Initialize results dictionary OUTSIDE the loop

#     # Loop through each specified country ID
#     for country_id_str in TARGET_COUNTRY_IDS:
#         print(f"\n--- Processing Country ID: {country_id_str} ---")

#         # Fetch the specific country document
#         country_ref = db.collection("allplaces").document(country_id_str)
#         country_doc = country_ref.get()

#         if not country_doc.exists:
#             print(f"Error: Country with ID '{country_id_str}' not found in 'allplaces' collection. Skipping.")
#             continue # Skip to the next country ID in the list

#         country_data = slim(country_doc)
#         if not country_data:
#              print(f"Error: Could not extract data for country ID '{country_id_str}'. Skipping.")
#              continue # Skip to the next country ID

#         # Get popular states data for this country
#         popular_states_data = get_popular_states_for_country(country_doc)

#         # Add this country's data to the results dictionary
#         results[country_id_str] = {
#             "country_info": country_data,
#             "popular_states": popular_states_data,
#         }
#         print(f"--- Finished processing Country ID: {country_id_str} ---")


#     # Write accumulated results to JSON file AFTER the loop finishes
#     if not results:
#         print("\nNo data processed for any country. Output file will be empty or not created.")
#         # Decide if you want to write an empty file or skip writing
#         # Option 1: Write empty JSON
#         # output_data_to_write = {}
#         # Option 2: Skip writing
#         print("Skipping file write as no results were generated.")
#         return

#     output_data_to_write = results
#     print(f"\nWriting accumulated results for {len(output_data_to_write)} countries to {OUTPUT_JSON_FILE}...")
#     try:
#         with open(OUTPUT_JSON_FILE, 'w', encoding='utf-8') as f:
#             json.dump(output_data_to_write, f, indent=4, ensure_ascii=False, default=str)
#         print(f"--- Successfully completed trial run. Results saved to {OUTPUT_JSON_FILE} ---")
#     except IOError as e:
#         print(f"Error writing JSON file: {e}")
#     except TypeError as e:
#         print(f"Error serializing data to JSON: {e}")
#         print("Problematic data structure might contain non-standard types.")


# if __name__ == "__main__":
#     main()




# #!/usr/bin/env python3
# """
# Trial run: Populate `popular_states` for a SPECIFIC country
# by aggregating the popularity of child cities, and output to JSON.
# Optionally includes popular cities for each state of that country.
# """

# import json # Added for JSON output
# import firebase_admin
# from firebase_admin import credentials, firestore

# # --- Configuration ---
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json" # <<< UPDATE THIS PATH
# TARGET_COUNTRY_ID = "86661" # <<< SET THE COUNTRY ID TO TEST
# OUTPUT_JSON_FILE = "trial_run_output.json"
# TOP_STATES = 10
# TOP_CITIES = 10
# CITY_CATEGORIES = {"city", "municipality"} # adjust if needed
# INCLUDE_POPULAR_CITIES = True # Set to False if you only want popular states

# # --- Firestore init ---
# if not firebase_admin._apps:
#     firebase_admin.initialize_app(
#         credentials.Certificate(SERVICE_ACCOUNT_JSON))
# db = firestore.client()

# # --- Helpers ---
# def slim(doc):
#     """Creates a simplified dictionary from a Firestore document snapshot."""
#     d = doc.to_dict()
#     if not d: # Handle cases where doc might be empty unexpectedly
#         return None
#     return {
#         "place_id": d.get("place_id") or int(doc.id),
#         "name":     d.get("name") or d.get("city_name") or d.get("state_name"),
#         "latitude": d.get("latitude"),
#         "longitude": d.get("longitude"),
#         "country_name": d.get("country_name"),
#         # Add other simple fields if needed, e.g. 'popularity': d.get('popularity')
#     }

# def child_cities_of_state(state_id):
#     """Return generator of child-city snapshots under a given state."""
#     return (db.collection("allplaces")
#               .where("parent_id", "==", state_id)
#               .where("subcategory", "in", list(CITY_CATEGORIES))
#               .stream())

# def score_state(state_doc):
#     """Sum popularity of all child cities to build a state score."""
#     total = 0
#     try:
#         state_id = int(state_doc.id)
#     except ValueError:
#         print(f"Warning: Invalid state ID format: {state_doc.id}. Skipping scoring.")
#         return 0

#     for city in child_cities_of_state(state_id):
#         city_data = city.to_dict()
#         if city_data:
#             total += city_data.get("popularity", 0)
#     return total # can be 0 if no cities or cities have no popularity

# def get_popular_states_for_country(country_doc):
#     """Calculates and returns the data for popular states for a given country."""
#     print(f"Processing states for country ID: {country_doc.id}...")
#     try:
#         country_id = int(country_doc.id)
#     except ValueError:
#         print(f"Error: Invalid country ID format: {country_doc.id}. Cannot find states.")
#         return [], [] # Return empty lists

#     state_snapshots = (db.collection("allplaces")
#                          .where("parent_id", "==", country_id)
#                          .where("subcategory", "==", "state")
#                          .stream())

#     scored_states = []
#     processed_states = [] # Keep track of all states processed for this country
#     for state in state_snapshots:
#         processed_states.append(state) # Add state to the list of processed states
#         score = score_state(state)
#         # Optional: skip states with zero calculated score
#         # if score == 0:
#         #     continue
#         scored_states.append((score, state))
#         # print(f"  State {state.id}: score {score}") # Uncomment for debugging scores

#     # sort by score desc and keep top N
#     scored_states.sort(key=lambda tup: tup[0], reverse=True)
#     top_scored_states = scored_states[:TOP_STATES]

#     # Convert top states to slim format for output
#     top_states_data = [slim(s) for _, s in top_scored_states if slim(s)] # Filter out None results from slim

#     print(f"  Found {len(scored_states)} states, selected top {len(top_states_data)}.")
#     # Return the slimmed data of top states AND the full snapshot objects of ALL processed states
#     return top_states_data, processed_states

# def get_popular_cities_for_state(state_doc):
#     """Gets and returns the data for popular cities for a given state."""
#     print(f"  Processing popular cities for state ID: {state_doc.id}...")
#     try:
#         state_id = int(state_doc.id)
#     except ValueError:
#         print(f"  Warning: Invalid state ID format: {state_doc.id}. Skipping cities.")
#         return []

#     child_cities_query = (db.collection("allplaces")
#                            .where("parent_id", "==", state_id)
#                            .where("subcategory", "in", list(CITY_CATEGORIES))
#                            .order_by("popularity", direction=firestore.Query.DESCENDING)
#                            .limit(TOP_CITIES))

#     child_cities_snapshots = child_cities_query.stream()

#     top_cities_data = [slim(c) for c in child_cities_snapshots if slim(c)] # Filter out None results

#     print(f"    Found top {len(top_cities_data)} cities for state {state_doc.id}.")
#     return top_cities_data

# # --- Main run ---
# def main():
#     print(f"--- Starting Trial Run for Country ID: {TARGET_COUNTRY_ID} ---")
#     results = {}

#     # Fetch the specific country document
#     country_ref = db.collection("allplaces").document(TARGET_COUNTRY_ID)
#     country_doc = country_ref.get()

#     if not country_doc.exists:
#         print(f"Error: Country with ID '{TARGET_COUNTRY_ID}' not found in 'allplaces' collection.")
#         return

#     country_data = slim(country_doc)
#     if not country_data:
#          print(f"Error: Could not extract data for country ID '{TARGET_COUNTRY_ID}'.")
#          return

#     country_id_str = country_doc.id # Use the actual document ID as the key

#     # Get popular states data and all processed state snapshots for this country
#     popular_states_data, all_country_states = get_popular_states_for_country(country_doc)

#     results[country_id_str] = {
#         "country_info": country_data,
#         "popular_states": popular_states_data,
#         "states_details": {} # Initialize dict to hold details for each state
#     }

#     # Optional: Get popular cities for each state found in this country
#     if INCLUDE_POPULAR_CITIES and all_country_states:
#         print(f"\nProcessing popular cities for states of country {country_id_str}...")
#         for state_doc in all_country_states: # Use the list of states we already found
#              state_id_str = state_doc.id
#              state_data = slim(state_doc)
#              if not state_data:
#                  print(f"  Warning: Could not extract data for state ID '{state_id_str}'. Skipping cities.")
#                  continue

#              popular_cities_data = get_popular_cities_for_state(state_doc)
#              results[country_id_str]["states_details"][state_id_str] = {
#                  "state_info": state_data,
#                  "popular_cities": popular_cities_data
#              }
#         print("Finished processing popular cities.")
#     elif not INCLUDE_POPULAR_CITIES:
#          print("\nSkipping popular cities processing as INCLUDE_POPULAR_CITIES is False.")
#     else:
#          print("\nNo states found for this country to process popular cities.")


#     # Write results to JSON file
#     print(f"\nWriting results to {OUTPUT_JSON_FILE}...")
#     try:
#         with open(OUTPUT_JSON_FILE, 'w', encoding='utf-8') as f:
#             json.dump(results, f, indent=4, ensure_ascii=False)
#         print(f"--- Successfully completed trial run. Results saved to {OUTPUT_JSON_FILE} ---")
#     except IOError as e:
#         print(f"Error writing JSON file: {e}")
#     except TypeError as e:
#         print(f"Error serializing data to JSON (maybe due to Firestore types?): {e}")
#         print("Problematic data structure:", results)


# if __name__ == "__main__":
#     main()