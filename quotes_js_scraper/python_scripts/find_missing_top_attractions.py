# import os
# import json
# from typing import List, Dict

# import firebase_admin
# from firebase_admin import credentials, firestore

# # ───── CONFIG ─────────────────────────────────────────────────────────────────

# # Path to your Firebase service account JSON
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# # Path to your input JSON file (the Wanderlog geoCategory list)
# INPUT_JSON_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wanderlog_explore_attractions_full_urls_data2.json"

# # Output file containing only entries that are missing `top_attractions`
# OUTPUT_JSON_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\missing_top_attractions.json"

# # Firestore collection / subcollection names
# ALLPLACES_COLLECTION = "allplaces"
# SUBCOLLECTION_NAME = "top_attractions"


# # ───── FIREBASE INIT ──────────────────────────────────────────────────────────

# def init_firestore() -> firestore.Client:
#     cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
#     try:
#         firebase_admin.get_app()
#     except ValueError:
#         firebase_admin.initialize_app(cred)
#     return firestore.client()


# # ───── HELPERS ────────────────────────────────────────────────────────────────

# def has_top_attractions(db: firestore.Client, place_id: str) -> bool:
#     """
#     Returns True if:
#       - Document allplaces/{place_id} exists, AND
#       - Its subcollection 'top_attractions' has at least one document.

#     Otherwise returns False (treated as "missing" for our use-case).
#     """
#     doc_ref = db.collection(ALLPLACES_COLLECTION).document(place_id)
#     snapshot = doc_ref.get()

#     if not snapshot.exists:
#         # No such place document at all
#         return False

#     # Check if subcollection has at least one doc
#     subcoll_ref = doc_ref.collection(SUBCOLLECTION_NAME)
#     docs = list(subcoll_ref.limit(1).stream())
#     return len(docs) > 0


# def load_input_json(path: str) -> List[Dict]:
#     with open(path, "r", encoding="utf-8") as f:
#         return json.load(f)


# def save_output_json(path: str, data: List[Dict]):
#     os.makedirs(os.path.dirname(path), exist_ok=True)
#     with open(path, "w", encoding="utf-8") as f:
#         json.dump(data, f, ensure_ascii=False, indent=2)


# # ───── MAIN LOGIC ─────────────────────────────────────────────────────────────

# def main():
#     print("🔗 Initializing Firestore...")
#     db = init_firestore()

#     print(f"📥 Loading input JSON from: {INPUT_JSON_PATH}")
#     entries = load_input_json(INPUT_JSON_PATH)
#     print(f"   Found {len(entries)} entries in input file.\n")

#     missing_entries: List[Dict] = []
#     with_top_attractions = 0
#     without_top_attractions = 0
#     not_found_docs = 0

#     for idx, entry in enumerate(entries, 1):
#         place_id_raw = entry.get("place_id")
#         place_id = str(place_id_raw) if place_id_raw is not None else ""

#         city_name = entry.get("city_name", "Unknown")
#         print(f"[{idx}/{len(entries)}] Checking place_id={place_id} | city={city_name}")

#         if not place_id:
#             print("   ⚠️  Missing place_id in entry, treating as 'missing top_attractions'")
#             missing_entries.append(entry)
#             without_top_attractions += 1
#             continue

#         doc_ref = db.collection(ALLPLACES_COLLECTION).document(place_id)
#         snapshot = doc_ref.get()

#         if not snapshot.exists:
#             print("   ❌ allplaces document does NOT exist -> considered missing")
#             missing_entries.append(entry)
#             without_top_attractions += 1
#             not_found_docs += 1
#             continue

#         # If doc exists, check subcollection
#         if has_top_attractions(db, place_id):
#             print("   ✅ Has top_attractions subcollection")
#             with_top_attractions += 1
#         else:
#             print("   🚫 No top_attractions subcollection (or empty) -> considered missing")
#             missing_entries.append(entry)
#             without_top_attractions += 1

#         print()

#     # Save the missing ones
#     print(f"💾 Saving {len(missing_entries)} entries without 'top_attractions' to: {OUTPUT_JSON_PATH}")
#     save_output_json(OUTPUT_JSON_PATH, missing_entries)

#     # Summary
#     print("\n" + "─" * 80)
#     print("Summary:")
#     print(f"  Total input entries:          {len(entries)}")
#     print(f"  With top_attractions:         {with_top_attractions}")
#     print(f"  Missing top_attractions:      {without_top_attractions}")
#     print(f"    • allplaces doc not found:  {not_found_docs}")
#     print(f"    • doc exists but no subcoll:{without_top_attractions - not_found_docs}")
#     print("─" * 80)


# if __name__ == "__main__":
#     main()


# Fetch city_names into a list from above script.
import json
import os

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# Path to the JSON file you generated from the previous step
INPUT_JSON_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\missing_top_attractions.json"

# Optional: Path to save the extracted list (if you want to save it to a file)
OUTPUT_TXT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\city_names_list.txt"

# ═══════════════════════════════════════════════════════════════════════════════
# SCRIPT
# ═══════════════════════════════════════════════════════════════════════════════

def extract_city_names():
    # 1. Check if file exists
    if not os.path.exists(INPUT_JSON_PATH):
        print(f"❌ Error: File not found at {INPUT_JSON_PATH}")
        return

    try:
        # 2. Load the JSON data
        with open(INPUT_JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        print(f"📦 Loaded {len(data)} entries from JSON.")

        # 3. Extract city names using a list comprehension
        # We use .get() to avoid crashing if a key is missing, though your data looks clean.
        city_names = [entry.get("city_name") for entry in data if entry.get("city_name")]

        # 4. Print the list to the console
        print("\n--- Extracted City Names ---")
        print(city_names)
        
        # 5. (Optional) Save to a text file for easy copying
        if OUTPUT_TXT_PATH:
            with open(OUTPUT_TXT_PATH, "w", encoding="utf-8") as f:
                for name in city_names:
                    f.write(name + "\n")
            print(f"\n✅ List saved to {OUTPUT_TXT_PATH}")

    except json.JSONDecodeError:
        print("❌ Error: Failed to decode JSON. Please check the file format.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    extract_city_names()