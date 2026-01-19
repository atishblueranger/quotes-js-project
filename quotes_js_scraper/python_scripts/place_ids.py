"""
Fetch all 'explore' documents with place_ids in the input JSON array file,
then filter out only those documents where country_name is 'India',
and write the place_ids of those documents to a new JSON file as an array.
"""

import os
import json
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore

# ── CONFIG ──────────────────────────────────────────────────────────────────────
# 1) Service account key for Firebase Admin SDK
firebase_credentials_path = Path(
    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
)

# 2) Input file: your list of all top-city place_ids (JSON array of ints)
input_ids_path = Path(r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\top_cities_place_ids.json")

# 3) Firestore collection
collection_name = "explore"

# 4) Output file
output_path = Path("india_top_cities_ids.json")
# ────────────────────────────────────────────────────────────────────────────────

# Initialize Firebase Admin
cred = credentials.Certificate(str(firebase_credentials_path))
firebase_admin.initialize_app(cred)
db = firestore.client()

# Load your list of place_ids
with input_ids_path.open(encoding="utf-8") as f:
    place_ids = json.load(f)


india_ids = []
missing_ids = []

for pid in place_ids:
    doc_ref = db.collection(collection_name).document(str(pid))
    doc = doc_ref.get()
    if not doc.exists:
        missing_ids.append(pid)
        continue

    data = doc.to_dict()  # this is always a dict if exists
    # safe‐guard against country_name being None
    country = data.get("country_name") or ""
    if country.strip().lower() == "india":
        india_ids.append(pid)


# Prepare JSON output
output = {
    "total_input_ids": len(place_ids),
    "total_india": len(india_ids),
    "india_place_ids": india_ids,
    "total_missing": len(missing_ids),
    "missing_document_ids": missing_ids
}

# Write to file
with output_path.open("w", encoding="utf-8") as out:
    json.dump(output, out, indent=2)

print(f"✅  Done! Found {len(india_ids)} place(s) in India; {len(missing_ids)} missing docs.")
print(f"Output written to {output_path.resolve()}")





# import json
# from pathlib import Path

# # --- 1️⃣  Load the JSON --------------------------------------------------------
# json_path = Path(r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\citiesData.json")
# with json_path.open(encoding="utf-8") as f:
#     data = json.load(f)                  # data is a Python list of dicts

# # --- 2️⃣  Grab all place_id values --------------------------------------------
# place_ids = [item["place_id"] for item in data]

# # # --- 3️⃣  Pick an output style -------------------------------------------------
# # # A) one id per line  ➜  place_ids.txt
# # Path("place_ids.txt").write_text("\n".join(map(str, place_ids)), encoding="utf-8")

# # # B) comma-separated string  ➜  place_ids.csv
# # Path("place_ids.csv").write_text(",".join(map(str, place_ids)), encoding="utf-8")

# # # C) plain Python list literal  ➜  place_ids.py
# # Path("place_ids.py").write_text("place_ids = " + repr(place_ids), encoding="utf-8")

# # D) JSON array file         ➜  place_ids.json
# Path("top_cities_place_ids.json").write_text(json.dumps(place_ids, indent=2), encoding="utf-8")

# print("Done!  Extracted", len(place_ids), "place_id values.")
