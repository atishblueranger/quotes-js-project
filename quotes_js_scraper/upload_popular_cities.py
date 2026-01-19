#!/usr/bin/env python3
"""
upload_popular_cities_by_state.py
───────────────────────────────────
Reads popular_cities_by_state.json and writes its contents into Firestore:

  allplaces/<state_place_id>/popular_cities/<city_place_id>

• Skips any city without an image_url.
• Clears the sub-collection first and then writes in batches of ≤400 ops.
"""

import json
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore

# ─── CONFIG ───────────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_JSON = (
    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
    r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
)
INPUT_JSON_FILE    = "popular_cities_by_state.json"
SUBCOLLECTION_NAME = "popular_cities"
BATCH_SIZE         = 400  # stay under Firestore’s 500-op limit

# ─── INIT FIRESTORE ───────────────────────────────────────────────────────────
if not firebase_admin._apps:
    firebase_admin.initialize_app(credentials.Certificate(SERVICE_ACCOUNT_JSON))
db = firestore.client()

# ─── HELPERS ───────────────────────────────────────────────────────────────────
def image_is_missing(img):
    """
    Returns True if img is None, "", or an empty list/tuple.
    """
    if img is None:
        return True
    if isinstance(img, str) and img.strip() == "":
        return True
    if isinstance(img, (list, tuple)) and len(img) == 0:
        return True
    return False

# ─── LOAD JSON ────────────────────────────────────────────────────────────────
json_path = Path(INPUT_JSON_FILE)
if not json_path.exists():
    raise FileNotFoundError(f"Cannot find {INPUT_JSON_FILE}")

states = json.loads(json_path.read_text(encoding="utf-8"))
print(f"Loaded {len(states)} states from JSON")

# ─── UPLOAD LOOP ──────────────────────────────────────────────────────────────
for state_name, payload in states.items():
    state_id = str(payload["place_id"])
    cities   = payload.get("popular_cities", [])

    # Filter out cities without images
    cities_to_write = [c for c in cities if not image_is_missing(c.get("image_url"))]
    skipped = len(cities) - len(cities_to_write)

    print(f"\n→ {state_name!r} (ID {state_id}) — keeping {len(cities_to_write)}, skipped {skipped}")

    parent = db.collection("allplaces").document(state_id)

    # 1. Clear existing sub-collection
    existing_refs = list(parent.collection(SUBCOLLECTION_NAME).list_documents())
    batch, ops = db.batch(), 0
    for ref in existing_refs:
        batch.delete(ref); ops += 1
        if ops >= BATCH_SIZE:
            batch.commit(); batch, ops = db.batch(), 0
    if ops:
        batch.commit()

    # 2. Write filtered cities
    batch, ops = db.batch(), 0
    for city in cities_to_write:
        doc_ref = parent.collection(SUBCOLLECTION_NAME).document(str(city["place_id"]))
        batch.set(doc_ref, city); ops += 1
        if ops >= BATCH_SIZE:
            batch.commit(); batch, ops = db.batch(), 0
    if ops:
        batch.commit()

    print("   ✔︎  upload complete")

print("\n✅ All states processed (empty-image cities skipped).")


# #!/usr/bin/env python3
# """
# upload_popular_cities.py  (v2 – skip empty image_url)
# ─────────────────────────────────────────────────────
# Reads popular_cities_by_country2.json and writes the data to Firestore
# under:   allplaces/<country_id>/popular_cities/<city_place_id>

# • Cities whose image_url is None, "", or [] are *ignored*.
# • The sub-collection is deleted and rebuilt to stay idempotent.
# • Uses batch writes (≤400 ops/commit) for efficiency.
# """

# import json
# from pathlib import Path

# import firebase_admin
# from firebase_admin import credentials, firestore

# # ─── CONFIG ───────────────────────────────────────────────────────────────────
# SERVICE_ACCOUNT_JSON = (
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#     r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )

# INPUT_JSON_FILE      = "popular_cities_by_state.json"
# SUBCOLLECTION_NAME   = "popular_cities"
# BATCH_SIZE           = 400          # below Firestore’s 500-op limit

# # ─── INIT FIRESTORE ───────────────────────────────────────────────────────────
# if not firebase_admin._apps:
#     firebase_admin.initialize_app(credentials.Certificate(SERVICE_ACCOUNT_JSON))
# db = firestore.client()

# # ─── HELPER ───────────────────────────────────────────────────────────────────
# def image_is_missing(img):
#     """
#     Returns True if img is None, "", or an empty list/tuple.
#     """
#     if img is None:
#         return True
#     if isinstance(img, str) and img.strip() == "":
#         return True
#     if isinstance(img, (list, tuple)) and len(img) == 0:
#         return True
#     return False

# # ─── LOAD JSON ────────────────────────────────────────────────────────────────
# json_path = Path(INPUT_JSON_FILE)
# if not json_path.exists():
#     raise FileNotFoundError(f"Cannot find {INPUT_JSON_FILE}")

# countries = json.loads(json_path.read_text(encoding="utf-8"))
# print(f"Loaded {len(countries)} countries from JSON")

# # ─── UPLOAD LOOP ──────────────────────────────────────────────────────────────
# for c_name, payload in countries.items():
#     c_id = str(payload["place_id"])
#     cities = payload.get("popular_cities", [])

#     # Filter out cities with empty image_url
#     cities_to_write = [c for c in cities if not image_is_missing(c.get("image_url"))]
#     skipped = len(cities) - len(cities_to_write)

#     print(f"\n→ {c_name} (ID {c_id}) — keeping {len(cities_to_write)}, skipped {skipped}")

#     parent = db.collection("allplaces").document(c_id)

#     # 1. Clear existing sub-collection
#     existing_refs = list(parent.collection(SUBCOLLECTION_NAME).list_documents())
#     batch, ops = db.batch(), 0
#     for ref in existing_refs:
#         batch.delete(ref); ops += 1
#         if ops >= BATCH_SIZE:
#             batch.commit(); batch, ops = db.batch(), 0
#     if ops:
#         batch.commit()

#     # 2. Write filtered cities
#     batch, ops = db.batch(), 0
#     for city in cities_to_write:
#         doc_ref = parent.collection(SUBCOLLECTION_NAME).document(str(city["place_id"]))
#         batch.set(doc_ref, city); ops += 1
#         if ops >= BATCH_SIZE:
#             batch.commit(); batch, ops = db.batch(), 0
#     if ops:
#         batch.commit()

#     print("   ✔︎ upload complete")

# print("\n✅ All countries processed (empty-image cities skipped).")



# #!/usr/bin/env python3
# """
# upload_popular_cities_india.py
# ──────────────────────────────
# Imports the India section from popular_cities_by_country2.json
# and writes it to:

#    allplaces/86661/popular_cities/<city_place_id>

# • Any city whose image_url is None / "" / [] is skipped.
# """

# import json
# from pathlib import Path

# import firebase_admin
# from firebase_admin import credentials, firestore

# # ─── CONFIG ───────────────────────────────────────────────────────────────────
# SERVICE_ACCOUNT_JSON = (
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#     r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )

# INPUT_JSON_FILE      = "popular_cities_by_country2.json"
# COUNTRY_NAME         = "India"   # key in the JSON
# COUNTRY_ID           = "86661"   # Firestore doc ID
# SUBCOLLECTION_NAME   = "popular_cities"
# BATCH_SIZE           = 400

# # ─── INIT FIRESTORE ───────────────────────────────────────────────────────────
# if not firebase_admin._apps:
#     firebase_admin.initialize_app(credentials.Certificate(SERVICE_ACCOUNT_JSON))
# db = firestore.client()

# # ─── HELPER ───────────────────────────────────────────────────────────────────
# def image_is_missing(img):
#     return (
#         img is None or
#         (isinstance(img, str)  and img.strip() == "") or
#         (isinstance(img, (list, tuple)) and len(img) == 0)
#     )

# # ─── LOAD JSON ────────────────────────────────────────────────────────────────
# json_path = Path(INPUT_JSON_FILE)
# if not json_path.exists():
#     raise FileNotFoundError(f"Could not find {INPUT_JSON_FILE}")

# data = json.loads(json_path.read_text(encoding="utf-8"))
# if COUNTRY_NAME not in data:
#     raise KeyError(f"{COUNTRY_NAME} not found in JSON file")

# payload = data[COUNTRY_NAME]
# cities  = [
#     c for c in payload.get("popular_cities", [])
#     if not image_is_missing(c.get("image_url"))
# ]
# print(f"India: {len(cities)} cities will be written (skipping any without images)")

# # ─── DELETE EXISTING + WRITE NEW ──────────────────────────────────────────────
# parent = db.collection("allplaces").document(COUNTRY_ID)

# # 1. clear sub-collection
# for ref in parent.collection(SUBCOLLECTION_NAME).list_documents():
#     ref.delete()

# # 2. batch write
# batch, ops = db.batch(), 0
# for city in cities:
#     doc_ref = parent.collection(SUBCOLLECTION_NAME).document(str(city["place_id"]))
#     batch.set(doc_ref, city); ops += 1
#     if ops >= BATCH_SIZE:
#         batch.commit(); batch, ops = db.batch(), 0
# if ops:
#     batch.commit()

# print("✅ Upload complete for India.")



# #!/usr/bin/env python3
# """
# check_missing_city_images.py
# ────────────────────────────
# Reads popular_cities_by_country2.json and creates missing_city_images.json
# with a summary of cities that have no image_url.
# """

# import json
# from pathlib import Path

# INPUT_FILE  = "popular_cities_by_country2.json"
# OUTPUT_FILE = "missing_city_images.json"

# def image_is_missing(image_field):
#     """
#     Returns True if image_url is None, an empty string, or
#     an empty list/tuple. Otherwise False.
#     """
#     if image_field is None:
#         return True
#     if isinstance(image_field, str) and image_field.strip() == "":
#         return True
#     if isinstance(image_field, (list, tuple)) and len(image_field) == 0:
#         return True
#     return False

# def main():
#     in_path = Path(INPUT_FILE)
#     if not in_path.exists():
#         raise FileNotFoundError(f"Could not find {INPUT_FILE}")

#     data = json.loads(in_path.read_text(encoding="utf-8"))

#     result = {}

#     for country_name, payload in data.items():
#         missing = [
#             city["name"]
#             for city in payload.get("popular_cities", [])
#             if image_is_missing(city.get("image_url"))
#         ]

#         result[country_name] = {
#             "country_name": country_name,
#             "place_id": payload.get("place_id"),
#             "missing_image_cities": missing,
#             "missing_count": len(missing),
#         }

#     # Write to file
#     out_path = Path(OUTPUT_FILE)
#     out_path.write_text(
#         json.dumps(result, indent=4, ensure_ascii=False), encoding="utf-8"
#     )
#     print(f"✅ Summary written to {out_path.resolve()}")

# if __name__ == "__main__":
#     main()



# #!/usr/bin/env python3
# """
# upload_popular_cities.py
# ────────────────────────
# Reads `popular_cities_by_country2.json` (output of the previous script)
# and writes its contents into Firestore:

# allplaces/<country_id>/popular_cities/<city_place_id>

# The script is idempotent: it first clears the sub-collection,
# then writes the new docs in batches of ≤400 operations.
# """

# import json
# from pathlib import Path

# import firebase_admin
# from firebase_admin import credentials, firestore

# # ─── CONFIG ───────────────────────────────────────────────────────────────────
# SERVICE_ACCOUNT_JSON = (
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#     r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )
# INPUT_JSON_FILE = "popular_cities_by_country2.json"
# SUBCOLLECTION_NAME = "popular_cities"      # change if you want another name
# BATCH_SIZE = 400                           # stay under 500-op limit

# # ─── INIT FIRESTORE ───────────────────────────────────────────────────────────
# if not firebase_admin._apps:
#     firebase_admin.initialize_app(credentials.Certificate(SERVICE_ACCOUNT_JSON))
# db = firestore.client()

# # ─── LOAD JSON ────────────────────────────────────────────────────────────────
# json_path = Path(INPUT_JSON_FILE)
# if not json_path.exists():
#     raise FileNotFoundError(f"Cannot find {INPUT_JSON_FILE}")

# data = json.loads(json_path.read_text(encoding="utf-8"))
# print(f"Loaded {len(data)} countries from JSON")

# # ─── UPLOAD LOOP ──────────────────────────────────────────────────────────────
# for c_name, c_payload in data.items():
#     country_id = str(c_payload["place_id"])

#     print(f"\n→ {c_name}  (doc ID: {country_id})")
#     parent_ref = db.collection("allplaces").document(country_id)

#     # 1. clear any existing docs in sub-collection
#     print("   • Deleting existing docs …")
#     to_delete = list(parent_ref.collection(SUBCOLLECTION_NAME).list_documents())
#     batch = db.batch()
#     op_count = 0
#     for doc_ref in to_delete:
#         batch.delete(doc_ref)
#         op_count += 1
#         if op_count >= BATCH_SIZE:
#             batch.commit(); batch, op_count = db.batch(), 0
#     if op_count:
#         batch.commit()

#     # 2. write new docs
#     print(f"   • Writing {len(c_payload['popular_cities'])} new docs …")
#     batch = db.batch()
#     op_count = 0
#     for city in c_payload["popular_cities"]:
#         doc_ref = parent_ref.collection(SUBCOLLECTION_NAME).document(str(city["place_id"]))
#         batch.set(doc_ref, city)
#         op_count += 1
#         if op_count >= BATCH_SIZE:
#             batch.commit(); batch, op_count = db.batch(), 0
#     if op_count:
#         batch.commit()

#     print("   ✔︎ Done.")

# print("\n✅ All countries processed.")
