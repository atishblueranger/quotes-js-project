"""
Add a "city" field to each document in "top_attractions" subcollections of
documents in "allplaces" collection, based on the "detail_url" field.

The "city" field will be set to the humanized version of the city slug found in
the detail URL (e.g. "new-york-city" becomes "New York City").

This script will only update documents where the "city" field is missing or
incorrect.

Example URL format: https://www.lonelyplanet.com/{country}/{city}/…
"""


#!/usr/bin/env python3
import os
import time
from urllib.parse import urlparse
import firebase_admin
from firebase_admin import credentials, firestore

# # ───── CONFIG ────────────────────────────────────────────────────────────────
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# # If you want only specific countryIds, list them here; otherwise leave empty to do all:
# TARGET_COUNTRY_IDS = [
#     "86661",  # india
#     # "86647",  # japan
#     # …
# ]

# # ───── INIT FIRESTORE ──────────────────────────────────────────────────────────
# cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
# firebase_admin.initialize_app(cred)
# db = firestore.client()

# # ───── HELPER TO EXTRACT CITY ─────────────────────────────────────────────────
# def city_from_detail_url(detail_url: str) -> str:
#     """
#     e.g. https://www.lonelyplanet.com/india/amritsar/attractions/…
#     → returns "Amritsar"
#     """
#     try:
#         path = urlparse(detail_url).path  # "/india/amritsar/attractions/…"
#         segs = [s for s in path.split("/") if s]
#         # segs[0] is country, segs[1] is city
#         city_slug = segs[1]
#         return city_slug.replace("-", " ").title()
#     except Exception:
#         return None

# # ───── MIGRATION ──────────────────────────────────────────────────────────────
# def add_city_field():
#     coll_allplaces = db.collection("allplaces")
#     for country_doc in coll_allplaces.list_documents():
#         country_id = country_doc.id
#         if TARGET_COUNTRY_IDS and country_id not in TARGET_COUNTRY_IDS:
#             continue

#         print(f"Processing countryId={country_id} …")
#         top_coll = country_doc.collection("top_attractions")
#         for doc in top_coll.stream():
#             data = doc.to_dict()
#             if "city" in data:
#                 # already has it, skip
#                 continue

#             detail_url = data.get("detail_url", "")
#             city_name  = city_from_detail_url(detail_url)
#             if city_name:
#                 print(f" • {doc.id}: setting city={city_name}")
#                 top_coll.document(doc.id).update({"city": city_name})
#             else:
#                 print(f" ⚠️  {doc.id}: couldn’t parse city from '{detail_url}'")

#     print("✅  Migration complete.")

# if __name__ == "__main__":
#     add_city_field()






# ─── CONFIGURATION ───────────────────────────────────────




SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
COUNTRY_ID_MAP = {
    # "india":       "86661",
    # "japan":       "86647",
    "egypt":         "90763", 
    "cambodia":      "86659",
    "nepal":         "86667",
    "philippines":   "86652",
    "singapore":     "86654",
    "south-korea":   "86656",
    "sri-lanka":     "86693",
    "taiwan":        "86668",
    "thailand":      "86651",
    "vietnam":       "86655",
    "united-arab-emirates": "91524",
    "israel":        "91525",
    "jordan":        "91534", 
    "oman":          "91536",
    "pakistan":      "86831",
    "qatar":         "91535",
    "turkey":        "88367",
    "croatia":       "88438",
    "czech-republic":"88366",
    "england":       "88359",
    "france":        "88358",
    "germany":       "88368",
    "greece":        "88375",
    "hungary":       "88380",
    "iceland":       "88419",
    "ireland":       "88386",
    "italy":         "88352",
    "malta":         "88407",
    "the-netherlands": "88373",
        # "portugal":      "",
    "spain":         "88362",
    "canada":        "90374",
    "usa":           "90407",
    "mexico":        "91342",
    "south-africa":  "90764",
    "mauritius":     "79304",
    "morocco":       "90759",
    "argentina":     "135385",
    "brazil":        "135383",
    "chile":         "135391",
    "australia":     "91387",
    "new-zealand": "91394",
    "fiji": "91403",
    "california": "90405",
    "bavaria": "88406",
    "kerala": "86714",
    "maharashtra": "86675",
    "uttar-pradesh": "86706",
    "west-bengal": "86716",
    "rajasthan": "86674",
    "karnataka": "86686",
    "uttarakhand-uttaranchal": "86805",
    "punjab": "86887",
    "orissa": "86904",
    "assam": "86937",
    "meghalaya": "86988",
    "arunachal-pradesh": "87236",
    "nagaland": "87411",
    "new-york-state": "90404",
    "florida": "90414",
    "nevada": "90409",
    "texas": "90419",
    "massachusetts": "90420",
    "washington": "90412",
    "louisiana": "90413",
    "hokkaido": "86676",
    "south-australia": "91397",
    "jiangsu": "86722",
    "guangdong": "86689",
    "zhejiang": "86710",
    "sichuan": "86712",
    "yunnan": "86774",
    "guangxi": "86784",
    "shandong": "86745",
    "fujian": "86743",
    "new-south-wales": "91388",
    "victoria": "91386",
    "queensland": "91389",
    "western-australia": "91399",
    "tasmania": "91406",    
    # …etc…
}

# ─── FIREBASE INIT ────────────────────────────────────────

cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
firebase_admin.initialize_app(cred)
db = firestore.client()

# ─── UTILS ────────────────────────────────────────────────

def humanize_slug(slug):
    """Convert 'new-york-city' → 'New York City'"""
    return slug.replace("-", " ").title()

# ─── MIGRATION ────────────────────────────────────────────

def add_cities():
    for slug, country_id in COUNTRY_ID_MAP.items():
        coll = db.collection("allplaces") \
                 .document(country_id) \
                 .collection("top_attractions")

        docs = coll.stream()
        print(f"Processing {slug} ({country_id})…")
        for doc in docs:
            data = doc.to_dict()
            detail = data.get("detail_url", "")
            # URL format: https://www.lonelyplanet.com/{country}/{city}/…
            parts = detail.split("/")
            city_slug = parts[4] if len(parts) > 4 else None
            city_name = humanize_slug(city_slug) if city_slug else None

            if not city_name:
                print(f"  ⚠️  No city slug for doc {doc.id}, url={detail}")
                continue

            # update only if missing or incorrect
            if data.get("city") != city_name:
                coll.document(doc.id).update({ "city": city_name })
                print(f"  ✔️  {doc.id} → city='{city_name}'")

        # small delay in case you have a lot of writes
        time.sleep(0.2)

if __name__ == "__main__":
    add_cities()
    print("Done.")
