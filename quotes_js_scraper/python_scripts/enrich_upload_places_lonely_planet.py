#!/usr/bin/env python3
import json
import time
import requests
import firebase_admin
from firebase_admin import credentials, firestore, storage

# ───── CONFIGURATION ─────
INPUT_FILE           = "C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lonely_planet_data\lonelyplanet_india_varanasi_attractions.json"
GOOGLE_API_KEY       = "AIzaSyA6eTQaFPPidJ6oWWClNmqb_dHbhtmMzn8"
SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
STORAGE_BUCKET       = "mycasavsc.appspot.com"
CITY_ID              = "122"
API_CALL_INTERVAL    = 0.5  # seconds between Google API calls

# ───── INITIALIZE FIREBASE ─────
cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
firebase_admin.initialize_app(cred, {"storageBucket": STORAGE_BUCKET})
db     = firestore.client()
bucket = storage.bucket()

# ───── HELPERS ─────

def find_google_place_id(name, lat, lon):
    """Use Google Places FindPlaceFromText to get a place_id."""
    url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input":        name,
        "inputtype":    "textquery",
        "fields":       "place_id",
        "locationbias": f"point:{lat},{lon}",
        "key":          GOOGLE_API_KEY,
    }
    time.sleep(API_CALL_INTERVAL)
    resp = requests.get(url, params=params).json()
    candidates = resp.get("candidates", [])
    return candidates[0]["place_id"] if candidates else None

def build_image_urls(photos):
    """
    From Google Place Details 'photos' array build:
      - list of photo URLs
      - list of photo_reference keys
    """
    urls, keys = [], []
    if not photos:
        return urls, keys
    for photo in photos:
        ref = photo.get("photo_reference")
        if not ref: 
            continue
        keys.append(ref)
        urls.append(
            f"https://maps.googleapis.com/maps/api/place/photo?"
            f"maxwidth=400&photo_reference={ref}&key={GOOGLE_API_KEY}"
        )
    return urls, keys

def get_place_details(place_id):
    """
    Calls Google Place Details (editorial_summary & photos).
    Returns dict containing:
      - g_description
      - g_image_urls, imageKeys
      - formatted_address, phone, website, priceLevel, rating, ratingCount,
        types, permanentlyClosed, utcOffset, openingPeriods, latitude, longitude
    """
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = ",".join([
        "formatted_address","international_phone_number","geometry",
        "website","price_level","rating","user_ratings_total",
        "opening_hours","types","permanently_closed","utc_offset",
        "editorial_summary","photos"
    ])
    params = {"place_id": place_id, "fields": fields, "key": GOOGLE_API_KEY}
    time.sleep(API_CALL_INTERVAL)
    result = requests.get(url, params=params).json().get("result", {})

    out = {
        "g_description": result.get("editorial_summary",{}).get("overview", "") or "",
        "address":      result.get("formatted_address"),
        "phone":        result.get("international_phone_number"),
        "website":      result.get("website"),
        "priceLevel":   result.get("price_level"),
        "rating":       result.get("rating"),
        "ratingCount":  result.get("user_ratings_total"),
        "types":        result.get("types"),
        "permanentlyClosed": result.get("permanently_closed"),
        "utcOffset":    result.get("utc_offset"),
        # opening_hours → periods
        "openingPeriods": result.get("opening_hours",{}).get("periods"),
    }
    # geometry → lat/lng
    geom = result.get("geometry",{}).get("location",{})
    out["latitude"]  = geom.get("lat")
    out["longitude"] = geom.get("lng")

    # photos → g_image_urls & imageKeys
    urls, keys = build_image_urls(result.get("photos"))
    out["g_image_urls"] = urls
    out["imageKeys"]    = keys

    return out

def download_and_upload_image(original_url, place_id, index):
    """
    Download an image and upload it to Firebase Storage as:
      lp_attractions/{place_id}_{index+1}.jpg
    Returns the public URL (or signed URL) of the new upload.
    """
    try:
        resp = requests.get(original_url, stream=True, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  ⚠️  Failed to download image: {e}")
        return None

    filename = f"lp_attractions/{place_id}_{index+1}.jpg"
    blob = bucket.blob(filename)
    blob.upload_from_string(resp.content, content_type="image/jpeg")
    try:
        blob.make_public()
        return blob.public_url
    except:
        return blob.generate_signed_url(version="v4", expiration=3600)

# ───── MAIN PROCESS ─────

def main():
    # Load scraped LP attractions data
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        records = json.load(f)

    coll = db.collection("explore").document(CITY_ID).collection("lp")

    for rec in records:
        print(f"▶️  [{rec['index']}] {rec['name']}")

        # — 1) Google Places enrichment —
        pid = find_google_place_id(rec["name"], rec["latitude"], rec["longitude"])
        if pid:
            rec["placeId"] = pid
            details = get_place_details(pid)

            # Prepend the original LP image to g_image_urls
            lp_img = rec.get("image_url")
            rec["g_image_urls"] = ([lp_img] if lp_img else []) + details["g_image_urls"]
            rec["imageKeys"]    = details["imageKeys"]

            # Merge other enriched fields
            rec["g_description"] = details["g_description"]
            for k in (
                "address","phone","website","priceLevel","rating","ratingCount",
                "types","permanentlyClosed","utcOffset","openingPeriods",
                "latitude","longitude"
            ):
                # prefer Google value, fallback to existing
                rec[k] = details.get(k, rec.get(k))
        else:
            print("   ⚠️  No Google place_id found; skipping enrichment.")

        # — 2) Re-upload LP image as index=0 —
        if rec.get("image_url"):
            new_lp = download_and_upload_image(rec["image_url"], rec.get("placeId") or rec["index"], 0)
            if new_lp:
                rec["image_url"] = new_lp

        # — 3) Re-upload every Google photo for g_image_urls —
        new_g = []
        for idx, gp in enumerate(rec.get("g_image_urls", [])):
            print(f"    ↪️  Uploading image #{idx+1} for {rec.get('placeId') or rec['index']}")
            up = download_and_upload_image(gp, rec.get("placeId") or rec["index"], idx)
            new_g.append(up or gp)
        rec["g_image_urls"] = new_g

        # — 4) Upload final record to Firestore —
        doc_id = rec.get("placeId") or str(rec["index"])
        coll.document(doc_id).set(rec)
        print(f"   ✅  Uploaded as {doc_id}")

    print("🎉  All done.")

if __name__ == "__main__":
    main()



# import json
# import time
# import requests
# import firebase_admin
# from firebase_admin import credentials, firestore, storage

# # ───── CONFIGURATION ─────

# # 1) Path to your scraped JSON file
# INPUT_FILE = "lonelyplanet_indian_attractions.json"

# # 2) Google Places API key
# GOOGLE_API_KEY = "AIzaSyA6eTQaFPPidJ6oWWClNmqb_dHbhtmMzn8"

# # 3) Firebase Admin credentials & storage bucket
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# STORAGE_BUCKET     = "mycasavsc.appspot.com"

# # 4) Firestore collection path: explore/{CITY_ID}/attractions
# CITY_ID = "57"

# # 5) Rate‑limit between Google API calls (seconds)
# API_CALL_INTERVAL = 0.5

# # ───── INITIALIZE FIREBASE ─────

# cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
# firebase_admin.initialize_app(cred, {"storageBucket": STORAGE_BUCKET})
# db     = firestore.client()
# bucket = storage.bucket()

# # ───── HELPERS ─────

# def find_google_place_id(name, lat, lon):
#     """
#     Use Google Places FindPlaceFromText to get a place_id.
#     """
#     url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
#     params = {
#         "input":        name,
#         "inputtype":    "textquery",
#         "fields":       "place_id",
#         "locationbias": f"point:{lat},{lon}",
#         "key":          GOOGLE_API_KEY,
#     }
#     time.sleep(API_CALL_INTERVAL)
#     resp = requests.get(url, params=params).json()
#     candidates = resp.get("candidates", [])
#     return candidates[0]["place_id"] if candidates else None

# def get_place_details(place_id):
#     """
#     Use Google Place Details to pull extra fields.
#     Returns a dict of any of these fields that succeed.
#     """
#     url = "https://maps.googleapis.com/maps/api/place/details/json"
#     fields = ",".join([
#         "formatted_address", "international_phone_number", "geometry",
#         "website", "price_level", "rating", "user_ratings_total",
#         "opening_hours", "types", "permanently_closed", "utc_offset"
#     ])
#     params = {"place_id": place_id, "fields": fields, "key": GOOGLE_API_KEY}
#     time.sleep(API_CALL_INTERVAL)
#     data = requests.get(url, params=params).json().get("result", {})
#     out = {
#         "address":    data.get("formatted_address"),
#         "phone":      data.get("international_phone_number"),
#         "website":    data.get("website"),
#         "priceLevel": data.get("price_level"),
#         "rating":     data.get("rating"),
#         "ratingCount":data.get("user_ratings_total"),
#         "types":      data.get("types"),
#         "permanentlyClosed": data.get("permanently_closed"),
#         "utcOffset":  data.get("utc_offset"),
#     }
#     # geometry → lat/lng
#     if data.get("geometry",{}).get("location"):
#         loc = data["geometry"]["location"]
#         out["latitude"]  = loc.get("lat")
#         out["longitude"] = loc.get("lng")
#     # opening hours periods
#     oh = data.get("opening_hours", {}).get("periods")
#     out["openingPeriods"] = oh
#     return out

# def download_and_upload_image(original_url, place_id):
#     """
#     Download a single image URL and re-upload it to Firebase Storage
#     under `attractions/{place_id}.jpg`. Returns the new public URL.
#     """
#     try:
#         resp = requests.get(original_url, stream=True, timeout=30)
#         resp.raise_for_status()
#     except Exception as e:
#         print("  ⚠️ Error downloading image:", e)
#         return None

#     filename = f"attractions/{place_id}.jpg"
#     blob = bucket.blob(filename)
#     blob.upload_from_string(resp.content, content_type="image/jpeg")
#     # make public if your bucket allows it
#     try:
#         blob.make_public()
#         return blob.public_url
#     except:
#         return blob.generate_signed_url(version="v4", expiration=3600)

# # ───── MAIN PROCESS ─────

# def main():
#     # load your scraped data
#     with open(INPUT_FILE, "r", encoding="utf-8") as f:
#         records = json.load(f)

#     coll = db.collection("explore").document(CITY_ID).collection("attractions")

#     for rec in records:
#         print(f"▶️ Processing index {rec['index']}: {rec['name']}")

#         # 1) Enrich with Google Places
#         pid = find_google_place_id(rec["name"], rec["latitude"], rec["longitude"])
#         if pid:
#             rec["placeId"] = pid
#             details = get_place_details(pid)
#             rec.update(details)
#         else:
#             print("   ⚠️ No Google place_id found.")

#         # 2) Re-upload the LonelyPlanet image, if present
#         img = rec.get("image_url")
#         if img:
#             new_img = download_and_upload_image(img, rec.get("placeId", rec["index"]))
#             rec["image_url"] = new_img or img

#         # 3) Write into Firestore
#         doc_id = rec.get("placeId") or str(rec["index"])
#         coll.document(doc_id).set(rec)
#         print("   ✅ Uploaded to Firestore as", doc_id)

#     print("🎉 All done.")

# if __name__ == "__main__":
#     main()
