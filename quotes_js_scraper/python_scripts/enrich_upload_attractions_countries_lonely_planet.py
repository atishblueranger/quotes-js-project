#!/usr/bin/env python3
import json
import time
import requests
import glob
import os
import firebase_admin
from firebase_admin import credentials, firestore, storage

# ───── 1) CONFIG ────────────────────────────────────────────────────────────────

# Pattern matching all your trimmed/juggled files:
JSON_GLOB = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\*_top20.json"
MAX_DOWNLOAD_RETRIES = 3
INITIAL_BACKOFF = 1  # seconds
# Map filename-slug → allplaces document ID:
COUNTRY_ID_MAP = {
        # "india":         "86661",
        # "egypt":         "90763", 
        # # "cambodia":      "86659",
        # # "japan":         "86647",
        # "nepal":         "86667",
        # "philippines":   "86652",
        # "singapore":     "86654",
        # "south-korea":   "86656",
        # "sri-lanka":     "86693",
        # "taiwan":        "86668",
        # "thailand":      "86651",
        # "vietnam":       "86655",
        # "united-arab-emirates": "91524",
        # "israel":        "91525",
        # "jordan":        "91534", 
        # "oman":          "91536",
        # "pakistan":      "86831",
        # "qatar":         "91535",
        # "turkey":        "88367",
        # "croatia":       "88438",
        # "czech-republic":"88366",
        # "england":       "88359",
        # "france":        "88358",
        # "germany":       "88368",
        # "greece":        "88375",
        # "hungary":       "88380",
        # "iceland":       "88419",
        # "ireland":       "88386",
        # "italy":         "88352",
        # "malta":         "88407",
        # "the-netherlands": "88373",
        # # "portugal":      "",
        # "spain":         "88362",
        # "canada":        "90374",
        # "usa":           "90407",
        # "mexico":        "91342",
        # "south-africa":  "90764",
        # "mauritius":     "79304",
        # "morocco":       "90759",
        # # "argentina":     "135385",
        # # "brazil":        "135383",
        # "chile":         "135391",
        # "australia":     "91387",
        # "new-zealand": "91394",
        # "fiji": "91403",
        # "california": "90405",
        # "bavaria": "88406",
        # 'tamil-nadu':"86691",
        # "kerala": "86714",
        # "maharashtra": "86675",
        # "uttar-pradesh": "86706",
        # "west-bengal": "86716",
        # "rajasthan": "86674",
        # "karnataka": "86686",
        # "uttarakhand-uttaranchal": "86805",
        # "punjab": "86887",
        # "orissa": "86904",
    # "assam": "86937",
    # "meghalaya": "86988",
    # "arunachal-pradesh": "87236",
    # "nagaland": "87411",
    # "new-york-state": "90404",
    # "florida": "90414",
    # "nevada": "90409",
    # "texas": "90419",
    # "massachusetts": "90420",
    # "washington": "90412",
    # "louisiana": "90413",
    # "hokkaido": "86676",
    # "south-australia": "91397",
    # "jiangsu": "86722",
    # "guangdong": "86689",
    # "zhejiang": "86710",
    # "sichuan": "86712",
    # "yunnan": "86774",
    # "guangxi": "86784",
    # "shandong": "86745",
    # "fujian": "86743",
    # "new-south-wales": "91388",
    # "victoria": "91386",
    # "queensland": "91389",
    # "western-australia": "91399",
    # "tasmania": "91406",
# "ecuador": "'135396", // Redo this
    # "colombia": "135393",
    # "uruguay": "135408",
    # "bolivia": "135438",
    # "venezuela": "135468",
    # "paraguay": "135498",
    # "armenia": "135498",
    # "azerbaijan": "86729",
    # "kazakhstan": "86731",
    # "mongolia": "86772",
    # "kyrgyzstan": "86776",
    # "brunei-darussalam": "86779",
    # "laos": "86797",
    # "bhutan": "86819",
    # "bangladesh": "86851",
    # "uzbekistan": "86865",
    # "tajikistan": "86938",
    # # "maldives": "86983",
    # "north-korea": "87077",
    # "east-timor": "87139",
    # "turkmenistan": "87175",
    # # "afghanistan": "87389",
    #  "russia": "88360",
    #  "greece": "88375",
    #  "austria": "88384",
    #  "ukraine": "88394",
    # "denmark": "88402",
    # "poland": "88403",
    # "belgium": "88408",
    # "georgia": "88420",
    # "latvia": "88423",
    # "norway": "88432",
    # "finland": "88434",
    # "switzerland": "88437",
    # "sweden": "88445",
    # "bulgaria": "88449",
    # "estonia": "88455",
    # "serbia": "88464",
    # "lithuania": "88477",
    # "slovakia": "88478",
    # "slovenia": "88486",
    # "belarus": "88503",
    # "cyprus": "88527",
    # "albania": "88609",
    # "montenegro": "88654",
    # "luxembourg": "88723",
    # "moldova": "88731",
    # "panama": "90647",
    # "honduras": "90648",
    # "costa-rica": "90651",
    # "guatemala": "90652",
    # "belize": "90656",
    # # "el-salvador": "90656",
    # "nicaragua": "90660",
    # "kuwait": "91537",
    # "bahrain": "91540",
    # "saudi-arabia": "91548",
    # "lebanon": "91526",
    # "iran": "91530",
    # "new-york-city": "58144",
    # "london": "9613",
    # "paris": "9614",
    # "rome": "9616",
    # "barcelona": "9617",
    # "amsterdam": "9625",#Done
    # "madrid": "9621",
    # "rio-de-janeiro": "131072",
    # "las-vegas": "58148",
    # "singapore": "7", # Note: 'singapore' is already in your country map, consider renaming or handling duplicates
    # "berlin": "9623",
    # "bangkok": "4", #Done
    # "prague": "9620",Done
    # "lisbon": "9626",
    # "buenos-aires": "131071",
    # "san-francisco": "58147",
    # "istanbul": "9622",
    # "sao-paulo": "131073",
    # "budapest": "9629",
    # "dublin": "57258", # Note: Duplicate 'dublin' entries, only one will be kept if you convert directly.
    # "dublin": "9633",
    # "washington-dc": "58159",
    # "vienna": "9631",
    # "venice": "9634",
    # "milan": "9624",
    # "los-angeles": "58145",c
    # "chicago": "58146",
    # "santiago": "131075",
    # "gramado": "131103",
    # "new-orleans": "58156",
    # "seville": "9641",
    # "moscow": "9615",
    # "honolulu-and-waikiki": "58153", # Error coming
    # "marrakesh": "79299",
    # "toronto": "58045",
    # "beijing": "6",
    # "boston": "58162",
    # "mexico-city": "81903",
    # "seattle": "58154",
    # "ho-chi-minh-city": "16",
    # "valencia": "9657",
    # "kuala-lumpur": "33",
    # "munich": "9645",
    # "foz-do-iguacu": "131112",
    # "curitiba": "131082",
    # "stockholm": "9673",
    # "vancouver": "58047",
    # "melbourne": "82575",
    # "seoul": "9",
    # "hanoi": "8", Redo Error Coming
    # "krakow": "9640",
    # "lima": "135226", # Note: Duplicate 'lima' entries, only one will be kept.
    # "turin": "9650",
    # "new-delhi": "13",
    # "porto": "9653",
    # "havana": "81182",
    # "montreal": "58046",
    # "nashville": "58171",
    # "granada": "9693", #Not Done
    # "brasilia": "131087",#Not Done
    # " Salvador": "131085",#Not Done
    # "niagara-falls": "58058",#Not Done
    # "montevideo": "131084",#Not Done
    # "shanghai": "10",
    # # "san-antonio": "58168", # Done
    # "cape-town": "79300", #Done
    # "philadelphia": "58160", #Done
    # "atlanta": "58169", #Not Done
    # "belo-horizonte": "131088", #Not Done
    # "dubrovnik": "9704", # Done
    # "fortaleza": "131091", #Not Done
    # "abu-dhabi": "85942", #Not Done
    "mumbai-bombay": "25", #Not Done
    # "naples":"58189",
    # "rhodes-town":"9819",
    # "santo-domingo":"81197",
    # "guangzhou":"39",
    # "oklahoma":"58232",
    # "manila":"175",
    # "nagasaki":"75",
    # "baku-baki":"82",
    # "hangzhou":"64",
    # "guadalajara":"81912",
    # "ooty-udhagamandalam":"480",
    # "cairo":"79302",
    # "rotterdam":"9719",
    # "vilnius":"9695",
    # "georgetown":"137",
    # "luxor":"'79316",
    # "kolkata-calcutta":"69",
    # "hobart":"82586",
    # "kyiv":"9638",
    # "tbilisi":"9658",
    # "zagreb":"9669",
    # "melaka":"187",
    # "hue":"92",
    # "milwaukee":"58195",
    # "ghent":"9744",
    # "sarajevo":"9804",
    # "tehran":"85943",
    # "guayaquil":"131102",
    # "suzhou":"85",
    # "kuching":"223",
    # "lviv":"9790",
    # "palenque":"81981",
    # "puerto-ayora":"131180",
    # "thimphu":"209",
    # "cafayate":"131362",
    # "bursa":"10030",
    # "gyeongju":"465",
    # "tashkent":"330",
    # "santiago-de-cuba":"81250",
    # "plovdiv":"9952",
    # "tunis":"79332",
    # "lhasa":"248",
    # "san-juan":"131168",
    # "bhaktapur":"1443",
    # "lahore":"235",
    # "athens":"58455",
    
    # "hamburg":"9647",
    # "panama-city":"58232",
    "tokyo":"1",

    # …add one entry per slug you have…
}

GOOGLE_API_KEY       = "AIzaSyBe-suda095R60l0G1NrdYeIVKjUCxwK_8"
SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
STORAGE_BUCKET       = "mycasavsc.appspot.com"

API_CALL_INTERVAL = 0.5   # throttle between Google API calls
MAX_ITEMS         = 20    # cap per country file


# ───── 2) FIREBASE INIT ─────────────────────────────────────────────────────────

cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
firebase_admin.initialize_app(cred, {"storageBucket": STORAGE_BUCKET})
db     = firestore.client()
bucket = storage.bucket()


# ───── 3) HELPERS ────────────────────────────────────────────────────────────────

def find_google_place_id(name, lat, lon):
    """Use Google Places FindPlaceFromText to get a place_id, or return None."""
    url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input":       name,
        "inputtype":   "textquery",
        "fields":      "place_id",
        "locationbias": f"point:{lat},{lon}",
        "key":         GOOGLE_API_KEY,
    }
    time.sleep(API_CALL_INTERVAL)
    resp = requests.get(url, params=params).json()

    candidates = resp.get("candidates")
    # Make sure it's a list and has at least one element:
    if isinstance(candidates, list) and candidates:
        return candidates[0].get("place_id")
    else:
        # optionally log the status or error_message for debugging:
        status = resp.get("status")
        msg    = resp.get("error_message")
        print(f"   ⚠️  Google Places find failed for '{name}' ({lat},{lon}) → status={status}, error={msg}")
        return None

# def find_google_place_id(name, lat, lon):
#     url    = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
#     params = {
#         "input":        name,
#         "inputtype":    "textquery",
#         "fields":       "place_id",
#         "locationbias": f"point:{lat},{lon}",
#         "key":          GOOGLE_API_KEY,
#     }
#     time.sleep(API_CALL_INTERVAL)
#     resp = requests.get(url, params=params).json()
#     return resp.get("candidates", [{}])[0].get("place_id")

def build_image_urls(photos):
    urls, keys = [], []
    for photo in photos or []:
        ref = photo.get("photo_reference")
        if not ref: 
            continue
        keys.append(ref)
        urls.append(
            f"https://maps.googleapis.com/maps/api/place/photo?"
            f"maxwidth=400&photo_reference={ref}&key={GOOGLE_API_KEY}"
        )
    return urls, keys

# def extract_city(address_components):
#     """
#     Pull out 'locality' or fallback to admin levels.
#     """
#     for comp in address_components:
#         if "locality" in comp.get("types", []):
#             return comp.get("long_name")
#     for comp in address_components:
#         if "administrative_area_level_2" in comp.get("types", []):
#             return comp.get("long_name")
#     for comp in address_components:
#         if "administrative_area_level_1" in comp.get("types", []):
#             return comp.get("long_name")
#     return None


def get_place_details(pid):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = ",".join([
        "formatted_address","international_phone_number","geometry",
        "website","price_level","rating","user_ratings_total",
        "opening_hours","types","permanently_closed","utc_offset",
        "editorial_summary","photos"
    ])
    params = {"place_id": pid, "fields": fields, "key": GOOGLE_API_KEY}
    time.sleep(API_CALL_INTERVAL)
    res = requests.get(url, params=params).json().get("result", {})
    # extract city
    # city = extract_city(res.get("address_components", []))
    out = {
        #  "city":            city,
        "g_description": res.get("editorial_summary", {}).get("overview","") or "",
        "address":       res.get("formatted_address"),
        "phone":         res.get("international_phone_number"),
        "website":       res.get("website"),
        "priceLevel":    res.get("price_level"),
        "rating":        res.get("rating"),
        "ratingCount":   res.get("user_ratings_total"),
        "types":         res.get("types"),
        "permanentlyClosed": res.get("permanently_closed"),
        "utcOffset":     res.get("utc_offset"),
        "openingPeriods": res.get("opening_hours", {}).get("periods"),
    }
    geom = res.get("geometry", {}).get("location", {})
    out["latitude"], out["longitude"] = geom.get("lat"), geom.get("lng")
    urls, keys = build_image_urls(res.get("photos"))
    out["g_image_urls"], out["imageKeys"] = urls, keys
    return out

# def download_and_upload_image(url, place_id, idx):
#     try:
#         resp = requests.get(url, stream=True, timeout=30)
#         resp.raise_for_status()
#     except Exception as e:
#         print(f"  ⚠️  Image download failed ({url}): {e}")
#         return None
#     filename = f"lp_attractions/{place_id}_{idx+1}.jpg"
#     blob = bucket.blob(filename)
#     blob.upload_from_string(resp.content, content_type="image/jpeg")
#     try:
#         blob.make_public()
#         return blob.public_url
#     except:
#         return blob.generate_signed_url(version="v4", expiration=3600)
def download_and_upload_image(url, place_id, idx):
    """Download an image with retries, then upload to GCS."""
    backoff = INITIAL_BACKOFF
    data = None

    for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        try:
            # you can try stream=False here if you prefer one-shot
            resp = requests.get(url, stream=True, timeout=30)
            resp.raise_for_status()
            # read content only once
            data = resp.content
            break
        except RequestException as e:
            print(f"  ⚠️  Attempt {attempt} failed for image {url!r}: {e}")
            if attempt < MAX_DOWNLOAD_RETRIES:
                time.sleep(backoff)
                backoff *= 2
            else:
                print(f"  ❌  Skipping image after {MAX_DOWNLOAD_RETRIES} attempts.")
    # if we never got data, fall back to returning None
    if data is None:
        return None

    # now upload to GCS
    filename = f"lp_attractions/{place_id}_{idx+1}.jpg"
    blob = bucket.blob(filename)
    try:
        blob.upload_from_string(data, content_type="image/jpeg")
        blob.make_public()
        return blob.public_url
    except Exception as e:
        print(f"  ⚠️  Upload failed for {filename}: {e}")
        return None


# ───── 4) MAIN LOOP ─────────────────────────────────────────────────────────────

def process_file(path):
    slug = os.path.basename(path).split("_")[0]
    country_id = COUNTRY_ID_MAP.get(slug)
    if not country_id:
        print(f"[!] No COUNTRY_ID_MAP entry for '{slug}', skipping.")
        return

    with open(path, "r", encoding="utf-8") as f:
        records = json.load(f)[:MAX_ITEMS]

    coll = db.collection("allplaces") \
             .document(country_id) \
             .collection("top_attractions")

    print(f"\n▶️  Uploading {len(records)} attractions for '{slug}' → countryId={country_id}")
    for rec in records:
        name = rec.get("name")
        lat  = rec.get("latitude")
        lon  = rec.get("longitude")
        idx  = rec.get("index")

        # 1) Google enrichment
        pid = find_google_place_id(name, lat, lon)
        if pid:
            rec["placeId"] = pid
            details = get_place_details(pid)

            # merge Google fields onto rec
            rec.update({
                # "city":            details["city"],
                "g_description":   details["g_description"],
                "address":         details["address"],
                "phone":           details["phone"],
                "website":         details["website"],
                "priceLevel":      details["priceLevel"],
                "rating":          details["rating"],
                "ratingCount":     details["ratingCount"],
                "types":           details["types"],
                "permanentlyClosed": details["permanentlyClosed"],
                "utcOffset":       details["utcOffset"],
                "openingPeriods":  details["openingPeriods"],
                "latitude":        details["latitude"],
                "longitude":       details["longitude"],
                "imageKeys":       details["imageKeys"],
            })

            # build combined URL list: LP first + Google ones
            lp_img = rec.get("image_url")
            rec["g_image_urls"] = ([lp_img] if lp_img else []) + details["g_image_urls"]
        else:
            rec["city"] = None
            print(f"   ⚠️  No Google place_id for [{idx}] {name}")

        # 2) Upload LP+Google images into Storage
        uploaded = []
        for i, url in enumerate(rec.get("g_image_urls", [])):
            up = download_and_upload_image(url, pid or idx, i)
            uploaded.append(up or url)
        rec["g_image_urls"] = uploaded
        if uploaded:
            rec["image_url"] = uploaded[0]

        # 3) Write into Firestore
        doc_id = pid or str(idx)
        coll.document(doc_id).set(rec)
        print(f"   ✅  {doc_id}: {name}")

def main():
    for path in glob.glob(JSON_GLOB):
        process_file(path)
    print("\n🎉  All countries uploaded.")

if __name__ == "__main__":
    main()

