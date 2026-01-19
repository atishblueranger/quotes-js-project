#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 3 — Build & Upload to Firestore/Storage
- Reads playlist_items_resolved.json from Step 2.5
- Fetches Google Place Details with proper field names (SDK, then REST fallback)
- Caches Details and photo refs locally
- Uploads up to N Google photo images per place (or static map fallback)
- Writes playlist doc and places subcollection in Firestore
"""

import json, os, re, sys, time, hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

# Optional: load .env
try:
    from dotenv import load_dotenv
    # Force load .env from the same directory as this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(script_dir, '.env'))
except Exception:
    pass

# ------------------------------- CONFIG -------------------------------
BASE_DIR = Path(__file__).resolve().parent

# Input / Firestore
INPUT_JSON = BASE_DIR / "playlist_items_resolved.json"   # <-- resolved by Step 2.5
SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
PROJECT_ID = "mycasavsc"
COLLECTION = "playlistsNew"
BUCKET_NAME = "mycasavsc.appspot.com"  # Firebase Storage bucket

# Presentation / metadata
IMAGE_BASE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg"
G_IMAGE_TEMPLATE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
G_IMAGE_COUNT = 1
SOURCE = "original"
CATEGORY = "Travel"
UTC_OFFSET_DEFAULT = 330   # minutes (fallback)

# Behavior
DRY_RUN = False
LIMIT = 0
FILTER_TO_PUBLISHABLE = True    # only upload items marked "publishable" by Step 2.5
MIN_ITEMS_REQUIRED = 5          # <--- NEW CONFIG

CITY_ID_MAP = {
    #     # "Chennai(Madras)":"40"
    # "Guwahati":"524"
#     # "Tamil Nadu":"86691"
#     # "Assam" :"86937"
#     # "Sikkim": "86928",
#     # "Phuket": "12",
#     # "India": "86661",
#     # "Malappuram":"1660"
    # "Agartala":"909"
    # 'Agra': '60' # Attractions Av
    # 'Ahemedabad': '161'#AA
    # "Alibaug": "1480"
    # "Alwar": "1241" # AA
    # "Andaman and Nicobar Islands":"86944",
    # "Auli":"2477"
    # "Aurangabad": "7353"
    # "Balasore":"2366" ##TRipadvisor Wanderlog
    # "Bathinda":"2346"
    # "Bekal": "4211"
    # "Bidar":"2736"
    # "Bhubaneswar":"444"
    # "Chikmagalur":"787"
    # "Coonoor":"1887"
    # "Darjeeling":"349"
    # "Diu": "1743"
    # "Dwarka":"1983"
    #    "Gangtok": "503" 
    # "Gulmarg":"1006"
    # "Gwalior":"1291"
    # "Hampi":"696"
    # "Idukki":"1059"
    # "Jabalpur":"1406"
    # "Khajuraho":"850"
    # "Kanyakumari":"926"
    # "Kollam":"714"
    # "Kodaikanal":"842"
# "Kozhikode": "527"
# "Kolkata": "69"
# "Lakshadweep": "88144"
# "Nagpur":"398"
# "Nashik": "449"
# "Ooty": "480"
# "Ottapalam": "9178"
# "Patna":"673"
# "Pattadakal":"3890" 
# "Pelling":"3126",
# "Poovar":"179499",
# "Porbandar":"1331",
# "Pondicherry":"334"
# "Puri":"975"
# "Rajkot":"894"
# "Ratnagiri":"982"
# "Saputara":"1854"
# "Shillong":"728"
# "Sirsi": "1604",
# "Surat":"478"
# "Udaipur":"91"
# "Vadodara":"348"
# "Uttarkashi": "2252"
# "Vellore":"1463"
# "Vrindavan":"1874"
# "Warangal":"1442"
# "Himachal Pradesh":"86863",
# "Bihar": "86927",
# "Odisha":"86904"
# "India":"86661"

# "Bali":"86662",
# "Baku":"82",
# "Bhutan":"86819"
# "Thimphu":"209"
# "Maldives":"86983"   
# "Hong Kong":"15"   
# "Australia":"91387"
# "Perth"
# "Sydney":"82574",
# "Gold Coast ":"82578",
# "lombok":"36"
# "Jakarta":"46"
# "Denpasar":"14"
# "Indonesia":"86663"
# "Malaysia":"86683"
# "Kuala Lumpur":"33"
# "Vancouver":"58047"
# "Bangkok":"4"
# "Chanthaburi":"1313"
# "Pattaya":"80"
# "Phuket":"51"
# "Thailand":"86651"
# "Japan":"86647"
# "Austria":"88384"
# "Vienna": "9631"
# "Brussels":"9649"
# "Belgium":"88408"
# "Varna":"9931"
# "Argentina":"135385",
# "Costa-Rica":"90651"
# "Dubrovnik":"9704"
# "Prague":"9620"
# "Chile":"135391" 
# "Ecuador":"135396"
# "Estonia":"88455",
# "Fiji":"91403"
# "Helsinki":"9667"
# "Finland":"88434"
# "France":"88358"
# "Paris":"9614"
# "Georgia":"88420"
#  "Frankfurt":"9655"
# "Munich":"9645"
# "Germany":"88368"
# "Greece":"88375"
# "Santorini":"9697"
# "Iceland":"88419"
# "Florence":"9630"
# "Bari":"9807"
# "Naples":"9635"
# "Sardinia":"9619"
# "Venice":"9634"
# "Italy":"88352"
# "Jamaica":"81179"
# "Laos":"86797"
# "Lithuania":"88477"
# "Mauritius":"79304"
# "Monaco":"89098"
# "Montenegro":"88654"j
# "Nepal":"86667"
# "New-zealand":"91394"
# "Auckland":"82576"
# "Christchurch":"82579"
# "Oman":"91536"
# "Cebu-city":"188"
# "Makati":"244"
# "Davao-city":"394"
# "Quezon-city":"258"
# "Poland":"88403"
# "Portugal":"88376"
# "Lisbon":"9626"
# "Singapore":"7",
# "Sri-lanka":"86693"
# "Kandy":"118"
# "Colombo":"43"
# "South-africa":"90764"
# "South-korea":"86656"
# "Seoul":"9"
# "Spain":"88362"
# "Barcelona":"9617"
# "Madrid":"9621"
# "Seville":"9641"
# "Valencia":"9657"
# "Switzerland":"88437"
# "Zurich":"9668"
# "Sweden":"88445"
# "Stockholm":"9673"
# "Tanzania":"90761"
# "Turkiye":"88367"
# "Dubai":"85939"
# "Scotland":"88390"
# "the-united-kingdom":"88359"
# "London":"9613"
# "Boston":"58162"
# "Chicago":"58146"
"Las-vegas"








}

# Google Places / Photos
GOOGLE_MAPS_API_KEY = "AIzaSyBe-suda095R60l0G1NrdYeIVKjUCxwK_8"  # <- prefer env var
PHOTO_MAX_WIDTH = 1600
PHOTO_SLEEP_SEC = 0.05
GMAPS_LANGUAGE = "en-IN"

# NEW: local caches to avoid repeated calls
CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
PHOTOS_META_CACHE = CACHE_DIR / "photos_meta_cache.json"
DETAILS_CACHE = CACHE_DIR / "details_cache.json"

# Optional Static Map fallback so we always have at least one image
STATICMAP_FALLBACK = True
STATICMAP_ZOOM = 12
STATICMAP_SIZE = "1600x900"

# -------------------------- Deps --------------------------------------
try:
    import firebase_admin
    from firebase_admin import credentials, firestore, storage
except Exception:
    firebase_admin = None
    credentials = None
    firestore = None
    storage = None

try:
    from google.cloud.firestore_v1 import FieldFilter
except Exception:
    FieldFilter = None

try:
    import requests
except Exception:
    requests = None

# Google Maps SDK (optional; we fallback to REST anyway)
try:
    import googlemaps
except Exception:
    googlemaps = None

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **kwargs): return x

# ----------------------------- Helpers --------------------------------
def ensure_playlist_cover_image(bucket, list_id: int, places_docs: List[Dict[str, Any]]) -> Optional[str]:
    """
    Ensures a cover image exists at playlistsNew_images/{list_id}/1.jpg.
    """
    if not bucket:
        return None

    dst_key = f"playlistsNew_images/{list_id}/1.jpg"
    dst_blob = bucket.blob(dst_key)
    if dst_blob.exists():
        return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

    # Find first place with an uploaded image
    for p in places_docs:
        pid = p.get("placeId")
        if not pid:
            continue
        src_key = f"playlistsPlaces/{list_id}/{pid}/1.jpg"
        src_blob = bucket.blob(src_key)
        if src_blob.exists():
            bucket.copy_blob(src_blob, bucket, dst_key)
            dst_blob = bucket.blob(dst_key)
            dst_blob.cache_control = "public, max-age=31536000"
            try:
                dst_blob.patch()
            except Exception:
                pass
            return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

    return None

def md5_8(s: str) -> str:
    import hashlib as _h
    return _h.md5(s.encode("utf-8")).hexdigest()[:8]

def slugify(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"[^\w\s-]+", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    text = re.sub(r"^-+|-+$", "", text)
    return text or "untitled"

def build_unique_slug(raw: Dict[str, Any]) -> str:
    title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
    city = raw.get("placeName") or raw.get("city") or "India"
    subtype = str(raw.get("subtype", "destination")).strip().lower()
    slug = f"{slugify(title)}-{slugify(city)}-{subtype}"
    source_urls = raw.get("source_urls", [])
    if source_urls:
        urls_str = str(sorted(source_urls))
        urls_hash = hashlib.md5(urls_str.encode()).hexdigest()[:6]
        slug = f"{slug}-{urls_hash}"
    return slug

# ----------------------- Photo Download + Upload ----------------------
class PhotoUploader:
    def __init__(self, api_key: Optional[str], bucket):
        if not requests:
            raise RuntimeError("requests not installed. pip install requests")
        self.api_key = api_key
        self.bucket = bucket

    # def _photo_url(self, ref: str, max_width: int) -> str:
    #     return (
    #         "https://maps.googleapis.com/maps/api/place/photo"
    #         f"?maxwidth={max_width}&photo_reference={ref}&key={self.api_key}"
    #     )
    def _photo_url(self, ref: str, max_width: int) -> str:
        return (
            "https://maps.googleapis.com/maps/api/place/photo"
            f"?maxwidth={max_width}&photo_reference={ref}&key={self.api_key}"
        )  # <-- Properly indented inside the function

    def upload_place_photos(self, list_id: int, place_id: str, refs: List[str], count: int) -> List[int]:
        """Return list of successful photo indices (e.g., [1,2,3])."""
        if not self.bucket or not place_id or not refs or not self.api_key:
            return []
        written = []
        for i, ref in enumerate(refs[:max(1, count)], start=1):
            dest = f"playlistsPlaces/{list_id}/{place_id}/{i}.jpg"
            blob = self.bucket.blob(dest)
            # Optimize: Check if exists first to save bandwidth? (Optional, skipping for now)
            try:
                resp = requests.get(self._photo_url(ref, PHOTO_MAX_WIDTH), timeout=30)
                resp.raise_for_status()
                blob.cache_control = "public, max-age=31536000"
                blob.upload_from_string(resp.content, content_type=resp.headers.get("Content-Type") or "image/jpeg")
                written.append(i)
                print(f"  📷 Uploaded photo {i} for {place_id}")
                time.sleep(PHOTO_SLEEP_SEC)
            except Exception as e:
                print(f"⚠️ Photo upload failed: {dest} -> {e}")
        return written

    def upload_static_map(self, list_id: int, place_id: str, lat: Optional[float], lng: Optional[float],
                          zoom: int = STATICMAP_ZOOM, size: str = STATICMAP_SIZE) -> Optional[int]:
        if not self.bucket or lat is None or lng is None or not GOOGLE_MAPS_API_KEY:
            return None
        dest = f"playlistsPlaces/{list_id}/{place_id}/1.jpg"
        blob = self.bucket.blob(dest)
        try:
            url = (
                "https://maps.googleapis.com/maps/api/staticmap"
                f"?center={lat},{lng}&zoom={zoom}&size={size}&markers={lat},{lng}"
                f"&scale=2&maptype=roadmap&key={GOOGLE_MAPS_API_KEY}"
            )
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            blob.cache_control = "public, max-age=31536000"
            blob.upload_from_string(resp.content, content_type=resp.headers.get("Content-Type") or "image/png")
            print(f"  🗺️  Uploaded static map as 1.jpg for {place_id}")
            return 1
        except Exception as e:
            print(f"⚠️ Static map upload failed for {place_id}: {e}")
            return None

# ---------------------- Photo meta cache helpers ----------------------
def _load_photos_cache() -> Dict[str, List[str]]:
    if PHOTOS_META_CACHE.exists():
        try:
            return json.loads(PHOTOS_META_CACHE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_photos_cache(cache: Dict[str, List[str]]) -> None:
    try:
        PHOTOS_META_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def fetch_photo_refs_for_place(place_id: str, gmaps_client, limit: int = 10) -> List[str]:
    if not place_id or not gmaps_client:
        return []
    _cache = _load_photos_cache()
    if place_id in _cache:
        return _cache[place_id][:limit]
    try:
        res = gmaps_client.place(
            place_id=place_id,
            fields=["photo"],
            language=GMAPS_LANGUAGE,
        )
        result = res.get("result") or {}
        refs: List[str] = []
        for p in (result.get("photos") or [])[:limit]:
            ref = p.get("photo_reference") or p.get("photoReference")
            if ref:
                refs.append(ref)
        _cache[place_id] = refs
        _save_photos_cache(_cache)
        time.sleep(PHOTO_SLEEP_SEC)
        return refs
    except Exception as e:
        print(f"⚠️ Failed to fetch photos for {place_id}: {e}")
        _cache[place_id] = []
        _save_photos_cache(_cache)
        return []

# ---------------------- Details cache + fetch -------------------------
DETAILS_FIELDS = [
    "place_id", "name", "geometry/location", "formatted_address",
    "type", "website", "formatted_phone_number", "international_phone_number",
    "opening_hours", "price_level", "permanently_closed", "business_status",
    "rating", "user_ratings_total", "photo", "utc_offset", "editorial_summary", "reviews"
]

def _load_details_cache() -> Dict[str, Any]:
    if DETAILS_CACHE.exists():
        try:
            return json.loads(DETAILS_CACHE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_details_cache(cache: Dict[str, Any]) -> None:
    try:
        DETAILS_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def fetch_place_details(place_id: str, gmaps_client, api_key: Optional[str], language: str = GMAPS_LANGUAGE) -> Dict[str, Any]:
    if not place_id:
        return {}

    result = {}
    if gmaps_client:
        try:
            res = gmaps_client.place(place_id=place_id, fields=DETAILS_FIELDS, language=language)
            result = res.get("result") or {}
        except Exception:
            pass

    if not result and requests and api_key:
        try:
            url = "https://maps.googleapis.com/maps/api/place/details/json"
            params = {"place_id": place_id, "fields": ",".join(DETAILS_FIELDS), "key": api_key, "language": language}
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200 and r.json().get("status") == "OK":
                result = r.json().get("result") or {}
        except Exception:
            pass

    geom = (result.get("geometry") or {}).get("location") or {}
    opening_hours = result.get("opening_hours") or {}
    editorial = result.get("editorial_summary") or {}
    photos = result.get("photos") or []
    types = result.get("types") or []
    business_status = (result.get("business_status") or "").upper()
    permanently_closed = bool(result.get("permanently_closed")) or (business_status == "CLOSED_PERMANENTLY")

    details_norm = {
        "placeId": result.get("place_id") or place_id,
        "name": result.get("name"),
        "latitude": geom.get("lat"),
        "longitude": geom.get("lng"),
        "types": types,
        "address": result.get("formatted_address"),
        "website": result.get("website"),
        "internationalPhoneNumber": result.get("international_phone_number") or result.get("formatted_phone_number"),
        "openingPeriods": opening_hours.get("periods") or [],
        "priceLevel": result.get("price_level"),
        "permanentlyClosed": permanently_closed,
        "rating": result.get("rating") or 0,
        "numRatings": result.get("user_ratings_total") or 0,
        "utcOffset": result.get("utc_offset"),
        "googleDescription": editorial.get("overview"),
        "reviews": result.get("reviews") or [],
        "photo_refs": [p.get("photo_reference") for p in photos if p.get("photo_reference")]
    }
    return details_norm

# ---------------------------- Firestore ------------------------------
class FirestoreWriter:
    def __init__(self, sa_path: str, project_id: Optional[str], collection: str, dry: bool = False):
        self.collection = collection
        self.dry = dry
        self.db = None
        self.col_ref = None
        self.next_id = None

        if not dry:
            if not firebase_admin:
                raise RuntimeError("firebase-admin not installed. pip install firebase-admin")
            if not firebase_admin._apps:
                cred = credentials.Certificate(sa_path)
                firebase_admin.initialize_app(cred, {
                    "projectId": project_id,
                    "storageBucket": BUCKET_NAME,
                })
            self.db = firestore.client()
            self.col_ref = self.db.collection(self.collection)
            self.next_id = self._compute_start_id()
        else:
            self.next_id = 1

    def _compute_start_id(self) -> int:
        max_id = 0
        try:
            for doc in self.col_ref.select([]).stream():
                try:
                    v = int(doc.id)
                    if v > max_id:
                        max_id = v
                except ValueError:
                    continue
        except Exception:
            pass
        return max_id + 1

    def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, Optional[Any], bool]:
        if self.dry:
            return str(self.next_id), None, False
        try:
            if FieldFilter is not None:
                existing = list(self.col_ref.where(filter=FieldFilter("slug", "==", slug)).limit(1).stream())
            else:
                existing = list(self.col_ref.where("slug", "==", slug).limit(1).stream())
        except Exception:
            existing = []
        if existing:
            ref = existing[0].reference
            print(f"  🔄 Reusing existing document ID {ref.id} for slug: {slug}")
            return ref.id, ref, True
        new_id = str(self.next_id)
        self.next_id += 1
        ref = self.col_ref.document(new_id)
        print(f"  ✨ Assigning new document ID {new_id} for slug: {slug}")
        return new_id, ref, False

    def upsert_playlist_with_known_id(self, doc_id: str, playlist_doc: Dict[str, Any], places: List[Dict[str, Any]]) -> str:
        if self.dry:
            print(f"[DRY-RUN] Would write '{playlist_doc['title']}' as doc {doc_id} with {len(places)} places")
            return doc_id

        doc_ref = self.col_ref.document(doc_id)
        doc_ref.set(playlist_doc, merge=False)

        sub = doc_ref.collection("places")
        old = list(sub.stream())
        for i in range(0, len(old), 200):
            batch = self.db.batch()
            for doc in old[i:i+200]:
                batch.delete(doc.reference)
            batch.commit()

        for i in range(0, len(places), 450):
            batch = self.db.batch()
            for item in places[i:i+450]:
                sub_id = item.get("placeId") or item.get("_id")
                batch.set(sub.document(sub_id), item)
            batch.commit()

        return doc_id

# ------------------------- Builders / Mappers -----------------------
def build_playlist_doc(raw: Dict[str, Any],
                       list_id: int,
                       image_base: str,
                       source: str,
                       category: str,
                       city_id_map: Dict[str, str],
                       slug: str) -> Dict[str, Any]:
    title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
    city = raw.get("placeName") or raw.get("city") or "India"
    subtype = str(raw.get("subtype","destination")).strip().lower()
    if subtype not in {"destination","poi"}:
        subtype = "destination"

    src_urls = raw.get("source_urls") or []
    if isinstance(src_urls, str):
        src_urls = [src_urls]
    dedup, seen = [], set()
    for u in src_urls:
        if isinstance(u, str):
            u2 = u.strip()
            if u2 and u2 not in seen:
                seen.add(u2); dedup.append(u2)

    return {
        "list_id": str(list_id),
        "title": title,
        "description": raw.get("description") ,
        "imageUrl": image_base.format(list_id=list_id),
        "source": source,
        "category": category,
        "city_id": city_id_map.get(city, city),
        "city": city,
        "slug": slug,
        "subtype": subtype,
        "source_urls": dedup,
        "created_ts": int(time.time())
    }

def build_g_image_urls(g_image_template: str, list_id: int, place_id: Optional[str], uploaded_indices: List[int]) -> List[str]:
    if not place_id or not uploaded_indices:
        return []
    return [g_image_template.format(list_id=list_id, placeId=place_id, n=n) for n in uploaded_indices]

def normalize_place_item(idx1_based: int,
                         item: Dict[str, Any],
                         enrich: Dict[str, Any],
                         utc_offset: int,
                         list_id: int,
                         g_image_urls: List[str]) -> Dict[str, Any]:
    general_desc = item.get("generalDescription") or item.get("description") or None
    description_val = enrich.get("googleDescription") or None
    place_id = enrich.get("placeId")

    return {
        "tripadvisorRating": item.get("tripadvisorRating", 0),
        "description": description_val,
        "website": enrich.get("website"),
        "index": idx1_based,
        "id": "",
        "categories": enrich.get("types", []),
        "utcOffset": utc_offset,
        "maxMinutesSpent": item.get("maxMinutesSpent", None),
        "rating": enrich.get("rating", 0) or 0,
        "numRatings": enrich.get("numRatings", 0) or 0,
        "sources": item.get("sources", []),
        "imageKeys": item.get("imageKeys", []),
        "tripadvisorNumRatings": item.get("tripadvisorNumRatings", 0),
        "openingPeriods": enrich.get("openingPeriods", []),
        "generalDescription": general_desc,
        "name": enrich.get("name") or item.get("name"),
        "placeId": place_id,
        "internationalPhoneNumber": enrich.get("internationalPhoneNumber"),
        "reviews": enrich.get("reviews", []),
        "ratingDistribution": item.get("ratingDistribution", {}),
        "priceLevel": item.get("priceLevel", None) if item.get("priceLevel", None) is not None else enrich.get("priceLevel"),
        "permanentlyClosed": bool(item.get("permanentlyClosed", False) or enrich.get("permanentlyClosed", False)),
        "minMinutesSpent": item.get("minMinutesSpent", None),
        "longitude": enrich.get("longitude"),
        "address": enrich.get("address"),
        "latitude": enrich.get("latitude"),
        "g_image_urls": g_image_urls,
        "travel_time": item.get("travel_time"),
    }

# -------------------------------- Main ------------------------------
def main():
    if not INPUT_JSON.exists():
        print(f"❌ Input file not found: {INPUT_JSON}", file=sys.stderr); sys.exit(1)
    if not DRY_RUN and not Path(SERVICE_ACCOUNT_JSON).exists():
        print(f"❌ Service account JSON not found: {SERVICE_ACCOUNT_JSON}", file=sys.stderr); sys.exit(1)
    if not requests:
        print("❌ 'requests' is required. pip install requests", file=sys.stderr); sys.exit(1)

    data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        print("❌ Input must be a JSON array of playlists.", file=sys.stderr); sys.exit(1)

    # Firestore / Storage init
    writer = FirestoreWriter(SERVICE_ACCOUNT_JSON, PROJECT_ID, COLLECTION, dry=bool(DRY_RUN))
    bucket = storage.bucket(BUCKET_NAME) if (not DRY_RUN and firebase_admin) else None

    # Photos
    photo_uploader = PhotoUploader(GOOGLE_MAPS_API_KEY, bucket) if (GOOGLE_MAPS_API_KEY and not DRY_RUN) else None
    print(f"Photo uploader enabled: {bool(photo_uploader)}")

    gmaps_client = googlemaps.Client(key=GOOGLE_MAPS_API_KEY) if (googlemaps and GOOGLE_MAPS_API_KEY) else None
    if not gmaps_client:
        print("ℹ️ Google Maps SDK client not initialized. Using REST fallback.")

    processed = 0

    for raw in tqdm(data, desc="Uploading playlists"):
        if LIMIT and processed >= LIMIT:
            break

        title = (raw.get("playlistTitle") or raw.get("title") or "Untitled")
        city  = (raw.get("placeName") or raw.get("city") or "India")
        slug = build_unique_slug(raw)
        print(f"\n🎯 Processing: '{title}' with slug: '{slug}'")

        doc_id, _, existed = writer.assign_doc_id_for_slug(slug)
        list_id = int(doc_id)

        playlist_doc = build_playlist_doc(
            raw=raw, list_id=list_id, image_base=IMAGE_BASE, source=SOURCE,
            category=CATEGORY, city_id_map=CITY_ID_MAP, slug=slug
        )

        items = raw.get("items", [])
        if FILTER_TO_PUBLISHABLE:
            items = [it for it in items if it.get("resolution_status") == "publishable"]
            print(f"  📋 Filtered to {len(items)} publishable items")

        # --- CRITICAL FIX: CHECK MIN ITEMS HERE ---
        if len(items) < MIN_ITEMS_REQUIRED:
            print(f"⚠️  SKIPPING UPLOAD for '{title}': Only {len(items)} publishable items found (Min required: {MIN_ITEMS_REQUIRED})")
            continue
        # ------------------------------------------

        places_docs = []
        details_cache = _load_details_cache()

        for idx, item in enumerate(items, start=1):
            place_id = item.get("place_id")
            print(f"    🏢 Processing: {item.get('name', 'Unknown')} (ID: {place_id})")

            offset_min = item.get("utc_offset_minutes", UTC_OFFSET_DEFAULT)
            enrich = {
                "placeId": place_id, "name": item.get("name"), "latitude": item.get("lat"),
                "longitude": item.get("lng"), "types": item.get("types", []), "rating": item.get("rating", 0) or 0,
                "numRatings": item.get("reviews", 0) or 0, "website": item.get("website"),
                "address": item.get("address"), "internationalPhoneNumber": item.get("phone"),
                "openingPeriods": item.get("opening", []), "priceLevel": item.get("price_level"),
                "permanentlyClosed": item.get("permanently_closed", False), "reviews": item.get("reviews", []),
                "utcOffset": offset_min, "googleDescription": None,
            }

            det = details_cache.get(place_id)
            if not det:
                det = fetch_place_details(place_id, gmaps_client, GOOGLE_MAPS_API_KEY)
                details_cache[place_id] = det or {}
                _save_details_cache(details_cache)

            if det:
                for k in ["name","latitude","longitude","types","rating","numRatings","website","address",
                          "internationalPhoneNumber","openingPeriods","priceLevel","permanentlyClosed",
                          "utcOffset","googleDescription","reviews"]:
                    v = det.get(k)
                    if v not in (None, []): enrich[k] = v

            # Gather photo refs (Updated robust logic)
            photo_refs = (item.get("photo_refs") or [])
            if len(photo_refs) < G_IMAGE_COUNT and det:
                for r in (det.get("photo_refs") or []):
                    if r not in photo_refs: photo_refs.append(r)
            
            if len(photo_refs) < G_IMAGE_COUNT and gmaps_client and place_id:
                more_refs = fetch_photo_refs_for_place(place_id, gmaps_client, limit=10)
                if more_refs:
                    for r in more_refs:
                        if r not in photo_refs: photo_refs.append(r)
                    print(f"    📷 Fetched more refs via API. Total: {len(photo_refs)}")
            
            photo_refs = photo_refs[:10]

            uploaded_idxs = []
            if photo_uploader and place_id and photo_refs:
                uploaded_idxs = photo_uploader.upload_place_photos(list_id, place_id, photo_refs, G_IMAGE_COUNT)
            
            if photo_uploader and STATICMAP_FALLBACK and not uploaded_idxs and place_id:
                static_idx = photo_uploader.upload_static_map(list_id, place_id, enrich.get("latitude"), enrich.get("longitude"))
                if static_idx: uploaded_idxs = [static_idx]

            g_image_urls = build_g_image_urls(G_IMAGE_TEMPLATE, list_id, place_id, uploaded_idxs)

            final_utc_offset = enrich.get("utcOffset", offset_min) or UTC_OFFSET_DEFAULT
            place_doc = normalize_place_item(idx, item, enrich, final_utc_offset, list_id, g_image_urls)
            places_docs.append(place_doc)

        if bucket:
            cover_url = ensure_playlist_cover_image(bucket, list_id, places_docs)
            if cover_url:
                playlist_doc["imageUrl"] = cover_url
                print(f"  🖼️ Set cover image: {cover_url}")

        writer.upsert_playlist_with_known_id(str(list_id), playlist_doc, places_docs)
        processed += 1
        print(f"→ {'Updated' if existed else 'Created'} '{playlist_doc['title']}' as ID {list_id}")

    print(f"\n✅ Done. Processed {processed} playlist(s). DRY_RUN={DRY_RUN}")

if __name__ == "__main__":
    main()


# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# """
# Step 3 — Build & Upload to Firestore/Storage
# - Reads playlist_items_resolved.json from Step 2.5
# - Fetches Google Place Details with proper field names (SDK, then REST fallback)
# - Caches Details and photo refs locally
# - Uploads up to N Google photo images per place (or static map fallback)
# - Writes playlist doc and places subcollection in Firestore
# """

# import json, os, re, sys, time, hashlib
# from pathlib import Path
# from typing import Dict, Any, List, Optional, Tuple

# # Optional: load .env
# try:
#     from dotenv import load_dotenv
#     # Force load .env from the same directory as this script
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     load_dotenv(os.path.join(script_dir, '.env'))
# except Exception:
#     pass

# # ------------------------------- CONFIG -------------------------------
# BASE_DIR = Path(__file__).resolve().parent

# # Input / Firestore
# INPUT_JSON = BASE_DIR / "playlist_items_resolved.json"   # <-- resolved by Step 2.5
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# PROJECT_ID = "mycasavsc"
# COLLECTION = "playlistsNew"
# BUCKET_NAME = "mycasavsc.appspot.com"  # Firebase Storage bucket

# # Presentation / metadata
# IMAGE_BASE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg"
# G_IMAGE_TEMPLATE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
# G_IMAGE_COUNT = 3
# SOURCE = "original"
# CATEGORY = "Travel"
# UTC_OFFSET_DEFAULT = 330   # minutes (fallback)

# # Behavior
# DRY_RUN = False
# LIMIT = 0
# FILTER_TO_PUBLISHABLE = True    # only upload items marked "publishable" by Step 2.5

# CITY_ID_MAP = {
#     # "Chennai(Madras)":"40"
#     # "Guwahati":"524"
#     # "Tamil Nadu":"86691"
#     # "Assam" :"86937"
#     # "Sikkim": "86928",
#     # "Phuket": "12",
#     # "India": "86661",
#     # "Malappuram":"1660"
#     "Agartala":"909"
# }

# # Google Places / Photos
# GOOGLE_MAPS_API_KEY = "AIzaSyDEuy2i8AbSR-jnstZG22ndFqy71jtKbgg"  # <- prefer env var
# PHOTO_MAX_WIDTH = 1600
# PHOTO_SLEEP_SEC = 0.05
# GMAPS_LANGUAGE = "en-IN"

# # NEW: local caches to avoid repeated calls
# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(parents=True, exist_ok=True)
# PHOTOS_META_CACHE = CACHE_DIR / "photos_meta_cache.json"
# DETAILS_CACHE = CACHE_DIR / "details_cache.json"

# # Optional Static Map fallback so we always have at least one image
# STATICMAP_FALLBACK = True
# STATICMAP_ZOOM = 12
# STATICMAP_SIZE = "1600x900"

# # -------------------------- Deps --------------------------------------
# try:
#     import firebase_admin
#     from firebase_admin import credentials, firestore, storage
# except Exception:
#     firebase_admin = None
#     credentials = None
#     firestore = None
#     storage = None

# try:
#     from google.cloud.firestore_v1 import FieldFilter
# except Exception:
#     FieldFilter = None

# try:
#     import requests
# except Exception:
#     requests = None

# # Google Maps SDK (optional; we fallback to REST anyway)
# try:
#     import googlemaps
# except Exception:
#     googlemaps = None

# try:
#     from tqdm import tqdm
# except Exception:
#     def tqdm(x, **kwargs): return x

# # ----------------------------- Helpers --------------------------------
# def ensure_playlist_cover_image(bucket, list_id: int, places_docs: List[Dict[str, Any]]) -> Optional[str]:
#     """
#     Ensures a cover image exists at playlistsNew_images/{list_id}/1.jpg.
#     Copies the first existing place photo (…/placeId/1.jpg) to that path.
#     Returns the public URL if created/already present, else None.
#     """
#     if not bucket:
#         return None

#     dst_key = f"playlistsNew_images/{list_id}/1.jpg"
#     dst_blob = bucket.blob(dst_key)
#     if dst_blob.exists():
#         return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

#     # Find first place with an uploaded image
#     for p in places_docs:
#         pid = p.get("placeId")
#         if not pid:
#             continue
#         src_key = f"playlistsPlaces/{list_id}/{pid}/1.jpg"
#         src_blob = bucket.blob(src_key)
#         if src_blob.exists():
#             bucket.copy_blob(src_blob, bucket, dst_key)
#             dst_blob = bucket.blob(dst_key)
#             dst_blob.cache_control = "public, max-age=31536000"
#             try:
#                 dst_blob.patch()
#             except Exception:
#                 pass
#             return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

#     return None

# def md5_8(s: str) -> str:
#     import hashlib as _h
#     return _h.md5(s.encode("utf-8")).hexdigest()[:8]

# def slugify(text: str) -> str:
#     text = (text or "").strip().lower()
#     text = re.sub(r"[^\w\s-]+", "", text)
#     text = re.sub(r"[\s_-]+", "-", text)
#     text = re.sub(r"^-+|-+$", "", text)
#     return text or "untitled"

# def build_unique_slug(raw: Dict[str, Any]) -> str:
#     """
#     Build a more unique slug by including title, city, subtype, and URL hash.
#     This reduces the chance of slug collisions.
#     """
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"
#     subtype = str(raw.get("subtype", "destination")).strip().lower()
#     slug = f"{slugify(title)}-{slugify(city)}-{subtype}"
#     source_urls = raw.get("source_urls", [])
#     if source_urls:
#         urls_str = str(sorted(source_urls))
#         urls_hash = hashlib.md5(urls_str.encode()).hexdigest()[:6]
#         slug = f"{slug}-{urls_hash}"
#     return slug

# def default_description(title: str) -> str:
#     return (f'Dive into "{title}" — a handpicked list of places with quick notes, links, and essentials '
#             f'for fast trip planning and discovery.')

# # ----------------------- Photo Download + Upload ----------------------
# class PhotoUploader:
#     def __init__(self, api_key: Optional[str], bucket):
#         if not requests:
#             raise RuntimeError("requests not installed. pip install requests")
#         self.api_key = api_key
#         self.bucket = bucket

#     def _photo_url(self, ref: str, max_width: int) -> str:
#         return (
#             "https://maps.googleapis.com/maps/api/place/photo"
#             f"?maxwidth={max_width}&photo_reference={ref}&key={self.api_key}"
#         )

#     def upload_place_photos(self, list_id: int, place_id: str, refs: List[str], count: int) -> List[int]:
#         """Return list of successful photo indices (e.g., [1,2,3])."""
#         if not self.bucket or not place_id or not refs or not self.api_key:
#             return []
#         written = []
#         for i, ref in enumerate(refs[:max(1, count)], start=1):
#             dest = f"playlistsPlaces/{list_id}/{place_id}/{i}.jpg"
#             blob = self.bucket.blob(dest)
#             try:
#                 resp = requests.get(self._photo_url(ref, PHOTO_MAX_WIDTH), timeout=30)
#                 resp.raise_for_status()
#                 blob.cache_control = "public, max-age=31536000"
#                 blob.upload_from_string(resp.content, content_type=resp.headers.get("Content-Type") or "image/jpeg")
#                 written.append(i)
#                 print(f"  📷 Uploaded photo {i} for {place_id}")
#                 time.sleep(PHOTO_SLEEP_SEC)
#             except Exception as e:
#                 print(f"⚠️ Photo upload failed: {dest} -> {e}")
#         return written

#     # Static map fallback so we *always* have a hero image if coords exist
#     def upload_static_map(self, list_id: int, place_id: str, lat: Optional[float], lng: Optional[float],
#                           zoom: int = STATICMAP_ZOOM, size: str = STATICMAP_SIZE) -> Optional[int]:
#         if not self.bucket or lat is None or lng is None or not GOOGLE_MAPS_API_KEY:
#             return None
#         dest = f"playlistsPlaces/{list_id}/{place_id}/1.jpg"
#         blob = self.bucket.blob(dest)
#         try:
#             url = (
#                 "https://maps.googleapis.com/maps/api/staticmap"
#                 f"?center={lat},{lng}&zoom={zoom}&size={size}&markers={lat},{lng}"
#                 f"&scale=2&maptype=roadmap&key={GOOGLE_MAPS_API_KEY}"
#             )
#             resp = requests.get(url, timeout=30)
#             resp.raise_for_status()
#             blob.cache_control = "public, max-age=31536000"
#             blob.upload_from_string(resp.content, content_type=resp.headers.get("Content-Type") or "image/png")
#             print(f"  🗺️  Uploaded static map as 1.jpg for {place_id}")
#             return 1
#         except Exception as e:
#             print(f"⚠️ Static map upload failed for {place_id}: {e}")
#             return None

# # ---------------------- Photo meta cache helpers ----------------------
# def _load_photos_cache() -> Dict[str, List[str]]:
#     if PHOTOS_META_CACHE.exists():
#         try:
#             return json.loads(PHOTOS_META_CACHE.read_text(encoding="utf-8"))
#         except Exception:
#             return {}
#     return {}

# def _save_photos_cache(cache: Dict[str, List[str]]) -> None:
#     try:
#         PHOTOS_META_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
#     except Exception:
#         pass

# def fetch_photo_refs_for_place(place_id: str, gmaps_client, limit: int = 10) -> List[str]:
#     """
#     Ask Place Details for 'photo' (→ response has 'photos') and return up to `limit` photo_reference strings.
#     Uses a simple on-disk cache keyed by place_id (photos_meta_cache.json).
#     """
#     if not place_id or not gmaps_client:
#         return []
#     _cache = _load_photos_cache()
#     if place_id in _cache:
#         return _cache[place_id][:limit]
#     try:
#         res = gmaps_client.place(
#             place_id=place_id,
#             fields=["photo"],  # request 'photo' (valid field name)
#             language=GMAPS_LANGUAGE,
#         )
#         result = res.get("result") or {}
#         refs: List[str] = []
#         for p in (result.get("photos") or [])[:limit]:
#             ref = p.get("photo_reference") or p.get("photoReference")
#             if ref:
#                 refs.append(ref)
#         _cache[place_id] = refs
#         _save_photos_cache(_cache)
#         time.sleep(PHOTO_SLEEP_SEC)
#         return refs
#     except Exception as e:
#         print(f"⚠️ Failed to fetch photos for {place_id}: {e}")
#         _cache[place_id] = []
#         _save_photos_cache(_cache)
#         return []

# # ---------------------- Details cache + fetch -------------------------
# DETAILS_FIELDS = [
#     # Valid field names for Places Details (SDK will map to plural keys in response)
#     "place_id", "name", "geometry/location", "formatted_address",
#     "type",                      # request 'type' → response has 'types'
#     "website",
#     "formatted_phone_number", "international_phone_number",
#     "opening_hours",             # contains .periods
#     "price_level",
#     "permanently_closed", "business_status",
#     "rating", "user_ratings_total",
#     "photo",                     # request 'photo' → response has 'photos'
#     "utc_offset",                # minutes
#     "editorial_summary",
#     "reviews"
# ]

# def _load_details_cache() -> Dict[str, Any]:
#     if DETAILS_CACHE.exists():
#         try:
#             return json.loads(DETAILS_CACHE.read_text(encoding="utf-8"))
#         except Exception:
#             return {}
#     return {}

# def _save_details_cache(cache: Dict[str, Any]) -> None:
#     try:
#         DETAILS_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
#     except Exception:
#         pass

# def fetch_place_details(place_id: str, gmaps_client, api_key: Optional[str], language: str = GMAPS_LANGUAGE) -> Dict[str, Any]:
#     """
#     Fetch Place Details via googlemaps SDK; on failure, fall back to REST.
#     Returns a normalized dict with the keys Step 3 needs.
#     """
#     if not place_id:
#         return {}

#     result = {}

#     # Try SDK call first
#     if gmaps_client:
#         try:
#             res = gmaps_client.place(
#                 place_id=place_id,
#                 fields=DETAILS_FIELDS,
#                 language=language
#             )
#             result = res.get("result") or {}
#         except Exception as e:
#             print(f"⚠️ SDK details failed for {place_id}, falling back to REST: {e}")

#     # Fallback to REST if needed or if SDK returned empty
#     if not result and requests and api_key:
#         try:
#             url = "https://maps.googleapis.com/maps/api/place/details/json"
#             params = {"place_id": place_id, "fields": ",".join(DETAILS_FIELDS), "key": api_key, "language": language}
#             r = requests.get(url, params=params, timeout=20)
#             r.raise_for_status()
#             data = r.json()
#             if data.get("status") == "OK":
#                 result = data.get("result") or {}
#             else:
#                 print(f"⚠️ REST details returned status={data.get('status')} for {place_id}: {data.get('error_message')}")
#         except Exception as e:
#             print(f"❌ REST details failed for {place_id}: {e}")
#             result = {}

#     # Normalize to Step 3 'enrich' keys
#     geom = (result.get("geometry") or {}).get("location") or {}
#     opening_hours = result.get("opening_hours") or {}
#     editorial = result.get("editorial_summary") or {}
#     photos = result.get("photos") or []
#     types = result.get("types") or []

#     business_status = (result.get("business_status") or "").upper()
#     permanently_closed = bool(result.get("permanently_closed")) or (business_status == "CLOSED_PERMANENTLY")

#     details_norm = {
#         "placeId": result.get("place_id") or place_id,
#         "name": result.get("name"),
#         "latitude": geom.get("lat"),
#         "longitude": geom.get("lng"),
#         "types": types,
#         "address": result.get("formatted_address"),
#         "website": result.get("website"),
#         "internationalPhoneNumber": result.get("international_phone_number") or result.get("formatted_phone_number"),
#         "openingPeriods": opening_hours.get("periods") or [],
#         "priceLevel": result.get("price_level"),
#         "permanentlyClosed": permanently_closed,
#         "rating": result.get("rating") or 0,
#         "numRatings": result.get("user_ratings_total") or 0,
#         "utcOffset": result.get("utc_offset"),  # minutes
#         "googleDescription": editorial.get("overview"),
#         "reviews": result.get("reviews") or [],
#         "photo_refs": [p.get("photo_reference") for p in photos if p.get("photo_reference")]
#     }
#     return details_norm

# # ---------------------------- Firestore ------------------------------
# class FirestoreWriter:
#     def __init__(self, sa_path: str, project_id: Optional[str], collection: str, dry: bool = False):
#         self.collection = collection
#         self.dry = dry
#         self.db = None
#         self.col_ref = None
#         self.next_id = None

#         if not dry:
#             if not firebase_admin:
#                 raise RuntimeError("firebase-admin not installed. pip install firebase-admin")
#             if not firebase_admin._apps:
#                 cred = credentials.Certificate(sa_path)
#                 firebase_admin.initialize_app(cred, {
#                     "projectId": project_id,
#                     "storageBucket": BUCKET_NAME,
#                 })
#             self.db = firestore.client()
#             self.col_ref = self.db.collection(self.collection)
#             self.next_id = self._compute_start_id()
#         else:
#             self.next_id = 1

#     def _compute_start_id(self) -> int:
#         max_id = 0
#         try:
#             for doc in self.col_ref.select([]).stream():
#                 try:
#                     v = int(doc.id)
#                     if v > max_id:
#                         max_id = v
#                 except ValueError:
#                     continue
#         except Exception:
#             pass
#         return max_id + 1

#     def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, Optional[Any], bool]:
#         if self.dry:
#             return str(self.next_id), None, False
#         try:
#             if FieldFilter is not None:
#                 existing = list(self.col_ref.where(filter=FieldFilter("slug", "==", slug)).limit(1).stream())
#             else:
#                 existing = list(self.col_ref.where("slug", "==", slug).limit(1).stream())
#         except Exception:
#             existing = []
#         if existing:
#             ref = existing[0].reference
#             print(f"  🔄 Reusing existing document ID {ref.id} for slug: {slug}")
#             return ref.id, ref, True
#         new_id = str(self.next_id)
#         self.next_id += 1
#         ref = self.col_ref.document(new_id)
#         print(f"  ✨ Assigning new document ID {new_id} for slug: {slug}")
#         return new_id, ref, False

#     def upsert_playlist_with_known_id(self, doc_id: str, playlist_doc: Dict[str, Any], places: List[Dict[str, Any]]) -> str:
#         if self.dry:
#             print(f"[DRY-RUN] Would write '{playlist_doc['title']}' as doc {doc_id} with {len(places)} places")
#             return doc_id

#         doc_ref = self.col_ref.document(doc_id)
#         doc_ref.set(playlist_doc, merge=False)

#         sub = doc_ref.collection("places")
#         old = list(sub.stream())
#         for i in range(0, len(old), 200):
#             batch = self.db.batch()
#             for doc in old[i:i+200]:
#                 batch.delete(doc.reference)
#             batch.commit()

#         for i in range(0, len(places), 450):
#             batch = self.db.batch()
#             for item in places[i:i+450]:
#                 sub_id = item.get("placeId") or item.get("_id")
#                 batch.set(sub.document(sub_id), item)
#             batch.commit()

#         return doc_id

# # ------------------------- Builders / Mappers -----------------------
# def build_playlist_doc(raw: Dict[str, Any],
#                        list_id: int,
#                        image_base: str,
#                        source: str,
#                        category: str,
#                        city_id_map: Dict[str, str],
#                        slug: str) -> Dict[str, Any]:
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"
#     subtype = str(raw.get("subtype","destination")).strip().lower()
#     if subtype not in {"destination","poi"}:
#         subtype = "destination"

#     src_urls = raw.get("source_urls") or []
#     if isinstance(src_urls, str):
#         src_urls = [src_urls]
#     dedup, seen = [], set()
#     for u in src_urls:
#         if isinstance(u, str):
#             u2 = u.strip()
#             if u2 and u2 not in seen:
#                 seen.add(u2); dedup.append(u2)

#     return {
#         "list_id": str(list_id),
#         "title": title,
#         "description": raw.get("description") ,
#         "imageUrl": image_base.format(list_id=list_id),
#         "source": source,
#         "category": category,
#         "city_id": city_id_map.get(city, city),
#         "city": city,
#         "slug": slug,
#         "subtype": subtype,
#         "source_urls": dedup,
#         "created_ts": int(time.time())
#     }

# def build_g_image_urls(g_image_template: str, list_id: int, place_id: Optional[str], uploaded_indices: List[int]) -> List[str]:
#     """Build g_image_urls only for successfully uploaded photos."""
#     if not place_id or not uploaded_indices:
#         return []
#     return [g_image_template.format(list_id=list_id, placeId=place_id, n=n) for n in uploaded_indices]

# def normalize_place_item(idx1_based: int,
#                          item: Dict[str, Any],
#                          enrich: Dict[str, Any],
#                          utc_offset: int,
#                          list_id: int,
#                          g_image_urls: List[str]) -> Dict[str, Any]:
#     general_desc = item.get("generalDescription") or item.get("description") or None
#     description_val = enrich.get("googleDescription") or None
#     place_id = enrich.get("placeId")

#     return {
#         "tripadvisorRating": item.get("tripadvisorRating", 0),
#         "description": description_val,
#         "website": enrich.get("website"),
#         "index": idx1_based,
#         "id": "",
#         "categories": enrich.get("types", []),
#         "utcOffset": utc_offset,
#         "maxMinutesSpent": item.get("maxMinutesSpent", None),
#         "rating": enrich.get("rating", 0) or 0,
#         "numRatings": enrich.get("numRatings", 0) or 0,
#         "sources": item.get("sources", []),
#         "imageKeys": item.get("imageKeys", []),
#         "tripadvisorNumRatings": item.get("tripadvisorNumRatings", 0),
#         "openingPeriods": enrich.get("openingPeriods", []),
#         "generalDescription": general_desc,
#         "name": enrich.get("name") or item.get("name"),
#         "placeId": place_id,
#         "internationalPhoneNumber": enrich.get("internationalPhoneNumber"),
#         "reviews": enrich.get("reviews", []),
#         "ratingDistribution": item.get("ratingDistribution", {}),
#         "priceLevel": item.get("priceLevel", None) if item.get("priceLevel", None) is not None else enrich.get("priceLevel"),
#         "permanentlyClosed": bool(item.get("permanentlyClosed", False) or enrich.get("permanentlyClosed", False)),
#         "minMinutesSpent": item.get("minMinutesSpent", None),
#         "longitude": enrich.get("longitude"),
#         "address": enrich.get("address"),
#         "latitude": enrich.get("latitude"),
#         "g_image_urls": g_image_urls,
#         "travel_time": item.get("travel_time"),
#     }

# # -------------------------------- Main ------------------------------
# def main():
#     if not INPUT_JSON.exists():
#         print(f"❌ Input file not found: {INPUT_JSON}", file=sys.stderr); sys.exit(1)
#     if not DRY_RUN and not Path(SERVICE_ACCOUNT_JSON).exists():
#         print(f"❌ Service account JSON not found: {SERVICE_ACCOUNT_JSON}", file=sys.stderr); sys.exit(1)
#     if not requests:
#         print("❌ 'requests' is required. pip install requests", file=sys.stderr); sys.exit(1)

#     data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
#     if not isinstance(data, list):
#         print("❌ Input must be a JSON array of playlists.", file=sys.stderr); sys.exit(1)

#     # Firestore / Storage init
#     writer = FirestoreWriter(SERVICE_ACCOUNT_JSON, PROJECT_ID, COLLECTION, dry=bool(DRY_RUN))
#     bucket = storage.bucket(BUCKET_NAME) if (not DRY_RUN and firebase_admin) else None

#     # Photos
#     photo_uploader = PhotoUploader(GOOGLE_MAPS_API_KEY, bucket) if (GOOGLE_MAPS_API_KEY and not DRY_RUN) else None
#     print(f"Photo uploader enabled: {bool(photo_uploader)}")

#     # Google Maps client for Details/photos (if available)
#     gmaps_client = googlemaps.Client(key=GOOGLE_MAPS_API_KEY) if (googlemaps and GOOGLE_MAPS_API_KEY) else None
#     if not gmaps_client:
#         print("ℹ️ Google Maps SDK client not initialized (either no key or googlemaps not installed). Using REST fallback when needed.")

#     processed = 0

#     for raw in tqdm(data, desc="Uploading playlists"):
#         if LIMIT and processed >= LIMIT:
#             break

#         title = (raw.get("playlistTitle") or raw.get("title") or "Untitled")
#         city  = (raw.get("placeName") or raw.get("city") or "India")
#         slug = build_unique_slug(raw)
#         print(f"\n🎯 Processing: '{title}' with slug: '{slug}'")

#         doc_id, _, existed = writer.assign_doc_id_for_slug(slug)
#         list_id = int(doc_id)

#         # Base playlist doc (will override imageUrl after we create a cover)
#         playlist_doc = build_playlist_doc(
#             raw=raw, list_id=list_id, image_base=IMAGE_BASE, source=SOURCE,
#             category=CATEGORY, city_id_map=CITY_ID_MAP, slug=slug
#         )

#         # Optionally filter to publishable only
#         items = raw.get("items", [])
#         if FILTER_TO_PUBLISHABLE:
#             items = [it for it in items if it.get("resolution_status") == "publishable"]
#             print(f"  📋 Filtered to {len(items)} publishable items")

#         places_docs = []
#         details_cache = _load_details_cache()

#         for idx, item in enumerate(items, start=1):
#             place_id = item.get("place_id")
#             print(f"    🏢 Processing: {item.get('name', 'Unknown')} (ID: {place_id})")

#             # Step 2.5 base enrichment
#             offset_min = item.get("utc_offset_minutes", UTC_OFFSET_DEFAULT)
#             enrich = {
#                 "placeId": place_id,
#                 "name": item.get("name"),
#                 "latitude": item.get("lat"),
#                 "longitude": item.get("lng"),
#                 "types": item.get("types", []),
#                 "rating": item.get("rating", 0) or 0,
#                 "numRatings": item.get("reviews", 0) or 0,
#                 "website": item.get("website"),
#                 "address": item.get("address"),
#                 "internationalPhoneNumber": item.get("phone"),
#                 "openingPeriods": item.get("opening", []),
#                 "priceLevel": item.get("price_level"),
#                 "permanentlyClosed": item.get("permanently_closed", False),
#                 "reviews": item.get("reviews", []),
#                 "utcOffset": offset_min,
#                 "googleDescription": None,
#             }

#             # --- Details (cached) ---
#             det = details_cache.get(place_id)
#             if not det:
#                 det = fetch_place_details(place_id, gmaps_client, GOOGLE_MAPS_API_KEY)
#                 details_cache[place_id] = det or {}
#                 _save_details_cache(details_cache)

#             # Merge details into enrich (prefer Details when present)
#             if det:
#                 for k in [
#                     "name","latitude","longitude","types","rating","numRatings","website","address",
#                     "internationalPhoneNumber","openingPeriods","priceLevel","permanentlyClosed",
#                     "utcOffset","googleDescription","reviews"
#                 ]:
#                     v = det.get(k)
#                     if v not in (None, []):
#                         enrich[k] = v

#             # 1) Gather photo refs: prefer from Step 2.5; else use Details; else fetch via lightweight Details(photo) call
#             # 1) Gather photo refs
#             # Start with what we have from Step 2.5
#             photo_refs = (item.get("photo_refs") or [])
            
#             # If we have fewer than expected, try to supplement from Details Cache
#             if len(photo_refs) < G_IMAGE_COUNT and det:
#                 det_refs = (det.get("photo_refs") or [])
#                 # Add unique refs from Details that aren't already in the list
#                 for r in det_refs:
#                     if r not in photo_refs:
#                         photo_refs.append(r)
            
#             # If still empty/low, try fetching lightweight details via API
#             if len(photo_refs) < G_IMAGE_COUNT and gmaps_client and place_id:
#                 more_refs = fetch_photo_refs_for_place(place_id, gmaps_client, limit=10)
#                 if more_refs:
#                     for r in more_refs:
#                         if r not in photo_refs:
#                             photo_refs.append(r)
#                     print(f"    📷 Fetched more refs via API. Total: {len(photo_refs)}")
            
#             # Cap at 10 just to be safe
#             photo_refs = photo_refs[:10]
#             if not photo_refs and gmaps_client and place_id:
#                 more_refs = fetch_photo_refs_for_place(place_id, gmaps_client, limit=10)
#                 if more_refs:
#                     photo_refs = more_refs
#                     print(f"    📷 Fetched {len(photo_refs)} photo refs via Details(photo)")

#             # 2) Upload up to G_IMAGE_COUNT photos (or fallback static map)
#             uploaded_idxs: List[int] = []
#             if photo_uploader and place_id and photo_refs:
#                 uploaded_idxs = photo_uploader.upload_place_photos(
#                     list_id=list_id,
#                     place_id=place_id,
#                     refs=photo_refs,
#                     count=G_IMAGE_COUNT
#                 )
#             # Fallback if still no images uploaded but we have coordinates
#             if photo_uploader and STATICMAP_FALLBACK and not uploaded_idxs and place_id:
#                 static_idx = photo_uploader.upload_static_map(
#                     list_id=list_id,
#                     place_id=place_id,
#                     lat=enrich.get("latitude"),
#                     lng=enrich.get("longitude"),
#                 )
#                 if static_idx:
#                     uploaded_idxs = [static_idx]

#             g_image_urls = build_g_image_urls(G_IMAGE_TEMPLATE, list_id, place_id, uploaded_idxs)

#             # Normalize for Firestore
#             final_utc_offset = enrich.get("utcOffset", offset_min) or UTC_OFFSET_DEFAULT
#             place_doc = normalize_place_item(
#                 idx1_based=idx,
#                 item=item,
#                 enrich=enrich,
#                 utc_offset=final_utc_offset,
#                 list_id=list_id,
#                 g_image_urls=g_image_urls
#             )
#             places_docs.append(place_doc)

#         # Ensure playlist cover exists; override imageUrl if we created/found one
#         if bucket:
#             cover_url = ensure_playlist_cover_image(bucket, list_id, places_docs)
#             if cover_url:
#                 playlist_doc["imageUrl"] = cover_url
#                 print(f"  🖼️ Set cover image: {cover_url}")

#         writer.upsert_playlist_with_known_id(str(list_id), playlist_doc, places_docs)
#         processed += 1
#         print(f"→ {'Updated' if existed else 'Created'} '{playlist_doc['title']}' as ID {list_id}")

#     print(f"\n✅ Done. Processed {processed} playlist(s). DRY_RUN={DRY_RUN}")

# if __name__ == "__main__":
#     main()




# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# import json, os, re, sys, time, hashlib
# from pathlib import Path
# from typing import Dict, Any, List, Optional, Tuple

# # Optional: load .env
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except Exception:
#     pass

# # ------------------------------- CONFIG -------------------------------
# BASE_DIR = Path(__file__).resolve().parent

# # Input / Firestore
# INPUT_JSON = BASE_DIR / "playlist_items_resolved.json"   # <-- output of Step 2.5
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# PROJECT_ID = "mycasavsc"
# COLLECTION = "playlistsNew"
# BUCKET_NAME = "mycasavsc.appspot.com"  # Firebase Storage bucket

# # Presentation / metadata
# IMAGE_BASE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg"
# G_IMAGE_TEMPLATE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
# G_IMAGE_COUNT = 3
# SOURCE = "original"
# CATEGORY = "Travel"
# UTC_OFFSET_DEFAULT = 330   # minutes (fallback)

# # Behavior
# DRY_RUN = False
# LIMIT = 0
# FILTER_TO_PUBLISHABLE = True    # only upload items marked "publishable" by Step 2.5

# CITY_ID_MAP = {
#     "Bengaluru": "35",
# }

# # Google Places / Photos
# GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY") or "AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M"
# PHOTO_MAX_WIDTH = 1600
# PHOTO_SLEEP_SEC = 0.05
# GMAPS_LANGUAGE = "en-IN"

# # Caches
# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(parents=True, exist_ok=True)
# PHOTOS_META_CACHE = CACHE_DIR / "photos_meta_cache.json"
# DETAILS_CACHE = CACHE_DIR / "details_cache.json"

# # Optional Static Map fallback so we always have at least one image
# STATICMAP_FALLBACK = True
# STATICMAP_ZOOM = 12
# STATICMAP_SIZE = "1600x900"

# # -------------------------- Deps --------------------------------------
# try:
#     import firebase_admin
#     from firebase_admin import credentials, firestore, storage
# except Exception:
#     firebase_admin = None
#     credentials = None
#     firestore = None
#     storage = None

# try:
#     from google.cloud.firestore_v1 import FieldFilter
# except Exception:
#     FieldFilter = None

# try:
#     import requests
# except Exception:
#     requests = None

# try:
#     import googlemaps
# except Exception:
#     googlemaps = None

# try:
#     from tqdm import tqdm
# except Exception:
#     def tqdm(x, **kwargs): return x

# # ----------------------------- Helpers --------------------------------
# def ensure_playlist_cover_image(bucket, list_id: int, places_docs: List[Dict[str, Any]]) -> Optional[str]:
#     if not bucket:
#         return None
#     dst_key = f"playlistsNew_images/{list_id}/1.jpg"
#     dst_blob = bucket.blob(dst_key)
#     if dst_blob.exists():
#         return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"
#     for p in places_docs:
#         pid = p.get("placeId")
#         if not pid:
#             continue
#         src_key = f"playlistsPlaces/{list_id}/{pid}/1.jpg"
#         src_blob = bucket.blob(src_key)
#         if src_blob.exists():
#             bucket.copy_blob(src_blob, bucket, dst_key)
#             dst_blob = bucket.blob(dst_key)
#             dst_blob.cache_control = "public, max-age=31536000"
#             try:
#                 dst_blob.patch()
#             except Exception:
#                 pass
#             return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"
#     return None

# def md5_8(s: str) -> str:
#     import hashlib as _h
#     return _h.md5(s.encode("utf-8")).hexdigest()[:8]

# def slugify(text: str) -> str:
#     text = (text or "").strip().lower()
#     text = re.sub(r"[^\w\s-]+", "", text)
#     text = re.sub(r"[\s_-]+", "-", text)
#     text = re.sub(r"^-+|-+$", "", text)
#     return text or "untitled"

# def build_unique_slug(raw: Dict[str, Any]) -> str:
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"
#     subtype = str(raw.get("subtype", "destination")).strip().lower()
#     slug = f"{slugify(title)}-{slugify(city)}-{subtype}"
#     source_urls = raw.get("source_urls", [])
#     if source_urls:
#         urls_str = str(sorted(source_urls))
#         urls_hash = hashlib.md5(urls_str.encode()).hexdigest()[:6]
#         slug = f"{slug}-{urls_hash}"
#     return slug

# def default_description(title: str) -> str:
#     return (f'Dive into "{title}" — a handpicked list of places with quick notes, links, and essentials '
#             f'for fast trip planning and discovery.')

# # --------------- Local caches (photos + details) ----------------------
# def _load_json_cache(path: Path) -> dict:
#     if path.exists():
#         try:
#             return json.loads(path.read_text(encoding="utf-8"))
#         except Exception:
#             return {}
#     return {}

# def _save_json_cache(path: Path, data: dict) -> None:
#     try:
#         path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
#     except Exception:
#         pass

# # ----------------------- Photo Download + Upload ----------------------
# class PhotoUploader:
#     def __init__(self, api_key: str, bucket):
#         if not requests:
#             raise RuntimeError("requests not installed. pip install requests")
#         self.api_key = api_key
#         self.bucket = bucket

#     def _photo_url(self, ref: str, max_width: int) -> str:
#         return (
#             "https://maps.googleapis.com/maps/api/place/photo"
#             f"?maxwidth={max_width}&photo_reference={ref}&key={self.api_key}"
#         )

#     def upload_place_photos(self, list_id: int, place_id: str, refs: List[str], count: int) -> List[int]:
#         if not self.bucket or not place_id or not refs:
#             return []
#         written = []
#         for i, ref in enumerate(refs[:max(1, count)], start=1):
#             dest = f"playlistsPlaces/{list_id}/{place_id}/{i}.jpg"
#             blob = self.bucket.blob(dest)
#             try:
#                 resp = requests.get(self._photo_url(ref, PHOTO_MAX_WIDTH), timeout=30)
#                 resp.raise_for_status()
#                 blob.cache_control = "public, max-age=31536000"
#                 blob.upload_from_string(resp.content, content_type=resp.headers.get("Content-Type") or "image/jpeg")
#                 written.append(i)
#                 print(f"  📷 Uploaded photo {i} for {place_id}")
#                 time.sleep(PHOTO_SLEEP_SEC)
#             except Exception as e:
#                 print(f"⚠️ Photo upload failed: {dest} -> {e}")
#         return written

#     def upload_static_map(self, list_id: int, place_id: str, lat: Optional[float], lng: Optional[float],
#                           zoom: int = STATICMAP_ZOOM, size: str = STATICMAP_SIZE) -> Optional[int]:
#         if not self.bucket or lat is None or lng is None:
#             return None
#         dest = f"playlistsPlaces/{list_id}/{place_id}/1.jpg"
#         blob = self.bucket.blob(dest)
#         try:
#             url = (
#                 "https://maps.googleapis.com/maps/api/staticmap"
#                 f"?center={lat},{lng}&zoom={zoom}&size={size}&markers={lat},{lng}"
#                 f"&scale=2&maptype=roadmap&key={self.api_key}"
#             )
#             resp = requests.get(url, timeout=30)
#             resp.raise_for_status()
#             blob.cache_control = "public, max-age=31536000"
#             blob.upload_from_string(resp.content, content_type=resp.headers.get("Content-Type") or "image/png")
#             print(f"  🗺️  Uploaded static map as 1.jpg for {place_id}")
#             return 1
#         except Exception as e:
#             print(f"⚠️ Static map upload failed for {place_id}: {e}")
#             return None

# # --------- Place Details fetch (editorial, reviews, photos, etc.) -----
# def fetch_details_for_place(place_id: str, gmaps_client, limit_reviews: int = 10, limit_photos: int = 10) -> Dict[str, Any]:
#     """
#     Returns a compact dict with everything Step 3 needs:
#     {
#       'name','types','address','website','international_phone_number',
#       'opening_periods','price_level','permanently_closed',
#       'rating','user_ratings_total','utc_offset_minutes',
#       'googleDescription','reviews' (list), 'photo_refs' (list),
#       'lat','lng'
#     }
#     Cached by place_id in DETAILS_CACHE.
#     """
#     out_default = {
#         "name": None, "types": [], "address": None, "website": None,
#         "international_phone_number": None, "opening_periods": [],
#         "price_level": None, "permanently_closed": False,
#         "rating": None, "user_ratings_total": None, "utc_offset_minutes": None,
#         "googleDescription": None, "reviews": [], "photo_refs": [],
#         "lat": None, "lng": None,
#     }
#     if not place_id or not gmaps_client:
#         return out_default

#     cache = _load_json_cache(DETAILS_CACHE)
#     if place_id in cache:
#         d = cache[place_id]
#         # Trim oversized fields on reuse
#         d["reviews"] = (d.get("reviews") or [])[:limit_reviews]
#         d["photo_refs"] = (d.get("photo_refs") or [])[:limit_photos]
#         return d

#     try:
#         res = gmaps_client.place(
#             place_id=place_id,
#             fields=[
#                 "place_id","name","geometry/location","types","formatted_address",
#                 "website","international_phone_number","opening_hours/periods",
#                 "price_level","permanently_closed","rating","user_ratings_total",
#                 "photos","utc_offset_minutes","editorial_summary","reviews"
#             ],
#             language=GMAPS_LANGUAGE
#         )
#         result = res.get("result") or {}
#         loc = (result.get("geometry") or {}).get("location") or {}
#         # editorial summary
#         editorial = result.get("editorial_summary") or {}
#         google_desc = editorial.get("overview") or None

#         # normalize reviews (top N)
#         raw_reviews = (result.get("reviews") or [])[:limit_reviews]
#         reviews = []
#         for r in raw_reviews:
#             reviews.append({
#                 "author_name": r.get("author_name"),
#                 "rating": r.get("rating"),
#                 "relative_time_description": r.get("relative_time_description"),
#                 "text": r.get("text"),
#                 "time": r.get("time"),
#                 "language": r.get("language"),
#                 "profile_photo_url": r.get("profile_photo_url"),
#             })

#         # photos
#         photos = result.get("photos") or []
#         photo_refs = []
#         for p in photos[:limit_photos]:
#             ref = p.get("photo_reference") or p.get("photoReference")
#             if ref: photo_refs.append(ref)

#         out = {
#             "name": result.get("name"),
#             "types": result.get("types", []),
#             "address": result.get("formatted_address"),
#             "website": result.get("website"),
#             "international_phone_number": result.get("international_phone_number"),
#             "opening_periods": (result.get("opening_hours") or {}).get("periods", []),
#             "price_level": result.get("price_level"),
#             "permanently_closed": result.get("permanently_closed", False),
#             "rating": result.get("rating"),
#             "user_ratings_total": result.get("user_ratings_total"),
#             "utc_offset_minutes": result.get("utc_offset_minutes"),
#             "googleDescription": google_desc,
#             "reviews": reviews,
#             "photo_refs": photo_refs,
#             "lat": loc.get("lat"),
#             "lng": loc.get("lng"),
#         }

#         cache[place_id] = out
#         _save_json_cache(DETAILS_CACHE, cache)
#         time.sleep(PHOTO_SLEEP_SEC)
#         return out
#     except Exception as e:
#         print(f"⚠️ Details fetch failed for {place_id}: {e}")
#         cache[place_id] = out_default
#         _save_json_cache(DETAILS_CACHE, cache)
#         return out_default

# # ---------------------------- Firestore -------------------------------
# class FirestoreWriter:
#     def __init__(self, sa_path: str, project_id: Optional[str], collection: str, dry: bool = False):
#         self.collection = collection
#         self.dry = dry
#         self.db = None
#         self.col_ref = None
#         self.next_id = None

#         if not dry:
#             if not firebase_admin:
#                 raise RuntimeError("firebase-admin not installed. pip install firebase-admin")
#             if not firebase_admin._apps:
#                 cred = credentials.Certificate(sa_path)
#                 firebase_admin.initialize_app(cred, {
#                     "projectId": project_id,
#                     "storageBucket": BUCKET_NAME,
#                 })
#             self.db = firestore.client()
#             self.col_ref = self.db.collection(self.collection)
#             self.next_id = self._compute_start_id()
#         else:
#             self.next_id = 1

#     def _compute_start_id(self) -> int:
#         max_id = 0
#         try:
#             for doc in self.col_ref.select([]).stream():
#                 try:
#                     v = int(doc.id)
#                     if v > max_id:
#                         max_id = v
#                 except ValueError:
#                     continue
#         except Exception:
#             pass
#         return max_id + 1

#     def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, Optional[Any], bool]:
#         if self.dry:
#             return str(self.next_id), None, False
#         try:
#             if FieldFilter is not None:
#                 existing = list(self.col_ref.where(filter=FieldFilter("slug", "==", slug)).limit(1).stream())
#             else:
#                 existing = list(self.col_ref.where("slug", "==", slug).limit(1).stream())
#         except Exception:
#             existing = []
#         if existing:
#             ref = existing[0].reference
#             print(f"  🔄 Reusing existing document ID {ref.id} for slug: {slug}")
#             return ref.id, ref, True
#         new_id = str(self.next_id)
#         self.next_id += 1
#         ref = self.col_ref.document(new_id)
#         print(f"  ✨ Assigning new document ID {new_id} for slug: {slug}")
#         return new_id, ref, False

#     def upsert_playlist_with_known_id(self, doc_id: str, playlist_doc: Dict[str, Any], places: List[Dict[str, Any]]) -> str:
#         if self.dry:
#             print(f"[DRY-RUN] Would write '{playlist_doc['title']}' as doc {doc_id} with {len(places)} places")
#             return doc_id

#         doc_ref = self.col_ref.document(doc_id)
#         doc_ref.set(playlist_doc, merge=False)

#         sub = doc_ref.collection("places")
#         old = list(sub.stream())
#         for i in range(0, len(old), 200):
#             batch = self.db.batch()
#             for doc in old[i:i+200]:
#                 batch.delete(doc.reference)
#             batch.commit()

#         for i in range(0, len(places), 450):
#             batch = self.db.batch()
#             for item in places[i:i+450]:
#                 sub_id = item.get("placeId") or item.get("_id")
#                 batch.set(sub.document(sub_id), item)
#             batch.commit()

#         return doc_id

# # ------------------------- Builders / Mappers -------------------------
# def build_playlist_doc(raw: Dict[str, Any],
#                        list_id: int,
#                        image_base: str,
#                        source: str,
#                        category: str,
#                        city_id_map: Dict[str, str],
#                        slug: str) -> Dict[str, Any]:
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"
#     subtype = str(raw.get("subtype","destination")).strip().lower()
#     if subtype not in {"destination","poi"}:
#         subtype = "destination"

#     src_urls = raw.get("source_urls") or []
#     if isinstance(src_urls, str):
#         src_urls = [src_urls]
#     dedup, seen = [], set()
#     for u in src_urls:
#         if isinstance(u, str):
#             u2 = u.strip()
#             if u2 and u2 not in seen:
#                 seen.add(u2); dedup.append(u2)

#     return {
#         "list_id": str(list_id),
#         "title": title,
#         "description": raw.get("description") or default_description(title),
#         "imageUrl": image_base.format(list_id=list_id),
#         "source": source,
#         "category": category,
#         "city_id": city_id_map.get(city, city),
#         "city": city,
#         "slug": slug,
#         "subtype": subtype,
#         "source_urls": dedup,
#         "created_ts": int(time.time())
#     }

# def build_g_image_urls(g_image_template: str, list_id: int, place_id: Optional[str], uploaded_indices: List[int]) -> List[str]:
#     if not place_id or not uploaded_indices:
#         return []
#     return [g_image_template.format(list_id=list_id, placeId=place_id, n=n) for n in uploaded_indices]

# def normalize_place_item(idx1_based: int,
#                          item: Dict[str, Any],
#                          enrich: Dict[str, Any],
#                          utc_offset: int,
#                          list_id: int,
#                          g_image_urls: List[str]) -> Dict[str, Any]:
#     general_desc = item.get("generalDescription") or item.get("description") or None
#     description_val = enrich.get("googleDescription") or item.get("description") or None
#     place_id = enrich.get("placeId")

#     return {
#         # "_id": place_id or md5_8(f"{item.get('name','')}-{idx1_based}"),
#         "tripadvisorRating": item.get("tripadvisorRating", None),
#         "description": description_val,  # editorial summary if available
#         "website": enrich.get("website"),
#         "index": idx1_based,
#         "id": "",
#         "categories": enrich.get("types", []),
#         "utcOffset": utc_offset,
#         "maxMinutesSpent": item.get("maxMinutesSpent", None),
#         "rating": enrich.get("rating", 0) or 0,
#         "numRatings": enrich.get("numRatings", 0) or 0,
#         "sources": item.get("sources", []),
#         "imageKeys": item.get("imageKeys", []),
#         "tripadvisorNumRatings": item.get("tripadvisorNumRatings", 0),
#         "openingPeriods": enrich.get("openingPeriods", []),
#         "generalDescription": general_desc,
#         "name": enrich.get("name") or item.get("name"),
#         "placeId": place_id,
#         "internationalPhoneNumber": enrich.get("internationalPhoneNumber"),
#         "reviews": enrich.get("reviews", []),   # now an ARRAY of reviews
#         "ratingDistribution": item.get("ratingDistribution", {}),
#         "priceLevel": item.get("priceLevel", None) if item.get("priceLevel", None) is not None else enrich.get("priceLevel"),
#         "permanentlyClosed": bool(item.get("permanentlyClosed", False) or enrich.get("permanentlyClosed", False)),
#         "minMinutesSpent": item.get("minMinutesSpent", None),
#         "longitude": enrich.get("longitude"),
#         "address": enrich.get("address"),
#         "latitude": enrich.get("latitude"),
#         "g_image_urls": g_image_urls
#     }

# # -------------------------------- Main -------------------------------
# def main():
#     if not INPUT_JSON.exists():
#         print(f"❌ Input file not found: {INPUT_JSON}", file=sys.stderr); sys.exit(1)
#     if not DRY_RUN and not Path(SERVICE_ACCOUNT_JSON).exists():
#         print(f"❌ Service account JSON not found: {SERVICE_ACCOUNT_JSON}", file=sys.stderr); sys.exit(1)
#     if not requests:
#         print("❌ 'requests' is required. pip install requests", file=sys.stderr); sys.exit(1)

#     data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
#     if not isinstance(data, list):
#         print("❌ Input must be a JSON array of playlists.", file=sys.stderr); sys.exit(1)

#     # Firestore / Storage init
#     writer = FirestoreWriter(SERVICE_ACCOUNT_JSON, PROJECT_ID, COLLECTION, dry=bool(DRY_RUN))
#     bucket = storage.bucket(BUCKET_NAME) if not DRY_RUN else None

#     # Photos
#     photo_uploader = PhotoUploader(GOOGLE_MAPS_API_KEY, bucket) if (GOOGLE_MAPS_API_KEY and not DRY_RUN) else None
#     print(f"Photo uploader enabled: {bool(photo_uploader)}")

#     # Google Maps client
#     gmaps_client = googlemaps.Client(key=GOOGLE_MAPS_API_KEY) if (googlemaps and GOOGLE_MAPS_API_KEY and not DRY_RUN) else None
#     if not gmaps_client:
#         print("ℹ️ Google Maps client not initialized (no key or googlemaps not installed).")

#     processed = 0

#     for raw in tqdm(data, desc="Uploading playlists"):
#         if LIMIT and processed >= LIMIT:
#             break

#         title = (raw.get("playlistTitle") or raw.get("title") or "Untitled")
#         city  = (raw.get("placeName") or raw.get("city") or "India")
#         slug = build_unique_slug(raw)
#         print(f"\n🎯 Processing: '{title}' with slug: '{slug}'")

#         doc_id, _, existed = writer.assign_doc_id_for_slug(slug)
#         list_id = int(doc_id)

#         # Base playlist doc
#         playlist_doc = build_playlist_doc(
#             raw=raw, list_id=list_id, image_base=IMAGE_BASE, source=SOURCE,
#             category=CATEGORY, city_id_map=CITY_ID_MAP, slug=slug
#         )

#         # Optionally filter to publishable only
#         items = raw.get("items", [])
#         if FILTER_TO_PUBLISHABLE:
#             items = [it for it in items if it.get("resolution_status") == "publishable"]
#             print(f"  📋 Filtered to {len(items)} publishable items")

#         places_docs = []
#         for idx, item in enumerate(items, start=1):
#             place_id = item.get("place_id")
#             offset_min = item.get("utc_offset_minutes", UTC_OFFSET_DEFAULT)

#             print(f"    🏢 Processing: {item.get('name', 'Unknown')} (ID: {place_id})")

#             # --- A. Fetch Details (editorial, reviews, fresh metadata, photos) ---
#             details = fetch_details_for_place(place_id, gmaps_client, limit_reviews=10, limit_photos=10)

#             # --- B. Photo refs: prefer Step 2.5, else details ---
#             photo_refs = (item.get("photo_refs") or []) or details.get("photo_refs") or []
#             if not photo_refs and details.get("photo_refs"):
#                 print(f"    📷 Fetched {len(details['photo_refs'])} photo refs from Details")

#             # --- C. Build enrich dict (prefer Details, fallback to Step 2.5) ---
#             # 'reviews' from Step 2.5 is a COUNT; here we store array from Details
#             enrich = {
#                 "name": details.get("name") or item.get("name"),
#                 "placeId": place_id,
#                 "latitude": details.get("lat", item.get("lat")),
#                 "longitude": details.get("lng", item.get("lng")),
#                 "types": details.get("types") or item.get("types", []),
#                 "rating": (details.get("rating") if details.get("rating") is not None else item.get("rating", 0)) or 0,
#                 "numRatings": (details.get("user_ratings_total") if details.get("user_ratings_total") is not None else item.get("reviews", 0)) or 0,
#                 "website": details.get("website") or item.get("website"),
#                 "address": details.get("address") or item.get("address"),
#                 "internationalPhoneNumber": details.get("international_phone_number") or item.get("phone"),
#                 "openingPeriods": details.get("opening_periods") or item.get("opening", []),
#                 "priceLevel": details.get("price_level") if details.get("price_level") is not None else item.get("price_level"),
#                 "permanentlyClosed": bool(details.get("permanently_closed") or item.get("permanently_closed", False)),
#                 "reviews": details.get("reviews") or [],  # ARRAY of review dicts
#                 "utcOffset": details.get("utc_offset_minutes") if details.get("utc_offset_minutes") is not None else offset_min,
#                 "googleDescription": details.get("googleDescription"),  # editorial summary
#             }

#             # --- D. Upload up to G_IMAGE_COUNT photos (or fallback static map) ---
#             uploaded_idxs: List[int] = []
#             if photo_uploader and place_id and photo_refs:
#                 uploaded_idxs = photo_uploader.upload_place_photos(
#                     list_id=list_id, place_id=place_id, refs=photo_refs, count=G_IMAGE_COUNT
#                 )

#             if photo_uploader and STATICMAP_FALLBACK and not uploaded_idxs and place_id:
#                 static_idx = photo_uploader.upload_static_map(
#                     list_id=list_id, place_id=place_id, lat=enrich.get("latitude"), lng=enrich.get("longitude")
#                 )
#                 if static_idx:
#                     uploaded_idxs = [static_idx]

#             g_image_urls = build_g_image_urls(G_IMAGE_TEMPLATE, list_id, place_id, uploaded_idxs)

#             # --- E. Normalize to Firestore doc shape ---
#             place_doc = normalize_place_item(
#                 idx1_based=idx,
#                 item=item,
#                 enrich=enrich,
#                 utc_offset=enrich["utcOffset"],
#                 list_id=list_id,
#                 g_image_urls=g_image_urls
#             )
#             places_docs.append(place_doc)

#         # Cover image
#         if bucket:
#             cover_url = ensure_playlist_cover_image(bucket, list_id, places_docs)
#             if cover_url:
#                 playlist_doc["imageUrl"] = cover_url
#                 print(f"  🖼️ Set cover image: {cover_url}")

#         writer.upsert_playlist_with_known_id(str(list_id), playlist_doc, places_docs)
#         processed += 1
#         print(f"→ {'Updated' if existed else 'Created'} '{playlist_doc['title']}' as ID {list_id}")

#     print(f"\n✅ Done. Processed {processed} playlist(s). DRY_RUN={DRY_RUN}")

# if __name__ == "__main__":
#     main()



# import json, os, re, sys, time, hashlib
# from pathlib import Path
# from typing import Dict, Any, List, Optional, Tuple

# # Optional: load .env
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except Exception:
#     pass

# # ------------------------------- CONFIG -------------------------------
# BASE_DIR = Path(__file__).resolve().parent

# # Input / Firestore
# INPUT_JSON = BASE_DIR / "playlist_items_resolved.json"   # <-- resolved by Step 2.5
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# PROJECT_ID = "mycasavsc"
# COLLECTION = "playlistsNew"
# BUCKET_NAME = "mycasavsc.appspot.com"  # Firebase Storage bucket

# # Presentation / metadata
# IMAGE_BASE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg"
# G_IMAGE_TEMPLATE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
# G_IMAGE_COUNT = 3
# SOURCE = "original"
# CATEGORY = "Travel"
# UTC_OFFSET_DEFAULT = 330   # minutes (fallback)

# # Behavior
# DRY_RUN = False
# LIMIT = 0
# FILTER_TO_PUBLISHABLE = True    # only upload items marked "publishable" by Step 2.5

# CITY_ID_MAP = {
#     "Bengaluru": "35",
#     # "India": "86661",
# }

# # Google Places / Photos
# GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY") or "AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M"
# PHOTO_MAX_WIDTH = 1600
# PHOTO_SLEEP_SEC = 0.05
# GMAPS_LANGUAGE = "en-IN"

# # NEW: local cache to avoid repeated Details calls
# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(parents=True, exist_ok=True)
# PHOTOS_META_CACHE = CACHE_DIR / "photos_meta_cache.json"

# # Optional Static Map fallback so we always have at least one image
# STATICMAP_FALLBACK = True
# STATICMAP_ZOOM = 12
# STATICMAP_SIZE = "1600x900"

# # -------------------------- Deps --------------------------------------
# try:
#     import firebase_admin
#     from firebase_admin import credentials, firestore, storage
# except Exception:
#     firebase_admin = None
#     credentials = None
#     firestore = None
#     storage = None

# try:
#     from google.cloud.firestore_v1 import FieldFilter
# except Exception:
#     FieldFilter = None

# try:
#     import requests
# except Exception:
#     requests = None

# # NEW: Google Maps client (optional but recommended)
# try:
#     import googlemaps
# except Exception:
#     googlemaps = None

# try:
#     from tqdm import tqdm
# except Exception:
#     def tqdm(x, **kwargs): return x

# # ----------------------------- Helpers --------------------------------
# def ensure_playlist_cover_image(bucket, list_id: int, places_docs: List[Dict[str, Any]]) -> Optional[str]:
#     """
#     Ensures a cover image exists at playlistsNew_images/{list_id}/1.jpg.
#     Copies the first existing place photo (…/placeId/1.jpg) to that path.
#     Returns the public URL if created/already present, else None.
#     """
#     if not bucket:
#         return None

#     dst_key = f"playlistsNew_images/{list_id}/1.jpg"
#     dst_blob = bucket.blob(dst_key)
#     if dst_blob.exists():
#         return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

#     # Find first place with an uploaded image
#     for p in places_docs:
#         pid = p.get("placeId")
#         if not pid:
#             continue
#         src_key = f"playlistsPlaces/{list_id}/{pid}/1.jpg"
#         src_blob = bucket.blob(src_key)
#         if src_blob.exists():
#             bucket.copy_blob(src_blob, bucket, dst_key)
#             # optional: caching header
#             dst_blob = bucket.blob(dst_key)
#             dst_blob.cache_control = "public, max-age=31536000"
#             try:
#                 dst_blob.patch()
#             except Exception:
#                 pass
#             return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

#     return None

# def md5_8(s: str) -> str:
#     import hashlib as _h
#     return _h.md5(s.encode("utf-8")).hexdigest()[:8]

# def slugify(text: str) -> str:
#     text = (text or "").strip().lower()
#     text = re.sub(r"[^\w\s-]+", "", text)
#     text = re.sub(r"[\s_-]+", "-", text)
#     text = re.sub(r"^-+|-+$", "", text)
#     return text or "untitled"

# def build_unique_slug(raw: Dict[str, Any]) -> str:
#     """
#     Build a more unique slug by including title, city, subtype, and URL hash.
#     This reduces the chance of slug collisions.
#     """
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"
#     subtype = str(raw.get("subtype", "destination")).strip().lower()
#     slug = f"{slugify(title)}-{slugify(city)}-{subtype}"
#     source_urls = raw.get("source_urls", [])
#     if source_urls:
#         urls_str = str(sorted(source_urls))
#         urls_hash = hashlib.md5(urls_str.encode()).hexdigest()[:6]
#         slug = f"{slug}-{urls_hash}"
#     return slug

# def default_description(title: str) -> str:
#     return (f'Dive into "{title}" — a handpicked list of places with quick notes, links, and essentials '
#             f'for fast trip planning and discovery.')

# # ----------------------- Photo Download + Upload ----------------------
# class PhotoUploader:
#     def __init__(self, api_key: str, bucket):
#         if not requests:
#             raise RuntimeError("requests not installed. pip install requests")
#         self.api_key = api_key
#         self.bucket = bucket

#     def _photo_url(self, ref: str, max_width: int) -> str:
#         return (
#             "https://maps.googleapis.com/maps/api/place/photo"
#             f"?maxwidth={max_width}&photo_reference={ref}&key={self.api_key}"
#         )

#     def upload_place_photos(self, list_id: int, place_id: str, refs: List[str], count: int) -> List[int]:
#         """Return list of successful photo indices (e.g., [1,2,3])."""
#         if not self.bucket or not place_id or not refs:
#             return []
#         written = []
#         for i, ref in enumerate(refs[:max(1, count)], start=1):
#             dest = f"playlistsPlaces/{list_id}/{place_id}/{i}.jpg"
#             blob = self.bucket.blob(dest)
#             try:
#                 resp = requests.get(self._photo_url(ref, PHOTO_MAX_WIDTH), timeout=30)
#                 resp.raise_for_status()
#                 blob.cache_control = "public, max-age=31536000"
#                 blob.upload_from_string(resp.content, content_type=resp.headers.get("Content-Type") or "image/jpeg")
#                 written.append(i)
#                 print(f"  📷 Uploaded photo {i} for {place_id}")
#                 time.sleep(PHOTO_SLEEP_SEC)
#             except Exception as e:
#                 print(f"⚠️ Photo upload failed: {dest} -> {e}")
#         return written

#     # NEW: Static map fallback so we *always* have a hero image if coords exist
#     def upload_static_map(self, list_id: int, place_id: str, lat: Optional[float], lng: Optional[float],
#                           zoom: int = STATICMAP_ZOOM, size: str = STATICMAP_SIZE) -> Optional[int]:
#         if not self.bucket or lat is None or lng is None:
#             return None
#         dest = f"playlistsPlaces/{list_id}/{place_id}/1.jpg"
#         blob = self.bucket.blob(dest)
#         try:
#             url = (
#                 "https://maps.googleapis.com/maps/api/staticmap"
#                 f"?center={lat},{lng}&zoom={zoom}&size={size}&markers={lat},{lng}"
#                 f"&scale=2&maptype=roadmap&key={self.api_key}"
#             )
#             resp = requests.get(url, timeout=30)
#             resp.raise_for_status()
#             blob.cache_control = "public, max-age=31536000"
#             blob.upload_from_string(resp.content, content_type=resp.headers.get("Content-Type") or "image/png")
#             print(f"  🗺️  Uploaded static map as 1.jpg for {place_id}")
#             return 1
#         except Exception as e:
#             print(f"⚠️ Static map upload failed for {place_id}: {e}")
#             return None

# # NEW: minimal photo meta cache + resolver (Place Details → photos)
# def _load_photos_cache() -> Dict[str, List[str]]:
#     if PHOTOS_META_CACHE.exists():
#         try:
#             return json.loads(PHOTOS_META_CACHE.read_text(encoding="utf-8"))
#         except Exception:
#             return {}
#     return {}

# def _save_photos_cache(cache: Dict[str, List[str]]) -> None:
#     try:
#         PHOTOS_META_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
#     except Exception:
#         pass

# def fetch_photo_refs_for_place(place_id: str, gmaps_client, limit: int = 10) -> List[str]:
#     """
#     Ask Place Details for 'photos' and return up to `limit` photo_reference strings.
#     Uses a simple on-disk cache keyed by place_id.
#     """
#     if not place_id or not gmaps_client:
#         return []
#     _cache = _load_photos_cache()
#     if place_id in _cache:
#         return _cache[place_id][:limit]
#     try:
#         res = gmaps_client.place(
#             place_id=place_id,
#             fields=["photos"],
#             language=GMAPS_LANGUAGE,
#         )
#         result = res.get("result") or {}
#         refs: List[str] = []
#         for p in (result.get("photos") or [])[:limit]:
#             ref = p.get("photo_reference") or p.get("photoReference")
#             if ref:
#                 refs.append(ref)
#         _cache[place_id] = refs
#         _save_photos_cache(_cache)
#         time.sleep(PHOTO_SLEEP_SEC)
#         return refs
#     except Exception as e:
#         print(f"⚠️ Failed to fetch photos for {place_id}: {e}")
#         _cache[place_id] = []
#         _save_photos_cache(_cache)
#         return []

# # ---------------------------- Firestore ------------------------------
# class FirestoreWriter:
#     def __init__(self, sa_path: str, project_id: Optional[str], collection: str, dry: bool = False):
#         self.collection = collection
#         self.dry = dry
#         self.db = None
#         self.col_ref = None
#         self.next_id = None

#         if not dry:
#             if not firebase_admin:
#                 raise RuntimeError("firebase-admin not installed. pip install firebase-admin")
#             if not firebase_admin._apps:
#                 cred = credentials.Certificate(sa_path)
#                 firebase_admin.initialize_app(cred, {
#                     "projectId": project_id,
#                     "storageBucket": BUCKET_NAME,
#                 })
#             self.db = firestore.client()
#             self.col_ref = self.db.collection(self.collection)
#             self.next_id = self._compute_start_id()
#         else:
#             self.next_id = 1

#     def _compute_start_id(self) -> int:
#         max_id = 0
#         try:
#             for doc in self.col_ref.select([]).stream():
#                 try:
#                     v = int(doc.id)
#                     if v > max_id:
#                         max_id = v
#                 except ValueError:
#                     continue
#         except Exception:
#             pass
#         return max_id + 1

#     def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, Optional[Any], bool]:
#         if self.dry:
#             return str(self.next_id), None, False
#         try:
#             if FieldFilter is not None:
#                 existing = list(self.col_ref.where(filter=FieldFilter("slug", "==", slug)).limit(1).stream())
#             else:
#                 existing = list(self.col_ref.where("slug", "==", slug).limit(1).stream())
#         except Exception:
#             existing = []
#         if existing:
#             ref = existing[0].reference
#             print(f"  🔄 Reusing existing document ID {ref.id} for slug: {slug}")
#             return ref.id, ref, True
#         new_id = str(self.next_id)
#         self.next_id += 1
#         ref = self.col_ref.document(new_id)
#         print(f"  ✨ Assigning new document ID {new_id} for slug: {slug}")
#         return new_id, ref, False

#     def upsert_playlist_with_known_id(self, doc_id: str, playlist_doc: Dict[str, Any], places: List[Dict[str, Any]]) -> str:
#         if self.dry:
#             print(f"[DRY-RUN] Would write '{playlist_doc['title']}' as doc {doc_id} with {len(places)} places")
#             return doc_id

#         doc_ref = self.col_ref.document(doc_id)
#         doc_ref.set(playlist_doc, merge=False)

#         sub = doc_ref.collection("places")
#         old = list(sub.stream())
#         for i in range(0, len(old), 200):
#             batch = self.db.batch()
#             for doc in old[i:i+200]:
#                 batch.delete(doc.reference)
#             batch.commit()

#         for i in range(0, len(places), 450):
#             batch = self.db.batch()
#             for item in places[i:i+450]:
#                 sub_id = item.get("placeId") or item.get("_id")
#                 batch.set(sub.document(sub_id), item)
#             batch.commit()

#         return doc_id

# # ------------------------- Builders / Mappers -----------------------
# def build_playlist_doc(raw: Dict[str, Any],
#                        list_id: int,
#                        image_base: str,
#                        source: str,
#                        category: str,
#                        city_id_map: Dict[str, str],
#                        slug: str) -> Dict[str, Any]:
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"
#     subtype = str(raw.get("subtype","destination")).strip().lower()
#     if subtype not in {"destination","poi"}:
#         subtype = "destination"

#     src_urls = raw.get("source_urls") or []
#     if isinstance(src_urls, str):
#         src_urls = [src_urls]
#     dedup, seen = [], set()
#     for u in src_urls:
#         if isinstance(u, str):
#             u2 = u.strip()
#             if u2 and u2 not in seen:
#                 seen.add(u2); dedup.append(u2)

#     return {
#         "list_id": str(list_id),
#         "title": title,
#         "description": raw.get("description") or default_description(title),
#         "imageUrl": image_base.format(list_id=list_id),
#         "source": source,
#         "category": category,
#         "city_id": city_id_map.get(city, city),
#         "city": city,
#         "slug": slug,
#         "subtype": subtype,
#         "source_urls": dedup,
#         "created_ts": int(time.time())
#     }

# def build_g_image_urls(g_image_template: str, list_id: int, place_id: Optional[str], uploaded_indices: List[int]) -> List[str]:
#     """
#     Build g_image_urls only for successfully uploaded photos.
#     """
#     if not place_id or not uploaded_indices:
#         return []
#     return [g_image_template.format(list_id=list_id, placeId=place_id, n=n) for n in uploaded_indices]

# def normalize_place_item(idx1_based: int,
#                          item: Dict[str, Any],
#                          enrich: Dict[str, Any],
#                          utc_offset: int,
#                          list_id: int,
#                          g_image_urls: List[str]) -> Dict[str, Any]:
#     general_desc = item.get("generalDescription") or item.get("description") or None
#     description_val = enrich.get("googleDescription") or item.get("description") or None
#     place_id = enrich.get("placeId")

#     return {
#         # "_id": place_id or md5_8(f"{item.get('name','')}-{idx1_based}"),
#         "tripadvisorRating": item.get("tripadvisorRating", 0),
#         "description": description_val,
#         "website": enrich.get("website"),
#         "index": idx1_based,
#         "id": "",
#         "categories": enrich.get("types", []),
#         "utcOffset": utc_offset,
#         "maxMinutesSpent": item.get("maxMinutesSpent", None),
#         "rating": enrich.get("rating", 0) or 0,
#         "numRatings": enrich.get("numRatings", 0) or 0,
#         "sources": item.get("sources", []),
#         "imageKeys": item.get("imageKeys", []),
#         "tripadvisorNumRatings": item.get("tripadvisorNumRatings", 0),
#         "openingPeriods": enrich.get("openingPeriods", []),
#         "generalDescription": general_desc,
#         "name": enrich.get("name") or item.get("name"),
#         "placeId": place_id,
#         "internationalPhoneNumber": enrich.get("internationalPhoneNumber"),
#         "reviews": enrich.get("reviews", []),
#         "ratingDistribution": item.get("ratingDistribution", {}),
#         "priceLevel": item.get("priceLevel", None) if item.get("priceLevel", None) is not None else enrich.get("priceLevel"),
#         "permanentlyClosed": bool(item.get("permanentlyClosed", False) or enrich.get("permanentlyClosed", False)),
#         "minMinutesSpent": item.get("minMinutesSpent", None),
#         "longitude": enrich.get("longitude"),
#         "address": enrich.get("address"),
#         "latitude": enrich.get("latitude"),
#         "g_image_urls": g_image_urls
#     }

# # -------------------------------- Main ------------------------------
# def main():
#     if not INPUT_JSON.exists():
#         print(f"❌ Input file not found: {INPUT_JSON}", file=sys.stderr); sys.exit(1)
#     if not DRY_RUN and not Path(SERVICE_ACCOUNT_JSON).exists():
#         print(f"❌ Service account JSON not found: {SERVICE_ACCOUNT_JSON}", file=sys.stderr); sys.exit(1)
#     if not requests:
#         print("❌ 'requests' is required. pip install requests", file=sys.stderr); sys.exit(1)

#     data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
#     if not isinstance(data, list):
#         print("❌ Input must be a JSON array of playlists.", file=sys.stderr); sys.exit(1)

#     # Firestore / Storage init
#     writer = FirestoreWriter(SERVICE_ACCOUNT_JSON, PROJECT_ID, COLLECTION, dry=bool(DRY_RUN))
#     bucket = storage.bucket(BUCKET_NAME) if not DRY_RUN else None

#     # Photos
#     photo_uploader = PhotoUploader(GOOGLE_MAPS_API_KEY, bucket) if (GOOGLE_MAPS_API_KEY and not DRY_RUN) else None
#     print(f"Photo uploader enabled: {bool(photo_uploader)}")

#     # NEW: Google Maps client for fetching photo refs when Step 2.5 had none
#     gmaps_client = googlemaps.Client(key=GOOGLE_MAPS_API_KEY) if (googlemaps and GOOGLE_MAPS_API_KEY and not DRY_RUN) else None
#     if not gmaps_client:
#         print("ℹ️ Google Maps client not initialized (either no key or googlemaps not installed).")

#     processed = 0

#     for raw in tqdm(data, desc="Uploading playlists"):
#         if LIMIT and processed >= LIMIT:
#             break

#         title = (raw.get("playlistTitle") or raw.get("title") or "Untitled")
#         city  = (raw.get("placeName") or raw.get("city") or "India")
#         slug = build_unique_slug(raw)
#         print(f"\n🎯 Processing: '{title}' with slug: '{slug}'")

#         doc_id, _, existed = writer.assign_doc_id_for_slug(slug)
#         list_id = int(doc_id)

#         # Base playlist doc (will override imageUrl after we create a cover)
#         playlist_doc = build_playlist_doc(
#             raw=raw, list_id=list_id, image_base=IMAGE_BASE, source=SOURCE,
#             category=CATEGORY, city_id_map=CITY_ID_MAP, slug=slug
#         )

#         # Optionally filter to publishable only
#         items = raw.get("items", [])
#         if FILTER_TO_PUBLISHABLE:
#             items = [it for it in items if it.get("resolution_status") == "publishable"]
#             print(f"  📋 Filtered to {len(items)} publishable items")

#         places_docs = []
#         for idx, item in enumerate(items, start=1):
#             place_id = item.get("place_id")
#             offset_min = item.get("utc_offset_minutes", UTC_OFFSET_DEFAULT)

#             print(f"    🏢 Processing: {item.get('name', 'Unknown')} (ID: {place_id})")

#             # 1) Gather photo refs: prefer from Step 2.5; else fetch via Place Details now
#             photo_refs = item.get("photo_refs", []) or []
#             if not photo_refs and gmaps_client and place_id:
#                 photo_refs = fetch_photo_refs_for_place(place_id, gmaps_client, limit=10)
#                 if photo_refs:
#                     print(f"    📷 Fetched {len(photo_refs)} photo refs via Place Details")

#             enrich = {
#                 "name": item.get("name"),
#                 "placeId": place_id,
#                 "latitude": item.get("lat"),
#                 "longitude": item.get("lng"),
#                 "types": item.get("types", []),
#                 "rating": item.get("rating", 0) or 0,
#                 "numRatings": item.get("reviews", 0) or 0,
#                 "website": item.get("website"),
#                 "address": item.get("address"),
#                 "internationalPhoneNumber": item.get("phone"),
#                 "openingPeriods": item.get("opening", []),
#                 "priceLevel": item.get("price_level"),
#                 "permanentlyClosed": item.get("permanently_closed", False),
#                 "reviews": item.get("reviews", []),
#                 "utcOffset": offset_min,
#                 "googleDescription": None,
#             }

#             # 2) Upload up to G_IMAGE_COUNT photos (or fallback static map)
#             uploaded_idxs: List[int] = []
#             if photo_uploader and place_id and photo_refs:
#                 uploaded_idxs = photo_uploader.upload_place_photos(
#                     list_id=list_id,
#                     place_id=place_id,
#                     refs=photo_refs,
#                     count=G_IMAGE_COUNT
#                 )

#             # Fallback if still no images uploaded but we have coordinates
#             if photo_uploader and STATICMAP_FALLBACK and not uploaded_idxs and place_id:
#                 static_idx = photo_uploader.upload_static_map(
#                     list_id=list_id,
#                     place_id=place_id,
#                     lat=item.get("lat"),
#                     lng=item.get("lng"),
#                 )
#                 if static_idx:
#                     uploaded_idxs = [static_idx]

#             g_image_urls = build_g_image_urls(G_IMAGE_TEMPLATE, list_id, place_id, uploaded_idxs)

#             place_doc = normalize_place_item(
#                 idx1_based=idx,
#                 item=item,
#                 enrich=enrich,
#                 utc_offset=offset_min,
#                 list_id=list_id,
#                 g_image_urls=g_image_urls
#             )
#             places_docs.append(place_doc)

#         # Ensure playlist cover exists; override imageUrl if we created/found one
#         if bucket:
#             cover_url = ensure_playlist_cover_image(bucket, list_id, places_docs)
#             if cover_url:
#                 playlist_doc["imageUrl"] = cover_url
#                 print(f"  🖼️ Set cover image: {cover_url}")

#         writer.upsert_playlist_with_known_id(str(list_id), playlist_doc, places_docs)
#         processed += 1
#         print(f"→ {'Updated' if existed else 'Created'} '{playlist_doc['title']}' as ID {list_id}")

#     print(f"\n✅ Done. Processed {processed} playlist(s). DRY_RUN={DRY_RUN}")

# if __name__ == "__main__":
#     main()



# import json, os, re, sys, time, hashlib
# from pathlib import Path
# from typing import Dict, Any, List, Optional, Tuple

# # Optional: load .env
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except Exception:
#     pass

# # ------------------------------- CONFIG -------------------------------
# BASE_DIR = Path(__file__).resolve().parent

# # Input / Firestore
# INPUT_JSON = BASE_DIR / "playlist_items_resolved.json"   # <-- resolved by Step 2.5
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# PROJECT_ID = "mycasavsc"
# COLLECTION = "playlistsNew"
# BUCKET_NAME = "mycasavsc.appspot.com"  # Firebase Storage bucket

# # Presentation / metadata
# IMAGE_BASE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg"
# G_IMAGE_TEMPLATE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
# G_IMAGE_COUNT = 3
# SOURCE = "original"
# CATEGORY = "Travel"
# UTC_OFFSET_DEFAULT = 330   # minutes (fallback)

# # Behavior
# DRY_RUN = False
# LIMIT = 0
# FILTER_TO_PUBLISHABLE = True    # only upload items marked "publishable" by Step 2.5

# CITY_ID_MAP = {
#     "Bengaluru": "35",
#     # "India": "86661",
# }

# # Google Photos API (for Place photos only; no Place Search here)
# GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY") or "AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M"
# PHOTO_MAX_WIDTH = 1600
# PHOTO_SLEEP_SEC = 0.05

# # -------------------------- Deps --------------------------------------
# try:
#     import firebase_admin
#     from firebase_admin import credentials, firestore, storage
# except Exception:
#     firebase_admin = None
#     credentials = None
#     firestore = None
#     storage = None

# try:
#     from google.cloud.firestore_v1 import FieldFilter
# except Exception:
#     FieldFilter = None

# try:
#     import requests
# except Exception:
#     requests = None

# try:
#     from tqdm import tqdm
# except Exception:
#     def tqdm(x, **kwargs): return x

# # ----------------------------- Helpers --------------------------------
# def ensure_playlist_cover_image(bucket, list_id: int, places_docs: List[Dict[str, Any]]) -> Optional[str]:
#     """
#     Ensures a cover image exists at playlistsNew_images/{list_id}/1.jpg.
#     Copies the first existing place photo (…/placeId/1.jpg) to that path.
#     Returns the public URL if created/already present, else None.
#     """
#     if not bucket:
#         return None

#     dst_key = f"playlistsNew_images/{list_id}/1.jpg"
#     dst_blob = bucket.blob(dst_key)
#     if dst_blob.exists():
#         return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

#     # Find first place with an uploaded image
#     for p in places_docs:
#         pid = p.get("placeId")
#         if not pid:
#             continue
#         src_key = f"playlistsPlaces/{list_id}/{pid}/1.jpg"
#         src_blob = bucket.blob(src_key)
#         if src_blob.exists():
#             bucket.copy_blob(src_blob, bucket, dst_key)
#             # optional: caching header
#             dst_blob = bucket.blob(dst_key)
#             dst_blob.cache_control = "public, max-age=31536000"
#             try:
#                 dst_blob.patch()
#             except Exception:
#                 pass
#             return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

#     return None

# def md5_8(s: str) -> str:
#     import hashlib as _h
#     return _h.md5(s.encode("utf-8")).hexdigest()[:8]

# def slugify(text: str) -> str:
#     text = (text or "").strip().lower()
#     text = re.sub(r"[^\w\s-]+", "", text)
#     text = re.sub(r"[\s_-]+", "-", text)
#     text = re.sub(r"^-+|-+$", "", text)
#     return text or "untitled"

# def build_unique_slug(raw: Dict[str, Any]) -> str:
#     """
#     Build a more unique slug by including title, city, subtype, and URL hash.
#     This reduces the chance of slug collisions.
#     """
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"
#     subtype = str(raw.get("subtype", "destination")).strip().lower()
    
#     # Base slug with subtype for better uniqueness
#     slug = f"{slugify(title)}-{slugify(city)}-{subtype}"
    
#     # Add hash of source URLs for extra uniqueness
#     source_urls = raw.get("source_urls", [])
#     if source_urls:
#         # Create consistent hash from sorted URLs
#         urls_str = str(sorted(source_urls))
#         urls_hash = hashlib.md5(urls_str.encode()).hexdigest()[:6]
#         slug = f"{slug}-{urls_hash}"
    
#     return slug

# def default_description(title: str) -> str:
#     return (f'Dive into "{title}" — a handpicked list of places with quick notes, links, and essentials '
#             f'for fast trip planning and discovery.')

# # ----------------------- Photo Download + Upload ----------------------
# class PhotoUploader:
#     def __init__(self, api_key: str, bucket):
#         if not requests:
#             raise RuntimeError("requests not installed. pip install requests")
#         self.api_key = api_key
#         self.bucket = bucket

#     def _photo_url(self, ref: str, max_width: int) -> str:
#         return (
#             "https://maps.googleapis.com/maps/api/place/photo"
#             f"?maxwidth={max_width}&photo_reference={ref}&key={self.api_key}"
#         )

#     def upload_place_photos(self, list_id: int, place_id: str, refs: List[str], count: int) -> List[int]:
#         """Return list of successful photo indices (e.g., [1,2,3])."""
#         if not self.bucket or not place_id or not refs:
#             return []
#         written = []
#         for i, ref in enumerate(refs[:max(1, count)], start=1):
#             dest = f"playlistsPlaces/{list_id}/{place_id}/{i}.jpg"
#             blob = self.bucket.blob(dest)
#             try:
#                 resp = requests.get(self._photo_url(ref, PHOTO_MAX_WIDTH), timeout=30)
#                 resp.raise_for_status()
#                 blob.cache_control = "public, max-age=31536000"
#                 blob.upload_from_string(resp.content, content_type=resp.headers.get("Content-Type") or "image/jpeg")
#                 written.append(i)
#                 print(f"  📷 Uploaded photo {i} for {place_id}")
#                 time.sleep(PHOTO_SLEEP_SEC)
#             except Exception as e:
#                 print(f"⚠️ Photo upload failed: {dest} -> {e}")
#         return written

# # ---------------------------- Firestore ------------------------------
# class FirestoreWriter:
#     def __init__(self, sa_path: str, project_id: Optional[str], collection: str, dry: bool = False):
#         self.collection = collection
#         self.dry = dry
#         self.db = None
#         self.col_ref = None
#         self.next_id = None

#         if not dry:
#             if not firebase_admin:
#                 raise RuntimeError("firebase-admin not installed. pip install firebase-admin")
#             if not firebase_admin._apps:
#                 cred = credentials.Certificate(sa_path)
#                 firebase_admin.initialize_app(cred, {
#                     "projectId": project_id,
#                     "storageBucket": BUCKET_NAME,
#                 })
#             self.db = firestore.client()
#             self.col_ref = self.db.collection(self.collection)
#             self.next_id = self._compute_start_id()
#         else:
#             self.next_id = 1

#     def _compute_start_id(self) -> int:
#         max_id = 0
#         try:
#             for doc in self.col_ref.select([]).stream():
#                 try:
#                     v = int(doc.id)
#                     if v > max_id:
#                         max_id = v
#                 except ValueError:
#                     continue
#         except Exception:
#             pass
#         return max_id + 1

#     def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, Optional[Any], bool]:
#         if self.dry:
#             return str(self.next_id), None, False
#         try:
#             if FieldFilter is not None:
#                 existing = list(self.col_ref.where(filter=FieldFilter("slug", "==", slug)).limit(1).stream())
#             else:
#                 existing = list(self.col_ref.where("slug", "==", slug).limit(1).stream())
#         except Exception:
#             existing = []
#         if existing:
#             ref = existing[0].reference
#             print(f"  🔄 Reusing existing document ID {ref.id} for slug: {slug}")
#             return ref.id, ref, True
#         new_id = str(self.next_id)
#         self.next_id += 1
#         ref = self.col_ref.document(new_id)
#         print(f"  ✨ Assigning new document ID {new_id} for slug: {slug}")
#         return new_id, ref, False

#     def upsert_playlist_with_known_id(self, doc_id: str, playlist_doc: Dict[str, Any], places: List[Dict[str, Any]]) -> str:
#         if self.dry:
#             print(f"[DRY-RUN] Would write '{playlist_doc['title']}' as doc {doc_id} with {len(places)} places")
#             return doc_id

#         doc_ref = self.col_ref.document(doc_id)
#         doc_ref.set(playlist_doc, merge=False)

#         sub = doc_ref.collection("places")
#         old = list(sub.stream())
#         for i in range(0, len(old), 200):
#             batch = self.db.batch()
#             for doc in old[i:i+200]:
#                 batch.delete(doc.reference)
#             batch.commit()

#         for i in range(0, len(places), 450):
#             batch = self.db.batch()
#             for item in places[i:i+450]:
#                 sub_id = item.get("placeId") or item.get("_id")
#                 batch.set(sub.document(sub_id), item)
#             batch.commit()

#         return doc_id

# # ------------------------- Builders / Mappers -----------------------
# def build_playlist_doc(raw: Dict[str, Any],
#                        list_id: int,
#                        image_base: str,
#                        source: str,
#                        category: str,
#                        city_id_map: Dict[str, str],
#                        slug: str) -> Dict[str, Any]:
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"
#     subtype = str(raw.get("subtype","destination")).strip().lower()
#     if subtype not in {"destination","poi"}:
#         subtype = "destination"

#     src_urls = raw.get("source_urls") or []
#     if isinstance(src_urls, str):
#         src_urls = [src_urls]
#     dedup, seen = [], set()
#     for u in src_urls:
#         if isinstance(u, str):
#             u2 = u.strip()
#             if u2 and u2 not in seen:
#                 seen.add(u2); dedup.append(u2)

#     return {
#         "list_id": str(list_id),
#         "title": title,
#         "description": raw.get("description") or default_description(title),
#         "imageUrl": image_base.format(list_id=list_id),
#         "source": source,
#         "category": category,
#         "city_id": city_id_map.get(city, city),
#         "city": city,
#         "slug": slug,
#         "subtype": subtype,
#         "source_urls": dedup,
#         "created_ts": int(time.time())
#     }

# def build_g_image_urls(g_image_template: str, list_id: int, place_id: Optional[str], uploaded_indices: List[int]) -> List[str]:
#     """
#     Build g_image_urls only for successfully uploaded photos.
#     """
#     if not place_id or not uploaded_indices:
#         return []
#     return [g_image_template.format(list_id=list_id, placeId=place_id, n=n) for n in uploaded_indices]

# def normalize_place_item(idx1_based: int,
#                          item: Dict[str, Any],
#                          enrich: Dict[str, Any],
#                          utc_offset: int,
#                          list_id: int,
#                          g_image_urls: List[str]) -> Dict[str, Any]:
#     # Prefer Google editorial if present, else Step-2 description
#     general_desc = item.get("generalDescription") or item.get("description") or None
#     description_val = enrich.get("googleDescription") or item.get("description") or None
#     place_id = enrich.get("placeId")

#     return {
#         "_id": place_id or md5_8(f"{item.get('name','')}-{idx1_based}"),
#         "tripadvisorRating": item.get("tripadvisorRating", 0),
#         "description": description_val,
#         "website": enrich.get("website"),
#         "index": idx1_based,
#         "id": "",  # keep as-is for your schema
#         "categories": enrich.get("types", []),
#         "utcOffset": utc_offset,
#         "maxMinutesSpent": item.get("maxMinutesSpent", None),
#         "rating": enrich.get("rating", 0) or 0,
#         "numRatings": enrich.get("numRatings", 0) or 0,
#         "sources": item.get("sources", []),
#         "imageKeys": item.get("imageKeys", []),
#         "tripadvisorNumRatings": item.get("tripadvisorNumRatings", 0),
#         "openingPeriods": enrich.get("openingPeriods", []),
#         "generalDescription": general_desc,
#         "name": enrich.get("name") or item.get("name"),
#         "placeId": place_id,
#         "internationalPhoneNumber": enrich.get("internationalPhoneNumber"),
#         "reviews": enrich.get("reviews", []),
#         "ratingDistribution": item.get("ratingDistribution", {}),
#         "priceLevel": item.get("priceLevel", None) if item.get("priceLevel", None) is not None else enrich.get("priceLevel"),
#         "permanentlyClosed": bool(item.get("permanentlyClosed", False) or enrich.get("permanentlyClosed", False)),
#         "minMinutesSpent": item.get("minMinutesSpent", None),
#         "longitude": enrich.get("longitude"),
#         "address": enrich.get("address"),
#         "latitude": enrich.get("latitude"),
#         "g_image_urls": g_image_urls
#     }

# # -------------------------------- Main ------------------------------
# def main():
#     if not INPUT_JSON.exists():
#         print(f"❌ Input file not found: {INPUT_JSON}", file=sys.stderr); sys.exit(1)
#     if not DRY_RUN and not Path(SERVICE_ACCOUNT_JSON).exists():
#         print(f"❌ Service account JSON not found: {SERVICE_ACCOUNT_JSON}", file=sys.stderr); sys.exit(1)
#     if not requests:
#         print("❌ 'requests' is required. pip install requests", file=sys.stderr); sys.exit(1)

#     data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
#     if not isinstance(data, list):
#         print("❌ Input must be a JSON array of playlists.", file=sys.stderr); sys.exit(1)

#     # Firestore / Storage init
#     writer = FirestoreWriter(SERVICE_ACCOUNT_JSON, PROJECT_ID, COLLECTION, dry=bool(DRY_RUN))
#     bucket = storage.bucket(BUCKET_NAME) if not DRY_RUN else None

#     # Photos (Place Photos API only)
#     photo_uploader = PhotoUploader(GOOGLE_MAPS_API_KEY, bucket) if (GOOGLE_MAPS_API_KEY and not DRY_RUN) else None
#     print(f"Photo uploader enabled: {bool(photo_uploader)}")

#     processed = 0

#     for raw in tqdm(data, desc="Uploading playlists"):
#         if LIMIT and processed >= LIMIT:
#             break

#         title = (raw.get("playlistTitle") or raw.get("title") or "Untitled")
#         city  = (raw.get("placeName") or raw.get("city") or "India")
        
#         # Use improved slug generation
#         slug = build_unique_slug(raw)
#         print(f"\n🎯 Processing: '{title}' with slug: '{slug}'")

#         doc_id, _, existed = writer.assign_doc_id_for_slug(slug)
#         list_id = int(doc_id)

#         # Base playlist doc (will override imageUrl after we create a cover)
#         playlist_doc = build_playlist_doc(
#             raw=raw, list_id=list_id, image_base=IMAGE_BASE, source=SOURCE,
#             category=CATEGORY, city_id_map=CITY_ID_MAP, slug=slug
#         )

#         # Optionally filter to publishable only
#         items = raw.get("items", [])
#         if FILTER_TO_PUBLISHABLE:
#             items = [it for it in items if it.get("resolution_status") == "publishable"]
#             print(f"  📋 Filtered to {len(items)} publishable items")

#         places_docs = []
#         for idx, item in enumerate(items, start=1):
#             # Map resolved data from Step 2.5 → enrich dict expected by normalizer
#             place_id = item.get("place_id")
#             photo_refs = item.get("photo_refs", [])  # From resolver Step 2.5
#             offset_min = item.get("utc_offset_minutes", UTC_OFFSET_DEFAULT)

#             print(f"    🏢 Processing: {item.get('name', 'Unknown')} (ID: {place_id})")
#             if photo_refs:
#                 print(f"    📷 Found {len(photo_refs)} photo references")

#             enrich = {
#                 "name": item.get("name"),
#                 "placeId": place_id,
#                 "latitude": item.get("lat"),
#                 "longitude": item.get("lng"),
#                 "types": item.get("types", []),
#                 "rating": item.get("rating", 0) or 0,
#                 "numRatings": item.get("reviews", 0) or 0,
#                 "website": item.get("website"),
#                 "address": item.get("address"),
#                 "internationalPhoneNumber": item.get("phone"),
#                 "openingPeriods": item.get("opening", []),
#                 "priceLevel": item.get("price_level"),
#                 "permanentlyClosed": item.get("permanently_closed", False),
#                 "reviews": [],  # map item reviews here if you stored them in Step 2.5
#                 "utcOffset": offset_min,
#                 "googleDescription": None,  # not set by 2.5 currently
#             }

#             # Upload place photos using resolved photo_refs from Step 2.5
#             uploaded_idxs = []
#             if photo_uploader and place_id and photo_refs:
#                 uploaded_idxs = photo_uploader.upload_place_photos(
#                     list_id=list_id,
#                     place_id=place_id,
#                     refs=photo_refs,
#                     count=G_IMAGE_COUNT
#                 )

#             # Build g_image_urls only for successfully uploaded photos
#             g_image_urls = build_g_image_urls(G_IMAGE_TEMPLATE, list_id, place_id, uploaded_idxs)

#             place_doc = normalize_place_item(
#                 idx1_based=idx,
#                 item=item,
#                 enrich=enrich,
#                 utc_offset=offset_min,
#                 list_id=list_id,
#                 g_image_urls=g_image_urls
#             )
#             places_docs.append(place_doc)

#         # Ensure playlist cover exists; override imageUrl if we created/found one
#         if bucket:
#             cover_url = ensure_playlist_cover_image(bucket, list_id, places_docs)
#             if cover_url:
#                 playlist_doc["imageUrl"] = cover_url
#                 print(f"  🖼️ Set cover image: {cover_url}")

#         writer.upsert_playlist_with_known_id(str(list_id), playlist_doc, places_docs)
#         processed += 1
#         print(f"→ {'Updated' if existed else 'Created'} '{playlist_doc['title']}' as ID {list_id}")

#     print(f"\n✅ Done. Processed {processed} playlist(s). DRY_RUN={DRY_RUN}")

# if __name__ == "__main__":
#     main()

# import json, os, re, sys, time
# from pathlib import Path
# from typing import Dict, Any, List, Optional, Tuple

# # Optional: load .env
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except Exception:
#     pass

# # ------------------------------- CONFIG -------------------------------
# BASE_DIR = Path(__file__).resolve().parent

# # Input / Firestore
# INPUT_JSON = BASE_DIR / "playlist_items_resolved.json"   # <-- resolved by Step 2.5
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# PROJECT_ID = "mycasavsc"
# COLLECTION = "playlistsNew"
# BUCKET_NAME = "mycasavsc.appspot.com"  # Firebase Storage bucket

# # Presentation / metadata
# IMAGE_BASE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg"
# G_IMAGE_TEMPLATE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
# G_IMAGE_COUNT = 3
# SOURCE = "original"
# CATEGORY = "Travel"
# UTC_OFFSET_DEFAULT = 330   # minutes (fallback)

# # Behavior
# DRY_RUN = False
# LIMIT = 0
# FILTER_TO_PUBLISHABLE = True    # only upload items marked "publishable" by Step 2.5

# CITY_ID_MAP = {
#     "Bengaluru": "35",
# }

# # Google Photos API (for Place photos only; no Place Search here)
# GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY") or "AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M"
# PHOTO_MAX_WIDTH = 1600
# PHOTO_SLEEP_SEC = 0.05

# # -------------------------- Deps --------------------------------------
# try:
#     import firebase_admin
#     from firebase_admin import credentials, firestore, storage
# except Exception:
#     firebase_admin = None
#     credentials = None
#     firestore = None
#     storage = None

# try:
#     from google.cloud.firestore_v1 import FieldFilter
# except Exception:
#     FieldFilter = None

# try:
#     import requests
# except Exception:
#     requests = None

# try:
#     from tqdm import tqdm
# except Exception:
#     def tqdm(x, **kwargs): return x

# # ----------------------------- Helpers --------------------------------
# def ensure_playlist_cover_image(bucket, list_id: int, places_docs: List[Dict[str, Any]]) -> Optional[str]:
#     """
#     Ensures a cover image exists at playlistsNew_images/{list_id}/1.jpg.
#     Copies the first existing place photo (…/placeId/1.jpg) to that path.
#     Returns the public URL if created/already present, else None.
#     """
#     if not bucket:
#         return None

#     dst_key = f"playlistsNew_images/{list_id}/1.jpg"
#     dst_blob = bucket.blob(dst_key)
#     if dst_blob.exists():
#         return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

#     # Find first place with an uploaded image
#     for p in places_docs:
#         pid = p.get("placeId")
#         if not pid:
#             continue
#         src_key = f"playlistsPlaces/{list_id}/{pid}/1.jpg"
#         src_blob = bucket.blob(src_key)
#         if src_blob.exists():
#             bucket.copy_blob(src_blob, bucket, dst_key)
#             # optional: caching header
#             dst_blob = bucket.blob(dst_key)
#             dst_blob.cache_control = "public, max-age=31536000"
#             try:
#                 dst_blob.patch()
#             except Exception:
#                 pass
#             return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

#     return None

# def md5_8(s: str) -> str:
#     import hashlib as _h
#     return _h.md5(s.encode("utf-8")).hexdigest()[:8]

# def slugify(text: str) -> str:
#     text = (text or "").strip().lower()
#     text = re.sub(r"[^\w\s-]+", "", text)
#     text = re.sub(r"[\s_-]+", "-", text)
#     text = re.sub(r"^-+|-+$", "", text)
#     return text or "untitled"

# def default_description(title: str) -> str:
#     return (f"Dive into “{title}” — a handpicked list of places with quick notes, links, and essentials "
#             f"for fast trip planning and discovery.")

# # ----------------------- Photo Download + Upload ----------------------
# class PhotoUploader:
#     def __init__(self, api_key: str, bucket):
#         if not requests:
#             raise RuntimeError("requests not installed. pip install requests")
#         self.api_key = api_key
#         self.bucket = bucket

#     def _photo_url(self, ref: str, max_width: int) -> str:
#         return (
#             "https://maps.googleapis.com/maps/api/place/photo"
#             f"?maxwidth={max_width}&photo_reference={ref}&key={self.api_key}"
#         )

#     def upload_place_photos(self, list_id: int, place_id: str, refs: List[str], count: int) -> List[int]:
#         """Return list of successful photo indices (e.g., [1,2,3])."""
#         if not self.bucket or not place_id or not refs:
#             return []
#         written = []
#         for i, ref in enumerate(refs[:max(1, count)], start=1):
#             dest = f"playlistsPlaces/{list_id}/{place_id}/{i}.jpg"
#             blob = self.bucket.blob(dest)
#             try:
#                 resp = requests.get(self._photo_url(ref, PHOTO_MAX_WIDTH), timeout=30)
#                 resp.raise_for_status()
#                 blob.cache_control = "public, max-age=31536000"
#                 blob.upload_from_string(resp.content, content_type=resp.headers.get("Content-Type") or "image/jpeg")
#                 written.append(i)
#                 time.sleep(PHOTO_SLEEP_SEC)
#             except Exception as e:
#                 print(f"⚠️ Photo upload failed: {dest} -> {e}")
#         return written

# # ---------------------------- Firestore ------------------------------
# class FirestoreWriter:
#     def __init__(self, sa_path: str, project_id: Optional[str], collection: str, dry: bool = False):
#         self.collection = collection
#         self.dry = dry
#         self.db = None
#         self.col_ref = None
#         self.next_id = None

#         if not dry:
#             if not firebase_admin:
#                 raise RuntimeError("firebase-admin not installed. pip install firebase-admin")
#             if not firebase_admin._apps:
#                 cred = credentials.Certificate(sa_path)
#                 firebase_admin.initialize_app(cred, {
#                     "projectId": project_id,
#                     "storageBucket": BUCKET_NAME,
#                 })
#             self.db = firestore.client()
#             self.col_ref = self.db.collection(self.collection)
#             self.next_id = self._compute_start_id()
#         else:
#             self.next_id = 1

#     def _compute_start_id(self) -> int:
#         max_id = 0
#         try:
#             for doc in self.col_ref.select([]).stream():
#                 try:
#                     v = int(doc.id)
#                     if v > max_id:
#                         max_id = v
#                 except ValueError:
#                     continue
#         except Exception:
#             pass
#         return max_id + 1

#     def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, Optional[Any], bool]:
#         if self.dry:
#             return str(self.next_id), None, False
#         try:
#             if FieldFilter is not None:
#                 existing = list(self.col_ref.where(filter=FieldFilter("slug", "==", slug)).limit(1).stream())
#             else:
#                 existing = list(self.col_ref.where("slug", "==", slug).limit(1).stream())
#         except Exception:
#             existing = []
#         if existing:
#             ref = existing[0].reference
#             return ref.id, ref, True
#         new_id = str(self.next_id)
#         self.next_id += 1
#         ref = self.col_ref.document(new_id)
#         return new_id, ref, False

#     def upsert_playlist_with_known_id(self, doc_id: str, playlist_doc: Dict[str, Any], places: List[Dict[str, Any]]) -> str:
#         if self.dry:
#             print(f"[DRY-RUN] Would write '{playlist_doc['title']}' as doc {doc_id} with {len(places)} places")
#             return doc_id

#         doc_ref = self.col_ref.document(doc_id)
#         doc_ref.set(playlist_doc, merge=False)

#         sub = doc_ref.collection("places")
#         old = list(sub.stream())
#         for i in range(0, len(old), 200):
#             batch = self.db.batch()
#             for doc in old[i:i+200]:
#                 batch.delete(doc.reference)
#             batch.commit()

#         for i in range(0, len(places), 450):
#             batch = self.db.batch()
#             for item in places[i:i+450]:
#                 sub_id = item.get("placeId") or item.get("_id")
#                 batch.set(sub.document(sub_id), item)
#             batch.commit()

#         return doc_id

# # ------------------------- Builders / Mappers -----------------------
# def build_playlist_doc(raw: Dict[str, Any],
#                        list_id: int,
#                        image_base: str,
#                        source: str,
#                        category: str,
#                        city_id_map: Dict[str, str],
#                        slug: str) -> Dict[str, Any]:
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"
#     subtype = str(raw.get("subtype","destination")).strip().lower()
#     if subtype not in {"destination","poi"}:
#         subtype = "destination"

#     src_urls = raw.get("source_urls") or []
#     if isinstance(src_urls, str):
#         src_urls = [src_urls]
#     dedup, seen = [], set()
#     for u in src_urls:
#         if isinstance(u, str):
#             u2 = u.strip()
#             if u2 and u2 not in seen:
#                 seen.add(u2); dedup.append(u2)

#     return {
#         "list_id": str(list_id),
#         "title": title,
#         "description": raw.get("description") or default_description(title),
#         "imageUrl": image_base.format(list_id=list_id),
#         "source": source,
#         "category": category,
#         "city_id": city_id_map.get(city, city),
#         "city": city,
#         "slug": slug,
#         "subtype": subtype,
#         "source_urls": dedup,
#         "created_ts": int(time.time())
#     }

# def build_g_image_urls(g_image_template: str, list_id: int, place_id: Optional[str], count: int) -> List[str]:
#     if not place_id:
#         return []
#     return [g_image_template.format(list_id=list_id, placeId=place_id, n=n) for n in range(1, max(1, count)+1)]

# def normalize_place_item(idx1_based: int,
#                          item: Dict[str, Any],
#                          enrich: Dict[str, Any],
#                          utc_offset: int,
#                          list_id: int,
#                          g_image_template: str,
#                          g_image_count: int) -> Dict[str, Any]:
#     # Prefer Google editorial if present, else Step-2 description
#     general_desc = item.get("generalDescription") or item.get("description") or None
#     description_val = enrich.get("googleDescription") or item.get("description") or None
#     place_id = enrich.get("placeId")
#     g_imgs = build_g_image_urls(g_image_template, list_id, place_id, g_image_count)

#     return {
#         # "_id": place_id or md5_8(f"{item.get('name','')}-{idx1_based}"),
#         "tripadvisorRating": item.get("tripadvisorRating", 0),
#         "description": description_val,
#         "website": enrich.get("website"),
#         "index": idx1_based,
#         "id": "",  # keep as-is for your schema
#         "categories": enrich.get("types", []),
#         "utcOffset": utc_offset,
#         "maxMinutesSpent": item.get("maxMinutesSpent", None),
#         "rating": enrich.get("rating", 0) or 0,
#         "numRatings": enrich.get("numRatings", 0) or 0,
#         "sources": item.get("sources", []),
#         "imageKeys": item.get("imageKeys", []),
#         "tripadvisorNumRatings": item.get("tripadvisorNumRatings", 0),
#         "openingPeriods": enrich.get("openingPeriods", []),
#         "generalDescription": general_desc,
#         "name": enrich.get("name") or item.get("name"),
#         "placeId": place_id,
#         "internationalPhoneNumber": enrich.get("internationalPhoneNumber"),
#         "reviews": enrich.get("reviews", []),
#         "ratingDistribution": item.get("ratingDistribution", {}),
#         "priceLevel": item.get("priceLevel", None) if item.get("priceLevel", None) is not None else enrich.get("priceLevel"),
#         "permanentlyClosed": bool(item.get("permanentlyClosed", False) or enrich.get("permanentlyClosed", False)),
#         "minMinutesSpent": item.get("minMinutesSpent", None),
#         "longitude": enrich.get("longitude"),
#         "address": enrich.get("address"),
#         "latitude": enrich.get("latitude"),
#         "g_image_urls": (item.get("g_image_urls") if item.get("g_image_urls") is not None else g_imgs)
#     }

# # -------------------------------- Main ------------------------------
# def main():
#     if not INPUT_JSON.exists():
#         print(f"❌ Input file not found: {INPUT_JSON}", file=sys.stderr); sys.exit(1)
#     if not DRY_RUN and not Path(SERVICE_ACCOUNT_JSON).exists():
#         print(f"❌ Service account JSON not found: {SERVICE_ACCOUNT_JSON}", file=sys.stderr); sys.exit(1)
#     if not requests:
#         print("❌ 'requests' is required. pip install requests", file=sys.stderr); sys.exit(1)

#     data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
#     if not isinstance(data, list):
#         print("❌ Input must be a JSON array of playlists.", file=sys.stderr); sys.exit(1)

#     # Firestore / Storage init
#     writer = FirestoreWriter(SERVICE_ACCOUNT_JSON, PROJECT_ID, COLLECTION, dry=bool(DRY_RUN))
#     bucket = storage.bucket(BUCKET_NAME) if not DRY_RUN else None

#     # Photos (Place Photos API only)
#     photo_uploader = PhotoUploader(GOOGLE_MAPS_API_KEY, bucket) if (GOOGLE_MAPS_API_KEY and not DRY_RUN) else None
#     print(f"Photo uploader enabled: {bool(photo_uploader)}")

#     processed = 0

#     for raw in tqdm(data, desc="Uploading playlists"):
#         if LIMIT and processed >= LIMIT:
#             break

#         title = (raw.get("playlistTitle") or raw.get("title") or "Untitled")
#         city  = (raw.get("placeName") or raw.get("city") or "India")
#         slug  = f"{slugify(title)}-{slugify(city)}"

#         doc_id, _, existed = writer.assign_doc_id_for_slug(slug)
#         list_id = int(doc_id)

#         # Base playlist doc (will override imageUrl after we create a cover)
#         playlist_doc = build_playlist_doc(
#             raw=raw, list_id=list_id, image_base=IMAGE_BASE, source=SOURCE,
#             category=CATEGORY, city_id_map=CITY_ID_MAP, slug=slug
#         )

#         # Optionally filter to publishable only
#         items = raw.get("items", [])
#         if FILTER_TO_PUBLISHABLE:
#             items = [it for it in items if it.get("resolution_status") == "publishable"]

#         places_docs = []
#         for idx, item in enumerate(items, start=1):
#             # Map resolved (snake_case) → enrich dict expected by normalizer (camelCase-ish)
#             place_id   = item.get("place_id")
#             photo_refs = item.get("photo_refs", [])
#             offset_min = item.get("utc_offset_minutes", UTC_OFFSET_DEFAULT)

#             enrich = {
#                 "name":       item.get("name"),
#                 "placeId":    place_id,
#                 "latitude":   item.get("lat"),
#                 "longitude":  item.get("lng"),
#                 "types":      item.get("types", []),
#                 "rating":     item.get("rating", 0) or 0,
#                 "numRatings": item.get("reviews", 0) or 0,
#                 "website":    item.get("website"),
#                 "address":    item.get("address"),
#                 "internationalPhoneNumber": item.get("phone"),
#                 "openingPeriods": item.get("opening", []),
#                 "priceLevel": item.get("price_level"),
#                 "permanentlyClosed": item.get("permanently_closed", False),
#                 "reviews":    [],  # map item reviews here if you stored them in Step 2.5
#                 "utcOffset":  offset_min,
#                 "photoRefs":  photo_refs,
#                 "googleDescription": None,  # not set by 2.5 currently
#             }

#             # Upload place photos (if any) and record which indices were written
#             uploaded_idxs = []
#             if photo_uploader and place_id and photo_refs:
#                 uploaded_idxs = photo_uploader.upload_place_photos(
#                     list_id=list_id,
#                     place_id=place_id,
#                     refs=photo_refs,
#                     count=G_IMAGE_COUNT
#                 )

#             # Only build URLs for files that exist
#             item_out = dict(item)
#             item_out["g_image_urls"] = [
#                 G_IMAGE_TEMPLATE.format(list_id=list_id, placeId=place_id, n=n)
#                 for n in uploaded_idxs
#             ] if uploaded_idxs else []

#             place_doc = normalize_place_item(
#                 idx1_based=idx,
#                 item=item_out,
#                 enrich=enrich,
#                 utc_offset=offset_min,
#                 list_id=list_id,
#                 g_image_template=G_IMAGE_TEMPLATE,
#                 g_image_count=G_IMAGE_COUNT
#             )
#             places_docs.append(place_doc)

#         # Ensure playlist cover exists; override imageUrl if we created/found one
#         if bucket:
#             cover_url = ensure_playlist_cover_image(bucket, list_id, places_docs)
#             if cover_url:
#                 playlist_doc["imageUrl"] = cover_url

#         writer.upsert_playlist_with_known_id(str(list_id), playlist_doc, places_docs)
#         processed += 1
#         print(f"→ {'updated' if existed else 'created'} '{playlist_doc['title']}' as ID {list_id}")

#     print(f"✅ Done. Processed {processed} playlist(s). DRY_RUN={DRY_RUN}")

# if __name__ == "__main__":
#     main()



#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# As per original pipeline of 3 steps
# import json, os, re, sys, time
# from pathlib import Path
# from typing import Dict, Any, List, Optional, Tuple

# # Optional: load .env
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except Exception:
#     pass

# # ------------------------------- CONFIG -------------------------------
# BASE_DIR = Path(__file__).resolve().parent

# # Input / Firestore
# INPUT_JSON = BASE_DIR / "playlist_items_resolved.json"
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# PROJECT_ID = "mycasavsc"
# COLLECTION = "playlistsNew"
# BUCKET_NAME = "mycasavsc.appspot.com"           # Firebase Storage bucket

# # Presentation / metadata
# IMAGE_BASE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg"
# G_IMAGE_TEMPLATE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
# G_IMAGE_COUNT = 3
# SOURCE = "original"
# CATEGORY = "Travel"
# UTC_OFFSET_DEFAULT = 330   # minutes (fallback)

# # Behavior
# DRY_RUN = False
# LIMIT = 0
# CITY_ID_MAP = {
#     "India": "86661",
# }

# # Google Maps
# GOOGLE_MAPS_API_KEY = "AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M"   # hardcode here; env var can override if blank
# INDIA_BOUNDS = (6.55, 68.11, 35.67, 97.39)                 # SW(lat,lng), NE(lat,lng)
# PHOTO_MAX_WIDTH = 1600
# PHOTO_SLEEP_SEC = 0.05
# SKIP_EXISTING_PHOTOS = True

# # -------------------------- Deps --------------------------------------
# try:
#     import firebase_admin
#     from firebase_admin import credentials, firestore, storage
# except Exception:
#     firebase_admin = None
#     credentials = None
#     firestore = None
#     storage = None

# try:
#     from google.cloud.firestore_v1 import FieldFilter
# except Exception:
#     FieldFilter = None

# try:
#     import googlemaps
# except Exception:
#     googlemaps = None

# try:
#     import requests
# except Exception:
#     requests = None

# try:
#     from tqdm import tqdm
# except Exception:
#     def tqdm(x, **kwargs): return x

# # ----------------------------- Helpers --------------------------------
# def ensure_playlist_cover_image(bucket, list_id: int, places_docs: List[Dict[str, Any]]) -> Optional[str]:
#     """
#     Ensures a cover image exists at playlistsNew_images/{list_id}/1.jpg.
#     Copies the first existing place photo (…/placeId/1.jpg) to that path.
#     Returns the public URL if created/already present, else None.
#     """
#     if not bucket:
#         return None

#     dst_key = f"playlistsNew_images/{list_id}/1.jpg"
#     dst_blob = bucket.blob(dst_key)
#     if dst_blob.exists():
#         return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

#     # Find first place with an uploaded image
#     for p in places_docs:
#         pid = p.get("placeId")
#         if not pid:
#             continue
#         src_key = f"playlistsPlaces/{list_id}/{pid}/1.jpg"
#         src_blob = bucket.blob(src_key)
#         if src_blob.exists():
#             bucket.copy_blob(src_blob, bucket, dst_key)
#             # optional: caching
#             dst_blob = bucket.blob(dst_key)
#             dst_blob.cache_control = "public, max-age=31536000"
#             try:
#                 dst_blob.patch()
#             except Exception:
#                 pass
#             return f"https://storage.googleapis.com/{BUCKET_NAME}/{dst_key}"

#     return None

# def md5_8(s: str) -> str:
#     import hashlib as _h
#     return _h.md5(s.encode("utf-8")).hexdigest()[:8]

# def slugify(text: str) -> str:
#     text = (text or "").strip().lower()
#     text = re.sub(r"[^\w\s-]+", "", text)
#     text = re.sub(r"[\s_-]+", "-", text)
#     text = re.sub(r"^-+|-+$", "", text)
#     return text or "untitled"

# def default_description(title: str) -> str:
#     return (f"Dive into “{title}” — a handpicked list of places with quick notes, links, and essentials "
#             f"for fast trip planning and discovery.")

# # ----------------------- Google Places Enricher -----------------------
# PLACES_DETAIL_FIELDS = [
#     "place_id",
#     "name",
#     "geometry",
#     "formatted_address",
#     "international_phone_number",
#     "website",
#     "opening_hours",
#     "price_level",
#     "permanently_closed",
#     "rating",
#     "user_ratings_total",
#     "reviews",
#     "utc_offset",
#     "photo",
#     "editorial_summary",
#     # request photos metadata
# ]

# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(exist_ok=True)
# PLACES_CACHE_PATH = CACHE_DIR / "places_cache.json"

# def _pick_best(cands: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
#     if not cands:
#         return None
#     def score(c):
#         t = set(c.get("types", []))
#         return (("locality" in t) * 3 +
#                 ("tourist_attraction" in t) * 2 +
#                 ("point_of_interest" in t) * 1)
#     return sorted(cands, key=score, reverse=True)[0]

# class PlacesEnricher:
#     def __init__(self, api_key: Optional[str]):
#         self.enabled = bool(api_key and googlemaps)
#         self.api_key = api_key
#         self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None
#         self.cache: Dict[str, Any] = {}
#         if PLACES_CACHE_PATH.exists():
#             try:
#                 self.cache = json.loads(PLACES_CACHE_PATH.read_text(encoding="utf-8"))
#             except Exception:
#                 self.cache = {}

#     def _save(self):
#         try:
#             PLACES_CACHE_PATH.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass

#     def _key(self, name: str, city_hint: str) -> str:
#         return f"{(name or '').strip().lower()}|{(city_hint or '').strip().lower()}"

#     def enrich(self, name: str, city_hint: str = "") -> Dict[str, Any]:
#         base = {
#             "name": name, "placeId": None,
#             "latitude": None, "longitude": None,
#             "types": [], "rating": 0, "numRatings": 0,
#             "website": None, "address": None,
#             "internationalPhoneNumber": None,
#             "openingPeriods": [], "priceLevel": None,
#             "permanentlyClosed": False, "reviews": [],
#             "utcOffset": None,
#             "photoRefs": [],
#         }
#         if not self.enabled:
#             return base

#         key = self._key(name, city_hint)
#         if key in self.cache:
#             return {**base, **self.cache[key]}

#         query = f"{name} {city_hint}".strip()
#         try:
#             lb = f"rectangle:{INDIA_BOUNDS[0]},{INDIA_BOUNDS[1]}|{INDIA_BOUNDS[2]},{INDIA_BOUNDS[3]}"
#             res = self.gmaps.find_place(
#                 input=query,
#                 input_type="textquery",
#                 fields=["place_id", "name", "geometry", "formatted_address", "types"],
#                 location_bias=lb
#             )
#             cands = res.get("candidates", []) or []
#             if not cands:
#                 ts = self.gmaps.places(query=query, region="in")
#                 cands = ts.get("results", []) or []

#             cand = _pick_best(cands)
#             if not cand:
#                 self.cache[key] = base
#                 self._save()
#                 return base

#             pid = cand.get("place_id")
#             det = {}
#             if pid:
#                 det = self.gmaps.place(place_id=pid, fields=PLACES_DETAIL_FIELDS).get("result", {}) or {}

#             geo = det.get("geometry") or {}
#             loc = geo.get("location") or {}
#             editorial = det.get("editorial_summary") or {}
#             editorial_overview = editorial.get("overview")
#             photos_meta = det.get("photos") or det.get("photo") or []  # tolerate both keys
#             photo_refs = []
#             for p in photos_meta[:10]:
#                 ref = p.get("photo_reference") or p.get("photoReference")
#                 if ref:
#                     photo_refs.append(ref)

#             out = {
#                 "name": det.get("name") or cand.get("name") or name,
#                 "placeId": det.get("place_id") or pid,
#                 "latitude": loc.get("lat"),
#                 "longitude": loc.get("lng"),
#                 "types": det.get("types") or cand.get("types") or [],
#                 "rating": det.get("rating", 0) or 0,
#                 "numRatings": det.get("user_ratings_total", 0) or 0,
#                 "website": det.get("website"),
#                 "address": det.get("formatted_address") or cand.get("formatted_address"),
#                 "internationalPhoneNumber": det.get("international_phone_number"),
#                 "openingPeriods": ((det.get("opening_hours") or {}).get("periods")) or [],
#                 "priceLevel": det.get("price_level"),
#                 "permanentlyClosed": det.get("permanently_closed", False) or False,
#                 "googleDescription": editorial_overview,
#                 "reviews": [
#                     {
#                         "rating": r.get("rating"),
#                         "text": r.get("text"),
#                         "author_name": r.get("author_name"),
#                         "relative_time_description": r.get("relative_time_description"),
#                         "time": r.get("time"),
#                         "profile_photo_url": r.get("profile_photo_url")
#                     }
#                     for r in (det.get("reviews") or [])[:5]
#                 ],
#                 "utcOffset": det.get("utc_offset"),
#                 "photoRefs": photo_refs,
#             }

#             self.cache[key] = out
#             time.sleep(0.05)
#             self._save()
#             return {**base, **out}

#         except Exception as e:
#             print(f"⚠️ Places enrich failed for query='{query}': {e}")
#             self.cache[key] = base
#             self._save()
#             return base

# # ----------------------- Photo Download + Upload ----------------------
# class PhotoUploader:
#     def __init__(self, api_key: str, bucket):
#         if not requests:
#             raise RuntimeError("requests not installed. pip install requests")
#         self.api_key = api_key
#         self.bucket = bucket

#     def _photo_url(self, ref: str, max_width: int) -> str:
#         return (
#             "https://maps.googleapis.com/maps/api/place/photo"
#             f"?maxwidth={max_width}&photo_reference={ref}&key={self.api_key}"
#         )

#     def upload_place_photos(self, list_id: int, place_id: str, refs: List[str], count: int) -> List[int]:
#         """Return list of successful photo indices (e.g., [1,2,3])."""
#         if not place_id or not refs:
#             return []
#         written = []
#         for i, ref in enumerate(refs[:max(1, count)], start=1):
#             dest = f"playlistsPlaces/{list_id}/{place_id}/{i}.jpg"
#             blob = self.bucket.blob(dest)
#             try:
#                 # force-write unless you want to skip existing
#                 # if SKIP_EXISTING_PHOTOS and blob.exists(): written.append(i); continue
#                 resp = requests.get(self._photo_url(ref, PHOTO_MAX_WIDTH), timeout=30)
#                 resp.raise_for_status()
#                 blob.cache_control = "public, max-age=31536000"
#                 blob.upload_from_string(resp.content, content_type=resp.headers.get("Content-Type") or "image/jpeg")
#                 written.append(i)
#                 time.sleep(PHOTO_SLEEP_SEC)
#             except Exception as e:
#                 print(f"⚠️ Photo upload failed: {dest} -> {e}")
#         return written

# # ---------------------------- Firestore ------------------------------
# class FirestoreWriter:
#     def __init__(self, sa_path: str, project_id: Optional[str], collection: str, dry: bool = False):
#         self.collection = collection
#         self.dry = dry
#         self.db = None
#         self.col_ref = None
#         self.next_id = None

#         if not dry:
#             if not firebase_admin:
#                 raise RuntimeError("firebase-admin not installed. pip install firebase-admin")
#             if not firebase_admin._apps:
#                 cred = credentials.Certificate(sa_path)
#                 firebase_admin.initialize_app(cred, {
#                     "projectId": project_id,
#                     "storageBucket": BUCKET_NAME,
#                 })
#             self.db = firestore.client()
#             self.col_ref = self.db.collection(self.collection)
#             self.next_id = self._compute_start_id()
#         else:
#             self.next_id = 1

#     def _compute_start_id(self) -> int:
#         max_id = 0
#         try:
#             for doc in self.col_ref.select([]).stream():
#                 try:
#                     v = int(doc.id)
#                     if v > max_id:
#                         max_id = v
#                 except ValueError:
#                     continue
#         except Exception:
#             pass
#         return max_id + 1

#     def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, Optional[Any], bool]:
#         if self.dry:
#             return str(self.next_id), None, False
#         try:
#             if FieldFilter is not None:
#                 existing = list(self.col_ref.where(filter=FieldFilter("slug", "==", slug)).limit(1).stream())
#             else:
#                 existing = list(self.col_ref.where("slug", "==", slug).limit(1).stream())
#         except Exception:
#             existing = []
#         if existing:
#             ref = existing[0].reference
#             return ref.id, ref, True
#         new_id = str(self.next_id)
#         self.next_id += 1
#         ref = self.col_ref.document(new_id)
#         return new_id, ref, False

#     def upsert_playlist_with_known_id(self, doc_id: str, playlist_doc: Dict[str, Any], places: List[Dict[str, Any]]) -> str:
#         if self.dry:
#             print(f"[DRY-RUN] Would write '{playlist_doc['title']}' as doc {doc_id} with {len(places)} places")
#             return doc_id

#         doc_ref = self.col_ref.document(doc_id)
#         doc_ref.set(playlist_doc, merge=False)

#         sub = doc_ref.collection("places")
#         old = list(sub.stream())
#         for i in range(0, len(old), 200):
#             batch = self.db.batch()
#             for doc in old[i:i+200]:
#                 batch.delete(doc.reference)
#             batch.commit()

#         for i in range(0, len(places), 450):
#             batch = self.db.batch()
#             for item in places[i:i+450]:
#                 sub_id = item.get("placeId") or item.get("_id")
#                 batch.set(sub.document(sub_id), item)
#             batch.commit()

#         return doc_id

# # ------------------------- Builders / Mappers -----------------------
# def build_playlist_doc(raw: Dict[str, Any],
#                        list_id: int,
#                        image_base: str,
#                        source: str,
#                        category: str,
#                        city_id_map: Dict[str, str],
#                        slug: str) -> Dict[str, Any]:
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"
#     subtype = str(raw.get("subtype","destination")).strip().lower()
#     if subtype not in {"destination","poi"}:
#         subtype = "destination"

#     src_urls = raw.get("source_urls") or []
#     if isinstance(src_urls, str):
#         src_urls = [src_urls]
#     dedup, seen = [], set()
#     for u in src_urls:
#         if isinstance(u, str):
#             u2 = u.strip()
#             if u2 and u2 not in seen:
#                 seen.add(u2); dedup.append(u2)

#     return {
#         # "_id": str(list_id),
#         "list_id": str(list_id),
#         "title": title,
#         "description": raw.get("description") or default_description(title),
#         "imageUrl": image_base.format(list_id=list_id),
#         "source": source,
#         "category": category,
#         "city_id": city_id_map.get(city, city),
#         "city": city,
#         "slug": slug,
#         "subtype": subtype,
#         "source_urls": dedup,
#         "created_ts": int(time.time())
#     }

# def build_g_image_urls(g_image_template: str, list_id: int, place_id: Optional[str], count: int) -> List[str]:
#     if not place_id:
#         return []
#     return [g_image_template.format(list_id=list_id, placeId=place_id, n=n) for n in range(1, max(1, count)+1)]

# def normalize_place_item(idx1_based: int,
#                          item: Dict[str, Any],
#                          enrich: Dict[str, Any],
#                          utc_offset: int,
#                          list_id: int,
#                          g_image_template: str,
#                          g_image_count: int) -> Dict[str, Any]:
#     general_desc = item.get("generalDescription") or item.get("description") or None
#     description_val = enrich.get("googleDescription") 
#     place_id = enrich.get("placeId")
#     g_imgs = build_g_image_urls(g_image_template, list_id, place_id, g_image_count)

#     return {
#         "_id": place_id or md5_8(f"{item.get('name','')}-{idx1_based}"),
#         "tripadvisorRating": item.get("tripadvisorRating", 0),
#         "description": description_val,
#         "website": enrich.get("website"),
#         "index": idx1_based,
#         "id":  "",
#         "categories": enrich.get("types", []),
#         "utcOffset": utc_offset,
#         "maxMinutesSpent": item.get("maxMinutesSpent", None),
#         "rating": enrich.get("rating", 0) or 0,
#         "numRatings": enrich.get("numRatings", 0) or 0,
#         "sources": item.get("sources", []),
#         "imageKeys": item.get("imageKeys", []),
#         "tripadvisorNumRatings": item.get("tripadvisorNumRatings", 0),
#         "openingPeriods": enrich.get("openingPeriods", []),
#         "generalDescription": general_desc,
#         "name": enrich.get("name") or item.get("name"),
#         "placeId": place_id,
#         "internationalPhoneNumber": enrich.get("internationalPhoneNumber"),
#         "reviews": enrich.get("reviews", []),
#         "ratingDistribution": item.get("ratingDistribution", {}),
#         "priceLevel": item.get("priceLevel", None) if item.get("priceLevel", None) is not None else enrich.get("priceLevel"),
#         "permanentlyClosed": bool(item.get("permanentlyClosed", False) or enrich.get("permanentlyClosed", False)),
#         "minMinutesSpent": item.get("minMinutesSpent", None),
#         "longitude": enrich.get("longitude"),
#         "address": enrich.get("address"),
#         "latitude": enrich.get("latitude"),
#         "g_image_urls": (item.get("g_image_urls") if item.get("g_image_urls") is not None else g_imgs)
#     }

# # -------------------------------- Main ------------------------------
# # 
# def main():
#     if not INPUT_JSON.exists():
#         print(f"❌ Input file not found: {INPUT_JSON}", file=sys.stderr); sys.exit(1)
#     if not DRY_RUN and not Path(SERVICE_ACCOUNT_JSON).exists():
#         print(f"❌ Service account JSON not found: {SERVICE_ACCOUNT_JSON}", file=sys.stderr); sys.exit(1)
#     if not requests:
#         print("❌ 'requests' is required. pip install requests", file=sys.stderr); sys.exit(1)

#     data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
#     if not isinstance(data, list):
#         print("❌ Input must be a JSON array of playlists.", file=sys.stderr); sys.exit(1)

#     # Firestore / Storage init
#     writer = FirestoreWriter(SERVICE_ACCOUNT_JSON, PROJECT_ID, COLLECTION, dry=bool(DRY_RUN))
#     bucket = storage.bucket(BUCKET_NAME) if not DRY_RUN else None

#     # Google Maps init
#     api_key = GOOGLE_MAPS_API_KEY or os.environ.get("GOOGLE_MAPS_API_KEY")
#     print(f"GOOGLE_MAPS_API_KEY configured: {bool(api_key)}")
#     enricher = PlacesEnricher(api_key)
#     print(f"Places enricher enabled: {enricher.enabled}")
#     photo_uploader = PhotoUploader(api_key, bucket) if (enricher.enabled and not DRY_RUN) else None

#     processed = 0

#     for raw in tqdm(data, desc="Uploading playlists"):
#         if LIMIT and processed >= LIMIT:
#             break

#         title = (raw.get("playlistTitle") or raw.get("title") or "Untitled")
#         city  = (raw.get("placeName") or raw.get("city") or "India")
#         slug  = f"{slugify(title)}-{slugify(city)}"

#         doc_id, _, existed = writer.assign_doc_id_for_slug(slug)
#         list_id = int(doc_id)

#         # Base playlist doc (will override imageUrl after we create a cover)
#         playlist_doc = build_playlist_doc(
#             raw=raw, list_id=list_id, image_base=IMAGE_BASE, source=SOURCE,
#             category=CATEGORY, city_id_map=CITY_ID_MAP, slug=slug
#         )

#         items = raw.get("items", [])
#         city_hint = raw.get("placeName") or playlist_doc.get("city") or "India"

#         places_docs = []
#         for idx, item in enumerate(items, start=1):
#             e = enricher.enrich(item.get("name", ""), city_hint)

#             # Upload place photos (if any)
#             uploaded_idxs = []
#             if photo_uploader and e.get("placeId") and e.get("photoRefs"):
#                 uploaded_idxs = photo_uploader.upload_place_photos(
#                     list_id=list_id, place_id=e["placeId"], refs=e["photoRefs"], count=G_IMAGE_COUNT
#                 )

#             # Only write URLs for files that exist
#             item_out = dict(item)
#             item_out["g_image_urls"] = [
#                 G_IMAGE_TEMPLATE.format(list_id=list_id, placeId=e["placeId"], n=n)
#                 for n in uploaded_idxs
#             ] if uploaded_idxs else []

#             # Prefer API utcOffset; fallback to default
#             offset_min = e.get("utcOffset") if e.get("utcOffset") is not None else UTC_OFFSET_DEFAULT

#             place_doc = normalize_place_item(
#                 idx1_based=idx,
#                 item=item_out,
#                 enrich=e,
#                 utc_offset=offset_min,
#                 list_id=list_id,
#                 g_image_template=G_IMAGE_TEMPLATE,
#                 g_image_count=G_IMAGE_COUNT
#             )
#             places_docs.append(place_doc)

#         # Ensure playlist cover exists; override imageUrl if we created/found one
#         if bucket:
#             cover_url = ensure_playlist_cover_image(bucket, list_id, places_docs)
#             if cover_url:
#                 playlist_doc["imageUrl"] = cover_url
#             # else optionally set a known placeholder you’ve uploaded:
#             # playlist_doc["imageUrl"] = f"https://storage.googleapis.com/{BUCKET_NAME}/defaults/playlist.jpg"

#         writer.upsert_playlist_with_known_id(str(list_id), playlist_doc, places_docs)
#         processed += 1
#         print(f"→ {'updated' if existed else 'created'} '{playlist_doc['title']}' as ID {list_id}")

#     print(f"✅ Done. Processed {processed} playlist(s). DRY_RUN={DRY_RUN}")


# if __name__ == "__main__":
#     main()




# Working but without Google Photos
# """
# # STEP 3 — Build & Upload Playlists to Firestore (numeric doc IDs, fixed Places fields)
# # Run: python 03_build_and_upload.py

# # What it does
# # ------------
# # - Reads input from a hardcoded JSON file (CONFIG below)
# # - Hardcoded Google Maps API key (env var can override)
# # - Scans Firestore collection once to find highest numeric doc.id
# # - For each playlist:
# #     * If slug exists -> reuse that numeric ID (update)
# #     * Else -> allocate next numeric ID (max+1)
# # - Clears & re-inserts "places" subcollection
# #     * Uses Google placeId as sub-doc ID (fallback to a stable hash)
# # - Places enrichment uses valid field masks (no more field errors)
# # """

# import json
# import os
# import re
# import sys
# import time
# from pathlib import Path
# from typing import Dict, Any, List, Optional, Tuple

# # ---------- Optional .env loader ----------
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except Exception:
#     pass

# # ------------------------------- CONFIG -------------------------------
# BASE_DIR = Path(__file__).resolve().parent

# # 1) Input / Firestore
# INPUT_JSON = BASE_DIR / "playlist_items.json"      # hardcoded input
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# PROJECT_ID = "mycasavsc"
# COLLECTION = "playlistsNew"

# # 2) Presentation / metadata
# IMAGE_BASE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg"
# G_IMAGE_TEMPLATE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
# G_IMAGE_COUNT = 3
# SOURCE = "original"
# CATEGORY = "Travel"
# UTC_OFFSET_DEFAULT = 330  # minutes; fallback if Details doesn't return utc_offset

# # 3) Behavior
# DRY_RUN = False
# LIMIT = 0  # 0 = no limit
# CITY_ID_MAP = {
#     "India": "86661",
# }

# # 4) Google Maps
# GOOGLE_MAPS_API_KEY = "AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M"  # hardcoded; env can override if blank
# # India bounds to bias search: SW(lat,lng), NE(lat,lng)
# INDIA_BOUNDS = (6.55, 68.11, 35.67, 97.39)

# # -------------------------- Optional deps -----------------------------
# # pip install firebase-admin googlemaps tqdm python-dotenv
# try:
#     import firebase_admin
#     from firebase_admin import credentials, firestore
# except Exception:
#     firebase_admin = None
#     credentials = None
#     firestore = None

# # for modern filter (to silence warning)
# try:
#     from google.cloud.firestore_v1 import FieldFilter
# except Exception:
#     FieldFilter = None

# try:
#     import googlemaps
# except Exception:
#     googlemaps = None

# try:
#     from tqdm import tqdm
# except Exception:
#     def tqdm(x, **kwargs): return x

# # ----------------------------- Helpers --------------------------------
# def md5_8(s: str) -> str:
#     import hashlib as _h
#     return _h.md5(s.encode("utf-8")).hexdigest()[:8]

# def slugify(text: str) -> str:
#     text = (text or "").strip().lower()
#     text = re.sub(r"[^\w\s-]+", "", text)
#     text = re.sub(r"[\s_-]+", "-", text)
#     text = re.sub(r"^-+|-+$", "", text)
#     return text or "untitled"

# def default_description(title: str) -> str:
#     return (f"Dive into “{title}” — a handpicked list of places with quick notes, links, and essentials "
#             f"for fast trip planning and discovery.")

# # ----------------------- Google Places Enricher -----------------------
# # Valid top-level fields for Place Details (no nested '/periods' in the mask)
# PLACES_DETAIL_FIELDS = [
#     "place_id",
#     "name",
#     "geometry",
#     "formatted_address",
#     "international_phone_number",
#     "website",
#     "opening_hours",
#     "price_level",
#     "permanently_closed",
#     "rating",
#     "user_ratings_total",
#     "reviews",
#     "utc_offset",
# ]

# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(exist_ok=True)
# PLACES_CACHE_PATH = CACHE_DIR / "places_cache.json"

# def _pick_best(cands: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
#     """Prefer locality / tourist attractions if multiple matches."""
#     if not cands:
#         return None
#     def score(c):
#         t = set(c.get("types", []))
#         return (("locality" in t) * 3 +
#                 ("tourist_attraction" in t) * 2 +
#                 ("point_of_interest" in t) * 1)
#     return sorted(cands, key=score, reverse=True)[0]

# class PlacesEnricher:
#     def __init__(self, api_key: Optional[str]):
#         self.enabled = bool(api_key and googlemaps)
#         self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None
#         self.cache: Dict[str, Any] = {}
#         if PLACES_CACHE_PATH.exists():
#             try:
#                 self.cache = json.loads(PLACES_CACHE_PATH.read_text(encoding="utf-8"))
#             except Exception:
#                 self.cache = {}

#     def _save(self):
#         try:
#             PLACES_CACHE_PATH.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass

#     def _key(self, name: str, city_hint: str) -> str:
#         return f"{(name or '').strip().lower()}|{(city_hint or '').strip().lower()}"

#     def enrich(self, name: str, city_hint: str = "") -> Dict[str, Any]:
#         base = {
#             "name": name, "placeId": None,
#             "latitude": None, "longitude": None,
#             "types": [], "rating": 0, "numRatings": 0,
#             "website": None, "address": None,
#             "internationalPhoneNumber": None,
#             "openingPeriods": [], "priceLevel": None,
#             "permanentlyClosed": False, "reviews": [],
#             "utcOffset": None,
#         }
#         if not self.enabled:
#             return base

#         key = self._key(name, city_hint)
#         if key in self.cache:
#             return {**base, **self.cache[key]}

#         query = f"{name} {city_hint}".strip()
#         try:
#             lb = f"rectangle:{INDIA_BOUNDS[0]},{INDIA_BOUNDS[1]}|{INDIA_BOUNDS[2]},{INDIA_BOUNDS[3]}"
#             # Find Place (textquery) first — can ask for 'types' here
#             res = self.gmaps.find_place(
#                 input=query,
#                 input_type="textquery",
#                 fields=["place_id", "name", "geometry", "formatted_address", "types"],
#                 location_bias=lb
#             )
#             cands = res.get("candidates", []) or []

#             # Fallback: Text Search (region bias to India)
#             if not cands:
#                 ts = self.gmaps.places(query=query, region="in")
#                 cands = ts.get("results", []) or []

#             cand = _pick_best(cands)
#             if not cand:
#                 self.cache[key] = base
#                 self._save()
#                 return base

#             pid = cand.get("place_id")
#             det = {}
#             if pid:
#                 det = self.gmaps.place(place_id=pid, fields=PLACES_DETAIL_FIELDS).get("result", {}) or {}

#             geo = det.get("geometry") or {}
#             loc = geo.get("location") or {}

#             out = {
#                 "name": det.get("name") or cand.get("name") or name,
#                 "placeId": det.get("place_id") or pid,
#                 "latitude": loc.get("lat"),
#                 "longitude": loc.get("lng"),
#                 "types": det.get("types") or cand.get("types") or [],
#                 "rating": det.get("rating", 0) or 0,
#                 "numRatings": det.get("user_ratings_total", 0) or 0,
#                 "website": det.get("website"),
#                 "address": det.get("formatted_address") or cand.get("formatted_address"),
#                 "internationalPhoneNumber": det.get("international_phone_number"),
#                 "openingPeriods": ((det.get("opening_hours") or {}).get("periods")) or [],
#                 "priceLevel": det.get("price_level"),
#                 "permanentlyClosed": det.get("permanently_closed", False) or False,
#                 "reviews": [
#                     {
#                         "rating": r.get("rating"),
#                         "text": r.get("text"),
#                         "author_name": r.get("author_name"),
#                         "relative_time_description": r.get("relative_time_description"),
#                         "time": r.get("time"),
#                         "profile_photo_url": r.get("profile_photo_url")
#                     }
#                     for r in (det.get("reviews") or [])[:5]
#                 ],
#                 "utcOffset": det.get("utc_offset"),  # minutes, if provided
#             }

#             self.cache[key] = out
#             time.sleep(0.05)  # gentle on quota
#             self._save()
#             return {**base, **out}

#         except Exception as e:
#             print(f"⚠️ Places enrich failed for query='{query}': {e}")
#             self.cache[key] = base
#             self._save()
#             return base

# # ---------------------------- Firestore ------------------------------
# class FirestoreWriter:
#     """
#     - Computes starting numeric ID = (max numeric doc.id in collection) + 1
#     - For each slug:
#         * reuse existing doc's ID if found
#         * else allocate next numeric ID, and advance the counter
#     """
#     def __init__(self, sa_path: str, project_id: Optional[str], collection: str, dry: bool = False):
#         self.collection = collection
#         self.dry = dry
#         self.db = None
#         self.col_ref = None
#         self.next_id = None

#         if not dry:
#             if not firebase_admin:
#                 raise RuntimeError("firebase-admin not installed. pip install firebase-admin")
#             if not firebase_admin._apps:
#                 cred = credentials.Certificate(sa_path)
#                 firebase_admin.initialize_app(cred, {"projectId": project_id} if project_id else None)
#             self.db = firestore.client()
#             self.col_ref = self.db.collection(self.collection)
#             self.next_id = self._compute_start_id()
#         else:
#             self.next_id = 1  # dry-run fallback

#     def _compute_start_id(self) -> int:
#         max_id = 0
#         try:
#             for doc in self.col_ref.select([]).stream():
#                 try:
#                     v = int(doc.id)
#                     if v > max_id:
#                         max_id = v
#                 except ValueError:
#                     continue
#         except Exception:
#             pass
#         return max_id + 1

#     def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, Optional[Any], bool]:
#         if self.dry:
#             return str(self.next_id), None, False
#         try:
#             if FieldFilter is not None:
#                 existing = list(self.col_ref.where(filter=FieldFilter("slug", "==", slug)).limit(1).stream())
#             else:
#                 # fallback (may warn)
#                 existing = list(self.col_ref.where("slug", "==", slug).limit(1).stream())
#         except Exception:
#             existing = []
#         if existing:
#             ref = existing[0].reference
#             return ref.id, ref, True
#         new_id = str(self.next_id)
#         self.next_id += 1
#         ref = self.col_ref.document(new_id)
#         return new_id, ref, False

#     def upsert_playlist_with_known_id(self, doc_id: str, playlist_doc: Dict[str, Any], places: List[Dict[str, Any]]) -> str:
#         if self.dry:
#             print(f"[DRY-RUN] Would write '{playlist_doc['title']}' as doc {doc_id} with {len(places)} places")
#             return doc_id

#         doc_ref = self.col_ref.document(doc_id)
#         doc_ref.set(playlist_doc, merge=False)

#         # Clear & reinsert subcollection "places"
#         sub = doc_ref.collection("places")
#         old = list(sub.stream())
#         for i in range(0, len(old), 200):
#             batch = self.db.batch()
#             for doc in old[i:i+200]:
#                 batch.delete(doc.reference)
#             batch.commit()

#         for i in range(0, len(places), 450):
#             batch = self.db.batch()
#             for item in places[i:i+450]:
#                 # Use Google placeId as sub-doc ID; fallback to stable hash in _id
#                 sub_id = item.get("placeId") or item.get("_id")
#                 batch.set(sub.document(sub_id), item)
#             batch.commit()

#         return doc_id

# # ------------------------- Builders / Mappers -----------------------
# def build_playlist_doc(raw: Dict[str, Any],
#                        list_id: int,
#                        image_base: str,
#                        source: str,
#                        category: str,
#                        city_id_map: Dict[str, str],
#                        slug: str) -> Dict[str, Any]:
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"

#     subtype = str(raw.get("subtype","destination")).strip().lower()
#     if subtype not in {"destination","poi"}:
#         subtype = "destination"

#     src_urls = raw.get("source_urls") or []
#     if isinstance(src_urls, str):
#         src_urls = [src_urls]
#     dedup, seen = [], set()
#     for u in src_urls:
#         if isinstance(u, str):
#             u2 = u.strip()
#             if u2 and u2 not in seen:
#                 seen.add(u2)
#                 dedup.append(u2)

#     return {
#         "_id": str(list_id),
#         "list_id": str(list_id),
#         "title": title,
#         "description": raw.get("description") or default_description(title),
#         "imageUrl": image_base.format(list_id=list_id),
#         "source": source,
#         "category": category,
#         "city_id": city_id_map.get(city, city),
#         "city": city,
#         "slug": slug,
#         "subtype": subtype,
#         "source_urls": dedup,
#         "created_ts": int(time.time())
#     }

# def build_g_image_urls(g_image_template: str, list_id: int, place_id: Optional[str], count: int) -> List[str]:
#     if not place_id:
#         return []
#     return [g_image_template.format(list_id=list_id, placeId=place_id, n=n) for n in range(1, max(1, count)+1)]

# def normalize_place_item(idx1_based: int,
#                          item: Dict[str, Any],
#                          enrich: Dict[str, Any],
#                          utc_offset: int,
#                          list_id: int,
#                          g_image_template: str,
#                          g_image_count: int) -> Dict[str, Any]:
#     general_desc = item.get("generalDescription") or item.get("description") or None
#     description_val = item.get("description_explicit")

#     place_id = enrich.get("placeId")
#     g_imgs = build_g_image_urls(g_image_template, list_id, place_id, g_image_count)

#     return {
#         "_id": place_id or md5_8(f"{item.get('name','')}-{idx1_based}"),
#         "tripadvisorRating": item.get("tripadvisorRating", 0),
#         "description": description_val if description_val is not None else None,
#         "website": enrich.get("website"),
#         "index": idx1_based,
#         "id": place_id or "",
#         "categories": enrich.get("types", []),
#         "utcOffset": utc_offset,
#         "maxMinutesSpent": item.get("maxMinutesSpent", None),
#         "rating": enrich.get("rating", 0) or 0,
#         "numRatings": enrich.get("numRatings", 0) or 0,
#         "sources": item.get("sources", []),
#         "imageKeys": item.get("imageKeys", []),
#         "tripadvisorNumRatings": item.get("tripadvisorNumRatings", 0),
#         "openingPeriods": enrich.get("openingPeriods", []),
#         "generalDescription": general_desc,
#         "name": enrich.get("name") or item.get("name"),
#         "placeId": place_id,
#         "internationalPhoneNumber": enrich.get("internationalPhoneNumber"),
#         "reviews": enrich.get("reviews", []),
#         "ratingDistribution": item.get("ratingDistribution", {}),
#         "priceLevel": item.get("priceLevel", None) if item.get("priceLevel", None) is not None else enrich.get("priceLevel"),
#         "permanentlyClosed": bool(item.get("permanentlyClosed", False) or enrich.get("permanentlyClosed", False)),
#         "minMinutesSpent": item.get("minMinutesSpent", None),
#         "longitude": enrich.get("longitude"),
#         "address": enrich.get("address"),
#         "latitude": enrich.get("latitude"),
#         "g_image_urls": item.get("g_image_urls") or g_imgs
#     }

# # -------------------------------- Main ------------------------------
# def main():
#     # 0) Sanity checks
#     if not INPUT_JSON.exists():
#         print(f"❌ Input file not found: {INPUT_JSON}", file=sys.stderr)
#         sys.exit(1)
#     if not DRY_RUN and not Path(SERVICE_ACCOUNT_JSON).exists():
#         print(f"❌ Service account JSON not found: {SERVICE_ACCOUNT_JSON}", file=sys.stderr)
#         sys.exit(1)

#     # 1) Load input
#     data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
#     if not isinstance(data, list):
#         print("❌ Input must be a JSON array of playlists.", file=sys.stderr)
#         sys.exit(1)

#     # 2) Init Firestore + Enricher
#     writer = FirestoreWriter(
#         sa_path=SERVICE_ACCOUNT_JSON,
#         project_id=PROJECT_ID,
#         collection=COLLECTION,
#         dry=bool(DRY_RUN)
#     )

#     # Prefer hardcoded key; fallback to env if hardcoded is empty
#     api_key = GOOGLE_MAPS_API_KEY or os.environ.get("GOOGLE_MAPS_API_KEY")
#     print(f"GOOGLE_MAPS_API_KEY configured: {bool(api_key)}")
#     enricher = PlacesEnricher(api_key)
#     print(f"Places enricher enabled: {enricher.enabled}")

#     processed = 0

#     # 3) Process playlists
#     for raw in tqdm(data, desc="Uploading playlists"):
#         if LIMIT and processed >= LIMIT:
#             break

#         title = (raw.get("playlistTitle") or raw.get("title") or "Untitled")
#         city = (raw.get("placeName") or raw.get("city") or "India")
#         slug = f"{slugify(title)}-{slugify(city)}"

#         doc_id, _, existed = writer.assign_doc_id_for_slug(slug)
#         list_id = int(doc_id)

#         playlist_doc = build_playlist_doc(
#             raw=raw,
#             list_id=list_id,
#             image_base=IMAGE_BASE,
#             source=SOURCE,
#             category=CATEGORY,
#             city_id_map=CITY_ID_MAP,
#             slug=slug
#         )

#         items = raw.get("items", [])
#         city_hint = raw.get("placeName") or playlist_doc.get("city") or "India"

#         places_docs = []
#         for idx, item in enumerate(items, start=1):
#             e = enricher.enrich(item.get("name", ""), city_hint)

#             # utc_offset: prefer Details' utc_offset; fallback to default
#             offset_min = e.get("utcOffset")
#             if offset_min is None:
#                 offset_min = UTC_OFFSET_DEFAULT

#             place_doc = normalize_place_item(
#                 idx1_based=idx,
#                 item=item,
#                 enrich=e,
#                 utc_offset=offset_min,
#                 list_id=list_id,
#                 g_image_template=G_IMAGE_TEMPLATE,
#                 g_image_count=G_IMAGE_COUNT
#             )
#             # Optional: quick debug to see which ID will be used for the sub-doc
#             # print("→ place:", place_doc["name"], "id used:", place_doc.get("placeId") or place_doc["_id"])
#             places_docs.append(place_doc)

#         writer.upsert_playlist_with_known_id(str(list_id), playlist_doc, places_docs)
#         processed += 1
#         action = "updated" if existed else "created"
#         print(f"→ {action} '{playlist_doc['title']}' as ID {list_id}")

#     print(f"✅ Done. Processed {processed} playlist(s). DRY_RUN={DRY_RUN}")

# if __name__ == "__main__":
#     main()






# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# """
# STEP 3 — Build & Upload Playlists to Firestore (numeric doc IDs, hardcoded config)
# Run: python 03_build_and_upload.py

# What it does
# ------------
# - Loads GOOGLE_MAPS_API_KEY from .env (optional enrichment)
# - Reads input from a hardcoded JSON file
# - Scans Firestore collection once to find highest numeric doc.id
# - For each playlist:
#     * If slug exists -> reuse that doc's numeric ID (update)
#     * Else -> allocate next numeric ID (max+1) and create
# - Clears & re-inserts real "places" subcollection per playlist

# Edit CONFIG below once; then you can run this script 100x with no flags.
# """

# import json
# import os
# import re
# import sys
# import time
# import hashlib
# from pathlib import Path
# from typing import Dict, Any, List, Optional, Tuple

# # ---------- Load .env automatically (optional but convenient) ----------
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except Exception:
#     pass

# # ------------------------------- CONFIG -------------------------------
# BASE_DIR = Path(__file__).resolve().parent

# # 1) Input / Firestore
# INPUT_JSON = BASE_DIR / "playlist_items.json"          # your input file (hardcoded)
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"  # <-- edit once (Windows example)
# PROJECT_ID = "mycasavsc"                               # <-- edit once
# COLLECTION = "playlistsNew"
# GOOGLE_MAPS_API_KEY = "AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M" 
# # 2) Presentation / metadata
# IMAGE_BASE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg"
# G_IMAGE_TEMPLATE = "https://storage.googleapis.com/mycasavsc.appspot.com/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
# G_IMAGE_COUNT = 3
# SOURCE = "original"
# CATEGORY = "Travel"
# UTC_OFFSET = 330

# # 3) Behavior
# DRY_RUN = False          # flip to False to write
# LIMIT = 0               # 0 = no limit
# CITY_ID_MAP = {
#     "India": "86661",
#     # "Srinagar": "256",
#     # "India": "00",
# }

# # -------------------------- Optional deps -----------------------------
# # pip install firebase-admin googlemaps tqdm python-dotenv
# try:
#     import firebase_admin
#     from firebase_admin import credentials, firestore
# except Exception:
#     firebase_admin = None
#     credentials = None
#     firestore = None

# try:
#     import googlemaps
# except Exception:
#     googlemaps = None

# try:
#     from tqdm import tqdm
# except Exception:
#     def tqdm(x, **kwargs): return x

# # ----------------------------- Helpers --------------------------------
# def md5_8(s: str) -> str:
#     import hashlib as _h
#     return _h.md5(s.encode("utf-8")).hexdigest()[:8]

# def slugify(text: str) -> str:
#     text = (text or "").strip().lower()
#     text = re.sub(r"[^\w\s-]+", "", text)
#     text = re.sub(r"[\s_-]+", "-", text)
#     text = re.sub(r"^-+|-+$", "", text)
#     return text or "untitled"

# def default_description(title: str) -> str:
#     return (f"Dive into “{title}” — a handpicked list of places with quick notes, links, and essentials "
#             f"for fast trip planning and discovery.")

# # ----------------------- Google Places Enricher -----------------------
# PLACES_DETAIL_FIELDS = [
#     "place_id","name","geometry/location","types","rating","user_ratings_total",
#     "formatted_address","international_phone_number","website",
#     "opening_hours/periods","price_level","permanently_closed","reviews"
# ]

# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(exist_ok=True)
# PLACES_CACHE_PATH = CACHE_DIR / "places_cache.json"

# class PlacesEnricher:
#     def __init__(self, api_key: Optional[str]):
#         self.enabled = bool(api_key and googlemaps)
#         self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None
#         self.cache: Dict[str, Any] = {}
#         if PLACES_CACHE_PATH.exists():
#             try:
#                 self.cache = json.loads(PLACES_CACHE_PATH.read_text(encoding="utf-8"))
#             except Exception:
#                 self.cache = {}

#     def _save(self):
#         try:
#             PLACES_CACHE_PATH.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass

#     def _key(self, name: str, city_hint: str) -> str:
#         return f"{(name or '').strip().lower()}|{(city_hint or '').strip().lower()}"

#     def enrich(self, name: str, city_hint: str = "") -> Dict[str, Any]:
#         base = {
#             "name": name, "placeId": None,
#             "latitude": None, "longitude": None,
#             "types": [], "rating": 0, "numRatings": 0,
#             "website": None, "address": None,
#             "internationalPhoneNumber": None,
#             "openingPeriods": [], "priceLevel": None,
#             "permanentlyClosed": False, "reviews": []
#         }
#         if not self.enabled:
#             return base

#         key = self._key(name, city_hint)
#         if key in self.cache:
#             return {**base, **self.cache[key]}

#         query = f"{name} {city_hint}".strip()
#         try:
#             res = self.gmaps.find_place(
#                 input=query, input_type="textquery",
#                 fields=["place_id","name","geometry","formatted_address","types"]
#             )
#             cands = res.get("candidates", [])
#             if not cands:
#                 ts = self.gmaps.places(query=query)
#                 cands = ts.get("results", [])
#                 if not cands:
#                     self.cache[key] = base
#                     self._save()
#                     return base

#             pid = cands[0].get("place_id")
#             det = {}
#             if pid:
#                 det = self.gmaps.place(place_id=pid, fields=PLACES_DETAIL_FIELDS).get("result", {}) or {}

#             out = {
#                 "name": det.get("name") or cands[0].get("name") or name,
#                 "placeId": det.get("place_id") or pid,
#                 "latitude": (det.get("geometry") or {}).get("location", {}).get("lat"),
#                 "longitude": (det.get("geometry") or {}).get("location", {}).get("lng"),
#                 "types": det.get("types", []) or cands[0].get("types", []) or [],
#                 "rating": det.get("rating", 0) or 0,
#                 "numRatings": det.get("user_ratings_total", 0) or 0,
#                 "website": det.get("website"),
#                 "address": det.get("formatted_address") or cands[0].get("formatted_address"),
#                 "internationalPhoneNumber": det.get("international_phone_number"),
#                 "openingPeriods": ((det.get("opening_hours") or {}).get("periods")) or [],
#                 "priceLevel": det.get("price_level"),
#                 "permanentlyClosed": det.get("permanently_closed", False) or False,
#                 "reviews": [
#                     {
#                         "rating": r.get("rating"),
#                         "text": r.get("text"),
#                         "author_name": r.get("author_name"),
#                         "relative_time_description": r.get("relative_time_description"),
#                         "time": r.get("time"),
#                         "profile_photo_url": r.get("profile_photo_url")
#                     }
#                     for r in (det.get("reviews") or [])[:5]
#                 ]
#             }
#             self.cache[key] = out
#             time.sleep(0.05)
#             self._save()
#             return {**base, **out}
#         except Exception as e:
#             print(f"⚠️ Places enrich failed for query='{query}': {e}")
#             self.cache[key] = base
#             self._save()
#             return base

# # ---------------------------- Firestore ------------------------------
# class FirestoreWriter:
#     """
#     - Computes starting numeric ID = (max numeric doc.id in collection) + 1
#     - For each slug:
#         * reuse existing doc's ID if found
#         * else allocate next numeric ID, and advance the counter
#     """
#     def __init__(self, sa_path: str, project_id: Optional[str], collection: str, dry: bool = False):
#         self.collection = collection
#         self.dry = dry
#         self.db = None
#         self.col_ref = None
#         self.next_id = None

#         if not dry:
#             if not firebase_admin:
#                 raise RuntimeError("firebase-admin not installed. pip install firebase-admin")
#             if not firebase_admin._apps:
#                 cred = credentials.Certificate(sa_path)
#                 firebase_admin.initialize_app(cred, {"projectId": project_id} if project_id else None)
#             self.db = firestore.client()
#             self.col_ref = self.db.collection(self.collection)
#             self.next_id = self._compute_start_id()
#         else:
#             self.next_id = 1  # dry-run fallback

#     def _compute_start_id(self) -> int:
#         max_id = 0
#         try:
#             for doc in self.col_ref.select([]).stream():
#                 try:
#                     v = int(doc.id)
#                     if v > max_id:
#                         max_id = v
#                 except ValueError:
#                     continue
#         except Exception:
#             pass
#         return max_id + 1

#     def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, Optional[Any], bool]:
#         if self.dry:
#             return str(self.next_id), None, False
#         existing = list(self.col_ref.where("slug", "==", slug).limit(1).stream())
#         if existing:
#             ref = existing[0].reference
#             return ref.id, ref, True
#         new_id = str(self.next_id)
#         self.next_id += 1
#         ref = self.col_ref.document(new_id)
#         return new_id, ref, False

#     def upsert_playlist_with_known_id(self, doc_id: str, playlist_doc: Dict[str, Any], places: List[Dict[str, Any]]) -> str:
#         if self.dry:
#             print(f"[DRY-RUN] Would write '{playlist_doc['title']}' as doc {doc_id} with {len(places)} places")
#             return doc_id

#         doc_ref = self.col_ref.document(doc_id)
#         doc_ref.set(playlist_doc, merge=False)

#         sub = doc_ref.collection("places")
#         old = list(sub.stream())
#         for i in range(0, len(old), 200):
#             batch = self.db.batch()
#             for doc in old[i:i+200]:
#                 batch.delete(doc.reference)
#             batch.commit()

#         for i in range(0, len(places), 450):
#             batch = self.db.batch()
#             for item in places[i:i+450]:
#                 # Use Google placeId as the document ID; fallback to our stable hash in _id
#                 sub_id = item.get("placeId") or item.get("_id")
#                 batch.set(sub.document(sub_id), item)
#             batch.commit()


#         return doc_id

# # ------------------------- Builders / Mappers -----------------------
# def build_playlist_doc(raw: Dict[str, Any],
#                        list_id: int,
#                        image_base: str,
#                        source: str,
#                        category: str,
#                        city_id_map: Dict[str, str],
#                        slug: str) -> Dict[str, Any]:
#     title = raw.get("playlistTitle") or raw.get("title") or "Untitled"
#     city = raw.get("placeName") or raw.get("city") or "India"

#     subtype = str(raw.get("subtype","destination")).strip().lower()
#     if subtype not in {"destination","poi"}:
#         subtype = "destination"

#     src_urls = raw.get("source_urls") or []
#     if isinstance(src_urls, str):
#         src_urls = [src_urls]
#     dedup, seen = [], set()
#     for u in src_urls:
#         if isinstance(u, str):
#             u2 = u.strip()
#             if u2 and u2 not in seen:
#                 seen.add(u2)
#                 dedup.append(u2)

#     return {
#         "_id": str(list_id),
#         "list_id": str(list_id),
#         "title": title,
#         "description": raw.get("description") or default_description(title),
#         "imageUrl": image_base.format(list_id=list_id),
#         "source": source,
#         "category": category,
#         "city_id": city_id_map.get(city, city),
#         "city": city,
#         "slug": slug,
#         "subtype": subtype,
#         "source_urls": dedup,
#         "created_ts": int(time.time())
#     }

# def build_g_image_urls(g_image_template: str, list_id: int, place_id: Optional[str], count: int) -> List[str]:
#     if not place_id:
#         return []
#     return [g_image_template.format(list_id=list_id, placeId=place_id, n=n) for n in range(1, max(1, count)+1)]

# def normalize_place_item(idx1_based: int,
#                          item: Dict[str, Any],
#                          enrich: Dict[str, Any],
#                          utc_offset: int,
#                          list_id: int,
#                          g_image_template: str,
#                          g_image_count: int) -> Dict[str, Any]:
#     general_desc = item.get("generalDescription") or item.get("description") or None
#     description_val = item.get("description_explicit")

#     place_id = enrich.get("placeId")
#     g_imgs = build_g_image_urls(g_image_template, list_id, place_id, g_image_count)

#     return {
#         "_id": place_id or md5_8(f"{item.get('name','')}-{idx1_based}"),
        # "tripadvisorRating": item.get("tripadvisorRating", 0),
        # "description": description_val if description_val is not None else None,
        # "website": enrich.get("website"),
        # "index": idx1_based,
        # "id": place_id or "",
        # "categories": enrich.get("types", []),
        # "utcOffset": utc_offset,
        # "maxMinutesSpent": item.get("maxMinutesSpent", None),
        # "rating": enrich.get("rating", 0) or 0,
        # "numRatings": enrich.get("numRatings", 0) or 0,
        # "sources": item.get("sources", []),
        # "imageKeys": item.get("imageKeys", []),
        # "tripadvisorNumRatings": item.get("tripadvisorNumRatings", 0),
        # "openingPeriods": enrich.get("openingPeriods", []),
        # "generalDescription": general_desc,
        # "name": enrich.get("name") or item.get("name"),
        # "placeId": place_id,
        # "internationalPhoneNumber": enrich.get("internationalPhoneNumber"),
        # "reviews": enrich.get("reviews", []),
        # "ratingDistribution": item.get("ratingDistribution", {}),
        # "priceLevel": item.get("priceLevel", None) if item.get("priceLevel", None) is not None else enrich.get("priceLevel"),
        # "permanentlyClosed": bool(item.get("permanentlyClosed", False) or enrich.get("permanentlyClosed", False)),
        # "minMinutesSpent": item.get("minMinutesSpent", None),
        # "longitude": enrich.get("longitude"),
        # "address": enrich.get("address"),
        # "latitude": enrich.get("latitude"),
        # "g_image_urls": item.get("g_image_urls") or g_imgs
#     }

# # -------------------------------- Main ------------------------------
# def main():
#     # 0) Sanity checks
#     if not INPUT_JSON.exists():
#         print(f"❌ Input file not found: {INPUT_JSON}", file=sys.stderr)
#         sys.exit(1)
#     if not DRY_RUN and not Path(SERVICE_ACCOUNT_JSON).exists():
#         print(f"❌ Service account JSON not found: {SERVICE_ACCOUNT_JSON}", file=sys.stderr)
#         sys.exit(1)

#     # 1) Load input
#     data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
#     if not isinstance(data, list):
#         print("❌ Input must be a JSON array of playlists.", file=sys.stderr)
#         sys.exit(1)

#     # 2) Init Firestore + Enricher
#     writer = FirestoreWriter(
#         sa_path=SERVICE_ACCOUNT_JSON,
#         project_id=PROJECT_ID,
#         collection=COLLECTION,
#         dry=bool(DRY_RUN)
#     )
#     api_key = GOOGLE_MAPS_API_KEY or os.environ.get("GOOGLE_MAPS_API_KEY")  # from .env
#     if not api_key:
#         print("ℹ️  GOOGLE_MAPS_API_KEY not set; skipping enrichment.")
#     print(f"GOOGLE_MAPS_API_KEY configured: {bool(api_key)}")    
#     enricher = PlacesEnricher(api_key)
#     print(f"Places enricher enabled: {enricher.enabled}")

#     processed = 0

#     # 3) Process playlists
#     for raw in tqdm(data, desc="Uploading playlists"):
#         if LIMIT and processed >= LIMIT:
#             break

#         title = (raw.get("playlistTitle") or raw.get("title") or "Untitled")
#         city = (raw.get("placeName") or raw.get("city") or "India")
#         slug = f"{slugify(title)}-{slugify(city)}"

#         doc_id, _, existed = writer.assign_doc_id_for_slug(slug)
#         list_id = int(doc_id)

#         playlist_doc = build_playlist_doc(
#             raw=raw,
#             list_id=list_id,
#             image_base=IMAGE_BASE,
#             source=SOURCE,
#             category=CATEGORY,
#             city_id_map=CITY_ID_MAP,
#             slug=slug
#         )

#         items = raw.get("items", [])
#         city_hint = raw.get("placeName") or playlist_doc.get("city") or "India"

#         places_docs = []
#         for idx, item in enumerate(items, start=1):
#             e = enricher.enrich(item.get("name", ""), city_hint)
#             place_doc = normalize_place_item(
#                 idx1_based=idx,
#                 item=item,
#                 enrich=e,
#                 utc_offset=UTC_OFFSET,
#                 list_id=list_id,
#                 g_image_template=G_IMAGE_TEMPLATE,
#                 g_image_count=G_IMAGE_COUNT
#             )
#             places_docs.append(place_doc)

#         writer.upsert_playlist_with_known_id(str(list_id), playlist_doc, places_docs)
#         processed += 1
#         action = "updated" if existed else "created"
#         print(f"→ {action} '{playlist_doc['title']}' as ID {list_id}")

#     enricher._save()
#     print(f"✅ Done. Processed {processed} playlist(s). DRY_RUN={DRY_RUN}")

# if __name__ == "__main__":
#     main()




