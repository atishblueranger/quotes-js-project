
import os
import re
import json
import math
import time
import random
import argparse
import requests
from pathlib import Path
from typing import Any, Dict, List, Tuple, Set, Optional
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.api_core.exceptions import ServiceUnavailable, DeadlineExceeded

# ───── CONFIGURATION (DEFAULTS) ────────────────────────────────────────────

# 1. Firebase Credentials & Bucket
SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
STORAGE_BUCKET       = "mycasavsc.appspot.com"

# 2. Google Places API (Needed to get photos + editorial_summary)
# You can also move this to an env var: os.getenv("GOOGLE_MAPS_API_KEY")
GOOGLE_API_KEY       = "AIzaSyDEuy2i8AbSR-jnstZG22ndFqy71jtKbgg"

# 3. Input Data File (list of { place_id, city_name, geoCategoryUrl })
DEFAULT_DATASET_FILE = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wanderlog_explore_attractions_full_urls_data2.json"

# 4. Defaults (overridable by CLI)
MIN_RATING_COUNT_DEFAULT       = 5      # quality gate
KEEP_RATIO_DEFAULT             = 1.0    # 1.0 = keep all, 0.7 = keep top 70%
MAX_PHOTOS_PER_PLACE_DEFAULT   = 10     # absolute cap per place
N_WANDERLOG_PHOTOS_DEFAULT     = 1      # default Wanderlog photos to use
N_GOOGLE_PHOTOS_DEFAULT        = 1      # default Google photos to fetch
SHUFFLE_SEED_DEFAULT           = 42
SHUFFLE_MAX_DISPLACEMENT       = 2      # how much each item can move up/down
BATCH_SIZE_DEFAULT             = 400    # max writes per Firestore batch

# 5. Collections / Paths
TARGET_COLLECTION    = "allplaces"
TARGET_SUBCOLLECTION = "top_attractions"
WANDERLOG_IMG_PREFIX = "https://itin-dev.wanderlogstatic.com/freeImage/"
LP_ATTRACTIONS_PREFIX = "lp_attractions"  # folder in GCS: lp_attractions/{placeId}_{idx}.jpg

# 6. City Whitelist
CITIES_WHITELIST: Set[str] = {
    # "Agartala",
    #  "Anjuna", 
    #  "Arpora",
    #   "Bodh Gaya", "Faridabad", "Ghaziabad",
    # "Hubli-Dharwad", "Idukki", "Jim Corbett National Park", "Kandaghat Tehsil",
    # "Khandala",
    #  "Kurnool",
    #   "Mandi", "Matheran", "Mysuru (Mysore)",
    # "Palampur", "Panchkula", "Porbandar", "Siliguri", "Silvassa", "Somnath",
    # "St. Petersburg", 
    # "Niagara Falls", 
    # "Hakone-machi", "Lille",
    # "Snowdonia National Park", # Done
    # "Denpasar",
    #  "Kaohsiung",
    # "Sumida",
    # "Wieliczka", # Done
    # "Xiamen", "Southampton", "El Nido", # Done
    # "Utrecht", "Port Douglas", "Soufriere",
    # "Dalian", "Ohrid", "Makati", "Hallstatt", "Strahan", "Sagres", "Pistoia",#Done
    #  "Tomar", "Falmouth", "Volterra", "Le Mans", "Varese", "Olbia", "Braies",#Done
    # "Mississauga", "Capitol Reef National Park", "Windsor", "Crystal River",#Done
    # "Uchisar", "San Michele Al Tagliamento", "Accra", "Vigan", "Piraeus",#Done
    # "Kaikoura", "Fujiyoshida", "Vancouver", "Vung Tau", "Almagro",#Done
    # "Victor Harbor", "Tomigusuku", "Soltau", "Reus", "Dinard", "Hondarribia",#Done
    # "Bangor", "Kumejima-cho", "Monreale", "Positano", "Stresa", "Toyako-cho",#Done
    # "Zakopane", "Troyes", "Sainte-Maxime", "Lihue", "Cefalu", "Cadaques",#Done
    # "Ragusa", "Kalambaka", "Himare", "Plitvice Lakes National Park", "Arles",
    # "Colonia del Sacramento", "Vik", "Kalmar", "San Sebastian - Donostia",
    # "Arcos de la Frontera", "Berat", "Rovinj", "Alesund", "Shirakawa-mura",
    # "Naxos Town", "Cork", "Durnstein", "Sibenik", "Chioggia", "Visby",
    # "Valladolid", "Hirosaki", "Monteverde Cloud Forest Reserve", "Sibiu",
    # "Ayvalik", "Sayulita"
    # "Lansdowne"
    # "Chittaurgarh"
    # "Panchgani"
    # "Arambol"
    # "Auroville"
    # "Kullu"
    # "Salem" # Salem (1251)
#    - Salem (58375)
#    - Salem (1251)
#    - Salem (58289)
# "Kargil"
# "Bhimtal"
# "Badrinath"
# "Jalandhar"
# "Kota"
# "Kohima"
# "Howrah"
# "Patnitop"
# "Konark"
# "Tawang"
# "Patiala"
# "Kurukshetra"
# "Thiruvarur"
# "Rupnagar"
# "Ratlam"
# "Udhampur"
# "Jhansi"
# "Kathua"
# "Kumarakom"
# "Pollachi"
# "Auli"
# "Barmer"
# "Ludhiana",
# "Rourkela",  
# "Bhavnagar",
# "Bathinda",


# "Calangute"
# "Kasaragod"
# "Tirunelveli"
# "Karwar"
# "Margao"
# "Vasco da Gama"

# "Ambaji",
# "Katra",
# "Gangotri",
# "Jhansi",
# "Guruvayur",
# "Fatehpur Sikri",
# "Agonda",
# "Cavelossim",
# "Saputara",
# "Chamba",
# "Mandu",
# "Murshidabad",
# "Valparai",
# "Mandya",

# "Greater Noida",
# "Bharuch",
# "Palani",
# "Rajgir",
# "Solapur",
# "Cuttack",
# "Jhansi",
# "Bishnupur",
# "Valsad",

# "Sirsi",
# "Chittoor",
# "Kolar",
# "Balasore",
# "Amarkantak",
# "Belgaum",
# "Sambalpur",
# "Dindigul",
# "Theni",
# "Pathanamthitta",
# "Hooghly",
# "Malappuram",
# "Secunderabad",
# "Jalpaiguri",
# "Tumkur",
# "Tezpur",
# "Nagapattinam",


# "Aizawl",
# "Anantnag",
# "Chhatarpur",
# "Uttarkashi",
# "Ganjam",
# "Bilaspur",
# "Rupnagar",
# "Bankura",
# "Jorhat",
# "Gulbarga",
# "Erode",
# "Jowai",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    ),
}

# Optional city → country mapping (fill as you go)
CITY_COUNTRY_MAP: Dict[str, str] = {
    # "New York City": "United States",
    # "Niagara Falls": "Canada / USA",
    # "Agartala": "India",
}

# ───── FIREBASE & STORAGE INIT ─────────────────────────────────────────────

if not firebase_admin._apps:
    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
    firebase_admin.initialize_app(cred, {"storageBucket": STORAGE_BUCKET})

db = firestore.client()
bucket = storage.bucket()

# ───── GENERIC HELPERS ─────────────────────────────────────────────────────

def coerce_int(v) -> int:
    try:
        if v is None:
            return 0
        return int(float(str(v)))
    except (ValueError, TypeError):
        return 0


def clean_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    return text.replace("\u2019", "'").replace("\u2014", "-").strip()


def iso_to_epoch_seconds(iso_str: str) -> Optional[int]:
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None


def point_read_exists(coll: str, doc_id: str, attempts: int = 5, base_delay: float = 0.5) -> bool:
    """Cheap parent existence check with retry."""
    ref = db.collection(coll).document(doc_id)
    delay = base_delay
    for i in range(attempts):
        try:
            return ref.get().exists
        except (ServiceUnavailable, DeadlineExceeded) as e:
            print(f"   ⚠️  point read retry {i+1}/{attempts} for {coll}/{doc_id}: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 4.0)
        except Exception as e:
            print(f"   ❌ failed to check {coll}/{doc_id}: {e}")
            return False
    try:
        return ref.get().exists
    except Exception as e:
        print(f"   ❌ failed to check {coll}/{doc_id} (final): {e}")
        return False


def ensure_parent_exists(coll: str, parent_id: str):
    """Create a minimal parent doc if missing (to avoid italic docs)."""
    ref = db.collection(coll).document(parent_id)
    snap = ref.get()
    if not snap.exists:
        print(f"   ➕ Creating parent {coll}/{parent_id}")
        ref.set({"_created_from_geoCategory": firestore.SERVER_TIMESTAMP}, merge=True)


def commit_with_retry(batch, label: str, attempts: int = 5) -> bool:
    delay = 1.0
    for i in range(attempts):
        try:
            batch.commit()
            return True
        except Exception as e:
            print(f"   ⚠️  commit failed [{label}] attempt {i+1}/{attempts}: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 16.0)
    return False


def get_target_state(parent_id: str) -> Tuple[Set[str], int]:
    """
    Return (existing IDs in subcollection, next index to assign)
    for allplaces/{parent_id}/top_attractions.
    """
    ref = db.collection(TARGET_COLLECTION).document(parent_id).collection(TARGET_SUBCOLLECTION)
    docs = list(ref.stream())
    existing_ids = {d.id for d in docs}
    max_idx = -1
    for d in docs:
        data = d.to_dict() or {}
        idx = coerce_int(data.get("index"))
        if idx > max_idx:
            max_idx = idx
    return existing_ids, max_idx + 1

# ───── GOOGLE PLACES IMAGE + EDITORIAL HELPERS ─────────────────────────────

def get_google_photos_and_overview(place_id: str,
                                   max_photos: int = 5) -> Tuple[List[str], str]:
    """
    Fetch photo URLs + editorial_summary.overview from Google Places Details API.

    Returns (photo_urls, overview_text).
    If API key or place_id missing, returns ([], "").
    """
    if not place_id or not GOOGLE_API_KEY:
        return [], ""

    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "photos,editorial_summary",
        "key": GOOGLE_API_KEY,
    }
    try:
        time.sleep(0.2)  # be polite
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        result = resp.json().get("result", {}) or {}

        # Photos
        urls: List[str] = []
        for p in (result.get("photos") or [])[:max_photos]:
            ref = p.get("photo_reference")
            if ref:
                dl_url = (
                    "https://maps.googleapis.com/maps/api/place/photo"
                    f"?maxwidth=1200&photo_reference={ref}&key={GOOGLE_API_KEY}"
                )
                urls.append(dl_url)

        # Editorial summary
        editorial = result.get("editorial_summary") or {}
        overview = editorial.get("overview") or ""

        return urls, overview
    except Exception as e:
        print(f"   ⚠️  Google Places details error for {place_id}: {e}")
        return [], ""


def download_and_upload_image(url: str, place_id: str, idx: int) -> Optional[str]:
    """
    Download image from source URL and upload to Firebase Storage at:
    lp_attractions/{place_id}_{idx}.jpg
    Returns public URL or None.
    """
    blob_path = f"{LP_ATTRACTIONS_PREFIX}/{place_id}_{idx}.jpg"

    backoff = 1.0
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            image_data = resp.content
            break
        except Exception:
            time.sleep(backoff)
            backoff *= 2
    else:
        print(f"   ⚠️  Failed to download: {url}")
        return None

    try:
        blob = bucket.blob(blob_path)
        blob.upload_from_string(image_data, content_type="image/jpeg")
        blob.make_public()
        return blob.public_url
    except Exception as e:
        print(f"   ⚠️  Upload failed for {blob_path}: {e}")
        return None

# ───── MOBX DATA & MERGE ───────────────────────────────────────────────────

def fetch_mobx_data(url: str) -> Optional[Dict[str, Any]]:
    print(f"   🌐 Fetching: {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        match = re.search(
            r'window\.__MOBX_STATE__\s*=\s*({.*?});',
            resp.text,
            re.DOTALL
        )
        if match:
            return json.loads(match.group(1))
    except Exception as e:
        print(f"   ❌ Network/Parse error: {e}")
    return None



def merge_places(mobx_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Merge placeMetadata + boardSections into per-place dicts.

    - Wanderlog’s generatedDescription is stored as `detail_description`
      (fallback to description if missing)
    - Board text (list note) is appended after that
    - raw_reviews is passed through from metadata for later use
    - latitude / longitude are taken from the BLOCK's place node (like in
      wanderlog_publish_scraper), falling back to metadata if needed.
    """
    places_list_page = (mobx_data.get("placesListPage") or {}).get("data", {}) or {}
    place_meta = places_list_page.get("placeMetadata", []) or []
    board_sections = places_list_page.get("boardSections", []) or []

    meta_map: Dict[str, Dict[str, Any]] = {}

    # --- Base metadata from placeMetadata ---------------------------------
    for p in place_meta:
        pid = str(p.get("placeId") or p.get("id") or "")
        if not pid:
            continue

        wl_desc = p.get("generatedDescription") or p.get("description") or ""

        meta_map[pid] = {
            "placeId": pid,
            "name": p.get("name"),
            "rating": p.get("rating") or 0.0,
            "numRatings": p.get("numRatings") or 0,
            "priceLevel": p.get("priceLevel"),
            "utcOffset": p.get("utcOffset"),
            "latitude": p.get("latitude"),          # may be None; will be overridden by block
            "longitude": p.get("longitude"),        # may be None; will be overridden by block
            "address": p.get("address"),
            "phone": p.get("internationalPhoneNumber"),
            "website": p.get("website"),
            "openingPeriods": p.get("openingPeriods") or [],
            "permanentlyClosed": p.get("permanentlyClosed", False),
            "categories": p.get("categories") or [],
            # Canonical Wanderlog description:
            "detail_description": wl_desc.strip(),
            # raw reviews (for reviews[] in Firestore doc)
            "raw_reviews": p.get("reviews") or [],
            # Wanderlog image keys:
            "imageKeys": p.get("imageKeys") or [],
        }

    merged_items: List[Dict[str, Any]] = []

    # --- Enrich from boardSections[*].blocks[*].place ---------------------
    for section in board_sections:
        items = section.get("items") or section.get("blocks") or []
        for item in items:
            if item.get("type") != "place":
                continue

            place_node = item.get("place") or {}
            pid = str(place_node.get("placeId") or place_node.get("id") or "")
            if not pid:
                continue

            if pid in meta_map:
                place_obj = meta_map[pid].copy()
            else:
                # minimal shell if metadata was missing
                place_obj = {
                    "placeId": pid,
                    "name": place_node.get("name"),
                    "rating": 0.0,
                    "numRatings": 0,
                    "priceLevel": None,
                    "utcOffset": None,
                    "address": None,
                    "phone": None,
                    "website": None,
                    "openingPeriods": [],
                    "permanentlyClosed": False,
                    "categories": [],
                    "detail_description": "",
                    "raw_reviews": [],
                    "imageKeys": [],
                }

            # ✅ Latitude / longitude from the BLOCK'S place node
            lat = place_node.get("latitude")
            lon = place_node.get("longitude")
            if lat is not None:
                place_obj["latitude"] = lat
            elif "latitude" not in place_obj:
                place_obj["latitude"] = None

            if lon is not None:
                place_obj["longitude"] = lon
            elif "longitude" not in place_obj:
                place_obj["longitude"] = None

            # Board-note / article text → append to detail_description
            text_ops = (item.get("text") or {}).get("ops", []) or []
            note_text = "".join(
                [t.get("insert", "") for t in text_ops if isinstance(t, dict)]
            ).strip()

            if note_text:
                base = place_obj.get("detail_description") or ""
                if base:
                    place_obj["detail_description"] = f"{base}\n\n{note_text}"
                else:
                    place_obj["detail_description"] = note_text

            # Image keys from the board block (prepend)
            block_keys = item.get("imageKeys") or []
            if block_keys:
                place_obj["imageKeys"] = block_keys + place_obj.get("imageKeys", [])

            merged_items.append(place_obj)

    return merged_items


# ───── RANKING / SCORING ───────────────────────────────────────────────────

def score_and_rank(items: List[Dict[str, Any]],
                   min_rating_count: int,
                   keep_ratio: float,
                   seed: int,
                   max_displacement: int = SHUFFLE_MAX_DISPLACEMENT) -> List[Dict[str, Any]]:
    """
    Score: rating * log10(count+1), filter by rating count, keep top fraction,
    then apply a light shuffle for better UX.
    """
    scored: List[Dict[str, Any]] = []
    for item in items:
        rating = float(item.get("rating") or 0.0)
        count = float(item.get("numRatings") or 0.0)
        score = rating * math.log10(max(count, 1.0) + 1.0)
        item["_score"] = score
        scored.append(item)

    scored.sort(key=lambda x: x["_score"], reverse=True)
    filtered = [i for i in scored if coerce_int(i.get("numRatings")) >= min_rating_count]

    if keep_ratio <= 0.0:
        return []
    cutoff = max(1, int(len(filtered) * keep_ratio)) if filtered else 0
    final_list = filtered[:cutoff]

    # Light shuffle (±max_displacement)
    rng = random.Random(seed)
    for i in range(len(final_list)):
        j = min(len(final_list) - 1, max(0, i + rng.randint(-max_displacement, max_displacement)))
        if i != j:
            final_list[i], final_list[j] = final_list[j], final_list[i]

    return final_list

# ───── DOC BUILDER ─────────────────────────────────────────────────────────

def build_doc_data(place: Dict[str, Any],
                   city_name: str,
                   parent_id: str,
                   url: str,
                   index_val: int,
                   uploaded_urls: List[str],
                   g_description: str) -> Dict[str, Any]:
    """
    Build final Firestore document for allplaces/{parent_id}/top_attractions/{placeId}
    with the exact field semantics:

    - detail_description: Wanderlog generatedDescription/description (+ optional board note)
    - g_description: Google Places editorial_summary.overview (if fetched), else ""
    - reviews: first few Wanderlog reviews (same format as wanderlog_publish_scraper)
    """
    pid = place.get("placeId")
    detail_desc = (place.get("detail_description") or "").strip()
    excerpt = detail_desc[:200] if detail_desc else ""

    types = [str(c) for c in place.get("categories", [])]
    country = CITY_COUNTRY_MAP.get(city_name, "")

    primary_image = uploaded_urls[0] if uploaded_urls else ""

    # Build reviews like wanderlog_publish_scraper
    reviews_list: List[Dict[str, Any]] = []
    for r in (place.get("raw_reviews") or [])[:3]:
        reviews_list.append({
            "rating": int(r.get("rating", 0)),
            "text": clean_text(r.get("reviewText")),
            "author_name": r.get("reviewerName") or "",
            "relative_time_description": "",
            "time": iso_to_epoch_seconds(r.get("time") or "") or 0,
            "profile_photo_url": "",
        })

    return {
        "placeId": pid,
        "name": place.get("name"),
        "city": city_name,
        "country": country,
        "source_url": url,
        "index": index_val,
        "detail_description": detail_desc,
        "g_description": g_description or "",
        "excerpt": excerpt,
        "rating": place.get("rating"),
        "ratingCount": place.get("numRatings"),
        "priceLevel": place.get("priceLevel"),
        "latitude": place.get("latitude"),
        "longitude": place.get("longitude"),
        "address": place.get("address"),
        "utcOffset": place.get("utcOffset"),
        "phone": place.get("phone"),
        "website": place.get("website"),
        "openingPeriods": place.get("openingPeriods"),
        "permanentlyClosed": place.get("permanentlyClosed"),
        "types": types,
        # Wanderlog image keys + uploaded images:
        "imageKeys": place.get("imageKeys", []),
        "image_url": primary_image,
        "g_image_urls": uploaded_urls,
        # Wanderlog reviews:
        "reviews": reviews_list,
    }

# ───── JOB PROCESSING ──────────────────────────────────────────────────────

def process_city(job_data: Dict[str, Any],
                 dry_run: bool,
                 n_wl_photos: int,
                 n_google_photos: int,
                 max_photos_per_place: int,
                 min_rating_count: int,
                 keep_ratio: float,
                 shuffle_seed: int,
                 batch_size: int) -> None:
    city_name = job_data.get("city_name")
    parent_id = str(job_data.get("place_id"))
    url = job_data.get("geoCategoryUrl")

    print(f"\n🏙️  Processing: {city_name} (ID: {parent_id})")

    # Ensure parent exists (LIVE mode)
    if not point_read_exists(TARGET_COLLECTION, parent_id):
        if dry_run:
            print(f"   ℹ️  Parent {TARGET_COLLECTION}/{parent_id} missing (DRY RUN, not creating).")
        else:
            ensure_parent_exists(TARGET_COLLECTION, parent_id)

    mobx = fetch_mobx_data(url)
    if not mobx:
        print("   ❌ Skipping city due to MOBX fetch failure.")
        return

    raw_places = merge_places(mobx)
    if not raw_places:
        print("   ⚠️  No places merged from MOBX.")
        return

    final_places = score_and_rank(
        raw_places,
        min_rating_count=min_rating_count,
        keep_ratio=keep_ratio,
        seed=shuffle_seed,
    )

    existing_ids, next_index = get_target_state(parent_id)
    print(f"   Found {len(final_places)} filtered items.")
    print(f"   Existing DB children: {len(existing_ids)} (next index = {next_index})")

    batch = db.batch()
    op_count = 0

    collection_ref = (
        db.collection(TARGET_COLLECTION)
          .document(parent_id)
          .collection(TARGET_SUBCOLLECTION)
    )

    for place in final_places:
        pid = place.get("placeId")
        if not pid:
            continue
        if pid in existing_ids:
            # dedupe on placeId
            continue

        # ─── IMAGE ENRICHMENT + g_description ─────────────────────────────
        uploaded_urls: List[str] = []
        g_description_for_doc: str = ""

        if dry_run:
            # Simulate image URLs
            simulated_w = min(len(place.get("imageKeys", [])), n_wl_photos)
            simulated_g = n_google_photos
            total_sim = min(simulated_w + simulated_g, max_photos_per_place)
            uploaded_urls = [f"DRY_RUN_URL_{i}.jpg" for i in range(1, total_sim + 1)]
            g_description_for_doc = ""
        else:
            # A. Wanderlog images (limit by n_wl_photos)
            w_keys = place.get("imageKeys", [])[:n_wl_photos]
            candidate_urls = [f"{WANDERLOG_IMG_PREFIX}{k}" for k in w_keys]

            # B. Google images + editorial_summary
            if n_google_photos > 0:
                print(f"   📸 Fetching Google photos for {place.get('name')} ({pid})...")
                g_urls, overview = get_google_photos_and_overview(
                    pid,
                    max_photos=n_google_photos,
                )
                candidate_urls.extend(g_urls)
                if overview:
                    g_description_for_doc = overview

            # Download + upload with global cap
            for idx, src_url in enumerate(candidate_urls, start=1):
                if len(uploaded_urls) >= max_photos_per_place:
                    break
                public_link = download_and_upload_image(src_url, pid, idx)
                if public_link:
                    uploaded_urls.append(public_link)

        # ─── WRITE DOC ────────────────────────────────────────────────────
        doc_data = build_doc_data(
            place=place,
            city_name=city_name,
            parent_id=parent_id,
            url=url,
            index_val=next_index,
            uploaded_urls=uploaded_urls,
            g_description=g_description_for_doc,
        )

        doc_ref = collection_ref.document(pid)

        if dry_run:
            preview = {
                "name": doc_data.get("name"),
                "placeId": doc_data.get("placeId"),
                "index": doc_data.get("index"),
                "rating": doc_data.get("rating"),
                "ratingCount": doc_data.get("ratingCount"),
                "permanentlyClosed": doc_data.get("permanentlyClosed"),
                "priceLevel": doc_data.get("priceLevel"),
                "types": doc_data.get("types"),
                "num_images": len(uploaded_urls),
                "has_g_description": bool(doc_data.get("g_description")),
            }
            print(
                f"   [DRY] Would write {doc_data.get('name')} :: "
                f"{ {k: v for k, v in preview.items() if v not in (None, [], '')} }"
            )
        else:
            batch.set(doc_ref, doc_data)
            op_count += 1

            if op_count >= batch_size:
                ok = commit_with_retry(batch, f"{parent_id}:{op_count}")
                print("   💾 Batch committed." if ok else "   ❌ Batch commit failed.")
                batch = db.batch()
                op_count = 0

        next_index += 1

    if not dry_run and op_count > 0:
        ok = commit_with_retry(batch, f"{parent_id}:final")
        print("   💾 Final batch committed." if ok else "   ❌ Final batch failed.")

# ───── JOB LOADING & MAIN ─────────────────────────────────────────────────

def load_jobs(dataset_path: str) -> List[Dict[str, Any]]:
    try:
        raw = json.loads(Path(dataset_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"❌ Could not find dataset file: {dataset_path}")
        return []
    whitelist_norm = {c.lower() for c in CITIES_WHITELIST}
    jobs = [j for j in raw if j.get("city_name", "").lower() in whitelist_norm]
    return jobs


def main():
    parser = argparse.ArgumentParser(
        description="Wanderlog geoCategory → allplaces/top_attractions with GCS images (best-of-both-worlds)."
    )
    parser.add_argument(
        "--dataset-file",
        default=DEFAULT_DATASET_FILE,
        help=f"Path to JSON dataset file (default: {DEFAULT_DATASET_FILE})",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Actually write to Firestore & GCS (otherwise DRY RUN).",
    )
    parser.add_argument(
        "--min-ratings",
        type=int,
        default=MIN_RATING_COUNT_DEFAULT,
        help=f"Minimum ratingCount for a place to be kept (default: {MIN_RATING_COUNT_DEFAULT})",
    )
    parser.add_argument(
        "--keep-ratio",
        type=float,
        default=KEEP_RATIO_DEFAULT,
        help=f"Fraction of sorted places to keep, after rating filter (0–1, default: {KEEP_RATIO_DEFAULT})",
    )
    parser.add_argument(
        "--max-photos-per-place",
        type=int,
        default=MAX_PHOTOS_PER_PLACE_DEFAULT,
        help=f"Hard cap on total photos per place (default: {MAX_PHOTOS_PER_PLACE_DEFAULT})",
    )
    parser.add_argument(
        "--wl-photos",
        type=int,
        default=N_WANDERLOG_PHOTOS_DEFAULT,
        help=f"Max Wanderlog CDN photos per place (default: {N_WANDERLOG_PHOTOS_DEFAULT})",
    )
    parser.add_argument(
        "--google-photos",
        type=int,
        default=N_GOOGLE_PHOTOS_DEFAULT,
        help=f"Max Google photos per place (default: {N_GOOGLE_PHOTOS_DEFAULT})",
    )
    parser.add_argument(
        "--shuffle-seed",
        type=int,
        default=SHUFFLE_SEED_DEFAULT,
        help=f"Seed for light shuffle (default: {SHUFFLE_SEED_DEFAULT})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE_DEFAULT,
        help=f"Max Firestore writes per batch commit (default: {BATCH_SIZE_DEFAULT})",
    )

    args = parser.parse_args()

    dry_run = not args.live
    dataset_path = args.dataset_file
    min_ratings = max(0, args.min_ratings)
    keep_ratio = max(0.0, min(1.0, args.keep_ratio))
    max_photos_per_place = max(0, args.max_photos_per_place)
    n_wl_photos = max(0, args.wl_photos)
    n_google_photos = max(0, args.google_photos)
    shuffle_seed = args.shuffle_seed
    batch_size = max(1, args.batch_size)

    print("🚀 Wanderlog geoCategory → allplaces/top_attractions (with mixed images)")
    print(f"   Dataset: {dataset_path}")
    print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"   MIN_RATING_COUNT: {min_ratings}")
    print(f"   KEEP_RATIO: {keep_ratio}")
    print(f"   Max photos per place: {max_photos_per_place}")
    print(f"   Wanderlog photos per place: {n_wl_photos}")
    print(f"   Google photos per place: {n_google_photos}")
    print(f"   Google API key set: {bool(GOOGLE_API_KEY)}")
    print(f"   Storage bucket: {STORAGE_BUCKET}\n")

    jobs_to_run = load_jobs(dataset_path)
    if not jobs_to_run:
        print("❌ No jobs to process (check dataset file or CITIES_WHITELIST).")
        return

    print(f"📋 Cities to process: {len(jobs_to_run)}")
    for j in jobs_to_run:
        print(f"   - {j.get('city_name')} ({j.get('place_id')})")

    if not dry_run:
        confirm = input("\n⚠️  This will WRITE to Firestore & GCS. Type 'yes' to continue: ").strip().lower()
        if confirm != "yes":
            print("Cancelled.")
            return

    for job in jobs_to_run:
        process_city(
            job_data=job,
            dry_run=dry_run,
            n_wl_photos=n_wl_photos,
            n_google_photos=n_google_photos,
            max_photos_per_place=max_photos_per_place,
            min_rating_count=min_ratings,
            keep_ratio=keep_ratio,
            shuffle_seed=shuffle_seed,
            batch_size=batch_size,
        )

    print("\n✅ Done.")


if __name__ == "__main__":
    main()



# import os
# import re
# import json
# import math
# import time
# import random
# import argparse
# import requests
# from pathlib import Path
# from typing import Any, Dict, List, Tuple, Set, Optional

# import firebase_admin
# from firebase_admin import credentials, firestore, storage
# from google.api_core.exceptions import ServiceUnavailable, DeadlineExceeded

# # ───── CONFIGURATION (DEFAULTS) ────────────────────────────────────────────

# # 1. Firebase Credentials & Bucket
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# STORAGE_BUCKET       = "mycasavsc.appspot.com"

# # 2. Google Places API (Needed to get the photo references)
# # You can also move this to an env var: os.getenv("GOOGLE_MAPS_API_KEY")
# GOOGLE_API_KEY       = "AIzaSyANekcM8Dmyyf8oe-tUVadBTC4xvBEi43o"

# # 3. Input Data File (list of { place_id, city_name, geoCategoryUrl })
# DEFAULT_DATASET_FILE = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wanderlog_explore_attractions_full_urls_data2.json"

# # 4. Defaults (overridable by CLI)
# MIN_RATING_COUNT_DEFAULT       = 5      # quality gate
# KEEP_RATIO_DEFAULT             = 1.0    # 1.0 = keep all, 0.7 = keep top 70%
# MAX_PHOTOS_PER_PLACE_DEFAULT   = 10     # absolute cap per place
# N_WANDERLOG_PHOTOS_DEFAULT     = 1      # default Wanderlog photos to use
# N_GOOGLE_PHOTOS_DEFAULT        = 1      # default Google photos to fetch
# SHUFFLE_SEED_DEFAULT           = 42
# SHUFFLE_MAX_DISPLACEMENT       = 2      # how much each item can move up/down
# BATCH_SIZE_DEFAULT             = 400    # max writes per Firestore batch

# # 5. Collections / Paths
# TARGET_COLLECTION   = "allplaces"
# TARGET_SUBCOLLECTION = "top_attractions"
# WANDERLOG_IMG_PREFIX = "https://itin-dev.wanderlogstatic.com/freeImage/"
# LP_ATTRACTIONS_PREFIX = "lp_attractions"  # folder in GCS: lp_attractions/{placeId}_{idx}.jpg

# # 6. City Whitelist
# CITIES_WHITELIST = {
#     "Agartala", 
#     # "Anjuna", "Arpora", "Bodh Gaya", "Faridabad", "Ghaziabad",
#     # "Hubli-Dharwad", "Idukki", "Jim Corbett National Park", "Kandaghat Tehsil",
#     # "Khandala", "Kochi", "Kurnool", "Mandi", "Matheran", "Mysuru (Mysore)",
#     # "Palampur", "Panchkula", "Porbandar", "Siliguri", "Silvassa", "Somnath",
#     # "St. Petersburg", "Niagara Falls", "Hakone-machi", "Lille",
#     # "Snowdonia National Park", "Denpasar", "Kaohsiung", "Sumida", "Wieliczka",
#     # "Xiamen", "Southampton", "El Nido", "Utrecht", "Port Douglas", "Soufriere",
#     # "Dalian", "Ohrid", "Makati", "Hallstatt", "Strahan", "Sagres", "Pistoia",
#     # "Tomar", "Falmouth", "Volterra", "Le Mans", "Varese", "Olbia", "Braies",
#     # "Mississauga", "Capitol Reef National Park", "Windsor", "Crystal River",
#     # "Uchisar", "San Michele Al Tagliamento", "Accra", "Vigan", "Piraeus",
#     # "Kaikoura", "Fujiyoshida", "Vancouver", "Vung Tau", "Almagro",
#     # "Victor Harbor", "Tomigusuku", "Soltau", "Reus", "Dinard", "Hondarribia",
#     # "Bangor", "Kumejima-cho", "Monreale", "Positano", "Stresa", "Toyako-cho",
#     # "Zakopane", "Troyes", "Sainte-Maxime", "Lihue", "Cefalu", "Cadaques",
#     # "Ragusa", "Kalambaka", "Himare", "Plitvice Lakes National Park", "Arles",
#     # "Colonia del Sacramento", "Vik", "Kalmar", "San Sebastian - Donostia",
#     # "Arcos de la Frontera", "Berat", "Rovinj", "Alesund", "Shirakawa-mura",
#     # "Naxos Town", "Cork", "Durnstein", "Sibenik", "Chioggia", "Visby",
#     # "Valladolid", "Hirosaki", "Monteverde Cloud Forest Reserve", "Sibiu",
#     # "Ayvalik", "Sayulita"
# }

# HEADERS = {
#     "User-Agent": (
#         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#         "AppleWebKit/537.36 (KHTML, like Gecko) "
#         "Chrome/91.0.4472.124 Safari/537.36"
#     ),
# }

# # Optional city → country mapping (fill as you go)
# CITY_COUNTRY_MAP: Dict[str, str] = {
#     # "New York City": "United States",
#     # "Niagara Falls": "Canada / USA",
#     # "Agartala": "India",
# }


# # ───── FIREBASE & STORAGE INIT ─────────────────────────────────────────────

# if not firebase_admin._apps:
#     cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
#     firebase_admin.initialize_app(cred, {"storageBucket": STORAGE_BUCKET})

# db = firestore.client()
# bucket = storage.bucket()


# # ───── GENERIC HELPERS ─────────────────────────────────────────────────────

# def coerce_int(v) -> int:
#     try:
#         if v is None:
#             return 0
#         return int(float(str(v)))
#     except (ValueError, TypeError):
#         return 0


# def point_read_exists(coll: str, doc_id: str, attempts: int = 5, base_delay: float = 0.5) -> bool:
#     """Cheap parent existence check with retry."""
#     ref = db.collection(coll).document(doc_id)
#     delay = base_delay
#     for i in range(attempts):
#         try:
#             return ref.get().exists
#         except (ServiceUnavailable, DeadlineExceeded) as e:
#             print(f"   ⚠️  point read retry {i+1}/{attempts} for {coll}/{doc_id}: {e}")
#             time.sleep(delay)
#             delay = min(delay * 2, 4.0)
#         except Exception as e:
#             print(f"   ❌ failed to check {coll}/{doc_id}: {e}")
#             return False
#     try:
#         return ref.get().exists
#     except Exception as e:
#         print(f"   ❌ failed to check {coll}/{doc_id} (final): {e}")
#         return False


# def ensure_parent_exists(coll: str, parent_id: str):
#     """Create a minimal parent doc if missing (to avoid italic docs)."""
#     ref = db.collection(coll).document(parent_id)
#     snap = ref.get()
#     if not snap.exists:
#         print(f"   ➕ Creating parent {coll}/{parent_id}")
#         ref.set({"_created_from_geoCategory": firestore.SERVER_TIMESTAMP}, merge=True)


# def commit_with_retry(batch, label: str, attempts: int = 5) -> bool:
#     delay = 1.0
#     for i in range(attempts):
#         try:
#             batch.commit()
#             return True
#         except Exception as e:
#             print(f"   ⚠️  commit failed [{label}] attempt {i+1}/{attempts}: {e}")
#             time.sleep(delay)
#             delay = min(delay * 2, 16.0)
#     return False


# def get_target_state(parent_id: str) -> Tuple[Set[str], int]:
#     """
#     Return (existing IDs in subcollection, next index to assign)
#     for allplaces/{parent_id}/top_attractions.
#     """
#     ref = db.collection(TARGET_COLLECTION).document(parent_id).collection(TARGET_SUBCOLLECTION)
#     docs = list(ref.stream())
#     existing_ids = {d.id for d in docs}
#     max_idx = -1
#     for d in docs:
#         data = d.to_dict() or {}
#         idx = coerce_int(data.get("index"))
#         if idx > max_idx:
#             max_idx = idx
#     return existing_ids, max_idx + 1


# # ───── GOOGLE PLACES IMAGE HELPERS ─────────────────────────────────────────

# def get_google_photo_urls(place_id: str, max_photos: int = 5) -> List[str]:
#     """Fetch photo URLs from Google Places API."""
#     if not place_id or not GOOGLE_API_KEY:
#         return []
#     url = "https://maps.googleapis.com/maps/api/place/details/json"
#     params = {
#         "place_id": place_id,
#         "fields": "photos",
#         "key": GOOGLE_API_KEY,
#     }
#     try:
#         time.sleep(0.2)  # be polite
#         resp = requests.get(url, params=params, timeout=10)
#         data = resp.json().get("result", {})
#         urls: List[str] = []
#         for p in data.get("photos", [])[:max_photos]:
#             ref = p.get("photo_reference")
#             if ref:
#                 # Direct Google photo URL (we then re-upload to GCS)
#                 dl_url = (
#                     "https://maps.googleapis.com/maps/api/place/photo"
#                     f"?maxwidth=1200&photo_reference={ref}&key={GOOGLE_API_KEY}"
#                 )
#                 urls.append(dl_url)
#         return urls
#     except Exception as e:
#         print(f"   ⚠️  Google API Error for {place_id}: {e}")
#         return []


# def download_and_upload_image(url: str, place_id: str, idx: int) -> Optional[str]:
#     """
#     Download image from source URL and upload to Firebase Storage at:
#     lp_attractions/{place_id}_{idx}.jpg
#     Returns public URL or None.
#     """
#     blob_path = f"{LP_ATTRACTIONS_PREFIX}/{place_id}_{idx}.jpg"

#     backoff = 1.0
#     for attempt in range(3):
#         try:
#             resp = requests.get(url, timeout=20)
#             resp.raise_for_status()
#             image_data = resp.content
#             break
#         except Exception:
#             time.sleep(backoff)
#             backoff *= 2
#     else:
#         print(f"   ⚠️  Failed to download: {url}")
#         return None

#     try:
#         blob = bucket.blob(blob_path)
#         blob.upload_from_string(image_data, content_type="image/jpeg")
#         blob.make_public()
#         return blob.public_url
#     except Exception as e:
#         print(f"   ⚠️  Upload failed for {blob_path}: {e}")
#         return None


# # ───── MOBX DATA & MERGE ───────────────────────────────────────────────────

# def fetch_mobx_data(url: str) -> Optional[Dict[str, Any]]:
#     print(f"   🌐 Fetching: {url}")
#     try:
#         resp = requests.get(url, headers=HEADERS, timeout=30)
#         resp.raise_for_status()
#         match = re.search(
#             r'window\.__MOBX_STATE__\s*=\s*({.*?});',
#             resp.text,
#             re.DOTALL
#         )
#         if match:
#             return json.loads(match.group(1))
#     except Exception as e:
#         print(f"   ❌ Network/Parse error: {e}")
#     return None


# def merge_places(mobx_data: Dict[str, Any]) -> List[Dict[str, Any]]:
#     """
#     Merge placeMetadata + boardSections into per-place dicts.
#     """
#     places_list_page = (mobx_data.get("placesListPage") or {}).get("data", {}) or {}
#     place_meta = places_list_page.get("placeMetadata", []) or []
#     board_sections = places_list_page.get("boardSections", []) or []

#     meta_map: Dict[str, Dict[str, Any]] = {}

#     # Base metadata
#     for p in place_meta:
#         pid = str(p.get("placeId") or p.get("id") or "")
#         if not pid:
#             continue
#         meta_map[pid] = {
#             "placeId": pid,
#             "name": p.get("name"),
#             "rating": p.get("rating") or 0.0,
#             "numRatings": p.get("numRatings") or 0,
#             "priceLevel": p.get("priceLevel"),
#             "utcOffset": p.get("utcOffset"),
#             "latitude": p.get("latitude"),
#             "longitude": p.get("longitude"),
#             "address": p.get("address"),
#             "phone": p.get("internationalPhoneNumber"),
#             "website": p.get("website"),
#             "openingPeriods": p.get("openingPeriods") or [],
#             "permanentlyClosed": p.get("permanentlyClosed", False),
#             "categories": p.get("categories") or [],
#             "generalDescription": p.get("generatedDescription") or p.get("description"),
#             "imageKeys": p.get("imageKeys") or [],
#         }

#     merged_items: List[Dict[str, Any]] = []

#     for section in board_sections:
#         items = section.get("items") or section.get("blocks") or []
#         for item in items:
#             if item.get("type") != "place":
#                 continue
#             place_node = item.get("place") or {}
#             pid = str(place_node.get("placeId") or place_node.get("id") or "")
#             if not pid:
#                 continue

#             if pid in meta_map:
#                 place_obj = meta_map[pid].copy()
#             else:
#                 place_obj = {"placeId": pid, "name": place_node.get("name"), "imageKeys": []}

#             # Description from board text
#             text_ops = (item.get("text") or {}).get("ops", []) or []
#             note_text = "".join([t.get("insert", "") for t in text_ops if isinstance(t, dict)])
#             place_obj["detail_description"] = note_text.strip()

#             # Image keys from the board block
#             block_keys = item.get("imageKeys") or []
#             if block_keys:
#                 place_obj["imageKeys"] = block_keys + place_obj.get("imageKeys", [])

#             merged_items.append(place_obj)

#     return merged_items


# def score_and_rank(items: List[Dict[str, Any]],
#                    min_rating_count: int,
#                    keep_ratio: float,
#                    seed: int,
#                    max_displacement: int = SHUFFLE_MAX_DISPLACEMENT) -> List[Dict[str, Any]]:
#     """
#     Score: rating * log10(count+1), filter by rating count, keep top fraction,
#     then apply a light shuffle for better UX.
#     """
#     scored: List[Dict[str, Any]] = []
#     for item in items:
#         rating = float(item.get("rating") or 0.0)
#         count = float(item.get("numRatings") or 0.0)
#         score = rating * math.log10(max(count, 1.0) + 1.0)
#         item["_score"] = score
#         scored.append(item)

#     scored.sort(key=lambda x: x["_score"], reverse=True)
#     filtered = [i for i in scored if coerce_int(i.get("numRatings")) >= min_rating_count]

#     if keep_ratio <= 0.0:
#         return []
#     cutoff = max(1, int(len(filtered) * keep_ratio)) if filtered else 0
#     final_list = filtered[:cutoff]

#     # Light shuffle (±max_displacement)
#     rng = random.Random(seed)
#     for i in range(len(final_list)):
#         j = min(len(final_list) - 1, max(0, i + rng.randint(-max_displacement, max_displacement)))
#         if i != j:
#             final_list[i], final_list[j] = final_list[j], final_list[i]

#     return final_list


# # ───── JOB PROCESSING ──────────────────────────────────────────────────────

# def build_doc_data(place: Dict[str, Any],
#                    city_name: str,
#                    parent_id: str,
#                    url: str,
#                    index_val: int,
#                    uploaded_urls: List[str]) -> Dict[str, Any]:
#     """
#     Build final Firestore document for allplaces/{parent_id}/top_attractions/{placeId}
#     with the exact fields you want.
#     """
#     pid = place.get("placeId")
#     detail_desc = place.get("detail_description") or ""
#     g_desc = place.get("generalDescription") or ""
#     excerpt = (detail_desc or "")[:200]

#     types = [str(c) for c in place.get("categories", [])]
#     country = CITY_COUNTRY_MAP.get(city_name, "")

#     primary_image = uploaded_urls[0] if uploaded_urls else ""

#     return {
#         "placeId": pid,
#         "name": place.get("name"),
#         "city": city_name,
#         "country": country,
#         "source_url": url,
#         "index": index_val,
#         "detail_description": detail_desc,
#         "g_description": g_desc,
#         "excerpt": excerpt,
#         "rating": place.get("rating"),
#         "ratingCount": place.get("numRatings"),
#         "priceLevel": place.get("priceLevel"),
#         "latitude": place.get("latitude"),
#         "longitude": place.get("longitude"),
#         "address": place.get("address"),
#         "utcOffset": place.get("utcOffset"),
#         "phone": place.get("phone"),
#         "website": place.get("website"),
#         "openingPeriods": place.get("openingPeriods"),
#         "permanentlyClosed": place.get("permanentlyClosed"),
#         "types": types,
#         # Key image fields:
#         "imageKeys": place.get("imageKeys", []),
#         "image_url": primary_image,
#         "g_image_urls": uploaded_urls,
#     }


# def process_city(job_data: Dict[str, Any],
#                  dry_run: bool,
#                  n_wl_photos: int,
#                  n_google_photos: int,
#                  max_photos_per_place: int,
#                  min_rating_count: int,
#                  keep_ratio: float,
#                  shuffle_seed: int,
#                  batch_size: int) -> None:
#     city_name = job_data.get("city_name")
#     parent_id = str(job_data.get("place_id"))
#     url = job_data.get("geoCategoryUrl")

#     print(f"\n🏙️  Processing: {city_name} (ID: {parent_id})")

#     # Ensure parent exists (LIVE mode)
#     if not point_read_exists(TARGET_COLLECTION, parent_id):
#         if dry_run:
#             print(f"   ℹ️  Parent {TARGET_COLLECTION}/{parent_id} missing (DRY RUN, not creating).")
#         else:
#             ensure_parent_exists(TARGET_COLLECTION, parent_id)

#     mobx = fetch_mobx_data(url)
#     if not mobx:
#         print("   ❌ Skipping city due to MOBX fetch failure.")
#         return

#     raw_places = merge_places(mobx)
#     if not raw_places:
#         print("   ⚠️  No places merged from MOBX.")
#         return

#     final_places = score_and_rank(
#         raw_places,
#         min_rating_count=min_rating_count,
#         keep_ratio=keep_ratio,
#         seed=shuffle_seed,
#     )

#     existing_ids, next_index = get_target_state(parent_id)
#     print(f"   Found {len(final_places)} filtered items.")
#     print(f"   Existing DB children: {len(existing_ids)} (next index = {next_index})")

#     batch = db.batch()
#     op_count = 0

#     collection_ref = (
#         db.collection(TARGET_COLLECTION)
#           .document(parent_id)
#           .collection(TARGET_SUBCOLLECTION)
#     )

#     for place in final_places:
#         pid = place.get("placeId")
#         if not pid:
#             continue
#         if pid in existing_ids:
#             # dedupe on placeId
#             continue

#         # ─── IMAGE ENRICHMENT ─────────────────────────────────────────
#         uploaded_urls: List[str] = []

#         if dry_run:
#             # Simulate image URLs
#             simulated_w = min(len(place.get("imageKeys", [])), n_wl_photos)
#             simulated_g = n_google_photos
#             total_sim = min(simulated_w + simulated_g, max_photos_per_place)
#             uploaded_urls = [f"DRY_RUN_URL_{i}.jpg" for i in range(1, total_sim + 1)]
#         else:
#             # A. Wanderlog images (limit by n_wl_photos)
#             w_keys = place.get("imageKeys", [])[:n_wl_photos]
#             candidate_urls = [f"{WANDERLOG_IMG_PREFIX}{k}" for k in w_keys]

#             # B. Google images (limit by n_google_photos)
#             if n_google_photos > 0:
#                 print(f"   📸 Fetching Google photos for {place.get('name')} ({pid})...")
#                 g_urls = get_google_photo_urls(pid, max_photos=n_google_photos)
#                 candidate_urls.extend(g_urls)

#             # Download + upload with global cap
#             for idx, src_url in enumerate(candidate_urls, start=1):
#                 if len(uploaded_urls) >= max_photos_per_place:
#                     break
#                 public_link = download_and_upload_image(src_url, pid, idx)
#                 if public_link:
#                     uploaded_urls.append(public_link)

#         # ─── WRITE DOC ────────────────────────────────────────────────
#         doc_data = build_doc_data(
#             place=place,
#             city_name=city_name,
#             parent_id=parent_id,
#             url=url,
#             index_val=next_index,
#             uploaded_urls=uploaded_urls,
#         )

#         doc_ref = collection_ref.document(pid)

#         if dry_run:
#             preview = {
#                 "name": doc_data.get("name"),
#                 "placeId": doc_data.get("placeId"),
#                 "index": doc_data.get("index"),
#                 "rating": doc_data.get("rating"),
#                 "ratingCount": doc_data.get("ratingCount"),
#                 "permanentlyClosed": doc_data.get("permanentlyClosed"),
#                 "priceLevel": doc_data.get("priceLevel"),
#                 "types": doc_data.get("types"),
#                 "num_images": len(uploaded_urls),
#             }
#             print(f"   [DRY] Would write {doc_data.get('name')} :: "
#                   f"{ {k: v for k, v in preview.items() if v not in (None, [], '')} }")
#         else:
#             batch.set(doc_ref, doc_data)
#             op_count += 1

#             if op_count >= batch_size:
#                 ok = commit_with_retry(batch, f"{parent_id}:{op_count}")
#                 print("   💾 Batch committed." if ok else "   ❌ Batch commit failed.")
#                 batch = db.batch()
#                 op_count = 0


#         next_index += 1

#     if not dry_run and op_count > 0:
#         ok = commit_with_retry(batch, f"{parent_id}:final")
#         print("   💾 Final batch committed." if ok else "   ❌ Final batch failed.")


# # ───── JOB LOADING & MAIN ─────────────────────────────────────────────────

# def load_jobs(dataset_path: str) -> List[Dict[str, Any]]:
#     try:
#         raw = json.loads(Path(dataset_path).read_text(encoding="utf-8"))
#     except FileNotFoundError:
#         print(f"❌ Could not find dataset file: {dataset_path}")
#         return []
#     whitelist_norm = {c.lower() for c in CITIES_WHITELIST}
#     jobs = [j for j in raw if j.get("city_name", "").lower() in whitelist_norm]
#     return jobs


# def main():
#     parser = argparse.ArgumentParser(
#         description="Wanderlog geoCategory → allplaces/top_attractions with GCS images (best-of-both-worlds)."
#     )
#     parser.add_argument(
#         "--dataset-file",
#         default=DEFAULT_DATASET_FILE,
#         help=f"Path to JSON dataset file (default: {DEFAULT_DATASET_FILE})",
#     )
#     parser.add_argument(
#         "--live",
#         action="store_true",
#         help="Actually write to Firestore & GCS (otherwise DRY RUN).",
#     )
#     parser.add_argument(
#         "--min-ratings",
#         type=int,
#         default=MIN_RATING_COUNT_DEFAULT,
#         help=f"Minimum ratingCount for a place to be kept (default: {MIN_RATING_COUNT_DEFAULT})",
#     )
#     parser.add_argument(
#         "--keep-ratio",
#         type=float,
#         default=KEEP_RATIO_DEFAULT,
#         help=f"Fraction of sorted places to keep, after rating filter (0–1, default: {KEEP_RATIO_DEFAULT})",
#     )
#     parser.add_argument(
#         "--max-photos-per-place",
#         type=int,
#         default=MAX_PHOTOS_PER_PLACE_DEFAULT,
#         help=f"Hard cap on total photos per place (default: {MAX_PHOTOS_PER_PLACE_DEFAULT})",
#     )
#     parser.add_argument(
#         "--wl-photos",
#         type=int,
#         default=N_WANDERLOG_PHOTOS_DEFAULT,
#         help=f"Max Wanderlog CDN photos per place (default: {N_WANDERLOG_PHOTOS_DEFAULT})",
#     )
#     parser.add_argument(
#         "--google-photos",
#         type=int,
#         default=N_GOOGLE_PHOTOS_DEFAULT,
#         help=f"Max Google photos per place (default: {N_GOOGLE_PHOTOS_DEFAULT})",
#     )
#     parser.add_argument(
#         "--shuffle-seed",
#         type=int,
#         default=SHUFFLE_SEED_DEFAULT,
#         help=f"Seed for light shuffle (default: {SHUFFLE_SEED_DEFAULT})",
#     )
#     parser.add_argument(
#         "--batch-size",
#         type=int,
#         default=BATCH_SIZE_DEFAULT,
#         help=f"Max Firestore writes per batch commit (default: {BATCH_SIZE_DEFAULT})",
#     )


#     args = parser.parse_args()

#     dry_run = not args.live
#     dataset_path = args.dataset_file
#     min_ratings = max(0, args.min_ratings)
#     keep_ratio = max(0.0, min(1.0, args.keep_ratio))
#     max_photos_per_place = max(0, args.max_photos_per_place)
#     n_wl_photos = max(0, args.wl_photos)
#     n_google_photos = max(0, args.google_photos)
#     shuffle_seed = args.shuffle_seed
#     batch_size = max(1, args.batch_size)


#     print("🚀 Wanderlog geoCategory → allplaces/top_attractions (with mixed images)")
#     print(f"   Dataset: {dataset_path}")
#     print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE'}")
#     print(f"   MIN_RATING_COUNT: {min_ratings}")
#     print(f"   KEEP_RATIO: {keep_ratio}")
#     print(f"   Max photos per place: {max_photos_per_place}")
#     print(f"   Wanderlog photos per place: {n_wl_photos}")
#     print(f"   Google photos per place: {n_google_photos}")
#     print(f"   Google API key set: {bool(GOOGLE_API_KEY)}")
#     print(f"   Storage bucket: {STORAGE_BUCKET}\n")

#     jobs_to_run = load_jobs(dataset_path)
#     if not jobs_to_run:
#         print("❌ No jobs to process (check dataset file or CITIES_WHITELIST).")
#         return

#     print(f"📋 Cities to process: {len(jobs_to_run)}")
#     for j in jobs_to_run:
#         print(f"   - {j.get('city_name')} ({j.get('place_id')})")

#     if not dry_run:
#         confirm = input("\n⚠️  This will WRITE to Firestore & GCS. Type 'yes' to continue: ").strip().lower()
#         if confirm != "yes":
#             print("Cancelled.")
#             return

#     for job in jobs_to_run:
#         process_city(
#             job_data=job,
#             dry_run=dry_run,
#             n_wl_photos=n_wl_photos,
#             n_google_photos=n_google_photos,
#             max_photos_per_place=max_photos_per_place,
#             min_rating_count=min_ratings,
#             keep_ratio=keep_ratio,
#             shuffle_seed=shuffle_seed,
#             batch_size=batch_size,
#         )


#     print("\n✅ Done.")


# if __name__ == "__main__":
#     main()
