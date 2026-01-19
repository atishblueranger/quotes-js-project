# adding_missing_fields_playlistsNew_collection.py
# Paginated enrichment for ALL playlists, minimal API calls, supports:
#  • sub-collection 'places' (primary path)
#  • inline array fallback (subcollections.places) if present

import argparse, time, json, os
from typing import Any, Dict, List, Optional, Tuple

import requests
import firebase_admin
from firebase_admin import credentials, firestore as fa_fs, storage as fa_storage
from google.api_core.exceptions import DeadlineExceeded

# ── CONFIG ────────────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
BUCKET_NAME          = "mycasavsc.appspot.com"
COLLECTION_NAME      = "playlistsNew"

# Hardcoded API key (as requested)
GOOGLE_PLACES_API_KEY = "AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M"

MAX_PHOTOS  = 3
MAX_REVIEWS = 5

HTTP_TIMEOUT_SECS = 15
MAX_RETRIES       = 5
BACKOFF_BASE_SECS = 0.8
BACKOFF_JITTER    = 0.25

PAGE_SIZE = 100
PROGRESS_FILE = "enrich_playlists_progress.json"

# In-memory cache: placeId -> {"location": {...}|None, "reviews": [...], "photo_refs":[...]}
PLACE_CACHE: Dict[str, Dict[str, Any]] = {}

# ── INIT ──────────────────────────────────────────────────────────────────────
def init_firebase() -> tuple[fa_fs.Client, fa_storage.bucket]:
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred, {"storageBucket": BUCKET_NAME})
    db = fa_fs.client()
    bucket = fa_storage.bucket(BUCKET_NAME)
    return db, bucket

# ── Backoff helpers ───────────────────────────────────────────────────────────
def _sleep_with_jitter(base: float, attempt: int) -> None:
    import random
    time.sleep((base * (2 ** (attempt - 1))) + random.uniform(0, BACKOFF_JITTER))

def get_json_with_backoff(url: str) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT_SECS)
            if r.status_code in (429, 500, 502, 503, 504):
                _sleep_with_jitter(BACKOFF_BASE_SECS, attempt); continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last_exc = e
            _sleep_with_jitter(BACKOFF_BASE_SECS, attempt)
    raise RuntimeError(f"HTTP request failed after {MAX_RETRIES} retries: {last_exc}")

def stream_bytes_with_backoff(url: str) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT_SECS, stream=True)
            if r.status_code in (429, 500, 502, 503, 504):
                _sleep_with_jitter(BACKOFF_BASE_SECS, attempt); continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last_exc = e
            _sleep_with_jitter(BACKOFF_BASE_SECS, attempt)
    raise RuntimeError(f"Photo download failed after {MAX_RETRIES} retries: {last_exc}")

# ── Google Places helpers ─────────────────────────────────────────────────────
def build_fields(need_geom: bool, need_reviews: bool, need_photos: bool) -> Optional[str]:
    fields = []
    if need_geom:
        fields.append("geometry")
    if need_photos:
        fields.append("photos")
    if need_reviews:
        fields.append("reviews")
    return ",".join(fields) if fields else None

def fetch_place_details_partial(place_id: str, fields: str) -> Dict[str, Any]:
    url = ("https://maps.googleapis.com/maps/api/place/details/json"
           f"?place_id={place_id}&fields={fields}&key={GOOGLE_PLACES_API_KEY}")
    data = get_json_with_backoff(url)
    status = data.get("status")
    if status != "OK":
        raise RuntimeError(f"Places Details status={status} place_id={place_id} msg={data.get('error_message')}")
    result = data.get("result", {})
    out: Dict[str, Any] = {"location": None, "reviews": [], "photo_refs": []}
    if "geometry" in fields:
        out["location"] = (result.get("geometry") or {}).get("location")
    if "reviews" in fields:
        out["reviews"] = result.get("reviews", []) or []
    if "photos" in fields:
        photos = result.get("photos", []) or []
        out["photo_refs"] = [p.get("photo_reference") for p in photos[:MAX_PHOTOS] if p.get("photo_reference")]
    return out

def get_enrichment(place_id: str,
                   need_geom: bool, need_reviews: bool, need_photos: bool,
                   stats: Dict[str, int]) -> Dict[str, Any]:
    # in-memory cache reuse
    if place_id in PLACE_CACHE:
        cached = PLACE_CACHE[place_id]
        return {
            "location": cached.get("location") if need_geom else None,
            "reviews":  cached.get("reviews", []) if need_reviews else [],
            "photo_refs": cached.get("photo_refs", []) if need_photos else []
        }

    fields = build_fields(need_geom, need_reviews, need_photos)
    if not fields:
        return {"location": None, "reviews": [], "photo_refs": []}

    partial = fetch_place_details_partial(place_id, fields)
    stats["api_calls"] += 1

    existing = PLACE_CACHE.get(place_id, {"location": None, "reviews": [], "photo_refs": []})
    if "geometry" in fields:
        existing["location"] = partial.get("location")
    if "reviews" in fields:
        existing["reviews"] = partial.get("reviews", [])
    if "photos" in fields:
        existing["photo_refs"] = partial.get("photo_refs", [])
    PLACE_CACHE[place_id] = existing

    return {
        "location": existing.get("location") if need_geom else None,
        "reviews":  existing.get("reviews", []) if need_reviews else [],
        "photo_refs": existing.get("photo_refs", []) if need_photos else []
    }

# ── Upload photo (MISSING earlier; now included) ──────────────────────────────
def download_and_upload_photo(bucket: fa_storage.bucket,
                              playlist_id: str,
                              place_id: str,
                              index: int,
                              photo_reference: str) -> Optional[str]:
    url = ("https://maps.googleapis.com/maps/api/place/photo"
           f"?maxwidth=800&photoreference={photo_reference}&key={GOOGLE_PLACES_API_KEY}")
    try:
        resp = stream_bytes_with_backoff(url)
        blob_path = f"playlistsPlaces/{playlist_id}/{place_id}/{index}.jpg"
        blob = bucket.blob(blob_path)
        blob.upload_from_file(resp.raw, content_type="image/jpeg")
        blob.make_public()
        return blob.public_url
    except Exception as e:
        print(f"      - Photo upload failed (#{index}) for {place_id}: {e}")
        return None

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_place_id_from_obj(obj: Dict[str, Any]) -> Optional[str]:
    return obj.get("placeId") or obj.get("_id") or obj.get("id")

def place_needs_any(p: Dict[str, Any]) -> Tuple[bool, bool, bool]:
    need_geom    = not ("latitude" in p and "longitude" in p)
    need_reviews = ("reviews" not in p) or (not p.get("reviews"))
    need_photos  = ("g_image_urls" not in p) or (not p.get("g_image_urls"))
    return need_geom, need_reviews, need_photos

def trim_reviews(reviews: List[Dict[str, Any]], limit: int = MAX_REVIEWS) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in reviews[:limit]:
        out.append({
            "author_name": r.get("author_name"),
            "rating": r.get("rating"),
            "text": r.get("text"),
            "relative_time_description": r.get("relative_time_description"),
            "time": r.get("time"),
            "profile_photo_url": r.get("profile_photo_url"),
        })
    return out

def detect_inline_places(data: Dict[str, Any]) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    sc = data.get("subcollections")
    if isinstance(sc, dict):
        cand = sc.get("places")
        if isinstance(cand, list):
            return "subcollections.places", cand
        for k, v in sc.items():
            if isinstance(v, list):
                return f"subcollections.{k}", v
    if isinstance(data.get("places"), list):
        return "places", data["places"]
    return None, []

# ── Inline enrichment ─────────────────────────────────────────────────────────
def enrich_inline_array(db: fa_fs.Client, bucket: fa_storage.bucket,
                        doc_ref: fa_fs.DocumentReference, pid: str,
                        path: str, places: List[Dict[str, Any]],
                        sleep_seconds: float, dry_run: bool,
                        totals: Dict[str, int]) -> Tuple[int, int]:
    changed = 0
    start_calls = totals["api_calls"]
    updated: List[Dict[str, Any]] = []

    for idx, place in enumerate(places, start=1):
        place_id = get_place_id_from_obj(place)
        if not place_id:
            updated.append(place); continue

        need_geom, need_reviews, need_photos = place_needs_any(place)
        if not (need_geom or need_reviews or need_photos):
            updated.append(place); continue

        print(f"  - [INLINE {idx:03d}/{len(places)}] {place.get('name') or place_id} "
              f"(geom={need_geom}, reviews={need_reviews}, photos={need_photos})")

        try:
            enrich = get_enrichment(place_id, need_geom, need_reviews, need_photos, totals)
        except Exception as e:
            print(f"    ⚠️ Details fetch failed: {e}")
            updated.append(place); continue

        if need_geom and enrich["location"]:
            place["latitude"]  = enrich["location"].get("lat")
            place["longitude"] = enrich["location"].get("lng")
        if need_reviews and enrich["reviews"]:
            place["reviews"] = trim_reviews(enrich["reviews"])
        if need_photos and enrich["photo_refs"]:
            urls: List[str] = []
            for i, ref in enumerate(enrich["photo_refs"][:MAX_PHOTOS], start=1):
                if dry_run:
                    urls.append(f"DRYRUN://{pid}/{place_id}/{i}.jpg")
                else:
                    url = download_and_upload_photo(bucket, pid, place_id, i, ref)
                    if url: urls.append(url)
            if urls:
                place["g_image_urls"] = urls

        updated.append(place)
        changed += 1
        time.sleep(sleep_seconds)

    if changed and not dry_run:
        print("✨ Committing INLINE update…")
        doc_ref.update({path: updated})
        print("✅ Inline array updated.")

    return changed, (totals["api_calls"] - start_calls)

# ── Sub-collection enrichment ─────────────────────────────────────────────────
def enrich_subcollection(db: fa_fs.Client, bucket: fa_storage.bucket,
                         doc_ref: fa_fs.DocumentReference, pid: str,
                         sleep_seconds: float, dry_run: bool,
                         totals: Dict[str, int]) -> Tuple[int, int]:
    try:
        docs = list(doc_ref.collection("places").stream())
    except DeadlineExceeded:
        print("   ⚠️ sub-collection list hit DeadlineExceeded; retrying…")
        docs = list(doc_ref.collection("places").stream())

    if not docs:
        return 0, 0

    changed = 0
    start_calls = totals["api_calls"]

    for i, pd in enumerate(docs, start=1):
        place = pd.to_dict() or {}
        place_id = get_place_id_from_obj(place)
        if not place_id:
            continue

        need_geom, need_reviews, need_photos = place_needs_any(place)
        if not (need_geom or need_reviews or need_photos):
            continue

        print(f"  - [SUBCOL {i:03d}/{len(docs)}] {place.get('name') or place_id} "
              f"(geom={need_geom}, reviews={need_reviews}, photos={need_photos})")

        try:
            enrich = get_enrichment(place_id, need_geom, need_reviews, need_photos, totals)
        except Exception as e:
            print(f"    ⚠️ Details fetch failed: {e}")
            continue

        if need_geom and enrich["location"]:
            place["latitude"]  = enrich["location"].get("lat")
            place["longitude"] = enrich["location"].get("lng")
        if need_reviews and enrich["reviews"]:
            place["reviews"] = trim_reviews(enrich["reviews"])
        if need_photos and enrich["photo_refs"]:
            urls: List[str] = []
            for j, ref in enumerate(enrich["photo_refs"][:MAX_PHOTOS], start=1):
                if dry_run:
                    urls.append(f"DRYRUN://{pid}/{place_id}/{j}.jpg")
                else:
                    url = download_and_upload_photo(bucket, pid, place_id, j, ref)
                    if url: urls.append(url)
            if urls:
                place["g_image_urls"] = urls

        if not dry_run:
            pd.reference.update(place)

        changed += 1
        time.sleep(sleep_seconds)

    return changed, (totals["api_calls"] - start_calls)

# ── Progress persistence ──────────────────────────────────────────────────────
def load_progress() -> Optional[str]:
    if not os.path.exists(PROGRESS_FILE):
        return None
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj.get("last_doc_id")
    except Exception:
        return None

def save_progress(last_doc_id: str) -> None:
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_doc_id": last_doc_id, "timestamp": time.time()}, f)

# ── Paged scan ────────────────────────────────────────────────────────────────
def iter_playlists_paged(db: fa_fs.Client,
                         start_after_id: Optional[str],
                         page_size: int,
                         limit_pages: Optional[int]):

    coll = db.collection(COLLECTION_NAME)
    last_snap = None

    if start_after_id:
        try:
            last_snap = coll.document(start_after_id).get()
            if not last_snap.exists:
                last_snap = None
        except Exception:
            last_snap = None

    pages_done = 0
    while True:
        q = coll.order_by("__name__").limit(page_size)
        if last_snap:
            q = q.start_after(last_snap)

        for attempt in range(1, 4):
            try:
                snaps = list(q.stream())
                break
            except DeadlineExceeded:
                print(f"⚠️ Page fetch DeadlineExceeded (attempt {attempt}); retrying…")
                _sleep_with_jitter(BACKOFF_BASE_SECS, attempt)
        else:
            raise RuntimeError("Repeated DeadlineExceeded while listing collection pages.")

        if not snaps:
            return

        yield snaps
        last_snap = snaps[-1]
        save_progress(last_snap.id)

        pages_done += 1
        if limit_pages and pages_done >= limit_pages:
            return

# ── MAIN ─────────────────────────────────────────────────────────────────────
def run(page_size: int, limit_pages: Optional[int], sleep_seconds: float,
        dry_run: bool, resume: bool) -> None:

    db, bucket = init_firebase()

    start_after_id = load_progress() if resume else None
    if start_after_id:
        print(f"⏩ Resuming after doc id: {start_after_id}")

    totals = {"playlists": 0, "inline_changed": 0, "subcol_changed": 0, "api_calls": 0}

    for page in iter_playlists_paged(db, start_after_id, page_size, limit_pages):
        for snap in page:
            pid = snap.id
            data = snap.to_dict() or {}
            title = data.get("title", pid)
            totals["playlists"] += 1

            print("\n==============================")
            print(f"🔎 Playlist: {title} ({pid})")

            path, places = detect_inline_places(data)
            inline_changed = subcol_changed = api_page_calls = 0

            if path and places:
                print(f"➡️  Found inline places at '{path}' ({len(places)} items).")
                c, calls = enrich_inline_array(db, bucket, snap.reference, pid, path, places, sleep_seconds, dry_run, totals)
                inline_changed += c
                api_page_calls += calls
            else:
                print("ℹ️ No inline places array; using sub-collection 'places' …")
                c, calls = enrich_subcollection(db, bucket, snap.reference, pid, sleep_seconds, dry_run, totals)
                subcol_changed += c
                api_page_calls += calls

            totals["inline_changed"] += inline_changed
            totals["subcol_changed"] += subcol_changed

            print(f"Summary for {pid}: inline_changed={inline_changed}, subcol_changed={subcol_changed}, api_calls(+{api_page_calls})")

    print("\n==============================")
    print("🏁 DONE")
    print(f"Playlists scanned: {totals['playlists']}")
    print(f"Inline places updated: {totals['inline_changed']}")
    print(f"Sub-collection places updated: {totals['subcol_changed']}")
    print(f"Google Places API calls (this run): {totals['api_calls']}")

# ── CLI ──────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Enrich all playlists with minimal API calls, pagination & resume.")
    ap.add_argument("--page_size", type=int, default=PAGE_SIZE, help="Docs per page (default 100).")
    ap.add_argument("--limit_pages", type=int, default=None, help="Process at most N pages.")
    ap.add_argument("--sleep", type=float, default=0.10, help="Seconds to sleep between API calls.")
    ap.add_argument("--dry_run", action="store_true", help="Do not write updates.")
    ap.add_argument("--resume", action="store_true", help="Resume from last saved progress.")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run(args.page_size, args.limit_pages, args.sleep, args.dry_run, args.resume)


# # enrich_playlists_all.py
# # Enriches ALL documents in 'playlistsNew' collection.
# # Supports both storage layouts:
# #   1) Inline array:   playlistsNew/<doc>.subcollections.places  (updates array)
# #   2) Sub-collection: playlistsNew/<doc>/places/*               (updates each doc)

# import argparse
# import time
# from typing import Any, Dict, List, Optional, Tuple

# import requests
# import firebase_admin
# from firebase_admin import credentials, firestore as fa_fs, storage as fa_storage

# # ─── CONFIG ────────────────────────────────────────────────────────────────────

# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# BUCKET_NAME          = "mycasavsc.appspot.com"
# COLLECTION_NAME      = "playlistsNew"

# # ⚠️ Hardcoded as requested
# GOOGLE_PLACES_API_KEY = "AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M"

# MAX_PHOTOS  = 3
# MAX_REVIEWS = 5

# HTTP_TIMEOUT_SECS = 15
# MAX_RETRIES       = 5
# BACKOFF_BASE_SECS = 0.8
# BACKOFF_JITTER    = 0.25

# FIELDS = "geometry,photos,reviews"

# # ─── INIT ──────────────────────────────────────────────────────────────────────

# def init_firebase() -> tuple[fa_fs.Client, fa_storage.bucket]:
#     if not firebase_admin._apps:
#         cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
#         firebase_admin.initialize_app(cred, {"storageBucket": BUCKET_NAME})
#     db = fa_fs.client()
#     bucket = fa_storage.bucket(BUCKET_NAME)
#     return db, bucket

# # ─── BACKOFF HELPERS ───────────────────────────────────────────────────────────

# def _sleep_with_jitter(base: float, attempt: int) -> None:
#     import random
#     time.sleep((base * (2 ** (attempt - 1))) + random.uniform(0, BACKOFF_JITTER))

# def get_json_with_backoff(url: str) -> Dict[str, Any]:
#     last_exc: Optional[Exception] = None
#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             r = requests.get(url, timeout=HTTP_TIMEOUT_SECS)
#             if r.status_code in (429, 500, 502, 503, 504):
#                 _sleep_with_jitter(BACKOFF_BASE_SECS, attempt); continue
#             r.raise_for_status()
#             return r.json()
#         except requests.RequestException as e:
#             last_exc = e
#             _sleep_with_jitter(BACKOFF_BASE_SECS, attempt)
#     raise RuntimeError(f"HTTP request failed after {MAX_RETRIES} retries: {last_exc}")

# def stream_bytes_with_backoff(url: str) -> requests.Response:
#     last_exc: Optional[Exception] = None
#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             r = requests.get(url, timeout=HTTP_TIMEOUT_SECS, stream=True)
#             if r.status_code in (429, 500, 502, 503, 504):
#                 _sleep_with_jitter(BACKOFF_BASE_SECS, attempt); continue
#             r.raise_for_status()
#             return r
#         except requests.RequestException as e:
#             last_exc = e
#             _sleep_with_jitter(BACKOFF_BASE_SECS, attempt)
#     raise RuntimeError(f"Photo download failed after {MAX_RETRIES} retries: {last_exc}")

# # ─── GOOGLE PLACES ─────────────────────────────────────────────────────────────

# def fetch_place_details(place_id: str) -> Tuple[Optional[Dict[str, float]], List[Dict[str, Any]], List[str]]:
#     url = ("https://maps.googleapis.com/maps/api/place/details/json"
#            f"?place_id={place_id}&fields={FIELDS}&key={GOOGLE_PLACES_API_KEY}")
#     data = get_json_with_backoff(url)
#     status = data.get("status")
#     if status != "OK":
#         raise RuntimeError(f"Places Details status={status} place_id={place_id} msg={data.get('error_message')}")
#     result = data.get("result", {})
#     location = (result.get("geometry") or {}).get("location")
#     reviews  = result.get("reviews", []) or []
#     photos   = result.get("photos", []) or []
#     photo_refs = [p.get("photo_reference") for p in photos[:MAX_PHOTOS] if p.get("photo_reference")]
#     return location, reviews, photo_refs

# def download_and_upload_photo(bucket: fa_storage.bucket,
#                               playlist_id: str,
#                               place_id: str,
#                               index: int,
#                               photo_reference: str) -> Optional[str]:
#     url = ("https://maps.googleapis.com/maps/api/place/photo"
#            f"?maxwidth=800&photoreference={photo_reference}&key={GOOGLE_PLACES_API_KEY}")
#     try:
#         resp = stream_bytes_with_backoff(url)
#         blob_path = f"playlistsPlaces/{playlist_id}/{place_id}/{index}.jpg"
#         blob = bucket.blob(blob_path)
#         blob.upload_from_file(resp.raw, content_type="image/jpeg")
#         blob.make_public()
#         return blob.public_url
#     except Exception as e:
#         print(f"      - Photo upload failed (#{index}) for {place_id}: {e}")
#         return None

# # ─── HELPERS ──────────────────────────────────────────────────────────────────

# def get_place_id_from_obj(obj: Dict[str, Any]) -> Optional[str]:
#     return obj.get("placeId") or obj.get("_id") or obj.get("id")

# def place_needs_enrichment(p: Dict[str, Any]) -> bool:
#     return ("g_image_urls" not in p) or ("latitude" not in p) or ("longitude" not in p) or ("reviews" not in p)

# def trim_reviews(reviews: List[Dict[str, Any]], limit: int = MAX_REVIEWS) -> List[Dict[str, Any]]:
#     out: List[Dict[str, Any]] = []
#     for r in reviews[:limit]:
#         out.append({
#             "author_name": r.get("author_name"),
#             "rating": r.get("rating"),
#             "text": r.get("text"),
#             "relative_time_description": r.get("relative_time_description"),
#             "time": r.get("time"),
#             "profile_photo_url": r.get("profile_photo_url"),
#         })
#     return out

# def detect_inline_places(data: Dict[str, Any]) -> Tuple[Optional[str], List[Dict[str, Any]]]:
#     sc = data.get("subcollections")
#     if isinstance(sc, dict):
#         cand = sc.get("places")
#         if isinstance(cand, list):
#             return "subcollections.places", cand
#         # fallback to any list under subcollections/*
#         for k, v in sc.items():
#             if isinstance(v, list):
#                 return f"subcollections.{k}", v
#     if isinstance(data.get("places"), list):
#         return "places", data["places"]
#     return None, []

# # ─── ENRICH INLINE ARRAY ──────────────────────────────────────────────────────

# def enrich_inline_array(db: fa_fs.Client, bucket: fa_storage.bucket,
#                         doc_ref: fa_fs.DocumentReference, pid: str,
#                         path: str, places: List[Dict[str, Any]],
#                         sleep_seconds: float, dry_run: bool) -> Tuple[int, int]:
#     changed = 0
#     total_calls = 0
#     updated: List[Dict[str, Any]] = []

#     for idx, place in enumerate(places, start=1):
#         place_id = get_place_id_from_obj(place)
#         if not place_id:
#             updated.append(place); continue
#         if not place_needs_enrichment(place):
#             updated.append(place); continue

#         print(f"  - [INLINE {idx:03d}/{len(places)}] {place.get('name') or place_id}")
#         try:
#             location, reviews, photo_refs = fetch_place_details(place_id)
#             total_calls += 1
#         except Exception as e:
#             print(f"    ⚠️ Details fetch failed: {e}")
#             updated.append(place); continue

#         if location:
#             place["latitude"]  = location.get("lat")
#             place["longitude"] = location.get("lng")
#         if reviews:
#             place["reviews"] = trim_reviews(reviews)
#         if photo_refs:
#             urls: List[str] = []
#             for i, ref in enumerate(photo_refs[:MAX_PHOTOS], start=1):
#                 if dry_run:
#                     urls.append(f"DRYRUN://{pid}/{place_id}/{i}.jpg")
#                 else:
#                     url = download_and_upload_photo(bucket, pid, place_id, i, ref)
#                     if url: urls.append(url)
#             if urls:
#                 place["g_image_urls"] = urls

#         updated.append(place)
#         changed += 1
#         time.sleep(sleep_seconds)

#     if changed and not dry_run:
#         print("✨ Committing INLINE update…")
#         doc_ref.update({path: updated})
#         print("✅ Inline array updated.")

#     return changed, total_calls

# # ─── ENRICH SUB-COLLECTION ────────────────────────────────────────────────────

# def enrich_subcollection(db: fa_fs.Client, bucket: fa_storage.bucket,
#                          doc_ref: fa_fs.DocumentReference, pid: str,
#                          sleep_seconds: float, dry_run: bool) -> Tuple[int, int]:
#     places_coll = doc_ref.collection("places")
#     docs = list(places_coll.stream())
#     if not docs:
#         return 0, 0

#     changed = 0
#     total_calls = 0

#     for i, pd in enumerate(docs, start=1):
#         place = pd.to_dict() or {}
#         place_id = get_place_id_from_obj(place)
#         if not place_id:
#             continue
#         if not place_needs_enrichment(place):
#             continue

#         print(f"  - [SUBCOL {i:03d}/{len(docs)}] {place.get('name') or place_id}")
#         try:
#             location, reviews, photo_refs = fetch_place_details(place_id)
#             total_calls += 1
#         except Exception as e:
#             print(f"    ⚠️ Details fetch failed: {e}")
#             continue

#         if location:
#             place["latitude"]  = location.get("lat")
#             place["longitude"] = location.get("lng")
#         if reviews:
#             place["reviews"] = trim_reviews(reviews)
#         if photo_refs:
#             urls: List[str] = []
#             for j, ref in enumerate(photo_refs[:MAX_PHOTOS], start=1):
#                 if dry_run:
#                     urls.append(f"DRYRUN://{pid}/{place_id}/{j}.jpg")
#                 else:
#                     url = download_and_upload_photo(bucket, pid, place_id, j, ref)
#                     if url: urls.append(url)
#             if urls:
#                 place["g_image_urls"] = urls

#         if not dry_run:
#             pd.reference.update(place)
#         changed += 1
#         time.sleep(sleep_seconds)

#     return changed, total_calls

# # ─── MAIN ─────────────────────────────────────────────────────────────────────

# def run(limit: Optional[int], sleep_seconds: float, dry_run: bool) -> None:
#     db, bucket = init_firebase()

#     snaps = db.collection(COLLECTION_NAME).stream()
#     processed = 0
#     totals = {
#         "playlists": 0,
#         "inline_changed": 0,
#         "subcol_changed": 0,
#         "api_calls": 0,
#     }

#     for snap in snaps:
#         pid = snap.id
#         data = snap.to_dict() or {}
#         title = data.get("title", pid)
#         totals["playlists"] += 1
#         processed += 1

#         if limit and processed > limit:
#             break

#         print(f"\n==============================")
#         print(f"🔎 Playlist: {title} ({pid})")
#         path, places = detect_inline_places(data)

#         inline_changed = subcol_changed = api_calls = 0

#         if path and places:
#             print(f"➡️  Found inline places at '{path}' ({len(places)} items).")
#             changed, calls = enrich_inline_array(db, bucket, snap.reference, pid, path, places, sleep_seconds, dry_run)
#             inline_changed += changed
#             api_calls += calls
#         else:
#             print("ℹ️ No inline places array; trying sub-collection 'places' …")
#             changed, calls = enrich_subcollection(db, bucket, snap.reference, pid, sleep_seconds, dry_run)
#             subcol_changed += changed
#             api_calls += calls

#         totals["inline_changed"]  += inline_changed
#         totals["subcol_changed"]  += subcol_changed
#         totals["api_calls"]       += api_calls

#         print(f"Summary for {pid}: inline_changed={inline_changed}, subcol_changed={subcol_changed}, api_calls={api_calls}")

#     print("\n==============================")
#     print("🏁 DONE")
#     print(f"Playlists processed: {totals['playlists']}")
#     print(f"Inline places updated: {totals['inline_changed']}")
#     print(f"Sub-collection places updated: {totals['subcol_changed']}")
#     print(f"Google Places API calls: {totals['api_calls']}")
#     if dry_run:
#         print("Mode: DRY RUN (no writes performed)")

# # ─── CLI ──────────────────────────────────────────────────────────────────────

# def parse_args() -> argparse.Namespace:
#     ap = argparse.ArgumentParser(description="Enrich all playlists in 'playlistsNew' (inline arrays and/or sub-collections).")
#     ap.add_argument("--limit", type=int, default=None, help="Process at most N playlists.")
#     ap.add_argument("--sleep", type=float, default=0.10, help="Seconds to sleep between API calls.")
#     ap.add_argument("--dry_run", action="store_true", help="Do not write updates (for testing).")
#     return ap.parse_args()

# if __name__ == "__main__":
#     args = parse_args()
#     run(args.limit, args.sleep, args.dry_run)









# import os
# import io
# import time
# import requests
# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.cloud import storage

# # ─── CONFIG ────────────────────────────────────────────────────────────────────
# SERVICE_ACCOUNT_JSON = 'C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json'
# FIRESTORE_PLAYLISTS   = 'playlistsNew'
# GOOGLE_API_KEY        = 'AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M'
# STORAGE_BUCKET_NAME   = 'mycasavsc.appspot.com'
# MAX_PHOTOS            = 3
# MAX_REVIEWS           = 5

# # ─── INIT ──────────────────────────────────────────────────────────────────────
# cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
# firebase_admin.initialize_app(cred, {'storageBucket': STORAGE_BUCKET_NAME})
# db     = firestore.client()
# bucket = storage.Client().bucket(STORAGE_BUCKET_NAME)

# # ─── HELPERS ───────────────────────────────────────────────────────────────────
# def fetch_place_details(place_id: str) -> dict:
#     """Call Google Places Details API for geometry, photos, reviews."""
#     url = (
#         'https://maps.googleapis.com/maps/api/place/details/json'
#         f'?place_id={place_id}'
#         f'&fields=geometry,photos,reviews'
#         f'&key={GOOGLE_API_KEY}'
#     )
#     resp = requests.get(url, timeout=5)
#     resp.raise_for_status()
#     result = resp.json().get('result', {})
#     return {
#         'location': result.get('geometry', {}).get('location', {}),
#         'photo_refs': [p['photo_reference'] for p in result.get('photos', [])],
#         'reviews': result.get('reviews', [])[:MAX_REVIEWS]
#     }

# def download_photo(photo_ref: str) -> bytes:
#     """Download a photo from Google Places Photo endpoint."""
#     url = (
#         'https://maps.googleapis.com/maps/api/place/photo'
#         f'?maxwidth=800'
#         f'&photoreference={photo_ref}'
#         f'&key={GOOGLE_API_KEY}'
#     )
#     resp = requests.get(url, timeout=5, allow_redirects=True)
#     resp.raise_for_status()
#     return resp.content

# def upload_to_storage(data: bytes, dest_path: str) -> str:
#     """Upload binary to Cloud Storage and return its public URL."""
#     blob = bucket.blob(dest_path)
#     blob.upload_from_string(data, content_type='image/jpeg')
#     blob.make_public()
#     return blob.public_url

# # ─── MAIN MIGRATION ────────────────────────────────────────────────────────────
# def enrich_playlists():
#     coll = db.collection(FIRESTORE_PLAYLISTS)
#     for pl_doc in coll.stream():
#         pl = pl_doc.to_dict()
#         playlist_id = pl_doc.id
#         places = pl.get('subcollections', {}).get('places', [])
#         if not places:
#             continue

#         print(f"[{playlist_id}] Enriching {len(places)} places…")
#         enriched = []

#         for p in places:
#             pid = p.get('_id')
#             if not pid:
#                 enriched.append(p); continue

#             try:
#                 details = fetch_place_details(pid)
#             except Exception as e:
#                 print(f"  ⚠️ Place {pid}: details API failed: {e}")
#                 enriched.append(p); continue

#             # geometry
#             p['latitude']  = details['location'].get('lat')
#             p['longitude'] = details['location'].get('lng')

#             # reviews
#             p['reviews'] = details['reviews']

#             # photos: download & reupload up to MAX_PHOTOS
#             new_urls = []
#             for idx, ref in enumerate(details['photo_refs'][:MAX_PHOTOS], start=1):
#                 try:
#                     img = download_photo(ref)
#                     path = f"playlistsPlaces/{playlist_id}/{pid}/{idx}.jpg"
#                     url  = upload_to_storage(img, path)
#                     new_urls.append(url)
#                     time.sleep(0.1)  # throttle
#                 except Exception as e:
#                     print(f"    ❌ photo#{idx} for {pid} failed: {e}")

#             if new_urls:
#                 p['g_image_urls'] = new_urls

#             enriched.append(p)

#         # write back
#         pl_doc.reference.update({
#             'subcollections.places': enriched
#         })
#         print(f"✅ Updated playlist {playlist_id}")

# if __name__ == '__main__':
#     enrich_playlists()
