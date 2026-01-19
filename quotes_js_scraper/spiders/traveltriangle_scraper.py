# # tt_blog_to_playlist.py
# # TravelTriangle blog → Firestore-ready "playlist" + places
# # Dry-run: full Google data, no uploads, g_image_urls stays [] and imageUrl = null

# import os
# import re
# import json
# import time
# import math
# import random
# import argparse
# import hashlib
# from pathlib import Path
# from typing import Dict, Any, List, Optional, Tuple

# import requests
# from bs4 import BeautifulSoup

# SOURCE_TAG = "traveltriangle"
# SUBTYPE_TAG = "poi"
# CATEGORY_DEFAULT = "Travel"

# CITY_ID_MAP = {
#     # "Ahmedabad": "123",
# }

# GCS_BUCKET_DEFAULT = os.getenv("GCS_BUCKET", "mycasavsc.appspot.com")
# IMAGE_BASE = "https://storage.googleapis.com/{bucket}/playlistsNew_images/{list_id}/1.jpg"
# G_IMAGE_TEMPLATE = "https://storage.googleapis.com/{bucket}/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
# G_IMAGE_COUNT = 3

# def slugify(s: str) -> str:
#     s = (s or "").lower().strip()
#     s = re.sub(r"[^\w\s-]", "", s)
#     s = re.sub(r"[\s_-]+", "-", s)
#     return re.sub(r"^-+|-+$", "", s)

# def build_unique_slug(title: str, city: str, subtype: str, source_url: str) -> str:
#     base = f"{slugify(title)}-{slugify(city)}-{subtype}"
#     h = hashlib.md5(source_url.encode("utf-8")).hexdigest()[:6]
#     return f"{base}-{h}"

# def clean_txt(s: Optional[str]) -> str:
#     if not s:
#         return ""
#     s = s.replace("\u2019", "'").replace("\u2014", "-")
#     s = re.sub(r"\s+", " ", s).strip()
#     return s

# def strip_number_prefix(s: str) -> str:
#     # e.g. "1. Something" / "10) Place" → "Something" / "Place"
#     s = re.sub(r"^\s*\d+[\.\)]\s*", "", s or "")
#     return re.sub(r"\s+", " ", s).strip()

# # ---------- Google Places ----------

# def gp_find_place(name: str, city: str, api_key: Optional[str], location_hint: Optional[str] = None) -> Optional[str]:
#     if not api_key:
#         return None
#     base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
#     fields = "place_id"
#     queries = []
#     if location_hint:
#         queries.append(f"{name}, {location_hint}, {city}".strip(", "))
#     queries.append(f"{name}, {city}".strip(", "))
#     queries.append(name)
#     for q in queries:
#         try:
#             r = requests.get(base_url, params={
#                 "input": q, "inputtype": "textquery", "fields": fields, "key": api_key
#             }, timeout=15)
#             js = r.json()
#             cands = js.get("candidates") or []
#             if cands:
#                 return cands[0].get("place_id")
#         except Exception:
#             continue
#     return None

# def gp_place_details(place_id: str, api_key: Optional[str]) -> Dict[str, Any]:
#     if not (api_key and place_id):
#         return {}
#     url = "https://maps.googleapis.com/maps/api/place/details/json"
#     fields = ",".join([
#         "rating",
#         "user_ratings_total",
#         "formatted_address",
#         "geometry/location",
#         "website",
#         "opening_hours/weekday_text",
#         "price_level",
#         "utc_offset_minutes",
#         "photos",
#         "reviews"
#     ])
#     params = {"place_id": place_id, "fields": fields, "key": api_key}
#     try:
#         r = requests.get(url, params=params, timeout=20)
#         return r.json().get("result", {}) or {}
#     except Exception:
#         return {}

# def gp_photo_bytes(photo_ref: str, api_key: str, maxwidth: int = 1600) -> Optional[bytes]:
#     try:
#         url = "https://maps.googleapis.com/maps/api/place/photo"
#         params = {"maxwidth": str(maxwidth), "photo_reference": photo_ref, "key": api_key}
#         resp = requests.get(url, params=params, timeout=30, allow_redirects=True)
#         resp.raise_for_status()
#         return resp.content
#     except Exception:
#         return None

# # ---------- GCS & Firestore (used only when publishing) ----------

# def gcs_upload_bytes(storage_client, bucket_name: str, blob_path: str, data: bytes,
#                      content_type: str = "image/jpeg", make_public: bool = True) -> str:
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(blob_path)
#     blob.cache_control = "public, max-age=31536000"
#     blob.upload_from_string(data, content_type=content_type)
#     if make_public:
#         try:
#             blob.make_public()
#         except Exception:
#             pass
#     return f"https://storage.googleapis.com/{bucket_name}/{blob_path}"

# class FirestoreIdAssigner:
#     def __init__(self, collection: str, project: Optional[str] = None):
#         from google.cloud import firestore as _fs
#         self.client = _fs.Client(project=project) if project else _fs.Client()
#         self.col = self.client.collection(collection)
#         self.next_id = self._compute_next()
#     def _compute_next(self) -> int:
#         max_id = 0
#         try:
#             for d in self.col.select([]).stream():
#                 try:
#                     v = int(d.id)
#                     if v > max_id: max_id = v
#                 except ValueError:
#                     continue
#         except Exception:
#             pass
#         return max_id + 1
#     def assign(self, slug: str) -> Tuple[str, bool]:
#         try:
#             try:
#                 from google.cloud.firestore_v1 import FieldFilter
#                 q = self.col.where(filter=FieldFilter("slug", "==", slug)).limit(1)
#             except Exception:
#                 q = self.col.where("slug", "==", slug).limit(1)
#             docs = list(q.stream())
#             if docs:
#                 return docs[0].id, True
#         except Exception:
#             pass
#         nid = str(self.next_id); self.next_id += 1
#         return nid, False

# def publish_playlist(collection: str, list_id: str, playlist_doc: Dict[str, Any],
#                      places_docs: List[Dict[str, Any]], project: Optional[str]) -> None:
#     from google.cloud import firestore as _fs
#     db = _fs.Client(project=project) if project else _fs.Client()
#     col = db.collection(collection)
#     doc = col.document(list_id)
#     body = dict(playlist_doc)
#     body.pop("subcollections", None)
#     doc.set(body, merge=False)
#     sub = doc.collection("places")
#     olds = list(sub.stream())
#     for i in range(0, len(olds), 400):
#         batch = db.batch()
#         for d in olds[i:i+400]:
#             batch.delete(d.reference)
#         batch.commit()
#     for i in range(0, len(places_docs), 400):
#         batch = db.batch()
#         for it in places_docs[i:i+400]:
#             sid = it.get("placeId") or it.get("_id")
#             batch.set(sub.document(str(sid)), it)
#         batch.commit()

# # ---------- TravelTriangle parsing ----------

# def parse_tt_article(html: str) -> Dict[str, Any]:
#     soup = BeautifulSoup(html, "lxml")

#     h2 = soup.select_one("h2.h2_Waypoints_blogpage")
#     playlist_title = clean_txt(h2.get_text()) if h2 else ""

#     playlist_desc = ""
#     if h2:
#         nxt = h2.find_next_sibling()
#         while nxt and nxt.name != "p":
#             nxt = nxt.find_next_sibling()
#         if nxt and nxt.name == "p":
#             playlist_desc = clean_txt(nxt.get_text())

#     places: List[Dict[str, Any]] = []
#     for h3 in soup.select("h3"):
#         name = strip_number_prefix(h3.get_text())
#         if not name or len(name) < 2:
#             continue
#         desc = ""
#         details_p = None
#         p = h3.find_next_sibling()
#         first_p_taken = False
#         while p and p.name in ("p", "div"):
#             if p.name == "p" and not first_p_taken:
#                 desc = clean_txt(p.get_text())
#                 first_p_taken = True
#             if p.name == "p" and (p.find("strong")):
#                 details_p = p
#                 break
#             p = p.find_next_sibling()

#         details = {"location": "", "opening_hours": "", "entry_fee": ""}
#         if details_p:
#             for br in details_p.find_all("br"):
#                 br.replace_with("\n")
#             txt = details_p.get_text("\n", strip=True)
#             m_loc = re.search(r"(?i)\bLocation:\s*(.+)", txt)
#             m_hrs = re.search(r"(?i)\bOpening\s*hours?:\s*(.+)", txt)
#             m_fee = re.search(r"(?i)\bEntry\s*fee:\s*(.+)", txt)
#             if m_loc: details["location"] = clean_txt(m_loc.group(1))
#             if m_hrs: details["opening_hours"] = clean_txt(m_hrs.group(1))
#             if m_fee: details["entry_fee"] = clean_txt(m_fee.group(1))

#         places.append({"name": name, "desc": desc, "details": details})

#     return {
#         "playlist_title": playlist_title,
#         "playlist_description": playlist_desc,
#         "places": places
#     }

# # ---------- Scoring & trimming ----------

# def score_item(it: Dict[str, Any]) -> float:
#     rating = float(it.get("rating") or 0.0)
#     num = float(it.get("numRatings") or 0.0)
#     desc_bonus = 0.2 if it.get("generalDescription") else 0.0
#     vol = math.log10(max(1.0, num + 1.0))
#     return 0.6 * rating + 0.3 * vol + 0.1 * desc_bonus

# def trim_and_light_shuffle(items: List[Dict[str, Any]], keep_ratio: float = 0.7,
#                            seed: int = 42, max_displacement: int = 2) -> List[Dict[str, Any]]:
#     rng = random.Random(seed)
#     n = len(items)
#     if n == 0:
#         return []
#     k = max(1, int(math.ceil(n * keep_ratio)))
#     ranked = sorted(items, key=score_item, reverse=True)[:k]
#     for i in range(len(ranked)):
#         j = min(len(ranked) - 1, max(0, i + rng.randint(-max_displacement, max_displacement)))
#         if i != j:
#             ranked[i], ranked[j] = ranked[j], ranked[i]
#     for idx, it in enumerate(ranked, start=1):
#         it["index"] = idx
#     return ranked

# # ---------- Main ----------

# def main():
#     ap = argparse.ArgumentParser("TravelTriangle blog → Playlist (Firestore-ready)")
#     ap.add_argument("--url", required=True, help="TravelTriangle blog URL")
#     ap.add_argument("--city", required=True, help="City/region name (for search + metadata)")
#     ap.add_argument("--category", default=CATEGORY_DEFAULT, help="Top-level category (default: Travel)")
#     ap.add_argument("--out-dir", default="tt_out", help="Folder to write local JSON artifacts")
#     ap.add_argument("--keep-ratio", type=float, default=0.7, help="Fraction to keep after trimming (0..1)")
#     ap.add_argument("--min-items", type=int, default=7, help="Publish only if >= this many places remain")
#     ap.add_argument("--publish", action="store_true", help="Publish to Firestore")
#     ap.add_argument("--dry-run", action="store_true",
#                     help="Simulate publish: no Firestore/GCS writes; still fetch Google data; g_image_urls stays []")
#     ap.add_argument("--collection", default="playlistsNew", help="Firestore collection")
#     ap.add_argument("--project", default=os.getenv("GOOGLE_CLOUD_PROJECT"), help="GCP project id")
#     ap.add_argument("--bucket", default=GCS_BUCKET_DEFAULT, help="GCS bucket for images")
#     ap.add_argument("--max-photos", type=int, default=G_IMAGE_COUNT, help="Max photos per place (used if publishing)")
#     ap.add_argument("--skip-photos", action="store_true", help="Skip photo upload step (when publishing)")
#     args = ap.parse_args()

#     out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)

#     # Fetch page
#     ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
#           "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
#     resp = requests.get(args.url, headers={"User-Agent": ua}, timeout=30)
#     resp.raise_for_status()

#     parsed = parse_tt_article(resp.text)
#     soup = BeautifulSoup(resp.text, "lxml")
#     playlist_title = parsed["playlist_title"] or clean_txt(soup.title.get_text() if soup.title else "") or "TravelTriangle Picks"
#     playlist_desc = parsed["playlist_description"] or f"Places featured in: {playlist_title}"

#     slug = build_unique_slug(playlist_title, args.city, SUBTYPE_TAG, args.url)
#     list_id = slug  # string id by default

#     # IMPORTANT: in DRY-RUN, imageUrl should be null (no uploads happen)
#     image_url_value = None if args.dry_run else IMAGE_BASE.format(bucket=args.bucket, list_id=list_id)

#     playlist_doc = {
#         "list_id": str(list_id),
#         "imageUrl": image_url_value,
#         "description": playlist_desc,
#         "source_urls": [args.url],
#         "source": SOURCE_TAG,
#         "category": args.category,
#         "title": playlist_title,
#         "city_id": CITY_ID_MAP.get(args.city, args.city),
#         "subtype": SUBTYPE_TAG,
#         "city": args.city,
#         "created_ts": int(time.time()),
#         "slug": slug
#     }

#     # Enrich places with Google (reviews included); g_image_urls remains []
#     api_key = "AIzaSyBe-suda095R60l0G1NrdYeIVKjUCxwK_8"
#     places_raw: List[Dict[str, Any]] = []
#     for p in parsed["places"]:
#         name = p["name"]
#         desc = p["desc"]
#         details = p["details"]
#         place_id = gp_find_place(name, args.city, api_key, location_hint=details.get("location")) if api_key else None
#         det = gp_place_details(place_id, api_key) if (api_key and place_id) else {}

#         loc = (det.get("geometry") or {}).get("location") or {}
#         weekday_text = (det.get("opening_hours") or {}).get("weekday_text") or []
#         raw_reviews = det.get("reviews") or []
#         reviews = []
#         for r in raw_reviews[:5]:
#             reviews.append({
#                 "rating": int(r.get("rating") or 0),
#                 "text": (r.get("text") or "").strip(),
#                 "author_name": r.get("author_name") or "",
#                 "relative_time_description": r.get("relative_time_description") or "",
#                 "time": int(r.get("time") or 0),
#                 "profile_photo_url": r.get("profile_photo_url") or ""
#             })

#         places_raw.append({
#             "_id": place_id or name,
#             "placeId": place_id,
#             "name": name,                            # from page
#             "generalDescription": desc or None,      # from page
#             "address": det.get("formatted_address") or None,
#             "website": det.get("website"),
#             "rating": det.get("rating") or 0,
#             "numRatings": det.get("user_ratings_total") or 0,
#             "priceLevel": det.get("price_level"),
#             "utcOffset": det.get("utc_offset_minutes"),
#             "openingPeriods": weekday_text,
#             "latitude": loc.get("lat"),
#             "longitude": loc.get("lng"),
#             "categories": [],
#             "imageKeys": [],
#             "g_image_urls": [],                      # stays empty in dry-run
#             "permanentlyClosed": False,
#             "reviews": reviews,
#             "ratingDistribution": {},                # keep shape like reference
#             "minMinutesSpent": None,
#             "maxMinutesSpent": None,
#             "internationalPhoneNumber": None,
#             "tripadvisorRating": 0,
#             "tripadvisorNumRatings": 0,
#             # index filled after trimming
#             "tt_extra": {
#                 "location_hint": details.get("location") or "",
#                 "opening_hours_text": details.get("opening_hours") or "",
#                 "entry_fee_text": details.get("entry_fee") or ""
#             }
#         })

#     # Trim & index
#     places_docs = trim_and_light_shuffle(places_raw, keep_ratio=args.keep_ratio)

#     # Write complete local JSON (this is your dry-run deliverable too)
#     local = dict(playlist_doc)
#     local["subcollections"] = {"places": places_docs}
#     base = f"{slugify(args.city)}_{slugify(playlist_title)[:50]}"
#     out_file = out_dir / f"{base}.json"
#     out_file.write_text(json.dumps(local, ensure_ascii=False, indent=2), encoding="utf-8")
#     print(f"📝 Wrote local playlist JSON → {out_file} (kept {len(places_docs)}/{len(places_raw)})")

#     # Optional: a small dry-run report (kept; no uploads planned here)
#     if args.dry_run:
#         dryrun = {
#             "url": args.url,
#             "city": args.city,
#             "slug": slug,
#             "kept": len(places_docs),
#             "min_items": args.min_items,
#             "can_publish": bool(len(places_docs) >= args.min_items),
#             "max_photos": args.max_photos
#         }
#         dryrun_file = out_dir / f"{base}.dryrun.json"
#         dryrun_file.write_text(json.dumps(dryrun, ensure_ascii=False, indent=2), encoding="utf-8")
#         print("🔎 DRY RUN — full Google data included; no uploads; g_image_urls left empty.")
#         print(f"   Kept {dryrun['kept']} items; "
#               f"{'✅ would publish' if dryrun['can_publish'] else '⛔ would NOT publish'} "
#               f"(min-items={args.min_items}).")
#         print(f"   Dry-run report → {dryrun_file}")
#         return

#     # --- Real publish path (unchanged from earlier) ---
#     if args.publish and len(places_docs) >= args.min_items:
#         from google.cloud import storage as _storage
#         assigner = FirestoreIdAssigner(collection=args.collection, project=args.project)
#         numeric_id, existed = assigner.assign(playlist_doc["slug"])
#         playlist_doc["list_id"] = numeric_id

#         cover_url = None
#         if not args.skip_photos and os.getenv("GOOGLE_MAPS_API_KEY"):
#             storage_client = _storage.Client()
#             cover_done = False
#             for place in places_docs:
#                 pid = place.get("placeId")
#                 if not pid:
#                     continue
#                 d2 = gp_place_details(pid, os.getenv("GOOGLE_MAPS_API_KEY"))
#                 photos = [p.get("photo_reference") for p in (d2.get("photos") or []) if p.get("photo_reference")]
#                 photos = photos[:args.max_photos]

#                 uploaded = 0
#                 for i, pref in enumerate(photos, start=1):
#                     data = gp_photo_bytes(pref, os.getenv("GOOGLE_MAPS_API_KEY"))
#                     if not data:
#                         continue
#                     blob = f"playlistsPlaces/{numeric_id}/{pid}/{i}.jpg"
#                     gcs_upload_bytes(storage_client, args.bucket, blob, data)
#                     uploaded += 1
#                     if not cover_done and i == 1:
#                         cover_blob = f"playlistsNew_images/{numeric_id}/1.jpg"
#                         gcs_upload_bytes(storage_client, args.bucket, cover_blob, data)
#                         cover_url = IMAGE_BASE.format(bucket=args.bucket, list_id=numeric_id)
#                         cover_done = True
#                 if uploaded:
#                     place["g_image_urls"] = [
#                         G_IMAGE_TEMPLATE.format(bucket=args.bucket, list_id=numeric_id, placeId=pid, n=n)
#                         for n in range(1, uploaded + 1)
#                     ]
#         if cover_url:
#             playlist_doc["imageUrl"] = cover_url

#         publish_playlist(args.collection, str(numeric_id), playlist_doc, places_docs, args.project)
#         print(f"✅ Published to Firestore '{args.collection}' as ID {numeric_id}")
#     elif args.publish:
#         print(f"⛔ Not publishing: kept {len(places_docs)} < --min-items {args.min_items}")
#     else:
#         print("ℹ️ Publish disabled (use --publish to write into Firestore).")

# if __name__ == "__main__":
#     main()


import os
import re
import json
import time
import math
import random
import argparse
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, NamedTuple

import requests
from bs4 import BeautifulSoup

SOURCE_TAG = "traveltriangle"
SUBTYPE_TAG = "poi"
CATEGORY_DEFAULT = "Travel"

CITY_ID_MAP = {
    # "Ahmedabad": "123",
}

GCS_BUCKET_DEFAULT = os.getenv("GCS_BUCKET", "mycasavsc.appspot.com")
IMAGE_BASE = "https://storage.googleapis.com/{bucket}/playlistsNew_images/{list_id}/1.jpg"
G_IMAGE_TEMPLATE = "https://storage.googleapis.com/{bucket}/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
G_IMAGE_COUNT = 3

# ===================== AI CONFIG =====================
OPENAI_API_KEY =  "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"
OPENAI_TEXT_MODEL = "gpt-4o-mini"

# Title parameters
MIN_TITLE_LENGTH = 2
MAX_TITLE_LENGTH = 5

# Description parameters
DESCRIPTION_MIN_LENGTH = 60
DESCRIPTION_MAX_LENGTH = 110
DESCRIPTION_TARGET = 90
# ==================================================


class TitleResult(NamedTuple):
    """Result from title generation"""
    simple_title: str
    catchy_title: str
    confidence: float


class DescriptionResult(NamedTuple):
    """Result from description generation"""
    description: str
    alt: str
    style: str
    confidence: float


def slugify(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_-]+", "-", s)
    return re.sub(r"^-+|-+$", "", s)

def build_unique_slug(title: str, city: str, subtype: str, source_url: str) -> str:
    base = f"{slugify(title)}-{slugify(city)}-{subtype}"
    h = hashlib.md5(source_url.encode("utf-8")).hexdigest()[:6]
    return f"{base}-{h}"

def clean_txt(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("\u2019", "'").replace("\u2014", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def strip_number_prefix(s: str) -> str:
    # e.g. "1. Something" / "10) Place" → "Something" / "Place"
    s = re.sub(r"^\s*\d+[\.\)]\s*", "", s or "")
    return re.sub(r"\s+", " ", s).strip()


# ===================== AI TITLE OPTIMIZATION =====================

def remove_numbers_from_title(title: str) -> str:
    """
    Remove numbers from titles while keeping the rest natural.
    
    Examples:
        "Top 27 Haridwar Getaways" → "Top Haridwar Getaways"
        "Best 45 Mumbai Cafes" → "Best Mumbai Cafes"
        "30 Things to Do in Delhi" → "Things to Do in Delhi"
    """
    patterns = [
        r'\b\d+\b\s*',           # Any standalone number with optional space
        r'^\d+\s+',              # Number at start
        r'\s+\d+\s+',            # Number in middle
    ]
    
    result = title
    for pattern in patterns:
        result = re.sub(pattern, ' ', result)
    
    # Clean up multiple spaces
    result = ' '.join(result.split())
    
    return result.strip()


def build_title_system_prompt() -> str:
    """Build system prompt for title optimization"""
    return """You are a travel content editor specializing in creating catchy, memorable playlist titles.

Your mission: Transform titles into short, punchy alternatives that are:
- 2-4 words maximum (3 words is ideal)
- Catchy and memorable
- Easy to say and remember
- Natural sounding
- Travel-focused

REQUIREMENTS:
✓ Keep it SHORT (2-4 words)
✓ Make it catchy and memorable
✓ Include the city/place name if present
✓ Use simple, powerful words
✓ Make it feel curated and exclusive

STYLE EXAMPLES:
"Mumbai Eats" (not "Best Places to Eat in Mumbai")
"Delhi Gems" (not "Top Hidden Gems in Delhi")
"Goa Vibes" (not "Best Things to Do in Goa")
"Jaipur Escapes" (not "Top Tourist Destinations in Jaipur")
"Kolkata Culture" (not "Cultural Experiences in Kolkata")

✗ Avoid generic words like "Guide", "List", "Collection"
✗ Don't use numbers
✗ Don't be too formal or long"""


def build_title_user_prompt(original: str, simplified: str, city: str, country: str) -> str:
    """Build user prompt for title generation"""
    
    prompt_parts = [
        f'ORIGINAL TITLE: "{original}"',
        f'SIMPLIFIED (numbers removed): "{simplified}"',
        f'City: {city or "Unknown"}',
        f'Country: {country or "Unknown"}',
        f'',
        f'TASK:',
        f'Create a catchy 2-4 word title that:',
        f'1. Is shorter and punchier than the simplified version',
        f'2. Captures the essence of the playlist',
        f'3. Includes the city/place name if relevant',
        f'4. Is memorable and shareable',
        f'',
        f'STYLE TO MATCH:',
        f'- "Mumbai Eats" (short, direct, catchy)',
        f'- "Delhi Gems" (implies curation)',
        f'- "Goa Escapes" (evokes feeling)',
        f'- "Jaipur Culture" (focused topic)',
        f'',
        f'Return JSON with:',
        f'{{',
        f'  "catchy_title": "Your 2-4 word catchy title",',
        f'  "confidence": 0.9  // How confident (0.0-1.0)',
        f'}}',
        f'',
        f'Make it punchy and memorable!',
    ]
    
    return '\n'.join(prompt_parts)


def optimize_title_with_ai(
    original_title: str,
    city: str,
    country: str,
    use_catchy: bool = True,
    auto_mode: bool = False
) -> TitleResult:
    """
    Optimize title using AI or simple number removal.
    
    Args:
        original_title: The scraped title
        city: City name
        country: Country name
        use_catchy: If True, use AI-generated catchy title; else use simplified
        auto_mode: If True, skip AI and just remove numbers
    
    Returns TitleResult with simple_title, catchy_title, and confidence.
    """
    # First, remove numbers
    simple_title = remove_numbers_from_title(original_title)
    
    # If auto mode or no API key, just return simplified
    if auto_mode or not OPENAI_API_KEY:
        return TitleResult(
            simple_title=simple_title,
            catchy_title=simple_title,
            confidence=0.5
        )
    
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        system_prompt = build_title_system_prompt()
        user_prompt = build_title_user_prompt(original_title, simple_title, city, country)
        
        print(f"      🤖 Calling OpenAI API...", end=" ", flush=True)
        
        response = client.chat.completions.create(
            model=OPENAI_TEXT_MODEL,
            temperature=0.7,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=100,
            response_format={"type": "json_object"},
            timeout=30
        )
        
        print(f"✅")
        
        result_text = response.choices[0].message.content.strip()
        data = json.loads(result_text)
        
        catchy = (data.get("catchy_title") or simple_title).strip()
        confidence = float(data.get("confidence", 0.8))
        
        # Validate: must be 2-5 words
        word_count = len(catchy.split())
        if word_count < MIN_TITLE_LENGTH or word_count > MAX_TITLE_LENGTH:
            print(f"      ⚠️  Title word count ({word_count}) out of range, using simplified")
            catchy = simple_title
            confidence = 0.6
        
        return TitleResult(
            simple_title=simple_title,
            catchy_title=catchy,
            confidence=confidence
        )
        
    except ImportError as e:
        print(f"\n      ❌ OpenAI library not installed: {e}")
        print(f"      💡 Install with: pip install openai")
        return TitleResult(
            simple_title=simple_title,
            catchy_title=simple_title,
            confidence=0.5
        )
    except Exception as e:
        print(f"\n      ⚠️  AI title generation failed: {e}, using simplified")
        return TitleResult(
            simple_title=simple_title,
            catchy_title=simple_title,
            confidence=0.5
        )


# ===================== AI DESCRIPTION OPTIMIZATION =====================

def build_description_system_prompt(lang: str, tone: str, max_chars: int) -> str:
    """Build system prompt for creative, engaging descriptions"""
    return f"""You are a creative travel content writer specializing in engaging, Instagram-worthy descriptions that inspire wanderlust.

Your mission: Write catchy, concise descriptions that make destinations irresistible.

REQUIREMENTS:
- Language: {lang}
- Tone: {tone} (but always exciting and inviting)
- Target length: {max_chars} characters (±20 chars is fine)
- Style: Instagram-worthy, sensory, vivid
- Focus: What makes this experience special and worth sharing

GUIDELINES:
✓ Use vivid, sensory language that paints a picture
✓ Be specific about what users will experience
✓ Include hooks or clever wordplay when appropriate
✓ Make it shareable and quotable
✓ Focus on feelings and experiences, not just facts
✗ Avoid clichés like "must-see", "don't miss", "hidden gem" (unless creative)
✗ No emojis or hashtags
✗ Don't be generic or boring

Think: Would someone want to screenshot this and share it?"""


def build_description_user_prompt(
    title: str, 
    city: str, 
    country: str, 
    existing: Optional[str]
) -> str:
    """Build user prompt with examples and context"""
    
    examples = [
        "We've curated the best Instagrammable places to help you build a picture-perfect feed!",
        "Matcha made in heaven! From creamy lattes to dreamy desserts, these spots are every matcha lover's go to",
        "Hidden gems waiting to be discovered! Experience the authentic soul through local eyes",
        "From sunrise to sunset, these spots capture the magic that makes this place unforgettable",
        "Where locals eat, drink, and gather. No tourist traps—just authentic experiences",
    ]
    
    prompt_parts = [
        f'PLAYLIST DETAILS:',
        f'- Title: "{title}"',
        f'- City: {city or "Various"}',
        f'- Country: {country or "Various"}',
        f'',
        f'INSPIRATION (match this style and energy):',
    ]
    
    for i, ex in enumerate(examples, 1):
        prompt_parts.append(f'{i}. "{ex}"')
    
    if existing:
        prompt_parts.extend([
            f'',
            f'CURRENT DESCRIPTION (improve on this):',
            f'"{existing}"',
        ])
    
    prompt_parts.extend([
        f'',
        f'TASK:',
        f'Create an engaging description that:',
        f'1. Captures what makes this playlist special',
        f'2. Makes people excited to explore',
        f'3. Is quotable and shareable',
        f'4. Mentions the city/destination naturally',
        f'5. Focuses on the experience, not just places',
        f'',
        f'Return JSON with:',
        f'{{',
        f'  "description": "The main description ({DESCRIPTION_MIN_LENGTH}-{DESCRIPTION_MAX_LENGTH} chars)",',
        f'  "alt": "Optional shorter version (max 90 chars) for meta/preview",',
        f'  "style": "Style/theme used (e.g., Foodie, Adventure, Instagram-worthy)",',
        f'  "confidence": 0.8  // How confident you are (0.0-1.0)',
        f'}}',
        f'',
        f'Make it irresistible! 🎯',
    ])
    
    return '\n'.join(prompt_parts)


def generate_fallback_description(title: str, city: str, country: str) -> str:
    """Generate simple fallback description if AI fails completely"""
    if city and country:
        return f"Discover the best of {city}, {country}! Handpicked spots and experiences that capture what makes this destination special."
    elif city:
        return f"Explore {city} through carefully curated locations that showcase the city's unique character and charm."
    elif country:
        return f"Experience the essence of {country} with our collection of must-visit places and unforgettable moments."
    else:
        return f"A curated collection of amazing places and experiences. {title}"


def optimize_description_with_ai(
    title: str,
    city: str,
    country: str,
    existing: Optional[str],
    lang: str = "en",
    tone: str = "friendly",
    max_chars: int = DESCRIPTION_TARGET
) -> DescriptionResult:
    """
    Generate engaging description using OpenAI.
    
    Returns DescriptionResult with description, alt, style, and confidence.
    """
    if not OPENAI_API_KEY:
        fallback = generate_fallback_description(title, city, country)
        return DescriptionResult(
            description=fallback,
            alt="",
            style="Fallback",
            confidence=0.5
        )
    
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        system_prompt = build_description_system_prompt(lang, tone, max_chars)
        user_prompt = build_description_user_prompt(title, city, country, existing)
        
        print(f"      🤖 Calling OpenAI API...", end=" ", flush=True)
        
        response = client.chat.completions.create(
            model=OPENAI_TEXT_MODEL,
            temperature=0.75,  # Higher for creativity
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=400,
            response_format={"type": "json_object"},
            timeout=30
        )
        
        print(f"✅")
        
        result_text = response.choices[0].message.content.strip()
        
        # Parse JSON (with fallback for markdown blocks)
        if result_text.startswith("```"):
            chunks = result_text.split("```")
            if len(chunks) >= 3:
                result_text = chunks[1].replace("json", "").strip()
        
        data = json.loads(result_text)
        
        # Extract and validate
        description = (data.get("description") or "").strip()
        alt = (data.get("alt") or "").strip()
        style = (data.get("style") or "General").strip()
        confidence = float(data.get("confidence", 0.8))
        
        # Clean up (remove multiple spaces, newlines)
        description = " ".join(description.split())
        alt = " ".join(alt.split())
        
        # Validate length
        if len(description) < DESCRIPTION_MIN_LENGTH:
            print(f"      ⚠️  Description too short ({len(description)} chars), using fallback")
            description = generate_fallback_description(title, city, country)
            confidence = 0.5
            style = "Fallback"
        elif len(description) > DESCRIPTION_MAX_LENGTH + 20:
            # Truncate if way too long
            description = description[:DESCRIPTION_MAX_LENGTH] + "..."
            
        return DescriptionResult(
            description=description,
            alt=alt,
            style=style,
            confidence=confidence
        )
        
    except json.JSONDecodeError as e:
        print(f"\n      ⚠️  JSON parse error: {e}")
        return DescriptionResult(
            description=result_text[:DESCRIPTION_MAX_LENGTH] if result_text else generate_fallback_description(title, city, country),
            alt="",
            style="Raw",
            confidence=0.6
        )
    except ImportError as e:
        print(f"\n      ❌ OpenAI library not installed: {e}")
        print(f"      💡 Install with: pip install openai")
        fallback = generate_fallback_description(title, city, country)
        return DescriptionResult(
            description=fallback,
            alt="",
            style="Fallback",
            confidence=0.5
        )
    except Exception as e:
        print(f"\n      ⚠️  API error: {e}")
        fallback = generate_fallback_description(title, city, country)
        return DescriptionResult(
            description=fallback,
            alt="",
            style="Fallback",
            confidence=0.3
        )


# ---------- Google Places ----------

def gp_find_place(name: str, city: str, api_key: Optional[str], location_hint: Optional[str] = None) -> Optional[str]:
    if not api_key:
        return None
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    fields = "place_id"
    queries = []
    if location_hint:
        queries.append(f"{name}, {location_hint}, {city}".strip(", "))
    queries.append(f"{name}, {city}".strip(", "))
    queries.append(name)
    for q in queries:
        try:
            r = requests.get(base_url, params={
                "input": q, "inputtype": "textquery", "fields": fields, "key": api_key
            }, timeout=15)
            js = r.json()
            cands = js.get("candidates") or []
            if cands:
                return cands[0].get("place_id")
        except Exception:
            continue
    return None

def gp_place_details(place_id: str, api_key: Optional[str]) -> Dict[str, Any]:
    if not (api_key and place_id):
        return {}
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = ",".join([
        "rating",
        "user_ratings_total",
        "formatted_address",
        "geometry/location",
        "website",
        "opening_hours/weekday_text",
        "price_level",
        "utc_offset_minutes",
        "photos",
        "reviews"
    ])
    params = {"place_id": place_id, "fields": fields, "key": api_key}
    try:
        r = requests.get(url, params=params, timeout=20)
        return r.json().get("result", {}) or {}
    except Exception:
        return {}

def gp_photo_bytes(photo_ref: str, api_key: str, maxwidth: int = 1600) -> Optional[bytes]:
    try:
        url = "https://maps.googleapis.com/maps/api/place/photo"
        params = {"maxwidth": str(maxwidth), "photo_reference": photo_ref, "key": api_key}
        resp = requests.get(url, params=params, timeout=30, allow_redirects=True)
        resp.raise_for_status()
        return resp.content
    except Exception:
        return None

# ---------- GCS & Firestore (used only when publishing) ----------

def gcs_upload_bytes(storage_client, bucket_name: str, blob_path: str, data: bytes,
                     content_type: str = "image/jpeg", make_public: bool = True) -> str:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.cache_control = "public, max-age=31536000"
    blob.upload_from_string(data, content_type=content_type)
    if make_public:
        try:
            blob.make_public()
        except Exception:
            pass
    return f"https://storage.googleapis.com/{bucket_name}/{blob_path}"

class FirestoreIdAssigner:
    def __init__(self, collection: str, project: Optional[str] = None):
        from google.cloud import firestore as _fs
        self.client = _fs.Client(project=project) if project else _fs.Client()
        self.col = self.client.collection(collection)
        self.next_id = self._compute_next()
    def _compute_next(self) -> int:
        max_id = 0
        try:
            for d in self.col.select([]).stream():
                try:
                    v = int(d.id)
                    if v > max_id: max_id = v
                except ValueError:
                    continue
        except Exception:
            pass
        return max_id + 1
    def assign(self, slug: str) -> Tuple[str, bool]:
        try:
            try:
                from google.cloud.firestore_v1 import FieldFilter
                q = self.col.where(filter=FieldFilter("slug", "==", slug)).limit(1)
            except Exception:
                q = self.col.where("slug", "==", slug).limit(1)
            docs = list(q.stream())
            if docs:
                return docs[0].id, True
        except Exception:
            pass
        nid = str(self.next_id); self.next_id += 1
        return nid, False

def publish_playlist(collection: str, list_id: str, playlist_doc: Dict[str, Any],
                     places_docs: List[Dict[str, Any]], project: Optional[str]) -> None:
    from google.cloud import firestore as _fs
    db = _fs.Client(project=project) if project else _fs.Client()
    col = db.collection(collection)
    doc = col.document(list_id)
    body = dict(playlist_doc)
    body.pop("subcollections", None)
    doc.set(body, merge=False)
    sub = doc.collection("places")
    olds = list(sub.stream())
    for i in range(0, len(olds), 400):
        batch = db.batch()
        for d in olds[i:i+400]:
            batch.delete(d.reference)
        batch.commit()
    for i in range(0, len(places_docs), 400):
        batch = db.batch()
        for it in places_docs[i:i+400]:
            sid = it.get("placeId") or it.get("_id")
            batch.set(sub.document(str(sid)), it)
        batch.commit()

# ---------- TravelTriangle parsing ----------

def parse_tt_article(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")

    h2 = soup.select_one("h2.h2_Waypoints_blogpage")
    playlist_title = clean_txt(h2.get_text()) if h2 else ""

    playlist_desc = ""
    if h2:
        nxt = h2.find_next_sibling()
        while nxt and nxt.name != "p":
            nxt = nxt.find_next_sibling()
        if nxt and nxt.name == "p":
            playlist_desc = clean_txt(nxt.get_text())

    places: List[Dict[str, Any]] = []
    for h3 in soup.select("h3"):
        name = strip_number_prefix(h3.get_text())
        if not name or len(name) < 2:
            continue
        desc = ""
        details_p = None
        p = h3.find_next_sibling()
        first_p_taken = False
        while p and p.name in ("p", "div"):
            if p.name == "p" and not first_p_taken:
                desc = clean_txt(p.get_text())
                first_p_taken = True
            if p.name == "p" and (p.find("strong")):
                details_p = p
                break
            p = p.find_next_sibling()

        details = {"location": "", "opening_hours": "", "entry_fee": ""}
        if details_p:
            for br in details_p.find_all("br"):
                br.replace_with("\n")
            txt = details_p.get_text("\n", strip=True)
            m_loc = re.search(r"(?i)\bLocation:\s*(.+)", txt)
            m_hrs = re.search(r"(?i)\bOpening\s*hours?:\s*(.+)", txt)
            m_fee = re.search(r"(?i)\bEntry\s*fee:\s*(.+)", txt)
            if m_loc: details["location"] = clean_txt(m_loc.group(1))
            if m_hrs: details["opening_hours"] = clean_txt(m_hrs.group(1))
            if m_fee: details["entry_fee"] = clean_txt(m_fee.group(1))

        places.append({"name": name, "desc": desc, "details": details})

    return {
        "playlist_title": playlist_title,
        "playlist_description": playlist_desc,
        "places": places
    }

# ---------- Scoring & trimming ----------

def score_item(it: Dict[str, Any]) -> float:
    rating = float(it.get("rating") or 0.0)
    num = float(it.get("numRatings") or 0.0)
    desc_bonus = 0.2 if it.get("generalDescription") else 0.0
    vol = math.log10(max(1.0, num + 1.0))
    return 0.6 * rating + 0.3 * vol + 0.1 * desc_bonus

def trim_and_light_shuffle(items: List[Dict[str, Any]], keep_ratio: float = 0.7,
                           seed: int = 42, max_displacement: int = 2) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    n = len(items)
    if n == 0:
        return []
    k = max(1, int(math.ceil(n * keep_ratio)))
    ranked = sorted(items, key=score_item, reverse=True)[:k]
    for i in range(len(ranked)):
        j = min(len(ranked) - 1, max(0, i + rng.randint(-max_displacement, max_displacement)))
        if i != j:
            ranked[i], ranked[j] = ranked[j], ranked[i]
    for idx, it in enumerate(ranked, start=1):
        it["index"] = idx
    return ranked

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser("TravelTriangle blog → Playlist (Firestore-ready) with AI optimization")
    ap.add_argument("--url", required=True, help="TravelTriangle blog URL")
    ap.add_argument("--city", required=True, help="City/region name (for search + metadata)")
    ap.add_argument("--country", default="", help="Country name (for AI context)")
    ap.add_argument("--category", default=CATEGORY_DEFAULT, help="Top-level category (default: Travel)")
    ap.add_argument("--out-dir", default="tt_out", help="Folder to write local JSON artifacts")
    ap.add_argument("--keep-ratio", type=float, default=0.7, help="Fraction to keep after trimming (0..1)")
    ap.add_argument("--min-items", type=int, default=7, help="Publish only if >= this many places remain")
    
    # AI optimization options
    ap.add_argument("--optimize-ai", action="store_true", help="Use AI to optimize title & description")
    ap.add_argument("--title-mode", choices=["simple", "catchy"], default="catchy",
                    help="Title mode: 'simple' (just remove numbers) or 'catchy' (AI-generated)")
    ap.add_argument("--desc-tone", default="friendly", 
                    choices=["friendly", "editorial", "playful", "luxury", "adventurous", "foodie", "cultural"],
                    help="Tone for description generation")
    
    ap.add_argument("--publish", action="store_true", help="Publish to Firestore")
    ap.add_argument("--dry-run", action="store_true",
                    help="Simulate publish: no Firestore/GCS writes; still fetch Google data; g_image_urls stays []")
    ap.add_argument("--collection", default="playlistsNew", help="Firestore collection")
    ap.add_argument("--project", default=os.getenv("GOOGLE_CLOUD_PROJECT"), help="GCP project id")
    ap.add_argument("--bucket", default=GCS_BUCKET_DEFAULT, help="GCS bucket for images")
    ap.add_argument("--max-photos", type=int, default=G_IMAGE_COUNT, help="Max photos per place (used if publishing)")
    ap.add_argument("--skip-photos", action="store_true", help="Skip photo upload step (when publishing)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    # Fetch page
    print(f"\n🌐 Fetching {args.url}...")
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
    resp = requests.get(args.url, headers={"User-Agent": ua}, timeout=30)
    resp.raise_for_status()

    parsed = parse_tt_article(resp.text)
    soup = BeautifulSoup(resp.text, "lxml")
    
    # Get raw title and description
    raw_title = parsed["playlist_title"] or clean_txt(soup.title.get_text() if soup.title else "") or "TravelTriangle Picks"
    raw_description = parsed["playlist_description"] or f"Places featured in: {raw_title}"

    print(f"📝 Raw title: {raw_title}")
    print(f"📝 Raw description: {raw_description[:100]}...")

    # ===== AI OPTIMIZATION =====
    if args.optimize_ai:
        print(f"\n🤖 AI OPTIMIZATION ENABLED")
        print(f"   Mode: Title={args.title_mode}, Description tone={args.desc_tone}")
        
        # Optimize title
        print(f"\n✂️  Optimizing title...")
        title_result = optimize_title_with_ai(
            original_title=raw_title,
            city=args.city,
            country=args.country,
            use_catchy=(args.title_mode == "catchy"),
            auto_mode=False
        )
        
        if args.title_mode == "catchy":
            final_title = title_result.catchy_title
            print(f"   ✨ Catchy title: {final_title}")
        else:
            final_title = title_result.simple_title
            print(f"   ✂️  Simplified title: {final_title}")
        print(f"   📊 Confidence: {title_result.confidence:.2f}")
        
        # Optimize description
        print(f"\n💬 Optimizing description...")
        desc_result = optimize_description_with_ai(
            title=final_title,
            city=args.city,
            country=args.country,
            existing=raw_description,
            lang="en",
            tone=args.desc_tone,
            max_chars=DESCRIPTION_TARGET
        )
        
        final_description = desc_result.description
        print(f"   ✨ Style: {desc_result.style}")
        print(f"   📝 Description ({len(final_description)} chars):")
        print(f"      {final_description}")
        print(f"   📊 Confidence: {desc_result.confidence:.2f}")
        
    else:
        print(f"\n⏭️  AI optimization disabled (use --optimize-ai to enable)")
        final_title = raw_title
        final_description = raw_description

    # Build slug and list_id
    slug = build_unique_slug(final_title, args.city, SUBTYPE_TAG, args.url)
    list_id = slug

    # IMPORTANT: in DRY-RUN, imageUrl should be null (no uploads happen)
    image_url_value = None if args.dry_run else IMAGE_BASE.format(bucket=args.bucket, list_id=list_id)

    playlist_doc = {
        "list_id": str(list_id),
        "imageUrl": image_url_value,
        "description": final_description,
        "source_urls": [args.url],
        "source": SOURCE_TAG,
        "category": args.category,
        "title": final_title,
        "city_id": CITY_ID_MAP.get(args.city, args.city),
        "subtype": SUBTYPE_TAG,
        "city": args.city,
        "created_ts": int(time.time()),
        "slug": slug
    }
    
    if args.country:
        playlist_doc["country"] = args.country

    # Enrich places with Google (reviews included); g_image_urls remains []
    print(f"\n🔍 Enriching {len(parsed['places'])} places with Google Places data...")
    api_key = "AIzaSyAtfCzxVca4ngbJEr4CcvbI02KItlxtKfw"
    places_raw: List[Dict[str, Any]] = []
    for idx, p in enumerate(parsed["places"], 1):
        name = p["name"]
        desc = p["desc"]
        details = p["details"]
        
        print(f"   [{idx}/{len(parsed['places'])}] {name}...", end=" ")
        
        place_id = gp_find_place(name, args.city, api_key, location_hint=details.get("location")) if api_key else None
        det = gp_place_details(place_id, api_key) if (api_key and place_id) else {}

        if place_id:
            print(f"✅")
        else:
            print(f"⚠️  (no Place ID)")

        loc = (det.get("geometry") or {}).get("location") or {}
        weekday_text = (det.get("opening_hours") or {}).get("weekday_text") or []
        raw_reviews = det.get("reviews") or []
        reviews = []
        for r in raw_reviews[:5]:
            reviews.append({
                "rating": int(r.get("rating") or 0),
                "text": (r.get("text") or "").strip(),
                "author_name": r.get("author_name") or "",
                "relative_time_description": r.get("relative_time_description") or "",
                "time": int(r.get("time") or 0),
                "profile_photo_url": r.get("profile_photo_url") or ""
            })

        places_raw.append({
            "_id": place_id or name,
            "placeId": place_id,
            "name": name,
            "generalDescription": desc or None,
            "address": det.get("formatted_address") or None,
            "website": det.get("website"),
            "rating": det.get("rating") or 0,
            "numRatings": det.get("user_ratings_total") or 0,
            "priceLevel": det.get("price_level"),
            "utcOffset": det.get("utc_offset_minutes"),
            "openingPeriods": weekday_text,
            "latitude": loc.get("lat"),
            "longitude": loc.get("lng"),
            "categories": [],
            "imageKeys": [],
            "g_image_urls": [],
            "permanentlyClosed": False,
            "reviews": reviews,
            "ratingDistribution": {},
            "minMinutesSpent": None,
            "maxMinutesSpent": None,
            "internationalPhoneNumber": None,
            "tripadvisorRating": 0,
            "tripadvisorNumRatings": 0,
            "tt_extra": {
                "location_hint": details.get("location") or "",
                "opening_hours_text": details.get("opening_hours") or "",
                "entry_fee_text": details.get("entry_fee") or ""
            }
        })

    # Trim & index
    print(f"\n🎯 Trimming and scoring places (keep_ratio={args.keep_ratio})...")
    places_docs = trim_and_light_shuffle(places_raw, keep_ratio=args.keep_ratio)
    print(f"   Kept {len(places_docs)}/{len(places_raw)} places")

    # Write complete local JSON (this is your dry-run deliverable too)
    local = dict(playlist_doc)
    local["subcollections"] = {"places": places_docs}
    
    # Add AI metadata if used
    if args.optimize_ai:
        local["ai_metadata"] = {
            "title_mode": args.title_mode,
            "title_confidence": title_result.confidence,
            "desc_tone": args.desc_tone,
            "desc_style": desc_result.style,
            "desc_confidence": desc_result.confidence,
            "raw_title": raw_title,
            "raw_description": raw_description
        }
    
    base = f"{slugify(args.city)}_{slugify(final_title)[:50]}"
    out_file = out_dir / f"{base}.json"
    out_file.write_text(json.dumps(local, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n📝 Wrote local playlist JSON → {out_file}")

    # Optional: a small dry-run report
    if args.dry_run:
        dryrun = {
            "url": args.url,
            "city": args.city,
            "slug": slug,
            "kept": len(places_docs),
            "min_items": args.min_items,
            "can_publish": bool(len(places_docs) >= args.min_items),
            "max_photos": args.max_photos,
            "ai_optimized": args.optimize_ai
        }
        dryrun_file = out_dir / f"{base}.dryrun.json"
        dryrun_file.write_text(json.dumps(dryrun, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n🔎 DRY RUN — full Google data included; no uploads; g_image_urls left empty.")
        print(f"   Kept {dryrun['kept']} items; "
              f"{'✅ would publish' if dryrun['can_publish'] else '⛔ would NOT publish'} "
              f"(min-items={args.min_items}).")
        print(f"   Dry-run report → {dryrun_file}")
        return

    # --- Real publish path ---
    if args.publish and len(places_docs) >= args.min_items:
        from google.cloud import storage as _storage
        print(f"\n🚀 PUBLISHING TO FIRESTORE...")
        
        assigner = FirestoreIdAssigner(collection=args.collection, project=args.project)
        numeric_id, existed = assigner.assign(playlist_doc["slug"])
        playlist_doc["list_id"] = numeric_id
        print(f"   Assigned ID: {numeric_id} {'(existed)' if existed else '(new)'}")

        cover_url = None
        if not args.skip_photos and os.getenv("GOOGLE_MAPS_API_KEY"):
            print(f"   📸 Uploading photos...")
            storage_client = _storage.Client()
            cover_done = False
            for place in places_docs:
                pid = place.get("placeId")
                if not pid:
                    continue
                d2 = gp_place_details(pid, os.getenv("GOOGLE_MAPS_API_KEY"))
                photos = [p.get("photo_reference") for p in (d2.get("photos") or []) if p.get("photo_reference")]
                photos = photos[:args.max_photos]

                uploaded = 0
                for i, pref in enumerate(photos, start=1):
                    data = gp_photo_bytes(pref, os.getenv("GOOGLE_MAPS_API_KEY"))
                    if not data:
                        continue
                    blob = f"playlistsPlaces/{numeric_id}/{pid}/{i}.jpg"
                    gcs_upload_bytes(storage_client, args.bucket, blob, data)
                    uploaded += 1
                    if not cover_done and i == 1:
                        cover_blob = f"playlistsNew_images/{numeric_id}/1.jpg"
                        gcs_upload_bytes(storage_client, args.bucket, cover_blob, data)
                        cover_url = IMAGE_BASE.format(bucket=args.bucket, list_id=numeric_id)
                        cover_done = True
                if uploaded:
                    place["g_image_urls"] = [
                        G_IMAGE_TEMPLATE.format(bucket=args.bucket, list_id=numeric_id, placeId=pid, n=n)
                        for n in range(1, uploaded + 1)
                    ]
            print(f"      Uploaded photos for playlist")
            
        if cover_url:
            playlist_doc["imageUrl"] = cover_url

        publish_playlist(args.collection, str(numeric_id), playlist_doc, places_docs, args.project)
        print(f"   ✅ Published to Firestore '{args.collection}' as ID {numeric_id}")
        
    elif args.publish:
        print(f"⛔ Not publishing: kept {len(places_docs)} < --min-items {args.min_items}")
    else:
        print("\nℹ️  Publish disabled (use --publish to write into Firestore).")

if __name__ == "__main__":
    main()


