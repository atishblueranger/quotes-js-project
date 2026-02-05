#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 5 — Upload to Firestore + Storage

- Reads: run_dir/04_final_itinerary.json
- Writes: run_dir/05_upload_report.json
- Creates:
  itineraries/{itineraryId}
  itineraries/{itineraryId}/placeMetadata/{placeId}
  itineraries/{itineraryId}/sections/{sectionId}

Uploads:
  - up to N photos per place (Google Place Photo), else static map fallback
  - cover image: reuse first place image
"""

import argparse
import os, time
from pathlib import Path
from typing import Any, Dict, List, Optional

from itinerary_utils import load_json, save_json, slugify

# Optional deps
try:
    import requests
except Exception:
    requests = None

try:
    import firebase_admin
    from firebase_admin import credentials, firestore, storage
except Exception:
    firebase_admin = None
    credentials = None
    firestore = None
    storage = None

def ensure_firebase(service_account_path: str, bucket_name: str):
    if not firebase_admin:
        raise RuntimeError("firebase-admin not installed. pip install firebase-admin")
    if not firebase_admin._apps:
        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred, {"storageBucket": bucket_name})
    db = firestore.client()
    bucket = storage.bucket(bucket_name)
    return db, bucket

def google_photo_url(api_key: str, photo_ref: str, max_width: int = 1600) -> str:
    return (
        "https://maps.googleapis.com/maps/api/place/photo"
        f"?maxwidth={max_width}&photo_reference={photo_ref}&key={api_key}"
    )

def static_map_url(api_key: str, lat: float, lng: float, zoom: int = 12, size: str = "1600x900") -> str:
    return (
        "https://maps.googleapis.com/maps/api/staticmap"
        f"?center={lat},{lng}&zoom={zoom}&size={size}&markers={lat},{lng}"
        f"&scale=2&maptype=roadmap&key={api_key}"
    )

def upload_bytes(bucket, path: str, content: bytes, content_type: str = "image/jpeg") -> str:
    blob = bucket.blob(path)
    blob.cache_control = "public, max-age=31536000"
    blob.upload_from_string(content, content_type=content_type)
    try:
        blob.patch()
    except Exception:
        pass
    return f"https://storage.googleapis.com/{bucket.name}/{path}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-items", type=int, default=8, help="Skip upload if fewer than this")
    ap.add_argument("--photos-per-place", type=int, default=1)
    ap.add_argument("--itinerary-id", default="", help="Override Firestore doc id")
    ap.add_argument("--collection", default="itineraries")
    ap.add_argument("--bucket", default="", help="Override bucket name")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    data = load_json(run_dir / "04_final_itinerary.json", default={})
    if not data:
        raise SystemExit("Missing 04_final_itinerary.json")

    places = data.get("places") or []
    if len(places) < args.min_items:
        print(f"⚠️ Skipping upload: only {len(places)} places (min {args.min_items})")
        save_json(run_dir / "05_upload_report.json", {"skipped": True, "reason": "low_item_count"})
        return

    google_key = os.getenv("GOOGLE_MAPS_API_KEY") or ""
    sa_path = os.getenv("FIREBASE_SERVICE_ACCOUNT") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or ""
    bucket_name = args.bucket or os.getenv("FIREBASE_BUCKET") or ""

    if not args.dry_run:
        if not sa_path or not Path(sa_path).exists():
            raise RuntimeError("Missing FIREBASE_SERVICE_ACCOUNT (or GOOGLE_APPLICATION_CREDENTIALS) path.")
        if not bucket_name:
            raise RuntimeError("Missing FIREBASE_BUCKET (e.g., myproject.appspot.com).")
        if not google_key:
            raise RuntimeError("Missing GOOGLE_MAPS_API_KEY (needed for photos/static map).")

        db, bucket = ensure_firebase(sa_path, bucket_name)
    else:
        db, bucket = None, None

    city = data.get("city", "")
    country = data.get("country", "")
    title = data.get("title", "")
    created_ts = int(time.time())

    # Build itineraryId
    itinerary_id = args.itinerary_id.strip()
    if not itinerary_id:
        itinerary_id = f"planup_{slugify(city)}_1day"

    parent_doc = {
        "itineraryId": itinerary_id,
        "city_id": str(data.get("city_id") or ""),   # optional; fill later if you map cities
        "city": city,
        "country": country,
        "type": "native",
        "source": "planup",
        "source_id": data.get("run_id"),
        "url": None,
        "title": title,
        "days": 1,
        "summary": data.get("summary", ""),
        "cover_image_url": None,
        "tags": data.get("tags", []),
        "section": "PlanUp Picks",
        "category": "Travel",
        "featured": False,
        "rank_manual": 100,
        "popularity_score": 0,
        "language": "en",
        "status": "active",
        "createdAt": firestore.SERVER_TIMESTAMP if not args.dry_run else created_ts,
        "updatedAt": firestore.SERVER_TIMESTAMP if not args.dry_run else created_ts,
        "total_places": 8,
        "split": data.get("split", {"sightseeing": 5, "eating": 3}),
        "route_stats": data.get("route_stats", {}),
        "llm_match_percentage": (data.get("llm_comparison") or {}).get("match_percentage"),
        "source_urls": data.get("source_urls", []),
    }

    report = {"itineraryId": itinerary_id, "uploaded_places": 0, "place_images": {}, "cover": None}

    # Upload place images
    for p in places:
        pid = p.get("place_id")
        if not pid:
            continue

        image_urls: List[str] = []
        if not args.dry_run and bucket and requests:
            # Try google photo refs
            refs = (p.get("photo_refs") or [])[: max(1, args.photos_per_place)]
            for i, ref in enumerate(refs, start=1):
                try:
                    r = requests.get(google_photo_url(google_key, ref), timeout=30)
                    r.raise_for_status()
                    path = f"itineraries_places/{itinerary_id}/{pid}/{i}.jpg"
                    url = upload_bytes(bucket, path, r.content, content_type=r.headers.get("Content-Type") or "image/jpeg")
                    image_urls.append(url)
                except Exception:
                    pass

            # Static map fallback
            if not image_urls and p.get("lat") is not None and p.get("lng") is not None:
                try:
                    r = requests.get(static_map_url(google_key, p["lat"], p["lng"]), timeout=30)
                    r.raise_for_status()
                    path = f"itineraries_places/{itinerary_id}/{pid}/1.jpg"
                    url = upload_bytes(bucket, path, r.content, content_type=r.headers.get("Content-Type") or "image/png")
                    image_urls.append(url)
                except Exception:
                    pass

        p["image_urls"] = image_urls
        report["place_images"][pid] = image_urls

    # Cover image: reuse first place image if present
    if not args.dry_run and bucket:
        first = places[0]
        first_pid = first.get("place_id")
        first_imgs = first.get("image_urls") or []
        if first_pid and first_imgs:
            # copy blob
            src_path = f"itineraries_places/{itinerary_id}/{first_pid}/1.jpg"
            dst_path = f"itineraries_covers/{itinerary_id}/cover.jpg"
            src_blob = bucket.blob(src_path)
            if src_blob.exists():
                bucket.copy_blob(src_blob, bucket, dst_path)
                parent_doc["cover_image_url"] = f"https://storage.googleapis.com/{bucket.name}/{dst_path}"
                report["cover"] = parent_doc["cover_image_url"]

    if args.dry_run:
        save_json(run_dir / "05_upload_report.json", {"dry_run": True, "parent_doc": parent_doc, "places": places})
        print("✅ DRY RUN: wrote 05_upload_report.json")
        return

    # Firestore write
    col = db.collection(args.collection)
    doc_ref = col.document(itinerary_id)
    doc_ref.set(parent_doc, merge=False)

    # placeMetadata subcollection
    place_sub = doc_ref.collection("placeMetadata")
    # wipe old
    old = list(place_sub.stream())
    for i in range(0, len(old), 200):
        batch = db.batch()
        for d in old[i:i+200]:
            batch.delete(d.reference)
        batch.commit()

    # write new
    for p in places:
        pid = p.get("place_id")
        if not pid:
            continue
        place_doc = {
            "place_id": pid,
            "index": int(p.get("index") or 0),
            "name": p.get("name"),
            "place_type": p.get("place_type"),
            "slot": p.get("slot"),
            "role": p.get("role"),
            "rating": p.get("rating", 0),
            "numRatings": p.get("reviews", 0),
            "categories": p.get("types", []),
            "latitude": p.get("lat"),
            "longitude": p.get("lng"),
            "address": p.get("address", ""),
            "website": p.get("website"),
            "phone": p.get("phone"),
            "opening_periods": p.get("opening_periods", []),
            "price_level": p.get("price_level"),
            "frequency": int(p.get("frequency") or 0),
            "sources": p.get("sources", []),
            "confidence": float(p.get("confidence") or 0),
            "resolution_status": p.get("resolution_status", ""),
            "image_urls": p.get("image_urls", []),
            "reason": p.get("reason", ""),
        }
        place_sub.document(pid).set(place_doc, merge=False)
        report["uploaded_places"] += 1

    # sections subcollection
    sec_sub = doc_ref.collection("sections")
    old = list(sec_sub.stream())
    for i in range(0, len(old), 200):
        batch = db.batch()
        for d in old[i:i+200]:
            batch.delete(d.reference)
        batch.commit()

    for s in data.get("sections") or []:
        sec_id = s.get("id")
        if not sec_id:
            continue
        sec_sub.document(sec_id).set({
            "id": sec_id,
            "heading": s.get("heading"),
            "short_description": s.get("short_description"),
            "placeMarkerColor": s.get("placeMarkerColor"),
            "time_range": s.get("time_range"),
            "sectionPlaces": s.get("sectionPlaces", []),
            "place_count": s.get("place_count", len(s.get("sectionPlaces") or [])),
            "order": s.get("order", 0),
        }, merge=False)

    save_json(run_dir / "05_upload_report.json", report)
    print(f"✅ Uploaded itinerary: {itinerary_id}")
    print(f"✅ Wrote: {run_dir / '05_upload_report.json'}")

if __name__ == "__main__":
    main()
