#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
import time
import googlemaps

# ======= CONFIG =======
GOOGLE_MAPS_API_KEY = "AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M"  # <— put your key
DEFAULT_QUERY = "Shimla, Himachal Pradesh"
# India bounding box (lat1,lng1|lat2,lng2): SW | NE
INDIA_BOUNDS = (6.55, 68.11, 35.67, 97.39)

# Valid, top-level fields for Place Details (no nested '/periods' in mask)
DETAIL_FIELDS = [
    "place_id",
    "name",
    "geometry",
    "formatted_address",
    "international_phone_number",
    "website",
    "opening_hours",
    "price_level",
    "permanently_closed",
    "rating",
    "user_ratings_total",
    "reviews",
    "utc_offset",
]

def pick_best(cands):
    """Prefer locality / tourist attractions if multiple matches."""
    if not cands:
        return None
    def score(c):
        t = set(c.get("types", []))
        return (("locality" in t) * 3 +
                ("tourist_attraction" in t) * 2 +
                ("point_of_interest" in t) * 1)
    return sorted(cands, key=score, reverse=True)[0]

def find_candidate(gmaps_client, query):
    """Find Place (textquery) with India bias; fallback to text search."""
    lb = f"rectangle:{INDIA_BOUNDS[0]},{INDIA_BOUNDS[1]}|{INDIA_BOUNDS[2]},{INDIA_BOUNDS[3]}"
    res = gmaps_client.find_place(
        input=query,
        input_type="textquery",
        fields=["place_id", "name", "geometry", "formatted_address", "types"],
        location_bias=lb
    )
    cands = res.get("candidates", []) or []
    if not cands:
        # Fallback: Text Search with India region bias
        ts = gmaps_client.places(query=query, region="in")
        cands = ts.get("results", []) or []
    return pick_best(cands)

def get_place_details(gmaps_client, place_id):
    det = gmaps_client.place(place_id=place_id, fields=DETAIL_FIELDS).get("result", {}) or {}
    geo = det.get("geometry") or {}
    loc = geo.get("location") or {}
    return {
        "placeId": det.get("place_id"),
        "name": det.get("name"),
        "address": det.get("formatted_address"),
        "latitude": loc.get("lat"),
        "longitude": loc.get("lng"),
        # types may only exist on the candidate; this may be empty here
        "types": det.get("types", []),
        "rating": det.get("rating", 0),
        "numRatings": det.get("user_ratings_total", 0),
        "website": det.get("website"),
        "internationalPhoneNumber": det.get("international_phone_number"),
        "priceLevel": det.get("price_level"),
        "permanentlyClosed": det.get("permanently_closed", False),
        "utcOffset": det.get("utc_offset"),  # minutes, if present
        "hasOpeningHours": bool(det.get("opening_hours")),
        "reviewsSample": [
            {
                "rating": r.get("rating"),
                "text": r.get("text"),
                "author_name": r.get("author_name"),
                "relative_time_description": r.get("relative_time_description"),
            }
            for r in (det.get("reviews") or [])[:3]
        ],
    }

def main():
    query = " ".join(sys.argv[1:]).strip() or DEFAULT_QUERY
    if not GOOGLE_MAPS_API_KEY or GOOGLE_MAPS_API_KEY.startswith("PASTE_"):
        print("❌ Please put your Google Maps API key in GOOGLE_MAPS_API_KEY.")
        sys.exit(1)

    gmaps_client = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
    print(f"Query: {query}")

    try:
        cand = find_candidate(gmaps_client, query)
        if not cand:
            print(json.dumps({"ok": False, "error": "No candidates found"}, indent=2))
            sys.exit(2)

        # Merge candidate types into details afterwards
        pid = cand.get("place_id")
        details = get_place_details(gmaps_client, pid)
        details["types"] = details.get("types") or cand.get("types", [])

        # Nice JSON print
        print(json.dumps({"ok": True, "candidate": cand, "details": details}, indent=2, ensure_ascii=False))

    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}, indent=2))
        sys.exit(3)

if __name__ == "__main__":
    main()
