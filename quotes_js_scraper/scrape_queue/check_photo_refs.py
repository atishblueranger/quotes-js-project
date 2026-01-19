#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Quick test: fetch photo_reference list for a single Google Place ID
Prints: {"photo_refs": [...]}
"""

import sys
import json
import requests

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# PUT YOUR KEY HERE (hardcoded as requested):
API_KEY = "AIzaSyACj4TeFbllDT9lRjL-N4qtfnrnqwiHb1M"
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

DEFAULT_PLACE_ID = "ChIJfzr0gspgvjsRbiPqS8Mkl9c"  # your example ID

def fetch_photo_refs(place_id: str, api_key: str):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "photos",
        "key": api_key
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        # On any error, return empty list and a hint
        return [], f"HTTP/parse error: {e}"

    status = data.get("status")
    if status != "OK":
        # Common reasons: API not enabled, wrong key, restricted key, or no photos
        return [], f"Places API status: {status}; error_message={data.get('error_message')}"

    photos = (data.get("result") or {}).get("photos") or []
    refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]
    return refs, None

def main():
    place_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLACE_ID
    refs, err = fetch_photo_refs(place_id, API_KEY)

    # Print exactly the shape you asked for
    print(json.dumps({"photo_refs": refs}, ensure_ascii=False, indent=2))

    # If something went wrong, also print a hint to stderr (optional)
    if err:
        sys.stderr.write(f"[hint] {err}\n")

if __name__ == "__main__":
    main()
