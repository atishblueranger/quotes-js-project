#!/usr/bin/env python3
"""
build_popular_cities_by_state.py
─────────────────────────────────
For each state in TARGET_STATES (with known place_id):

1. Query all docs in `allplaces` where
     - state_name == <that state>
     - subcategory in {"city","municipality"}
2. Sort those cities by their own `popularity` field
3. Keep the top TOP_CITIES entries
4. Dump the results to OUTPUT_JSON_FILE, including each state’s place_id
"""

import json
from pathlib import Path
from typing import List, Dict

import firebase_admin
from firebase_admin import credentials, firestore

# ─── CONFIG ────────────────────────────────────────────────────────────────────

SERVICE_ACCOUNT_JSON = (
    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
    r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
)

# Provide each state with the exact Firestore place_id
TARGET_STATES = [
    {"state_name": "Jammu and Kashmir", "place_id": 86839},
    {"state_name": "California", "place_id": 90405},
    {"state_name": "Bavaria", "place_id": 88406},
    {"state_name": "Kerala", "place_id": 86714},
    {"state_name": "Maharashtra", "place_id": 86675},
    {"state_name": "National Capital Territory of Delhi", "place_id": 86660},
    {"state_name": "Uttar Pradesh", "place_id": 86706},
    {"state_name": "Andhra Pradesh", "place_id": 86933},
    {"state_name": "West Bengal", "place_id": 86716},
    {"state_name": "Gujarat", "place_id": 86781},
    {"state_name": "Telangana", "place_id": 86724},
    {"state_name": "Haryana", "place_id": 86816},
    {"state_name": "Madhya Pradesh", "place_id": 86920},
    {"state_name": "Rajasthan", "place_id": 86674},
    {"state_name": "Karnataka", "place_id": 86686},
    {"state_name": "Uttarakhand", "place_id": 86805},
    {"state_name": "Punjab", "place_id": 86887},
    {"state_name": "Odisha", "place_id": 86904},
    {"state_name": "Sikkim", "place_id": 86928},
    {"state_name": "Assam", "place_id": 86937},
    {"state_name": "Bihar", "place_id": 86972},
    {"state_name": "Meghalaya", "place_id": 86988},
    {"state_name": "Chhattisgarh", "place_id": 87007},
    {"state_name": "Tripura", "place_id": 87038},
    {"state_name": "Manipur", "place_id": 87070},
    {"state_name": "Jharkhand", "place_id": 87113},
    {"state_name": "Arunachal Pradesh", "place_id": 87236},
    {"state_name": "Mizoram", "place_id": 87378},
    {"state_name": "Nagaland", "place_id": 87411},
    {"state_name": "New York", "place_id": 90404},
    {"state_name": "Florida", "place_id": 90414},
    {"state_name": "Nevada", "place_id": 90409},
    {"state_name": "Texas", "place_id": 90419},
    {"state_name": "Illinois", "place_id": 90406},
    {"state_name": "District of Columbia", "place_id": 90415},
    {"state_name": "Massachusetts", "place_id": 90420},
    {"state_name": "Washington", "place_id": 90412},
    {"state_name": "Louisiana", "place_id": 90413},
    {"state_name": "Hokkaido", "place_id": 86676},
    {"state_name": "Shanghai Region", "place_id": 86657},
    {"state_name": "Shaanxi", "place_id": 86720},
    {"state_name": "Guangdong", "place_id": 86689},
    {"state_name": "Jiangsu", "place_id": 86722},
    {"state_name": "Zhejiang", "place_id": 86710},
    {"state_name": "Sichuan", "place_id": 86712},
    {"state_name": "Yunnan", "place_id": 86774},
    {"state_name": "Guangxi", "place_id": 86784},
    {"state_name": "Shandong", "place_id": 86745},
    {"state_name": "Fujian", "place_id": 86743},
    {"state_name": "North Rhine-Westphalia", "place_id": 88457},
    {"state_name": "Baden-Wurttemberg", "place_id": 88528},
    {"state_name": "Hesse", "place_id": 88416},
    {"state_name": "Saxony", "place_id": 88504},
    {"state_name": "Rhineland-Palatinate", "place_id": 88629},
    {"state_name": "Lower Saxony", "place_id": 88559},
    {"state_name": "Brandenburg", "place_id": 88797},
    {"state_name": "Schleswig-Holstein", "place_id": 88721},
    {"state_name": "State of Bremen", "place_id": 88627},
    {"state_name": "Mecklenburg-West Pomerania", "place_id": 88641},
    {"state_name": "New South Wales", "place_id": 91388},
    {"state_name": "Victoria", "place_id": 91386},
    {"state_name": "Queensland", "place_id": 91389},
    {"state_name": "Western Australia", "place_id": 91399},
    {"state_name": "Tasmania", "place_id": 91406},
    {"state_name": "South Australia", "place_id": 91397},
    {"state_name": "Northern Territory", "place_id": 91411}
]

TOP_CITIES: int = 10                          # how many cities per state
CITY_CATEGORIES = {"city", "municipality"}    # subcategories to include
OUTPUT_JSON_FILE = "popular_cities_by_state.json"

# ─── FIRESTORE INIT (read-only) ───────────────────────────────────────────────

if not firebase_admin._apps:
    firebase_admin.initialize_app(
        credentials.Certificate(SERVICE_ACCOUNT_JSON)
    )
db = firestore.client()

# ─── HELPERS ───────────────────────────────────────────────────────────────────

def slim(snapshot: firestore.DocumentSnapshot) -> Dict:
    d = snapshot.to_dict() or {}
    raw = d.get("place_id", snapshot.id)
    try:
        pid = int(raw)
    except (ValueError, TypeError):
        pid = str(raw)
    return {
        "place_id":    pid,
        "name":        d.get("name") or d.get("city_name") or d.get("state_name"),
        "latitude":    d.get("latitude"),
        "longitude":   d.get("longitude"),
        "country_name":d.get("country_name"),
        "image_url":   d.get("image_url"),
        "popularity":  d.get("popularity", 0),
    }

def fetch_top_cities_for_state(state_name: str) -> List[Dict]:
    snaps = (
        db.collection("allplaces")
          .where("state_name", "==", state_name)
          .where("subcategory", "in", list(CITY_CATEGORIES))
          .stream()
    )
    scored = []
    for snap in snaps:
        data = snap.to_dict() or {}
        try:
            pop = float(data.get("popularity", 0))
        except (ValueError, TypeError):
            pop = 0.0
        scored.append((pop, snap))

    top = sorted(scored, key=lambda x: x[0], reverse=True)[:TOP_CITIES]
    return [slim(s) for _, s in top]

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"\n=== Top {TOP_CITIES} Cities by State ===\n")
    results: Dict[str, Dict] = {}

    for entry in TARGET_STATES:
        state_name = entry["state_name"]
        state_pid  = entry["place_id"]
        print(f"→ {state_name!r} (ID {state_pid})")

        cities = fetch_top_cities_for_state(state_name)
        print(f"   ✔︎  Found {len(cities)} cities")

        results[state_name] = {
            "state_name":     state_name,
            "place_id":       state_pid,
            "popular_cities": cities,
        }

    if not results:
        print("No states processed; exiting.")
        return

    out_path = Path(OUTPUT_JSON_FILE)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False, default=str)

    print(f"\n✅ Done! JSON written to {out_path.resolve()}\n")

if __name__ == "__main__":
    main()


# #!/usr/bin/env python3
# """
# build_popular_cities_by_state.py
# ─────────────────────────────────
# For each state in TARGET_STATE_NAMES:

# 1. Lookup the state doc in `allplaces` by:
#      - state_name == <that state>
#      - subcategory in {"state","province"}
#    and grab its place_id
# 2. Query all child docs in `allplaces` where
#      - state_name == <that state>
#      - subcategory in {"city","municipality"}
# 3. Sort those cities by their own `popularity` field
# 4. Keep the top TOP_CITIES entries
# 5. Dump the results to OUTPUT_JSON_FILE, including each state’s place_id
# """

# import json
# from pathlib import Path
# from typing import List, Dict, Optional

# import firebase_admin
# from firebase_admin import credentials, firestore

# # ─── CONFIG ───────────────────────────────────────────────────────────────────

# SERVICE_ACCOUNT_JSON = (
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#     r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )

# # Exact state_name values you want to process:
# TARGET_STATE_NAMES = [
#     "Jammu and Kashmir",
#     "California",
#     "Bavaria",
#     "Kerala",
#     "Maharashtra",
#     "National Capital Territory of Delhi",
#     "Uttar Pradesh",
#     "Andhra Pradesh",
#     "West Bengal",
#     "Gujarat",
#     "Telangana",
#     "Haryana",
#     "Madhya Pradesh",
#     "Rajasthan",
#     "Karnataka" ,
#     "Uttarakhand",
#     "Jammu and Kashmir",
#     "Punjab",
#     "Odisha",
#     "Sikkim",
#     "Andhra Pradesh",
#     "Assam",
#     "Bihar",
#     "Meghalaya",
#     "Chhattisgarh",
#     "Tripura",
#     "Manipur",
#     "Jharkhand",
#     "Arunachal Pradesh",
#     "Mizoram",
#     "Nagaland",
#     "New York",
#     "California",
#     "Florida",
#     "Nevada",
#     "Texas",
#     "Illinois",
#     "District of Columbia",
#     "Massachusetts",
#     "Washington",
#     "Louisiana",
#     "Hokkaido",
#     "Shanghai Region",
#     "Shaanxi",
#     "Guangdong",
#     "Jiangsu",
#     "Zhejiang",
#     "Sichuan",
#     "Yunnan",
#     "Guangxi",
#     "Shandong",
#     "Fujian",
#     "North Rhine-Westphalia",
#     "Baden-Wurttemberg",
#     "Hesse",
#     "Saxony",
#     "Rhineland-Palatinate",
#     "Lower Saxony",
#     "Brandenburg",
#     "Schleswig-Holstein",
#     "State of Bremen",
#     "Mecklenburg-West Pomerania",
#     "New South Wales",
#     "Victoria",
#     "Queensland",
#     "Western Australia",
#     "Tasmania",
#     "South Australia",
#     "Northern Territory",
#     # …etc…
# ]

# TOP_CITIES: int = 10                          # how many cities to keep per state
# CITY_CATEGORIES = {"city", "municipality"}    # what counts as a city
# STATE_CATEGORIES = {"state", "province"}      # what counts as a state/province
# OUTPUT_JSON_FILE = "popular_cities_by_state.json"

# # ─── FIRESTORE INIT (read-only) ───────────────────────────────────────────────

# if not firebase_admin._apps:
#     firebase_admin.initialize_app(
#         credentials.Certificate(SERVICE_ACCOUNT_JSON)
#     )
# db = firestore.client()

# # ─── HELPERS ───────────────────────────────────────────────────────────────────

# def slim(snapshot: firestore.DocumentSnapshot) -> Dict:
#     """Trim a Firestore doc to only the fields we need."""
#     d = snapshot.to_dict() or {}
#     raw = d.get("place_id", snapshot.id)
#     try:
#         pid = int(raw)
#     except (ValueError, TypeError):
#         pid = str(raw)
#     return {
#         "place_id": pid,
#         "name":      d.get("name") or d.get("city_name") or d.get("state_name"),
#         "latitude":  d.get("latitude"),
#         "longitude": d.get("longitude"),
#         "country_name": d.get("country_name"),
#         "image_url": d.get("image_url"),
#         "popularity": d.get("popularity", 0),
#     }

# def find_state_place_id(state_name: str) -> Optional[int]:
#     """
#     Look up the state (or province) doc by its state_name and return its place_id (int).
#     Returns None if not found.
#     """
#     snaps = (
#         db.collection("allplaces")
#           .where("state_name", "==", state_name)
#           .where("subcategory", "in", list(STATE_CATEGORIES))
#           .stream()
#     )
#     for snap in snaps:
#         data = snap.to_dict() or {}
#         raw = data.get("place_id", snap.id)
#         try:
#             return int(raw)
#         except (ValueError, TypeError):
#             return None
#     return None

# def fetch_top_cities_for_state(state_name: str) -> List[Dict]:
#     """
#     Query all cities/municipalities whose state_name == state_name,
#     then sort them by popularity and return the top TOP_CITIES.
#     """
#     snaps = (
#         db.collection("allplaces")
#           .where("state_name", "==", state_name)
#           .where("subcategory", "in", list(CITY_CATEGORIES))
#           .stream()
#     )
#     scored = []
#     for snap in snaps:
#         data = snap.to_dict() or {}
#         try:
#             pop = float(data.get("popularity", 0))
#         except (ValueError, TypeError):
#             pop = 0.0
#         scored.append((pop, snap))

#     top = sorted(scored, key=lambda x: x[0], reverse=True)[:TOP_CITIES]
#     return [slim(s) for _, s in top]

# # ─── MAIN ─────────────────────────────────────────────────────────────────────

# def main() -> None:
#     print(f"\n=== Top {TOP_CITIES} Cities by State (with state place_id) ===\n")
#     results: Dict[str, Dict] = {}

#     for state in TARGET_STATE_NAMES:
#         if not state:
#             continue
#         print(f"→ Processing state: {state!r}")

#         state_pid = find_state_place_id(state)
#         if state_pid is None:
#             print(f"   ⚠️  Could not find state doc for {state!r}; skipping.")
#             continue
#         print(f"   🆔 state place_id = {state_pid}")

#         cities = fetch_top_cities_for_state(state)
#         print(f"   ✔︎  Found {len(cities)} popular cities")

#         results[state] = {
#             "state_name":    state,
#             "place_id":      state_pid,
#             "popular_cities": cities,
#         }

#     if not results:
#         print("No states processed; exiting.")
#         return

#     out_path = Path(OUTPUT_JSON_FILE)
#     with out_path.open("w", encoding="utf-8") as f:
#         json.dump(results, f, indent=4, ensure_ascii=False, default=str)

#     print(f"\n✅ Done! Results written to {out_path.resolve()}\n")

# if __name__ == "__main__":
#     main()




# #!/usr/bin/env python3
# """
# build_popular_cities_by_state.py
# ─────────────────────────────────
# For each state in TARGET_STATE_NAMES:

# 1. Query all docs in `allplaces` where
#      - state_name == <that state>
#      - subcategory in {"city","municipality"}
# 2. Sort them in Python by their own `popularity` field
# 3. Keep the top TOP_CITIES entries
# 4. Dump the results to OUTPUT_JSON_FILE

# ⚠️  This version is read-only.
# """

# import json
# from pathlib import Path
# from typing import List, Dict

# import firebase_admin
# from firebase_admin import credentials, firestore

# # ─── CONFIG ───────────────────────────────────────────────────────────────────

# SERVICE_ACCOUNT_JSON = (
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#     r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )

# # List the exact state_name values you want to process:
# TARGET_STATE_NAMES = [
    # "Jammu and Kashmir",
    # "California",
    # "Bavaria",
    # "Kerala",
    # "Maharashtra",
    # "National Capital Territory of Delhi",
    # "Uttar Pradesh",
    # "Andhra Pradesh",
    # "West Bengal",
    # "Gujarat",
    # "Telangana",
    # "Haryana",
    # "Madhya Pradesh",
    # "Rajasthan",
    # "Karnataka" ,
    # "Uttarakhand",
    # "Jammu and Kashmir",
    # "Punjab",
    # "Odisha",
    # "Sikkim",
    # "Andhra Pradesh",
    # "Assam",
    # "Bihar",
    # "Meghalaya",
    # "Chhattisgarh",
    # "Tripura",
    # "Manipur",
    # "Jharkhand",
    # "Arunachal Pradesh",
    # "Mizoram",
    # "Nagaland",
    # "New York",
    # "California",
    # "Florida",
    # "Nevada",
    # "Texas",
    # "Illinois",
    # "District of Columbia",
    # "Massachusetts",
    # "Washington",
    # "Louisiana",
    # "Hokkaido",
    # "Shanghai Region",
    # "Shaanxi",
    # "Guangdong",
    # "Jiangsu",
    # "Zhejiang",
    # "Sichuan",
    # "Yunnan",
    # "Guangxi",
    # "Shandong",
    # "Fujian",
    # "North Rhine-Westphalia",
    # "Baden-Wurttemberg",
    # "Hesse",
    # "Saxony",
    # "Rhineland-Palatinate",
    # "Lower Saxony",
    # "Brandenburg",
    # "Schleswig-Holstein",
    # "State of Bremen",
    # "Mecklenburg-West Pomerania",
    # "New South Wales",
    # "Victoria",
    # "Queensland",
    # "Western Australia",
    # "Tasmania",
    # "South Australia",
    # "Northern Territory",
#     # … add more …
# ]

# TOP_CITIES: int = 10                          # how many cities to keep per state
# CITY_CATEGORIES = {"city", "municipality"}    # the subcategories we care about
# OUTPUT_JSON_FILE = "popular_cities_by_state.json"

# # ─── FIRESTORE INIT (read-only) ───────────────────────────────────────────────

# if not firebase_admin._apps:
#     firebase_admin.initialize_app(
#         credentials.Certificate(SERVICE_ACCOUNT_JSON)
#     )
# db = firestore.client()

# # ─── HELPERS ───────────────────────────────────────────────────────────────────

# def slim(snapshot: firestore.DocumentSnapshot) -> Dict:
#     """
#     Return a minimal dict with only the fields we need,
#     with sane fallbacks for missing data.
#     """
#     d = snapshot.to_dict() or {}
#     raw = d.get("place_id", snapshot.id)
#     try:
#         place_id = int(raw)
#     except (ValueError, TypeError):
#         place_id = str(raw)
#     return {
#         "place_id": place_id,
#         "name": d.get("name") or d.get("city_name") or d.get("state_name"),
#         "latitude": d.get("latitude"),
#         "longitude": d.get("longitude"),
#         "country_name": d.get("country_name"),
#         "image_url": d.get("image_url"),
#         "popularity": d.get("popularity", 0),
#     }

# def fetch_top_cities_for_state(state_name: str) -> List[Dict]:
#     """
#     Query all cities/municipalities whose state_name == state_name,
#     then sort them by 'popularity' and return the top TOP_CITIES.
#     """
#     snaps = (
#         db.collection("allplaces")
#           .where("state_name", "==", state_name)
#           .where("subcategory", "in", list(CITY_CATEGORIES))
#           .stream()
#     )
#     scored = []
#     for snap in snaps:
#         data = snap.to_dict() or {}
#         try:
#             pop = float(data.get("popularity", 0))
#         except (ValueError, TypeError):
#             pop = 0.0
#         scored.append((pop, snap))

#     # sort descending by popularity, take top N
#     top = sorted(scored, key=lambda x: x[0], reverse=True)[:TOP_CITIES]
#     return [slim(snap) for _, snap in top]

# # ─── MAIN ─────────────────────────────────────────────────────────────────────

# def main() -> None:
#     print(f"\n=== Top {TOP_CITIES} Cities by State ===\n")
#     results: Dict[str, Dict] = {}

#     for state in TARGET_STATE_NAMES:
#         if not state:
#             continue
#         print(f"→ State: {state!r}")
#         top_cities = fetch_top_cities_for_state(state)
#         print(f"   ✔︎ Found {len(top_cities)} cities")

#         results[state] = {
#             "state_name": state,
#             "popular_cities": top_cities,
#         }

#     if not results:
#         print("No data fetched; exiting.")
#         return

#     out_path = Path(OUTPUT_JSON_FILE)
#     with out_path.open("w", encoding="utf-8") as f:
#         json.dump(results, f, indent=4, ensure_ascii=False, default=str)

#     print(f"\n✅ Done! Results written to {out_path.resolve()}\n")

# if __name__ == "__main__":
#     main()
