#!/usr/bin/env python3
"""
build_popular_cities_by_country.py
───────────────────────────────────
For each country in TARGET_COUNTRY_IDS / TARGET_COUNTRY_NAMES:

1. Query all docs in `allplaces` where
     - country_name == <that country>
     - subcategory in {"city","municipality"}
2. Sort them in Python by their own `popularity` field
3. Keep the top TOP_CITIES entries
4. Dump the results to OUTPUT_JSON_FILE

⚠️  This version is read-only.
"""

import json
from pathlib import Path
from typing import List, Dict

import firebase_admin
from firebase_admin import credentials, firestore

# ─── CONFIG ───────────────────────────────────────────────────────────────────

SERVICE_ACCOUNT_JSON = (
    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
    r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
)

# Parallel lists of country IDs and country names:
TARGET_COUNTRY_IDS = [
    "10165", "10705", "135383", "135385", "135391", "135393", "135396",
    "135408", "135438", "135468", "135498", "135642", "135733", "135766",
    "136048", "145918", "3651", "79304", "81176", "81178", "81179", "81184",
    "81185", "81188", "81191", "81192", "81193", "81204", "81214", "81238",
    "81239", "81309", "83438", "84883", "86647", "86651", "86652", "86653",
    "86654", "86655", "86656", "86659", "86661", "86663", "86667", "86668",
    "86683", "86693", "86698", "86719", "86729", "86731", "86772", "86776",
    "86779", "86797", "86819", "86831", "86851", "86865", "86938", "86983",
    "87077", "87139", "87175", "87389", "88352", "88358", "88359", "88360",
    "88362", "88366", "88367", "88368", "88373", "88375", "88380", "88384",
    "88386", "88394", "88402", "88403", "88405", "88407", "88408", "88419",
    "88420", "88423", "88432", "88434", "88437", "88438", "88445", "88449",
    "88455", "88464", "88477", "88478", "88486", "88503", "88527", "88597",
    "88609", "88654", "88723", "88727", "88731", "88978", "89073", "89098",
    "89102", "89196", "89330", "90374", "90407", "90647", "90648", "90651",
    "90652", "90656", "90660", "90663", "90759", "90761", "90762", "90763",
    "90764", "90774", "90776", "90785", "90788", "90790", "90791", "90796",
    "90797", "90798", "90801", "90806", "90810", "90815", "90816", "90817",
    "90821", "90823", "90828", "90832", "90840", "90841", "90847", "90848",
    "90852", "90855", "90857", "90858", "90861", "90862", "90869", "90871",
    "90872", "90877", "90882", "90883", "90884", "90889", "90899", "90900",
    "90906", "90917", "90919", "90922", "90934", "90940", "90947", "90948",
    "90950", "90951", "90962", "90963", "91219", "91220", "91223", "91224",
    "91227", "91232", "91233", "91235", "91342", "91387", "91394", "91403",
    "91409", "91413", "91416", "91420", "91427", "91433", "91440", "91453",
    "91463", "91473", "91474", "91478", "91479", "91491", "91506", "91507",
    "91516", "91524", "91525", "91526", "91530", "91534", "91535", "91536",
    "91537", "91540", "91548", "91554", "91555", "91564", "91579"
]

TARGET_COUNTRY_NAMES = [
    "Gibraltar", "Vatican City", "Brazil", "Argentina", "Chile", "Colombia",
    "Ecuador", "Uruguay", "Bolivia", "Venezuela", "Paraguay", "Suriname",
    "Guyana", "", "French Guiana", "Greenland", "Cocos (Keeling) Islands",
    "Mauritius", "Dominican Republic", "Cuba", "Jamaica", "Guadeloupe",
    "Barbados", "Aruba", "Martinique", "Bermuda", "Curacao", "Grenada",
    "Dominica", "Haiti", "Anguilla", "Montserrat", "Nauru", "Tokelau",
    "Japan", "Thailand", "Philippines", "China", "Singapore", "Vietnam",
    "South Korea", "Cambodia", "India", "Indonesia", "Nepal", "Taiwan",
    "Malaysia", "Sri Lanka", "Myanmar", "Armenia", "Azerbaijan",
    "Kazakhstan", "Mongolia", "Kyrgyzstan", "Brunei Darussalam", "Laos",
    "Bhutan", "Pakistan", "Bangladesh", "Uzbekistan", "Tajikistan",
    "Maldives", "North Korea", "Timor-Leste", "Turkmenistan",
    "Afghanistan", "Italy", "France", "United Kingdom", "Russia", "Spain",
    "Czech Republic", "Turkiye", "Germany", "The Netherlands", "Greece",
    "Hungary", "Austria", "Ireland", "Ukraine", "Denmark", "Poland",
    "Romania", "Malta", "Belgium", "Iceland", "Georgia", "Latvia",
    "Norway", "Finland", "Switzerland", "Croatia", "Sweden", "Bulgaria",
    "Estonia", "Serbia", "Lithuania", "Slovakia", "Slovenia", "Belarus",
    "Cyprus", "Bosnia and Herzegovina", "Albania", "Montenegro",
    "Luxembourg", "Republic of North Macedonia", "Moldova", "Andorra",
    "San Marino", "Monaco", "Kosovo", "Faroe Islands", "Liechtenstein",
    "Canada", "United States", "Panama", "Honduras", "Costa Rica",
    "Guatemala", "Belize", "Nicaragua", "El Salvador", "Morocco",
    "Tanzania", "Kenya", "Egypt", "South Africa", "Uganda", "Ethiopia",
    "Rwanda", "Seychelles", "Tunisia", "Cape Verde", "Namibia", "Ghana",
    "Madagascar", "Zimbabwe", "Zambia", "Senegal", "Nigeria", "Mozambique",
    "Reunion Island", "Botswana", "Algeria", "Ivory Coast",
    "Democratic Republic of the Congo", "Gambia", "Sao Tome and Principe",
    "Angola", "Eritrea", "Malawi", "Gabon", "Sierra Leone", "Togo",
    "Benin", "Sudan", "Burkina Faso", "Mali", "Cameroon", "Somalia",
    "Lesotho", "Western Sahara", "Eswatini (Swaziland)", "Libya",
    "Djibouti", "Mauritania", "Liberia", "Burundi", "Guinea", "Mayotte",
    "Niger", "Comoros", "Central African Republic", "Guinea-Bissau",
    "Equatorial Guinea", "Republic of the Congo", "Chad", "South Sudan",
    "Caribbean", "U.S. Virgin Islands", "Cayman Islands", "Bahamas",
    "Antigua and Barbuda", "Trinidad and Tobago", "St. Kitts and Nevis",
    "British Virgin Islands", "Mexico", "Australia", "New Zealand", "Fiji",
    "Mariana Islands", "New Caledonia", "Vanuatu", "French Polynesia",
    "Cook Islands", "Samoa", "Palau", "Tonga", "Papua New Guinea",
    "Solomon Islands", "Niue", "American Samoa",
    "Federated States of Micronesia", "Marshall Islands",
    "Republic of Kiribati", "Tuvalu", "Wallis and Futuna",
    "United Arab Emirates", "Israel", "Lebanon", "Iran", "Jordan", "Qatar",
    "Oman", "Kuwait", "Bahrain", "Saudi Arabia", "Syria",
    "Palestinian Territories", "Iraq", "Yemen"
]

TOP_CITIES: int = 10                          # how many top cities per country
CITY_CATEGORIES = {"city", "municipality"}    # only these subcategories
OUTPUT_JSON_FILE = "popular_cities_by_country2.json"

# ─── FIRESTORE INIT (read-only) ────────────────────────────────────────────────

if not firebase_admin._apps:
    firebase_admin.initialize_app(
        credentials.Certificate(SERVICE_ACCOUNT_JSON)
    )
db = firestore.client()

# ─── HELPERS ───────────────────────────────────────────────────────────────────

def slim(snapshot: firestore.DocumentSnapshot) -> Dict:
    """Trim a Firestore doc to the fields we need, with safe fallbacks."""
    d = snapshot.to_dict() or {}
    raw_id = d.get("place_id", snapshot.id)
    try:
        place_id = int(raw_id)
    except (ValueError, TypeError):
        place_id = str(raw_id)
    return {
        "place_id": place_id,
        "name": d.get("name") or d.get("city_name") or d.get("state_name"),
        "latitude": d.get("latitude"),
        "longitude": d.get("longitude"),
        "country_name": d.get("country_name"),
        "image_url": d.get("image_url"),
        "popularity": d.get("popularity", 0),
    }

def fetch_top_cities_by_country_name(country_name: str) -> List[Dict]:
    """
    Query ALL city/municipality docs with country_name==country_name,
    sort by their 'popularity', and return the top TOP_CITIES slim dicts.
    """
    snaps = (
        db.collection("allplaces")
          .where("country_name", "==", country_name)
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
    return [slim(snap) for _, snap in top]

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"\n=== Building popular cities for {len(TARGET_COUNTRY_IDS)} countries ===\n")
    results: Dict[str, Dict] = {}

    for cid, cname in zip(TARGET_COUNTRY_IDS, TARGET_COUNTRY_NAMES):
        if not cname:
            continue  # skip empty names
        print(f"→ {cname} (ID {cid})")
        try:
            cid_int = int(cid)
        except ValueError:
            print("   ⚠️  Invalid country ID; skipping")
            continue

        top_cities = fetch_top_cities_by_country_name(cname)
        print(f"   ✔︎  Found {len(top_cities)} popular cities")

        results[cname] = {
            "country_name": cname,
            "place_id": cid_int,
            "popular_cities": top_cities,
        }

    if not results:
        print("No data fetched; exiting.")
        return

    out_path = Path(OUTPUT_JSON_FILE)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False, default=str)

    print(f"\n✅ Done! Results written to {out_path.resolve()}\n")


if __name__ == "__main__":
    main()



# #!/usr/bin/env python3
# """
# build_popular_cities_by_countryname.py
# ───────────────────────────────────────
# For each country name in TARGET_COUNTRY_NAMES:

# 1. Query all docs in `allplaces` where
#      - country_name == <that country>
#      - subcategory in {"city","municipality"}
# 2. Sort them in Python by their own 'popularity' field
# 3. Keep the top TOP_CITIES entries
# 4. Dump the results to OUTPUT_JSON_FILE

# This version is read-only.
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

# # The country names you want to process
# TARGET_COUNTRY_NAMES = ["Gibraltar", "Vatican City", "Brazil", "Argentina", "Chile", "Colombia", "Ecuador", "Uruguay", "Bolivia", "Venezuela", "Paraguay", "Suriname", "Guyana", "", "French Guiana", "Greenland", "Cocos (Keeling) Islands", "Mauritius", "Dominican Republic", "Cuba", "Jamaica", "Guadeloupe", "Barbados", "Aruba", "Martinique", "Bermuda", "Curacao", "Grenada", "Dominica", "Haiti", "Anguilla", "Montserrat", "Nauru", "Tokelau", "Japan", "Thailand", "Philippines", "China", "Singapore", "Vietnam", "South Korea", "Cambodia", "India", "Indonesia", "Nepal", "Taiwan", "Malaysia", "Sri Lanka", "Myanmar", "Armenia", "Azerbaijan", "Kazakhstan", "Mongolia", "Kyrgyzstan", "Brunei Darussalam", "Laos", "Bhutan", "Pakistan", "Bangladesh", "Uzbekistan", "Tajikistan", "Maldives", "North Korea", "Timor-Leste", "Turkmenistan", "Afghanistan", "Italy", "France", "United Kingdom", "Russia", "Spain", "Czech Republic", "Turkiye", "Germany", "The Netherlands", "Greece", "Hungary", "Austria", "Ireland", "Ukraine", "Denmark", "Poland", "Romania", "Malta", "Belgium", "Iceland", "Georgia", "Latvia", "Norway", "Finland", "Switzerland", "Croatia", "Sweden", "Bulgaria", "Estonia", "Serbia", "Lithuania", "Slovakia", "Slovenia", "Belarus", "Cyprus", "Bosnia and Herzegovina", "Albania", "Montenegro", "Luxembourg", "Republic of North Macedonia", "Moldova", "Andorra", "San Marino", "Monaco", "Kosovo", "Faroe Islands", "Liechtenstein", "Canada", "United States", "Panama", "Honduras", "Costa Rica", "Guatemala", "Belize", "Nicaragua", "El Salvador", "Morocco", "Tanzania", "Kenya", "Egypt", "South Africa", "Uganda", "Ethiopia", "Rwanda", "Seychelles", "Tunisia", "Cape Verde", "Namibia", "Ghana", "Madagascar", "Zimbabwe", "Zambia", "Senegal", "Nigeria", "Mozambique", "Reunion Island", "Botswana", "Algeria", "Ivory Coast", "Democratic Republic of the Congo", "Gambia", "Sao Tome and Principe", "Angola", "Eritrea", "Malawi", "Gabon", "Sierra Leone", "Togo", "Benin", "Sudan", "Burkina Faso", "Mali", "Cameroon", "Somalia", "Lesotho", "Western Sahara", "Eswatini (Swaziland)", "Libya", "Djibouti", "Mauritania", "Liberia", "Burundi", "Guinea", "Mayotte", "Niger", "Comoros", "Central African Republic", "Guinea-Bissau", "Equatorial Guinea", "Republic of the Congo", "Chad", "South Sudan", "Caribbean", "U.S. Virgin Islands", "Cayman Islands", "Bahamas", "Antigua and Barbuda", "Trinidad and Tobago", "St. Kitts and Nevis", "British Virgin Islands", "Mexico", "Australia", "New Zealand", "Fiji", "Mariana Islands", "New Caledonia", "Vanuatu", "French Polynesia", "Cook Islands", "Samoa", "Palau", "Tonga", "Papua New Guinea", "Solomon Islands", "Niue", "American Samoa", "Federated States of Micronesia", "Marshall Islands", "Republic of Kiribati", "Tuvalu", "Wallis and Futuna", "United Arab Emirates", "Israel", "Lebanon", "Iran", "Jordan", "Qatar", "Oman", "Kuwait", "Bahrain", "Saudi Arabia", "Syria", "Palestinian Territories", "Iraq", "Yemen"]

# TOP_CITIES: int = 10                          # how many top cities per country
# CITY_CATEGORIES = {"city", "municipality"}    # only these subcategories
# OUTPUT_JSON_FILE = "popular_cities_by_country.json"

# # ─── FIRESTORE INIT (read-only) ────────────────────────────────────────────────

# if not firebase_admin._apps:
#     firebase_admin.initialize_app(
#         credentials.Certificate(SERVICE_ACCOUNT_JSON)
#     )
# db = firestore.client()

# # ─── HELPERS ───────────────────────────────────────────────────────────────────

# def slim(snapshot: firestore.DocumentSnapshot) -> Dict:
#     """Trim a Firestore doc to the fields we need, with safe fallbacks."""
#     d = snapshot.to_dict() or {}

#     raw_id = d.get("place_id", snapshot.id)
#     try:
#         place_id = int(raw_id)
#     except (ValueError, TypeError):
#         place_id = str(raw_id)

#     return {
#         "place_id": place_id,
#         "name": d.get("name") or d.get("city_name") or d.get("state_name"),
#         "latitude": d.get("latitude"),
#         "longitude": d.get("longitude"),
#         "country_name": d.get("country_name"),
#         "image_url": d.get("image_url"),    # may be None or list
#         "popularity": d.get("popularity", 0),
#     }

# def fetch_top_cities_by_country_name(country_name: str) -> List[Dict]:
#     """
#     Query ALL city/municipality docs with country_name==country_name,
#     sort by their 'popularity', and return the top TOP_CITIES slim dicts.
#     """
#     snaps = (
#         db.collection("allplaces")
#           .where("country_name", "==", country_name)
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

#     # sort descending and take top N
#     top = sorted(scored, key=lambda x: x[0], reverse=True)[:TOP_CITIES]
#     return [slim(snap) for _, snap in top]

# # ─── MAIN ─────────────────────────────────────────────────────────────────────

# def main() -> None:
#     print(f"\n=== Top {TOP_CITIES} Cities by country_name ===\n")
#     results: Dict[str, Dict] = {}

#     for cname in TARGET_COUNTRY_NAMES:
#         print(f"→ Fetching cities for: {cname}")
#         top_cities = fetch_top_cities_by_country_name(cname)
#         print(f"   ✔︎  Found {len(top_cities)} entries")

#         results[cname] = {
#             "country_name": cname,
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



# #!/usr/bin/env python3
# """
# build_popular_cities_json.py
# ────────────────────────────
# For each country ID in TARGET_COUNTRY_IDS, find its most-popular
# cities / municipalities (by each doc’s own `popularity` field),
# keep the top N, and dump the results to a single JSON file.

# ⚠️  NOTE: This version NEVER writes back to Firestore.
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

# # TARGET_COUNTRY_IDS = ["10165", "10705", "135383", "135385", "135391", "135393", "135396", "135408", "135438", "135468", "135498", "135642", "135733", "135766", "136048", "145918", "3651", "79304", "81176", "81178", "81179", "81184", "81185", "81188", "81191", "81192", "81193", "81204", "81214", "81238", "81239", "81309", "83438", "84883", "86647", "86651", "86652", "86653", "86654", "86655", "86656", "86659", "86661", "86663", "86667", "86668", "86683", "86693", "86698", "86719", "86729", "86731", "86772", "86776", "86779", "86797", "86819", "86831", "86851", "86865", "86938", "86983", "87077", "87139", "87175", "87389", "88352", "88358", "88359", "88360", "88362", "88366", "88367", "88368", "88373", "88375", "88380", "88384", "88386", "88394", "88402", "88403", "88405", "88407", "88408", "88419", "88420", "88423", "88432", "88434", "88437", "88438", "88445", "88449", "88455", "88464", "88477", "88478", "88486", "88503", "88527", "88597", "88609", "88654", "88723", "88727", "88731", "88978", "89073", "89098", "89102", "89196", "89330", "90374", "90407", "90647", "90648", "90651", "90652", "90656", "90660", "90663", "90759", "90761", "90762", "90763", "90764", "90774", "90776", "90785", "90788", "90790", "90791", "90796", "90797", "90798", "90801", "90806", "90810", "90815", "90816", "90817", "90821", "90823", "90828", "90832", "90840", "90841", "90847", "90848", "90852", "90855", "90857", "90858", "90861", "90862", "90869", "90871", "90872", "90877", "90882", "90883", "90884", "90889", "90899", "90900", "90906", "90917", "90919", "90922", "90934", "90940", "90947", "90948", "90950", "90951", "90962", "90963", "91219", "91220", "91223", "91224", "91227", "91232", "91233", "91235", "91342", "91387", "91394", "91403", "91409", "91413", "91416", "91420", "91427", "91433", "91440", "91453", "91463", "91473", "91474", "91478", "91479", "91491", "91506", "91507", "91516", "91524", "91525", "91526", "91530", "91534", "91535", "91536", "91537", "91540", "91548", "91554", "91555", "91564", "91579"]
# TARGET_COUNTRY_IDS = ["86661"]

# TOP_CITIES: int = 15                           # number of cities to keep
# CITY_CATEGORIES = {"city", "municipality"}     # subcategories to include
# OUTPUT_JSON_FILE = "popular_india_cities_output.json"

# # ─── INITIALISE FIRESTORE (read-only) ─────────────────────────────────────────

# if not firebase_admin._apps:
#     firebase_admin.initialize_app(credentials.Certificate(SERVICE_ACCOUNT_JSON))
# db = firestore.client()

# # ─── HELPERS ─────────────────────────────────────────────────────────────────


# def slim(snapshot: firestore.DocumentSnapshot) -> Dict:
#     """Return a minimal dict with safe fallbacks."""
#     data = snapshot.to_dict() or {}

#     pid_raw = data.get("place_id", snapshot.id)
#     try:
#         place_id = int(pid_raw)
#     except (ValueError, TypeError):
#         place_id = str(pid_raw)

#     return {
#         "place_id": place_id,
#         "name": data.get("name") or data.get("city_name") or data.get("state_name"),
#         "latitude": data.get("latitude"),
#         "longitude": data.get("longitude"),
#         "country_name": data.get("country_name"),
#         "image_url": data.get("image_url"),  # may be None
#         "popularity": data.get("popularity", 0),
#     }


# def fetch_top_cities_for_country(country_id: int) -> List[Dict]:
#     """Return the TOP_CITIES most-popular child cities for the country."""
#     city_snaps = (
#         db.collection("allplaces")
#         .where("parent_id", "==", country_id)
#         .where("subcategory", "in", list(CITY_CATEGORIES))
#         .stream()
#     )

#     scored = []
#     for snap in city_snaps:
#         data = snap.to_dict() or {}
#         try:
#             pop = float(data.get("popularity", 0))
#         except (ValueError, TypeError):
#             pop = 0.0
#         scored.append((pop, snap))

#     scored.sort(key=lambda t: t[0], reverse=True)
#     top = scored[:TOP_CITIES]
#     return [slim(snap) for _, snap in top]


# # ─── MAIN ────────────────────────────────────────────────────────────────────


# def main() -> None:
#     print(f"\n=== Popular-Cities JSON Run (TOP {TOP_CITIES}) ===\n")

#     results: Dict[str, Dict] = {}

#     for cid in TARGET_COUNTRY_IDS:
#         print(f"→ Country {cid}")
#         country_snap = db.collection("allplaces").document(cid).get()
#         if not country_snap.exists:
#             print("   ❌ not found, skipping")
#             continue

#         country_info = slim(country_snap)
#         try:
#             country_id_int = int(country_snap.id)
#         except ValueError:
#             print("   ⚠️  country ID is non-numeric; skipping city query")
#             continue

#         popular_cities = fetch_top_cities_for_country(country_id_int)
#         print(f"   ✔︎ found {len(popular_cities)} popular cities")

#         results[cid] = {
#             "country_info": country_info,
#             "popular_cities": popular_cities,
#         }

#     if not results:
#         print("\nNothing processed—exiting without writing JSON.")
#         return

#     out_path = Path(OUTPUT_JSON_FILE)
#     with out_path.open("w", encoding="utf-8") as fh:
#         json.dump(results, fh, indent=4, ensure_ascii=False, default=str)

#     print(f"\n✅ Completed. JSON saved to {out_path.resolve()}")


# if __name__ == "__main__":
#     main()





# #!/usr/bin/env python3
# """
# build_popular_cities.py
# ───────────────────────
# For every country ID listed in `TARGET_COUNTRY_IDS`:

# 1.  Finds direct child docs in `allplaces` whose `subcategory`
#     is either "city" or "municipality".
# 2.  Ranks them by their own `popularity` field.
# 3.  Keeps the top N (`TOP_CITIES`).
# 4.  Saves the results to JSON **and**, if `WRITE_TO_FIRESTORE`
#     is True, writes them to a `popular_cities` sub-collection
#     beneath each country doc.

# The script is idempotent: rerunning it just overwrites the
# `popular_cities` sub-collection for the same country.
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

# # Country document IDs you want to process
# TARGET_COUNTRY_IDS = ["10165", "10705", "135383", "135385", "135391", "135393", "135396", "135408", "135438", "135468", "135498", "135642", "135733", "135766", "136048", "145918", "3651", "79304", "81176", "81178", "81179", "81184", "81185", "81188", "81191", "81192", "81193", "81204", "81214", "81238", "81239", "81309", "83438", "84883", "86647", "86651", "86652", "86653", "86654", "86655", "86656", "86659", "86661", "86663", "86667", "86668", "86683", "86693", "86698", "86719", "86729", "86731", "86772", "86776", "86779", "86797", "86819", "86831", "86851", "86865", "86938", "86983", "87077", "87139", "87175", "87389", "88352", "88358", "88359", "88360", "88362", "88366", "88367", "88368", "88373", "88375", "88380", "88384", "88386", "88394", "88402", "88403", "88405", "88407", "88408", "88419", "88420", "88423", "88432", "88434", "88437", "88438", "88445", "88449", "88455", "88464", "88477", "88478", "88486", "88503", "88527", "88597", "88609", "88654", "88723", "88727", "88731", "88978", "89073", "89098", "89102", "89196", "89330", "90374", "90407", "90647", "90648", "90651", "90652", "90656", "90660", "90663", "90759", "90761", "90762", "90763", "90764", "90774", "90776", "90785", "90788", "90790", "90791", "90796", "90797", "90798", "90801", "90806", "90810", "90815", "90816", "90817", "90821", "90823", "90828", "90832", "90840", "90841", "90847", "90848", "90852", "90855", "90857", "90858", "90861", "90862", "90869", "90871", "90872", "90877", "90882", "90883", "90884", "90889", "90899", "90900", "90906", "90917", "90919", "90922", "90934", "90940", "90947", "90948", "90950", "90951", "90962", "90963", "91219", "91220", "91223", "91224", "91227", "91232", "91233", "91235", "91342", "91387", "91394", "91403", "91409", "91413", "91416", "91420", "91427", "91433", "91440", "91453", "91463", "91473", "91474", "91478", "91479", "91491", "91506", "91507", "91516", "91524", "91525", "91526", "91530", "91534", "91535", "91536", "91537", "91540", "91548", "91554", "91555", "91564", "91579"]

# TOP_CITIES: int = 15                           # keep this many per country
# CITY_CATEGORIES = {"city", "municipality"}     # child subcategories to rank
# OUTPUT_JSON_FILE = "popular_country_cities_output.json"

# # Flip this to False for a full dry-run (JSON only, no Firestore writes)
# WRITE_TO_FIRESTORE: bool = True

# # ─── INITIALISE FIRESTORE ────────────────────────────────────────────────────

# if not firebase_admin._apps:
#     firebase_admin.initialize_app(credentials.Certificate(SERVICE_ACCOUNT_JSON))
# db = firestore.client()

# # ─── HELPERS ─────────────────────────────────────────────────────────────────


# def slim(snapshot: firestore.DocumentSnapshot) -> Dict:
#     """
#     Return a trimmed dict with only the fields we need,
#     plus sane fallbacks for missing data.
#     """
#     data = snapshot.to_dict() or {}

#     # Prefer explicit field; fall back to snapshot ID
#     pid_raw = data.get("place_id", snapshot.id)

#     # Try to coerce to int for consistency; keep as str if not numeric
#     try:
#         place_id = int(pid_raw)
#     except (ValueError, TypeError):
#         place_id = str(pid_raw)

#     return {
#         "place_id": place_id,
#         "name": data.get("name") or data.get("city_name") or data.get("state_name"),
#         "latitude": data.get("latitude"),
#         "longitude": data.get("longitude"),
#         "country_name": data.get("country_name"),
#         "image_url": data.get("image_url"),  # may be None
#         "popularity": data.get("popularity", 0),
#     }


# def fetch_top_cities_for_country(country_id: int) -> List[Dict]:
#     """
#     Collect all child city / municipality docs under the given country,
#     rank by their own 'popularity', and return the TOP_CITIES of them
#     (each already slimmed).
#     """
#     city_snaps = (
#         db.collection("allplaces")
#         .where("parent_id", "==", country_id)
#         .where("subcategory", "in", list(CITY_CATEGORIES))
#         .stream()
#     )

#     scored = []
#     for snap in city_snaps:
#         data = snap.to_dict() or {}
#         try:
#             pop = float(data.get("popularity", 0))
#         except (ValueError, TypeError):
#             pop = 0.0
#         scored.append((pop, snap))

#     scored.sort(key=lambda t: t[0], reverse=True)
#     top = scored[:TOP_CITIES]
#     return [slim(snap) for _, snap in top]


# def write_popular_cities_subcol(country_id: str, cities: List[Dict]) -> None:
#     """
#     Overwrite the `popular_cities` sub-collection for the specified country.
#     Commits in batches of ≤400 writes to stay within Firestore limits.
#     """
#     parent_ref = db.collection("allplaces").document(country_id)
#     batch = db.batch()
#     writes_in_batch = 0

#     # Clear existing sub-collection (small utility query)
#     docs_to_delete = parent_ref.collection("popular_cities").list_documents()
#     for doc_ref in docs_to_delete:
#         batch.delete(doc_ref)
#         writes_in_batch += 1
#         if writes_in_batch >= 400:
#             batch.commit()
#             batch, writes_in_batch = db.batch(), 0

#     # Write new docs
#     for city in cities:
#         doc_ref = parent_ref.collection("popular_cities").document(str(city["place_id"]))
#         batch.set(doc_ref, city)
#         writes_in_batch += 1
#         if writes_in_batch >= 400:
#             batch.commit()
#             batch, writes_in_batch = db.batch(), 0

#     if writes_in_batch:
#         batch.commit()


# # ─── MAIN ────────────────────────────────────────────────────────────────────


# def main() -> None:
#     print(
#         f"\n=== Popular-Cities Run "
#         f"(TOP {TOP_CITIES}, write_to_firestore={WRITE_TO_FIRESTORE}) ===\n"
#     )

#     results: Dict[str, Dict] = {}

#     for cid in TARGET_COUNTRY_IDS:
#         print(f"→ Country {cid}")
#         country_snap = db.collection("allplaces").document(cid).get()
#         if not country_snap.exists:
#             print("   ❌ not found, skipping")
#             continue

#         country_info = slim(country_snap)
#         country_id_int = int(country_snap.id) if country_snap.id.isdigit() else None

#         if country_id_int is None:
#             print("   ⚠️  country ID is non-numeric; skipping city query")
#             continue

#         popular_cities = fetch_top_cities_for_country(country_id_int)
#         print(f"   ✔︎ found {len(popular_cities)} popular cities")

#         # Store in results dict (for JSON)
#         results[cid] = {
#             "country_info": country_info,
#             "popular_cities": popular_cities,
#         }

#         # Optionally write back to Firestore
#         if WRITE_TO_FIRESTORE:
#             write_popular_cities_subcol(cid, popular_cities)
#             print("   🔄 Firestore sub-collection updated")

#     if not results:
#         print("\nNothing processed—exiting without writing JSON.")
#         return

#     # Dump JSON
#     out_path = Path(OUTPUT_JSON_FILE)
#     with out_path.open("w", encoding="utf-8") as fh:
#         json.dump(results, fh, indent=4, ensure_ascii=False, default=str)

#     print(f"\n✅ Completed. JSON saved to {out_path.resolve()}")


# if __name__ == "__main__":
#     main()
