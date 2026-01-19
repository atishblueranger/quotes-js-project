# Filter by popularity
# #!/usr/bin/env python3
# import json
# import argparse

# def main():
#     parser = argparse.ArgumentParser(
#         description="Filter JSON entries by popularity threshold."
#     )
#     parser.add_argument(
#         "--input_file",
#         required=True,
#         help="Path to the input JSON file (array of objects with a 'popularity' field)."
#     )
#     parser.add_argument(
#         "--output_file",
#         help="Path to write the filtered JSON. If omitted, prints to stdout."
#     )
#     parser.add_argument(
#         "--popularity_threshold",
#         type=int,
#         default=25000,
#         help="Keep only entries where popularity > this value (default: 25000)."
#     )
#     args = parser.parse_args()

#     # Load the JSON array
#     with open(args.input_file, "r", encoding="utf-8") as f:
#         items = json.load(f)

#     # Filter
#     filtered = [
#         entry for entry in items
#         if isinstance(entry.get("popularity"), (int, float))
#            and entry["popularity"] > args.popularity_threshold
#     ]

#     # Output
#     if args.output_file:
#         with open(args.output_file, "w", encoding="utf-8") as f:
#             json.dump(filtered, f, indent=2, ensure_ascii=False)
#         print(f"✅ Wrote {len(filtered)} entries to {args.output_file}")
#     else:
#         print(json.dumps(filtered, indent=2, ensure_ascii=False))
#         print(f"\n✅ Kept {len(filtered)} entries with popularity > {args.popularity_threshold}")

# if __name__ == "__main__":
#     main()

# #!/usr/bin/env python3

import os
import argparse
import json
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore import FieldFilter

def initialize_firestore():
    """
    Initializes the Firestore client using your service account.
    """
    if not firebase_admin._apps:
        cred_path = (
            r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
            r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
        )
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
        print("✅ Firebase Admin SDK initialized.")
    else:
        print("ℹ️ Firebase Admin SDK already initialized.")
    return firestore.client()

def fetch_regions_with_popularity(db, collection_name, page_size=500):
    col_ref = db.collection(collection_name)
    # use the filter keyword to avoid the positional-args warning:
    base_query = (
        col_ref
        .where(filter=FieldFilter('subcategory', '==', 'territory'))
        # pick only the fields you need — name & popularity — to reduce payload:
        .select(['city_name', 'country_name', 'popularity'])
    )

    results = []
    next_query = base_query.limit(page_size)
    last_doc = None

    while True:
        docs = list(next_query.stream())
        if not docs:
            break

        for doc in docs:
            data = doc.to_dict()
            results.append({
                'id':        doc.id,
                'name':      data.get('city_name'),
                'country':   data.get('country_name'),
                'popularity':data.get('popularity')
            })

        last_doc = docs[-1]
        next_query = base_query.start_after(last_doc).limit(page_size)

    return results
# def fetch_regions_with_popularity(db, collection_name):
#     """
#     Queries `collection_name` for documents where 'subcategory' == 'region',
#     and returns a list of dicts with each document's ID, its 'name' field,
#     and its 'popularity' field.
#     """
#     col_ref = db.collection(collection_name)
#     query = col_ref.where('subcategory', '==', 'city')
#     docs = query.stream()

#     results = []
#     for doc in docs:
#         data = doc.to_dict() or {}
#         # adjust key if you store the region's name under a different field
#         name = data.get('name', data.get('city_name', None))
#         popularity = data.get('popularity', None)
#         results.append({
#             'id': doc.id,
#             'name': name,
#             'popularity': popularity
#         })
#     return results

def main():
    parser = argparse.ArgumentParser(
        description="Fetch all 'region' docs from 'allplaces', including popularity."
    )
    parser.add_argument(
        '--output_file',
        required=False,
        help='Path to write the results as JSON. If omitted, prints to stdout.'
    )
    args = parser.parse_args()

    db = initialize_firestore()
    regions = fetch_regions_with_popularity(db, 'allplaces')

    if args.output_file:
        with open(args.output_file, 'w', encoding='utf-8') as f:
            json.dump(regions, f, indent=2, ensure_ascii=False)
        print(f"✅ Wrote {len(regions)} records to {args.output_file}")
    else:
        print(json.dumps(regions, indent=2, ensure_ascii=False))
        print(f"\n✅ Retrieved {len(regions)} regions with their popularity")

if __name__ == '__main__':
    main()


# Working  
# #!/usr/bin/env python3

# import os
# import argparse
# import json
# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore():
#     """
#     Initializes the Firestore client using your service account.
#     """
#     if not firebase_admin._apps:
#         cred_path = (
#             r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#             r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#         )
#         cred = credentials.Certificate(cred_path)
#         firebase_admin.initialize_app(cred)
#         print("✅ Firebase Admin SDK initialized.")
#     else:
#         print("ℹ️ Firebase Admin SDK already initialized.")
#     return firestore.client()

# def fetch_places_by_subcategory(db, collection_name, subcategory):
#     """
#     Queries `collection_name` for documents where 'subcategory' == subcategory,
#     and returns a list of dicts with each document's ID and its 'name' field.
#     """
#     col_ref = db.collection(collection_name)
#     query = col_ref.where('subcategory', '==', subcategory)
#     docs = query.stream()

#     results = []
#     for doc in docs:
#         data = doc.to_dict() or {}
#         name = data.get('city_name', None)
#         results.append({
#             'id': doc.id,
#             'name': name
#         })
#     return results

# def main():
#     parser = argparse.ArgumentParser(
#         description="Fetch place IDs and names in 'allplaces' with a given subcategory."
#     )
#     parser.add_argument(
#         '--subcategory',
#         required=True,
#         help='The subcategory value to filter on (e.g. "country").'
#     )
#     parser.add_argument(
#         '--output_file',
#         required=False,
#         help='Optional path to write the results as JSON. If omitted, prints to stdout.'
#     )
#     args = parser.parse_args()

#     db = initialize_firestore()
#     places = fetch_places_by_subcategory(db, 'allplaces', args.subcategory)

#     if args.output_file:
#         with open(args.output_file, 'w', encoding='utf-8') as f:
#             json.dump(places, f, indent=2, ensure_ascii=False)
#         print(f"✅ Wrote {len(places)} records to {args.output_file}")
#     else:
#         print(json.dumps(places, indent=2, ensure_ascii=False))
#         print(f"\n✅ Retrieved {len(places)} places where subcategory='{args.subcategory}'")

# if __name__ == '__main__':
#     main()
