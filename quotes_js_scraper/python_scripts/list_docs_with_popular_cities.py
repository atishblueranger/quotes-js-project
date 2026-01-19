"""
Script to list all docs in a Firestore collection having a given subcollection
and subcategory == 'country'.
"""
import json
import logging
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import firebase_admin
from firebase_admin import credentials, firestore

MAX_PAGE_SIZE = 1000

def initialize_firestore(cred_path: Path) -> firestore.Client:
    """Initialize Firebase Admin SDK and return a Firestore client."""
    if not firebase_admin._apps:
        logging.info(f"Initializing Firebase Admin SDK with credentials: {cred_path}")
        cred = credentials.Certificate(str(cred_path))
        firebase_admin.initialize_app(cred)
    else:
        logging.info("Firebase Admin SDK already initialized.")
    return firestore.client()

def fetch_filtered_docs(db, collection_name, filter_field, filter_value, page_size):
    collection_ref = db.collection(collection_name)
    query = (
        collection_ref
        .where(filter_field, "==", filter_value)
        .select(["city_name"])
        .order_by("__name__")
        .limit(page_size)
    )

    last = None
    while True:
        q = query.start_after(last) if last else query
        snapshots = list(q.stream())
        if not snapshots:
            break
        for snap in snapshots:
            yield snap.reference, snap.get("city_name")
        last = snapshots[-1]


def has_subcollection(doc_ref: firestore.DocumentReference, subcol_name: str) -> bool:
    """Return True if the document has a subcollection named subcol_name."""
    # Listing subcollections is a network call, so we parallelize it below.
    return any(c.id == subcol_name for c in doc_ref.collections())

def main():
    p = argparse.ArgumentParser(
        description="List docs in a Firestore collection having a given subcollection\n"
                    "and subcategory == filter_value."
    )
    p.add_argument("-c", "--credentials", type=Path, required=True,
                   help="Path to your Firebase service-account JSON.")
    p.add_argument("-C", "--collection", default="allplaces",
                   help="Top-level collection name (default: allplaces).")
    p.add_argument("-f", "--filter_field", default="subcategory",
                   help="Field to filter on (default: subcategory).")
    p.add_argument("-v", "--filter_value", default="country",
                   help="Value of filter_field to match (default: country).")
    p.add_argument("-s", "--subcol", default="top_attractions",
                   help="Subcollection to look for (default: top_attractions).")
    p.add_argument("-p", "--page_size", type=int, default=MAX_PAGE_SIZE,
                   help=f"Docs per batch (max {MAX_PAGE_SIZE}, default).")
    p.add_argument("-w", "--max_workers", type=int, default=10,
                   help="Number of threads for subcollection checks (default: 10).")
    p.add_argument("-o", "--output", type=Path, default=None,
                   help="JSON output file path (defaults to '<subcol>_docs.json').")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")

    db = initialize_firestore(args.credentials)

    # Gather all filtered docs first
    docs = list(fetch_filtered_docs(
        db,
        collection_name=args.collection,
        filter_field=args.filter_field,
        filter_value=args.filter_value,
        page_size=args.page_size
    ))
    logging.info(f"Found {len(docs)} documents with {args.filter_field} == '{args.filter_value}'.")

    results = []
    # Check subcollections in parallel
    with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        future_to_doc = {
            pool.submit(has_subcollection, doc_ref, args.subcol): (doc_id, name)
            for doc_ref, name in docs
            for doc_id in [doc_ref.id]
        }
        for future in as_completed(future_to_doc):
            doc_id, name = future_to_doc[future]
            if future.result():
                results.append({"_id": doc_id, "name": name})

    out_path = args.output or Path(f"{args.subcol}_docs.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    logging.info(f"✅ Results written to {out_path} ({len(results)} matches).")

if __name__ == "__main__":
    main()



# import json
# import logging
# import argparse
# from pathlib import Path

# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore(cred_path: Path) -> firestore.Client:
#     """Initialize Firebase Admin SDK and return a Firestore client."""
#     if not firebase_admin._apps:
#         logging.info(f"Initializing Firebase Admin SDK with credentials: {cred_path}")
#         cred = credentials.Certificate(str(cred_path))
#         firebase_admin.initialize_app(cred)
#     else:
#         logging.info("Firebase Admin SDK already initialized.")
#     return firestore.client()

# def find_docs_with_subcollection(
#     db: firestore.Client,
#     collection_name: str,
#     subcol_name: str,
#     page_size: int
# ) -> list[dict]:
#     """
#     Page through `collection_name` in chunks of `page_size`,
#     returning [{"_id": doc_id, "name": name}, …]
#     for every document that
#       - has a subcollection named `subcol_name`, and
#       - whose 'subcategory' field == "country"
#     """
#     results = []
#     last_doc = None
#     total_scanned = 0

#     logging.info(f"Scanning '{collection_name}' for subcollection '{subcol_name}' "
#                  f"and subcategory='country' in batches of {page_size}…")

#     while True:
#         q = db.collection(collection_name).order_by("__name__").limit(page_size)
#         if last_doc:
#             q = q.start_after(last_doc)

#         batch = list(q.stream())
#         if not batch:
#             break

#         for doc in batch:
#             total_scanned += 1
#             data = doc.to_dict() or {}

#             # check subcollection
#             subcols = [c.id for c in doc.reference.collections()]
#             has_subcol = subcol_name in subcols

#             # check the 'subcategory' field
#             is_country = data.get("subcategory") == "city"

#             if has_subcol and is_country:
#                 results.append({
#                     "_id": doc.id,
#                     "name": data.get("name")  # or adjust this key if your field is called 'city_name'
#                 })

#         logging.info(f"  • Scanned {total_scanned} docs; found {len(results)} matches so far.")
#         last_doc = batch[-1]

#     logging.info(f"Finished scanning. Total scanned: {total_scanned}, total matches: {len(results)}.")
#     return results

# def main():
#     p = argparse.ArgumentParser(
#         description="List all docs in a Firestore collection having a given subcollection "
#                     "and subcategory == 'country'."
#     )
#     p.add_argument(
#         "-c", "--credentials",
#         type=Path,
#         required=True,
#         help="Path to your Firebase service-account JSON."
#     )
#     p.add_argument(
#         "-C", "--collection",
#         default="allplaces",
#         help="Top-level collection name (default: allplaces)."
#     )
#     p.add_argument(
#         "-s", "--subcol",
#         default="top_attractions",
#         help="Subcollection to look for (default: top_attractions)."
#     )
#     p.add_argument(
#         "-p", "--page_size",
#         type=int,
#         default=500,
#         help="Number of docs to fetch per batch (default: 500)."
#     )
#     p.add_argument(
#         "-o", "--output",
#         type=Path,
#         default=None,
#         help="JSON output file path (defaults to '<subcol>_docs.json')."
#     )
#     args = p.parse_args()

#     logging.basicConfig(level=logging.INFO,
#                         format="%(asctime)s %(levelname)s: %(message)s")

#     db = initialize_firestore(args.credentials)
#     matches = find_docs_with_subcollection(
#         db,
#         collection_name=args.collection,
#         subcol_name=args.subcol,
#         page_size=args.page_size
#     )

#     out_path = args.output or Path(f"{args.subcol}_docs.json")
#     with open(out_path, "w", encoding="utf-8") as f:
#         json.dump(matches, f, indent=2, ensure_ascii=False)

#     logging.info(f"✅ Results written to {out_path}")

# if __name__ == "__main__":
#     main()





# # check_subcollections.py
# import firebase_admin
# from firebase_admin import credentials, firestore

# # 1) initialise
# cred = credentials.Certificate(r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
# firebase_admin.initialize_app(cred)
# db = firestore.client()

# # 2) point at your document
# doc_ref = db.collection("allplaces").document("86647")

# # 3) list its subcollections
# subs = [c.id for c in doc_ref.collections()]
# print("Subcollections on 86647:", subs)

# # 4) show docs under top_attractions (first 5 for brevity)
# if "top_attractions" in subs:
#     docs = list(doc_ref.collection("top_attractions").stream())
#     print(f"→ Found {len(docs)} attraction docs under 86647")
#     for d in docs[:5]:
#         print(" ", d.id, d.to_dict().get("name"))
# else:
#     print("No 'top_attractions' subcollection on 86647")



#!/usr/bin/env python3
import json
import logging
import argparse
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore

def initialize_firestore(cred_path: Path) -> firestore.Client:
    """Initialize Firebase Admin SDK and return a Firestore client."""
    if not firebase_admin._apps:
        logging.info(f"Initializing Firebase Admin SDK with credentials: {cred_path}")
        cred = credentials.Certificate(str(cred_path))
        firebase_admin.initialize_app(cred)
    else:
        logging.info("Firebase Admin SDK already initialized.")
    return firestore.client()

def find_docs_with_subcollection(
    db: firestore.Client,
    collection_name: str,
    subcol_name: str,
    page_size: int
) -> list[dict]:
    """
    Page through `collection_name` in chunks of `page_size`,
    returning [{"_id": doc_id, "city_name": city_name}, …]
    for every document that has a subcollection named `subcol_name`.
    """
    results = []
    last_doc = None
    total_scanned = 0

    logging.info(f"Scanning '{collection_name}' for subcollection '{subcol_name}' in batches of {page_size}…")

    while True:
        q = db.collection(collection_name).order_by("__name__").limit(page_size)
        if last_doc:
            q = q.start_after(last_doc)

        batch = list(q.stream())
        if not batch:
            break

        for doc in batch:
            total_scanned += 1
            # use the DocumentReference from the snapshot
            subcols = [c.id for c in doc.reference.collections()]
            if subcol_name in subcols:
                data = doc.to_dict() or {}
                results.append({
                    "_id": doc.id,
                    "city_name": data.get("city_name")
                })

        logging.info(f"  • Scanned {total_scanned} docs; found {len(results)} matches so far.")
        last_doc = batch[-1]

    logging.info(f"Finished scanning. Total scanned: {total_scanned}, total matches: {len(results)}.")
    return results

def main():
    p = argparse.ArgumentParser(
        description="List all docs in a Firestore collection having a given subcollection."
    )
    p.add_argument(
        "-c", "--credentials",
        type=Path,
        required=True,
        help="Path to your Firebase service-account JSON."
    )
    p.add_argument(
        "-C", "--collection",
        default="allplaces",
        help="Top-level collection name (default: allplaces)."
    )
    p.add_argument(
        "-s", "--subcol",
        default="popular_cities",
        help="Subcollection to look for (default: popular_cities)."
    )
    p.add_argument(
        "-p", "--page_size",
        type=int,
        default=500,
        help="Number of docs to fetch per batch (default: 500)."
    )
    p.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        help="JSON output file path (defaults to '<subcol>_docs.json')."
    )
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    db = initialize_firestore(args.credentials)
    matches = find_docs_with_subcollection(
        db,
        collection_name=args.collection,
        subcol_name=args.subcol,
        page_size=args.page_size
    )

    out_path = args.output or Path(f"{args.subcol}_docs.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(matches, f, indent=2, ensure_ascii=False)

    logging.info(f"✅ Results written to {out_path}")

if __name__ == "__main__":
    main()



# import os
# import json
# import logging
# import argparse

# import firebase_admin
# from firebase_admin import credentials, firestore

# # ─── CONFIG: hard-coded service account ─────────────────────────────────────────
# SERVICE_ACCOUNT_PATH = (
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#     r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )

# def initialize_firestore() -> firestore.Client:
#     """Initialize Firebase Admin SDK and return Firestore client."""
#     if not firebase_admin._apps:
#         logging.info(f"Initializing Firebase Admin SDK with credentials: {SERVICE_ACCOUNT_PATH}")
#         cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
#         firebase_admin.initialize_app(cred)
#     else:
#         logging.info("Firebase Admin SDK already initialized.")
#     return firestore.client()

# def find_docs_with_subcollection(
#     db: firestore.Client,
#     collection_name: str,
#     subcol_name: str,
#     page_size: int
# ) -> list[dict]:
#     """
#     Page through `collection_name` in chunks of `page_size`, and return a list of
#     {"_id": doc_id, "city_name": city_name} for those having subcollection `subcol_name`.
#     """
#     results = []
#     last_doc = None
#     total_scanned = 0

#     logging.info(f"Scanning '{collection_name}' for subcollection '{subcol_name}' in batches of {page_size}...")

#     while True:
#         # Build the query
#         query = db.collection(collection_name).order_by("__name__").limit(page_size)
#         if last_doc:
#             query = query.start_after(last_doc)

#         batch = list(query.stream())
#         if not batch:
#             break

#         for doc in batch:
#             total_scanned += 1
#             subcols = [c.id for c in db.collection(collection_name).document(doc.id).collections()]
#             if subcol_name in subcols:
#                 data = doc.to_dict() or {}
#                 results.append({"_id": doc.id, "city_name": data.get("city_name")})

#         logging.info(f"  • Scanned {total_scanned} documents; found {len(results)} matches so far.")

#         # Prepare for next page
#         last_doc = batch[-1]

#     logging.info(f"Finished scanning. Total scanned: {total_scanned}, total matches: {len(results)}.")
#     return results

# def main():
#     parser = argparse.ArgumentParser(
#         description="List all docs in a Firestore collection having a given subcollection (batched)."
#     )
#     parser.add_argument(
#         "--collection", default="allplaces",
#         help="Top-level collection name (default: allplaces)."
#     )
#     parser.add_argument(
#         "--subcol", default="popular_cities",
#         help="Subcollection to look for (default: popular_cities)."
#     )
#     parser.add_argument(
#         "--page_size", type=int, default=500,
#         help="Number of docs to fetch per batch (default: 500)."
#     )
#     parser.add_argument(
#         "--output", default=None,
#         help="JSON output file path (defaults to '<subcol>_docs.json')."
#     )
#     args = parser.parse_args()

#     logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

#     db = initialize_firestore()
#     matches = find_docs_with_subcollection(
#         db,
#         collection_name=args.collection,
#         subcol_name=args.subcol,
#         page_size=args.page_size
#     )

#     out_path = args.output or f"{args.subcol}_docs.json"
#     with open(out_path, "w", encoding="utf-8") as f:
#         json.dump(matches, f, indent=2, ensure_ascii=False)

#     logging.info(f"✅ Results written to {out_path}")

# if __name__ == "__main__":
#     main()
