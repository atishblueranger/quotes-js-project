#!/usr/bin/env python3
"""
sync_image_urls.py

For each document in 'explore', fetch its 'image_url' array
and batch-update the matching document in 'allplaces',
skipping any IDs that aren’t already there—and then
reporting which ones were skipped.
"""

import firebase_admin
from firebase_admin import credentials, firestore

def initialize_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(
            r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
        )
        firebase_admin.initialize_app(cred)
        print("✅ Firebase Admin SDK initialized.")
    else:
        print("ℹ️ Firebase Admin SDK already initialized.")
    return firestore.client()

def sync_image_urls(
    explore_coll: str = 'explore',
    allplaces_coll: str = 'allplaces',
    fetch_page_size: int = 1000,
    write_batch_size: int = 500
):
    db = initialize_firestore()
    explore_ref   = db.collection(explore_coll)
    allplaces_ref = db.collection(allplaces_coll)

    last_doc_snapshot = None
    total_processed   = 0
    total_updated     = 0
    write_batch       = db.batch()
    batch_counter     = 0
    skipped_ids       = []  # <-- collect IDs that don't exist in allplaces

    while True:
        query = explore_ref.order_by("__name__").limit(fetch_page_size)
        if last_doc_snapshot:
            query = query.start_after(last_doc_snapshot)

        docs = list(query.stream())
        if not docs:
            print("🎉 No more documents to process.")
            break

        print(f"Fetched {len(docs)} docs from '{explore_coll}'…")

        for doc in docs:
            total_processed += 1
            data = doc.to_dict() or {}
            image_urls = data.get('image_url')

            if isinstance(image_urls, list) and image_urls:
                doc_ref = allplaces_ref.document(doc.id)
                if doc_ref.get().exists:
                    write_batch.update(doc_ref, {'image_url': image_urls})
                    batch_counter += 1
                    total_updated += 1
                else:
                    skipped_ids.append(doc.id)

            last_doc_snapshot = doc

            if batch_counter >= write_batch_size:
                write_batch.commit()
                print(f"→ Committed batch of {batch_counter} updates. Total updated: {total_updated}")
                write_batch    = db.batch()
                batch_counter  = 0

        print(f"Processed page: {total_processed} read, {total_updated} queued so far.")

    # Final commit of any remaining updates
    if batch_counter > 0:
        write_batch.commit()
        print(f"→ Committed final batch of {batch_counter} updates. Total updated: {total_updated}")

    # Summary
    print("\n✅ Done!")
    print(f"  • Documents read:    {total_processed}")
    print(f"  • Documents updated: {total_updated}")
    if skipped_ids:
        print(f"  • Documents skipped (no matching allplaces): {len(skipped_ids)}")
        # Print all skipped IDs
        for sid in skipped_ids:
            print(f"    - {sid}")

if __name__ == '__main__':
    sync_image_urls()

# #!/usr/bin/env python3
# """
# sync_image_urls.py

# For each document in 'explore', fetch its 'image_url' (an array of strings)
# and batch-update the matching document in 'allplaces' (by ID), using Firestore
# pagination and batched writes for efficiency and reliability.
# """

# import os
# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore():
#     """Initializes and returns the Firestore client (singleton)."""
#     if not firebase_admin._apps:
#         # Point to your service-account JSON
#         firebase_credentials_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         print("✅ Firebase Admin SDK initialized.")
#     else:
#         print("ℹ️ Firebase Admin SDK already initialized.")
#     return firestore.client()

# def sync_image_urls(
#     explore_coll: str = 'explore',
#     allplaces_coll: str = 'allplaces',
#     fetch_page_size: int = 1000,
#     write_batch_size: int = 500
# ):
#     db = initialize_firestore()
#     explore_ref   = db.collection(explore_coll)
#     allplaces_ref = db.collection(allplaces_coll)

#     last_doc_snapshot = None
#     total_processed   = 0
#     total_updated     = 0
#     write_batch       = db.batch()
#     batch_counter     = 0

#     while True:
#         # 1. Build paginated query on 'explore'
#         query = explore_ref.order_by("__name__").limit(fetch_page_size)
#         if last_doc_snapshot:
#             query = query.start_after(last_doc_snapshot)

#         docs = list(query.stream())
#         if not docs:
#             print("🎉 No more documents to process.")
#             break

#         print(f"Fetched {len(docs)} docs from '{explore_coll}'…")

#         # 2. Iterate and queue updates
#         for doc in docs:
#             total_processed += 1
#             data = doc.to_dict() or {}
#             image_urls = data.get('image_url')

#             # Only update if image_url exists and is a list
#             if isinstance(image_urls, list) and image_urls:
#                 write_batch.update(
#                     allplaces_ref.document(doc.id),
#                     {'image_url': image_urls}
#                 )
#                 batch_counter += 1
#                 total_updated += 1

#             last_doc_snapshot = doc  # advance pagination cursor

#             # 3. Commit batch when it reaches the limit
#             if batch_counter >= write_batch_size:
#                 write_batch.commit()
#                 print(f"→ Committed batch of {batch_counter} updates. Total updated: {total_updated}")
#                 write_batch    = db.batch()
#                 batch_counter  = 0

#         print(f"Processed page: {total_processed} read, {total_updated} queued so far.")

#     # 4. Commit any remaining writes
#     if batch_counter > 0:
#         write_batch.commit()
#         print(f"→ Committed final batch of {batch_counter} updates. Total updated: {total_updated}")

#     print(f"\n✅ Done! {total_processed} documents read, {total_updated} documents updated in '{allplaces_coll}'.")

# if __name__ == '__main__':
#     sync_image_urls()
