""" 
Script to migrate all top_attractions docs under any allplaces/<id> to have a source_url field.
"""
# ───── MIGRATION SCRIPT Complete ───────────────────────────────────────────────────────
import firebase_admin
from firebase_admin import credentials, firestore

# ───── CONFIG ────────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
SUBCOLLECTION_NAME   = "top_attractions"
BATCH_SIZE           = 500

# ───── FIRESTORE INIT ─────────────────────────────────────────────────────────
cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
firebase_admin.initialize_app(cred)
db = firestore.client()

# ───── MIGRATION ──────────────────────────────────────────────────────────────
def migrate_all_top_attractions():
    batch = db.batch()
    count = 0

    # collection_group finds every top_attractions doc under any allplaces/<id>
    for doc in db.collection_group(SUBCOLLECTION_NAME).stream():
        data = doc.to_dict()
        if "detail_url" not in data:
            continue  # skip docs that already lack detail_url

        ref    = doc.reference
        detail = data["detail_url"]

        # queue update: copy detail_url → source_url, then delete detail_url
        batch.update(ref, {
            "source_url": detail,
            "detail_url": firestore.DELETE_FIELD
        })
        count += 1

        # commit in batches to avoid over-sized writes
        if count % BATCH_SIZE == 0:
            batch.commit()
            print(f"Committed {count} updates so far…")
            batch = db.batch()

    # commit any remaining updates
    if count % BATCH_SIZE != 0:
        batch.commit()

    print(f"\n✅ Migration complete: {count} documents updated across all '{SUBCOLLECTION_NAME}' subcollections.")

if __name__ == "__main__":
    migrate_all_top_attractions()







# Migration script for single attraction
# import firebase_admin
# from firebase_admin import credentials, firestore

# # ───── CONFIG ────────────────────────────────────────────────────────────────
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# PARENT_DOC_ID        = "9617"
# SUBCOLLECTION_NAME   = "top_attractions"
# BATCH_SIZE           = 500

# # ───── FIRESTORE INIT ─────────────────────────────────────────────────────────
# cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
# firebase_admin.initialize_app(cred)
# db = firestore.client()

# # ───── MIGRATION ──────────────────────────────────────────────────────────────
# def migrate_top_attractions(parent_id):
#     col_ref = (db.collection("allplaces")
#                  .document(parent_id)
#                  .collection(SUBCOLLECTION_NAME))
#     docs = col_ref.stream()

#     batch = db.batch()
#     count = 0

#     for doc in docs:
#         data = doc.to_dict()
#         if "detail_url" not in data:
#             # nothing to do if field doesn't exist
#             continue

#         ref    = doc.reference
#         detail = data["detail_url"]

#         # queue update: set source_url, delete detail_url
#         batch.update(ref, {
#             "source_url": detail,
#             "detail_url": firestore.DELETE_FIELD
#         })
#         count += 1

#         # commit batch every BATCH_SIZE updates
#         if count % BATCH_SIZE == 0:
#             batch.commit()
#             print(f"Committed {count} updates so far…")
#             batch = db.batch()

#     # commit any remaining updates
#     if count % BATCH_SIZE != 0:
#         batch.commit()

#     print(f"\n✅ Migration complete: {count} documents updated under allplaces/{parent_id}/{SUBCOLLECTION_NAME}.")

# if __name__ == "__main__":
#     migrate_top_attractions(PARENT_DOC_ID)



