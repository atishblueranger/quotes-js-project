import os
import argparse
import json
import logging
from datetime import datetime, date

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore import GeoPoint, DocumentReference

# ─── CONFIG: hard-coded path to your service-account JSON ────────────────────────
SERVICE_ACCOUNT_PATH = (
    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
    r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
)


def initialize_firestore() -> firestore.Client:
    """
    Initializes and returns a Firestore client using the hard-coded service account.
    """
    if not firebase_admin._apps:
        logging.info(f"Initializing Firebase Admin SDK with credentials: {SERVICE_ACCOUNT_PATH}")
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    else:
        logging.info("Firebase Admin SDK already initialized.")
    return firestore.client()


class FirestoreJSONEncoder(json.JSONEncoder):
    """Encodes Firestore types into JSON-serializable objects."""
    def default(self, obj):
        if isinstance(obj, GeoPoint):
            return {"latitude": obj.latitude, "longitude": obj.longitude}
        if isinstance(obj, DocumentReference):
            return obj.path
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def fetch_doc_and_subcols(
    db: firestore.Client,
    collection: str,
    doc_id: str,
    recurse: bool = False,
) -> dict:
    """
    Recursively fetches a document and its nested subcollections (if recurse=True).
    Returns a nested dict representing the document structure.
    """
    doc_ref = db.collection(collection).document(doc_id)
    snap = doc_ref.get()
    if not snap.exists:
        logging.error(f"Document '{doc_id}' not found in '{collection}'.")
        return {}

    result = {"_id": doc_id, **snap.to_dict()}
    subcols = list(doc_ref.collections())

    if subcols:
        result["subcollections"] = {}
        for subcol in subcols:
            col_name = subcol.id
            result["subcollections"][col_name] = []
            for subdoc in subcol.stream():
                data = subdoc.to_dict()
                if recurse:
                    nested = fetch_doc_and_subcols(
                        db,
                        f"{collection}/{doc_id}/{col_name}",
                        subdoc.id,
                        recurse
                    )
                    result["subcollections"][col_name].append(nested)
                else:
                    result["subcollections"][col_name].append({
                        "_id": subdoc.id,
                        **data
                    })
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Fetch a Firestore document and its subcollections."
    )
    parser.add_argument(
        "--doc_id", required=True,
        help="Document ID to retrieve."
    )
    parser.add_argument(
        "--collection", default="allplaces",
        help="Top-level collection name (default: allplaces)."
    )
    parser.add_argument(
        "--recurse", action="store_true",
        help="Recursively fetch nested subcollections."
    )
    parser.add_argument(
        "--output", default=None,
        help="File path for JSON output (defaults to '<doc_id>.json')."
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    # Initialize Firestore with the hard-coded path
    db = initialize_firestore()

    data = fetch_doc_and_subcols(
        db,
        args.collection,
        args.doc_id,
        recurse=args.recurse
    )

    # Serialize to JSON
    json_output = json.dumps(data, indent=2, ensure_ascii=False, cls=FirestoreJSONEncoder)

    # Determine output path (default to '<doc_id>.json')
    output_path = args.output or f"{args.doc_id}.json"
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(json_output)
        logging.info(f"✅ Output written to {output_path}")
    except Exception as e:
        logging.error(f"❌ Failed to write output: {e}")


if __name__ == '__main__':
    main()



# import os
# import argparse
# import json
# from datetime import datetime, date

# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore():
#     """Initializes the Firestore client."""
#     if not firebase_admin._apps:
#         firebase_credentials_path = (
#             r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#             r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#         )
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         print("✅ Firebase Admin SDK initialized.")
#     else:
#         print("ℹ️ Firebase Admin SDK already initialized.")
#     return firestore.client()

# class FirestoreJSONEncoder(json.JSONEncoder):
#     """JSON encoder that knows how to serialize Firestore types."""
#     def default(self, obj):
#         # GeoPoint -> dict
#         if hasattr(obj, 'latitude') and hasattr(obj, 'longitude'):
#             return {'latitude': obj.latitude, 'longitude': obj.longitude}
#         # DocumentReference -> its full path
#         if hasattr(obj, 'path') and isinstance(obj.path, str):
#             return obj.path
#         # Timestamp or datetime/date -> ISO string
#         if isinstance(obj, (datetime, date)):
#             return obj.isoformat()
#         return super().default(obj)

# def fetch_document_and_subcollections(doc_id):
#     """
#     Fetches and prints all fields and subcollections of `doc_id`
#     in the 'allplaces' collection, using FirestoreJSONEncoder to
#     handle GeoPoint, DocumentReference, and timestamp types.
#     """
#     db = initialize_firestore()
#     doc_ref = db.collection('explore').document(doc_id)

#     # Fetch the main document
#     snapshot = doc_ref.get()
#     if not snapshot.exists:
#         print(f"❌ Document '{doc_id}' not found in 'allplaces'.")
#         return

#     # Print top‐level fields
#     data = snapshot.to_dict() or {}
#     print(f"\n📄 Fields of document '{doc_id}':")
#     print(
#         json.dumps(
#             data,
#             indent=2,
#             ensure_ascii=False,
#             cls=FirestoreJSONEncoder
#         )
#     )

#     # Enumerate subcollections
#     print(f"\n🔎 Subcollections under '{doc_id}':")
#     subcols = list(doc_ref.collections())
#     if not subcols:
#         print("  (none)")
#         return

#     for subcol in subcols:
#         print(f"\n● Subcollection: {subcol.id}")
#         docs = list(subcol.stream())
#         if not docs:
#             print("    (empty)")
#             continue
#         for subdoc in docs:
#             subdata = subdoc.to_dict() or {}
#             print(f"    • Doc ID: {subdoc.id}")
#             print(
#                 json.dumps(
#                     subdata,
#                     indent=6,
#                     ensure_ascii=False,
#                     cls=FirestoreJSONEncoder
#                 )
#             )

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(
#         description="Fetch a document and its subcollections from 'allplaces'."
#     )
#     parser.add_argument(
#         '--doc_id',
#         required=True,
#         help="The document ID in 'allplaces' to retrieve."
#     )
#     args = parser.parse_args()
#     fetch_document_and_subcollections(args.doc_id)






# import os
# import argparse

# import firebase_admin
# from firebase_admin import credentials, firestore

# def initialize_firestore():
#     """Initializes the Firestore client."""
#     if not firebase_admin._apps:
#         # It's recommended to use environment variables for credential paths
#         # for better security and portability.
#         firebase_credentials_path = os.environ.get(
#             "FIREBASE_APPLICATION_CREDENTIALS",
#             # UPDATE THIS PATH IF NEEDED
#             r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#         )
#         if not os.path.exists(firebase_credentials_path):
#             raise FileNotFoundError(
#                 f"Firebase credentials not found at: {firebase_credentials_path}. "
#                 "Please set the FIREBASE_APPLICATION_CREDENTIALS environment variable "
#                 "or update the path in the script."
#             )
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         print("✅ Firebase Admin SDK initialized.")
#     else:
#         print("ℹ️ Firebase Admin SDK already initialized.")
#     return firestore.client()

# def delete_subcollection(doc_id, subcollection_name, batch_size=500):
#     """
#     Deletes all documents in a subcollection for a given document ID.
#     The subcollection is effectively deleted once all its documents are gone.

#     Args:
#         doc_id (str): The ID of the document in the 'allplaces' collection.
#         subcollection_name (str): The name of the subcollection to delete.
#         batch_size (int): The number of documents to delete in each batch.
#                           Max 500.
#     """
#     db = initialize_firestore()
#     parent_doc_ref = db.collection('allplaces').document(doc_id)
#     subcollection_ref = parent_doc_ref.collection(subcollection_name)

#     # Get all documents in the subcollection
#     docs = list(subcollection_ref.stream())

#     if not docs:
#         print(f"ℹ️ Subcollection '{subcollection_name}' for document '{doc_id}' is empty or does not exist. Nothing to do.")
#         return

#     print(f"🔥 Found {len(docs)} documents in subcollection '{subcollection_name}' for document '{doc_id}'.")
    
#     # Safety confirmation prompt
#     confirm = input("❓ Are you sure you want to PERMANENTLY delete them all? (y/N): ")
#     if confirm.lower() != 'y':
#         print("🚫 Deletion aborted by user.")
#         return

#     # Delete documents in batches
#     deleted_count = 0
#     for i in range(0, len(docs), batch_size):
#         batch = db.batch()
#         # Get the next chunk of documents
#         docs_chunk = docs[i:i + batch_size]
        
#         for doc in docs_chunk:
#             print(f"   - Queuing document for deletion: {doc.id}")
#             batch.delete(doc.reference)
        
#         # Commit the batch
#         batch.commit()
#         deleted_count += len(docs_chunk)
#         print(f"🚀 Committed batch. {deleted_count}/{len(docs)} documents deleted.")

#     print(f"\n✅ Successfully deleted all {deleted_count} documents from subcollection '{subcollection_name}'.")

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(
#         description="Delete a subcollection from a Firestore document in the 'allplaces' collection."
#     )
#     parser.add_argument(
#         '--doc_id',
#         required=True,
#         help="The document ID in 'allplaces' whose subcollection you want to delete."
#     )
#     parser.add_argument(
#         '--subcollection',
#         required=True,
#         help="The name of the subcollection to delete."
#     )
#     args = parser.parse_args()

#     # Add a final, hard-coded warning for safety
#     print("======================================================")
#     print("⚠️ WARNING: This script will permanently delete data. ⚠️")
#     print("======================================================")
    
#     delete_subcollection(args.doc_id, args.subcollection)