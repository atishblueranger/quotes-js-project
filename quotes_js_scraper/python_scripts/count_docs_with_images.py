#!/usr/bin/env python3

import os
import argparse
import json
import time
import firebase_admin
from firebase_admin import credentials, firestore
from tqdm import tqdm

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

def fetch_docs_with_images_batched(db, collection_name, batch_size=500):
    """
    Fetches documents using pagination/batching to handle large collections efficiently.
    Returns document IDs where the 'image_url' field exists, is a list, and is non-empty.
    """
    col_ref = db.collection(collection_name)
    has_images = []
    
    # Get the total count first (optional but useful for progress tracking)
    try:
        # This is an expensive operation on large collections
        total_docs = len(list(col_ref.limit(1).stream()))
        print(f"Starting query of collection with approximately {total_docs} documents")
    except Exception as e:
        print(f"Could not determine total count: {e}")
        total_docs = "unknown"
    
    # Start with the first batch
    query = col_ref.limit(batch_size)
    docs_processed = 0
    batch_num = 1
    
    # Use a progress bar if total is known
    pbar = tqdm(total=total_docs if isinstance(total_docs, int) else None, 
                desc="Processing documents", unit="docs")
    
    while True:
        # Get the current batch
        start_time = time.time()
        docs = list(query.stream())
        batch_time = time.time() - start_time
        
        if not docs:
            # No more documents
            break
            
        batch_count = len(docs)
        docs_processed += batch_count
        
        # Process this batch
        for doc in docs:
            data = doc.to_dict() or {}
            image_urls = data.get('image_url')
            if isinstance(image_urls, list) and len(image_urls) > 0:
                has_images.append(doc.id)
        
        # Update progress
        pbar.update(batch_count)
        print(f"Batch {batch_num}: Processed {batch_count} docs in {batch_time:.2f}s, " 
              f"Found {len(has_images)} docs with images so far")
        
        # Set up for the next batch (using the last document as a cursor)
        last_doc = docs[-1]
        query = col_ref.start_after(last_doc).limit(batch_size)
        batch_num += 1
    
    pbar.close()
    print(f"Completed processing {docs_processed} documents")
    return has_images

def main():
    parser = argparse.ArgumentParser(
        description="Count and list 'allplaces' docs with a non-empty image_url field"
    )
    parser.add_argument(
        '--collection',
        default='allplaces',
        help="Firestore collection name (default: allplaces)"
    )
    parser.add_argument(
        '--output_file',
        required=False,
        help="If provided, writes the list of matching IDs to this JSON file"
    )
    parser.add_argument(
        '--batch_size',
        type=int,
        default=500,
        help="Number of documents to process in each batch (default: 500)"
    )
    args = parser.parse_args()

    db = initialize_firestore()
    
    print(f"\n🔍 Fetching documents from '{args.collection}' with batch size: {args.batch_size}")
    start_time = time.time()
    ids_with_images = fetch_docs_with_images_batched(db, args.collection, args.batch_size)
    total_time = time.time() - start_time
    count = len(ids_with_images)

    print(f"\n📊 Found {count} documents in '{args.collection}' with a non-empty image_url list.")
    print(f"⏱️ Total execution time: {total_time:.2f} seconds")

    if args.output_file:
        with open(args.output_file, 'w', encoding='utf-8') as f:
            json.dump(ids_with_images, f, indent=2, ensure_ascii=False)
        print(f"✅ Wrote {count} document IDs to {args.output_file}")
    else:
        # For large results, it might be better to only print the count and not the actual IDs
        if count > 100:
            print(f"Results too large to display ({count} items). Use --output_file to save to disk.")
        else:
            print(json.dumps(ids_with_images, indent=2, ensure_ascii=False))

if __name__ == '__main__':
    main()


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

# def fetch_docs_with_images(db, collection_name):
#     """
#     Streams all documents in `collection_name` and returns a list of
#     document IDs where the 'image_url' field exists, is a list, and is non-empty.
#     """
#     col_ref = db.collection(collection_name)
#     docs = col_ref.stream()

#     has_images = []
#     for doc in docs:
#         data = doc.to_dict() or {}
#         image_urls = data.get('image_url')
#         if isinstance(image_urls, list) and len(image_urls) > 0:
#             has_images.append(doc.id)

#     return has_images

# def main():
#     parser = argparse.ArgumentParser(
#         description="Count and list 'allplaces' docs with a non-empty image_url field"
#     )
#     parser.add_argument(
#         '--collection',
#         default='allplaces',
#         help="Firestore collection name (default: allplaces)"
#     )
#     parser.add_argument(
#         '--output_file',
#         required=False,
#         help="If provided, writes the list of matching IDs to this JSON file"
#     )
#     args = parser.parse_args()

#     db = initialize_firestore()
#     ids_with_images = fetch_docs_with_images(db, args.collection)
#     count = len(ids_with_images)

#     print(f"\n📊 Found {count} documents in '{args.collection}' with a non-empty image_url list.")

#     if args.output_file:
#         with open(args.output_file, 'w', encoding='utf-8') as f:
#             json.dump(ids_with_images, f, indent=2, ensure_ascii=False)
#         print(f"✅ Wrote {count} document IDs to {args.output_file}")
#     else:
#         # Print the list of IDs to stdout
#         print(json.dumps(ids_with_images, indent=2, ensure_ascii=False))

# if __name__ == '__main__':
#     main()
