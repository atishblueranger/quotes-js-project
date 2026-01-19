#!/usr/bin/env python3
import os, logging, argparse, json
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore

# ──────────────────────────────────────────
SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def init_firebase_once():
    """Initialize Firebase Admin SDK once."""
    try:
        firebase_admin.get_app()
        logging.info("Firebase app already initialized.")
    except ValueError:
        if not os.path.exists(SERVICE_ACCOUNT_PATH):
            raise SystemExit(f"Service account not found:\n{SERVICE_ACCOUNT_PATH}")
        logging.info(f"Initializing Firebase app with:\n{SERVICE_ACCOUNT_PATH}")
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)

def inspect_field_data(collection_name: str, field_name: str, doc_ids: list):
    """
    Inspect the structure of the field data before migration.
    """
    init_firebase_once()
    db = firestore.client()
    
    logging.info("=" * 70)
    logging.info("INSPECTING FIELD DATA STRUCTURE")
    logging.info("=" * 70)
    logging.info(f"Collection: {collection_name}")
    logging.info(f"Field:      {field_name}")
    logging.info("=" * 70)
    logging.info("")
    
    for doc_id in doc_ids[:3]:  # Inspect first 3 documents
        doc_ref = db.collection(collection_name).document(doc_id)
        doc = doc_ref.get()
        
        if doc.exists:
            data = doc.to_dict()
            field_data = data.get(field_name)
            
            logging.info(f"Document ID: {doc_id}")
            logging.info(f"Field type:  {type(field_data).__name__}")
            
            if isinstance(field_data, dict):
                logging.info(f"Keys found:  {list(field_data.keys())}")
                for key, value in field_data.items():
                    logging.info(f"  - '{key}': {type(value).__name__} with {len(value) if isinstance(value, (list, dict)) else 1} items")
            
            logging.info(f"Field data preview:")
            logging.info(json.dumps(field_data, indent=2, default=str)[:800])
            logging.info("-" * 70)
    
    logging.info("")

def migrate_field_to_subcollection(
    collection_name: str,
    field_name: str,
    doc_ids: list = None,
    dry_run: bool = True,
    id_field: str = None
):
    """
    Migrate nested field data to Firestore subcollections.
    Expects field_data structure like: {"places": [...], "restaurants": [...]}
    Each key becomes a subcollection name, each array item becomes a document.
    
    Args:
        collection_name: Parent collection name
        field_name: Field containing data to migrate
        doc_ids: List of document IDs to process (if None, find all)
        dry_run: If True, only show what would be done
        id_field: Field name to use as document ID (e.g., 'placeId')
    """
    init_firebase_once()
    db = firestore.client()
    
    logging.info("=" * 70)
    logging.info("MIGRATING FIELD TO SUBCOLLECTIONS")
    logging.info("=" * 70)
    logging.info(f"Collection:       {collection_name}")
    logging.info(f"Field:            {field_name}")
    logging.info(f"ID Field:         {id_field if id_field else 'Auto-generated'}")
    logging.info(f"Mode:             {'DRY RUN (no changes)' if dry_run else 'LIVE (will write data)'}")
    logging.info("=" * 70)
    logging.info("")
    
    # Find documents if not provided
    if doc_ids is None:
        logging.info("Finding documents with the field...")
        docs = db.collection(collection_name).stream()
        doc_ids = []
        for doc in docs:
            data = doc.to_dict() or {}
            if field_name in data:
                doc_ids.append(doc.id)
        logging.info(f"Found {len(doc_ids)} documents with field '{field_name}'")
        logging.info("")
    
    if not doc_ids:
        logging.warning("No documents to process!")
        return
    
    # Process each document
    success_count = 0
    error_count = 0
    skipped_count = 0
    total_subcollections_created = 0
    total_documents_created = 0
    
    for i, doc_id in enumerate(doc_ids, 1):
        logging.info(f"[{i}/{len(doc_ids)}] Processing document: {doc_id}")
        
        try:
            # Get parent document
            parent_ref = db.collection(collection_name).document(doc_id)
            parent_doc = parent_ref.get()
            
            if not parent_doc.exists:
                logging.warning(f"  ⚠️  Document does not exist: {doc_id}")
                skipped_count += 1
                continue
            
            data = parent_doc.to_dict()
            field_data = data.get(field_name)
            
            if field_data is None:
                logging.warning(f"  ⚠️  Field '{field_name}' not found in document")
                skipped_count += 1
                continue
            
            if not isinstance(field_data, dict):
                logging.warning(f"  ⚠️  Field '{field_name}' is not a dict. Found: {type(field_data).__name__}")
                logging.warning(f"      Expected structure: {{\"subcollection_name\": [array of items]}}")
                skipped_count += 1
                continue
            
            # Process each key in the dict as a subcollection
            doc_subcollections = 0
            doc_documents = 0
            
            for subcollection_name, items in field_data.items():
                if not isinstance(items, list):
                    logging.warning(f"  ⚠️  Key '{subcollection_name}' value is not a list. Skipping.")
                    continue
                
                logging.info(f"  📁 Creating subcollection '{subcollection_name}' with {len(items)} documents")
                
                subcollection_ref = parent_ref.collection(subcollection_name)
                
                # Create documents with auto-generated IDs or custom ID field
                if dry_run:
                    logging.info(f"     🔍 Would create {len(items)} documents:")
                    for idx, item in enumerate(items[:3], 1):  # Show first 3
                        # Determine document ID
                        if id_field and isinstance(item, dict) and id_field in item:
                            doc_id_to_use = str(item[id_field])
                        else:
                            doc_id_to_use = "Auto-ID"
                        
                        if isinstance(item, dict):
                            preview = {k: v for k, v in list(item.items())[:3]}
                            logging.info(f"        {idx}. {doc_id_to_use}: {json.dumps(preview, default=str)[:80]}...")
                        else:
                            logging.info(f"        {idx}. {doc_id_to_use}: {json.dumps(item, default=str)[:80]}")
                    if len(items) > 3:
                        logging.info(f"        ... and {len(items) - 3} more")
                else:
                    batch = db.batch()
                    batch_count = 0
                    
                    for item in items:
                        # Ensure item is a dict
                        if not isinstance(item, dict):
                            item = {'value': item}
                        
                        # Determine document ID
                        if id_field and id_field in item:
                            doc_id_to_use = str(item[id_field])
                            doc_ref = subcollection_ref.document(doc_id_to_use)
                        else:
                            # Fall back to auto-generated ID
                            doc_ref = subcollection_ref.document()
                        
                        batch.set(doc_ref, item)
                        batch_count += 1
                        doc_documents += 1
                        
                        # Commit batch every 500 documents (Firestore limit)
                        if batch_count >= 500:
                            batch.commit()
                            batch = db.batch()
                            batch_count = 0
                    
                    # Commit remaining documents
                    if batch_count > 0:
                        batch.commit()
                    
                    logging.info(f"     ✅ Created {len(items)} documents in '{subcollection_name}'")
                
                doc_subcollections += 1
                total_documents_created += len(items)
            
            total_subcollections_created += doc_subcollections
            
            if doc_subcollections > 0:
                logging.info(f"  ✅ Processed {doc_subcollections} subcollection(s), {doc_documents} document(s)")
                success_count += 1
            else:
                logging.warning(f"  ⚠️  No valid subcollections found in document")
                skipped_count += 1
            
        except Exception as e:
            logging.error(f"  ❌ Error processing document {doc_id}: {e}")
            import traceback
            logging.error(traceback.format_exc())
            error_count += 1
        
        logging.info("")
    
    # Summary
    logging.info("=" * 70)
    logging.info("MIGRATION SUMMARY")
    logging.info("=" * 70)
    logging.info(f"Total parent documents:      {len(doc_ids)}")
    logging.info(f"Successfully {'checked' if dry_run else 'migrated'}:       {success_count}")
    logging.info(f"Skipped:                     {skipped_count}")
    logging.info(f"Errors:                      {error_count}")
    logging.info(f"Total subcollections:        {total_subcollections_created}")
    logging.info(f"Total documents created:     {total_documents_created}")
    logging.info("=" * 70)
    
    if dry_run:
        logging.info("")
        logging.info("🔍 This was a DRY RUN - no data was written")
        logging.info("   Run with --execute to perform the actual migration")
    else:
        logging.info("")
        logging.info("✅ Migration complete!")
        logging.info("   Remember to manually delete the old field from parent documents")
        logging.info("")
        logging.info("💡 To delete the field, you can run:")
        logging.info(f"   # Example for one document:")
        logging.info(f"   db.collection('{collection_name}').document('DOC_ID').update({{'{field_name}': firestore.DELETE_FIELD}})")

def main():
    parser = argparse.ArgumentParser(
        description="Migrate nested field data to Firestore subcollections.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Inspect data structure first
  python %(prog)s --collection playlistsNew --field subcollections --inspect --doc-ids 433
  
  # Dry run with placeId as document ID
  python %(prog)s --collection playlistsNew --field subcollections --doc-ids 433 --id-field placeId
  
  # Execute migration for all 12 documents using placeId
  python %(prog)s --collection playlistsNew --field subcollections \\
    --doc-ids 433,434,435,436,437,438,439,440,441,442,443,444 \\
    --id-field placeId --execute
  
  # Execute for all documents with the field (auto-find)
  python %(prog)s --collection playlistsNew --field subcollections \\
    --id-field placeId --execute

Expected field structure:
  {
    "places": [
      {"placeId": "ChTJ2wq...", "name": "Place 1", ...},
      {"placeId": "ChIJ5xY...", "name": "Place 2", ...}
    ],
    "restaurants": [
      {"placeId": "ChABc12...", "name": "Restaurant 1", ...}
    ]
  }
  
Result with --id-field placeId:
  - Creates subcollection "places" with document IDs from placeId field
  - playlistsNew/432/places/ChTJ2wq...
  - playlistsNew/432/places/ChIJ5xY...
        """
    )
    
    parser.add_argument("--collection", required=True,
                       help="Parent collection name")
    parser.add_argument("--field", required=True,
                       help="Field name containing data to migrate")
    parser.add_argument("--doc-ids",
                       help="Comma-separated list of document IDs to process")
    parser.add_argument("--id-field",
                       help="Field name to use as document ID (e.g., 'placeId')")
    parser.add_argument("--execute", action="store_true",
                       help="Execute the migration (default is dry-run)")
    parser.add_argument("--inspect", action="store_true",
                       help="Only inspect the data structure without migration")
    
    args = parser.parse_args()
    
    # Parse doc IDs
    doc_ids = None
    if args.doc_ids:
        doc_ids = [x.strip() for x in args.doc_ids.split(',')]
    
    # Inspect mode
    if args.inspect:
        if not doc_ids:
            # Find all documents with the field
            init_firebase_once()
            db = firestore.client()
            docs = db.collection(args.collection).stream()
            doc_ids = []
            for doc in docs:
                data = doc.to_dict() or {}
                if args.field in data:
                    doc_ids.append(doc.id)
        
        inspect_field_data(args.collection, args.field, doc_ids)
        return
    
    # Migration mode
    migrate_field_to_subcollection(
        collection_name=args.collection,
        field_name=args.field,
        doc_ids=doc_ids,
        dry_run=not args.execute,
        id_field=args.id_field
    )

if __name__ == "__main__":
    main()

# #!/usr/bin/env python3
# import os, logging, argparse, json
# from datetime import datetime
# import firebase_admin
# from firebase_admin import credentials, firestore

# # ──────────────────────────────────────────
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# def init_firebase_once():
#     """Initialize Firebase Admin SDK once."""
#     try:
#         firebase_admin.get_app()
#         logging.info("Firebase app already initialized.")
#     except ValueError:
#         if not os.path.exists(SERVICE_ACCOUNT_PATH):
#             raise SystemExit(f"Service account not found:\n{SERVICE_ACCOUNT_PATH}")
#         logging.info(f"Initializing Firebase app with:\n{SERVICE_ACCOUNT_PATH}")
#         cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
#         firebase_admin.initialize_app(cred)

# def inspect_field_data(collection_name: str, field_name: str, doc_ids: list):
#     """
#     Inspect the structure of the field data before migration.
#     """
#     init_firebase_once()
#     db = firestore.client()
    
#     logging.info("=" * 70)
#     logging.info("INSPECTING FIELD DATA STRUCTURE")
#     logging.info("=" * 70)
#     logging.info(f"Collection: {collection_name}")
#     logging.info(f"Field:      {field_name}")
#     logging.info("=" * 70)
#     logging.info("")
    
#     for doc_id in doc_ids[:3]:  # Inspect first 3 documents
#         doc_ref = db.collection(collection_name).document(doc_id)
#         doc = doc_ref.get()
        
#         if doc.exists:
#             data = doc.to_dict()
#             field_data = data.get(field_name)
            
#             logging.info(f"Document ID: {doc_id}")
#             logging.info(f"Field type:  {type(field_data).__name__}")
#             logging.info(f"Field data preview:")
#             logging.info(json.dumps(field_data, indent=2, default=str)[:500])
#             logging.info("-" * 70)
    
#     logging.info("")

# def migrate_field_to_subcollection(
#     collection_name: str,
#     field_name: str,
#     subcollection_name: str,
#     doc_ids: list = None,
#     dry_run: bool = True,
#     auto_id: bool = False
# ):
#     """
#     Migrate data from a field to a proper Firestore subcollection.
    
#     Args:
#         collection_name: Parent collection name
#         field_name: Field containing data to migrate
#         subcollection_name: Name for the new subcollection
#         doc_ids: List of document IDs to process (if None, find all)
#         dry_run: If True, only show what would be done
#         auto_id: If True, use Firestore auto-generated IDs for subcollection docs
#     """
#     init_firebase_once()
#     db = firestore.client()
    
#     logging.info("=" * 70)
#     logging.info("MIGRATING FIELD TO SUBCOLLECTION")
#     logging.info("=" * 70)
#     logging.info(f"Collection:       {collection_name}")
#     logging.info(f"Field:            {field_name}")
#     logging.info(f"Subcollection:    {subcollection_name}")
#     logging.info(f"Mode:             {'DRY RUN (no changes)' if dry_run else 'LIVE (will write data)'}")
#     logging.info(f"Auto IDs:         {auto_id}")
#     logging.info("=" * 70)
#     logging.info("")
    
#     # Find documents if not provided
#     if doc_ids is None:
#         logging.info("Finding documents with the field...")
#         docs = db.collection(collection_name).stream()
#         doc_ids = []
#         for doc in docs:
#             data = doc.to_dict() or {}
#             if field_name in data:
#                 doc_ids.append(doc.id)
#         logging.info(f"Found {len(doc_ids)} documents with field '{field_name}'")
#         logging.info("")
    
#     if not doc_ids:
#         logging.warning("No documents to process!")
#         return
    
#     # Process each document
#     success_count = 0
#     error_count = 0
#     skipped_count = 0
    
#     for i, doc_id in enumerate(doc_ids, 1):
#         logging.info(f"[{i}/{len(doc_ids)}] Processing document: {doc_id}")
        
#         try:
#             # Get parent document
#             parent_ref = db.collection(collection_name).document(doc_id)
#             parent_doc = parent_ref.get()
            
#             if not parent_doc.exists:
#                 logging.warning(f"  ⚠️  Document does not exist: {doc_id}")
#                 skipped_count += 1
#                 continue
            
#             data = parent_doc.to_dict()
#             field_data = data.get(field_name)
            
#             if field_data is None:
#                 logging.warning(f"  ⚠️  Field '{field_name}' not found in document")
#                 skipped_count += 1
#                 continue
            
#             # Handle different data types
#             subcollection_ref = parent_ref.collection(subcollection_name)
#             items_to_write = []
            
#             if isinstance(field_data, list):
#                 # If it's a list, create numbered documents
#                 logging.info(f"  📋 Field is a list with {len(field_data)} items")
#                 for idx, item in enumerate(field_data):
#                     if auto_id:
#                         doc_ref = subcollection_ref.document()
#                         doc_id_to_use = doc_ref.id
#                     else:
#                         doc_id_to_use = str(idx)
#                         doc_ref = subcollection_ref.document(doc_id_to_use)
                    
#                     # If item is not a dict, wrap it
#                     if not isinstance(item, dict):
#                         item = {'value': item}
                    
#                     items_to_write.append((doc_ref, item, doc_id_to_use))
            
#             elif isinstance(field_data, dict):
#                 # If it's a dict, use keys as document IDs
#                 logging.info(f"  📋 Field is a dict with {len(field_data)} items")
#                 for key, value in field_data.items():
#                     if auto_id:
#                         doc_ref = subcollection_ref.document()
#                         doc_id_to_use = doc_ref.id
#                     else:
#                         doc_id_to_use = str(key)
#                         doc_ref = subcollection_ref.document(doc_id_to_use)
                    
#                     # If value is not a dict, wrap it
#                     if not isinstance(value, dict):
#                         value = {'key': key, 'value': value}
                    
#                     items_to_write.append((doc_ref, value, doc_id_to_use))
            
#             else:
#                 # Single value - create one document
#                 logging.info(f"  📋 Field is a single value ({type(field_data).__name__})")
#                 if auto_id:
#                     doc_ref = subcollection_ref.document()
#                     doc_id_to_use = doc_ref.id
#                 else:
#                     doc_id_to_use = "data"
#                     doc_ref = subcollection_ref.document(doc_id_to_use)
                
#                 value = {'value': field_data} if not isinstance(field_data, dict) else field_data
#                 items_to_write.append((doc_ref, value, doc_id_to_use))
            
#             # Write to subcollection
#             if dry_run:
#                 logging.info(f"  🔍 Would create {len(items_to_write)} documents in subcollection:")
#                 for doc_ref, item, item_id in items_to_write[:3]:  # Show first 3
#                     logging.info(f"      - {item_id}: {json.dumps(item, default=str)[:100]}")
#                 if len(items_to_write) > 3:
#                     logging.info(f"      ... and {len(items_to_write) - 3} more")
#             else:
#                 batch = db.batch()
#                 for doc_ref, item, item_id in items_to_write:
#                     batch.set(doc_ref, item)
#                 batch.commit()
#                 logging.info(f"  ✅ Created {len(items_to_write)} documents in subcollection '{subcollection_name}'")
            
#             success_count += 1
            
#         except Exception as e:
#             logging.error(f"  ❌ Error processing document {doc_id}: {e}")
#             error_count += 1
        
#         logging.info("")
    
#     # Summary
#     logging.info("=" * 70)
#     logging.info("MIGRATION SUMMARY")
#     logging.info("=" * 70)
#     logging.info(f"Total documents:     {len(doc_ids)}")
#     logging.info(f"Successfully {'checked' if dry_run else 'migrated'}:   {success_count}")
#     logging.info(f"Skipped:             {skipped_count}")
#     logging.info(f"Errors:              {error_count}")
#     logging.info("=" * 70)
    
#     if dry_run:
#         logging.info("")
#         logging.info("🔍 This was a DRY RUN - no data was written")
#         logging.info("   Run with --execute to perform the actual migration")
#     else:
#         logging.info("")
#         logging.info("✅ Migration complete!")
#         logging.info("   Remember to manually delete the old field from parent documents")

# def main():
#     parser = argparse.ArgumentParser(
#         description="Migrate field data to Firestore subcollection.",
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#         epilog="""
# Examples:
#   # Inspect data structure first
#   python %(prog)s --collection playlistsNew --field subcollections \\
#     --subcollection-name items --inspect
  
#   # Dry run (see what would happen)
#   python %(prog)s --collection playlistsNew --field subcollections \\
#     --subcollection-name items --doc-ids 433,434,435
  
#   # Execute migration with specific documents
#   python %(prog)s --collection playlistsNew --field subcollections \\
#     --subcollection-name items --doc-ids 433,434,435,436,437,438,439,440,441,442,443,444 \\
#     --execute
  
#   # Execute with auto-generated IDs
#   python %(prog)s --collection playlistsNew --field subcollections \\
#     --subcollection-name items --execute --auto-id
#         """
#     )
    
#     parser.add_argument("--collection", required=True,
#                        help="Parent collection name")
#     parser.add_argument("--field", required=True,
#                        help="Field name containing data to migrate")
#     parser.add_argument("--subcollection-name", required=True,
#                        help="Name for the new subcollection")
#     parser.add_argument("--doc-ids",
#                        help="Comma-separated list of document IDs to process")
#     parser.add_argument("--execute", action="store_true",
#                        help="Execute the migration (default is dry-run)")
#     parser.add_argument("--auto-id", action="store_true",
#                        help="Use Firestore auto-generated IDs for subcollection documents")
#     parser.add_argument("--inspect", action="store_true",
#                        help="Only inspect the data structure without migration")
    
#     args = parser.parse_args()
    
#     # Parse doc IDs
#     doc_ids = None
#     if args.doc_ids:
#         doc_ids = [x.strip() for x in args.doc_ids.split(',')]
    
#     # Inspect mode
#     if args.inspect:
#         if not doc_ids:
#             # Find all documents with the field
#             init_firebase_once()
#             db = firestore.client()
#             docs = db.collection(args.collection).stream()
#             doc_ids = []
#             for doc in docs:
#                 data = doc.to_dict() or {}
#                 if args.field in data:
#                     doc_ids.append(doc.id)
        
#         inspect_field_data(args.collection, args.field, doc_ids)
#         return
    
#     # Migration mode
#     migrate_field_to_subcollection(
#         collection_name=args.collection,
#         field_name=args.field,
#         subcollection_name=args.subcollection_name,
#         doc_ids=doc_ids,
#         dry_run=not args.execute,
#         auto_id=args.auto_id
#     )

# if __name__ == "__main__":
#     main()