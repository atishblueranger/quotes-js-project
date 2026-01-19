# Delete certain ids in sucollection
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

# def delete_subcollection_documents(
#     collection_name: str,
#     parent_doc_id: str,
#     subcollection_name: str,
#     doc_ids_to_delete: list,
#     dry_run: bool = True,
#     show_data: bool = False
# ):
#     """
#     Delete specific documents from a subcollection.
    
#     Args:
#         collection_name: Parent collection name
#         parent_doc_id: Parent document ID
#         subcollection_name: Subcollection name
#         doc_ids_to_delete: List of document IDs to delete
#         dry_run: If True, only show what would be deleted
#         show_data: If True, display document data before deletion
#     """
#     init_firebase_once()
#     db = firestore.client()
    
#     logging.info("=" * 70)
#     logging.info("DELETE SUBCOLLECTION DOCUMENTS")
#     logging.info("=" * 70)
#     logging.info(f"Collection:       {collection_name}")
#     logging.info(f"Parent Doc ID:    {parent_doc_id}")
#     logging.info(f"Subcollection:    {subcollection_name}")
#     logging.info(f"Documents to delete: {len(doc_ids_to_delete)}")
#     logging.info(f"Mode:             {'DRY RUN (no changes)' if dry_run else 'LIVE DELETE'}")
#     logging.info("=" * 70)
#     logging.info("")
    
#     if not doc_ids_to_delete:
#         logging.warning("No document IDs provided. Nothing to delete.")
#         return
    
#     # Get parent document reference
#     parent_ref = db.collection(collection_name).document(parent_doc_id)
#     subcol_ref = parent_ref.collection(subcollection_name)
    
#     # Track results
#     deleted_count = 0
#     not_found_count = 0
#     error_count = 0
#     deleted_docs = []
#     not_found_docs = []
    
#     logging.info(f"Processing {len(doc_ids_to_delete)} documents...")
#     logging.info("")
    
#     # Process each document
#     for i, doc_id in enumerate(doc_ids_to_delete, 1):
#         logging.info(f"[{i}/{len(doc_ids_to_delete)}] Document: {doc_id}")
        
#         try:
#             doc_ref = subcol_ref.document(doc_id)
#             doc = doc_ref.get()
            
#             if not doc.exists:
#                 logging.warning(f"  ⚠️  Document does not exist - skipping")
#                 not_found_count += 1
#                 not_found_docs.append(doc_id)
#                 continue
            
#             # Show document data if requested
#             if show_data:
#                 data = doc.to_dict()
#                 logging.info(f"  📄 Document data:")
#                 logging.info(f"     {json.dumps(data, indent=6, default=str)[:300]}...")
            
#             if dry_run:
#                 logging.info(f"  🔍 Would DELETE this document")
#             else:
#                 doc_ref.delete()
#                 logging.info(f"  ✅ DELETED")
#                 deleted_count += 1
#                 deleted_docs.append(doc_id)
        
#         except Exception as e:
#             logging.error(f"  ❌ Error: {e}")
#             error_count += 1
    
#     # Summary
#     logging.info("")
#     logging.info("=" * 70)
#     logging.info("DELETION SUMMARY")
#     logging.info("=" * 70)
#     logging.info(f"Total documents requested:   {len(doc_ids_to_delete)}")
#     logging.info(f"{'Would be deleted' if dry_run else 'Successfully deleted'}:       {deleted_count if not dry_run else len(doc_ids_to_delete) - not_found_count}")
#     logging.info(f"Not found (skipped):         {not_found_count}")
#     logging.info(f"Errors:                      {error_count}")
#     logging.info("=" * 70)
    
#     if not_found_docs:
#         logging.info("")
#         logging.info("Documents not found:")
#         for doc_id in not_found_docs:
#             logging.info(f"  - {doc_id}")
    
#     if dry_run:
#         logging.info("")
#         logging.info("🔍 This was a DRY RUN - no documents were deleted")
#         logging.info("   Run with --execute to perform actual deletion")
#     else:
#         logging.info("")
#         logging.info("✅ Deletion complete!")
#         if deleted_docs:
#             logging.info(f"   {len(deleted_docs)} documents were permanently deleted")

# def delete_from_multiple_parents(
#     collection_name: str,
#     subcollection_name: str,
#     deletion_map: dict,
#     dry_run: bool = True,
#     show_data: bool = False
# ):
#     """
#     Delete documents from subcollections across multiple parent documents.
    
#     Args:
#         collection_name: Parent collection name
#         subcollection_name: Subcollection name
#         deletion_map: Dict mapping parent_doc_id -> [list of doc_ids to delete]
#         dry_run: If True, only show what would be deleted
#         show_data: If True, display document data before deletion
#     """
#     init_firebase_once()
#     db = firestore.client()
    
#     logging.info("=" * 70)
#     logging.info("BATCH DELETE FROM MULTIPLE PARENTS")
#     logging.info("=" * 70)
#     logging.info(f"Collection:       {collection_name}")
#     logging.info(f"Subcollection:    {subcollection_name}")
#     logging.info(f"Parent documents: {len(deletion_map)}")
#     logging.info(f"Mode:             {'DRY RUN (no changes)' if dry_run else 'LIVE DELETE'}")
#     logging.info("=" * 70)
#     logging.info("")
    
#     total_to_delete = sum(len(docs) for docs in deletion_map.values())
#     logging.info(f"Total documents to delete: {total_to_delete}")
#     logging.info("")
    
#     overall_deleted = 0
#     overall_not_found = 0
#     overall_errors = 0
    
#     for parent_idx, (parent_id, doc_ids) in enumerate(deletion_map.items(), 1):
#         logging.info("=" * 70)
#         logging.info(f"Parent [{parent_idx}/{len(deletion_map)}]: {parent_id}")
#         logging.info(f"Documents to delete: {len(doc_ids)}")
#         logging.info("=" * 70)
        
#         parent_ref = db.collection(collection_name).document(parent_id)
#         subcol_ref = parent_ref.collection(subcollection_name)
        
#         for i, doc_id in enumerate(doc_ids, 1):
#             logging.info(f"  [{i}/{len(doc_ids)}] Document: {doc_id}")
            
#             try:
#                 doc_ref = subcol_ref.document(doc_id)
#                 doc = doc_ref.get()
                
#                 if not doc.exists:
#                     logging.warning(f"    ⚠️  Not found - skipping")
#                     overall_not_found += 1
#                     continue
                
#                 if show_data:
#                     data = doc.to_dict()
#                     logging.info(f"    📄 Data: {json.dumps(data, default=str)[:150]}...")
                
#                 if dry_run:
#                     logging.info(f"    🔍 Would DELETE")
#                 else:
#                     doc_ref.delete()
#                     logging.info(f"    ✅ DELETED")
#                     overall_deleted += 1
            
#             except Exception as e:
#                 logging.error(f"    ❌ Error: {e}")
#                 overall_errors += 1
        
#         logging.info("")
    
#     # Final summary
#     logging.info("=" * 70)
#     logging.info("OVERALL SUMMARY")
#     logging.info("=" * 70)
#     logging.info(f"Total documents requested:   {total_to_delete}")
#     logging.info(f"{'Would be deleted' if dry_run else 'Successfully deleted'}:       {overall_deleted if not dry_run else total_to_delete - overall_not_found}")
#     logging.info(f"Not found (skipped):         {overall_not_found}")
#     logging.info(f"Errors:                      {overall_errors}")
#     logging.info("=" * 70)
    
#     if dry_run:
#         logging.info("")
#         logging.info("🔍 This was a DRY RUN - no documents were deleted")
#         logging.info("   Run with --execute to perform actual deletion")

# def main():
#     parser = argparse.ArgumentParser(
#         description="Delete specific documents from Firestore subcollections.",
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#         epilog="""
# Examples:
#   # Single parent - delete specific documents (dry run)
#   python %(prog)s --collection allplaces --parent-id PLACE123 \\
#     --subcollection top_attractions --doc-ids DOC1,DOC2,DOC3
  
#   # Single parent - execute deletion
#   python %(prog)s --collection allplaces --parent-id PLACE123 \\
#     --subcollection top_attractions --doc-ids DOC1,DOC2,DOC3 --execute
  
#   # Show data before deleting
#   python %(prog)s --collection allplaces --parent-id PLACE123 \\
#     --subcollection top_attractions --doc-ids DOC1,DOC2 --show-data
  
#   # Multiple parents - use JSON file
#   python %(prog)s --collection allplaces --subcollection top_attractions \\
#     --json-file documents_to_delete.json --execute
  
# JSON file format for multiple parents:
# {
#   "PARENT_DOC_ID_1": ["doc1", "doc2", "doc3"],
#   "PARENT_DOC_ID_2": ["doc4", "doc5"],
#   "PARENT_DOC_ID_3": ["doc6"]
# }

# JSON file format for single parent:
# {
#   "parent_id": "PLACE123",
#   "doc_ids": ["doc1", "doc2", "doc3"]
# }
#         """
#     )
    
#     parser.add_argument("--collection", required=True,
#                        help="Parent collection name")
#     parser.add_argument("--parent-id",
#                        help="Parent document ID (for single parent deletion)")
#     parser.add_argument("--subcollection", required=True,
#                        help="Subcollection name")
#     parser.add_argument("--doc-ids",
#                        help="Comma-separated list of document IDs to delete")
#     parser.add_argument("--json-file",
#                        help="JSON file containing documents to delete")
#     parser.add_argument("--execute", action="store_true",
#                        help="Execute the deletion (default is dry-run)")
#     parser.add_argument("--show-data", action="store_true",
#                        help="Display document data before deletion")
    
#     args = parser.parse_args()
    
#     # Validate input
#     if not args.json_file and not (args.parent_id and args.doc_ids):
#         parser.error("Either --json-file OR (--parent-id AND --doc-ids) must be provided")
    
#     # Mode 1: Single parent with doc IDs from command line
#     if args.parent_id and args.doc_ids:
#         doc_ids = [x.strip() for x in args.doc_ids.split(',')]
#         delete_subcollection_documents(
#             collection_name=args.collection,
#             parent_doc_id=args.parent_id,
#             subcollection_name=args.subcollection,
#             doc_ids_to_delete=doc_ids,
#             dry_run=not args.execute,
#             show_data=args.show_data
#         )
    
#     # Mode 2: JSON file (single or multiple parents)
#     elif args.json_file:
#         if not os.path.exists(args.json_file):
#             parser.error(f"JSON file not found: {args.json_file}")
        
#         with open(args.json_file, 'r', encoding='utf-8') as f:
#             data = json.load(f)
        
#         # Check if it's single parent format
#         if 'parent_id' in data and 'doc_ids' in data:
#             delete_subcollection_documents(
#                 collection_name=args.collection,
#                 parent_doc_id=data['parent_id'],
#                 subcollection_name=args.subcollection,
#                 doc_ids_to_delete=data['doc_ids'],
#                 dry_run=not args.execute,
#                 show_data=args.show_data
#             )
#         # Multiple parents format
#         else:
#             delete_from_multiple_parents(
#                 collection_name=args.collection,
#                 subcollection_name=args.subcollection,
#                 deletion_map=data,
#                 dry_run=not args.execute,
#                 show_data=args.show_data
#             )

# if __name__ == "__main__":
#     main()



#!/usr/bin/env python3
import os, logging, argparse, json
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore

# ──────────────────────────────────────────
# UPDATE THIS PATH TO YOUR KEY FILE
SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def init_firebase_once():
    """Initialize Firebase Admin SDK once."""
    try:
        firebase_admin.get_app()
    except ValueError:
        if not os.path.exists(SERVICE_ACCOUNT_PATH):
            raise SystemExit(f"Service account not found:\n{SERVICE_ACCOUNT_PATH}")
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)

def delete_entire_subcollection(
    collection_name: str,
    parent_doc_id: str,
    subcollection_name: str,
    dry_run: bool = True
):
    """
    Fetch ALL documents in a subcollection and delete them.
    """
    init_firebase_once()
    db = firestore.client()
    
    # 1. Get Reference
    parent_ref = db.collection(collection_name).document(parent_doc_id)
    subcol_ref = parent_ref.collection(subcollection_name)
    
    # 2. Fetch all documents (batch size can be adjusted for huge collections)
    logging.info("Fetching documents... please wait.")
    docs = list(subcol_ref.stream())
    
    logging.info("=" * 70)
    logging.info("DELETE ENTIRE SUBCOLLECTION")
    logging.info("=" * 70)
    logging.info(f"Collection:       {collection_name}")
    logging.info(f"Parent Doc ID:    {parent_doc_id}")
    logging.info(f"Subcollection:    {subcollection_name}")
    logging.info(f"Total found:      {len(docs)}")
    logging.info(f"Mode:             {'DRY RUN' if dry_run else 'LIVE DELETE'}")
    logging.info("=" * 70)

    if not docs:
        logging.info("Subcollection is already empty.")
        return

    # 3. Process Deletion
    batch_size = 500
    deleted_count = 0
    
    if dry_run:
        logging.info("")
        logging.info(f"🔍 The following {len(docs)} documents WOULD be deleted:")
        for doc in docs[:10]: # Only show first 10 ID to keep log clean
            logging.info(f"   - {doc.id}")
        if len(docs) > 10:
            logging.info(f"   ... and {len(docs)-10} more.")
        
        logging.info("")
        logging.info("🔍 This was a DRY RUN. Use --execute to delete.")
        return

    # 4. Actual Deletion (using Batches for efficiency)
    batch = db.batch()
    for i, doc in enumerate(docs):
        batch.delete(doc.reference)
        deleted_count += 1
        
        # Commit every 500 docs (Firestore limit)
        if deleted_count % batch_size == 0:
            batch.commit()
            logging.info(f"   Committed batch delete of {batch_size} docs...")
            batch = db.batch()

    # Commit remaining
    if deleted_count % batch_size != 0:
        batch.commit()
    
    logging.info("")
    logging.info(f"✅ Successfully deleted {deleted_count} documents.")

# (Keep previous functions: delete_subcollection_documents, delete_from_multiple_parents...)
# For brevity, I am not pasting the old functions here, but you should keep them in the file.
# Below is the UPDATED main() function.

def main():
    parser = argparse.ArgumentParser(description="Delete documents from Firestore.")
    
    parser.add_argument("--collection", required=True, help="Parent collection name")
    parser.add_argument("--parent-id", help="Parent document ID")
    parser.add_argument("--subcollection", required=True, help="Subcollection name")
    
    # Arguments for specific deletion
    parser.add_argument("--doc-ids", help="Comma-separated list of IDs")
    parser.add_argument("--json-file", help="JSON file input")
    
    # NEW ARGUMENT
    parser.add_argument("--delete-all", action="store_true", 
                        help="Delete ALL documents in the specified subcollection")
    
    parser.add_argument("--execute", action="store_true", help="Execute the deletion")
    parser.add_argument("--show-data", action="store_true", help="Show data (for specific delete)")
    
    args = parser.parse_args()
    
    # Mode 1: Delete ALL in subcollection
    if args.delete_all:
        if not args.parent_id:
            parser.error("--delete-all requires --parent-id")
        
        delete_entire_subcollection(
            collection_name=args.collection,
            parent_doc_id=args.parent_id,
            subcollection_name=args.subcollection,
            dry_run=not args.execute
        )

    # Mode 2: Specific Doc IDs (The original logic)
    elif args.parent_id and args.doc_ids:
        # ... (Call your original delete_subcollection_documents function here)
        pass # Placeholder for your existing code logic
        
    # Mode 3: JSON File (The original logic)
    elif args.json_file:
         # ... (Call your original logic here)
        pass # Placeholder for your existing code logic

    else:
        parser.error("You must provide either --doc-ids, --json-file, OR --delete-all")

if __name__ == "__main__":
    main()