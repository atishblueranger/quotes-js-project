# #!/usr/bin/env python3
# import os, logging, time, argparse, json
# import firebase_admin
# from firebase_admin import credentials, firestore

# # ──────────────────────────────────────────
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# # Hardcoded JSON file path - UPDATE THIS with your JSON file path
# JSON_FILE_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\missing_is_searchable_allplaces_20251005_115134.json"

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

# def update_from_json(collection_name: str,
#                     json_file: str,
#                     batch_size: int = 50,
#                     sleep_between_batches: float = 1.0,
#                     dry_run: bool = False,
#                     field_value: bool = True,
#                     max_docs: int = None):
#     """
#     Update documents from a JSON file containing missing document IDs.
    
#     Args:
#         collection_name: Name of the Firestore collection
#         json_file: Path to JSON file with missing document IDs
#         batch_size: Number of documents to update per batch
#         sleep_between_batches: Seconds to sleep between batches
#         dry_run: If True, only simulate updates without writing
#         field_value: Value to set for is_searchable field
#         max_docs: Maximum number of documents to update (for testing)
#     """
    
#     init_firebase_once()
#     db = firestore.client()
#     collection_ref = db.collection(collection_name)
    
#     # Load JSON file
#     logging.info(f"Loading document IDs from: {json_file}")
#     try:
#         with open(json_file, 'r', encoding='utf-8') as f:
#             data = json.load(f)
        
#         doc_ids = data.get('missing_document_ids', [])
#         metadata = data.get('metadata', {})
        
#         logging.info(f"Loaded {len(doc_ids):,} document IDs from JSON")
#         logging.info(f"Original scan date: {metadata.get('scan_timestamp', 'Unknown')}")
#         logging.info(f"Original collection: {metadata.get('collection', 'Unknown')}")
#         logging.info("")
        
#         if not doc_ids:
#             logging.warning("No document IDs found in JSON file!")
#             return
            
#     except FileNotFoundError:
#         logging.error(f"JSON file not found: {json_file}")
#         return
#     except json.JSONDecodeError as e:
#         logging.error(f"Invalid JSON file: {e}")
#         return
#     except Exception as e:
#         logging.error(f"Error reading JSON file: {e}")
#         return
    
#     # Limit docs if max_docs specified
#     if max_docs and max_docs < len(doc_ids):
#         logging.info(f"Limiting to first {max_docs:,} documents (testing mode)")
#         doc_ids = doc_ids[:max_docs]
    
#     # Counters
#     total_processed = 0
#     updated_count = 0
#     error_count = 0
#     already_has_field = 0
#     doc_not_found = 0
    
#     start_time = time.time()
#     mode = "DRY RUN" if dry_run else "LIVE UPDATE"
    
#     logging.info("=" * 70)
#     logging.info(f"🚀 Starting {mode} from JSON file")
#     logging.info("=" * 70)
#     logging.info(f"Collection:       {collection_name}")
#     logging.info(f"Field to add:     is_searchable = {field_value}")
#     logging.info(f"Total docs:       {len(doc_ids):,}")
#     logging.info(f"Batch size:       {batch_size}")
#     logging.info(f"Sleep interval:   {sleep_between_batches}s")
#     logging.info("=" * 70)
#     logging.info("")
    
#     try:
#         # Process in batches
#         for i in range(0, len(doc_ids), batch_size):
#             batch = doc_ids[i:i + batch_size]
            
#             for doc_id in batch:
#                 total_processed += 1
                
#                 try:
#                     # Get document reference
#                     doc_ref = collection_ref.document(str(doc_id))
                    
#                     # Check if document exists and if it already has the field
#                     doc = doc_ref.get()
                    
#                     if not doc.exists:
#                         doc_not_found += 1
#                         logging.warning(f"Document not found: {doc_id}")
#                         continue
                    
#                     data = doc.to_dict() or {}
                    
#                     # Check if field already exists
#                     if 'is_searchable' in data:
#                         already_has_field += 1
#                         logging.debug(f"Document {doc_id} already has field, skipping")
#                         continue
                    
#                     # Update the document
#                     if dry_run:
#                         logging.debug(f"[DRY RUN] Would update {doc_id}")
#                         updated_count += 1
#                     else:
#                         doc_ref.update({'is_searchable': field_value})
#                         updated_count += 1
                        
#                 except Exception as e:
#                     error_count += 1
#                     logging.error(f"Error updating {doc_id}: {e}")
            
#             # Progress logging
#             if total_processed % 500 == 0 or total_processed == len(doc_ids):
#                 elapsed = time.time() - start_time
#                 rate = total_processed / elapsed if elapsed > 0 else 0
#                 remaining = len(doc_ids) - total_processed
#                 eta = remaining / rate if rate > 0 else 0
                
#                 logging.info("─" * 70)
#                 logging.info(f"📊 PROGRESS - Processed {total_processed:,}/{len(doc_ids):,} ({total_processed/len(doc_ids)*100:.1f}%)")
#                 logging.info(f"   Rate: {rate:.1f} docs/sec | ETA: {eta/60:.1f} minutes")
#                 logging.info("─" * 70)
#                 logging.info(f"  🔄 Updated:           {updated_count:6,} ({updated_count/total_processed*100:.1f}%)")
#                 logging.info(f"  ✅ Already had field: {already_has_field:6,} ({already_has_field/total_processed*100:.1f}%)")
#                 if doc_not_found > 0:
#                     logging.info(f"  ⚠️  Not found:        {doc_not_found:6,} ({doc_not_found/total_processed*100:.1f}%)")
#                 if error_count > 0:
#                     logging.info(f"  ❌ Errors:            {error_count:6,} ({error_count/total_processed*100:.1f}%)")
#                 logging.info("─" * 70)
#                 logging.info("")
            
#             # Sleep between batches
#             if sleep_between_batches > 0 and i + batch_size < len(doc_ids):
#                 time.sleep(sleep_between_batches)
        
#     except KeyboardInterrupt:
#         logging.warning("\n⚠️  Update interrupted by user.")
        
#     except Exception as e:
#         error_msg = str(e).lower()
#         if 'quota exceeded' in error_msg or 'resource exhausted' in error_msg:
#             logging.error("\n" + "=" * 70)
#             logging.error("❌ QUOTA EXCEEDED")
#             logging.error("=" * 70)
#             logging.error("Your Firestore quota has been exhausted.")
#             logging.error(f"Processed {total_processed}/{len(doc_ids)} documents")
#             logging.error("=" * 70)
#         else:
#             logging.error(f"❌ Unexpected error: {e}")
#         raise
        
#     finally:
#         # Final summary
#         elapsed_time = time.time() - start_time
#         avg_rate = total_processed / elapsed_time if elapsed_time > 0 else 0
        
#         logging.info("\n" + "=" * 70)
#         logging.info(f"🎯 FINAL SUMMARY - {mode}")
#         logging.info("=" * 70)
#         logging.info(f"Collection:              {collection_name}")
#         logging.info(f"Documents processed:     {total_processed:,}/{len(doc_ids):,}")
#         logging.info(f"Time elapsed:            {elapsed_time:.1f}s ({avg_rate:.1f} docs/sec)")
#         logging.info("")
#         logging.info("📊 RESULTS:")
#         logging.info(f"  🔄 Updated:              {updated_count:6,} ({updated_count/total_processed*100 if total_processed > 0 else 0:.1f}%)")
#         logging.info(f"  ✅ Already had field:    {already_has_field:6,} ({already_has_field/total_processed*100 if total_processed > 0 else 0:.1f}%)")
#         if doc_not_found > 0:
#             logging.info(f"  ⚠️  Not found:           {doc_not_found:6,} ({doc_not_found/total_processed*100:.1f}%)")
#         if error_count > 0:
#             logging.info(f"  ❌ Errors:               {error_count:6,} ({error_count/total_processed*100:.1f}%)")
#         logging.info("")
        
#         # Quota usage estimate
#         reads_used = total_processed  # One read per doc to check if field exists
#         writes_used = updated_count if not dry_run else 0
#         logging.info("📈 QUOTA USAGE:")
#         logging.info(f"  Reads:  ~{reads_used:,}")
#         logging.info(f"  Writes: ~{writes_used:,}")
#         logging.info("")
        
#         # Recommendations
#         logging.info("💡 SUMMARY:")
#         if dry_run:
#             logging.info("  This was a DRY RUN - no changes were made.")
#             logging.info("  Remove --dry-run flag to apply changes.")
#         else:
#             if error_count == 0 and doc_not_found == 0:
#                 logging.info(f"  ✅ Successfully updated {updated_count:,} documents!")
#             else:
#                 if error_count > 0:
#                     logging.info(f"  ⚠️  {error_count} errors occurred - check logs above")
#                 if doc_not_found > 0:
#                     logging.info(f"  ⚠️  {doc_not_found} documents were not found")
        
#         logging.info("=" * 70)

# def add_is_searchable_field(collection_name: str,
#                            max_docs_to_update: int = None,
#                            page_size: int = 50,
#                            sleep_between_batches: float = 1.0,
#                            resume_after_doc_id: str = None,
#                            dry_run: bool = False,
#                            only_missing: bool = True,
#                            field_value: bool = True):
#     """
#     Add 'is_searchable' field to documents in the collection (scan mode).
#     """
    
#     init_firebase_once()
#     db = firestore.client()
#     collection_ref = db.collection(collection_name)
    
#     # Counters
#     total_processed = 0
#     already_has_field = 0
#     updated_count = 0
#     error_count = 0
    
#     last_doc_id = resume_after_doc_id
#     start_time = time.time()
    
#     mode = "DRY RUN" if dry_run else "LIVE UPDATE"
#     logging.info("=" * 70)
#     logging.info(f"🚀 Starting {mode} for collection '{collection_name}'")
#     logging.info("=" * 70)
#     logging.info(f"Field to add:     is_searchable = {field_value}")
#     logging.info(f"Only missing:     {only_missing}")
#     logging.info(f"Max documents:    {max_docs_to_update if max_docs_to_update else 'ALL'}")
#     logging.info(f"Page size:        {page_size}")
#     logging.info(f"Sleep interval:   {sleep_between_batches}s")
#     if resume_after_doc_id:
#         logging.info(f"Resuming after:   {resume_after_doc_id}")
#     logging.info("=" * 70)
#     logging.info("")
    
#     try:
#         while True:
#             # Check if we've hit the max limit
#             if max_docs_to_update and total_processed >= max_docs_to_update:
#                 logging.info(f"✅ Reached maximum document limit: {max_docs_to_update}")
#                 break
            
#             # Calculate batch size (respect max limit if set)
#             current_batch_size = page_size
#             if max_docs_to_update:
#                 remaining = max_docs_to_update - total_processed
#                 current_batch_size = min(page_size, remaining)
            
#             # Build query for next page
#             query = collection_ref.order_by('__name__').limit(current_batch_size)
            
#             if last_doc_id:
#                 query = query.start_after({'__name__': last_doc_id})
            
#             # Fetch documents
#             docs = list(query.stream())
#             if not docs:
#                 logging.info("✅ No more documents found - reached end of collection.")
#                 break
            
#             # Process each document
#             for doc in docs:
#                 total_processed += 1
#                 data = doc.to_dict() or {}
                
#                 # Check if document already has the field
#                 if 'is_searchable' in data:
#                     already_has_field += 1
#                     if only_missing:
#                         continue  # Skip this document
                
#                 # Update the document
#                 try:
#                     if dry_run:
#                         logging.debug(f"[DRY RUN] Would update {doc.id}")
#                         updated_count += 1
#                     else:
#                         doc.reference.update({'is_searchable': field_value})
#                         updated_count += 1
                        
#                 except Exception as e:
#                     error_count += 1
#                     logging.error(f"❌ Error updating {doc.id}: {e}")
            
#             # Update pagination cursor
#             last_doc_id = docs[-1].id
            
#             # Progress logging
#             if total_processed % 500 == 0 or len(docs) < current_batch_size:
#                 elapsed = time.time() - start_time
#                 rate = total_processed / elapsed if elapsed > 0 else 0
                
#                 logging.info("─" * 70)
#                 logging.info(f"📊 PROGRESS - Processed {total_processed:,} documents ({rate:.1f} docs/sec)")
#                 logging.info("─" * 70)
#                 logging.info(f"  ✅ Already had field:  {already_has_field:6,} ({already_has_field/total_processed*100:.1f}%)")
#                 logging.info(f"  🔄 Updated:            {updated_count:6,} ({updated_count/total_processed*100:.1f}%)")
#                 if error_count > 0:
#                     logging.info(f"  ❌ Errors:             {error_count:6,} ({error_count/total_processed*100:.1f}%)")
#                 logging.info(f"  📍 Current position:   {last_doc_id}")
#                 logging.info("─" * 70)
#                 logging.info("")
            
#             # Sleep between batches
#             if sleep_between_batches > 0:
#                 time.sleep(sleep_between_batches)
        
#     except KeyboardInterrupt:
#         logging.warning("\n⚠️  Update interrupted by user.")
        
#     except Exception as e:
#         error_msg = str(e).lower()
#         if 'quota exceeded' in error_msg or 'resource exhausted' in error_msg:
#             logging.error("\n" + "=" * 70)
#             logging.error("❌ QUOTA EXCEEDED")
#             logging.error("=" * 70)
#             logging.error("Your Firestore quota has been exhausted.")
#             logging.error(f"Resume from this point using: --resume-after {last_doc_id}")
#             logging.error("=" * 70)
#         else:
#             logging.error(f"❌ Unexpected error: {e}")
#         raise
        
#     finally:
#         # Final summary
#         elapsed_time = time.time() - start_time
#         avg_rate = total_processed / elapsed_time if elapsed_time > 0 else 0
        
#         logging.info("\n" + "=" * 70)
#         logging.info(f"🎯 FINAL SUMMARY - {mode}")
#         logging.info("=" * 70)
#         logging.info(f"Collection:              {collection_name}")
#         logging.info(f"Documents processed:     {total_processed:,}")
#         logging.info(f"Time elapsed:            {elapsed_time:.1f}s ({avg_rate:.1f} docs/sec)")
#         logging.info("")
#         logging.info("📊 RESULTS:")
#         logging.info(f"  ✅ Already had field:    {already_has_field:6,} ({already_has_field/total_processed*100 if total_processed > 0 else 0:.1f}%)")
#         logging.info(f"  🔄 Updated/Would update: {updated_count:6,} ({updated_count/total_processed*100 if total_processed > 0 else 0:.1f}%)")
#         if error_count > 0:
#             logging.info(f"  ❌ Errors:               {error_count:6,} ({error_count/total_processed*100:.1f}%)")
#         logging.info("")
        
#         # Quota usage estimate
#         reads_used = total_processed
#         writes_used = updated_count if not dry_run else 0
#         logging.info("📈 QUOTA USAGE:")
#         logging.info(f"  Reads:  ~{reads_used:,}")
#         logging.info(f"  Writes: ~{writes_used:,}")
#         logging.info("")
        
#         # Recommendations
#         logging.info("💡 NEXT STEPS:")
#         if dry_run:
#             logging.info("  This was a DRY RUN - no changes were made.")
#             logging.info("  Remove --dry-run flag to apply changes.")
#         else:
#             if error_count == 0:
#                 logging.info("  ✅ All updates completed successfully!")
#             else:
#                 logging.info(f"  ⚠️  {error_count} errors occurred - check logs above")
        
#         if max_docs_to_update and total_processed >= max_docs_to_update:
#             logging.info(f"  ⏭️  More documents may exist. Remove --max-docs to process all.")
#             logging.info(f"  Or resume with: --resume-after {last_doc_id}")
        
#         logging.info("=" * 70)

# def main():
#     parser = argparse.ArgumentParser(
#         description="Add 'is_searchable' field to Firestore documents.",
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#         epilog="""
# Examples:
#   # Update from JSON file (recommended - more efficient)
#   python %(prog)s --from-json missing_is_searchable_allplaces_20251005_114129.json
  
#   # Dry run from JSON
#   python %(prog)s --from-json missing_docs.json --dry-run
  
#   # Update first 100 from JSON (testing)
#   python %(prog)s --from-json missing_docs.json --max-docs 100
  
#   # Scan mode - Update by scanning collection
#   python %(prog)s --max-docs 1000
  
#   # Resume from a specific document (scan mode)
#   python %(prog)s --resume-after ABC123XYZ
#         """
#     )
    
#     parser.add_argument("--collection", default="allplaces",
#                        help="Collection name. Default: allplaces")
#     parser.add_argument("--from-json", 
#                        help="JSON file with missing document IDs (more efficient)")
#     parser.add_argument("--max-docs", type=int,
#                        help="Maximum documents to process")
#     parser.add_argument("--page-size", type=int, default=50,
#                        help="Documents per batch. Default: 50")
#     parser.add_argument("--sleep", type=float, default=1.0,
#                        help="Seconds to sleep between batches. Default: 1.0")
#     parser.add_argument("--resume-after",
#                        help="Resume after this document ID (scan mode only)")
#     parser.add_argument("--dry-run", action="store_true",
#                        help="Simulate updates without writing to Firestore")
#     parser.add_argument("--no-only-missing", action="store_true",
#                        help="Update all docs, even those with existing field (scan mode)")
#     parser.add_argument("--field-value", type=str, default="true",
#                        choices=["true", "false"],
#                        help="Value for is_searchable field. Default: true")
    
#     args = parser.parse_args()
    
#     # Validate arguments
#     if args.page_size < 1:
#         parser.error("page-size must be at least 1")
#     if args.page_size > 500:
#         logging.warning("⚠️  Large page sizes may hit quota limits faster")
#     if args.sleep < 0:
#         parser.error("Sleep time cannot be negative")
    
#     # Convert field value string to boolean
#     field_value = args.field_value == "true"
#     only_missing = not args.no_only_missing
    
#     # Determine mode: JSON or Scan
#     if args.from_json:
#         # JSON mode
#         if not os.path.exists(args.from_json):
#             parser.error(f"JSON file not found: {args.from_json}")
        
#         if not args.dry_run:
#             logging.warning("")
#             logging.warning("⚠️  " + "=" * 60)
#             logging.warning("⚠️  LIVE UPDATE MODE - This will modify your Firestore data!")
#             logging.warning("⚠️  " + "=" * 60)
#             logging.warning("")
            
#             response = input("Type 'yes' to continue: ")
#             if response.lower() != 'yes':
#                 logging.info("❌ Update cancelled by user.")
#                 return
#             logging.info("")
        
#         # Run JSON-based update
#         update_from_json(
#             collection_name=args.collection,
#             json_file=args.from_json,
#             batch_size=args.page_size,
#             sleep_between_batches=args.sleep,
#             dry_run=args.dry_run,
#             field_value=field_value,
#             max_docs=args.max_docs
#         )
#     else:
#         # Scan mode
#         if not args.max_docs:
#             args.max_docs = 100000  # Default for scan mode
        
#         if args.max_docs < 1:
#             parser.error("max-docs must be at least 1")
        
#         # Warnings
#         if not args.dry_run:
#             logging.warning("")
#             logging.warning("⚠️  " + "=" * 60)
#             logging.warning("⚠️  LIVE UPDATE MODE - This will modify your Firestore data!")
#             logging.warning("⚠️  " + "=" * 60)
#             logging.warning("")
#             if args.max_docs > 10000:
#                 logging.warning(f"⚠️  You're about to update up to {args.max_docs:,} documents!")
#                 logging.warning("⚠️  Consider testing with --dry-run first.")
#                 logging.warning("")
            
#             response = input("Type 'yes' to continue: ")
#             if response.lower() != 'yes':
#                 logging.info("❌ Update cancelled by user.")
#                 return
#             logging.info("")
        
#         # Quota warning
#         estimated_reads = args.max_docs
#         estimated_writes = estimated_reads if not args.dry_run else 0
        
#         logging.info("📊 ESTIMATED QUOTA USAGE:")
#         logging.info(f"  Reads:  ~{estimated_reads:,}")
#         if not args.dry_run:
#             logging.info(f"  Writes: ~{estimated_writes:,}")
#         logging.info("  Free tier: 50,000 reads + 20,000 writes per day")
#         logging.info("")
        
#         # Run the update
#         add_is_searchable_field(
#             collection_name=args.collection,
#             max_docs_to_update=args.max_docs,
#             page_size=args.page_size,
#             sleep_between_batches=args.sleep,
#             resume_after_doc_id=args.resume_after,
#             dry_run=args.dry_run,
#             only_missing=only_missing,
#             field_value=field_value
#         )

# if __name__ == "__main__":
#     main()




#!/usr/bin/env python3
import os, logging, time, argparse, json
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

def find_missing_field_docs(collection_name: str,
                           max_docs_to_check: int = None,
                           page_size: int = 100,
                           sleep_between_batches: float = 0.5,
                           resume_after_doc_id: str = None,
                           output_file: str = None):
    """
    Find all documents missing the 'is_searchable' field and save to JSON.
    
    Args:
        collection_name: Name of the Firestore collection
        max_docs_to_check: Maximum number of documents to check (None = all)
        page_size: Number of documents to fetch per batch
        sleep_between_batches: Seconds to sleep between batches
        resume_after_doc_id: Document ID to resume from
        output_file: Path to output JSON file
    """
    
    init_firebase_once()
    db = firestore.client()
    collection_ref = db.collection(collection_name)
    
    # Counters and data
    total_checked = 0
    has_field_count = 0
    missing_field_count = 0
    missing_doc_ids = []
    
    last_doc_id = resume_after_doc_id
    start_time = time.time()
    
    # Default output file name
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"missing_is_searchable_{collection_name}_{timestamp}.json"
    
    logging.info("=" * 70)
    logging.info(f"🔍 Finding documents missing 'is_searchable' field")
    logging.info("=" * 70)
    logging.info(f"Collection:       {collection_name}")
    logging.info(f"Output file:      {output_file}")
    logging.info(f"Max documents:    {max_docs_to_check if max_docs_to_check else 'ALL'}")
    logging.info(f"Page size:        {page_size}")
    logging.info(f"Sleep interval:   {sleep_between_batches}s")
    if resume_after_doc_id:
        logging.info(f"Resuming after:   {resume_after_doc_id}")
    logging.info("=" * 70)
    logging.info("")
    
    try:
        while True:
            # Check if we've hit the max limit
            if max_docs_to_check and total_checked >= max_docs_to_check:
                logging.info(f"✅ Reached maximum document limit: {max_docs_to_check}")
                break
            
            # Calculate batch size (respect max limit if set)
            current_batch_size = page_size
            if max_docs_to_check:
                remaining = max_docs_to_check - total_checked
                current_batch_size = min(page_size, remaining)
            
            # Build query for next page
            query = collection_ref.order_by('__name__').limit(current_batch_size)
            
            if last_doc_id:
                query = query.start_after({'__name__': last_doc_id})
            
            # Fetch documents
            docs = list(query.stream())
            if not docs:
                logging.info("✅ No more documents found - reached end of collection.")
                break
            
            # Process each document
            for doc in docs:
                total_checked += 1
                data = doc.to_dict() or {}
                
                # Check if document has the field
                if 'is_searchable' in data:
                    has_field_count += 1
                else:
                    missing_field_count += 1
                    missing_doc_ids.append(doc.id)
            
            # Update pagination cursor
            last_doc_id = docs[-1].id
            
            # Progress logging
            if total_checked % 1000 == 0 or len(docs) < current_batch_size:
                elapsed = time.time() - start_time
                rate = total_checked / elapsed if elapsed > 0 else 0
                
                logging.info("─" * 70)
                logging.info(f"📊 PROGRESS - Checked {total_checked:,} documents ({rate:.1f} docs/sec)")
                logging.info("─" * 70)
                logging.info(f"  ✅ Has field:       {has_field_count:6,} ({has_field_count/total_checked*100:.1f}%)")
                logging.info(f"  ❌ Missing field:   {missing_field_count:6,} ({missing_field_count/total_checked*100:.1f}%)")
                logging.info(f"  📍 Current position: {last_doc_id}")
                logging.info("─" * 70)
                logging.info("")
            
            # Periodically save progress to file (every 5000 docs)
            if total_checked % 5000 == 0 and missing_doc_ids:
                temp_file = f"{output_file}.temp"
                save_to_json(missing_doc_ids, temp_file, collection_name, total_checked, 
                           has_field_count, missing_field_count, last_doc_id, in_progress=True)
                logging.info(f"💾 Progress saved to {temp_file}")
            
            # Sleep between batches
            if sleep_between_batches > 0:
                time.sleep(sleep_between_batches)
        
    except KeyboardInterrupt:
        logging.warning("\n⚠️  Scan interrupted by user.")
        logging.info(f"💾 Saving partial results to {output_file}")
        
    except Exception as e:
        error_msg = str(e).lower()
        if 'quota exceeded' in error_msg or 'resource exhausted' in error_msg:
            logging.error("\n" + "=" * 70)
            logging.error("❌ QUOTA EXCEEDED")
            logging.error("=" * 70)
            logging.error("Your Firestore quota has been exhausted.")
            logging.error(f"Resume from this point using: --resume-after {last_doc_id}")
            logging.error(f"Partial results will be saved to {output_file}")
            logging.error("=" * 70)
        else:
            logging.error(f"❌ Unexpected error: {e}")
        raise
        
    finally:
        # Save final results to JSON
        elapsed_time = time.time() - start_time
        avg_rate = total_checked / elapsed_time if elapsed_time > 0 else 0
        
        # Save the JSON file
        save_to_json(missing_doc_ids, output_file, collection_name, total_checked,
                   has_field_count, missing_field_count, last_doc_id, in_progress=False)
        
        # Final summary
        logging.info("\n" + "=" * 70)
        logging.info(f"🎯 FINAL SUMMARY")
        logging.info("=" * 70)
        logging.info(f"Collection:              {collection_name}")
        logging.info(f"Documents checked:       {total_checked:,}")
        logging.info(f"Time elapsed:            {elapsed_time:.1f}s ({avg_rate:.1f} docs/sec)")
        logging.info("")
        logging.info("📊 RESULTS:")
        logging.info(f"  ✅ Has 'is_searchable':     {has_field_count:6,} ({has_field_count/total_checked*100 if total_checked > 0 else 0:.1f}%)")
        logging.info(f"  ❌ Missing 'is_searchable': {missing_field_count:6,} ({missing_field_count/total_checked*100 if total_checked > 0 else 0:.1f}%)")
        logging.info("")
        logging.info(f"💾 OUTPUT FILE:")
        logging.info(f"  File: {output_file}")
        logging.info(f"  Missing document IDs saved: {len(missing_doc_ids):,}")
        logging.info("")
        
        # Quota usage estimate
        reads_used = total_checked
        logging.info("📈 QUOTA USAGE:")
        logging.info(f"  Reads: ~{reads_used:,}")
        logging.info("")
        
        # Next steps
        if missing_field_count > 0:
            logging.info("💡 NEXT STEPS:")
            logging.info(f"  📝 {missing_field_count:,} documents need the 'is_searchable' field")
            logging.info(f"  🔧 Use the update script to add the field to these documents")
            logging.info("")
        
        if max_docs_to_check and total_checked >= max_docs_to_check:
            logging.info(f"  ⏭️  More documents may exist. Remove --max-docs to check all.")
            logging.info(f"  Or resume with: --resume-after {last_doc_id}")
        
        # Clean up temp file if exists
        temp_file = f"{output_file}.temp"
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                logging.info(f"🧹 Cleaned up temporary file: {temp_file}")
            except:
                pass
        
        logging.info("=" * 70)

def save_to_json(missing_doc_ids, output_file, collection_name, total_checked,
                has_field_count, missing_field_count, last_doc_id, in_progress=False):
    """Save results to JSON file with metadata."""
    
    output_data = {
        "metadata": {
            "collection": collection_name,
            "scan_timestamp": datetime.now().isoformat(),
            "scan_status": "in_progress" if in_progress else "completed",
            "total_documents_checked": total_checked,
            "documents_with_field": has_field_count,
            "documents_missing_field": missing_field_count,
            "last_document_id_checked": last_doc_id
        },
        "missing_document_ids": missing_doc_ids,
        "summary": {
            "total_missing": len(missing_doc_ids),
            "percentage_missing": round(missing_field_count / total_checked * 100, 2) if total_checked > 0 else 0
        }
    }
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        logging.info(f"✅ Saved {len(missing_doc_ids):,} missing document IDs to {output_file}")
    except Exception as e:
        logging.error(f"❌ Error saving JSON file: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="Find documents missing 'is_searchable' field and output to JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check first 1000 documents
  python %(prog)s --max-docs 1000
  
  # Check all documents in collection
  python %(prog)s --max-docs 999999
  
  # Resume from a specific document
  python %(prog)s --resume-after ABC123XYZ
  
  # Custom output file
  python %(prog)s --output missing_docs.json
        """
    )
    
    parser.add_argument("--collection", default="allplaces",
                       help="Collection name. Default: allplaces")
    parser.add_argument("--max-docs", type=int, default=None,
                       help="Maximum documents to check. Default: all documents")
    parser.add_argument("--page-size", type=int, default=100,
                       help="Documents per batch. Default: 100")
    parser.add_argument("--sleep", type=float, default=0.5,
                       help="Seconds to sleep between batches. Default: 0.5")
    parser.add_argument("--resume-after",
                       help="Resume after this document ID")
    parser.add_argument("--output", "-o",
                       help="Output JSON file path. Default: auto-generated with timestamp")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.max_docs and args.max_docs < 1:
        parser.error("max-docs must be at least 1")
    if args.page_size < 1:
        parser.error("page-size must be at least 1")
    if args.page_size > 1000:
        logging.warning("⚠️  Large page sizes may hit quota limits faster")
    if args.sleep < 0:
        parser.error("Sleep time cannot be negative")
    
    # Quota warning
    estimated_reads = args.max_docs if args.max_docs else 50000
    
    logging.info("📊 ESTIMATED QUOTA USAGE:")
    logging.info(f"  Reads: ~{estimated_reads:,}")
    logging.info("  Free tier: 50,000 reads per day")
    logging.info("")
    
    # Run the scan
    find_missing_field_docs(
        collection_name=args.collection,
        max_docs_to_check=args.max_docs,
        page_size=args.page_size,
        sleep_between_batches=args.sleep,
        resume_after_doc_id=args.resume_after,
        output_file=args.output
    )

if __name__ == "__main__":
    main()

# #!/usr/bin/env python3
# import os, logging, time, argparse
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

# def add_is_searchable_field(collection_name: str,
#                            max_docs_to_update: int = None,
#                            page_size: int = 50,
#                            sleep_between_batches: float = 1.0,
#                            resume_after_doc_id: str = None,
#                            dry_run: bool = False,
#                            only_missing: bool = True,
#                            field_value: bool = True):
#     """
#     Add 'is_searchable' field to documents in the collection.
    
#     Args:
#         collection_name: Name of the Firestore collection
#         max_docs_to_update: Maximum number of documents to process (None = all)
#         page_size: Number of documents to fetch per batch
#         sleep_between_batches: Seconds to sleep between batches
#         resume_after_doc_id: Document ID to resume from
#         dry_run: If True, only simulate updates without writing
#         only_missing: If True, only update docs missing the field
#         field_value: Value to set for is_searchable field
#     """
    
#     init_firebase_once()
#     db = firestore.client()
#     collection_ref = db.collection(collection_name)
    
#     # Counters
#     total_processed = 0
#     already_has_field = 0
#     updated_count = 0
#     error_count = 0
    
#     last_doc_id = resume_after_doc_id
#     start_time = time.time()
    
#     mode = "DRY RUN" if dry_run else "LIVE UPDATE"
#     logging.info("=" * 70)
#     logging.info(f"🚀 Starting {mode} for collection '{collection_name}'")
#     logging.info("=" * 70)
#     logging.info(f"Field to add:     is_searchable = {field_value}")
#     logging.info(f"Only missing:     {only_missing}")
#     logging.info(f"Max documents:    {max_docs_to_update if max_docs_to_update else 'ALL'}")
#     logging.info(f"Page size:        {page_size}")
#     logging.info(f"Sleep interval:   {sleep_between_batches}s")
#     if resume_after_doc_id:
#         logging.info(f"Resuming after:   {resume_after_doc_id}")
#     logging.info("=" * 70)
#     logging.info("")
    
#     try:
#         while True:
#             # Check if we've hit the max limit
#             if max_docs_to_update and total_processed >= max_docs_to_update:
#                 logging.info(f"✅ Reached maximum document limit: {max_docs_to_update}")
#                 break
            
#             # Calculate batch size (respect max limit if set)
#             current_batch_size = page_size
#             if max_docs_to_update:
#                 remaining = max_docs_to_update - total_processed
#                 current_batch_size = min(page_size, remaining)
            
#             # Build query for next page
#             query = collection_ref.order_by('__name__').limit(current_batch_size)
            
#             if last_doc_id:
#                 query = query.start_after({'__name__': last_doc_id})
            
#             # Fetch documents
#             docs = list(query.stream())
#             if not docs:
#                 logging.info("✅ No more documents found - reached end of collection.")
#                 break
            
#             # Process each document
#             for doc in docs:
#                 total_processed += 1
#                 data = doc.to_dict() or {}
                
#                 # Check if document already has the field
#                 if 'is_searchable' in data:
#                     already_has_field += 1
#                     if only_missing:
#                         continue  # Skip this document
                
#                 # Update the document
#                 try:
#                     if dry_run:
#                         logging.debug(f"[DRY RUN] Would update {doc.id}")
#                         updated_count += 1
#                     else:
#                         doc.reference.update({'is_searchable': field_value})
#                         updated_count += 1
                        
#                 except Exception as e:
#                     error_count += 1
#                     logging.error(f"❌ Error updating {doc.id}: {e}")
            
#             # Update pagination cursor
#             last_doc_id = docs[-1].id
            
#             # Progress logging
#             if total_processed % 500 == 0 or len(docs) < current_batch_size:
#                 elapsed = time.time() - start_time
#                 rate = total_processed / elapsed if elapsed > 0 else 0
                
#                 logging.info("─" * 70)
#                 logging.info(f"📊 PROGRESS - Processed {total_processed:,} documents ({rate:.1f} docs/sec)")
#                 logging.info("─" * 70)
#                 logging.info(f"  ✅ Already had field:  {already_has_field:6,} ({already_has_field/total_processed*100:.1f}%)")
#                 logging.info(f"  🔄 Updated:            {updated_count:6,} ({updated_count/total_processed*100:.1f}%)")
#                 if error_count > 0:
#                     logging.info(f"  ❌ Errors:             {error_count:6,} ({error_count/total_processed*100:.1f}%)")
#                 logging.info(f"  📍 Current position:   {last_doc_id}")
#                 logging.info("─" * 70)
#                 logging.info("")
            
#             # Sleep between batches
#             if sleep_between_batches > 0:
#                 time.sleep(sleep_between_batches)
        
#     except KeyboardInterrupt:
#         logging.warning("\n⚠️  Update interrupted by user.")
        
#     except Exception as e:
#         error_msg = str(e).lower()
#         if 'quota exceeded' in error_msg or 'resource exhausted' in error_msg:
#             logging.error("\n" + "=" * 70)
#             logging.error("❌ QUOTA EXCEEDED")
#             logging.error("=" * 70)
#             logging.error("Your Firestore quota has been exhausted.")
#             logging.error(f"Resume from this point using: --resume-after {last_doc_id}")
#             logging.error("=" * 70)
#         else:
#             logging.error(f"❌ Unexpected error: {e}")
#         raise
        
#     finally:
#         # Final summary
#         elapsed_time = time.time() - start_time
#         avg_rate = total_processed / elapsed_time if elapsed_time > 0 else 0
        
#         logging.info("\n" + "=" * 70)
#         logging.info(f"🎯 FINAL SUMMARY - {mode}")
#         logging.info("=" * 70)
#         logging.info(f"Collection:              {collection_name}")
#         logging.info(f"Documents processed:     {total_processed:,}")
#         logging.info(f"Time elapsed:            {elapsed_time:.1f}s ({avg_rate:.1f} docs/sec)")
#         logging.info("")
#         logging.info("📊 RESULTS:")
#         logging.info(f"  ✅ Already had field:    {already_has_field:6,} ({already_has_field/total_processed*100 if total_processed > 0 else 0:.1f}%)")
#         logging.info(f"  🔄 Updated/Would update: {updated_count:6,} ({updated_count/total_processed*100 if total_processed > 0 else 0:.1f}%)")
#         if error_count > 0:
#             logging.info(f"  ❌ Errors:               {error_count:6,} ({error_count/total_processed*100:.1f}%)")
#         logging.info("")
        
#         # Quota usage estimate
#         reads_used = total_processed
#         writes_used = updated_count if not dry_run else 0
#         logging.info("📈 QUOTA USAGE:")
#         logging.info(f"  Reads:  ~{reads_used:,}")
#         logging.info(f"  Writes: ~{writes_used:,}")
#         logging.info("")
        
#         # Recommendations
#         logging.info("💡 NEXT STEPS:")
#         if dry_run:
#             logging.info("  This was a DRY RUN - no changes were made.")
#             logging.info("  Remove --dry-run flag to apply changes.")
#         else:
#             if error_count == 0:
#                 logging.info("  ✅ All updates completed successfully!")
#             else:
#                 logging.info(f"  ⚠️  {error_count} errors occurred - check logs above")
        
#         if max_docs_to_update and total_processed >= max_docs_to_update:
#             logging.info(f"  ⏭️  More documents may exist. Remove --max-docs to process all.")
#             logging.info(f"  Or resume with: --resume-after {last_doc_id}")
        
#         logging.info("=" * 70)

# def main():
#     parser = argparse.ArgumentParser(
#         description="Add 'is_searchable' field to Firestore documents.",
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#         epilog="""
# Examples:
#   # Dry run to see what would be updated
#   python %(prog)s --dry-run
  
#   # Update only first 100 documents
#   python %(prog)s --max-docs 100
  
#   # Update all documents (use with caution!)
#   python %(prog)s --max-docs 999999
  
#   # Resume from a specific document
#   python %(prog)s --resume-after ABC123XYZ
  
#   # Update even documents that already have the field
#   python %(prog)s --no-only-missing
#         """
#     )
    
#     parser.add_argument("--collection", default="allplaces",
#                        help="Collection name. Default: allplaces")
#     parser.add_argument("--max-docs", type=int, default=1000,
#                        help="Maximum documents to process. Default: 1000")
#     parser.add_argument("--page-size", type=int, default=50,
#                        help="Documents per batch. Default: 50")
#     parser.add_argument("--sleep", type=float, default=1.0,
#                        help="Seconds to sleep between batches. Default: 1.0")
#     parser.add_argument("--resume-after",
#                        help="Resume after this document ID")
#     parser.add_argument("--dry-run", action="store_true",
#                        help="Simulate updates without writing to Firestore")
#     parser.add_argument("--no-only-missing", action="store_true",
#                        help="Update all docs, even those with existing field")
#     parser.add_argument("--field-value", type=str, default="true",
#                        choices=["true", "false"],
#                        help="Value for is_searchable field. Default: true")
    
#     args = parser.parse_args()
    
#     # Validate arguments
#     if args.max_docs and args.max_docs < 1:
#         parser.error("max-docs must be at least 1")
#     if args.page_size < 1:
#         parser.error("page-size must be at least 1")
#     if args.page_size > 500:
#         logging.warning("⚠️  Large page sizes may hit quota limits faster")
#     if args.sleep < 0:
#         parser.error("Sleep time cannot be negative")
    
#     # Convert field value string to boolean
#     field_value = args.field_value == "true"
#     only_missing = not args.no_only_missing
    
#     # Warnings
#     if not args.dry_run:
#         logging.warning("")
#         logging.warning("⚠️  " + "=" * 60)
#         logging.warning("⚠️  LIVE UPDATE MODE - This will modify your Firestore data!")
#         logging.warning("⚠️  " + "=" * 60)
#         logging.warning("")
#         if args.max_docs and args.max_docs > 10000:
#             logging.warning(f"⚠️  You're about to update up to {args.max_docs:,} documents!")
#             logging.warning("⚠️  Consider testing with --dry-run first.")
#             logging.warning("")
        
#         response = input("Type 'yes' to continue: ")
#         if response.lower() != 'yes':
#             logging.info("❌ Update cancelled by user.")
#             return
#         logging.info("")
    
#     # Quota warning
#     estimated_reads = args.max_docs if args.max_docs else 50000
#     estimated_writes = estimated_reads if not args.dry_run else 0
    
#     logging.info("📊 ESTIMATED QUOTA USAGE:")
#     logging.info(f"  Reads:  ~{estimated_reads:,}")
#     if not args.dry_run:
#         logging.info(f"  Writes: ~{estimated_writes:,}")
#     logging.info("  Free tier: 50,000 reads + 20,000 writes per day")
#     logging.info("")
    
#     # Run the update
#     add_is_searchable_field(
#         collection_name=args.collection,
#         max_docs_to_update=args.max_docs,
#         page_size=args.page_size,
#         sleep_between_batches=args.sleep,
#         resume_after_doc_id=args.resume_after,
#         dry_run=args.dry_run,
#         only_missing=only_missing,
#         field_value=field_value
#     )

# if __name__ == "__main__":
#     main()












# #!/usr/bin/env python3
# import os, logging, time, argparse
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

# def check_progress(collection_name: str, 
#                   max_docs_to_check: int = 1000,
#                   page_size: int = 50,
#                   sleep_between_batches: float = 1.0,
#                   resume_after_doc_id: str = None,
#                   sample_missing: bool = False):
#     """
#     Check progress of is_searchable field addition in the collection.
#     """
    
#     init_firebase_once()
#     db = firestore.client()
#     collection_ref = db.collection(collection_name)
    
#     # Counters
#     total_checked = 0
#     has_field_count = 0
#     missing_field_count = 0
#     field_true_count = 0
#     field_false_count = 0
#     field_other_count = 0
    
#     # Track sample documents missing the field
#     missing_samples = []
    
#     last_doc_id = resume_after_doc_id
#     start_time = time.time()
    
#     logging.info(f"Starting progress check of collection '{collection_name}'")
#     logging.info(f"Will check maximum {max_docs_to_check} documents in batches of {page_size}")
#     if resume_after_doc_id:
#         logging.info(f"Resuming after document ID: {resume_after_doc_id}")
    
#     try:
#         while total_checked < max_docs_to_check:
#             # Calculate how many docs to read in this batch
#             remaining = max_docs_to_check - total_checked
#             current_batch_size = min(page_size, remaining)
            
#             # Build the query for the next page
#             query = collection_ref.order_by('__name__').limit(current_batch_size)
            
#             # If we have a last_doc_id, start after it
#             if last_doc_id:
#                 query = query.start_after({'__name__': last_doc_id})

#             # Fetch docs in the current page
#             docs = list(query.stream())
#             if not docs:
#                 logging.info("No more documents found.")
#                 break
            
#             # Analyze each document
#             for doc in docs:
#                 data = doc.to_dict() or {}
#                 total_checked += 1
                
#                 if 'is_searchable' in data:
#                     has_field_count += 1
#                     field_value = data['is_searchable']
                    
#                     if field_value is True:
#                         field_true_count += 1
#                     elif field_value is False:
#                         field_false_count += 1
#                     else:
#                         field_other_count += 1
#                         logging.warning(f"Doc {doc.id} has unexpected is_searchable value: {field_value}")
#                 else:
#                     missing_field_count += 1
#                     if sample_missing and len(missing_samples) < 10:
#                         missing_samples.append(doc.id)

#             # Update last_doc_id for pagination
#             last_doc_id = docs[-1].id
            
#             # Progress logging
#             if total_checked % 500 == 0 or len(docs) < current_batch_size:
#                 elapsed = time.time() - start_time
#                 rate = total_checked / elapsed if elapsed > 0 else 0
                
#                 logging.info("=" * 60)
#                 logging.info(f"PROGRESS REPORT - Checked {total_checked} documents ({rate:.1f} docs/sec)")
#                 logging.info("=" * 60)
#                 logging.info(f"📊 FIELD STATUS:")
#                 logging.info(f"  ✅ Has 'is_searchable':     {has_field_count:6,} ({has_field_count/total_checked*100:.1f}%)")
#                 logging.info(f"  ❌ Missing 'is_searchable': {missing_field_count:6,} ({missing_field_count/total_checked*100:.1f}%)")
#                 logging.info(f"")
#                 logging.info(f"📈 FIELD VALUES (among those that have it):")
#                 if has_field_count > 0:
#                     logging.info(f"  🟢 True:  {field_true_count:6,} ({field_true_count/has_field_count*100:.1f}%)")
#                     logging.info(f"  🔴 False: {field_false_count:6,} ({field_false_count/has_field_count*100:.1f}%)")
#                     if field_other_count > 0:
#                         logging.info(f"  ⚠️  Other: {field_other_count:6,} ({field_other_count/has_field_count*100:.1f}%)")
#                 logging.info(f"")
#                 logging.info(f"🎯 Current position: {last_doc_id}")
#                 logging.info("=" * 60)
            
#             # Sleep to be gentle on quotas
#             if sleep_between_batches > 0:
#                 time.sleep(sleep_between_batches)
                
#     except KeyboardInterrupt:
#         logging.warning("Check interrupted by user.")
#     except Exception as e:
#         error_msg = str(e).lower()
#         if 'quota exceeded' in error_msg or 'resource exhausted' in error_msg:
#             logging.error("❌ QUOTA EXCEEDED during progress check")
#             logging.error("Your daily Firestore read quota has been exhausted.")
#             logging.error(f"Resume from: --resume-after {last_doc_id}")
#         else:
#             logging.error(f"❌ Unexpected error: {e}")
#         raise
#     finally:
#         # Final summary
#         elapsed_time = time.time() - start_time
#         avg_rate = total_checked / elapsed_time if elapsed_time > 0 else 0
        
#         logging.info("\n" + "=" * 70)
#         logging.info("🎯 FINAL PROGRESS SUMMARY")
#         logging.info("=" * 70)
#         logging.info(f"Collection:           {collection_name}")
#         logging.info(f"Documents checked:    {total_checked:,}")
#         logging.info(f"Time elapsed:         {elapsed_time:.1f}s ({avg_rate:.1f} docs/sec)")
#         logging.info("")
#         logging.info("📊 FIELD STATUS:")
#         logging.info(f"  ✅ Has 'is_searchable':     {has_field_count:6,} ({has_field_count/total_checked*100:.1f}%)")
#         logging.info(f"  ❌ Missing 'is_searchable': {missing_field_count:6,} ({missing_field_count/total_checked*100:.1f}%)")
#         logging.info("")
        
#         if has_field_count > 0:
#             logging.info("📈 FIELD VALUES:")
#             logging.info(f"  🟢 is_searchable = True:  {field_true_count:6,} ({field_true_count/has_field_count*100:.1f}%)")
#             logging.info(f"  🔴 is_searchable = False: {field_false_count:6,} ({field_false_count/has_field_count*100:.1f}%)")
#             if field_other_count > 0:
#                 logging.info(f"  ⚠️  Other values:         {field_other_count:6,} ({field_other_count/has_field_count*100:.1f}%)")
        
#         logging.info("")
#         logging.info("🚀 RECOMMENDATIONS:")
        
#         if missing_field_count == 0:
#             logging.info("✅ All checked documents have the 'is_searchable' field!")
#             if field_true_count == has_field_count:
#                 logging.info("🎉 And they're all set to True - job appears complete!")
#             else:
#                 logging.info(f"📝 Consider setting all to True (found {field_false_count} False values)")
#         else:
#             logging.info(f"📝 {missing_field_count:,} documents still need the 'is_searchable' field")
#             logging.info("   Use --only-missing flag in update script to add only missing fields")
            
#         if total_checked < max_docs_to_check and last_doc_id:
#             logging.info(f"⏭️  To continue checking more docs: --resume-after {last_doc_id}")
            
#         # Show sample missing documents if requested
#         if sample_missing and missing_samples:
#             logging.info("")
#             logging.info("📋 SAMPLE DOCUMENTS MISSING 'is_searchable':")
#             for doc_id in missing_samples[:5]:
#                 logging.info(f"   - {doc_id}")
#             if len(missing_samples) > 5:
#                 logging.info(f"   ... and {len(missing_samples) - 5} more in this batch")
                
#         logging.info("=" * 70)

# def main():
#     parser = argparse.ArgumentParser(description="Check progress of is_searchable field in Firestore collection.")
#     parser.add_argument("--collection", default="allplaces", 
#                        help="Collection name. Default: allplaces")
#     parser.add_argument("--max-docs", type=int, default=1000,
#                        help="Maximum documents to check. Default: 1000")
#     parser.add_argument("--page-size", type=int, default=50, 
#                        help="Documents per batch. Default: 50")
#     parser.add_argument("--sleep", type=float, default=1.0, 
#                        help="Seconds to sleep between batches. Default: 1.0")
#     parser.add_argument("--resume-after", 
#                        help="Resume checking after this document ID.")
#     parser.add_argument("--sample-missing", action="store_true",
#                        help="Show sample document IDs that are missing the field.")
    
#     args = parser.parse_args()

#     # Validate arguments
#     if args.max_docs < 1:
#         parser.error("max-docs must be at least 1")
#     if args.page_size < 1:
#         parser.error("page-size must be at least 1") 
#     if args.page_size > 1000:
#         logging.warning("Large page sizes may hit quota limits faster")
#     if args.sleep < 0:
#         parser.error("Sleep time cannot be negative")

#     # Warn about quota usage
#     logging.info("⚠️  QUOTA USAGE WARNING:")
#     logging.info(f"   This will use approximately {args.max_docs} reads from your daily quota")
#     logging.info("   Firestore free tier: 50,000 reads per day")
#     logging.info("   Press Ctrl+C to cancel if needed")
#     logging.info("")

#     # Configuration summary
#     logging.info(f"Configuration:")
#     logging.info(f"  Collection:     {args.collection}")
#     logging.info(f"  Max docs:       {args.max_docs:,}")
#     logging.info(f"  Page size:      {args.page_size}")
#     logging.info(f"  Sleep:          {args.sleep}s")
#     logging.info(f"  Sample missing: {args.sample_missing}")
#     if args.resume_after:
#         logging.info(f"  Resume after:   {args.resume_after}")
#     logging.info("")

#     # Run the check
#     check_progress(
#         collection_name=args.collection,
#         max_docs_to_check=args.max_docs,
#         page_size=args.page_size,
#         sleep_between_batches=args.sleep,
#         resume_after_doc_id=args.resume_after,
#         sample_missing=args.sample_missing
#     )

# if __name__ == "__main__":
#     main()

# #!/usr/bin/env python3
# import os, logging, time, random, argparse, sys

# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.api_core import exceptions as gexc
# from google.cloud.firestore_v1.field_path import FieldPath

# # ──────────────────────────────────────────
# # SERVICE ACCOUNT PATH
# # ──────────────────────────────────────────
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# # ──────────────────────────────────────────
# # Logging
# # ──────────────────────────────────────────
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# # ──────────────────────────────────────────
# def init_firebase_once():
#     try:
#         firebase_admin.get_app()
#     except ValueError:
#         if not os.path.exists(SERVICE_ACCOUNT_PATH):
#             raise SystemExit(f"Service account not found:\n{SERVICE_ACCOUNT_PATH}")
#         logging.info(f"Initializing Firebase app with:\n{SERVICE_ACCOUNT_PATH}")
#         cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
#         firebase_admin.initialize_app(cred)

# def run_with_retry(func, max_retries=5, base_backoff=5.0):
#     """A simple wrapper to retry a function with exponential backoff."""
#     attempt = 0
#     while attempt <= max_retries:
#         try:
#             return func()
#         except (gexc.ResourceExhausted, gexc.DeadlineExceeded, gexc.ServiceUnavailable) as e:
#             if attempt >= max_retries:
#                 logging.error(f"Giving up after {attempt+1} retries. Error: {e}")
#                 raise
#             sleep_s = base_backoff * (2 ** attempt) + random.uniform(0, 1)
#             logging.warning(f"Retryable error ({type(e).__name__}). Retrying in {sleep_s:.2f}s...")
#             time.sleep(sleep_s)
#             attempt += 1

# def main():
#     parser = argparse.ArgumentParser(description="Set is_searchable=True across a collection using robust pagination.")
#     parser.add_argument("--collection", default="allplaces")
#     parser.add_argument("--only-missing", action="store_true", help="Only add the field when it's missing.")
#     parser.add_argument("--batch-size", type=int, default=400, help="Docs to fetch and write per page (<=500).")
#     parser.add_argument("--resume-after", help="Resume processing after this document ID.")
#     args = parser.parse_args()

#     if args.batch_size > 500:
#         logging.error("--batch-size cannot exceed 500.")
#         sys.exit(1)

#     init_firebase_once()
#     db = firestore.client()
#     collection_ref = db.collection(args.collection)
    
#     # --- State for the main loop ---
#     last_doc_id = args.resume_after
#     total_seen = 0
#     total_touched = 0
#     total_skipped = 0
#     page_count = 0
#     start_time = time.time()

#     logging.info(f"Starting update for collection '{args.collection}' with page size {args.batch_size}.")
#     if last_doc_id:
#         logging.info(f"Resuming after document ID: {last_doc_id}")

#     try:
#         # --- The Main Pagination Loop ---
#         while True:
#             # 1. Build the query for the next page
#             query = collection_ref.order_by(FieldPath.document_id()).limit(args.batch_size)
#             if last_doc_id:
#                 query = query.start_after({FieldPath.document_id(): last_doc_id})

#             # 2. Fetch one page of documents with retry logic
#             docs = run_with_retry(lambda: list(query.stream()))
            
#             if not docs:
#                 logging.info("No more documents found. Process complete.")
#                 break

#             page_count += 1
#             page_seen = len(docs)
#             total_seen += page_seen
            
#             # 3. Create a batch write for the current page
#             batch = db.batch()
#             page_touched = 0

#             for doc in docs:
#                 data = doc.to_dict() or {}
#                 # Apply the same logic as before
#                 if (args.only_missing and 'is_searchable' in data) or \
#                    (not args.only_missing and data.get('is_searchable') is True):
#                     continue # Skip this document
                
#                 batch.update(doc.reference, {"is_searchable": True})
#                 page_touched += 1
            
#             # 4. Commit the batch with retry logic
#             if page_touched > 0:
#                 run_with_retry(lambda: batch.commit())
#                 total_touched += page_touched
            
#             page_skipped = page_seen - page_touched
#             total_skipped += page_skipped

#             # 5. Update the cursor for the next iteration
#             last_doc_id = docs[-1].id

#             # 6. Log progress
#             if page_count % 10 == 0: # Log every 10 pages
#                 elapsed = time.time() - start_time
#                 docs_per_sec = total_seen / elapsed if elapsed > 0 else 0
#                 logging.info(
#                     f"[Page {page_count}] Processed {total_seen} docs | "
#                     f"Updated: {total_touched} | Skipped: {total_skipped} | "
#                     f"Last ID: {last_doc_id} (~{docs_per_sec:.0f} docs/s)"
#                 )

#     except KeyboardInterrupt:
#         logging.warning("Process interrupted by user.")
#     except Exception as e:
#         logging.error(f"A fatal error occurred: {e}", exc_info=True)
#     finally:
#         if last_doc_id:
#             logging.info(f"To resume, use: --resume-after {last_doc_id}")
        
#         duration = time.time() - start_time
#         logging.info("--- Summary ---")
#         logging.info(f"Total documents scanned: {total_seen}")
#         logging.info(f"Total documents updated: {total_touched}")
#         logging.info(f"Total documents skipped: {total_skipped}")
#         logging.info(f"Total elapsed time: {duration:.2f} seconds")

# if __name__ == "__main__":
#     main()
# #!/usr/bin/env python3
# import os, logging, threading, queue, time, random, argparse
# from typing import List, Tuple, Optional

# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.api_core import exceptions as gexc
# from google.cloud.firestore_v1.field_path import FieldPath

# # ──────────────────────────────────────────
# # HARD-CODED SERVICE ACCOUNT PATH
# # ──────────────────────────────────────────
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# # ──────────────────────────────────────────
# # Logging
# # ──────────────────────────────────────────
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# # ──────────────────────────────────────────
# def init_firebase_once():
#     try:
#         firebase_admin.get_app()
#         logging.info("Firebase app already initialized.")
#     except ValueError:
#         if not os.path.exists(SERVICE_ACCOUNT_PATH):
#             raise SystemExit(f"Service account not found:\n{SERVICE_ACCOUNT_PATH}")
#         logging.info(f"Initializing Firebase app with:\n{SERVICE_ACCOUNT_PATH}")
#         cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
#         firebase_admin.initialize_app(cred)

# class Counters:
#     def __init__(self):
#         self.touched = 0
#         self.skipped = 0
#         self.seen    = 0
#         self.errors  = 0
#         self._lock   = threading.Lock()
#     def add(self, *, touched=0, skipped=0, seen=0, errors=0):
#         with self._lock:
#             self.touched += touched
#             self.skipped += skipped
#             self.seen    += seen
#             self.errors  += errors
#             return (self.touched, self.skipped, self.seen, self.errors)

# def is_retryable(err: Exception) -> bool:
#     """Check if an error is retryable, including wrapped RetryError exceptions."""
#     if isinstance(err, (gexc.ResourceExhausted, gexc.DeadlineExceeded, gexc.ServiceUnavailable, gexc.Aborted)):
#         return True
#     # Handle RetryError which wraps the actual quota exceeded error
#     if hasattr(err, 'cause') and err.cause:
#         return is_retryable(err.cause)
#     # Check error message for quota exceeded (fallback)
#     error_msg = str(err).lower()
#     return any(phrase in error_msg for phrase in ['quota exceeded', 'resource exhausted', 'deadline exceeded'])

# def commit_ops_with_retry(db, ops: List[Tuple[firestore.DocumentReference, dict]],
#                           commit_sleep_s: float,
#                           max_retries: int = 5,
#                           base_backoff: float = 2.0,
#                           max_backoff: float = 120.0) -> bool:
#     """Commit a list of operations as one batch with retries + throttle."""
#     attempt = 0
#     while attempt <= max_retries:
#         batch = db.batch()
#         for ref, data in ops:
#             batch.update(ref, data)
#         try:
#             batch.commit(timeout=180)  # 3 min safety timeout
#             # throttle total write rate
#             if commit_sleep_s > 0:
#                 time.sleep(commit_sleep_s)
#             return True
#         except Exception as e:
#             if not is_retryable(e) or attempt >= max_retries:
#                 logging.error(f"[commit] giving up after {attempt} retries: {str(e)[:150]}...")
#                 return False
#             # exponential backoff (+ jitter)
#             sleep_s = min(max_backoff, base_backoff * (2 ** attempt)) + random.uniform(0, 1.0)
#             logging.warning(f"[commit] retryable error ({type(e).__name__}): {str(e)[:100]}...")
#             logging.warning(f"[commit] sleeping {sleep_s:.2f}s (attempt {attempt+1}/{max_retries+1})")
#             time.sleep(sleep_s)
#             attempt += 1
#     return False

# def stream_documents_with_retry(db, collection_name: str,
#                                start_after_doc_id: Optional[str] = None,
#                                max_retries: int = 5,
#                                base_backoff: float = 2.0,
#                                max_backoff: float = 120.0):
#     """Stream documents with simple retry logic for quota exceeded errors."""
#     attempt = 0
    
#     while attempt <= max_retries:
#         try:
#             # Create query with ordering by document ID
#             query = db.collection(collection_name).order_by(FieldPath.document_id())
            
#             # Resume from where we left off if we have a starting point
#             if start_after_doc_id:
#                 query = query.start_after([start_after_doc_id])
            
#             # Stream documents
#             documents_yielded = 0
#             for snap in query.stream():
#                 yield snap
#                 documents_yielded += 1
            
#             # If we get here, streaming completed successfully
#             logging.info(f"[stream] Successfully streamed {documents_yielded} documents")
#             return
            
#         except Exception as e:
#             if not is_retryable(e) or attempt >= max_retries:
#                 logging.error(f"[stream] giving up after {attempt} retries: {e}")
#                 raise
            
#             # exponential backoff (+ jitter)
#             sleep_s = min(max_backoff, base_backoff * (2 ** attempt)) + random.uniform(0, 1.0)
#             logging.warning(f"[stream] retryable error ({type(e).__name__}): {str(e)[:100]}...")
#             logging.warning(f"[stream] sleeping {sleep_s:.2f}s (attempt {attempt+1}/{max_retries+1})")
#             time.sleep(sleep_s)
#             attempt += 1

# def worker(doc_q: "queue.Queue",
#            counters: Counters,
#            worker_id: int,
#            batch_size: int,
#            commit_sleep_s: float,
#            only_missing: bool):
#     db = firestore.client()
#     buffer: List[Tuple[firestore.DocumentReference, dict]] = []

#     def flush():
#         nonlocal buffer
#         if not buffer:
#             return True
#         ok = commit_ops_with_retry(db, buffer, commit_sleep_s=commit_sleep_s)
#         if not ok:
#             # write a small DLQ to inspect (optional)
#             logging.error(f"[worker {worker_id}] DLQ for {len(buffer)} ops (see dlq_worker{worker_id}.txt)")
#             try:
#                 with open(f"dlq_worker{worker_id}.txt", "a", encoding="utf-8") as f:
#                     for ref, _ in buffer:
#                         f.write(ref.path + "\n")
#             except Exception: pass
#         buffer = []
#         return ok

#     while True:
#         item = doc_q.get()
#         if item is None:
#             doc_q.task_done()
#             break

#         snap = item
#         data = snap.to_dict() or {}

#         # Skip writes that aren't needed
#         if only_missing and ('is_searchable' in data):
#             counters.add(skipped=1, seen=1)
#         elif (not only_missing) and (data.get('is_searchable') is True):
#             counters.add(skipped=1, seen=1)
#         else:
#             buffer.append((snap.reference, {"is_searchable": True}))
#             touched, skipped, seen, _ = counters.add(touched=1, seen=1)
#             if len(buffer) >= batch_size:
#                 flush()
#             if seen % 10000 == 0:
#                 logging.info(f"[progress] touched={touched}, skipped={skipped}, total_seen={seen}")

#         doc_q.task_done()

#     # flush remaining ops
#     flush()
#     logging.info(f"[worker {worker_id}] done")

# def main():
#     parser = argparse.ArgumentParser(description="Set is_searchable=True across a collection with throttled parallel batches.")
#     parser.add_argument("--collection", default="allplaces")
#     parser.add_argument("--only-missing", action="store_true", help="Only add the field when it's missing.")
#     parser.add_argument("--batch-size", type=int, default=100, help="Writes per commit (<=500). Default: 100.")
#     parser.add_argument("--workers", type=int, default=1, help="Parallel writer threads. Default: 1.")
#     parser.add_argument("--writes-per-sec", type=int, default=100, help="Global target write rate. Default: 100.")
#     parser.add_argument("--queue-size", type=int, default=2000, help="Doc queue size to bound memory. Default: 2000.")
#     parser.add_argument("--resume-after", help="Resume streaming after this document ID.")
#     args = parser.parse_args()

#     init_firebase_once()
#     db = firestore.client()

#     # throttle: compute per-commit sleep so total write rate ~= writes_per_sec
#     commit_sleep_s = max(0.0, args.batch_size / max(args.writes_per_sec, 1))
    
#     # Add extra throttling to be more conservative
#     if commit_sleep_s < 1.0:
#         commit_sleep_s = max(1.0, commit_sleep_s)  # Minimum 1 second between commits
        
#     logging.info(f"Throttle: ~{args.writes_per_sec} writes/sec -> per-commit sleep {commit_sleep_s:.3f}s "
#                  f"(batch={args.batch_size}, workers={args.workers})")
    
#     if args.writes_per_sec > 200:
#         logging.warning("High write rate detected. Consider reducing --writes-per-sec to avoid quota limits.")
#     if args.workers > 2:
#         logging.warning("Multiple workers detected. Consider using --workers 1 if hitting quota limits.")

#     doc_q: "queue.Queue" = queue.Queue(maxsize=args.queue_size)
#     counters = Counters()

#     # Start workers
#     threads = []
#     for i in range(args.workers):
#         t = threading.Thread(
#             target=worker,
#             args=(doc_q, counters, i+1, args.batch_size, commit_sleep_s, args.only_missing),
#             daemon=True
#         )
#         t.start()
#         threads.append(t)

#     # Producer: stream docs and enqueue with backpressure and retry logic
#     logging.info("Streaming documents...")
#     start = time.time()
#     last_processed_doc_id = args.resume_after
    
#     try:
#         # Use the new retry-enabled streaming function
#         doc_count = 0
#         for snap in stream_documents_with_retry(db, args.collection, start_after_doc_id=last_processed_doc_id):
#             doc_q.put(snap)  # blocks when workers are throttled, which is fine
#             last_processed_doc_id = snap.id
#             doc_count += 1
            
#             # Periodically log the last processed doc ID for manual resumption if needed
#             if doc_count % 25000 == 0:
#                 logging.info(f"[checkpoint] Processed {doc_count} docs. Last doc ID: {last_processed_doc_id}")
                
#     except KeyboardInterrupt:
#         logging.warning("Interrupted by user. Draining queue and shutting down...")
#         logging.info(f"[resume point] To resume, use: --resume-after {last_processed_doc_id}")
#     except Exception as e:
#         error_msg = str(e)
#         if "quota" in error_msg.lower() or "resource exhausted" in error_msg.lower():
#             logging.error("Quota exhausted. Try reducing --writes-per-sec, --workers, or --batch-size")
#             logging.info("Suggested command:")
#             logging.info(f"python update_firestore.py --collection {args.collection} --only-missing "
#                         f"--batch-size 50 --workers 1 --writes-per-sec 50 --resume-after {last_processed_doc_id}")
#         else:
#             logging.error(f"Fatal error during streaming: {error_msg}")
#         logging.info(f"[resume point] To resume, use: --resume-after {last_processed_doc_id}")
#         raise
#     finally:
#         # send sentinels
#         for _ in threads:
#             doc_q.put(None)
#         # wait for finish
#         for t in threads:
#             t.join()

#     duration = time.time() - start
#     touched, skipped, seen, errors = counters.touched, counters.skipped, counters.seen, counters.errors
#     logging.info("All set ✅")
#     logging.info(f"Collection: {args.collection}")
#     logging.info(f"Scanned:    {seen}")
#     logging.info(f"Updated T:  {touched} (set True)")
#     logging.info(f"Skipped:    {skipped}")
#     logging.info(f"Errors:     {errors}")
#     logging.info(f"Elapsed:    {duration:.1f}s  (~{seen/max(duration,1):.1f} docs/s)")

# if __name__ == "__main__":
#     main()

# #!/usr/bin/env python3
# import os, logging, threading, queue, time, random, argparse
# from typing import List, Tuple

# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.api_core import exceptions as gexc
# from google.cloud.firestore_v1.field_path import FieldPath

# # ──────────────────────────────────────────
# # HARD-CODED SERVICE ACCOUNT PATH
# # ──────────────────────────────────────────
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# # ──────────────────────────────────────────
# # Logging
# # ──────────────────────────────────────────
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# # ──────────────────────────────────────────
# def init_firebase_once():
#     try:
#         firebase_admin.get_app()
#         logging.info("Firebase app already initialized.")
#     except ValueError:
#         if not os.path.exists(SERVICE_ACCOUNT_PATH):
#             raise SystemExit(f"Service account not found:\n{SERVICE_ACCOUNT_PATH}")
#         logging.info(f"Initializing Firebase app with:\n{SERVICE_ACCOUNT_PATH}")
#         cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
#         firebase_admin.initialize_app(cred)

# class Counters:
#     def __init__(self):
#         self.touched = 0
#         self.skipped = 0
#         self.seen    = 0
#         self.errors  = 0
#         self._lock   = threading.Lock()
#     def add(self, *, touched=0, skipped=0, seen=0, errors=0):
#         with self._lock:
#             self.touched += touched
#             self.skipped += skipped
#             self.seen    += seen
#             self.errors  += errors
#             return (self.touched, self.skipped, self.seen, self.errors)

# def is_retryable(err: Exception) -> bool:
#     return isinstance(err, (gexc.ResourceExhausted, gexc.DeadlineExceeded, gexc.ServiceUnavailable, gexc.Aborted))

# def commit_ops_with_retry(db, ops: List[Tuple[firestore.DocumentReference, dict]],
#                           commit_sleep_s: float,
#                           max_retries: int = 8,
#                           base_backoff: float = 0.5,
#                           max_backoff: float = 30.0) -> bool:
#     """Commit a list of operations as one batch with retries + throttle."""
#     attempt = 0
#     while True:
#         batch = db.batch()
#         for ref, data in ops:
#             batch.update(ref, data)
#         try:
#             batch.commit(timeout=120)  # 2 min safety
#             # throttle total write rate
#             if commit_sleep_s > 0:
#                 time.sleep(commit_sleep_s)
#             return True
#         except Exception as e:
#             if not is_retryable(e) or attempt >= max_retries:
#                 logging.error(f"[commit] giving up after {attempt} retries: {e}")
#                 return False
#             # exponential backoff (+ jitter)
#             sleep_s = min(max_backoff, base_backoff * (2 ** attempt)) + random.uniform(0, 0.25)
#             logging.warning(f"[commit] retryable error ({type(e).__name__}): sleeping {sleep_s:.2f}s (attempt {attempt+1})")
#             time.sleep(sleep_s)
#             attempt += 1

# def worker(doc_q: "queue.Queue",
#            counters: Counters,
#            worker_id: int,
#            batch_size: int,
#            commit_sleep_s: float,
#            only_missing: bool):
#     db = firestore.client()
#     buffer: List[Tuple[firestore.DocumentReference, dict]] = []

#     def flush():
#         nonlocal buffer
#         if not buffer:
#             return True
#         ok = commit_ops_with_retry(db, buffer, commit_sleep_s=commit_sleep_s)
#         if not ok:
#             # write a small DLQ to inspect (optional)
#             logging.error(f"[worker {worker_id}] DLQ for {len(buffer)} ops (see dlq_worker{worker_id}.txt)")
#             try:
#                 with open(f"dlq_worker{worker_id}.txt", "a", encoding="utf-8") as f:
#                     for ref, _ in buffer:
#                         f.write(ref.path + "\n")
#             except Exception: pass
#         buffer = []
#         return ok

#     while True:
#         item = doc_q.get()
#         if item is None:
#             doc_q.task_done()
#             break

#         snap = item
#         data = snap.to_dict() or {}

#         # Skip writes that aren't needed
#         if only_missing and ('is_searchable' in data):
#             counters.add(skipped=1, seen=1)
#         elif (not only_missing) and (data.get('is_searchable') is True):
#             counters.add(skipped=1, seen=1)
#         else:
#             buffer.append((snap.reference, {"is_searchable": True}))
#             touched, skipped, seen, _ = counters.add(touched=1, seen=1)
#             if len(buffer) >= batch_size:
#                 flush()
#             if seen % 10000 == 0:
#                 logging.info(f"[progress] touched={touched}, skipped={skipped}, total_seen={seen}")

#         doc_q.task_done()

#     # flush remaining ops
#     flush()
#     logging.info(f"[worker {worker_id}] done")

# def main():
#     parser = argparse.ArgumentParser(description="Set is_searchable=True across a collection with throttled parallel batches.")
#     parser.add_argument("--collection", default="allplaces")
#     parser.add_argument("--only-missing", action="store_true", help="Only add the field when it's missing.")
#     parser.add_argument("--batch-size", type=int, default=250, help="Writes per commit (<=500). Default: 250.")
#     parser.add_argument("--workers", type=int, default=3, help="Parallel writer threads. Default: 3.")
#     parser.add_argument("--writes-per-sec", type=int, default=600, help="Global target write rate. Default: 600.")
#     parser.add_argument("--queue-size", type=int, default=2000, help="Doc queue size to bound memory. Default: 2000.")
#     args = parser.parse_args()

#     init_firebase_once()
#     db = firestore.client()

#     # throttle: compute per-commit sleep so total write rate ~= writes_per_sec
#     commit_sleep_s = max(0.0, args.batch_size / max(args.writes_per_sec, 1))
#     logging.info(f"Throttle: ~{args.writes_per_sec} writes/sec -> per-commit sleep {commit_sleep_s:.3f}s "
#                  f"(batch={args.batch_size}, workers={args.workers})")

#     doc_q: "queue.Queue" = queue.Queue(maxsize=args.queue_size)
#     counters = Counters()

#     # Start workers
#     threads = []
#     for i in range(args.workers):
#         t = threading.Thread(
#             target=worker,
#             args=(doc_q, counters, i+1, args.batch_size, commit_sleep_s, args.only_missing),
#             daemon=True
#         )
#         t.start()
#         threads.append(t)

#     # Producer: stream docs and enqueue with backpressure
#     logging.info("Streaming documents...")
#     start = time.time()
#     try:
#         # Order by __name__ (document ID) keeps streaming stable for restarts
#         q = db.collection(args.collection).order_by(FieldPath.document_id())
#         for snap in q.stream():
#             doc_q.put(snap)  # blocks when workers are throttled, which is fine
#     except KeyboardInterrupt:
#         logging.warning("Interrupted. Draining queue and shutting down...")
#     finally:
#         # send sentinels
#         for _ in threads:
#             doc_q.put(None)
#         # wait for finish
#         for t in threads:
#             t.join()

#     duration = time.time() - start
#     touched, skipped, seen, errors = counters.touched, counters.skipped, counters.seen, counters.errors
#     logging.info("All set ✅")
#     logging.info(f"Collection: {args.collection}")
#     logging.info(f"Scanned:    {seen}")
#     logging.info(f"Updated T:  {touched} (set True)")
#     logging.info(f"Skipped:    {skipped}")
#     logging.info(f"Errors:     {errors}")
#     logging.info(f"Elapsed:    {duration:.1f}s  (~{seen/max(duration,1):.1f} docs/s)")

# if __name__ == "__main__":
#     main()


# #!/usr/bin/env python3
# import os
# import argparse
# import logging
# from typing import List, Tuple

# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.cloud.firestore_v1.field_path import FieldPath

# # ──────────────────────────────────────────
# # HARD-CODED SERVICE ACCOUNT PATH (edit if needed)
# # ──────────────────────────────────────────
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# # ──────────────────────────────────────────
# # Logging
# # ──────────────────────────────────────────
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

# # ──────────────────────────────────────────
# # Firebase init
# # ──────────────────────────────────────────
# def initialize_firebase() -> None:
#     """Initializes Firebase Admin SDK (idempotent) using hard-coded path."""
#     try:
#         firebase_admin.get_app()
#         logging.info("Firebase app already initialized.")
#     except ValueError:
#         if not os.path.exists(SERVICE_ACCOUNT_PATH):
#             raise SystemExit(f"ERROR: Service account file not found:\n{SERVICE_ACCOUNT_PATH}")
#         logging.info(f"Initializing Firebase app with:\n{SERVICE_ACCOUNT_PATH}")
#         cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
#         firebase_admin.initialize_app(cred)

# # ──────────────────────────────────────────
# # Helpers
# # ──────────────────────────────────────────
# def commit_batch(db, batch, pending: int) -> Tuple[firestore.WriteBatch, int]:
#     if pending > 0:
#         batch.commit()
#     return db.batch(), 0

# def chunk(lst: List[str], size: int) -> List[List[str]]:
#     for i in range(0, len(lst), size):
#         yield lst[i:i+size]

# # ──────────────────────────────────────────
# # Core tasks
# # ──────────────────────────────────────────
# def set_is_searchable_for_all(collection_name: str, value: bool = True, only_missing: bool = False,
#                               batch_limit: int = 450) -> Tuple[int, int, int]:
#     """
#     Iterates all docs in collection and sets is_searchable=<value>.
#     Returns (touched, skipped, total).
#     """
#     db = firestore.client()
#     col = db.collection(collection_name)

#     total = 0
#     touched = 0
#     skipped = 0

#     batch = db.batch()
#     pending = 0

#     logging.info(
#         f"Scanning collection '{collection_name}' to set is_searchable={value} "
#         f"({'only if missing' if only_missing else 'for all docs'})..."
#     )
#     for snap in col.stream():
#         total += 1
#         data = snap.to_dict() or {}

#         if only_missing and ('is_searchable' in data):
#             skipped += 1
#             continue

#         # Avoid a write if it's already the same value
#         if (not only_missing) and data.get('is_searchable') == value:
#             skipped += 1
#             continue

#         # Only updates this field; does NOT touch other fields
#         batch.update(snap.reference, {"is_searchable": value})
#         touched += 1
#         pending += 1

#         if pending >= batch_limit:
#             batch, pending = commit_batch(db, batch, pending)

#         if touched % 2000 == 0:
#             logging.info(f"Progress: touched={touched}, skipped={skipped}, total_seen={total}")

#     batch, pending = commit_batch(db, batch, pending)
#     logging.info(f"Set complete. touched={touched}, skipped={skipped}, total_scanned={total}")
#     return touched, skipped, total

# def set_is_searchable_false_for_ids(collection_name: str, ids_to_false: List[str],
#                                     batch_limit: int = 450) -> int:
#     """
#     Flips is_searchable=False for specific document IDs.
#     IDs must match the Firestore DOCUMENT IDs for this collection.
#     """
#     if not ids_to_false:
#         return 0

#     db = firestore.client()
#     col = db.collection(collection_name)

#     total_changed = 0
#     batch = db.batch()
#     pending = 0

#     logging.info(f"Flipping {len(ids_to_false)} doc IDs to is_searchable=False...")

#     # Firestore 'in' max is 10 values
#     for group in chunk(ids_to_false, 10):
#         query = col.where(FieldPath.document_id(), "in", group)
#         docs = list(query.stream())
#         found = {d.id for d in docs}

#         # Update found
#         for d in docs:
#             batch.update(d.reference, {"is_searchable": False})
#             total_changed += 1
#             pending += 1
#             if pending >= batch_limit:
#                 batch, pending = commit_batch(db, batch, pending)

#         # Warn missing
#         missing = set(group) - found
#         if missing:
#             logging.warning(f"Document IDs not found in this chunk: {sorted(missing)}")

#     batch, pending = commit_batch(db, batch, pending)
#     logging.info(f"IDs flip complete. changed={total_changed}")
#     return total_changed

# # ──────────────────────────────────────────
# # CLI
# # ──────────────────────────────────────────
# def main():
#     parser = argparse.ArgumentParser(description="Set is_searchable on all docs in a Firestore collection.")
#     parser.add_argument("--collection", default="allplaces", help="Firestore collection name (default: allplaces).")
#     parser.add_argument("--only-missing", action="store_true",
#                         help="Only set the field when it's missing (skip docs that already have it).")
#     parser.add_argument("--batch-size", type=int, default=450, help="Writes per commit (<=500). Default: 450.")
#     parser.add_argument("--ids-to-false", type=str, default="",
#                         help="Comma-separated Firestore DOCUMENT IDs to set is_searchable=False after the global pass.")
#     args = parser.parse_args()

#     initialize_firebase()

#     # Step A: set True for all (or only missing)
#     touched, skipped, total = set_is_searchable_for_all(
#         collection_name=args.collection,
#         value=True,
#         only_missing=args.only_missing,
#         batch_limit=args.batch_size
#     )

#     # Step B: optionally flip specific IDs to False
#     ids_list = [s.strip() for s in args.ids_to_false.split(",") if s.strip()] if args.ids_to_false else []
#     changed = 0
#     if ids_list:
#         changed = set_is_searchable_false_for_ids(
#             collection_name=args.collection,
#             ids_to_false=ids_list,
#             batch_limit=args.batch_size
#         )

#     logging.info("Done. Summary:")
#     logging.info(f"  Collection: {args.collection}")
#     logging.info(f"  Scanned:    {total}")
#     logging.info(f"  Updated T:  {touched} (set True)")
#     logging.info(f"  Skipped:    {skipped}")
#     logging.info(f"  Flipped F:  {changed} (set False for IDs)")
#     logging.info("All set ✅")

# if __name__ == "__main__":
#     main()





# #!/usr/bin/env python3

# import os
# import argparse
# import logging

# import firebase_admin
# from firebase_admin import credentials, firestore
# from google.cloud.firestore_v1.field_path import FieldPath  # Correct import

# # Set up basic logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

# def initialize_firebase():
#     """Initializes the Firebase Admin SDK."""
#     firebase_credentials_path = os.path.join(
#         os.getcwd(),
#         r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     )
#     try:
#         # Avoid re-initializing if already done
#         firebase_admin.get_app()
#         logging.info("Firebase app already initialized.")
#     except ValueError:
#         logging.info("Initializing Firebase app.")
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)

# def update_specific_documents(place_ids_to_update):
#     """
#     Updates documents in the 'allplaces' collection for the given place_ids.
#     Adds/updates the 'city_name_search' field with the lowercase city name.
#     Handles Firestore's 'in' query limit by batching IDs.
#     """
#     db = firestore.client()
#     places_collection = db.collection('allplaces')

#     # Filter out any None or empty IDs and convert to strings
#     place_ids_str = [
#         str(id) for id in place_ids_to_update
#         if id is not None and str(id).strip()
#     ]

#     if not place_ids_str:
#         logging.warning("No valid place IDs provided. Exiting.")
#         return

#     firestore_in_limit = 10
#     id_batches = [
#         place_ids_str[i:i + firestore_in_limit]
#         for i in range(0, len(place_ids_str), firestore_in_limit)
#     ]

#     total_updated = 0
#     logging.info(f"Updating {len(place_ids_str)} documents in batches of {firestore_in_limit}.")

#     for batch_index, batch_ids in enumerate(id_batches, start=1):
#         logging.info(f"Batch {batch_index}/{len(id_batches)}: {batch_ids}")
#         try:
#             # Query by document ID
#             query = places_collection.where(FieldPath.document_id(), 'in', batch_ids)
#             docs = list(query.stream())

#             if not docs:
#                 logging.warning(f"No documents found for batch {batch_index}.")
#                 continue

#             batch = db.batch()
#             updated_in_batch = 0
#             found_ids = set()

#             for doc in docs:
#                 data = doc.to_dict()
#                 city_name = data.get('city_name')
#                 if isinstance(city_name, str) and city_name:
#                     batch.update(doc.reference, {
#                         'city_name_search': city_name.lower()
#                     })
#                     updated_in_batch += 1
#                     found_ids.add(doc.id)
#                     logging.debug(f"Queued update for {doc.id}")
#                 else:
#                     logging.warning(f"Skipping {doc.id}: invalid or missing 'city_name'")

#             batch.commit()
#             total_updated += updated_in_batch
#             logging.info(f"Batch {batch_index} committed: {updated_in_batch} updated.")

#             missing = set(batch_ids) - found_ids
#             if missing:
#                 logging.warning(f"IDs not found in Firestore for batch {batch_index}: {missing}")

#         except Exception as e:
#             logging.error(f"Error in batch {batch_index}: {e}")
#             continue

#     logging.info(f"Done. Total documents updated: {total_updated}")

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(
#         description='Update specific documents in Firestore based on place_ids.'
#     )
#     parser.add_argument(
#         '--place_ids',
#         type=str,
#         required=True,
#         help='Comma-separated list of place_ids (e.g. "123,456,789")'
#     )

#     args = parser.parse_args()
#     initialize_firebase()

#     # Parse and normalize IDs
#     raw_ids = [s.strip() for s in args.place_ids.split(',') if s.strip()]
#     processed_ids = []
#     for s in raw_ids:
#         try:
#             processed_ids.append(int(s))
#         except ValueError:
#             processed_ids.append(s)

#     logging.info(f"Received {len(processed_ids)} place IDs.")
#     update_specific_documents(processed_ids)

# Working for allplaces collection
# import firebase_admin
# from firebase_admin import credentials, firestore
# import os

# def update_all_documents():
#     # 1. Initialize the Firebase Admin SDK once.
#     firebase_credentials_path = os.path.join(
#         os.getcwd(), 
#         r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#     )
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)

#     db = firestore.client()
#     places_collection = db.collection('allplaces')
    
#     # 2. Pagination settings
#     page_size = 500  # How many docs to fetch in each batch
#     last_doc = None  # We'll store the last doc reference here to do startAfter
#     updated_count = 0

#     while True:
#         # 3. Build the query for the next page
#         query = places_collection.order_by('__name__').limit(page_size)
        
#         # If we have a last_doc from the previous iteration, start after it
#         if last_doc:
#             query = query.start_after({'__name__': last_doc.id})

#         # 4. Fetch docs in the current page
#         docs = list(query.stream())  # Convert the generator to a list
#         if not docs:
#             # No more docs -> break
#             break
        
#         # 5. Create a Firestore batch to update documents
#         batch = db.batch()
#         for doc in docs:
#             data = doc.to_dict()
#             city_name = data.get('city_name')
#             if city_name:
#                 batch.update(doc.reference, {
#                     'city_name_search': city_name.lower()
#                 })
#                 updated_count += 1

#         # 6. Commit the batch for this page
#         batch.commit()

#         # 7. Update last_doc to the last doc in this page for pagination
#         last_doc = docs[-1]

#     print(f'All documents updated successfully. Updated {updated_count} docs.')

# if __name__ == '__main__':
#     update_all_documents()



# working for explore collection
# def update_all_documents():
#     # Initialize the Firebase Admin SDK
#     firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#     cred = credentials.Certificate(firebase_credentials_path)
#     firebase_admin.initialize_app(cred)

#     db = firestore.client()

#     # Reference to the 'allplaces' collection
#     places_collection = db.collection('allplaces')

#     # Get all documents in the collection
#     docs = places_collection.stream()

#     # Batch write to Firestore (max 500 operations per batch)
#     batch = db.batch()
#     batch_counter = 0
#     batch_size = 500  # Firestore batch limit

#     for doc in docs:
#         doc_dict = doc.to_dict()
#         city_name = doc_dict.get('city_name')
#         if city_name:
#             doc_ref = doc.reference
#             batch.update(doc_ref, {
#                 'city_name_search': city_name.lower()
#             })
#             batch_counter += 1

#             if batch_counter == batch_size:
#                 # Commit the batch and reset
#                 batch.commit()
#                 batch = db.batch()
#                 batch_counter = 0

#     # Commit any remaining operations
#     if batch_counter > 0:
#         batch.commit()

#     print('All documents updated successfully.')

# if __name__ == '__main__':
#     update_all_documents()
