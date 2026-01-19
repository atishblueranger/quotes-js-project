# #!/usr/bin/env python3
# import os, logging, time, argparse, json
# import firebase_admin
# from firebase_admin import credentials, firestore

# # [... Same constants and init_firebase_once as before ...]
# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# def init_firebase_once():
#     """Initialize Firebase Admin SDK once."""
#     try:
#         firebase_admin.get_app()
#     except ValueError:
#         if not os.path.exists(SERVICE_ACCOUNT_PATH):
#             raise SystemExit(f"Service account not found:\n{SERVICE_ACCOUNT_PATH}")
#         cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
#         firebase_admin.initialize_app(cred)

# def chunk_list(data, chunk_size):
#     """Yield successive chunks from data."""
#     for i in range(0, len(data), chunk_size):
#         yield data[i:i + chunk_size]

# def process_query_update(collection_name: str, 
#                          popularity_list: list,
#                          batch_size: int = 400, 
#                          dry_run: bool = False):
#     """
#     Query Firestore for documents matching ANY of the popularity scores.
#     Handles the Firestore limit of 10 items per 'in' query.
#     """
#     init_firebase_once()
#     db = firestore.client()
#     collection_ref = db.collection(collection_name)

#     # Remove duplicates and sort for cleaner logs
#     target_values = sorted(list(set(popularity_list)))
    
#     logging.info("=" * 70)
#     logging.info(f"🔎 STARTING MULTI-VALUE QUERY")
#     logging.info(f"   Collection: {collection_name}")
#     logging.info(f"   Popularities: {target_values}")
#     logging.info(f"   Action: Set is_searchable = False")
#     logging.info("=" * 70)

#     total_scanned = 0
#     updated_count = 0
#     batch = db.batch()
#     batch_count = 0
#     start_time = time.time()

#     # Firestore allows max 10 values in an 'in' query.
#     # We chunk the input list into groups of 10.
#     chunks = list(chunk_list(target_values, 10))

#     for chunk_index, chunk in enumerate(chunks, 1):
#         logging.info(f"🔄 Processing query chunk {chunk_index}/{len(chunks)}: popularity in {chunk}")
        
#         # USE THE 'in' OPERATOR HERE
#         query = collection_ref.where(field_path='popularity', op_string='in', value=chunk)
        
#         try:
#             doc_stream = query.stream()
            
#             for doc in doc_stream:
#                 total_scanned += 1
#                 doc_data = doc.to_dict()
#                 doc_id = doc.id
                
#                 # Optimization: Skip if already False
#                 if doc_data.get('is_searchable') is False:
#                     continue

#                 if dry_run:
#                     logging.info(f"[DRY RUN] Would update {doc_id} (Pop: {doc_data.get('popularity')})")
#                     updated_count += 1
#                 else:
#                     doc_ref = collection_ref.document(doc_id)
#                     batch.update(doc_ref, {'is_searchable': False})
#                     batch_count += 1
#                     updated_count += 1

#                     if batch_count >= batch_size:
#                         batch.commit()
#                         logging.info(f"   ✓ Committed batch of {batch_count} updates...")
#                         batch = db.batch()
#                         batch_count = 0
#                         time.sleep(0.1)

#         except Exception as e:
#             logging.error(f"❌ Error querying chunk {chunk}: {e}")

#     # Final batch commit
#     if batch_count > 0 and not dry_run:
#         batch.commit()
#         logging.info(f"   ✓ Committed final batch of {batch_count} updates.")

#     elapsed = time.time() - start_time
#     logging.info("\n" + "=" * 70)
#     logging.info(f"🎯 FINAL SUMMARY")
#     logging.info(f"   Target Popularities: {len(target_values)}")
#     logging.info(f"   Docs Scanned:       {total_scanned}")
#     logging.info(f"   Docs Updated:       {updated_count}")
#     logging.info(f"   Time Elapsed:       {elapsed:.1f}s")
#     logging.info("=" * 70)

# def main():
#     parser = argparse.ArgumentParser(description="Update documents based on Popularity.")
    
#     parser.add_argument("--collection", default="allplaces", help="Collection name")
#     parser.add_argument("--dry-run", action="store_true", help="Simulate only")
#     parser.add_argument("--batch-size", type=int, default=400, help="Batch size")
    
#     # MODIFIED ARGUMENT: nargs='+' allows multiple values
#     parser.add_argument("--filter-popularity", type=int, nargs='+',
#                         help="List of popularity scores (e.g. 8 9 10 11)")

#     args = parser.parse_args()

#     if args.filter_popularity:
#         if not args.dry_run:
#             print(f"⚠️  WARNING: Updating ALL documents in '{args.collection}'")
#             print(f"   where popularity is in: {args.filter_popularity}")
#             if input("Type 'yes' to proceed: ").lower() != 'yes': return

#         process_query_update(
#             collection_name=args.collection,
#             popularity_list=args.filter_popularity, # Pass the list
#             batch_size=args.batch_size,
#             dry_run=args.dry_run
#         )
#     else:
#         print("Please provide --filter-popularity [numbers...]")

# if __name__ == "__main__":
#     main()

#!/usr/bin/env python3
import os, logging, time, argparse, json
import firebase_admin
from firebase_admin import credentials, firestore

# [Previous constants and init_firebase_once remain the same...]
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

def process_query_update(collection_name: str, 
                         popularity_filter: int,
                         batch_size: int = 50, 
                         dry_run: bool = False):
    """
    Query Firestore for documents matching popularity and update them.
    """
    init_firebase_once()
    db = firestore.client()
    collection_ref = db.collection(collection_name)

    # 1. Create the Query
    # This does NOT download data yet. It just defines the question.
    query = collection_ref.where(field_path='popularity', op_string='==', value=popularity_filter)

    logging.info("=" * 70)
    logging.info(f"🔎 STARTING QUERY MODE")
    logging.info(f"   Query: {collection_name} where popularity == {popularity_filter}")
    logging.info(f"   Action: Set is_searchable = False")
    logging.info("=" * 70)

    # 2. Counters
    total_scanned = 0
    updated_count = 0
    batch = db.batch()
    batch_count = 0
    
    start_time = time.time()

    # 3. Stream the results
    # .stream() is efficient; it downloads docs in chunks automatically
    try:
        doc_stream = query.stream()
        
        for doc in doc_stream:
            total_scanned += 1
            doc_data = doc.to_dict()
            doc_id = doc.id
            
            # Optional: Client-side check if you want to be extra safe
            # if doc_data.get('popularity') != popularity_filter: continue

            # Skip if already False (Save write costs)
            if doc_data.get('is_searchable') is False:
                logging.debug(f"Skipping {doc_id} (already False)")
                continue

            if dry_run:
                logging.info(f"[DRY RUN] Would update {doc_id} (Popularity: {doc_data.get('popularity')})")
                updated_count += 1
            else:
                # Add to write batch
                doc_ref = collection_ref.document(doc_id)
                batch.update(doc_ref, {'is_searchable': False})
                batch_count += 1
                updated_count += 1

                # Commit batch if full
                if batch_count >= batch_size:
                    batch.commit()
                    logging.info(f"✓ Committed batch of {batch_count} updates...")
                    batch = db.batch() # Reset batch
                    batch_count = 0
                    time.sleep(0.1) # Tiny sleep to prevent hotspotting

        # Commit any remaining in the final batch
        if batch_count > 0 and not dry_run:
            batch.commit()
            logging.info(f"✓ Committed final batch of {batch_count} updates.")

    except Exception as e:
        logging.error(f"❌ Error during query processing: {e}")
        return

    # 4. Summary
    elapsed = time.time() - start_time
    logging.info("\n" + "=" * 70)
    logging.info(f"🎯 QUERY SUMMARY")
    logging.info(f"   Docs Found (Reads): {total_scanned}")
    logging.info(f"   Docs Updated:       {updated_count}")
    logging.info(f"   Time Elapsed:       {elapsed:.1f}s")
    logging.info("=" * 70)

# [Previous helper functions like load_ids_from_json remain here...]

def main():
    parser = argparse.ArgumentParser(description="Update documents based on ID list OR Query.")
    
    # Existing args
    parser.add_argument("--collection", default="allplaces", help="Collection name")
    parser.add_argument("--from-json", help="JSON file containing IDs")
    parser.add_argument("--ids", nargs='+', help="Specific IDs")
    parser.add_argument("--dry-run", action="store_true", help="Simulate only")
    parser.add_argument("--batch-size", type=int, default=400, help="Batch size") # Increased default for batching
    
    # NEW ARGUMENT
    parser.add_argument("--filter-popularity", type=int, 
                        help="Run in Query Mode: Process all docs with this popularity score")

    args = parser.parse_args()

    # LOGIC BRANCHING
    if args.filter_popularity is not None:
        # --- PATH A: QUERY MODE ---
        if not args.dry_run:
            print(f"⚠️  WARNING: You are about to update ALL documents in '{args.collection}'")
            print(f"   where popularity == {args.filter_popularity}.")
            confirm = input("Type 'yes' to proceed: ")
            if confirm.lower() != 'yes': return

        process_query_update(
            collection_name=args.collection,
            popularity_filter=args.filter_popularity,
            batch_size=args.batch_size,
            dry_run=args.dry_run
        )
    
    else:
        # --- PATH B: ID LIST MODE (Your original logic) ---
        # (Paste your original doc_ids determination logic here)
        # ...
        pass # Placeholder for your existing ID list logic

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import os, logging, time, argparse, json
import firebase_admin
from firebase_admin import credentials, firestore

# ──────────────────────────────────────────
SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# Hardcode document IDs here (or leave empty to use --from-json or --ids)
DOCUMENT_IDS = [
    # Add your document IDs here, for example:
# "147667",
# "147679",
# "147905",
# "148232",
# "148634",
# "148711",
# "149892",
# "153208",
# "153348",
# "161313",
# "161433",
# "162226",
# "162338",
# "162339",
# "162340",
# "174440",
# "174555",
# "174559",
# "174560",
# "174561",
# "174562",
# "174563",
# "174568",
# "174573",
# "174574",
# "174575",
# "174591",
# "174594",
# "174595",
# "174601",
# "174631",
# "174638",
# "174639",
# "174641",
# "174741",
# "175069",
# "175073",
# "175082",
# "175204",
# "175214",
# "175322",
# "175323",
# "175324",
# "175325",
# "176172",
# "176182",
# "176192",
# "176196",
# "176207",
# "176278",
# "176314",
# "176320",
# "176401",
# "176411",
# "177129",
# "177501",
# "177552",
# "177553",
# "177554",
# "177567",
# "177568",
# "177570",
# "177571",
# "177572",
# "177573",
# "177578",
# "177581",
# "177582",
# "177583",
# "177584",
# "177585",
# "177586",
# "177587",
# "177588",
# "177589",
# "177590",
# "177591",
# "177592",
# "177593",
# "177594",
# "177595",
# "177596",
# "177609",
# "177610",
# "177611",
# "177612",
# "177613",
# "177614",
# "177615",
# "177616",
# "177617",
# "177628",
# "177629",
# "177630",
# "177670",
# "177673",
# "177674",
# "177675",
# "177688",
# # "177694",
# "4646",
# "174526",
# "87057"
# "88231"
"5015"

]

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

def update_is_searchable_to_false(collection_name: str,
                                  doc_ids: list,
                                  batch_size: int = 50,
                                  sleep_between_batches: float = 0.5,
                                  dry_run: bool = False):
    """
    Update is_searchable field to False for specific document IDs.
    
    Args:
        collection_name: Name of the Firestore collection
        doc_ids: List of document IDs to update
        batch_size: Number of documents to update per batch
        sleep_between_batches: Seconds to sleep between batches
        dry_run: If True, only simulate updates without writing
    """
    
    init_firebase_once()
    db = firestore.client()
    collection_ref = db.collection(collection_name)
    
    # Counters
    total_processed = 0
    updated_count = 0
    already_false = 0
    field_missing = 0
    doc_not_found = 0
    error_count = 0
    was_true_count = 0
    
    start_time = time.time()
    mode = "DRY RUN" if dry_run else "LIVE UPDATE"
    
    logging.info("=" * 70)
    logging.info(f"🚀 Starting {mode}")
    logging.info("=" * 70)
    logging.info(f"Collection:       {collection_name}")
    logging.info(f"Action:           Set is_searchable = False")
    logging.info(f"Total docs:       {len(doc_ids):,}")
    logging.info(f"Batch size:       {batch_size}")
    logging.info(f"Sleep interval:   {sleep_between_batches}s")
    logging.info("=" * 70)
    logging.info("")
    
    try:
        # Process in batches
        for i in range(0, len(doc_ids), batch_size):
            batch = doc_ids[i:i + batch_size]
            
            for doc_id in batch:
                total_processed += 1
                
                try:
                    # Get document reference
                    doc_ref = collection_ref.document(str(doc_id))
                    doc = doc_ref.get()
                    
                    # Check if document exists
                    if not doc.exists:
                        doc_not_found += 1
                        logging.warning(f"Document not found: {doc_id}")
                        continue
                    
                    data = doc.to_dict() or {}
                    
                    # Check if field exists
                    if 'is_searchable' not in data:
                        field_missing += 1
                        logging.warning(f"Document {doc_id} doesn't have 'is_searchable' field")
                        continue
                    
                    current_value = data['is_searchable']
                    
                    # Check current value
                    if current_value is False or current_value == False:
                        already_false += 1
                        logging.debug(f"Document {doc_id} already has is_searchable = False")
                        continue
                    
                    # Track if it was True
                    if current_value is True or current_value == True:
                        was_true_count += 1
                    
                    # Update the document
                    if dry_run:
                        logging.info(f"[DRY RUN] Would update {doc_id}: {current_value} → False")
                        updated_count += 1
                    else:
                        doc_ref.update({'is_searchable': False})
                        updated_count += 1
                        logging.info(f"✓ Updated {doc_id}: {current_value} → False")
                        
                except Exception as e:
                    error_count += 1
                    logging.error(f"❌ Error updating {doc_id}: {e}")
            
            # Progress logging
            if total_processed % 100 == 0 or total_processed == len(doc_ids):
                elapsed = time.time() - start_time
                rate = total_processed / elapsed if elapsed > 0 else 0
                
                logging.info("")
                logging.info("─" * 70)
                logging.info(f"📊 PROGRESS - Processed {total_processed:,}/{len(doc_ids):,} ({total_processed/len(doc_ids)*100:.1f}%)")
                logging.info(f"   Rate: {rate:.1f} docs/sec")
                logging.info("─" * 70)
                logging.info(f"  🔄 Updated to False:     {updated_count:6,}")
                logging.info(f"  ✅ Already False:        {already_false:6,}")
                logging.info(f"  ⚠️  Field missing:        {field_missing:6,}")
                logging.info(f"  ⚠️  Doc not found:        {doc_not_found:6,}")
                if error_count > 0:
                    logging.info(f"  ❌ Errors:               {error_count:6,}")
                logging.info("─" * 70)
                logging.info("")
            
            # Sleep between batches
            if sleep_between_batches > 0 and i + batch_size < len(doc_ids):
                time.sleep(sleep_between_batches)
        
    except KeyboardInterrupt:
        logging.warning("\n⚠️  Update interrupted by user.")
        
    except Exception as e:
        error_msg = str(e).lower()
        if 'quota exceeded' in error_msg or 'resource exhausted' in error_msg:
            logging.error("\n" + "=" * 70)
            logging.error("❌ QUOTA EXCEEDED")
            logging.error("=" * 70)
            logging.error("Your Firestore quota has been exhausted.")
            logging.error(f"Processed {total_processed}/{len(doc_ids)} documents")
            logging.error("=" * 70)
        else:
            logging.error(f"❌ Unexpected error: {e}")
        raise
        
    finally:
        # Final summary
        elapsed_time = time.time() - start_time
        avg_rate = total_processed / elapsed_time if elapsed_time > 0 else 0
        
        logging.info("\n" + "=" * 70)
        logging.info(f"🎯 FINAL SUMMARY - {mode}")
        logging.info("=" * 70)
        logging.info(f"Collection:              {collection_name}")
        logging.info(f"Documents processed:     {total_processed:,}/{len(doc_ids):,}")
        logging.info(f"Time elapsed:            {elapsed_time:.1f}s ({avg_rate:.1f} docs/sec)")
        logging.info("")
        logging.info("📊 RESULTS:")
        logging.info(f"  🔄 Updated to False:     {updated_count:6,} ({updated_count/total_processed*100 if total_processed > 0 else 0:.1f}%)")
        logging.info(f"     (Were True: {was_true_count})")
        logging.info(f"  ✅ Already False:        {already_false:6,} ({already_false/total_processed*100 if total_processed > 0 else 0:.1f}%)")
        if field_missing > 0:
            logging.info(f"  ⚠️  Field missing:        {field_missing:6,} ({field_missing/total_processed*100:.1f}%)")
        if doc_not_found > 0:
            logging.info(f"  ⚠️  Doc not found:        {doc_not_found:6,} ({doc_not_found/total_processed*100:.1f}%)")
        if error_count > 0:
            logging.info(f"  ❌ Errors:               {error_count:6,} ({error_count/total_processed*100:.1f}%)")
        logging.info("")
        
        # Quota usage estimate
        reads_used = total_processed
        writes_used = updated_count if not dry_run else 0
        logging.info("📈 QUOTA USAGE:")
        logging.info(f"  Reads:  ~{reads_used:,}")
        logging.info(f"  Writes: ~{writes_used:,}")
        logging.info("")
        
        # Recommendations
        logging.info("💡 SUMMARY:")
        if dry_run:
            logging.info("  This was a DRY RUN - no changes were made.")
            logging.info("  Remove --dry-run flag to apply changes.")
        else:
            if error_count == 0 and doc_not_found == 0 and field_missing == 0:
                logging.info(f"  ✅ Successfully updated {updated_count:,} documents to is_searchable = False!")
            else:
                if error_count > 0:
                    logging.info(f"  ⚠️  {error_count} errors occurred - check logs above")
                if doc_not_found > 0:
                    logging.info(f"  ⚠️  {doc_not_found} documents were not found")
                if field_missing > 0:
                    logging.info(f"  ⚠️  {field_missing} documents don't have the field")
        
        logging.info("=" * 70)

def load_ids_from_json(json_file: str) -> list:
    """Load document IDs from JSON file."""
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Support multiple JSON formats
        if 'missing_document_ids' in data:
            doc_ids = data['missing_document_ids']
        elif 'document_ids' in data:
            doc_ids = data['document_ids']
        elif 'results' in data and isinstance(data['results'], list):
            # Extract IDs from results array
            doc_ids = [r['document_id'] if isinstance(r, dict) else r for r in data['results']]
        elif isinstance(data, list):
            doc_ids = data
        else:
            raise ValueError("Unable to find document IDs in JSON file")
        
        logging.info(f"Loaded {len(doc_ids):,} document IDs from {json_file}")
        return doc_ids
        
    except FileNotFoundError:
        logging.error(f"JSON file not found: {json_file}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON file: {e}")
        raise
    except Exception as e:
        logging.error(f"Error reading JSON file: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(
        description="Update is_searchable field to False for specific documents.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update using hardcoded IDs in script
  python %(prog)s
  
  # Update from JSON file
  python %(prog)s --from-json document_ids.json
  
  # Specify IDs directly in command
  python %(prog)s --ids 12345 67890 86647
  
  # Dry run first
  python %(prog)s --ids 12345 67890 --dry-run
  
  # Different collection
  python %(prog)s --collection myCollection --ids 12345 67890
        """
    )
    
    parser.add_argument("--collection", default="allplaces",
                       help="Collection name. Default: allplaces")
    parser.add_argument("--from-json",
                       help="JSON file containing document IDs")
    parser.add_argument("--ids", nargs='+',
                       help="Document IDs (space-separated)")
    parser.add_argument("--batch-size", type=int, default=50,
                       help="Documents per batch. Default: 50")
    parser.add_argument("--sleep", type=float, default=0.5,
                       help="Seconds between batches. Default: 0.5")
    parser.add_argument("--dry-run", action="store_true",
                       help="Simulate updates without writing")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.batch_size < 1:
        parser.error("batch-size must be at least 1")
    if args.sleep < 0:
        parser.error("Sleep time cannot be negative")
    
    # Determine which document IDs to use
    doc_ids = []
    
    if args.from_json:
        # Load from JSON file
        doc_ids = load_ids_from_json(args.from_json)
    elif args.ids:
        # Use IDs from command line
        doc_ids = args.ids
        logging.info(f"Using {len(doc_ids)} document IDs from command line")
    elif DOCUMENT_IDS:
        # Use hardcoded IDs
        doc_ids = DOCUMENT_IDS
        logging.info(f"Using {len(doc_ids)} hardcoded document IDs")
    else:
        parser.error("No document IDs provided. Use --from-json, --ids, or add IDs to DOCUMENT_IDS in script")
    
    if not doc_ids:
        logging.error("No document IDs to process!")
        return
    
    # Show what will be updated
    logging.info("")
    logging.info("📋 DOCUMENT IDs TO UPDATE:")
    if len(doc_ids) <= 10:
        for doc_id in doc_ids:
            logging.info(f"  - {doc_id}")
    else:
        for doc_id in doc_ids[:5]:
            logging.info(f"  - {doc_id}")
        logging.info(f"  ... and {len(doc_ids) - 5} more")
    logging.info("")
    
    # Confirmation for live updates
    if not args.dry_run:
        logging.warning("⚠️  " + "=" * 60)
        logging.warning("⚠️  LIVE UPDATE MODE")
        logging.warning(f"⚠️  Will update {len(doc_ids)} documents: is_searchable → False")
        logging.warning("⚠️  " + "=" * 60)
        logging.warning("")
        
        response = input("Type 'yes' to continue: ")
        if response.lower() != 'yes':
            logging.info("❌ Update cancelled by user.")
            return
        logging.info("")
    
    # Run the update
    update_is_searchable_to_false(
        collection_name=args.collection,
        doc_ids=doc_ids,
        batch_size=args.batch_size,
        sleep_between_batches=args.sleep,
        dry_run=args.dry_run
    )

if __name__ == "__main__":
    main()