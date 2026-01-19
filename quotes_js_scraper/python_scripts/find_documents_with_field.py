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

def find_docs_with_field(collection_name: str,
                         field_name: str,
                         max_docs: int = None,
                         page_size: int = 100,
                         output_json: bool = False,
                         output_file: str = None,
                         show_field_value: bool = False):
    """
    Find all documents that have a specific field.
    
    Args:
        collection_name: Name of the Firestore collection
        field_name: Name of the field to check for
        max_docs: Maximum number of documents to check
        page_size: Number of documents per batch
        output_json: If True, save results to JSON file
        output_file: Custom output file path
        show_field_value: If True, display the field value
    """
    
    init_firebase_once()
    db = firestore.client()
    collection_ref = db.collection(collection_name)
    
    logging.info("=" * 70)
    logging.info(f"Finding documents with field '{field_name}'")
    logging.info("=" * 70)
    logging.info(f"Collection:    {collection_name}")
    logging.info(f"Field name:    {field_name}")
    logging.info(f"Max docs:      {max_docs if max_docs else 'ALL'}")
    logging.info("=" * 70)
    logging.info("")
    
    # Counters
    total_checked = 0
    docs_with_field = 0
    matching_docs = []
    
    last_doc_id = None
    start_time = datetime.now()
    
    try:
        while True:
            # Check if we've hit the max limit
            if max_docs and total_checked >= max_docs:
                logging.info(f"Reached maximum document limit: {max_docs}")
                break
            
            # Calculate batch size
            current_batch_size = page_size
            if max_docs:
                remaining = max_docs - total_checked
                current_batch_size = min(page_size, remaining)
            
            # Build query
            query = collection_ref.order_by('__name__').limit(current_batch_size)
            
            if last_doc_id:
                query = query.start_after({'__name__': last_doc_id})
            
            # Fetch documents
            docs = list(query.stream())
            if not docs:
                logging.info("No more documents found - reached end of collection.")
                break
            
            # Check each document
            for doc in docs:
                total_checked += 1
                data = doc.to_dict() or {}
                
                if field_name in data:
                    docs_with_field += 1
                    
                    doc_info = {
                        'document_id': doc.id,
                        'field_name': field_name
                    }
                    
                    if show_field_value:
                        field_value = data[field_name]
                        doc_info['field_value'] = field_value
                        doc_info['field_type'] = type(field_value).__name__
                    
                    matching_docs.append(doc_info)
            
            # Update pagination
            last_doc_id = docs[-1].id
            
            # Progress logging
            if total_checked % 1000 == 0 or len(docs) < current_batch_size:
                logging.info(f"Checked {total_checked:,} documents | Found {docs_with_field:,} with field '{field_name}'")
        
        # Display results
        logging.info("")
        logging.info("=" * 70)
        logging.info("RESULTS")
        logging.info("=" * 70)
        logging.info(f"Total documents checked:  {total_checked:,}")
        logging.info(f"Documents with field:     {docs_with_field:,} ({docs_with_field/total_checked*100 if total_checked > 0 else 0:.1f}%)")
        logging.info("=" * 70)
        logging.info("")
        
        if matching_docs:
            logging.info(f"Document IDs with field '{field_name}':")
            logging.info("")
            
            # Show first 20 document IDs
            display_limit = min(20, len(matching_docs))
            for i in range(display_limit):
                doc_info = matching_docs[i]
                if show_field_value:
                    logging.info(f"  {i+1}. {doc_info['document_id']} | Value: {doc_info['field_value']} ({doc_info['field_type']})")
                else:
                    logging.info(f"  {i+1}. {doc_info['document_id']}")
            
            if len(matching_docs) > display_limit:
                logging.info(f"  ... and {len(matching_docs) - display_limit} more")
            
            logging.info("")
            logging.info(f"All document IDs: {', '.join([d['document_id'] for d in matching_docs])}")
        else:
            logging.warning(f"No documents found with field '{field_name}'")
        
        # Save to JSON if requested
        if output_json and matching_docs:
            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"docs_with_{field_name}_{collection_name}_{timestamp}.json"
            
            output_data = {
                'metadata': {
                    'collection': collection_name,
                    'field_name': field_name,
                    'scan_timestamp': datetime.now().isoformat(),
                    'total_documents_checked': total_checked,
                    'documents_with_field': docs_with_field
                },
                'results': matching_docs,
                'summary': {
                    'total_found': len(matching_docs),
                    'document_ids': [d['document_id'] for d in matching_docs]
                }
            }
            
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, indent=2, ensure_ascii=False)
                logging.info(f"Results saved to: {output_file}")
            except Exception as e:
                logging.error(f"Error saving JSON file: {e}")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logging.info("")
        logging.info(f"Time elapsed: {elapsed:.1f}s")
        logging.info(f"Quota used: ~{total_checked:,} reads")
        
    except Exception as e:
        logging.error(f"Error during search: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(
        description="Find Firestore documents that have a specific field.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Find docs with 'subcollection' field in playlistsNew
  python %(prog)s --collection playlistsNew --field subcollection
  
  # Show field values
  python %(prog)s --collection playlistsNew --field subcollection --show-values
  
  # Save to JSON
  python %(prog)s --collection playlistsNew --field subcollection --json
  
  # Check only first 1000 docs
  python %(prog)s --collection playlistsNew --field subcollection --max-docs 1000
        """
    )
    
    parser.add_argument("--collection", required=True,
                       help="Collection name")
    parser.add_argument("--field", required=True,
                       help="Field name to check for")
    parser.add_argument("--max-docs", type=int,
                       help="Maximum documents to check")
    parser.add_argument("--page-size", type=int, default=100,
                       help="Documents per batch. Default: 100")
    parser.add_argument("--json", action="store_true",
                       help="Save results to JSON file")
    parser.add_argument("--output", "-o",
                       help="Custom output JSON file path")
    parser.add_argument("--show-values", action="store_true",
                       help="Display field values in output")
    
    args = parser.parse_args()
    
    # Validate
    if args.page_size < 1:
        parser.error("page-size must be at least 1")
    
    # Run search
    find_docs_with_field(
        collection_name=args.collection,
        field_name=args.field,
        max_docs=args.max_docs,
        page_size=args.page_size,
        output_json=args.json,
        output_file=args.output,
        show_field_value=args.show_values
    )

if __name__ == "__main__":
    main()