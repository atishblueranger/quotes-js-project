#!/usr/bin/env python3
import os, logging, argparse, json
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

def find_by_title(collection_name: str, 
                 title: str,
                 output_json: bool = False,
                 output_file: str = None,
                 show_full_doc: bool = False):
    """
    Find documents by title field value.
    
    Args:
        collection_name: Name of the Firestore collection
        title: Title value to search for
        output_json: If True, save results to JSON file
        output_file: Custom output file path
        show_full_doc: If True, show full document data
    """
    
    init_firebase_once()
    db = firestore.client()
    collection_ref = db.collection(collection_name)
    
    logging.info("=" * 70)
    logging.info("🔍 SEARCHING FOR DOCUMENTS BY TITLE")
    logging.info("=" * 70)
    logging.info(f"Collection:  {collection_name}")
    logging.info(f"Title:       '{title}'")
    logging.info("=" * 70)
    logging.info("")
    
    try:
        # Query for documents with matching title
        query = collection_ref.where('title', '==', title)
        docs = list(query.stream())
        
        if not docs:
            logging.warning(f"No documents found with title: '{title}'")
            logging.info("")
            logging.info("💡 SUGGESTIONS:")
            logging.info("  - Check if the title is spelled correctly")
            logging.info("  - Title search is case-sensitive")
            logging.info("  - Try searching for partial matches if exact match fails")
            return
        
        # Found documents
        logging.info(f"✅ Found {len(docs)} document(s) with matching title")
        logging.info("")
        
        results = []
        
        for idx, doc in enumerate(docs, 1):
            doc_data = doc.to_dict()
            
            logging.info("─" * 70)
            logging.info(f"📄 RESULT #{idx}")
            logging.info("─" * 70)
            logging.info(f"Document ID:  {doc.id}")
            logging.info(f"Title:        {doc_data.get('title', 'N/A')}")
            
            if show_full_doc:
                logging.info("")
                logging.info("Full Document Data:")
                for key, value in doc_data.items():
                    # Truncate long values for readability
                    if isinstance(value, str) and len(value) > 100:
                        value_display = value[:100] + "..."
                    else:
                        value_display = value
                    logging.info(f"  {key}: {value_display}")
            else:
                # Show just a few key fields
                logging.info(f"Subcategory:  {doc_data.get('subcategory', 'N/A')}")
                logging.info(f"City:         {doc_data.get('city_name', 'N/A')}")
                logging.info(f"Country:      {doc_data.get('country_name', 'N/A')}")
            
            logging.info("─" * 70)
            logging.info("")
            
            # Collect for JSON output
            results.append({
                'document_id': doc.id,
                'title': doc_data.get('title'),
                'data': doc_data if show_full_doc else {
                    'subcategory': doc_data.get('subcategory'),
                    'city_name': doc_data.get('city_name'),
                    'country_name': doc_data.get('country_name')
                }
            })
        
        # Summary
        logging.info("=" * 70)
        logging.info("📊 SUMMARY")
        logging.info("=" * 70)
        logging.info(f"Total matches:  {len(docs)}")
        logging.info(f"Document IDs:   {', '.join([doc.id for doc in docs])}")
        logging.info("=" * 70)
        
        # Save to JSON if requested
        if output_json:
            if not output_file:
                safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in title)
                safe_title = safe_title.replace(' ', '_')[:50]
                output_file = f"search_results_{safe_title}.json"
            
            output_data = {
                'search_query': {
                    'collection': collection_name,
                    'title': title
                },
                'total_results': len(docs),
                'results': results
            }
            
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, indent=2, ensure_ascii=False)
                logging.info(f"💾 Results saved to: {output_file}")
            except Exception as e:
                logging.error(f"Error saving JSON file: {e}")
        
    except Exception as e:
        logging.error(f"❌ Error during search: {e}")
        raise

def search_partial_title(collection_name: str, 
                        search_term: str,
                        max_results: int = 10):
    """
    Search for documents with titles containing the search term.
    Note: This requires fetching all documents since Firestore doesn't support 
    native text search. Use only for small collections or testing.
    
    Args:
        collection_name: Name of the Firestore collection
        search_term: Term to search for in titles
        max_results: Maximum number of results to return
    """
    
    init_firebase_once()
    db = firestore.client()
    collection_ref = db.collection(collection_name)
    
    logging.info("=" * 70)
    logging.info("🔍 PARTIAL TITLE SEARCH (Case-insensitive)")
    logging.info("=" * 70)
    logging.info(f"Collection:    {collection_name}")
    logging.info(f"Search term:   '{search_term}'")
    logging.info(f"Max results:   {max_results}")
    logging.info("=" * 70)
    logging.warning("⚠️  This scans all documents - may use significant quota!")
    logging.info("")
    
    try:
        search_lower = search_term.lower()
        matches = []
        checked = 0
        
        # Stream all documents (this is inefficient for large collections)
        docs = collection_ref.stream()
        
        for doc in docs:
            checked += 1
            data = doc.to_dict()
            title = data.get('title', '')
            
            if title and search_lower in title.lower():
                matches.append({
                    'id': doc.id,
                    'title': title,
                    'data': data
                })
                
                if len(matches) >= max_results:
                    break
        
        logging.info(f"Checked {checked} documents")
        logging.info(f"Found {len(matches)} matches")
        logging.info("")
        
        if matches:
            for idx, match in enumerate(matches, 1):
                logging.info(f"{idx}. ID: {match['id']}")
                logging.info(f"   Title: {match['title']}")
                logging.info("")
        else:
            logging.warning(f"No documents found containing: '{search_term}'")
        
    except Exception as e:
        logging.error(f"❌ Error during partial search: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(
        description="Find Firestore documents by title field.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Exact title search
  python %(prog)s --title "Affordable Day Trips from New Delhi"
  
  # Search and save to JSON
  python %(prog)s --title "Affordable Day Trips from New Delhi" --json
  
  # Show full document data
  python %(prog)s --title "Affordable Day Trips from New Delhi" --full
  
  # Partial search (searches all docs - use carefully!)
  python %(prog)s --partial "Day Trips" --max-results 5
        """
    )
    
    parser.add_argument("--collection", default="playlistsNew",
                       help="Collection name. Default: playlistsNew")
    parser.add_argument("--title", 
                       help="Exact title to search for")
    parser.add_argument("--partial",
                       help="Partial title search (case-insensitive)")
    parser.add_argument("--json", action="store_true",
                       help="Save results to JSON file")
    parser.add_argument("--output", "-o",
                       help="Custom output JSON file path")
    parser.add_argument("--full", action="store_true",
                       help="Show full document data")
    parser.add_argument("--max-results", type=int, default=10,
                       help="Max results for partial search. Default: 10")
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.title and not args.partial:
        parser.error("Either --title or --partial must be provided")
    
    if args.title and args.partial:
        parser.error("Use either --title or --partial, not both")
    
    # Run search
    if args.title:
        find_by_title(
            collection_name=args.collection,
            title=args.title,
            output_json=args.json,
            output_file=args.output,
            show_full_doc=args.full
        )
    elif args.partial:
        search_partial_title(
            collection_name=args.collection,
            search_term=args.partial,
            max_results=args.max_results
        )

if __name__ == "__main__":
    main()