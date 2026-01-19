# """
# Collect all document IDs from Firestore collection "playlistsNew"
# """

# import firebase_admin
# from firebase_admin import credentials, firestore
# from pathlib import Path
# import json

# # ---------------------- CONFIG ----------------------
# PROJECT_ID = "mycasavsc"
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# COLLECTION = "playlistsNew"
# OUTPUT_FILE = "playlist_ids.json"
# # ----------------------------------------------------


# def init_firebase():
#     """Initialize Firebase Admin SDK"""
#     if not firebase_admin._apps:
#         cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#         firebase_admin.initialize_app(cred, {
#             'projectId': PROJECT_ID,
#         })
#     return firestore.client()


# def collect_all_document_ids(collection_name: str) -> list:
#     """
#     Fetch all document IDs from a Firestore collection
    
#     Args:
#         collection_name: Name of the Firestore collection
        
#     Returns:
#         List of document IDs
#     """
#     db = init_firebase()
#     collection_ref = db.collection(collection_name)
    
#     # Stream all documents (only fetching IDs, not full data for efficiency)
#     docs = collection_ref.select([]).stream()
    
#     doc_ids = []
#     for doc in docs:
#         doc_ids.append(doc.id)
    
#     return doc_ids


# def main():
#     print(f"Fetching document IDs from collection: {COLLECTION}")
#     print("=" * 60)
    
#     try:
#         # Collect all document IDs
#         doc_ids = collect_all_document_ids(COLLECTION)
        
#         print(f"\nTotal documents found: {len(doc_ids)}")
#         print("\nDocument IDs:")
#         print("-" * 60)
        
#         # Print all IDs
#         for i, doc_id in enumerate(doc_ids, 1):
#             print(f"{i:4d}. {doc_id}")
        
#         # Save to JSON file
#         output_data = {
#             "collection": COLLECTION,
#             "total_count": len(doc_ids),
#             "document_ids": doc_ids
#         }
        
#         with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
#             json.dump(output_data, f, indent=2, ensure_ascii=False)
        
#         print("\n" + "=" * 60)
#         print(f"✓ Document IDs saved to: {OUTPUT_FILE}")
        
#     except Exception as e:
#         print(f"❌ Error: {e}")
#         import traceback
#         traceback.print_exc()


# if __name__ == "__main__":
#     main()


# """
# Validate city_id field in Firestore collection "playlistsNew"
# Check if city_id can be converted to integer (correct format)
# Identify documents with incorrect city_id values
# """

# import firebase_admin
# from firebase_admin import credentials, firestore
# import json
# from typing import Dict, List, Any

# # ---------------------- CONFIG ----------------------
# PROJECT_ID = "mycasavsc"
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# COLLECTION = "playlistsNew"
# OUTPUT_FILE = "incorrect_city_id_documents.json"
# # ----------------------------------------------------


# def init_firebase():
#     """Initialize Firebase Admin SDK"""
#     if not firebase_admin._apps:
#         cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#         firebase_admin.initialize_app(cred, {
#             'projectId': PROJECT_ID,
#         })
#     return firestore.client()


# def is_valid_city_id(city_id_value: Any) -> bool:
#     """
#     Check if city_id can be converted to integer
    
#     Args:
#         city_id_value: The value of city_id field
        
#     Returns:
#         True if valid (can be converted to int), False otherwise
#     """
#     if city_id_value is None:
#         return False
    
#     # If it's already an integer, it's valid
#     if isinstance(city_id_value, int):
#         return True
    
#     # If it's a string, try to convert to int
#     if isinstance(city_id_value, str):
#         try:
#             int(city_id_value)
#             return True
#         except (ValueError, TypeError):
#             return False
    
#     # Any other type is invalid
#     return False


# def validate_all_documents():
#     """
#     Check all documents in collection for city_id validity
    
#     Returns:
#         Tuple of (incorrect_docs, missing_city_id, correct_count, stats)
#     """
#     db = init_firebase()
#     collection_ref = db.collection(COLLECTION)
    
#     # Fetch all documents
#     docs = collection_ref.stream()
    
#     incorrect_docs = []
#     missing_city_id = []
#     correct_count = 0
#     total_count = 0
    
#     print("Validating documents...")
#     print("=" * 80)
    
#     for doc in docs:
#         total_count += 1
#         doc_id = doc.id
#         data = doc.to_dict()
        
#         # Check if city_id field exists
#         if 'city_id' not in data:
#             missing_city_id.append({
#                 'doc_id': doc_id,
#                 'reason': 'Missing city_id field',
#                 'city': data.get('city', 'N/A'),
#                 'slug': data.get('slug', 'N/A')
#             })
#             print(f"⚠️  {doc_id}: Missing city_id field")
#             continue
        
#         city_id_value = data.get('city_id')
        
#         # Validate city_id
#         if is_valid_city_id(city_id_value):
#             correct_count += 1
#             if total_count <= 5:  # Show first 5 correct ones as examples
#                 print(f"✓  {doc_id}: city_id = {city_id_value} (Valid)")
#         else:
#             incorrect_docs.append({
#                 'doc_id': doc_id,
#                 'city_id': city_id_value,
#                 'city_id_type': type(city_id_value).__name__,
#                 'city': data.get('city', 'N/A'),
#                 'slug': data.get('slug', 'N/A'),
#                 'reason': f'Cannot convert to integer: {repr(city_id_value)}'
#             })
#             print(f"❌ {doc_id}: city_id = {repr(city_id_value)} (Invalid - type: {type(city_id_value).__name__})")
    
#     stats = {
#         'total_documents': total_count,
#         'correct_city_id': correct_count,
#         'incorrect_city_id': len(incorrect_docs),
#         'missing_city_id': len(missing_city_id)
#     }
    
#     return incorrect_docs, missing_city_id, correct_count, stats


# def main():
#     print(f"Checking city_id field in collection: {COLLECTION}")
#     print("=" * 80)
#     print()
    
#     try:
#         # Validate all documents
#         incorrect_docs, missing_city_id, correct_count, stats = validate_all_documents()
        
#         print("\n" + "=" * 80)
#         print("VALIDATION SUMMARY")
#         print("=" * 80)
#         print(f"Total documents:           {stats['total_documents']}")
#         print(f"✓ Correct city_id:         {stats['correct_city_id']}")
#         print(f"❌ Incorrect city_id:       {stats['incorrect_city_id']}")
#         print(f"⚠️  Missing city_id field:  {stats['missing_city_id']}")
#         print("=" * 80)
        
#         # Prepare output data
#         output_data = {
#             'collection': COLLECTION,
#             'validation_summary': stats,
#             'documents_with_incorrect_city_id': incorrect_docs,
#             'documents_missing_city_id': missing_city_id
#         }
        
#         # Save to JSON file
#         with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
#             json.dump(output_data, f, indent=2, ensure_ascii=False)
        
#         print(f"\n✓ Results saved to: {OUTPUT_FILE}")
        
#         # Print list of incorrect document IDs
#         if incorrect_docs:
#             print("\n" + "=" * 80)
#             print("DOCUMENTS WITH INCORRECT city_id:")
#             print("=" * 80)
#             for i, doc_info in enumerate(incorrect_docs, 1):
#                 print(f"{i:3d}. {doc_info['doc_id']}")
#                 print(f"      city_id: {repr(doc_info['city_id'])} (type: {doc_info['city_id_type']})")
#                 print(f"      city: {doc_info['city']}")
#                 print(f"      reason: {doc_info['reason']}")
#                 print()
        
#         if missing_city_id:
#             print("\n" + "=" * 80)
#             print("DOCUMENTS MISSING city_id FIELD:")
#             print("=" * 80)
#             for i, doc_info in enumerate(missing_city_id, 1):
#                 print(f"{i:3d}. {doc_info['doc_id']}")
#                 print(f"      city: {doc_info['city']}")
#                 print()
        
#         # Print just the document IDs for easy copy-paste
#         if incorrect_docs or missing_city_id:
#             print("\n" + "=" * 80)
#             print("DOCUMENT IDs TO FIX (copy-paste ready):")
#             print("=" * 80)
#             all_problem_ids = [doc['doc_id'] for doc in incorrect_docs] + [doc['doc_id'] for doc in missing_city_id]
#             print(json.dumps(all_problem_ids, indent=2))
        
#     except Exception as e:
#         print(f"❌ Error: {e}")
#         import traceback
#         traceback.print_exc()


# if __name__ == "__main__":
#     main()

"""
Add country_name field to all documents in "playlistsNew" collection
by looking up city_id in "allplaces" collection
"""

import firebase_admin
from firebase_admin import credentials, firestore
import json
from typing import Dict, Optional, List

# ---------------------- CONFIG ----------------------
PROJECT_ID = "mycasavsc"
SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
COLLECTION_PLAYLISTS = "playlistsNew"
COLLECTION_PLACES = "allplaces"
OUTPUT_FILE = "country_name_update_report.json"
DRY_RUN = False  # Set to False to actually update Firestore
# ----------------------------------------------------


def init_firebase():
    """Initialize Firebase Admin SDK"""
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
        firebase_admin.initialize_app(cred, {
            'projectId': PROJECT_ID,
        })
    return firestore.client()


def build_city_country_mapping(db) -> Dict[str, str]:
    """
    Build a mapping of city_id -> country_name from allplaces collection
    
    Args:
        db: Firestore client
        
    Returns:
        Dictionary mapping city_id to country_name
    """
    print(f"Building city_id -> country_name mapping from '{COLLECTION_PLACES}' collection...")
    print("=" * 80)
    
    places_ref = db.collection(COLLECTION_PLACES)
    places_docs = places_ref.stream()
    
    mapping = {}
    total_places = 0
    places_with_country = 0
    
    for doc in places_docs:
        total_places += 1
        city_id = doc.id
        data = doc.to_dict()
        
        if 'country_name' in data:
            country_name = data.get('country_name')
            mapping[city_id] = country_name
            places_with_country += 1
            
            if total_places <= 10:  # Show first 10 as examples
                print(f"  {city_id} -> {country_name}")
    
    print(f"\nTotal places documents: {total_places}")
    print(f"Places with country_name: {places_with_country}")
    print(f"Mapping built with {len(mapping)} entries")
    print("=" * 80)
    print()
    
    return mapping


def update_playlists_with_country_name(db, city_country_mapping: Dict[str, str], dry_run: bool = True):
    """
    Update all playlist documents with country_name field
    
    Args:
        db: Firestore client
        city_country_mapping: Dictionary mapping city_id to country_name
        dry_run: If True, only simulate updates without writing to Firestore
        
    Returns:
        Dictionary with update statistics and details
    """
    playlists_ref = db.collection(COLLECTION_PLAYLISTS)
    playlists_docs = playlists_ref.stream()
    
    stats = {
        'total_playlists': 0,
        'updated': 0,
        'already_has_country': 0,
        'missing_city_id': 0,
        'city_id_not_found_in_places': 0,
        'invalid_city_id': 0,
        'errors': 0
    }
    
    updated_docs = []
    skipped_docs = []
    error_docs = []
    
    print(f"Processing playlists {'(DRY RUN - no changes will be made)' if dry_run else '(LIVE - updates will be written)'}...")
    print("=" * 80)
    
    for doc in playlists_docs:
        stats['total_playlists'] += 1
        doc_id = doc.id
        data = doc.to_dict()
        
        try:
            # Check if country_name already exists
            if 'country_name' in data and data.get('country_name'):
                stats['already_has_country'] += 1
                skipped_docs.append({
                    'doc_id': doc_id,
                    'reason': 'Already has country_name',
                    'country_name': data.get('country_name')
                })
                if stats['total_playlists'] <= 5:
                    print(f"⊗  {doc_id}: Already has country_name = '{data.get('country_name')}'")
                continue
            
            # Check if city_id exists
            if 'city_id' not in data:
                stats['missing_city_id'] += 1
                skipped_docs.append({
                    'doc_id': doc_id,
                    'reason': 'Missing city_id field'
                })
                print(f"⚠️  {doc_id}: Missing city_id field")
                continue
            
            city_id_value = data.get('city_id')
            
            # Validate and normalize city_id
            try:
                # Convert to string for lookup
                if isinstance(city_id_value, int):
                    city_id_str = str(city_id_value)
                elif isinstance(city_id_value, str):
                    # Verify it's a valid number
                    int(city_id_value)
                    city_id_str = city_id_value
                else:
                    raise ValueError(f"Invalid city_id type: {type(city_id_value)}")
            except (ValueError, TypeError):
                stats['invalid_city_id'] += 1
                skipped_docs.append({
                    'doc_id': doc_id,
                    'reason': 'Invalid city_id (cannot convert to int)',
                    'city_id': city_id_value
                })
                print(f"❌ {doc_id}: Invalid city_id = {repr(city_id_value)}")
                continue
            
            # Look up country_name in mapping
            if city_id_str not in city_country_mapping:
                stats['city_id_not_found_in_places'] += 1
                skipped_docs.append({
                    'doc_id': doc_id,
                    'reason': 'city_id not found in allplaces collection',
                    'city_id': city_id_str
                })
                print(f"⚠️  {doc_id}: city_id '{city_id_str}' not found in allplaces")
                continue
            
            country_name = city_country_mapping[city_id_str]
            
            # Update the document
            if not dry_run:
                playlists_ref.document(doc_id).update({
                    'country_name': country_name
                })
                print(f"✓  {doc_id}: Updated with country_name = '{country_name}' (city_id: {city_id_str})")
            else:
                print(f"✓  {doc_id}: Would update with country_name = '{country_name}' (city_id: {city_id_str})")
            
            stats['updated'] += 1
            updated_docs.append({
                'doc_id': doc_id,
                'city_id': city_id_str,
                'country_name': country_name,
                'city': data.get('city', 'N/A')
            })
            
        except Exception as e:
            stats['errors'] += 1
            error_docs.append({
                'doc_id': doc_id,
                'error': str(e)
            })
            print(f"❌ {doc_id}: Error - {e}")
    
    return {
        'stats': stats,
        'updated_docs': updated_docs,
        'skipped_docs': skipped_docs,
        'error_docs': error_docs
    }


def main():
    print(f"Adding country_name to playlists in collection: {COLLECTION_PLAYLISTS}")
    print(f"Looking up country_name from collection: {COLLECTION_PLACES}")
    print(f"Mode: {'DRY RUN (no changes)' if DRY_RUN else 'LIVE (will update Firestore)'}")
    print("=" * 80)
    print()
    
    try:
        db = init_firebase()
        
        # Step 1: Build city_id -> country_name mapping
        city_country_mapping = build_city_country_mapping(db)
        
        if not city_country_mapping:
            print("❌ No city-country mapping found. Exiting.")
            return
        
        # Step 2: Update playlists
        result = update_playlists_with_country_name(db, city_country_mapping, dry_run=DRY_RUN)
        
        # Print summary
        print("\n" + "=" * 80)
        print("UPDATE SUMMARY")
        print("=" * 80)
        print(f"Total playlists:                    {result['stats']['total_playlists']}")
        print(f"✓ Updated with country_name:        {result['stats']['updated']}")
        print(f"⊗ Already has country_name:         {result['stats']['already_has_country']}")
        print(f"⚠️  Missing city_id field:           {result['stats']['missing_city_id']}")
        print(f"⚠️  city_id not found in allplaces:  {result['stats']['city_id_not_found_in_places']}")
        print(f"❌ Invalid city_id:                  {result['stats']['invalid_city_id']}")
        print(f"❌ Errors:                           {result['stats']['errors']}")
        print("=" * 80)
        
        # Save detailed report to JSON
        output_data = {
            'mode': 'dry_run' if DRY_RUN else 'live',
            'collections': {
                'source': COLLECTION_PLACES,
                'target': COLLECTION_PLAYLISTS
            },
            'summary': result['stats'],
            'updated_documents': result['updated_docs'],
            'skipped_documents': result['skipped_docs'],
            'error_documents': result['error_docs']
        }
        
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Detailed report saved to: {OUTPUT_FILE}")
        
        # Show sample of updated documents
        if result['updated_docs']:
            print("\n" + "=" * 80)
            print("SAMPLE OF DOCUMENTS TO BE UPDATED (first 10):")
            print("=" * 80)
            for i, doc_info in enumerate(result['updated_docs'][:10], 1):
                print(f"{i:3d}. {doc_info['doc_id']}")
                print(f"      city_id: {doc_info['city_id']}")
                print(f"      country_name: {doc_info['country_name']}")
                print(f"      city: {doc_info['city']}")
                print()
        
        if DRY_RUN:
            print("\n" + "=" * 80)
            print("⚠️  DRY RUN MODE - No changes were made to Firestore")
            print("To actually update the database, set DRY_RUN = False in the script")
            print("=" * 80)
        else:
            print("\n" + "=" * 80)
            print("✓ LIVE MODE - Changes have been written to Firestore")
            print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Fatal Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()