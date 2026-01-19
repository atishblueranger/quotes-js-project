import firebase_admin
from firebase_admin import credentials, firestore
from collections import defaultdict
import time

# ───── CONFIG ─────────────────────────────────────────────────────────────────

SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

BATCH_SIZE = 500  # Process documents in batches to avoid timeout

# ───── PARALLEL PROCESSING SETTINGS ───────────────────────────────────────────
# Set START_DOC_ID and END_DOC_ID to scan a specific range
# This allows running multiple scripts in parallel for faster processing

START_DOC_ID ='9'  # Start from this doc ID (None = start from beginning)
END_DOC_ID ='10'    # Stop at this doc ID (None = scan until end)

# Examples for parallel execution:
# Script 1: START_DOC_ID = None,     END_DOC_ID = "30000"
# Script 2: START_DOC_ID = "30000",  END_DOC_ID = "60000"
# Script 3: START_DOC_ID = "60000",  END_DOC_ID = "90000"
# Script 4: START_DOC_ID = "90000",  END_DOC_ID = None

# Checkpoint file will be named based on range to avoid conflicts
def get_checkpoint_filename():
    start = START_DOC_ID or "start"
    end = END_DOC_ID or "end"
    return f"discovery_checkpoint_{start}_to_{end}.txt"

CHECKPOINT_FILE = get_checkpoint_filename()

# ───── INIT ───────────────────────────────────────────────────────────────────

cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
firebase_admin.initialize_app(cred)
db = firestore.client()

# ───── HELPER FUNCTIONS ───────────────────────────────────────────────────────

def save_checkpoint(last_doc_id, countries_found):
    """Save progress in case of interruption."""
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        f.write(f"LAST_DOC_ID:{last_doc_id}\n")
        for doc_id, info in countries_found.items():
            f.write(f"{doc_id}|{info['city_name']}|{info['attraction_count']}\n")

def load_checkpoint():
    """Load previous progress if available."""
    try:
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            last_doc_id = None
            countries_found = {}
            
            for line in lines:
                line = line.strip()
                if line.startswith("LAST_DOC_ID:"):
                    last_doc_id = line.split(":", 1)[1]
                elif "|" in line:
                    parts = line.split("|")
                    if len(parts) == 3:
                        doc_id, city_name, count = parts
                        countries_found[doc_id] = {
                            'city_name': city_name,
                            'place_id': '',
                            'attraction_count': int(count)
                        }
            
            return last_doc_id, countries_found
    except FileNotFoundError:
        return None, {}

# ───── DISCOVERY ──────────────────────────────────────────────────────────────

def discover_countries_with_attractions():
    """
    Scan allplaces collection in batches to find documents with top_attractions.
    Returns dict of {doc_id: city_name}
    """
    range_desc = f"from {START_DOC_ID or 'beginning'} to {END_DOC_ID or 'end'}"
    print(f"🔍 Scanning allplaces collection: {range_desc}\n")
    
    allplaces_ref = db.collection("allplaces")
    
    # Check for manual start point
    if START_DOC_ID:
        print(f"🎯 Starting from document ID: {START_DOC_ID}\n")
        
        # Fetch the specific document to use as starting point
        try:
            last_doc_snapshot = allplaces_ref.document(START_DOC_ID).get()
            if not last_doc_snapshot.exists:
                print(f"❌ Document ID '{START_DOC_ID}' not found!")
                return {}
        except Exception as e:
            print(f"❌ Error fetching document '{START_DOC_ID}': {e}")
            return {}
        
        countries_with_attractions = {}
        total_scanned = 0
        total_with_attractions = 0
    else:
        # Check for previous progress from checkpoint
        last_doc_id, countries_with_attractions = load_checkpoint()
        
        if last_doc_id:
            print(f"📂 Resuming from checkpoint: last document ID = {last_doc_id}")
            print(f"   Already found {len(countries_with_attractions)} countries\n")
            
            # Fetch the document to use as starting point
            try:
                last_doc_snapshot = allplaces_ref.document(last_doc_id).get()
                if not last_doc_snapshot.exists:
                    print(f"❌ Checkpoint document ID '{last_doc_id}' not found! Starting from beginning.")
                    last_doc_snapshot = None
            except Exception as e:
                print(f"⚠️  Error fetching checkpoint document: {e}")
                print(f"   Starting from beginning...")
                last_doc_snapshot = None
        else:
            last_doc_snapshot = None
        
        total_scanned = 0
        total_with_attractions = len(countries_with_attractions)
    
    batch_num = 0
    
    while True:
        batch_num += 1
        
        # Build query with pagination
        if last_doc_snapshot:
            query = allplaces_ref.order_by('__name__').start_after(last_doc_snapshot).limit(BATCH_SIZE)
        else:
            query = allplaces_ref.order_by('__name__').limit(BATCH_SIZE)
        
        # Get batch of documents
        try:
            docs = list(query.stream())
        except Exception as e:
            print(f"\n❌ Error fetching batch: {e}")
            print(f"   Saving checkpoint and retrying in 5 seconds...")
            if last_doc_snapshot:
                save_checkpoint(last_doc_snapshot.id, countries_with_attractions)
            time.sleep(5)
            continue
        
        # If no documents, we're done
        if not docs:
            print(f"\n✅ Reached end of collection")
            break
        
        # Check if we've reached the END_DOC_ID
        if END_DOC_ID:
            # Find if END_DOC_ID is in current batch or if we've passed it
            reached_end = False
            filtered_docs = []
            
            for doc in docs:
                if doc.id >= END_DOC_ID:
                    reached_end = True
                    break
                filtered_docs.append(doc)
            
            docs = filtered_docs
            
            if reached_end:
                print(f"\n🎯 Reached END_DOC_ID: {END_DOC_ID}")
                if docs:
                    print(f"   Processing remaining {len(docs)} documents in this batch...")
                else:
                    print(f"   No documents to process before end limit.")
                    break
        
        if not docs:
            break
        
        print(f"📦 Batch {batch_num}: Processing {len(docs)} documents (Total scanned: {total_scanned:,})")
        
        # Process each document in the batch
        for doc in docs:
            total_scanned += 1
            doc_id = doc.id
            data = doc.to_dict()
            
            # Get city_name from document
            city_name = (
                data.get('city_name') or 
                data.get('cityName') or 
                data.get('name') or 
                f"Unknown_{doc_id}"
            )
            
            # Check if this document has top_attractions subcollection
            try:
                attractions_ref = doc.reference.collection("top_attractions")
                attractions_sample = attractions_ref.limit(1).get()
                
                if len(attractions_sample) > 0:
                    # This document has attractions!
                    total_with_attractions += 1
                    
                    # Get the full count (with timeout protection)
                    try:
                        attractions_count = len(list(attractions_ref.stream()))
                    except:
                        # If count fails, just use 1 as placeholder
                        attractions_count = 1
                    
                    countries_with_attractions[doc_id] = {
                        'city_name': city_name,
                        'place_id': data.get('place_id', ''),
                        'attraction_count': attractions_count
                    }
                    
                    print(f"   ✅ Found: ID={doc_id}, City={city_name}, Attractions={attractions_count}")
            
            except Exception as e:
                print(f"   ⚠️  Error checking doc {doc_id}: {e}")
                continue
        
        # Update last document for pagination
        last_doc_snapshot = docs[-1]
        
        # Save checkpoint after each batch
        save_checkpoint(last_doc_snapshot.id, countries_with_attractions)
        
        # Progress update
        print(f"   Progress: {total_scanned:,} scanned, {total_with_attractions} with attractions")
        
        # Check if we've reached the end limit
        if END_DOC_ID and reached_end:
            print(f"\n✅ Completed range: {START_DOC_ID or 'start'} to {END_DOC_ID}")
            break
        
        # Small delay to avoid rate limiting
        time.sleep(0.5)
    
    print(f"\n{'='*70}")
    print(f"📊 Discovery Complete!")
    print(f"{'='*70}")
    print(f"Total documents scanned: {total_scanned:,}")
    print(f"Documents with top_attractions: {total_with_attractions}")
    print(f"{'='*70}\n")
    
    return countries_with_attractions

def generate_python_code(countries_dict):
    """Generate ready-to-use Python code for COUNTRY_NAMES."""
    
    print("📝 Generated COUNTRY_NAMES mapping:\n")
    print("="*70)
    print("# Copy and paste this into your content optimizer script:")
    print("="*70)
    print("\nCOUNTRY_NAMES = {")
    
    # Sort by doc_id for cleaner output
    sorted_items = sorted(countries_dict.items(), key=lambda x: x[0])
    
    for doc_id, info in sorted_items:
        city_name = info['city_name']
        attraction_count = info['attraction_count']
        print(f'    "{doc_id}": "{city_name}",  # {attraction_count} attractions')
    
    print("}")
    print("\n" + "="*70)
    
    # Also print summary stats
    print("\n📊 Summary by City:")
    print("="*70)
    
    # Group by city name (in case multiple IDs have same city)
    city_groups = defaultdict(list)
    for doc_id, info in countries_dict.items():
        city_groups[info['city_name']].append({
            'doc_id': doc_id,
            'count': info['attraction_count']
        })
    
    for city, entries in sorted(city_groups.items()):
        total_attractions = sum(e['count'] for e in entries)
        if len(entries) == 1:
            print(f"   {city}: {total_attractions} attractions (ID: {entries[0]['doc_id']})")
        else:
            print(f"   {city}: {total_attractions} attractions across {len(entries)} documents:")
            for entry in entries:
                print(f"      - ID {entry['doc_id']}: {entry['count']} attractions")
    
    print("="*70)

def save_to_file(countries_dict, filename=None):
    """Save the mapping to a Python file."""
    if filename is None:
        start = START_DOC_ID or "start"
        end = END_DOC_ID or "end"
        filename = f"country_mapping_{start}_to_{end}.py"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("# Auto-generated country mapping\n")
        f.write("# Generated by discover_countries.py\n")
        range_info = f"# Range: {START_DOC_ID or 'beginning'} to {END_DOC_ID or 'end'}\n\n"
        f.write(range_info)
        f.write("COUNTRY_NAMES = {\n")
        
        sorted_items = sorted(countries_dict.items(), key=lambda x: x[0])
        for doc_id, info in sorted_items:
            city_name = info['city_name']
            attraction_count = info['attraction_count']
            f.write(f'    "{doc_id}": "{city_name}",  # {attraction_count} attractions\n')
        
        f.write("}\n")
    
    print(f"\n💾 Mapping also saved to: {filename}")

# ───── MAIN ───────────────────────────────────────────────────────────────────

def main():
    print("🚀 Starting Discovery Process...")
    print("This will scan all documents in 'allplaces' collection")
    print(f"Batch size: {BATCH_SIZE} documents per batch")
    
    range_info = f"Range: {START_DOC_ID or 'beginning'} to {END_DOC_ID or 'end'}"
    print(f"📍 {range_info}")
    print(f"📂 Checkpoint file: {CHECKPOINT_FILE}")
    print()
    
    try:
        # Discover countries with attractions
        countries = discover_countries_with_attractions()
        
        if not countries:
            print("⚠️  No documents with top_attractions found!")
            return
        
        # Generate the Python code
        generate_python_code(countries)
        
        # Save to file
        save_to_file(countries)
        
        # Clean up checkpoint file
        import os
        try:
            os.remove(CHECKPOINT_FILE)
            print(f"🧹 Cleaned up checkpoint file")
        except:
            pass
        
        print("\n✅ Next steps:")
        if START_DOC_ID or END_DOC_ID:
            print(f"   This script scanned range: {START_DOC_ID or 'start'} to {END_DOC_ID or 'end'}")
            print("   If running multiple scripts in parallel:")
            print("   1. Wait for all parallel scripts to complete")
            print("   2. Merge all country_mapping_*.py files")
            print("   3. Paste combined COUNTRY_NAMES into your content optimizer")
        else:
            print("   1. Copy the COUNTRY_NAMES dictionary above")
            print("   2. Paste it into your content optimizer script")
            print("   3. Review the cities to ensure names are correct")
            print("   4. Run the content optimizer!")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        print(f"Progress has been saved to {CHECKPOINT_FILE}")
        print("Run the script again to resume from where you left off")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print(f"Progress has been saved to {CHECKPOINT_FILE}")

if __name__ == "__main__":
    main()

# import firebase_admin
# from firebase_admin import credentials, firestore
# from collections import defaultdict
# import time

# # ───── CONFIG ─────────────────────────────────────────────────────────────────

# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# BATCH_SIZE = 500  # Process documents in batches to avoid timeout
# CHECKPOINT_FILE = "discovery_checkpoint2.txt"  # Save progress

# # Manual resume point (set to None to use checkpoint file, or specify doc ID to start from)
# RESUME_FROM_DOC_ID = None  # Example: "131712" to start from that document
# RESUME_FROM_DOC_ID = "131712"  # Uncomment and set to skip to specific ID

# # ───── INIT ───────────────────────────────────────────────────────────────────

# cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
# firebase_admin.initialize_app(cred)
# db = firestore.client()

# # ───── HELPER FUNCTIONS ───────────────────────────────────────────────────────

# def save_checkpoint(last_doc_id, countries_found):
#     """Save progress in case of interruption."""
#     with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
#         f.write(f"LAST_DOC_ID:{last_doc_id}\n")
#         for doc_id, info in countries_found.items():
#             f.write(f"{doc_id}|{info['city_name']}|{info['attraction_count']}\n")

# def load_checkpoint():
#     """Load previous progress if available."""
#     try:
#         with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
#             lines = f.readlines()
#             last_doc_id = None
#             countries_found = {}
            
#             for line in lines:
#                 line = line.strip()
#                 if line.startswith("LAST_DOC_ID:"):
#                     last_doc_id = line.split(":", 1)[1]
#                 elif "|" in line:
#                     parts = line.split("|")
#                     if len(parts) == 3:
#                         doc_id, city_name, count = parts
#                         countries_found[doc_id] = {
#                             'city_name': city_name,
#                             'place_id': '',
#                             'attraction_count': int(count)
#                         }
            
#             return last_doc_id, countries_found
#     except FileNotFoundError:
#         return None, {}

# # ───── DISCOVERY ──────────────────────────────────────────────────────────────

# def discover_countries_with_attractions():
#     """
#     Scan allplaces collection in batches to find documents with top_attractions.
#     Returns dict of {doc_id: city_name}
#     """
#     print("🔍 Scanning allplaces collection with batching...\n")
    
#     allplaces_ref = db.collection("allplaces")
    
#     # Check for manual resume point first
#     if RESUME_FROM_DOC_ID:
#         print(f"🎯 Manual resume: Starting from document ID = {RESUME_FROM_DOC_ID}")
#         print(f"   (Ignoring checkpoint file)\n")
        
#         # Fetch the specific document to use as starting point
#         try:
#             last_doc_snapshot = allplaces_ref.document(RESUME_FROM_DOC_ID).get()
#             if not last_doc_snapshot.exists:
#                 print(f"❌ Document ID '{RESUME_FROM_DOC_ID}' not found!")
#                 return {}
#         except Exception as e:
#             print(f"❌ Error fetching document '{RESUME_FROM_DOC_ID}': {e}")
#             return {}
        
#         countries_with_attractions = {}
#         total_scanned = 0
#         total_with_attractions = 0
#     else:
#         # Check for previous progress from checkpoint
#         last_doc_id, countries_with_attractions = load_checkpoint()
        
#         if last_doc_id:
#             print(f"📂 Resuming from checkpoint: last document ID = {last_doc_id}")
#             print(f"   Already found {len(countries_with_attractions)} countries\n")
            
#             # Fetch the document to use as starting point
#             try:
#                 last_doc_snapshot = allplaces_ref.document(last_doc_id).get()
#                 if not last_doc_snapshot.exists:
#                     print(f"❌ Checkpoint document ID '{last_doc_id}' not found! Starting from beginning.")
#                     last_doc_snapshot = None
#             except Exception as e:
#                 print(f"⚠️  Error fetching checkpoint document: {e}")
#                 print(f"   Starting from beginning...")
#                 last_doc_snapshot = None
#         else:
#             last_doc_snapshot = None
        
#         total_scanned = 0
#         total_with_attractions = len(countries_with_attractions)
    
#     batch_num = 0
    
#     while True:
#         batch_num += 1
        
#         # Build query with pagination
#         if last_doc_snapshot:
#             query = allplaces_ref.order_by('__name__').start_after(last_doc_snapshot).limit(BATCH_SIZE)
#         else:
#             query = allplaces_ref.order_by('__name__').limit(BATCH_SIZE)
        
#         # Get batch of documents
#         try:
#             docs = list(query.stream())
#         except Exception as e:
#             print(f"\n❌ Error fetching batch: {e}")
#             print(f"   Saving checkpoint and retrying in 5 seconds...")
#             if last_doc_snapshot:
#                 save_checkpoint(last_doc_snapshot.id, countries_with_attractions)
#             time.sleep(5)
#             continue
        
#         # If no documents, we're done
#         if not docs:
#             print(f"\n✅ Reached end of collection")
#             break
        
#         print(f"📦 Batch {batch_num}: Processing {len(docs)} documents (Total scanned: {total_scanned:,})")
        
#         # Process each document in the batch
#         for doc in docs:
#             total_scanned += 1
#             doc_id = doc.id
#             data = doc.to_dict()
            
#             # Get city_name from document
#             city_name = (
#                 data.get('city_name') or 
#                 data.get('cityName') or 
#                 data.get('name') or 
#                 f"Unknown_{doc_id}"
#             )
            
#             # Check if this document has top_attractions subcollection
#             try:
#                 attractions_ref = doc.reference.collection("top_attractions")
#                 attractions_sample = attractions_ref.limit(1).get()
                
#                 if len(attractions_sample) > 0:
#                     # This document has attractions!
#                     total_with_attractions += 1
                    
#                     # Get the full count (with timeout protection)
#                     try:
#                         attractions_count = len(list(attractions_ref.stream()))
#                     except:
#                         # If count fails, just use 1 as placeholder
#                         attractions_count = 1
                    
#                     countries_with_attractions[doc_id] = {
#                         'city_name': city_name,
#                         'place_id': data.get('place_id', ''),
#                         'attraction_count': attractions_count
#                     }
                    
#                     print(f"   ✅ Found: ID={doc_id}, City={city_name}, Attractions={attractions_count}")
            
#             except Exception as e:
#                 print(f"   ⚠️  Error checking doc {doc_id}: {e}")
#                 continue
        
#         # Update last document for pagination
#         last_doc_snapshot = docs[-1]
        
#         # Save checkpoint after each batch
#         save_checkpoint(last_doc_snapshot.id, countries_with_attractions)
        
#         # Progress update
#         print(f"   Progress: {total_scanned:,} scanned, {total_with_attractions} with attractions")
        
#         # Small delay to avoid rate limiting
#         time.sleep(0.5)
    
#     print(f"\n{'='*70}")
#     print(f"📊 Discovery Complete!")
#     print(f"{'='*70}")
#     print(f"Total documents scanned: {total_scanned:,}")
#     print(f"Documents with top_attractions: {total_with_attractions}")
#     print(f"{'='*70}\n")
    
#     return countries_with_attractions

# def generate_python_code(countries_dict):
#     """Generate ready-to-use Python code for COUNTRY_NAMES."""
    
#     print("📝 Generated COUNTRY_NAMES mapping:\n")
#     print("="*70)
#     print("# Copy and paste this into your content optimizer script:")
#     print("="*70)
#     print("\nCOUNTRY_NAMES = {")
    
#     # Sort by doc_id for cleaner output
#     sorted_items = sorted(countries_dict.items(), key=lambda x: x[0])
    
#     for doc_id, info in sorted_items:
#         city_name = info['city_name']
#         attraction_count = info['attraction_count']
#         print(f'    "{doc_id}": "{city_name}",  # {attraction_count} attractions')
    
#     print("}")
#     print("\n" + "="*70)
    
#     # Also print summary stats
#     print("\n📊 Summary by City:")
#     print("="*70)
    
#     # Group by city name (in case multiple IDs have same city)
#     city_groups = defaultdict(list)
#     for doc_id, info in countries_dict.items():
#         city_groups[info['city_name']].append({
#             'doc_id': doc_id,
#             'count': info['attraction_count']
#         })
    
#     for city, entries in sorted(city_groups.items()):
#         total_attractions = sum(e['count'] for e in entries)
#         if len(entries) == 1:
#             print(f"   {city}: {total_attractions} attractions (ID: {entries[0]['doc_id']})")
#         else:
#             print(f"   {city}: {total_attractions} attractions across {len(entries)} documents:")
#             for entry in entries:
#                 print(f"      - ID {entry['doc_id']}: {entry['count']} attractions")
    
#     print("="*70)

# def save_to_file(countries_dict, filename="country_mapping.py"):
#     """Save the mapping to a Python file."""
#     with open(filename, 'w', encoding='utf-8') as f:
#         f.write("# Auto-generated country mapping\n")
#         f.write("# Generated by discover_countries.py\n\n")
#         f.write("COUNTRY_NAMES = {\n")
        
#         sorted_items = sorted(countries_dict.items(), key=lambda x: x[0])
#         for doc_id, info in sorted_items:
#             city_name = info['city_name']
#             attraction_count = info['attraction_count']
#             f.write(f'    "{doc_id}": "{city_name}",  # {attraction_count} attractions\n')
        
#         f.write("}\n")
    
#     print(f"\n💾 Mapping also saved to: {filename}")

# # ───── MAIN ───────────────────────────────────────────────────────────────────

# def main():
#     print("🚀 Starting Discovery Process...")
#     print("This will scan all documents in 'allplaces' collection")
#     print(f"Batch size: {BATCH_SIZE} documents per batch")
    
#     if RESUME_FROM_DOC_ID:
#         print(f"📍 Manual resume point: {RESUME_FROM_DOC_ID}")
#     else:
#         print(f"📂 Using checkpoint file: {CHECKPOINT_FILE}")
#     print()
    
#     try:
#         # Discover countries with attractions
#         countries = discover_countries_with_attractions()
        
#         if not countries:
#             print("⚠️  No documents with top_attractions found!")
#             return
        
#         # Generate the Python code
#         generate_python_code(countries)
        
#         # Save to file
#         save_to_file(countries)
        
#         # Clean up checkpoint file
#         import os
#         try:
#             os.remove(CHECKPOINT_FILE)
#             print(f"🧹 Cleaned up checkpoint file")
#         except:
#             pass
        
#         print("\n✅ Next steps:")
#         print("   1. Copy the COUNTRY_NAMES dictionary above")
#         print("   2. Paste it into your content optimizer script")
#         print("   3. Review the cities to ensure names are correct")
#         print("   4. Run the content optimizer!")
    
#     except KeyboardInterrupt:
#         print("\n\n⚠️  Interrupted by user")
#         print(f"Progress has been saved to {CHECKPOINT_FILE}")
#         print("Run the script again to resume from where you left off")
#     except Exception as e:
#         print(f"\n❌ Unexpected error: {e}")
#         print(f"Progress has been saved to {CHECKPOINT_FILE}")

# if __name__ == "__main__":
#     main()


# import firebase_admin
# from firebase_admin import credentials, firestore
# from collections import defaultdict
# import time

# # ───── CONFIG ─────────────────────────────────────────────────────────────────

# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# BATCH_SIZE = 500  # Process documents in batches to avoid timeout
# CHECKPOINT_FILE = "discovery_checkpoint.txt"  # Save progress

# # ───── INIT ───────────────────────────────────────────────────────────────────

# cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
# firebase_admin.initialize_app(cred)
# db = firestore.client()

# # ───── HELPER FUNCTIONS ───────────────────────────────────────────────────────

# def save_checkpoint(last_doc_id, countries_found):
#     """Save progress in case of interruption."""
#     with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
#         f.write(f"LAST_DOC_ID:{last_doc_id}\n")
#         for doc_id, info in countries_found.items():
#             f.write(f"{doc_id}|{info['city_name']}|{info['attraction_count']}\n")

# def load_checkpoint():
#     """Load previous progress if available."""
#     try:
#         with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
#             lines = f.readlines()
#             last_doc_id = None
#             countries_found = {}
            
#             for line in lines:
#                 line = line.strip()
#                 if line.startswith("LAST_DOC_ID:"):
#                     last_doc_id = line.split(":", 1)[1]
#                 elif "|" in line:
#                     parts = line.split("|")
#                     if len(parts) == 3:
#                         doc_id, city_name, count = parts
#                         countries_found[doc_id] = {
#                             'city_name': city_name,
#                             'place_id': '',
#                             'attraction_count': int(count)
#                         }
            
#             return last_doc_id, countries_found
#     except FileNotFoundError:
#         return None, {}

# # ───── DISCOVERY ──────────────────────────────────────────────────────────────

# def discover_countries_with_attractions():
#     """
#     Scan allplaces collection in batches to find documents with top_attractions.
#     Returns dict of {doc_id: city_name}
#     """
#     print("🔍 Scanning allplaces collection with batching...\n")
    
#     # Check for previous progress
#     last_doc_id, countries_with_attractions = load_checkpoint()
    
#     if last_doc_id:
#         print(f"📂 Resuming from checkpoint: last document ID = {last_doc_id}")
#         print(f"   Already found {len(countries_with_attractions)} countries\n")
    
#     allplaces_ref = db.collection("allplaces")
    
#     total_scanned = 0
#     total_with_attractions = len(countries_with_attractions)
#     last_doc_snapshot = None
#     batch_num = 0
    
#     while True:
#         batch_num += 1
        
#         # Build query with pagination
#         if last_doc_snapshot:
#             query = allplaces_ref.order_by('__name__').start_after(last_doc_snapshot).limit(BATCH_SIZE)
#         else:
#             query = allplaces_ref.order_by('__name__').limit(BATCH_SIZE)
        
#         # Get batch of documents
#         try:
#             docs = list(query.stream())
#         except Exception as e:
#             print(f"\n❌ Error fetching batch: {e}")
#             print(f"   Saving checkpoint and retrying in 5 seconds...")
#             if last_doc_snapshot:
#                 save_checkpoint(last_doc_snapshot.id, countries_with_attractions)
#             time.sleep(5)
#             continue
        
#         # If no documents, we're done
#         if not docs:
#             print(f"\n✅ Reached end of collection")
#             break
        
#         print(f"📦 Batch {batch_num}: Processing {len(docs)} documents (Total scanned: {total_scanned:,})")
        
#         # Process each document in the batch
#         for doc in docs:
#             total_scanned += 1
#             doc_id = doc.id
#             data = doc.to_dict()
            
#             # Get city_name from document
#             city_name = (
#                 data.get('city_name') or 
#                 data.get('cityName') or 
#                 data.get('name') or 
#                 f"Unknown_{doc_id}"
#             )
            
#             # Check if this document has top_attractions subcollection
#             try:
#                 attractions_ref = doc.reference.collection("top_attractions")
#                 attractions_sample = attractions_ref.limit(1).get()
                
#                 if len(attractions_sample) > 0:
#                     # This document has attractions!
#                     total_with_attractions += 1
                    
#                     # Get the full count (with timeout protection)
#                     try:
#                         attractions_count = len(list(attractions_ref.stream()))
#                     except:
#                         # If count fails, just use 1 as placeholder
#                         attractions_count = 1
                    
#                     countries_with_attractions[doc_id] = {
#                         'city_name': city_name,
#                         'place_id': data.get('place_id', ''),
#                         'attraction_count': attractions_count
#                     }
                    
#                     print(f"   ✅ Found: ID={doc_id}, City={city_name}, Attractions={attractions_count}")
            
#             except Exception as e:
#                 print(f"   ⚠️  Error checking doc {doc_id}: {e}")
#                 continue
        
#         # Update last document for pagination
#         last_doc_snapshot = docs[-1]
        
#         # Save checkpoint after each batch
#         save_checkpoint(last_doc_snapshot.id, countries_with_attractions)
        
#         # Progress update
#         print(f"   Progress: {total_scanned:,} scanned, {total_with_attractions} with attractions")
        
#         # Small delay to avoid rate limiting
#         time.sleep(0.5)
    
#     print(f"\n{'='*70}")
#     print(f"📊 Discovery Complete!")
#     print(f"{'='*70}")
#     print(f"Total documents scanned: {total_scanned:,}")
#     print(f"Documents with top_attractions: {total_with_attractions}")
#     print(f"{'='*70}\n")
    
#     return countries_with_attractions

# def generate_python_code(countries_dict):
#     """Generate ready-to-use Python code for COUNTRY_NAMES."""
    
#     print("📝 Generated COUNTRY_NAMES mapping:\n")
#     print("="*70)
#     print("# Copy and paste this into your content optimizer script:")
#     print("="*70)
#     print("\nCOUNTRY_NAMES = {")
    
#     # Sort by doc_id for cleaner output
#     sorted_items = sorted(countries_dict.items(), key=lambda x: x[0])
    
#     for doc_id, info in sorted_items:
#         city_name = info['city_name']
#         attraction_count = info['attraction_count']
#         print(f'    "{doc_id}": "{city_name}",  # {attraction_count} attractions')
    
#     print("}")
#     print("\n" + "="*70)
    
#     # Also print summary stats
#     print("\n📊 Summary by City:")
#     print("="*70)
    
#     # Group by city name (in case multiple IDs have same city)
#     city_groups = defaultdict(list)
#     for doc_id, info in countries_dict.items():
#         city_groups[info['city_name']].append({
#             'doc_id': doc_id,
#             'count': info['attraction_count']
#         })
    
#     for city, entries in sorted(city_groups.items()):
#         total_attractions = sum(e['count'] for e in entries)
#         if len(entries) == 1:
#             print(f"   {city}: {total_attractions} attractions (ID: {entries[0]['doc_id']})")
#         else:
#             print(f"   {city}: {total_attractions} attractions across {len(entries)} documents:")
#             for entry in entries:
#                 print(f"      - ID {entry['doc_id']}: {entry['count']} attractions")
    
#     print("="*70)

# def save_to_file(countries_dict, filename="country_mapping.py"):
#     """Save the mapping to a Python file."""
#     with open(filename, 'w', encoding='utf-8') as f:
#         f.write("# Auto-generated country mapping\n")
#         f.write("# Generated by discover_countries.py\n\n")
#         f.write("COUNTRY_NAMES = {\n")
        
#         sorted_items = sorted(countries_dict.items(), key=lambda x: x[0])
#         for doc_id, info in sorted_items:
#             city_name = info['city_name']
#             attraction_count = info['attraction_count']
#             f.write(f'    "{doc_id}": "{city_name}",  # {attraction_count} attractions\n')
        
#         f.write("}\n")
    
#     print(f"\n💾 Mapping also saved to: {filename}")

# # ───── MAIN ───────────────────────────────────────────────────────────────────

# def main():
#     print("🚀 Starting Discovery Process...")
#     print("This will scan all documents in 'allplaces' collection")
#     print(f"Batch size: {BATCH_SIZE} documents per batch\n")
    
#     try:
#         # Discover countries with attractions
#         countries = discover_countries_with_attractions()
        
#         if not countries:
#             print("⚠️  No documents with top_attractions found!")
#             return
        
#         # Generate the Python code
#         generate_python_code(countries)
        
#         # Save to file
#         save_to_file(countries)
        
#         # Clean up checkpoint file
#         import os
#         try:
#             os.remove(CHECKPOINT_FILE)
#             print(f"🧹 Cleaned up checkpoint file")
#         except:
#             pass
        
#         print("\n✅ Next steps:")
#         print("   1. Copy the COUNTRY_NAMES dictionary above")
#         print("   2. Paste it into your content optimizer script")
#         print("   3. Review the cities to ensure names are correct")
#         print("   4. Run the content optimizer!")
    
#     except KeyboardInterrupt:
#         print("\n\n⚠️  Interrupted by user")
#         print(f"Progress has been saved to {CHECKPOINT_FILE}")
#         print("Run the script again to resume from where you left off")
#     except Exception as e:
#         print(f"\n❌ Unexpected error: {e}")
#         print(f"Progress has been saved to {CHECKPOINT_FILE}")

# if __name__ == "__main__":
#     main()










# import firebase_admin
# from firebase_admin import credentials, firestore
# from collections import defaultdict

# # ───── CONFIG ─────────────────────────────────────────────────────────────────

# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# # ───── INIT ───────────────────────────────────────────────────────────────────

# cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
# firebase_admin.initialize_app(cred)
# db = firestore.client()

# # ───── DISCOVERY ──────────────────────────────────────────────────────────────

# def discover_countries_with_attractions():
#     """
#     Scan allplaces collection to find documents with top_attractions subcollection.
#     Returns dict of {doc_id: city_name}
#     """
#     print("🔍 Scanning allplaces collection (this may take a few minutes)...\n")
    
#     allplaces_ref = db.collection("allplaces")
    
#     # Get all documents in allplaces
#     all_docs = allplaces_ref.stream()
    
#     countries_with_attractions = {}
#     total_scanned = 0
#     total_with_attractions = 0
    
#     for doc in all_docs:
#         total_scanned += 1
        
#         # Show progress every 1000 docs
#         if total_scanned % 1000 == 0:
#             print(f"   Scanned {total_scanned:,} documents... Found {total_with_attractions} with attractions")
        
#         doc_id = doc.id
#         data = doc.to_dict()
        
#         # Get city_name from document
#         city_name = data.get('city_name', '') or data.get('cityName', '') or data.get('name', '')
        
#         # Check if this document has top_attractions subcollection
#         # We do this by trying to get at least one document from the subcollection
#         attractions_ref = doc.reference.collection("top_attractions")
#         attractions_sample = attractions_ref.limit(1).get()
        
#         if len(attractions_sample) > 0:
#             # This document has attractions!
#             total_with_attractions += 1
            
#             # Get the full count
#             attractions_count = len(list(attractions_ref.stream()))
            
#             countries_with_attractions[doc_id] = {
#                 'city_name': city_name,
#                 'place_id': data.get('place_id', ''),
#                 'attraction_count': attractions_count
#             }
            
#             print(f"   ✅ Found: ID={doc_id}, City={city_name}, Attractions={attractions_count}")
    
#     print(f"\n{'='*70}")
#     print(f"📊 Discovery Complete!")
#     print(f"{'='*70}")
#     print(f"Total documents scanned: {total_scanned:,}")
#     print(f"Documents with top_attractions: {total_with_attractions}")
#     print(f"{'='*70}\n")
    
#     return countries_with_attractions

# def generate_python_code(countries_dict):
#     """Generate ready-to-use Python code for COUNTRY_NAMES."""
    
#     print("📝 Generated COUNTRY_NAMES mapping:\n")
#     print("="*70)
#     print("# Copy and paste this into your content optimizer script:")
#     print("="*70)
#     print("\nCOUNTRY_NAMES = {")
    
#     # Sort by doc_id for cleaner output
#     sorted_items = sorted(countries_dict.items(), key=lambda x: x[0])
    
#     for doc_id, info in sorted_items:
#         city_name = info['city_name']
#         attraction_count = info['attraction_count']
#         print(f'    "{doc_id}": "{city_name}",  # {attraction_count} attractions')
    
#     print("}")
#     print("\n" + "="*70)
    
#     # Also print summary stats
#     print("\n📊 Summary by City:")
#     print("="*70)
    
#     # Group by city name (in case multiple IDs have same city)
#     city_groups = defaultdict(list)
#     for doc_id, info in countries_dict.items():
#         city_groups[info['city_name']].append({
#             'doc_id': doc_id,
#             'count': info['attraction_count']
#         })
    
#     for city, entries in sorted(city_groups.items()):
#         total_attractions = sum(e['count'] for e in entries)
#         if len(entries) == 1:
#             print(f"   {city}: {total_attractions} attractions (ID: {entries[0]['doc_id']})")
#         else:
#             print(f"   {city}: {total_attractions} attractions across {len(entries)} documents:")
#             for entry in entries:
#                 print(f"      - ID {entry['doc_id']}: {entry['count']} attractions")
    
#     print("="*70)

# def save_to_file(countries_dict, filename="country_mapping.py"):
#     """Save the mapping to a Python file."""
#     with open(filename, 'w', encoding='utf-8') as f:
#         f.write("# Auto-generated country mapping\n")
#         f.write("# Generated by discover_countries.py\n\n")
#         f.write("COUNTRY_NAMES = {\n")
        
#         sorted_items = sorted(countries_dict.items(), key=lambda x: x[0])
#         for doc_id, info in sorted_items:
#             city_name = info['city_name']
#             attraction_count = info['attraction_count']
#             f.write(f'    "{doc_id}": "{city_name}",  # {attraction_count} attractions\n')
        
#         f.write("}\n")
    
#     print(f"\n💾 Mapping also saved to: {filename}")

# # ───── MAIN ───────────────────────────────────────────────────────────────────

# def main():
#     print("🚀 Starting Discovery Process...")
#     print("This will scan all documents in 'allplaces' collection\n")
    
#     # Discover countries with attractions
#     countries = discover_countries_with_attractions()
    
#     if not countries:
#         print("⚠️  No documents with top_attractions found!")
#         return
    
#     # Generate the Python code
#     generate_python_code(countries)
    
#     # Save to file
#     save_to_file(countries)
    
#     print("\n✅ Next steps:")
#     print("   1. Copy the COUNTRY_NAMES dictionary above")
#     print("   2. Paste it into your content optimizer script")
#     print("   3. Review the cities to ensure names are correct")
#     print("   4. Run the content optimizer!")

# if __name__ == "__main__":
#     main()