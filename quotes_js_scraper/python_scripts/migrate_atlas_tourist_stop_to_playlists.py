from google.cloud import firestore
from google.oauth2 import service_account
import traceback

# Configuration
EXPLORE_COLLECTION      = 'explore'
PLAYLISTS_COLLECTION    = 'playlistsNew'
SOURCE_TAG              = 'atlas'
CATEGORY                = 'Travel'
BATCH_SIZE              = 500  # Firestore batch limit

# ←– ADD THIS:
SERVICE_ACCOUNT_JSON    = 'C:\\dev\\python_runs\\scrapy_selenium\\quotes-js-project\\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json'


def get_starting_playlist_id(db_client):
    """
    Scans playlistsNew collection to find highest numeric ID and returns next in sequence.
    """
    print("🔎 Scanning 'playlistsNew' to find current max ID...")
    max_id = 0
    
    try:
        playlists_ref = db_client.collection(PLAYLISTS_COLLECTION)
        for doc in playlists_ref.select([]).stream():
            try:
                doc_id_int = int(doc.id)
                if doc_id_int > max_id:
                    max_id = doc_id_int
            except ValueError:
                continue
    except Exception as e:
        print(f"⚠️  Error scanning existing playlists: {e}")
        return 1
                
    starting_id = max_id + 1
    print(f"✅ Found max ID: {max_id}. New playlists will start from ID: {starting_id}")
    return starting_id


def create_place_data(source_doc):
    """
    Creates standardized place data from atlas document with ALL available fields.
    """
    source = source_doc.to_dict()
    return {
        # Core identifiers
        "placeId": source.get("placeId"),
        "name": source.get("name"),
        "id": source.get("id"),
        "index": source.get("index"),
        
        # Basic info
        "description": source.get("description"),
        "website": source.get("website"),
        "address": source.get("address"),
        "internationalPhoneNumber": source.get("internationalPhoneNumber"),
        
        # Location data
        "latitude": source.get("latitude"),
        "longitude": source.get("longitude"),
        # "country": source.get("country"),
        
        # Categories and ratings
        "categories": source.get("categories", []),
        "rating": source.get("rating"),
        "numRatings": source.get("numRatings"),
        "ratingDistribution": source.get("ratingDistribution"),
        "tripadvisorRating": source.get("tripadvisorRating"),
        "tripadvisorNumRatings": source.get("tripadvisorNumRatings"),
        
        # Business info
        "priceLevel": source.get("priceLevel"),
        "permanentlyClosed": source.get("permanentlyClosed"),
        "utcOffset": source.get("utcOffset"),
        "openingPeriods": source.get("openingPeriods", []),
        
        # Time estimates
        "minMinutesSpent": source.get("minMinutesSpent"),
        "maxMinutesSpent": source.get("maxMinutesSpent"),
        
        # Media and content
        "imageKeys": source.get("imageKeys", []),
        "g_image_urls": source.get("g_image_urls", []),
        "generalDescription": source.get("generalDescription" or "description"),
        "reviews": source.get("reviews", []),
        
        # Sources (url field maps to sources array)
        "sources": [source["url"]] if source.get("url") else [],
    }


def migrate_atlas_data():
    """
    Migrates atlas subcollections to playlistsNew with auto-incrementing IDs.
    """
    print("🚀 Starting atlas migration with improved batch processing...")

    # ←– MODIFY THIS SECTION TO USE SERVICE_ACCOUNT_JSON
    try:
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON)
        db = firestore.Client(credentials=creds, project=creds.project_id)
        print("✅ Firestore client initialized with service account")
    except Exception as e:
        print(f"❌ Error initializing Firestore: {e}")
        return
    
    # Get starting ID
    current_id = get_starting_playlist_id(db)
    
    # Initialize counters
    created_count = 0
    skipped_count = 0
    error_count = 0
    
    # Process cities
    try:
        explore_docs = list(db.collection(EXPLORE_COLLECTION).stream())
        print(f"🏙️  Found {len(explore_docs)} cities to process")
        
        batch = db.batch()
        batch_count = 0
        
        for city_doc in explore_docs:
            try:
                city_data = city_doc.to_dict()
                city_id = city_doc.id
                city_name = city_data.get("city_name", "Unknown City")
                
                # Get atlas places
                atlas_ref = city_doc.reference.collection(SOURCE_TAG)
                atlas_docs = list(atlas_ref.order_by('index').stream())
                
                if not atlas_docs:
                    print(f"⏭️  Skipping {city_name}: no atlas data")
                    skipped_count += 1
                    continue
                
                # Create places data
                places = []
                for atlas_doc in atlas_docs:
                    place_data = create_place_data(atlas_doc)
                    places.append(place_data)
                
                # Create playlist document
                playlist_id = str(current_id)
                playlist_data = {
                    "list_id": playlist_id,
                    "title": f"Hidden Gems in {city_name}",
                    "description": city_data.get('manual_description') or f"Discover unique attractions in {city_name}",
                    "imageUrl": city_data.get('atlasImageUrl', ''),
                    "source": SOURCE_TAG,
                    "category": CATEGORY,
                    "city_id": city_id,
                    "city": city_name,
                    "subcollections": {
                        "places": places
                    },
                }
                
                # Add to batch
                playlist_ref = db.collection(PLAYLISTS_COLLECTION).document(playlist_id)
                batch.set(playlist_ref, playlist_data)
                batch_count += 1
                
                print(f"✅ Queued: {city_name} -> ID {playlist_id} ({len(places)} places)")
                
                current_id += 1
                created_count += 1
                
                # Commit batch if approaching limit
                if batch_count >= BATCH_SIZE:
                    print(f"💾 Committing batch of {batch_count} documents...")
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0
                    
            except Exception as e:
                print(f"❌ Error processing city {city_doc.id}: {e}")
                error_count += 1
                continue
        
        # Commit remaining batch
        if batch_count > 0:
            print(f"💾 Committing final batch of {batch_count} documents...")
            batch.commit()
            
    except Exception as e:
        print(f"❌ Critical error during migration: {e}")
        traceback.print_exc()
        return
    
    # Summary
    print("\n" + "="*50)
    print("📊 MIGRATION SUMMARY")
    print("="*50)
    print(f"Cities processed: {len(explore_docs)}")
    print(f"Playlists created: {created_count}")
    print(f"Cities skipped: {skipped_count}")
    print(f"Errors encountered: {error_count}")
    print(f"Next available ID: {current_id}")
    print("🎉 Migration complete!")


if __name__ == "__main__":
    migrate_atlas_data()



# from google.cloud import firestore
# import traceback

# # Configuration
# EXPLORE_COLLECTION = 'explore'
# PLAYLISTS_COLLECTION = 'playlistsNew'
# SOURCE_TAG = 'atlas'
# CATEGORY = 'Travel'
# BATCH_SIZE = 500  # Firestore batch limit

# def get_starting_playlist_id(db_client):
#     """
#     Scans playlistsNew collection to find highest numeric ID and returns next in sequence.
#     """
#     print("🔎 Scanning 'playlistsNew' to find current max ID...")
#     max_id = 0
    
#     try:
#         playlists_ref = db_client.collection(PLAYLISTS_COLLECTION)
#         for doc in playlists_ref.select([]).stream():
#             try:
#                 doc_id_int = int(doc.id)
#                 if doc_id_int > max_id:
#                     max_id = doc_id_int
#             except ValueError:
#                 continue
#     except Exception as e:
#         print(f"⚠️  Error scanning existing playlists: {e}")
#         return 1
                
#     starting_id = max_id + 1
#     print(f"✅ Found max ID: {max_id}. New playlists will start from ID: {starting_id}")
#     return starting_id

# def create_place_data(source_doc):
#     """
#     Creates standardized place data from atlas document with ALL available fields.
#     """
#     source = source_doc.to_dict()
#     return {
#         # Core identifiers
#         # "_id": source.get("placeId") or source.get("_id"),
#         "placeId": source.get("placeId"),
#         "name": source.get("name"),
#         "id": source.get("id"),
#         "index": source.get("index"),
        
#         # Basic info
#         "description": source.get("description"),
#         "website": source.get("website"),
#         "address": source.get("address"),
#         "internationalPhoneNumber": source.get("internationalPhoneNumber"),
        
#         # Location data
#         "latitude": source.get("latitude"),
#         "longitude": source.get("longitude"),
#         # "city": source.get("city"),
#         # "state": source.get("state"),
#         "country": source.get("country"),
        
#         # Categories and ratings
#         "categories": source.get("categories", []),
#         "rating": source.get("rating"),
#         "numRatings": source.get("numRatings"),
#         "ratingDistribution": source.get("ratingDistribution"),
#         "tripadvisorRating": source.get("tripadvisorRating"),
#         "tripadvisorNumRatings": source.get("tripadvisorNumRatings"),
        
#         # Business info
#         "priceLevel": source.get("priceLevel"),
#         "permanentlyClosed": source.get("permanentlyClosed"),
#         "utcOffset": source.get("utcOffset"),
#         "openingPeriods": source.get("openingPeriods", []),
        
#         # Time estimates
#         "minMinutesSpent": source.get("minMinutesSpent"),
#         "maxMinutesSpent": source.get("maxMinutesSpent"),
        
#         # Media and content
#         "imageKeys": source.get("imageKeys", []),
#         "g_image_urls": source.get("g_image_urls", []),
#         "generalDescription": source.get("generalDescription" or "description"),
#         "reviews": source.get("reviews", []),
        
#         # Sources (url field maps to sources array)
#         "sources": [source["url"]] if source.get("url") else [],
#     }

# def migrate_atlas_data():
#     """
#     Migrates atlas subcollections to playlistsNew with auto-incrementing IDs.
#     """
#     print("🚀 Starting atlas migration with improved batch processing...")
    
#     try:
#         db = firestore.Client()
#         print("✅ Firestore client initialized")
#     except Exception as e:
#         print(f"❌ Error initializing Firestore: {e}")
#         return
    
#     # Get starting ID
#     current_id = get_starting_playlist_id(db)
    
#     # Initialize counters
#     created_count = 0
#     skipped_count = 0
#     error_count = 0
    
#     # Process cities
#     try:
#         explore_docs = list(db.collection(EXPLORE_COLLECTION).stream())
#         print(f"🏙️  Found {len(explore_docs)} cities to process")
        
#         batch = db.batch()
#         batch_count = 0
        
#         for city_doc in explore_docs:
#             try:
#                 city_data = city_doc.to_dict()
#                 city_id = city_doc.id
#                 city_name = city_data.get("city_name", "Unknown City")
                
#                 # Get atlas places
#                 atlas_ref = city_doc.reference.collection(SOURCE_TAG)
#                 atlas_docs = list(atlas_ref.order_by('index').stream())
                
#                 if not atlas_docs:
#                     print(f"⏭️  Skipping {city_name}: no atlas data")
#                     skipped_count += 1
#                     continue
                
#                 # Create places data
#                 places = []
#                 for atlas_doc in atlas_docs:
#                     place_data = create_place_data(atlas_doc)
#                     places.append(place_data)
                
#                 # Create playlist document
#                 playlist_id = str(current_id)
#                 playlist_data = {
#                     # "_id": playlist_id,
#                     "list_id": playlist_id,
#                     "title": f"Hidden Gems in {city_name}",
#                     "description": city_data.get('manual_description') or f"Discover unique attractions in {city_name}",
#                     "imageUrl": city_data.get('atlasImageUrl', ''),
#                     "source": SOURCE_TAG,
#                     "category": CATEGORY,
#                     "city_id": city_id,
#                     "city": city_name,
#                     "subcollections": {
#                         "places": places
#                     },
#                     # "featured": False,
#                     # "rank_manual": 100,
#                     # "lastUpdated": firestore.SERVER_TIMESTAMP
#                 }
                
#                 # Add to batch
#                 playlist_ref = db.collection(PLAYLISTS_COLLECTION).document(playlist_id)
#                 batch.set(playlist_ref, playlist_data)
#                 batch_count += 1
                
#                 print(f"✅ Queued: {city_name} -> ID {playlist_id} ({len(places)} places)")
                
#                 current_id += 1
#                 created_count += 1
                
#                 # Commit batch if approaching limit
#                 if batch_count >= BATCH_SIZE:
#                     print(f"💾 Committing batch of {batch_count} documents...")
#                     batch.commit()
#                     batch = db.batch()
#                     batch_count = 0
                    
#             except Exception as e:
#                 print(f"❌ Error processing city {city_doc.id}: {e}")
#                 error_count += 1
#                 continue
        
#         # Commit remaining batch
#         if batch_count > 0:
#             print(f"💾 Committing final batch of {batch_count} documents...")
#             batch.commit()
            
#     except Exception as e:
#         print(f"❌ Critical error during migration: {e}")
#         traceback.print_exc()
#         return
    
#     # Summary
#     print("\n" + "="*50)
#     print("📊 MIGRATION SUMMARY")
#     print("="*50)
#     print(f"Cities processed: {len(explore_docs)}")
#     print(f"Playlists created: {created_count}")
#     print(f"Cities skipped: {skipped_count}")
#     print(f"Errors encountered: {error_count}")
#     print(f"Next available ID: {current_id}")
#     print("🎉 Migration complete!")

# if __name__ == "__main__":
#     migrate_atlas_data()