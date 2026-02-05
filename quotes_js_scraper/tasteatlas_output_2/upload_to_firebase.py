#!/usr/bin/env python
"""
Firebase Upload Script for TasteAtlas "What to Eat" Data
WITH Image Download & Firebase Storage Upload

Storage Structure (using placeDocId for scalability):
    what_to_eat_food/{placeDocId}/{dish_id}/1.jpg, 2.jpg, ...

Firestore Structure:
    allplaces/{placeDocId}/what_to_eat/{dish_id}
    
    Fields include:
    - photo_urls: ["url1", "url2", ...]  (list for multiple images)

Usage:
    # Test with dry run first
    python upload_to_firebase.py --credentials service_account.json --dry-run
    
    # Test single country
    python upload_to_firebase.py --credentials service_account.json --country india --dry-run
    
    # Upload all countries (with images)
    python upload_to_firebase.py --credentials service_account.json
    
    # Skip image upload (faster, just Firestore)
    python upload_to_firebase.py --credentials service_account.json --skip-images

Prerequisites:
    pip install firebase-admin requests
"""

import json
import os
import glob
import argparse
import re
import time
import requests
from datetime import datetime

try:
    import firebase_admin
    from firebase_admin import credentials, firestore, storage
except ImportError:
    print("❌ firebase-admin not installed!")
    print("   Run: pip install firebase-admin")
    exit(1)


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

OUTPUT_DIR = "tasteatlas_output"          # Original scraped files
ENHANCED_DIR = "tasteatlas_enhanced"      # Enhanced with OpenAI (preferred)
MAPPING_FILE = "country_to_placeDocId.json"

# Firebase Storage
STORAGE_ROOT_FOLDER = "what_to_eat_food"  # Root folder in Firebase Storage
IMAGE_DOWNLOAD_TIMEOUT = 20               # seconds
IMAGE_RETRY_ATTEMPTS = 3
DELAY_BETWEEN_UPLOADS = 0.1               # seconds (rate limiting)


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def load_json(file_path):
    """Load JSON file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def normalize_country_name(name):
    """Normalize country name for matching"""
    return name.lower().replace('_', '-').replace(' ', '-')


def normalize_dish_id(name):
    """Create clean document ID from dish name"""
    doc_id = name.lower().strip()
    doc_id = re.sub(r'[^\w\s-]', '', doc_id)
    doc_id = re.sub(r'[\s-]+', '_', doc_id)
    return doc_id.strip('_')[:100]  # Firestore doc ID limit


def is_valid_image_url(url):
    """Check if URL is a valid image URL"""
    if not url:
        return False
    if url.endswith('random'):
        return False
    if 'tasteatlas.com' not in url and 'cdn.tasteatlas' not in url:
        return False
    return True


def download_image(url, timeout=IMAGE_DOWNLOAD_TIMEOUT):
    """Download image with retries"""
    backoff = 1
    
    for attempt in range(IMAGE_RETRY_ATTEMPTS):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Referer': 'https://www.tasteatlas.com/',
            }
            
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            
            # Verify it's actually image data
            content_type = resp.headers.get('Content-Type', '')
            if 'image' not in content_type and len(resp.content) < 1000:
                return None
            
            return resp.content
            
        except Exception as e:
            if attempt < IMAGE_RETRY_ATTEMPTS - 1:
                time.sleep(backoff)
                backoff *= 2
            else:
                return None
    
    return None


def get_existing_images_count(bucket, place_doc_id, dish_id):
    """Get count of existing images in the dish folder"""
    prefix = f"{STORAGE_ROOT_FOLDER}/{place_doc_id}/{dish_id}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    return len(blobs)


def upload_image_to_storage(bucket, image_data, place_doc_id, dish_id, image_number=1):
    """
    Upload image to Firebase Storage with organized folder structure.
    
    Structure: what_to_eat_food/{placeDocId}/{dish_id}/{image_number}.jpg
    Example:   what_to_eat_food/86661/roti/1.jpg
    """
    try:
        # Create blob path: what_to_eat_food/86661/roti/1.jpg
        blob_path = f"{STORAGE_ROOT_FOLDER}/{place_doc_id}/{dish_id}/{image_number}.jpg"
        
        blob = bucket.blob(blob_path)
        blob.upload_from_string(image_data, content_type="image/jpeg")
        blob.make_public()
        
        return blob.public_url
        
    except Exception as e:
        print(f"         ⚠️  Storage upload failed: {e}")
        return None


def clean_dish_data(dish):
    """Clean and validate dish data for Firebase"""
    
    # Fix broken image URLs (some end with 'random')
    image_url = dish.get('image_url', '') or ''
    if image_url.endswith('random') or 'random' in image_url:
        image_url = ''
    
    return {
        'id': dish.get('id', ''),
        'name': dish.get('name', ''),
        'local_name': dish.get('local_name') or None,
        'category': dish.get('category', 'DISH'),
        'rank': int(dish.get('rank', 0)),
        'short_description': dish.get('short_description', ''),
        'image_url': image_url,      # Original TasteAtlas URL (backup)
        'photo_urls': [],            # List of Firebase Storage URLs
        'most_iconic_place': dish.get('most_iconic_place') or None,
        'most_iconic_location': dish.get('most_iconic_location') or None,
        'ingredients': dish.get('ingredients', []) or [],
        'is_active': True,
    }


def get_country_files(output_dir, enhanced_dir=None):
    """Get all country JSON files, preferring enhanced versions"""
    pattern = os.path.join(output_dir, 'tasteatlas_*_dishes.json')
    files = glob.glob(pattern)
    
    country_data = {}
    for file_path in files:
        basename = os.path.basename(file_path)
        country = basename.replace('tasteatlas_', '').replace('_dishes.json', '')
        country = normalize_country_name(country)
        
        # Check if enhanced version exists
        actual_file = file_path
        if enhanced_dir:
            enhanced_file = os.path.join(enhanced_dir, basename)
            if os.path.exists(enhanced_file):
                actual_file = enhanced_file
        
        dishes = load_json(actual_file)
        country_data[country] = {
            'dishes': dishes,
            'source': 'enhanced' if actual_file != file_path else 'original'
        }
    
    return country_data


# ═══════════════════════════════════════════════════════════════════════════
# UPLOAD FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def upload_country(db, bucket, country_name, dishes, place_doc_id, 
                   skip_images=False, dry_run=False):
    """Upload dishes for a single country to Firebase"""
    
    stats = {
        'uploaded': 0,
        'skipped': 0,
        'images_uploaded': 0,
        'images_failed': 0,
    }
    
    # Reference to allplaces document (only if not dry run)
    place_ref = None
    batch = None
    batch_count = 0
    MAX_BATCH = 400
    
    if not dry_run:
        place_ref = db.collection('allplaces').document(str(place_doc_id))
        batch = db.batch()
    
    for dish in dishes:
        # Clean dish data
        clean_dish = clean_dish_data(dish)
        
        # Generate document ID
        dish_id = normalize_dish_id(clean_dish['name'])
        if not dish_id or not clean_dish['name']:
            stats['skipped'] += 1
            continue
        
        # Handle image upload
        original_image_url = clean_dish['image_url']
        photo_urls = []  # List to store all image URLs
        
        if not skip_images and is_valid_image_url(original_image_url):
            if not dry_run:
                # Download image
                image_data = download_image(original_image_url)
                
                if image_data:
                    # Check if there are existing images (for future use)
                    existing_count = 0
                    try:
                        existing_count = get_existing_images_count(bucket, place_doc_id, dish_id)
                    except:
                        pass
                    
                    # Upload new image with next number
                    image_number = existing_count + 1
                    
                    firebase_image_url = upload_image_to_storage(
                        bucket=bucket,
                        image_data=image_data,
                        place_doc_id=place_doc_id,
                        dish_id=dish_id,
                        image_number=image_number
                    )
                    
                    if firebase_image_url:
                        photo_urls.append(firebase_image_url)
                        stats['images_uploaded'] += 1
                    else:
                        stats['images_failed'] += 1
                else:
                    stats['images_failed'] += 1
                
                # Rate limiting
                time.sleep(DELAY_BETWEEN_UPLOADS)
        
        # Update dish with Firebase Storage URLs
        if photo_urls:
            clean_dish['photo_urls'] = photo_urls
        else:
            # Fallback: use original URL as single item in list
            if original_image_url:
                clean_dish['photo_urls'] = [original_image_url]
            else:
                clean_dish['photo_urls'] = []
        
        if dry_run:
            # Show what would be uploaded
            img_status = ""
            if not skip_images and is_valid_image_url(original_image_url):
                img_status = f" → {place_doc_id}/{dish_id}/1.jpg"
            print(f"      [{clean_dish['rank']:2d}] {dish_id}: {clean_dish['name'][:30]}{img_status}")
        else:
            # Add to Firestore batch
            if place_ref is not None and batch is not None:
                dish_ref = place_ref.collection('what_to_eat').document(dish_id)
                batch.set(dish_ref, clean_dish, merge=True)
                batch_count += 1
                
                # Commit batch when full
                if batch_count >= MAX_BATCH:
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0
        
        stats['uploaded'] += 1
    
    # Commit remaining items
    if not dry_run and batch is not None and batch_count > 0:
        batch.commit()
    
    # Update parent document metadata
    if not dry_run and stats['uploaded'] > 0 and place_ref is not None:
        try:
            place_ref.set({
                'has_what_to_eat': True,
                'what_to_eat_count': stats['uploaded'],
                'what_to_eat_updated_at': firestore.SERVER_TIMESTAMP,
            }, merge=True)
        except Exception as e:
            print(f"      ⚠️  Could not update metadata: {e}")
    
    return stats


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Upload What to Eat data to Firebase')
    parser.add_argument('--credentials', '-c', type=str, required=True,
                       help='Path to Firebase service account JSON')
    parser.add_argument('--bucket', '-b', type=str, default=None,
                       help='Firebase Storage bucket (default: projectId.appspot.com)')
    parser.add_argument('--output-dir', '-o', type=str, default=OUTPUT_DIR,
                       help='Directory with scraped JSON files')
    parser.add_argument('--enhanced-dir', '-e', type=str, default=ENHANCED_DIR,
                       help='Directory with enhanced JSON files')
    parser.add_argument('--mapping', '-m', type=str, default=MAPPING_FILE,
                       help='Country to placeDocId mapping file')
    parser.add_argument('--country', type=str, default=None,
                       help='Upload only this country (for testing)')
    parser.add_argument('--skip-images', action='store_true',
                       help='Skip image download/upload (faster)')
    parser.add_argument('--dry-run', '-d', action='store_true',
                       help='Preview without uploading')
    
    args = parser.parse_args()
    
    # Validate files
    if not os.path.exists(args.credentials):
        print(f"❌ Credentials not found: {args.credentials}")
        return 1
    
    if not os.path.exists(args.output_dir):
        print(f"❌ Output directory not found: {args.output_dir}")
        return 1
    
    if not os.path.exists(args.mapping):
        print(f"❌ Mapping file not found: {args.mapping}")
        return 1
    
    # Load data
    print(f"\n{'='*60}")
    print(f"🍽️  What to Eat - Firebase Upload")
    print(f"{'='*60}")
    
    mapping = load_json(args.mapping)
    
    # Check for enhanced directory
    enhanced_dir = args.enhanced_dir if os.path.exists(args.enhanced_dir) else None
    if enhanced_dir:
        print(f"📂 Enhanced files: {enhanced_dir}")
    
    country_data = get_country_files(args.output_dir, enhanced_dir)
    
    print(f"📂 Found {len(country_data)} country files")
    print(f"📍 Mapping has {len([v for v in mapping.values() if v])} placeDocIds")
    print(f"📷 Image upload: {'DISABLED' if args.skip_images else 'ENABLED'}")
    
    if not args.skip_images:
        print(f"\n📁 Storage Structure (using placeDocId):")
        print(f"   {STORAGE_ROOT_FOLDER}/")
        print(f"   └── {{placeDocId}}/")
        print(f"       └── {{dish_id}}/")
        print(f"           └── 1.jpg, 2.jpg, ...")
        print(f"\n   Example: {STORAGE_ROOT_FOLDER}/86661/roti/1.jpg")
    
    if args.dry_run:
        print(f"\n🔍 DRY RUN - no actual uploads")
    
    # Filter to single country if specified
    if args.country:
        country_key = normalize_country_name(args.country)
        if country_key not in country_data:
            print(f"❌ Country '{args.country}' not found in scraped data")
            return 1
        country_data = {country_key: country_data[country_key]}
    
    # Initialize Firebase
    bucket = None
    if not args.dry_run:
        print(f"\n🔥 Connecting to Firebase...")
        
        # Load credentials to get project ID
        with open(args.credentials, 'r') as f:
            cred_data = json.load(f)
        
        project_id = cred_data.get('project_id', '')
        storage_bucket = args.bucket or f"{project_id}.appspot.com"
        
        cred = credentials.Certificate(args.credentials)
        firebase_admin.initialize_app(cred, {
            'storageBucket': storage_bucket
        })
        
        db = firestore.client()
        bucket = storage.bucket()
        
        print(f"   Firestore: ✅")
        print(f"   Storage bucket: {storage_bucket}")
    else:
        db = None
    
    # Upload each country
    print(f"\n{'─'*60}")
    
    results = {
        'success': [],
        'no_mapping': [],
        'failed': [],
        'total_dishes': 0,
        'total_images': 0,
        'failed_images': 0,
    }
    
    for country, data in sorted(country_data.items()):
        dishes = data['dishes']
        source = data['source']
        
        # Get placeDocId from mapping
        place_doc_id = mapping.get(country)
        if not place_doc_id:
            alt_key = country.replace('-', '_')
            place_doc_id = mapping.get(alt_key)
        
        if not place_doc_id:
            print(f"\n⏭️  {country.upper()}: No placeDocId - SKIPPED")
            results['no_mapping'].append(country)
            continue
        
        print(f"\n📍 {country.upper()} (placeDocId: {place_doc_id})")
        print(f"   Firestore: allplaces/{place_doc_id}/what_to_eat/")
        print(f"   Storage:   {STORAGE_ROOT_FOLDER}/{place_doc_id}/")
        print(f"   Source: {source} | Dishes: {len(dishes)}")
        
        try:
            stats = upload_country(
                db=db,
                bucket=bucket,
                country_name=country,
                dishes=dishes,
                place_doc_id=place_doc_id,
                skip_images=args.skip_images,
                dry_run=args.dry_run
            )
            
            results['success'].append({
                'country': country,
                'place_doc_id': place_doc_id,
                **stats
            })
            results['total_dishes'] += stats['uploaded']
            results['total_images'] += stats['images_uploaded']
            results['failed_images'] += stats['images_failed']
            
            status = "Would upload" if args.dry_run else "Uploaded"
            print(f"   ✅ {status}: {stats['uploaded']} dishes")
            if not args.skip_images:
                print(f"   📷 Images: {stats['images_uploaded']} uploaded, {stats['images_failed']} failed")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            results['failed'].append({'country': country, 'error': str(e)})
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📊 UPLOAD SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Countries successful: {len(results['success'])}")
    print(f"⏭️  Countries skipped (no mapping): {len(results['no_mapping'])}")
    print(f"❌ Countries failed: {len(results['failed'])}")
    print(f"🍽️  Total dishes uploaded: {results['total_dishes']}")
    
    if not args.skip_images:
        print(f"📷 Images uploaded: {results['total_images']}")
        print(f"📷 Images failed: {results['failed_images']}")
        print(f"\n📁 Storage Structure Created:")
        print(f"   {STORAGE_ROOT_FOLDER}/")
        print(f"   ├── 86661/              # India")
        print(f"   │   ├── roti/")
        print(f"   │   │   └── 1.jpg")
        print(f"   │   ├── naan/")
        print(f"   │   │   └── 1.jpg")
        print(f"   │   └── ...")
        print(f"   ├── 86647/              # Japan")
        print(f"   │   └── ...")
        print(f"   └── ...")
    
    if results['no_mapping']:
        print(f"\n⚠️  Countries without placeDocId mapping:")
        for c in results['no_mapping']:
            print(f"   - {c}")
    
    if args.dry_run:
        print(f"\n🔍 This was a DRY RUN")
        print(f"   Remove --dry-run to upload for real")
    
    print(f"{'='*60}")
    
    return 0


if __name__ == '__main__':
    exit(main())


# #!/usr/bin/env python
# """
# Firebase Upload Script for TasteAtlas "What to Eat" Data
# WITH Image Download & Firebase Storage Upload

# Storage Structure:
#     what_to_eat_food/{country}/{dish_id}/1.jpg, 2.jpg, ...

# Firestore Structure:
#     allplaces/{placeDocId}/what_to_eat/{dish_id}
    
#     Fields include:
#     - photo_urls: ["url1", "url2", ...]  (list for multiple images)

# Usage:
#     # Test with dry run first
#     python upload_to_firebase.py --credentials service_account.json --dry-run
    
#     # Test single country
#     python upload_to_firebase.py --credentials service_account.json --country india --dry-run
    
#     # Upload all countries (with images)
#     python upload_to_firebase.py --credentials service_account.json
    
#     # Skip image upload (faster, just Firestore)
#     python upload_to_firebase.py --credentials service_account.json --skip-images

# Prerequisites:
#     pip install firebase-admin requests
# """

# import json
# import os
# import glob
# import argparse
# import re
# import time
# import requests
# from datetime import datetime

# try:
#     import firebase_admin
#     from firebase_admin import credentials, firestore, storage
# except ImportError:
#     print("❌ firebase-admin not installed!")
#     print("   Run: pip install firebase-admin")
#     exit(1)


# # ═══════════════════════════════════════════════════════════════════════════
# # CONFIGURATION
# # ═══════════════════════════════════════════════════════════════════════════

# OUTPUT_DIR = "tasteatlas_output"          # Original scraped files
# ENHANCED_DIR = "tasteatlas_enhanced"      # Enhanced with OpenAI (preferred)
# MAPPING_FILE = "country_to_placeDocId.json"

# # Firebase Storage
# STORAGE_ROOT_FOLDER = "what_to_eat_food"  # Root folder in Firebase Storage
# IMAGE_DOWNLOAD_TIMEOUT = 20               # seconds
# IMAGE_RETRY_ATTEMPTS = 3
# DELAY_BETWEEN_UPLOADS = 0.1               # seconds (rate limiting)


# # ═══════════════════════════════════════════════════════════════════════════
# # HELPERS
# # ═══════════════════════════════════════════════════════════════════════════

# def load_json(file_path):
#     """Load JSON file"""
#     with open(file_path, 'r', encoding='utf-8') as f:
#         return json.load(f)


# def normalize_country_name(name):
#     """Normalize country name for matching"""
#     return name.lower().replace('_', '-').replace(' ', '-')


# def normalize_for_folder(name):
#     """Normalize name for folder path (use underscores)"""
#     folder = name.lower().strip()
#     folder = re.sub(r'[^\w\s-]', '', folder)
#     folder = re.sub(r'[\s-]+', '_', folder)
#     return folder.strip('_')


# def normalize_dish_id(name):
#     """Create clean document ID from dish name"""
#     doc_id = name.lower().strip()
#     doc_id = re.sub(r'[^\w\s-]', '', doc_id)
#     doc_id = re.sub(r'[\s-]+', '_', doc_id)
#     return doc_id.strip('_')[:100]  # Firestore doc ID limit


# def is_valid_image_url(url):
#     """Check if URL is a valid image URL"""
#     if not url:
#         return False
#     if url.endswith('random'):
#         return False
#     if 'tasteatlas.com' not in url and 'cdn.tasteatlas' not in url:
#         return False
#     return True


# def download_image(url, timeout=IMAGE_DOWNLOAD_TIMEOUT):
#     """Download image with retries"""
#     backoff = 1
    
#     for attempt in range(IMAGE_RETRY_ATTEMPTS):
#         try:
#             headers = {
#                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
#                 'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
#                 'Referer': 'https://www.tasteatlas.com/',
#             }
            
#             resp = requests.get(url, headers=headers, timeout=timeout)
#             resp.raise_for_status()
            
#             # Verify it's actually image data
#             content_type = resp.headers.get('Content-Type', '')
#             if 'image' not in content_type and len(resp.content) < 1000:
#                 return None
            
#             return resp.content
            
#         except Exception as e:
#             if attempt < IMAGE_RETRY_ATTEMPTS - 1:
#                 time.sleep(backoff)
#                 backoff *= 2
#             else:
#                 return None
    
#     return None


# def get_existing_images_count(bucket, country_folder, dish_id):
#     """Get count of existing images in the dish folder"""
#     prefix = f"{STORAGE_ROOT_FOLDER}/{country_folder}/{dish_id}/"
#     blobs = list(bucket.list_blobs(prefix=prefix))
#     return len(blobs)


# def upload_image_to_storage(bucket, image_data, country_folder, dish_id, image_number=1):
#     """
#     Upload image to Firebase Storage with organized folder structure.
    
#     Structure: what_to_eat_food/{country}/{dish_id}/{image_number}.jpg
#     Example:   what_to_eat_food/india/roti/1.jpg
#     """
#     try:
#         # Create blob path: what_to_eat_food/india/roti/1.jpg
#         blob_path = f"{STORAGE_ROOT_FOLDER}/{country_folder}/{dish_id}/{image_number}.jpg"
        
#         blob = bucket.blob(blob_path)
#         blob.upload_from_string(image_data, content_type="image/jpeg")
#         blob.make_public()
        
#         return blob.public_url
        
#     except Exception as e:
#         print(f"         ⚠️  Storage upload failed: {e}")
#         return None


# def clean_dish_data(dish):
#     """Clean and validate dish data for Firebase"""
    
#     # Fix broken image URLs (some end with 'random')
#     image_url = dish.get('image_url', '') or ''
#     if image_url.endswith('random') or 'random' in image_url:
#         image_url = ''
    
#     return {
#         'id': dish.get('id', ''),
#         'name': dish.get('name', ''),
#         'local_name': dish.get('local_name') or None,
#         'category': dish.get('category', 'DISH'),
#         'rank': int(dish.get('rank', 0)),
#         'short_description': dish.get('short_description', ''),
#         'image_url': image_url,      # Original TasteAtlas URL (backup)
#         'photo_urls': [],            # List of Firebase Storage URLs
#         'most_iconic_place': dish.get('most_iconic_place') or None,
#         'most_iconic_location': dish.get('most_iconic_location') or None,
#         'ingredients': dish.get('ingredients', []) or [],
#         'is_active': True,
#     }


# def get_country_files(output_dir, enhanced_dir=None):
#     """Get all country JSON files, preferring enhanced versions"""
#     pattern = os.path.join(output_dir, 'tasteatlas_*_dishes.json')
#     files = glob.glob(pattern)
    
#     country_data = {}
#     for file_path in files:
#         basename = os.path.basename(file_path)
#         country = basename.replace('tasteatlas_', '').replace('_dishes.json', '')
#         country = normalize_country_name(country)
        
#         # Check if enhanced version exists
#         actual_file = file_path
#         if enhanced_dir:
#             enhanced_file = os.path.join(enhanced_dir, basename)
#             if os.path.exists(enhanced_file):
#                 actual_file = enhanced_file
        
#         dishes = load_json(actual_file)
#         country_data[country] = {
#             'dishes': dishes,
#             'source': 'enhanced' if actual_file != file_path else 'original'
#         }
    
#     return country_data


# # ═══════════════════════════════════════════════════════════════════════════
# # UPLOAD FUNCTIONS
# # ═══════════════════════════════════════════════════════════════════════════

# def upload_country(db, bucket, country_name, dishes, place_doc_id, 
#                    skip_images=False, dry_run=False):
#     """Upload dishes for a single country to Firebase"""
    
#     stats = {
#         'uploaded': 0,
#         'skipped': 0,
#         'images_uploaded': 0,
#         'images_failed': 0,
#     }
    
#     # Country folder name for storage
#     country_folder = normalize_for_folder(country_name)
    
#     # Reference to allplaces document (only if not dry run)
#     place_ref = None
#     batch = None
#     batch_count = 0
#     MAX_BATCH = 400
    
#     if not dry_run:
#         place_ref = db.collection('allplaces').document(str(place_doc_id))
#         batch = db.batch()
    
#     for dish in dishes:
#         # Clean dish data
#         clean_dish = clean_dish_data(dish)
        
#         # Generate document ID
#         dish_id = normalize_dish_id(clean_dish['name'])
#         if not dish_id or not clean_dish['name']:
#             stats['skipped'] += 1
#             continue
        
#         # Handle image upload
#         original_image_url = clean_dish['image_url']
#         photo_urls = []  # List to store all image URLs
        
#         if not skip_images and is_valid_image_url(original_image_url):
#             if not dry_run:
#                 # Download image
#                 image_data = download_image(original_image_url)
                
#                 if image_data:
#                     # Check if there are existing images (for future use)
#                     existing_count = 0
#                     try:
#                         existing_count = get_existing_images_count(bucket, country_folder, dish_id)
#                     except:
#                         pass
                    
#                     # Upload new image with next number
#                     image_number = existing_count + 1
                    
#                     firebase_image_url = upload_image_to_storage(
#                         bucket=bucket,
#                         image_data=image_data,
#                         country_folder=country_folder,
#                         dish_id=dish_id,
#                         image_number=image_number
#                     )
                    
#                     if firebase_image_url:
#                         photo_urls.append(firebase_image_url)
#                         stats['images_uploaded'] += 1
#                     else:
#                         stats['images_failed'] += 1
#                 else:
#                     stats['images_failed'] += 1
                
#                 # Rate limiting
#                 time.sleep(DELAY_BETWEEN_UPLOADS)
        
#         # Update dish with Firebase Storage URLs
#         if photo_urls:
#             clean_dish['photo_urls'] = photo_urls
#         else:
#             # Fallback: use original URL as single item in list
#             if original_image_url:
#                 clean_dish['photo_urls'] = [original_image_url]
#             else:
#                 clean_dish['photo_urls'] = []
        
#         if dry_run:
#             # Show what would be uploaded
#             img_status = ""
#             if not skip_images and is_valid_image_url(original_image_url):
#                 img_status = f" → {country_folder}/{dish_id}/1.jpg"
#             print(f"      [{clean_dish['rank']:2d}] {dish_id}: {clean_dish['name'][:30]}{img_status}")
#         else:
#             # Add to Firestore batch
#             if place_ref is not None and batch is not None:
#                 dish_ref = place_ref.collection('what_to_eat').document(dish_id)
#                 batch.set(dish_ref, clean_dish, merge=True)
#                 batch_count += 1
                
#                 # Commit batch when full
#                 if batch_count >= MAX_BATCH:
#                     batch.commit()
#                     batch = db.batch()
#                     batch_count = 0
        
#         stats['uploaded'] += 1
    
#     # Commit remaining items
#     if not dry_run and batch is not None and batch_count > 0:
#         batch.commit()
    
#     # Update parent document metadata
#     if not dry_run and stats['uploaded'] > 0 and place_ref is not None:
#         try:
#             place_ref.set({
#                 'has_what_to_eat': True,
#                 'what_to_eat_count': stats['uploaded'],
#                 'what_to_eat_updated_at': firestore.SERVER_TIMESTAMP,
#             }, merge=True)
#         except Exception as e:
#             print(f"      ⚠️  Could not update metadata: {e}")
    
#     return stats


# # ═══════════════════════════════════════════════════════════════════════════
# # MAIN
# # ═══════════════════════════════════════════════════════════════════════════

# def main():
#     parser = argparse.ArgumentParser(description='Upload What to Eat data to Firebase')
#     parser.add_argument('--credentials', '-c', type=str, required=True,
#                        help='Path to Firebase service account JSON')
#     parser.add_argument('--bucket', '-b', type=str, default=None,
#                        help='Firebase Storage bucket (default: projectId.appspot.com)')
#     parser.add_argument('--output-dir', '-o', type=str, default=OUTPUT_DIR,
#                        help='Directory with scraped JSON files')
#     parser.add_argument('--enhanced-dir', '-e', type=str, default=ENHANCED_DIR,
#                        help='Directory with enhanced JSON files')
#     parser.add_argument('--mapping', '-m', type=str, default=MAPPING_FILE,
#                        help='Country to placeDocId mapping file')
#     parser.add_argument('--country', type=str, default=None,
#                        help='Upload only this country (for testing)')
#     parser.add_argument('--skip-images', action='store_true',
#                        help='Skip image download/upload (faster)')
#     parser.add_argument('--dry-run', '-d', action='store_true',
#                        help='Preview without uploading')
    
#     args = parser.parse_args()
    
#     # Validate files
#     if not os.path.exists(args.credentials):
#         print(f"❌ Credentials not found: {args.credentials}")
#         return 1
    
#     if not os.path.exists(args.output_dir):
#         print(f"❌ Output directory not found: {args.output_dir}")
#         return 1
    
#     if not os.path.exists(args.mapping):
#         print(f"❌ Mapping file not found: {args.mapping}")
#         return 1
    
#     # Load data
#     print(f"\n{'='*60}")
#     print(f"🍽️  What to Eat - Firebase Upload")
#     print(f"{'='*60}")
    
#     mapping = load_json(args.mapping)
    
#     # Check for enhanced directory
#     enhanced_dir = args.enhanced_dir if os.path.exists(args.enhanced_dir) else None
#     if enhanced_dir:
#         print(f"📂 Enhanced files: {enhanced_dir}")
    
#     country_data = get_country_files(args.output_dir, enhanced_dir)
    
#     print(f"📂 Found {len(country_data)} country files")
#     print(f"📍 Mapping has {len([v for v in mapping.values() if v])} placeDocIds")
#     print(f"📷 Image upload: {'DISABLED' if args.skip_images else 'ENABLED'}")
    
#     if not args.skip_images:
#         print(f"\n📁 Storage Structure:")
#         print(f"   {STORAGE_ROOT_FOLDER}/")
#         print(f"   └── {{country}}/")
#         print(f"       └── {{dish_id}}/")
#         print(f"           └── 1.jpg, 2.jpg, ...")
    
#     if args.dry_run:
#         print(f"\n🔍 DRY RUN - no actual uploads")
    
#     # Filter to single country if specified
#     if args.country:
#         country_key = normalize_country_name(args.country)
#         if country_key not in country_data:
#             print(f"❌ Country '{args.country}' not found in scraped data")
#             return 1
#         country_data = {country_key: country_data[country_key]}
    
#     # Initialize Firebase
#     bucket = None
#     if not args.dry_run:
#         print(f"\n🔥 Connecting to Firebase...")
        
#         # Load credentials to get project ID
#         with open(args.credentials, 'r') as f:
#             cred_data = json.load(f)
        
#         project_id = cred_data.get('project_id', '')
#         storage_bucket = args.bucket or f"{project_id}.appspot.com"
        
#         cred = credentials.Certificate(args.credentials)
#         firebase_admin.initialize_app(cred, {
#             'storageBucket': storage_bucket
#         })
        
#         db = firestore.client()
#         bucket = storage.bucket()
        
#         print(f"   Firestore: ✅")
#         print(f"   Storage bucket: {storage_bucket}")
#     else:
#         db = None
    
#     # Upload each country
#     print(f"\n{'─'*60}")
    
#     results = {
#         'success': [],
#         'no_mapping': [],
#         'failed': [],
#         'total_dishes': 0,
#         'total_images': 0,
#         'failed_images': 0,
#     }
    
#     for country, data in sorted(country_data.items()):
#         dishes = data['dishes']
#         source = data['source']
        
#         # Get placeDocId from mapping
#         place_doc_id = mapping.get(country)
#         if not place_doc_id:
#             alt_key = country.replace('-', '_')
#             place_doc_id = mapping.get(alt_key)
        
#         if not place_doc_id:
#             print(f"\n⏭️  {country.upper()}: No placeDocId - SKIPPED")
#             results['no_mapping'].append(country)
#             continue
        
#         country_folder = normalize_for_folder(country)
#         print(f"\n📍 {country.upper()}")
#         print(f"   Firestore: allplaces/{place_doc_id}/what_to_eat/")
#         print(f"   Storage:   {STORAGE_ROOT_FOLDER}/{country_folder}/")
#         print(f"   Source: {source} | Dishes: {len(dishes)}")
        
#         try:
#             stats = upload_country(
#                 db=db,
#                 bucket=bucket,
#                 country_name=country,
#                 dishes=dishes,
#                 place_doc_id=place_doc_id,
#                 skip_images=args.skip_images,
#                 dry_run=args.dry_run
#             )
            
#             results['success'].append({
#                 'country': country,
#                 'place_doc_id': place_doc_id,
#                 **stats
#             })
#             results['total_dishes'] += stats['uploaded']
#             results['total_images'] += stats['images_uploaded']
#             results['failed_images'] += stats['images_failed']
            
#             status = "Would upload" if args.dry_run else "Uploaded"
#             print(f"   ✅ {status}: {stats['uploaded']} dishes")
#             if not args.skip_images:
#                 print(f"   📷 Images: {stats['images_uploaded']} uploaded, {stats['images_failed']} failed")
            
#         except Exception as e:
#             print(f"   ❌ Error: {e}")
#             results['failed'].append({'country': country, 'error': str(e)})
    
#     # Summary
#     print(f"\n{'='*60}")
#     print(f"📊 UPLOAD SUMMARY")
#     print(f"{'='*60}")
#     print(f"✅ Countries successful: {len(results['success'])}")
#     print(f"⏭️  Countries skipped (no mapping): {len(results['no_mapping'])}")
#     print(f"❌ Countries failed: {len(results['failed'])}")
#     print(f"🍽️  Total dishes uploaded: {results['total_dishes']}")
    
#     if not args.skip_images:
#         print(f"📷 Images uploaded: {results['total_images']}")
#         print(f"📷 Images failed: {results['failed_images']}")
#         print(f"\n📁 Storage Structure Created:")
#         print(f"   {STORAGE_ROOT_FOLDER}/")
#         print(f"   ├── india/")
#         print(f"   │   ├── roti/")
#         print(f"   │   │   └── 1.jpg")
#         print(f"   │   ├── naan/")
#         print(f"   │   │   └── 1.jpg")
#         print(f"   │   └── ...")
#         print(f"   ├── japan/")
#         print(f"   │   └── ...")
#         print(f"   └── ...")
    
#     if results['no_mapping']:
#         print(f"\n⚠️  Countries without placeDocId mapping:")
#         for c in results['no_mapping']:
#             print(f"   - {c}")
    
#     if args.dry_run:
#         print(f"\n🔍 This was a DRY RUN")
#         print(f"   Remove --dry-run to upload for real")
    
#     print(f"{'='*60}")
    
#     return 0


# if __name__ == '__main__':
#     exit(main())

# #!/usr/bin/env python
# """
# Firebase Upload Script for TasteAtlas "What to Eat" Data
# WITH Image Download & Firebase Storage Upload

# Storage Structure:
#     what_to_eat_food/{country}/{dish_id}/1.jpg, 2.jpg, ...

# Firestore Structure:
#     allplaces/{placeDocId}/what_to_eat/{dish_id}
    
#     Fields include:
#     - photo_urls: ["url1", "url2", ...]  (list for multiple images)

# Usage:
#     # Test with dry run first
#     python upload_to_firebase.py --credentials service_account.json --dry-run
    
#     # Test single country
#     python upload_to_firebase.py --credentials service_account.json --country india --dry-run
    
#     # Upload all countries (with images)
#     python upload_to_firebase.py --credentials service_account.json
    
#     # Skip image upload (faster, just Firestore)
#     python upload_to_firebase.py --credentials service_account.json --skip-images

# Prerequisites:
#     pip install firebase-admin requests
# """

# import json
# import os
# import glob
# import argparse
# import re
# import time
# import requests
# from datetime import datetime

# try:
#     import firebase_admin
#     from firebase_admin import credentials, firestore, storage
# except ImportError:
#     print("❌ firebase-admin not installed!")
#     print("   Run: pip install firebase-admin")
#     exit(1)


# # ═══════════════════════════════════════════════════════════════════════════
# # CONFIGURATION
# # ═══════════════════════════════════════════════════════════════════════════

# OUTPUT_DIR = "tasteatlas_output"          # Original scraped files
# ENHANCED_DIR = "tasteatlas_enhanced"      # Enhanced with OpenAI (preferred)
# MAPPING_FILE = "country_to_placeDocId.json"

# # Firebase Storage
# STORAGE_ROOT_FOLDER = "what_to_eat_food"  # Root folder in Firebase Storage
# IMAGE_DOWNLOAD_TIMEOUT = 20               # seconds
# IMAGE_RETRY_ATTEMPTS = 3
# DELAY_BETWEEN_UPLOADS = 0.1               # seconds (rate limiting)


# # ═══════════════════════════════════════════════════════════════════════════
# # HELPERS
# # ═══════════════════════════════════════════════════════════════════════════

# def load_json(file_path):
#     """Load JSON file"""
#     with open(file_path, 'r', encoding='utf-8') as f:
#         return json.load(f)


# def normalize_country_name(name):
#     """Normalize country name for matching"""
#     return name.lower().replace('_', '-').replace(' ', '-')


# def normalize_for_folder(name):
#     """Normalize name for folder path (use underscores)"""
#     folder = name.lower().strip()
#     folder = re.sub(r'[^\w\s-]', '', folder)
#     folder = re.sub(r'[\s-]+', '_', folder)
#     return folder.strip('_')


# def normalize_dish_id(name):
#     """Create clean document ID from dish name"""
#     doc_id = name.lower().strip()
#     doc_id = re.sub(r'[^\w\s-]', '', doc_id)
#     doc_id = re.sub(r'[\s-]+', '_', doc_id)
#     return doc_id.strip('_')[:100]  # Firestore doc ID limit


# def is_valid_image_url(url):
#     """Check if URL is a valid image URL"""
#     if not url:
#         return False
#     if url.endswith('random'):
#         return False
#     if 'tasteatlas.com' not in url and 'cdn.tasteatlas' not in url:
#         return False
#     return True


# def download_image(url, timeout=IMAGE_DOWNLOAD_TIMEOUT):
#     """Download image with retries"""
#     backoff = 1
    
#     for attempt in range(IMAGE_RETRY_ATTEMPTS):
#         try:
#             headers = {
#                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
#                 'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
#                 'Referer': 'https://www.tasteatlas.com/',
#             }
            
#             resp = requests.get(url, headers=headers, timeout=timeout)
#             resp.raise_for_status()
            
#             # Verify it's actually image data
#             content_type = resp.headers.get('Content-Type', '')
#             if 'image' not in content_type and len(resp.content) < 1000:
#                 return None
            
#             return resp.content
            
#         except Exception as e:
#             if attempt < IMAGE_RETRY_ATTEMPTS - 1:
#                 time.sleep(backoff)
#                 backoff *= 2
#             else:
#                 return None
    
#     return None


# def get_existing_images_count(bucket, country_folder, dish_id):
#     """Get count of existing images in the dish folder"""
#     prefix = f"{STORAGE_ROOT_FOLDER}/{country_folder}/{dish_id}/"
#     blobs = list(bucket.list_blobs(prefix=prefix))
#     return len(blobs)


# def upload_image_to_storage(bucket, image_data, country_folder, dish_id, image_number=1):
#     """
#     Upload image to Firebase Storage with organized folder structure.
    
#     Structure: what_to_eat_food/{country}/{dish_id}/{image_number}.jpg
#     Example:   what_to_eat_food/india/roti/1.jpg
#     """
#     try:
#         # Create blob path: what_to_eat_food/india/roti/1.jpg
#         blob_path = f"{STORAGE_ROOT_FOLDER}/{country_folder}/{dish_id}/{image_number}.jpg"
        
#         blob = bucket.blob(blob_path)
#         blob.upload_from_string(image_data, content_type="image/jpeg")
#         blob.make_public()
        
#         return blob.public_url
        
#     except Exception as e:
#         print(f"         ⚠️  Storage upload failed: {e}")
#         return None


# def clean_dish_data(dish):
#     """Clean and validate dish data for Firebase"""
    
#     # Fix broken image URLs (some end with 'random')
#     image_url = dish.get('image_url', '') or ''
#     if image_url.endswith('random') or 'random' in image_url:
#         image_url = ''
    
#     return {
#         'id': dish.get('id', ''),
#         'name': dish.get('name', ''),
#         'local_name': dish.get('local_name') or None,
#         'category': dish.get('category', 'DISH'),
#         'rank': int(dish.get('rank', 0)),
#         'short_description': dish.get('short_description', ''),
#         'image_url': image_url,      # Original TasteAtlas URL (backup)
#         'photo_urls': [],            # List of Firebase Storage URLs
#         'most_iconic_place': dish.get('most_iconic_place') or None,
#         'most_iconic_location': dish.get('most_iconic_location') or None,
#         'ingredients': dish.get('ingredients', []) or [],
#         'is_active': True,
#     }


# def get_country_files(output_dir, enhanced_dir=None):
#     """Get all country JSON files, preferring enhanced versions"""
#     pattern = os.path.join(output_dir, 'tasteatlas_*_dishes.json')
#     files = glob.glob(pattern)
    
#     country_data = {}
#     for file_path in files:
#         basename = os.path.basename(file_path)
#         country = basename.replace('tasteatlas_', '').replace('_dishes.json', '')
#         country = normalize_country_name(country)
        
#         # Check if enhanced version exists
#         actual_file = file_path
#         if enhanced_dir:
#             enhanced_file = os.path.join(enhanced_dir, basename)
#             if os.path.exists(enhanced_file):
#                 actual_file = enhanced_file
        
#         dishes = load_json(actual_file)
#         country_data[country] = {
#             'dishes': dishes,
#             'source': 'enhanced' if actual_file != file_path else 'original'
#         }
    
#     return country_data


# # ═══════════════════════════════════════════════════════════════════════════
# # UPLOAD FUNCTIONS
# # ═══════════════════════════════════════════════════════════════════════════

# def upload_country(db, bucket, country_name, dishes, place_doc_id, 
#                    skip_images=False, dry_run=False):
#     """Upload dishes for a single country to Firebase"""
    
#     stats = {
#         'uploaded': 0,
#         'skipped': 0,
#         'images_uploaded': 0,
#         'images_failed': 0,
#     }
    
#     # Country folder name for storage
#     country_folder = normalize_for_folder(country_name)
    
#     # Reference to allplaces document (only if not dry run)
#     place_ref = None
#     batch = None
#     batch_count = 0
#     MAX_BATCH = 400
    
#     if not dry_run:
#         place_ref = db.collection('allplaces').document(str(place_doc_id))
#         batch = db.batch()
    
#     for dish in dishes:
#         # Clean dish data
#         clean_dish = clean_dish_data(dish)
        
#         # Generate document ID
#         dish_id = normalize_dish_id(clean_dish['name'])
#         if not dish_id or not clean_dish['name']:
#             stats['skipped'] += 1
#             continue
        
#         # Handle image upload
#         original_image_url = clean_dish['image_url']
#         photo_urls = []  # List to store all image URLs
        
#         if not skip_images and is_valid_image_url(original_image_url):
#             if not dry_run:
#                 # Download image
#                 image_data = download_image(original_image_url)
                
#                 if image_data:
#                     # Check if there are existing images (for future use)
#                     existing_count = 0
#                     try:
#                         existing_count = get_existing_images_count(bucket, country_folder, dish_id)
#                     except:
#                         pass
                    
#                     # Upload new image with next number
#                     image_number = existing_count + 1
                    
#                     firebase_image_url = upload_image_to_storage(
#                         bucket=bucket,
#                         image_data=image_data,
#                         country_folder=country_folder,
#                         dish_id=dish_id,
#                         image_number=image_number
#                     )
                    
#                     if firebase_image_url:
#                         photo_urls.append(firebase_image_url)
#                         stats['images_uploaded'] += 1
#                     else:
#                         stats['images_failed'] += 1
#                 else:
#                     stats['images_failed'] += 1
                
#                 # Rate limiting
#                 time.sleep(DELAY_BETWEEN_UPLOADS)
        
#         # Update dish with Firebase Storage URLs
#         if photo_urls:
#             clean_dish['photo_urls'] = photo_urls
#         else:
#             # Fallback: use original URL as single item in list
#             if original_image_url:
#                 clean_dish['photo_urls'] = [original_image_url]
#             else:
#                 clean_dish['photo_urls'] = []
        
#         if dry_run:
#             # Show what would be uploaded
#             img_status = ""
#             if not skip_images and is_valid_image_url(original_image_url):
#                 img_status = f" → {country_folder}/{dish_id}/1.jpg"
#             print(f"      [{clean_dish['rank']:2d}] {dish_id}: {clean_dish['name'][:30]}{img_status}")
#         else:
#             # Add to Firestore batch
#             if place_ref is not None and batch is not None:
#                 dish_ref = place_ref.collection('what_to_eat').document(dish_id)
#                 batch.set(dish_ref, clean_dish, merge=True)
#                 batch_count += 1
                
#                 # Commit batch when full
#                 if batch_count >= MAX_BATCH:
#                     batch.commit()
#                     batch = db.batch()
#                     batch_count = 0
        
#         stats['uploaded'] += 1
    
#     # Commit remaining items
#     if not dry_run and batch is not None and batch_count > 0:
#         batch.commit()
    
#     # Update parent document metadata
#     if not dry_run and stats['uploaded'] > 0 and place_ref is not None:
#         try:
#             place_ref.set({
#                 'has_what_to_eat': True,
#                 'what_to_eat_count': stats['uploaded'],
#                 'what_to_eat_updated_at': firestore.SERVER_TIMESTAMP,
#             }, merge=True)
#         except Exception as e:
#             print(f"      ⚠️  Could not update metadata: {e}")
    
#     return stats


# # ═══════════════════════════════════════════════════════════════════════════
# # MAIN
# # ═══════════════════════════════════════════════════════════════════════════

# def main():
#     parser = argparse.ArgumentParser(description='Upload What to Eat data to Firebase')
#     parser.add_argument('--credentials', '-c', type=str, required=True,
#                        help='Path to Firebase service account JSON')
#     parser.add_argument('--bucket', '-b', type=str, default=None,
#                        help='Firebase Storage bucket (default: projectId.appspot.com)')
#     parser.add_argument('--output-dir', '-o', type=str, default=OUTPUT_DIR,
#                        help='Directory with scraped JSON files')
#     parser.add_argument('--enhanced-dir', '-e', type=str, default=ENHANCED_DIR,
#                        help='Directory with enhanced JSON files')
#     parser.add_argument('--mapping', '-m', type=str, default=MAPPING_FILE,
#                        help='Country to placeDocId mapping file')
#     parser.add_argument('--country', type=str, default=None,
#                        help='Upload only this country (for testing)')
#     parser.add_argument('--skip-images', action='store_true',
#                        help='Skip image download/upload (faster)')
#     parser.add_argument('--dry-run', '-d', action='store_true',
#                        help='Preview without uploading')
    
#     args = parser.parse_args()
    
#     # Validate files
#     if not os.path.exists(args.credentials):
#         print(f"❌ Credentials not found: {args.credentials}")
#         return 1
    
#     if not os.path.exists(args.output_dir):
#         print(f"❌ Output directory not found: {args.output_dir}")
#         return 1
    
#     if not os.path.exists(args.mapping):
#         print(f"❌ Mapping file not found: {args.mapping}")
#         return 1
    
#     # Load data
#     print(f"\n{'='*60}")
#     print(f"🍽️  What to Eat - Firebase Upload")
#     print(f"{'='*60}")
    
#     mapping = load_json(args.mapping)
    
#     # Check for enhanced directory
#     enhanced_dir = args.enhanced_dir if os.path.exists(args.enhanced_dir) else None
#     if enhanced_dir:
#         print(f"📂 Enhanced files: {enhanced_dir}")
    
#     country_data = get_country_files(args.output_dir, enhanced_dir)
    
#     print(f"📂 Found {len(country_data)} country files")
#     print(f"📍 Mapping has {len([v for v in mapping.values() if v])} placeDocIds")
#     print(f"📷 Image upload: {'DISABLED' if args.skip_images else 'ENABLED'}")
    
#     if not args.skip_images:
#         print(f"\n📁 Storage Structure:")
#         print(f"   {STORAGE_ROOT_FOLDER}/")
#         print(f"   └── {{country}}/")
#         print(f"       └── {{dish_id}}/")
#         print(f"           └── 1.jpg, 2.jpg, ...")
    
#     if args.dry_run:
#         print(f"\n🔍 DRY RUN - no actual uploads")
    
#     # Filter to single country if specified
#     if args.country:
#         country_key = normalize_country_name(args.country)
#         if country_key not in country_data:
#             print(f"❌ Country '{args.country}' not found in scraped data")
#             return 1
#         country_data = {country_key: country_data[country_key]}
    
#     # Initialize Firebase
#     bucket = None
#     if not args.dry_run:
#         print(f"\n🔥 Connecting to Firebase...")
        
#         # Load credentials to get project ID
#         with open(args.credentials, 'r') as f:
#             cred_data = json.load(f)
        
#         project_id = cred_data.get('project_id', '')
#         storage_bucket = args.bucket or f"{project_id}.appspot.com"
        
#         cred = credentials.Certificate(args.credentials)
#         firebase_admin.initialize_app(cred, {
#             'storageBucket': storage_bucket
#         })
        
#         db = firestore.client()
#         bucket = storage.bucket()
        
#         print(f"   Firestore: ✅")
#         print(f"   Storage bucket: {storage_bucket}")
#     else:
#         db = None
    
#     # Upload each country
#     print(f"\n{'─'*60}")
    
#     results = {
#         'success': [],
#         'no_mapping': [],
#         'failed': [],
#         'total_dishes': 0,
#         'total_images': 0,
#         'failed_images': 0,
#     }
    
#     for country, data in sorted(country_data.items()):
#         dishes = data['dishes']
#         source = data['source']
        
#         # Get placeDocId from mapping
#         place_doc_id = mapping.get(country)
#         if not place_doc_id:
#             alt_key = country.replace('-', '_')
#             place_doc_id = mapping.get(alt_key)
        
#         if not place_doc_id:
#             print(f"\n⏭️  {country.upper()}: No placeDocId - SKIPPED")
#             results['no_mapping'].append(country)
#             continue
        
#         country_folder = normalize_for_folder(country)
#         print(f"\n📍 {country.upper()}")
#         print(f"   Firestore: allplaces/{place_doc_id}/what_to_eat/")
#         print(f"   Storage:   {STORAGE_ROOT_FOLDER}/{country_folder}/")
#         print(f"   Source: {source} | Dishes: {len(dishes)}")
        
#         try:
#             stats = upload_country(
#                 db=db,
#                 bucket=bucket,
#                 country_name=country,
#                 dishes=dishes,
#                 place_doc_id=place_doc_id,
#                 skip_images=args.skip_images,
#                 dry_run=args.dry_run
#             )
            
#             results['success'].append({
#                 'country': country,
#                 'place_doc_id': place_doc_id,
#                 **stats
#             })
#             results['total_dishes'] += stats['uploaded']
#             results['total_images'] += stats['images_uploaded']
#             results['failed_images'] += stats['images_failed']
            
#             status = "Would upload" if args.dry_run else "Uploaded"
#             print(f"   ✅ {status}: {stats['uploaded']} dishes")
#             if not args.skip_images:
#                 print(f"   📷 Images: {stats['images_uploaded']} uploaded, {stats['images_failed']} failed")
            
#         except Exception as e:
#             print(f"   ❌ Error: {e}")
#             results['failed'].append({'country': country, 'error': str(e)})
    
#     # Summary
#     print(f"\n{'='*60}")
#     print(f"📊 UPLOAD SUMMARY")
#     print(f"{'='*60}")
#     print(f"✅ Countries successful: {len(results['success'])}")
#     print(f"⏭️  Countries skipped (no mapping): {len(results['no_mapping'])}")
#     print(f"❌ Countries failed: {len(results['failed'])}")
#     print(f"🍽️  Total dishes uploaded: {results['total_dishes']}")
    
#     if not args.skip_images:
#         print(f"📷 Images uploaded: {results['total_images']}")
#         print(f"📷 Images failed: {results['failed_images']}")
#         print(f"\n📁 Storage Structure Created:")
#         print(f"   {STORAGE_ROOT_FOLDER}/")
#         print(f"   ├── india/")
#         print(f"   │   ├── roti/")
#         print(f"   │   │   └── 1.jpg")
#         print(f"   │   ├── naan/")
#         print(f"   │   │   └── 1.jpg")
#         print(f"   │   └── ...")
#         print(f"   ├── japan/")
#         print(f"   │   └── ...")
#         print(f"   └── ...")
    
#     if results['no_mapping']:
#         print(f"\n⚠️  Countries without placeDocId mapping:")
#         for c in results['no_mapping']:
#             print(f"   - {c}")
    
#     if args.dry_run:
#         print(f"\n🔍 This was a DRY RUN")
#         print(f"   Remove --dry-run to upload for real")
    
#     print(f"{'='*60}")
    
#     return 0


# if __name__ == '__main__':
#     exit(main())







