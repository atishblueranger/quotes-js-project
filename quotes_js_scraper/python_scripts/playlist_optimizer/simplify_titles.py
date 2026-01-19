"""
Playlist Title Simplifier & Optimizer
Removes numbers from titles and creates catchy, short alternatives.

Examples:
- "Top 27 Haridwar Getaways" → "Top Haridwar Getaways" or "Haridwar Escapes"
- "Top 30 Dharamshala Destinations" → "Top Dharamshala Destinations" or "Dharamshala Must-Sees"
- "Best 45 Mumbai Cafes" → "Best Mumbai Cafes" or "Mumbai Cafe Guide"

Features:
- Removes numbers from titles automatically
- Generates catchy 2-4 word alternatives using GPT-4o-mini
- Shows before/after preview
- Dry-run mode for testing
- Batch processing

Usage:
  python simplify_titles.py --all --dry-run           # Preview all
  python simplify_titles.py --pattern "Top \d+" --dry-run  # Match pattern
  python simplify_titles.py --ids 123 456 789         # Specific IDs
  python simplify_titles.py --all --auto              # Auto-remove numbers only
"""

import os
import re
import json
import argparse
import time
from pathlib import Path
from typing import List, Optional, Dict, NamedTuple

# Retry logic
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Firebase
import firebase_admin
from firebase_admin import credentials, firestore

# ===================== CONFIG =====================
PROJECT_ID           = "mycasavsc"
SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
COLLECTION           = "playlistsNew"

OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY")
OPENAI_TEXT_MODEL    = "gpt-4o-mini"  # Fast and cheap

# Title parameters
MIN_TITLE_LENGTH = 2   # Minimum words
MAX_TITLE_LENGTH = 5   # Maximum words (prefer 3-4)
# ==================================================


class TitleResult(NamedTuple):
    """Result from title generation"""
    simple_title: str      # Title with numbers removed
    catchy_title: str      # AI-generated catchy alternative
    confidence: float      # How confident AI is (0.0-1.0)


# ===================== TITLE PROCESSING =====================

def remove_numbers_from_title(title: str) -> str:
    """
    Remove numbers from titles while keeping the rest natural.
    
    Examples:
        "Top 27 Haridwar Getaways" → "Top Haridwar Getaways"
        "Best 45 Mumbai Cafes" → "Best Mumbai Cafes"
        "30 Things to Do in Delhi" → "Things to Do in Delhi"
    """
    # Pattern to match: "Top/Best [number] [place]" or "[number] [things]"
    patterns = [
        r'\b\d+\b\s*',           # Any standalone number with optional space
        r'^\d+\s+',              # Number at start
        r'\s+\d+\s+',            # Number in middle
    ]
    
    result = title
    for pattern in patterns:
        result = re.sub(pattern, ' ', result)
    
    # Clean up multiple spaces
    result = ' '.join(result.split())
    
    return result.strip()


def build_title_system_prompt() -> str:
    """Build system prompt for title optimization"""
    return """You are a travel content editor specializing in creating catchy, memorable playlist titles.

Your mission: Transform titles into short, punchy alternatives that are:
- 2-4 words maximum (3 words is ideal)
- Catchy and memorable
- Easy to say and remember
- Natural sounding
- Travel-focused

REQUIREMENTS:
✓ Keep it SHORT (2-4 words)
✓ Make it catchy and memorable
✓ Include the city/place name if present
✓ Use simple, powerful words
✓ Make it feel curated and exclusive

STYLE EXAMPLES:
"Mumbai Eats" (not "Best Places to Eat in Mumbai")
"Delhi Gems" (not "Top Hidden Gems in Delhi")
"Goa Vibes" (not "Best Things to Do in Goa")
"Jaipur Escapes" (not "Top Tourist Destinations in Jaipur")
"Kolkata Culture" (not "Cultural Experiences in Kolkata")

✗ Avoid generic words like "Guide", "List", "Collection"
✗ Don't use numbers
✗ Don't be too formal or long"""


def build_title_user_prompt(original: str, simplified: str, city: str, country: str) -> str:
    """Build user prompt for title generation"""
    
    prompt_parts = [
        f'ORIGINAL TITLE: "{original}"',
        f'SIMPLIFIED (numbers removed): "{simplified}"',
        f'City: {city or "Unknown"}',
        f'Country: {country or "Unknown"}',
        f'',
        f'TASK:',
        f'Create a catchy 2-4 word title that:',
        f'1. Is shorter and punchier than the simplified version',
        f'2. Captures the essence of the playlist',
        f'3. Includes the city/place name if relevant',
        f'4. Is memorable and shareable',
        f'',
        f'STYLE TO MATCH:',
        f'- "Mumbai Eats" (short, direct, catchy)',
        f'- "Delhi Gems" (implies curation)',
        f'- "Goa Escapes" (evokes feeling)',
        f'- "Jaipur Culture" (focused topic)',
        f'',
        f'Return JSON with:',
        f'{{',
        f'  "catchy_title": "Your 2-4 word catchy title",',
        f'  "confidence": 0.9  // How confident (0.0-1.0)',
        f'}}',
        f'',
        f'Make it punchy and memorable!',
    ]
    
    return '\n'.join(prompt_parts)


@retry(
    wait=wait_exponential(multiplier=1.5, min=2, max=30),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((Exception,))
)
def generate_catchy_title(
    original_title: str,
    city: str,
    country: str
) -> TitleResult:
    """
    Generate a catchy title using AI.
    
    Returns TitleResult with simple_title, catchy_title, and confidence.
    """
    from openai import OpenAI
    
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not set")
    
    # First, remove numbers
    simple_title = remove_numbers_from_title(original_title)
    
    # If no AI key, just return simplified version
    if not OPENAI_API_KEY:
        return TitleResult(
            simple_title=simple_title,
            catchy_title=simple_title,
            confidence=0.5
        )
    
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    system_prompt = build_title_system_prompt()
    user_prompt = build_title_user_prompt(original_title, simple_title, city, country)
    
    try:
        response = client.chat.completions.create(
            model=OPENAI_TEXT_MODEL,
            temperature=0.7,  # Balanced creativity
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=100,
            response_format={"type": "json_object"},  # Force JSON
        )
        
        result_text = response.choices[0].message.content.strip()
        data = json.loads(result_text)
        
        catchy = (data.get("catchy_title") or simple_title).strip()
        confidence = float(data.get("confidence", 0.8))
        
        # Validate: must be 2-5 words
        word_count = len(catchy.split())
        if word_count < MIN_TITLE_LENGTH or word_count > MAX_TITLE_LENGTH:
            print(f"      ⚠️  Title word count ({word_count}) out of range, using simplified")
            catchy = simple_title
            confidence = 0.6
        
        return TitleResult(
            simple_title=simple_title,
            catchy_title=catchy,
            confidence=confidence
        )
        
    except Exception as e:
        print(f"      ⚠️  AI generation failed: {e}, using simplified title")
        return TitleResult(
            simple_title=simple_title,
            catchy_title=simple_title,
            confidence=0.5
        )


# ===================== FIRESTORE =====================

def init_firebase():
    """Initialize Firebase Admin SDK"""
    if not Path(SERVICE_ACCOUNT_JSON).exists():
        raise RuntimeError(f"Service account JSON not found: {SERVICE_ACCOUNT_JSON}")
    
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
        firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})


def fetch_playlists(
    specific_ids: Optional[List[str]] = None,
    pattern: Optional[str] = None,
    limit: Optional[int] = None
) -> List[tuple]:
    """
    Fetch playlists from Firestore.
    
    Args:
        specific_ids: List of specific document IDs
        pattern: Regex pattern to match titles (e.g., "Top \d+")
        limit: Maximum number to fetch
    
    Returns list of (doc_id, data_dict) tuples.
    """
    db = firestore.client()
    playlists = []
    
    if specific_ids:
        # Fetch specific documents
        print(f"   📋 Fetching {len(specific_ids)} specific playlist(s)...")
        for doc_id in specific_ids:
            doc = db.collection(COLLECTION).document(str(doc_id)).get()
            if doc.exists:
                playlists.append((doc.id, doc.to_dict()))
            else:
                print(f"      ⚠️  Document {doc_id} not found")
    else:
        # Fetch all
        print(f"   📋 Fetching playlists from Firestore...")
        query = db.collection(COLLECTION)
        
        if limit:
            query = query.limit(limit)
        
        for doc in query.stream():
            data = doc.to_dict()
            title = (data.get("title") or data.get("name") or "").strip()
            
            # Apply pattern filter if specified
            if pattern:
                if not re.search(pattern, title, re.IGNORECASE):
                    continue
            
            playlists.append((doc.id, data))
    
    return playlists


def update_playlist_title(
    doc_id: str,
    new_title: str,
    dry_run: bool = False
):
    """
    Update playlist title in Firestore.
    
    Args:
        doc_id: Document ID
        new_title: New title text
        dry_run: If True, don't actually update
    """
    if dry_run:
        print(f"      [DRY RUN] Would update Firestore")
        return
    
    db = firestore.client()
    doc_ref = db.collection(COLLECTION).document(doc_id)
    
    # Update title field
    update_data = {
        "title": new_title,
    }
    
    doc_ref.set(update_data, merge=True)


# ===================== PROCESSING =====================

def process_single_playlist(
    doc_id: str,
    data: Dict,
    args: argparse.Namespace
) -> bool:
    """
    Process a single playlist and optimize title.
    
    Returns True if successful, False otherwise.
    """
    try:
        # Extract fields
        original_title = (data.get("title") or data.get("name") or "").strip()
        city = (data.get("city") or "").strip()
        country = (data.get("country_name") or data.get("country") or "").strip()
        
        if not original_title:
            print(f"   ⚠️  No title found, skipping")
            return True
        
        # Check if title has numbers
        has_numbers = bool(re.search(r'\d+', original_title))
        
        if not has_numbers and not args.force:
            print(f"   ⏭️  No numbers in title, skipping")
            return True
        
        # Display info
        print(f"\n📋 {doc_id}")
        print(f"   📍 City: {city}, {country}")
        print(f"   📝 Original: {original_title}")
        
        # Choose processing mode
        if args.auto:
            # Simple mode: just remove numbers
            simple_title = remove_numbers_from_title(original_title)
            new_title = simple_title
            print(f"   ✂️  Simplified: {simple_title}")
        else:
            # AI mode: generate catchy title
            print(f"   🤖 Generating catchy title...")
            result = generate_catchy_title(
                original_title=original_title,
                city=city,
                country=country
            )
            
            print(f"   ✂️  Simplified: {result.simple_title}")
            print(f"   ✨ Catchy: {result.catchy_title}")
            print(f"   📊 Confidence: {result.confidence:.2f}")
            
            # Choose which title to use
            if args.use_catchy:
                new_title = result.catchy_title
                print(f"   👉 Using catchy title")
            else:
                new_title = result.simple_title
                print(f"   👉 Using simplified title")
        
        # Check if title actually changed
        if new_title == original_title:
            print(f"   ⏭️  Title unchanged, skipping update")
            return True
        
        # Update Firestore
        if not args.dry_run:
            print(f"   💾 Updating Firestore...")
            update_playlist_title(
                doc_id=doc_id,
                new_title=new_title,
                dry_run=args.dry_run
            )
            print(f"   ✅ Updated!")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return False


def batch_process(args: argparse.Namespace):
    """Main batch processing function"""
    
    print("=" * 80)
    print("✂️  PLAYLIST TITLE SIMPLIFIER & OPTIMIZER")
    print("=" * 80)
    
    # Display mode
    if args.dry_run:
        print("⚠️  DRY RUN MODE - No changes will be made to Firestore")
    
    if args.auto:
        print("ℹ️  AUTO MODE - Will only remove numbers (no AI)")
    elif args.use_catchy:
        print("✨ CATCHY MODE - Will use AI-generated catchy titles")
    else:
        print("✂️  SIMPLE MODE - Will use simplified titles (numbers removed)")
    
    if args.force:
        print("🔄 FORCE MODE - Processing all titles (even without numbers)")
    
    print()
    
    # Determine document IDs
    document_ids = None
    
    if args.ids_file:
        print(f"📄 Loading IDs from {args.ids_file}...")
        try:
            with open(args.ids_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                document_ids = data.get("document_ids", [])
                if not document_ids:
                    print(f"❌ ERROR: 'document_ids' not found in file")
                    return
                print(f"✅ Loaded {len(document_ids)} IDs")
        except Exception as e:
            print(f"❌ ERROR reading file: {e}")
            return
    elif args.ids:
        document_ids = args.ids
        print(f"📋 Processing {len(document_ids)} IDs from command line")
    
    # Initialize Firebase
    print("\n🔥 Initializing Firebase...")
    init_firebase()
    
    # Fetch playlists
    print("🔍 Fetching playlists...")
    playlists = fetch_playlists(
        specific_ids=document_ids,
        pattern=args.pattern,
        limit=args.limit
    )
    
    if not playlists:
        print("❌ No playlists found!")
        return
    
    total = len(playlists)
    print(f"✅ Found {total} playlist(s) to process")
    
    # Process playlists
    print("\n" + "=" * 80)
    print(f"🚀 STARTING PROCESSING ({total} playlists)")
    print("=" * 80)
    
    success_count = 0
    skip_count = 0
    fail_count = 0
    start_time = time.time()
    
    for idx, (doc_id, data) in enumerate(playlists, 1):
        print(f"\n[{idx}/{total}] Processing {doc_id}...")
        
        success = process_single_playlist(doc_id, data, args)
        
        if success:
            # Check if title had numbers
            title = (data.get("title") or "").strip()
            has_numbers = bool(re.search(r'\d+', title))
            if not has_numbers and not args.force:
                skip_count += 1
            else:
                success_count += 1
        else:
            fail_count += 1
        
        # Rate limiting between requests (only if using AI)
        if idx < total and args.sleep > 0 and not args.auto:
            time.sleep(args.sleep)
    
    elapsed = time.time() - start_time
    
    # Summary
    print("\n" + "=" * 80)
    print("🎉 PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"✅ Successfully processed: {success_count}/{total}")
    print(f"⏭️  Skipped (no numbers): {skip_count}/{total}")
    print(f"❌ Failed: {fail_count}/{total}")
    print(f"⏱️  Total time: {elapsed/60:.1f} minutes")
    if total > 0:
        print(f"📊 Average: {elapsed/total:.1f} seconds per playlist")
    
    if args.dry_run:
        print("\n💡 This was a DRY RUN. Remove --dry-run to actually update Firestore.")


# ===================== CLI =====================

def main():
    parser = argparse.ArgumentParser(
        description="Simplify and optimize playlist titles by removing numbers and creating catchy alternatives",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview all titles with numbers (dry run)
  python %(prog)s --all --dry-run
  
  # Preview titles matching pattern
  python %(prog)s --pattern "Top \d+" --dry-run
  
  # Auto-remove numbers only (no AI)
  python %(prog)s --all --auto
  
  # Generate catchy titles with AI
  python %(prog)s --all --use-catchy
  
  # Process specific playlists
  python %(prog)s --ids 123 456 789
  
  # Test with limited number
  python %(prog)s --limit 10 --dry-run
        """
    )
    
    # Mode selection
    mode = parser.add_mutually_exclusive_group(required=False)
    mode.add_argument("--all", action="store_true",
                     help="Process ALL playlists in collection")
    mode.add_argument("--ids", nargs="+",
                     help="Specific document IDs (space-separated)")
    mode.add_argument("--ids-file", type=str,
                     help='JSON file with {"document_ids": [...]}')
    
    # Filtering options
    parser.add_argument("--pattern", type=str,
                       help='Regex pattern to match titles (e.g., "Top \\d+" or "Best \\d+")')
    parser.add_argument("--limit", type=int,
                       help="Limit number of playlists to process (for testing)")
    
    # Processing mode
    parser.add_argument("--auto", action="store_true",
                       help="Auto mode: only remove numbers, don't use AI")
    parser.add_argument("--use-catchy", action="store_true",
                       help="Use AI-generated catchy titles (default: use simplified)")
    parser.add_argument("--force", action="store_true",
                       help="Process all titles (even without numbers)")
    
    # Testing options
    parser.add_argument("--dry-run", action="store_true",
                       help="Preview without updating Firestore")
    parser.add_argument("--sleep", type=float, default=0.5,
                       help="Seconds to sleep between requests (default: 0.5)")
    parser.add_argument("--verbose", action="store_true",
                       help="Show full error tracebacks")
    
    args = parser.parse_args()
    
    # If --limit is specified without --all/--ids/--ids-file, treat as --all --limit
    if args.limit and not (args.all or args.ids or args.ids_file):
        args.all = True
        print(f"ℹ️  Using --limit {args.limit} implies --all mode")
    
    # Validation
    if not (args.all or args.ids or args.ids_file):
        print("❌ ERROR: Must specify --all, --ids, --ids-file, or --limit")
        parser.print_help()
        return 1
    
    if args.auto and args.use_catchy:
        print("❌ ERROR: Cannot use both --auto and --use-catchy")
        return 1
    
    if not args.auto and not OPENAI_API_KEY:
        print("⚠️  WARNING: OPENAI_API_KEY not set, using --auto mode (numbers removal only)")
        args.auto = True
    
    # Run processing
    try:
        batch_process(args)
        return 0
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        return 130
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())