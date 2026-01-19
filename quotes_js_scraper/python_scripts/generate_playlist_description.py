"""
Enhanced Playlist Description Generator
Combines robust error handling with creative, Instagram-worthy descriptions.

Features:
- Retry logic for API failures
- Flexible CLI options (--all, --ids, --ids-file)
- Creative, engaging descriptions optimized for social media
- Preserves old descriptions before updating
- Skip existing descriptions
- Dry-run mode for testing

Usage:
  python generate_playlist_descriptions_v3.py --test --batch-size 5
  python generate_playlist_descriptions_v3.py --ids-file playlists.json
  python generate_playlist_descriptions_v3.py --all --force
  python generate_playlist_descriptions_v3.py --ids ABC123 DEF456 --dry-run
"""

import os
import json
import argparse
import time
from pathlib import Path
from typing import List, Optional, Dict, NamedTuple
from datetime import datetime

# Retry logic
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Firebase
import firebase_admin
from firebase_admin import credentials, firestore

# ===================== CONFIG =====================
PROJECT_ID           = "mycasavsc"
SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
COLLECTION           = "playlistsNew"

OPENAI_API_KEY       = "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"
OPENAI_TEXT_MODEL    = "gpt-4o-mini"  # Latest mini model

# Description parameters
DESCRIPTION_MIN_LENGTH = 60
DESCRIPTION_MAX_LENGTH = 110
DESCRIPTION_TARGET     = 90  # Sweet spot for Instagram/Twitter
# ==================================================


class GenerationResult(NamedTuple):
    """Result from description generation"""
    description: str
    alt: str
    style: str
    confidence: float


# ===================== AI GENERATION =====================

def build_creative_system_prompt(lang: str, tone: str, max_chars: int) -> str:
    """Build system prompt for creative, engaging descriptions"""
    return f"""You are a creative travel content writer specializing in engaging, Instagram-worthy descriptions that inspire wanderlust.

Your mission: Write catchy, concise descriptions that make destinations irresistible.

REQUIREMENTS:
- Language: {lang}
- Tone: {tone} (but always exciting and inviting)
- Target length: {max_chars} characters (±20 chars is fine)
- Style: Instagram-worthy, sensory, vivid
- Focus: What makes this experience special and worth sharing

GUIDELINES:
✓ Use vivid, sensory language that paints a picture
✓ Be specific about what users will experience
✓ Include hooks or clever wordplay when appropriate
✓ Make it shareable and quotable
✓ Focus on feelings and experiences, not just facts
✗ Avoid clichés like "must-see", "don't miss", "hidden gem" (unless creative)
✗ No emojis or hashtags
✗ Don't be generic or boring

Think: Would someone want to screenshot this and share it?"""


def build_creative_user_prompt(
    title: str, 
    city: str, 
    country: str, 
    existing: Optional[str]
) -> str:
    """Build user prompt with examples and context"""
    
    examples = [
        "We've curated the best Instagrammable places to help you build a picture-perfect feed!",
        "Matcha made in heaven! From creamy lattes to dreamy desserts, these spots are every matcha lover's go to",
        "Hidden gems waiting to be discovered! Experience the authentic soul through local eyes",
        "From sunrise to sunset, these spots capture the magic that makes this place unforgettable",
        "Where locals eat, drink, and gather. No tourist traps—just authentic experiences",
    ]
    
    prompt_parts = [
        f'PLAYLIST DETAILS:',
        f'- Title: "{title}"',
        f'- City: {city or "Various"}',
        f'- Country: {country or "Various"}',
        f'',
        f'INSPIRATION (match this style and energy):',
    ]
    
    for i, ex in enumerate(examples, 1):
        prompt_parts.append(f'{i}. "{ex}"')
    
    if existing:
        prompt_parts.extend([
            f'',
            f'CURRENT DESCRIPTION (improve on this):',
            f'"{existing}"',
        ])
    
    prompt_parts.extend([
        f'',
        f'TASK:',
        f'Create an engaging description that:',
        f'1. Captures what makes this playlist special',
        f'2. Makes people excited to explore',
        f'3. Is quotable and shareable',
        f'4. Mentions the city/destination naturally',
        f'5. Focuses on the experience, not just places',
        f'',
        f'Return JSON with:',
        f'{{',
        f'  "description": "The main description ({DESCRIPTION_MIN_LENGTH}-{DESCRIPTION_MAX_LENGTH} chars)",',
        f'  "alt": "Optional shorter version (max 90 chars) for meta/preview",',
        f'  "style": "Style/theme used (e.g., Foodie, Adventure, Instagram-worthy)",',
        f'  "confidence": 0.8  // How confident you are (0.0-1.0)',
        f'}}',
        f'',
        f'Make it irresistible! 🎯',
    ])
    
    return '\n'.join(prompt_parts)


@retry(
    wait=wait_exponential(multiplier=1.5, min=2, max=30),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((Exception,))
)
def generate_description_with_ai(
    title: str,
    city: str,
    country: str,
    existing: Optional[str],
    lang: str = "en",
    tone: str = "friendly",
    max_chars: int = DESCRIPTION_TARGET
) -> GenerationResult:
    """
    Generate engaging description using OpenAI with retry logic.
    
    Returns GenerationResult with description, alt, style, and confidence.
    """
    from openai import OpenAI
    
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not set")
    
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    system_prompt = build_creative_system_prompt(lang, tone, max_chars)
    user_prompt = build_creative_user_prompt(title, city, country, existing)
    
    try:
        response = client.chat.completions.create(
            model=OPENAI_TEXT_MODEL,
            temperature=0.75,  # Higher for creativity
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=400,
            response_format={"type": "json_object"},  # Force JSON response
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # Parse JSON (with fallback for markdown blocks)
        if result_text.startswith("```"):
            chunks = result_text.split("```")
            if len(chunks) >= 3:
                result_text = chunks[1].replace("json", "").strip()
        
        data = json.loads(result_text)
        
        # Extract and validate
        description = (data.get("description") or "").strip()
        alt = (data.get("alt") or "").strip()
        style = (data.get("style") or "General").strip()
        confidence = float(data.get("confidence", 0.8))
        
        # Clean up (remove multiple spaces, newlines)
        description = " ".join(description.split())
        alt = " ".join(alt.split())
        
        # Validate length
        if len(description) < DESCRIPTION_MIN_LENGTH:
            print(f"      ⚠️  Description too short ({len(description)} chars), using fallback")
            description = generate_fallback_description(title, city, country)
            confidence = 0.5
            style = "Fallback"
        elif len(description) > DESCRIPTION_MAX_LENGTH + 20:
            # Truncate if way too long
            description = description[:DESCRIPTION_MAX_LENGTH] + "..."
            
        return GenerationResult(
            description=description,
            alt=alt,
            style=style,
            confidence=confidence
        )
        
    except json.JSONDecodeError as e:
        print(f"      ⚠️  JSON parse error: {e}")
        # Fallback: use the raw text as description
        return GenerationResult(
            description=result_text[:DESCRIPTION_MAX_LENGTH] if result_text else generate_fallback_description(title, city, country),
            alt="",
            style="Raw",
            confidence=0.6
        )
    except Exception as e:
        print(f"      ⚠️  API error: {e}")
        raise  # Let tenacity retry


def generate_fallback_description(title: str, city: str, country: str) -> str:
    """Generate simple fallback description if AI fails completely"""
    if city and country:
        return f"Discover the best of {city}, {country}! Handpicked spots and experiences that capture what makes this destination special."
    elif city:
        return f"Explore {city} through carefully curated locations that showcase the city's unique character and charm."
    elif country:
        return f"Experience the essence of {country} with our collection of must-visit places and unforgettable moments."
    else:
        return f"A curated collection of amazing places and experiences. {title}"


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
    start_after: Optional[str] = None,
    limit: Optional[int] = None
) -> List[tuple]:
    """
    Fetch playlists from Firestore.
    
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
        # Fetch all or limited
        print(f"   📋 Fetching playlists from Firestore...")
        query = db.collection(COLLECTION)
        
        if limit:
            query = query.limit(limit)
        
        for doc in query.stream():
            if start_after and doc.id <= start_after:
                continue
            playlists.append((doc.id, doc.to_dict()))
    
    return playlists


def update_playlist_description(
    doc_id: str,
    new_description: str,
    dry_run: bool = False
):
    """
    Update playlist description in Firestore.
    
    Args:
        doc_id: Document ID
        new_description: New description text
        dry_run: If True, don't actually update
    """
    if dry_run:
        print(f"      [DRY RUN] Would update Firestore")
        return
    
    db = firestore.client()
    doc_ref = db.collection(COLLECTION).document(doc_id)
    
    # Simply update the description field only
    update_data = {
        "description": new_description,
    }
    
    doc_ref.set(update_data, merge=True)


# ===================== PROCESSING =====================

def process_single_playlist(
    doc_id: str,
    data: Dict,
    args: argparse.Namespace
) -> bool:
    """
    Process a single playlist and generate description.
    
    Returns True if successful, False otherwise.
    """
    try:
        # Extract fields
        title = (data.get("title") or data.get("name") or f"Untitled-{doc_id}").strip()
        city = (data.get("city") or "").strip()
        country = (data.get("country_name") or data.get("country") or "").strip()
        
        # Get existing description
        existing_desc = (data.get("description") or "").strip()
        
        # Check if should skip
        if args.skip_existing and existing_desc:
            print(f"   ⏭️  Skipping (already has description)")
            return True
        
        # Display info
        print(f"\n📋 {doc_id}")
        print(f"   📍 Title: {title}")
        print(f"   🗺️  Location: {city}, {country}")
        
        if existing_desc and not args.force:
            print(f"   💬 Current: {existing_desc[:80]}...")
        
        # Generate description
        print(f"   🤖 Generating description...")
        
        result = generate_description_with_ai(
            title=title,
            city=city,
            country=country,
            existing=existing_desc if args.use_existing_context else None,
            lang=args.lang,
            tone=args.tone,
            max_chars=args.max_chars
        )
        
        # Display result
        print(f"   ✨ Style: {result.style}")
        print(f"   📝 Description ({len(result.description)} chars):")
        print(f"      {result.description}")
        if result.alt:
            print(f"   📄 Alt ({len(result.alt)} chars): {result.alt}")
        print(f"   📊 Confidence: {result.confidence:.2f}")
        
        # Update Firestore
        if not args.dry_run:
            print(f"   💾 Updating Firestore (description field)...")
            update_playlist_description(
                doc_id=doc_id,
                new_description=result.description,
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
    print("📝 PLAYLIST DESCRIPTION GENERATOR v3.0")
    print("=" * 80)
    
    # Display mode
    if args.dry_run:
        print("⚠️  DRY RUN MODE - No changes will be made to Firestore")
    
    print("ℹ️  Will update 'description' field directly")
    
    if args.force:
        print("🔄 FORCE MODE - Regenerating all descriptions (even existing ones)")
    
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
        start_after=args.start_after,
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
            # Check if actually updated or skipped
            if args.skip_existing and data.get("description"):
                skip_count += 1
            else:
                success_count += 1
        else:
            fail_count += 1
        
        # Rate limiting between requests
        if idx < total and args.sleep > 0:
            time.sleep(args.sleep)
    
    elapsed = time.time() - start_time
    
    # Summary
    print("\n" + "=" * 80)
    print("🎉 PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"✅ Successfully generated: {success_count}/{total}")
    print(f"⏭️  Skipped (already exist): {skip_count}/{total}")
    print(f"❌ Failed: {fail_count}/{total}")
    print(f"⏱️  Total time: {elapsed/60:.1f} minutes")
    print(f"📊 Average: {elapsed/total:.1f} seconds per playlist")
    
    if args.dry_run:
        print("\n💡 This was a DRY RUN. Remove --dry-run to actually update Firestore.")


# ===================== CLI =====================

TONES = ["friendly", "editorial", "playful", "luxury", "adventurous", "foodie", "cultural"]

def main():
    parser = argparse.ArgumentParser(
        description="Generate engaging, Instagram-worthy descriptions for playlists using AI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with 5 playlists (dry run)
  python %(prog)s --limit 5 --dry-run
  
  # Process specific playlists
  python %(prog)s --ids ABC123 DEF456 XYZ789
  
  # Process from JSON file
  python %(prog)s --ids-file my_playlists.json
  
  # Process all playlists
  python %(prog)s --all
  
  # Skip playlists that already have descriptions
  python %(prog)s --all --skip-existing
        """
    )
    
    # Mode selection
    mode = parser.add_mutually_exclusive_group(required=False)  # Changed to False
    mode.add_argument("--all", action="store_true",
                     help="Process ALL playlists in collection")
    mode.add_argument("--ids", nargs="+",
                     help="Specific document IDs (space-separated)")
    mode.add_argument("--ids-file", type=str,
                     help='JSON file with {"document_ids": ["id1", "id2", ...]}')
    
    # Filtering options
    parser.add_argument("--start-after", type=str,
                       help="Skip documents <= this ID (for resuming)")
    parser.add_argument("--limit", type=int,
                       help="Limit number of playlists to process (for testing)")
    
    # Description options
    parser.add_argument("--tone", choices=TONES, default="friendly",
                       help="Tone/style for descriptions (default: friendly)")
    parser.add_argument("--lang", default="en",
                       help="Language code (default: en)")
    parser.add_argument("--max-chars", type=int, default=DESCRIPTION_TARGET,
                       help=f"Target character length (default: {DESCRIPTION_TARGET})")
    
    # Behavior options
    parser.add_argument("--skip-existing", action="store_true",
                       help="Skip playlists that already have descriptions")
    parser.add_argument("--force", action="store_true",
                       help="Regenerate descriptions even if they exist")
    parser.add_argument("--use-existing-context", action="store_true",
                       help="Use existing description as context for AI")
    
    # Testing options
    parser.add_argument("--dry-run", action="store_true",
                       help="Preview without updating Firestore")
    parser.add_argument("--sleep", type=float, default=1.0,
                       help="Seconds to sleep between requests (default: 1.0)")
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
    
    if not OPENAI_API_KEY:
        print("❌ ERROR: OPENAI_API_KEY not set!")
        print("Set it with: export OPENAI_API_KEY='sk-...'  (or setx on Windows)")
        return 1
    
    if args.skip_existing and args.force:
        print("❌ ERROR: Cannot use both --skip-existing and --force")
        return 1
    
    if args.all and args.limit:
        print("⚠️  WARNING: Using --all with --limit will process limited playlists")
    
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
# """
# Enhanced Playlist Description Generator
# Combines robust error handling with creative, Instagram-worthy descriptions.

# Features:
# - Retry logic for API failures
# - Flexible CLI options (--all, --ids, --ids-file)
# - Creative, engaging descriptions optimized for social media
# - Preserves old descriptions before updating
# - Skip existing descriptions
# - Dry-run mode for testing

# Usage:
#   python generate_playlist_descriptions_v3.py --test --batch-size 5
#   python generate_playlist_descriptions_v3.py --ids-file playlists.json
#   python generate_playlist_descriptions_v3.py --all --force
#   python generate_playlist_descriptions_v3.py --ids ABC123 DEF456 --dry-run
# """

# import os
# import json
# import argparse
# import time
# from pathlib import Path
# from typing import List, Optional, Dict, NamedTuple
# from datetime import datetime

# # Retry logic
# from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# # Firebase
# import firebase_admin
# from firebase_admin import credentials, firestore

# # ===================== CONFIG =====================
# PROJECT_ID           = "mycasavsc"
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# COLLECTION           = "playlistsNew"

# OPENAI_API_KEY       = "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"
# OPENAI_TEXT_MODEL    = "gpt-4o-mini"  # Latest mini model

# # Description parameters
# DESCRIPTION_MIN_LENGTH = 60
# DESCRIPTION_MAX_LENGTH = 110
# DESCRIPTION_TARGET     = 90  # Sweet spot for Instagram/Twitter
# # ==================================================


# class GenerationResult(NamedTuple):
#     """Result from description generation"""
#     description: str
#     alt: str
#     style: str
#     confidence: float


# # ===================== AI GENERATION =====================

# def build_creative_system_prompt(lang: str, tone: str, max_chars: int) -> str:
#     """Build system prompt for creative, engaging descriptions"""
#     return f"""You are a creative travel content writer specializing in engaging, Instagram-worthy descriptions that inspire wanderlust.

# Your mission: Write catchy, concise descriptions that make destinations irresistible.

# REQUIREMENTS:
# - Language: {lang}
# - Tone: {tone} (but always exciting and inviting)
# - Target length: {max_chars} characters (±20 chars is fine)
# - Style: Instagram-worthy, sensory, vivid
# - Focus: What makes this experience special and worth sharing

# GUIDELINES:
# ✓ Use vivid, sensory language that paints a picture
# ✓ Be specific about what users will experience
# ✓ Include hooks or clever wordplay when appropriate
# ✓ Make it shareable and quotable
# ✓ Focus on feelings and experiences, not just facts
# ✗ Avoid clichés like "must-see", "don't miss", "hidden gem" (unless creative)
# ✗ No emojis or hashtags
# ✗ Don't be generic or boring

# Think: Would someone want to screenshot this and share it?"""


# def build_creative_user_prompt(
#     title: str, 
#     city: str, 
#     country: str, 
#     existing: Optional[str]
# ) -> str:
#     """Build user prompt with examples and context"""
    
#     examples = [
#         "We've curated the best Instagrammable places to help you build a picture-perfect feed!",
#         "Matcha made in heaven! From creamy lattes to dreamy desserts, these spots are every matcha lover's go to",
#         "Hidden gems waiting to be discovered! Experience the authentic soul through local eyes",
#         "From sunrise to sunset, these spots capture the magic that makes this place unforgettable",
#         "Where locals eat, drink, and gather. No tourist traps—just authentic experiences",
#     ]
    
#     prompt_parts = [
#         f'PLAYLIST DETAILS:',
#         f'- Title: "{title}"',
#         f'- City: {city or "Various"}',
#         f'- Country: {country or "Various"}',
#         f'',
#         f'INSPIRATION (match this style and energy):',
#     ]
    
#     for i, ex in enumerate(examples, 1):
#         prompt_parts.append(f'{i}. "{ex}"')
    
#     if existing:
#         prompt_parts.extend([
#             f'',
#             f'CURRENT DESCRIPTION (improve on this):',
#             f'"{existing}"',
#         ])
    
#     prompt_parts.extend([
#         f'',
#         f'TASK:',
#         f'Create an engaging description that:',
#         f'1. Captures what makes this playlist special',
#         f'2. Makes people excited to explore',
#         f'3. Is quotable and shareable',
#         f'4. Mentions the city/destination naturally',
#         f'5. Focuses on the experience, not just places',
#         f'',
#         f'Return JSON with:',
#         f'{{',
#         f'  "description": "The main description ({DESCRIPTION_MIN_LENGTH}-{DESCRIPTION_MAX_LENGTH} chars)",',
#         f'  "alt": "Optional shorter version (max 90 chars) for meta/preview",',
#         f'  "style": "Style/theme used (e.g., Foodie, Adventure, Instagram-worthy)",',
#         f'  "confidence": 0.8  // How confident you are (0.0-1.0)',
#         f'}}',
#         f'',
#         f'Make it irresistible! 🎯',
#     ])
    
#     return '\n'.join(prompt_parts)


# @retry(
#     wait=wait_exponential(multiplier=1.5, min=2, max=30),
#     stop=stop_after_attempt(5),
#     retry=retry_if_exception_type((Exception,))
# )
# def generate_description_with_ai(
#     title: str,
#     city: str,
#     country: str,
#     existing: Optional[str],
#     lang: str = "en",
#     tone: str = "friendly",
#     max_chars: int = DESCRIPTION_TARGET
# ) -> GenerationResult:
#     """
#     Generate engaging description using OpenAI with retry logic.
    
#     Returns GenerationResult with description, alt, style, and confidence.
#     """
#     from openai import OpenAI
    
#     if not OPENAI_API_KEY:
#         raise RuntimeError("OPENAI_API_KEY is not set")
    
#     client = OpenAI(api_key=OPENAI_API_KEY)
    
#     system_prompt = build_creative_system_prompt(lang, tone, max_chars)
#     user_prompt = build_creative_user_prompt(title, city, country, existing)
    
#     try:
#         response = client.chat.completions.create(
#             model=OPENAI_TEXT_MODEL,
#             temperature=0.75,  # Higher for creativity
#             messages=[
#                 {"role": "system", "content": system_prompt},
#                 {"role": "user", "content": user_prompt},
#             ],
#             max_tokens=400,
#             response_format={"type": "json_object"},  # Force JSON response
#         )
        
#         result_text = response.choices[0].message.content.strip()
        
#         # Parse JSON (with fallback for markdown blocks)
#         if result_text.startswith("```"):
#             chunks = result_text.split("```")
#             if len(chunks) >= 3:
#                 result_text = chunks[1].replace("json", "").strip()
        
#         data = json.loads(result_text)
        
#         # Extract and validate
#         description = (data.get("description") or "").strip()
#         alt = (data.get("alt") or "").strip()
#         style = (data.get("style") or "General").strip()
#         confidence = float(data.get("confidence", 0.8))
        
#         # Clean up (remove multiple spaces, newlines)
#         description = " ".join(description.split())
#         alt = " ".join(alt.split())
        
#         # Validate length
#         if len(description) < DESCRIPTION_MIN_LENGTH:
#             print(f"      ⚠️  Description too short ({len(description)} chars), using fallback")
#             description = generate_fallback_description(title, city, country)
#             confidence = 0.5
#             style = "Fallback"
#         elif len(description) > DESCRIPTION_MAX_LENGTH + 20:
#             # Truncate if way too long
#             description = description[:DESCRIPTION_MAX_LENGTH] + "..."
            
#         return GenerationResult(
#             description=description,
#             alt=alt,
#             style=style,
#             confidence=confidence
#         )
        
#     except json.JSONDecodeError as e:
#         print(f"      ⚠️  JSON parse error: {e}")
#         # Fallback: use the raw text as description
#         return GenerationResult(
#             description=result_text[:DESCRIPTION_MAX_LENGTH] if result_text else generate_fallback_description(title, city, country),
#             alt="",
#             style="Raw",
#             confidence=0.6
#         )
#     except Exception as e:
#         print(f"      ⚠️  API error: {e}")
#         raise  # Let tenacity retry


# def generate_fallback_description(title: str, city: str, country: str) -> str:
#     """Generate simple fallback description if AI fails completely"""
#     if city and country:
#         return f"Discover the best of {city}, {country}! Handpicked spots and experiences that capture what makes this destination special."
#     elif city:
#         return f"Explore {city} through carefully curated locations that showcase the city's unique character and charm."
#     elif country:
#         return f"Experience the essence of {country} with our collection of must-visit places and unforgettable moments."
#     else:
#         return f"A curated collection of amazing places and experiences. {title}"


# # ===================== FIRESTORE =====================

# def init_firebase():
#     """Initialize Firebase Admin SDK"""
#     if not Path(SERVICE_ACCOUNT_JSON).exists():
#         raise RuntimeError(f"Service account JSON not found: {SERVICE_ACCOUNT_JSON}")
    
#     if not firebase_admin._apps:
#         cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#         firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})


# def fetch_playlists(
#     specific_ids: Optional[List[str]] = None,
#     start_after: Optional[str] = None,
#     limit: Optional[int] = None
# ) -> List[tuple]:
#     """
#     Fetch playlists from Firestore.
    
#     Returns list of (doc_id, data_dict) tuples.
#     """
#     db = firestore.client()
#     playlists = []
    
#     if specific_ids:
#         # Fetch specific documents
#         print(f"   📋 Fetching {len(specific_ids)} specific playlist(s)...")
#         for doc_id in specific_ids:
#             doc = db.collection(COLLECTION).document(str(doc_id)).get()
#             if doc.exists:
#                 playlists.append((doc.id, doc.to_dict()))
#             else:
#                 print(f"      ⚠️  Document {doc_id} not found")
#     else:
#         # Fetch all or limited
#         print(f"   📋 Fetching playlists from Firestore...")
#         query = db.collection(COLLECTION)
        
#         if limit:
#             query = query.limit(limit)
        
#         for doc in query.stream():
#             if start_after and doc.id <= start_after:
#                 continue
#             playlists.append((doc.id, doc.to_dict()))
    
#     return playlists


# def update_playlist_description(
#     doc_id: str,
#     new_description: str,
#     alt: str,
#     style: str,
#     confidence: float,
#     old_description: Optional[str],
#     overwrite: bool = False,
#     dry_run: bool = False
# ):
#     """
#     Update playlist description in Firestore.
    
#     Args:
#         doc_id: Document ID
#         new_description: New description text
#         alt: Alternative/shorter description
#         style: Style used for generation
#         confidence: Confidence score
#         old_description: Previous description (for backup)
#         overwrite: If True, write to 'description' field (default: 'description_ai')
#         dry_run: If True, don't actually update
#     """
#     if dry_run:
#         print(f"      [DRY RUN] Would update Firestore")
#         return
    
#     db = firestore.client()
#     doc_ref = db.collection(COLLECTION).document(doc_id)
    
#     # if overwrite:
#     #     # Update main description field and preserve old one
#     #     update_data = {
#     #         "description": new_description,
#     #         "description_prev": old_description or firestore.DELETE_FIELD,
#     #         "description_alt": alt or firestore.DELETE_FIELD,
#     #         "description_style": style,
#     #         "description_confidence": confidence,
#     #         "description_updated_at": firestore.SERVER_TIMESTAMP,
#     #         "description_source": "ai",
#     #     }
#     # else:
#     #     # Update AI-specific field (safer default)
#     #     update_data = {
#     #         "description_ai": new_description,
#     #         "description_alt": alt or firestore.DELETE_FIELD,
#     #         "description_style": style,
#     #         "description_confidence": confidence,
#     #         "description_updated_at": firestore.SERVER_TIMESTAMP,
#     #         "description_source": "ai",
#     #     }
#     update_data = {
#         "description": new_description,
#     }
#     doc_ref.set(update_data, merge=True)


# # ===================== PROCESSING =====================

# def process_single_playlist(
#     doc_id: str,
#     data: Dict,
#     args: argparse.Namespace
# ) -> bool:
#     """
#     Process a single playlist and generate description.
    
#     Returns True if successful, False otherwise.
#     """
#     try:
#         # Extract fields
#         title = (data.get("title") or data.get("name") or f"Untitled-{doc_id}").strip()
#         city = (data.get("city") or "").strip()
#         country = (data.get("country_name") or data.get("country") or "").strip()
        
#         # Determine target field
#         target_field = "description" if args.overwrite else "description_ai"
#         existing_desc = (data.get("description") or data.get("description_ai") or "").strip()
        
#         # Check if should skip
#         if args.skip_existing and data.get(target_field):
#             print(f"   ⏭️  Skipping (already has {target_field})")
#             return True
        
#         # Display info
#         print(f"\n📋 {doc_id}")
#         print(f"   📍 Title: {title}")
#         print(f"   🗺️  Location: {city}, {country}")
        
#         if existing_desc and not args.force:
#             print(f"   💬 Current: {existing_desc[:80]}...")
        
#         # Generate description
#         print(f"   🤖 Generating description...")
        
#         result = generate_description_with_ai(
#             title=title,
#             city=city,
#             country=country,
#             existing=existing_desc if args.use_existing_context else None,
#             lang=args.lang,
#             tone=args.tone,
#             max_chars=args.max_chars
#         )
        
#         # Display result
#         print(f"   ✨ Style: {result.style}")
#         print(f"   📝 Description ({len(result.description)} chars):")
#         print(f"      {result.description}")
#         if result.alt:
#             print(f"   📄 Alt ({len(result.alt)} chars): {result.alt}")
#         print(f"   📊 Confidence: {result.confidence:.2f}")
        
#         # Update Firestore
#         if not args.dry_run:
#             print(f"   💾 Updating Firestore ({target_field})...")
#             update_playlist_description(
#                 doc_id=doc_id,
#                 new_description=result.description,
#                 alt=result.alt,
#                 style=result.style,
#                 confidence=result.confidence,
#                 old_description=existing_desc,
#                 overwrite=args.overwrite,
#                 dry_run=args.dry_run
#             )
#             print(f"   ✅ Updated!")
        
#         return True
        
#     except Exception as e:
#         print(f"   ❌ Failed: {e}")
#         if args.verbose:
#             import traceback
#             traceback.print_exc()
#         return False


# def batch_process(args: argparse.Namespace):
#     """Main batch processing function"""
    
#     print("=" * 80)
#     print("📝 PLAYLIST DESCRIPTION GENERATOR v3.0")
#     print("=" * 80)
    
#     # Display mode
#     if args.dry_run:
#         print("⚠️  DRY RUN MODE - No changes will be made to Firestore")
#     if args.overwrite:
#         print("⚠️  OVERWRITE MODE - Will update 'description' field directly")
#     else:
#         print("ℹ️  SAFE MODE - Will write to 'description_ai' field")
#     if args.force:
#         print("🔄 FORCE MODE - Regenerating all descriptions (even existing ones)")
    
#     print()
    
#     # Determine document IDs
#     document_ids = None
    
#     if args.ids_file:
#         print(f"📄 Loading IDs from {args.ids_file}...")
#         try:
#             with open(args.ids_file, 'r', encoding='utf-8') as f:
#                 data = json.load(f)
#                 document_ids = data.get("document_ids", [])
#                 if not document_ids:
#                     print(f"❌ ERROR: 'document_ids' not found in file")
#                     return
#                 print(f"✅ Loaded {len(document_ids)} IDs")
#         except Exception as e:
#             print(f"❌ ERROR reading file: {e}")
#             return
#     elif args.ids:
#         document_ids = args.ids
#         print(f"📋 Processing {len(document_ids)} IDs from command line")
    
#     # Initialize Firebase
#     print("\n🔥 Initializing Firebase...")
#     init_firebase()
    
#     # Fetch playlists
#     print("🔍 Fetching playlists...")
#     playlists = fetch_playlists(
#         specific_ids=document_ids,
#         start_after=args.start_after,
#         limit=args.limit
#     )
    
#     if not playlists:
#         print("❌ No playlists found!")
#         return
    
#     total = len(playlists)
#     print(f"✅ Found {total} playlist(s) to process")
    
#     # Process playlists
#     print("\n" + "=" * 80)
#     print(f"🚀 STARTING PROCESSING ({total} playlists)")
#     print("=" * 80)
    
#     success_count = 0
#     skip_count = 0
#     fail_count = 0
#     start_time = time.time()
    
#     for idx, (doc_id, data) in enumerate(playlists, 1):
#         print(f"\n[{idx}/{total}] Processing {doc_id}...")
        
#         success = process_single_playlist(doc_id, data, args)
        
#         if success:
#             # Check if actually updated or skipped
#             target_field = "description" if args.overwrite else "description_ai"
#             if args.skip_existing and data.get(target_field):
#                 skip_count += 1
#             else:
#                 success_count += 1
#         else:
#             fail_count += 1
        
#         # Rate limiting between requests
#         if idx < total and args.sleep > 0:
#             time.sleep(args.sleep)
    
#     elapsed = time.time() - start_time
    
#     # Summary
#     print("\n" + "=" * 80)
#     print("🎉 PROCESSING COMPLETE!")
#     print("=" * 80)
#     print(f"✅ Successfully generated: {success_count}/{total}")
#     print(f"⏭️  Skipped (already exist): {skip_count}/{total}")
#     print(f"❌ Failed: {fail_count}/{total}")
#     print(f"⏱️  Total time: {elapsed/60:.1f} minutes")
#     print(f"📊 Average: {elapsed/total:.1f} seconds per playlist")
    
#     if args.dry_run:
#         print("\n💡 This was a DRY RUN. Remove --dry-run to actually update Firestore.")


# # ===================== CLI =====================

# TONES = ["friendly", "editorial", "playful", "luxury", "adventurous", "foodie", "cultural"]

# def main():
#     parser = argparse.ArgumentParser(
#         description="Generate engaging, Instagram-worthy descriptions for playlists using AI",
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#         epilog="""
# Examples:
#   # Test with 5 playlists
#   python %(prog)s --limit 5 --dry-run
  
#   # Process specific playlists
#   python %(prog)s --ids ABC123 DEF456 XYZ789
  
#   # Process from JSON file
#   python %(prog)s --ids-file my_playlists.json
  
#   # Process all playlists (use with caution!)
#   python %(prog)s --all --force
  
#   # Safe mode (writes to description_ai field)
#   python %(prog)s --all
  
#   # Overwrite main description field
#   python %(prog)s --ids ABC123 --overwrite
#         """
#     )
    
#     # Mode selection
#     mode = parser.add_mutually_exclusive_group(required=True)
#     mode.add_argument("--all", action="store_true",
#                      help="Process ALL playlists in collection")
#     mode.add_argument("--ids", nargs="+",
#                      help="Specific document IDs (space-separated)")
#     mode.add_argument("--ids-file", type=str,
#                      help='JSON file with {"document_ids": ["id1", "id2", ...]}')
    
#     # Filtering options
#     parser.add_argument("--start-after", type=str,
#                        help="Skip documents <= this ID (for resuming)")
#     parser.add_argument("--limit", type=int,
#                        help="Limit number of playlists to process (for testing)")
    
#     # Description options
#     parser.add_argument("--tone", choices=TONES, default="friendly",
#                        help="Tone/style for descriptions (default: friendly)")
#     parser.add_argument("--lang", default="en",
#                        help="Language code (default: en)")
#     parser.add_argument("--max-chars", type=int, default=DESCRIPTION_TARGET,
#                        help=f"Target character length (default: {DESCRIPTION_TARGET})")
    
#     # Behavior options
#     parser.add_argument("--skip-existing", action="store_true",
#                        help="Skip playlists that already have descriptions")
#     parser.add_argument("--force", action="store_true",
#                        help="Regenerate descriptions even if they exist")
#     parser.add_argument("--use-existing-context", action="store_true",
#                        help="Use existing description as context for AI")
#     parser.add_argument("--overwrite", action="store_true",
#                        help="Write to 'description' field (default: 'description_ai')")
    
#     # Testing options
#     parser.add_argument("--dry-run", action="store_true",
#                        help="Preview without updating Firestore")
#     parser.add_argument("--sleep", type=float, default=1.0,
#                        help="Seconds to sleep between requests (default: 1.0)")
#     parser.add_argument("--verbose", action="store_true",
#                        help="Show full error tracebacks")
    
#     args = parser.parse_args()
    
#     # Validation
#     if not OPENAI_API_KEY:
#         print("❌ ERROR: OPENAI_API_KEY not set!")
#         print("Set it with: export OPENAI_API_KEY='sk-...'  (or setx on Windows)")
#         return 1
    
#     if args.skip_existing and args.force:
#         print("❌ ERROR: Cannot use both --skip-existing and --force")
#         return 1
    
#     if args.all and args.limit:
#         print("⚠️  WARNING: Using --all with --limit will process limited playlists")
    
#     # Run processing
#     try:
#         batch_process(args)
#         return 0
#     except KeyboardInterrupt:
#         print("\n\n⚠️  Interrupted by user")
#         return 130
#     except Exception as e:
#         print(f"\n❌ FATAL ERROR: {e}")
#         if args.verbose:
#             import traceback
#             traceback.print_exc()
#         return 1


# if __name__ == "__main__":
#     exit(main())