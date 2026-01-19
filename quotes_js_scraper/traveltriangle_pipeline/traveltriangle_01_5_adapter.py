#!/usr/bin/env python3
"""
TravelTriangle Step 1.5 — Format Adapter
Converts tt_extracted.json to playlist_items.json format expected by 02_5_resolve_validate.py

Input:  tt_extracted.json (from traveltriangle_01_extract.py)
Output: playlist_items.json (for 02_5_resolve_validate.py)
"""

import json
import re
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional

# ==================== CONFIG ====================
DEFAULT_IN = "tt_extracted.json"
DEFAULT_OUT = "playlist_items.json"

# Category inference patterns
CATEGORY_HINTS_RX = {
    "beach": re.compile(r"\bbeach(es)?\b", re.I),
    "island": re.compile(r"\bisland(s)?\b", re.I),
    "waterfall": re.compile(r"\bwaterfall(s)?|falls\b", re.I),
    "national_park": re.compile(r"\bnational\s+park|tiger\s+reserve|sanctuary\b", re.I),
    "trek": re.compile(r"\btrek(s)?|trail(s)?|pass\b", re.I),
    "lake": re.compile(r"\blake\b", re.I),
    "fort": re.compile(r"\bfort\b", re.I),
    "temple": re.compile(r"\btemple\b", re.I),
    "hill_station": re.compile(r"\bhill\s*station\b", re.I),
    "park": re.compile(r"\bpark\b", re.I),
    "city": re.compile(r"\bcity\b", re.I),
    "town": re.compile(r"\btown\b", re.I),
    "resort": re.compile(r"\b(resort|hotel|homestay|villa|lodge|camp|hostel)\b", re.I),
}

CATEGORY_TO_SCOPE = {
    # Natural
    "waterfall": "natural", "beach": "natural", "island": "natural", 
    "lake": "natural", "peak": "natural", "mountain": "natural",
    "national_park": "natural", "park": "natural", "sanctuary": "natural", 
    "reserve": "natural", "valley": "natural", "cave": "natural", 
    "trek": "natural", "trail": "natural",
    
    # Destination
    "city": "destination", "town": "destination", "district": "destination", 
    "region": "destination", "state": "destination", "hill_station": "destination",
    
    # POI
    "temple": "poi", "fort": "poi", "monument": "poi", "museum": "poi", 
    "zoo": "poi", "church": "poi", "mosque": "poi", "synagogue": "poi",
    "palace": "poi", "viewpoint": "poi", "dam": "poi", "bridge": "poi", 
    "garden": "poi", "market": "poi", "street": "poi", "neighborhood": "poi",
    "resort": "poi", "hotel": "poi", "camp": "poi", "homestay": "poi", 
    "villa": "poi", "lodge": "poi", "hostel": "poi"
}

# ==================== HELPERS ====================
def infer_category_heuristic(name: str, description: str, location_hint: str = "") -> Optional[str]:
    """Infer category from text patterns."""
    blob = " ".join([name or "", description or "", location_hint or ""])
    
    for cat, rx in CATEGORY_HINTS_RX.items():
        if rx.search(blob):
            return cat
    
    return None

def scope_from_category(cat: Optional[str]) -> str:
    """Get scope from category, default to 'poi'."""
    if not cat:
        return "poi"
    return CATEGORY_TO_SCOPE.get(cat.lower(), "poi")

def build_query_hints(name: str, anchor_city: str, location_hint: str = "") -> List[str]:
    """Build search query variations."""
    hints = {name}
    
    if location_hint:
        hints.add(f"{name} {location_hint}")
    
    if anchor_city and anchor_city.lower() not in name.lower():
        hints.add(f"{name} {anchor_city}")
    
    return sorted(hints)

def section_excerpt(text: str, max_chars: int = 400) -> str:
    """Create excerpt from description."""
    text = (text or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "…"

# ==================== CONVERSION ====================
def convert_tt_to_playlist_items(tt_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert TravelTriangle extraction format to playlist_items format.
    """
    
    title = tt_data.get("playlistTitle", "Untitled")
    anchor_city = tt_data.get("placeName", "")
    subtype = tt_data.get("subtype", "poi")
    source_urls = tt_data.get("source_urls", [])
    primary_url = source_urls[0] if source_urls else ""
    
    # --- FIX: Capture Description ---
    # Check both keys as Step 1 might use either depending on version
    description = tt_data.get("description") or tt_data.get("playlist_description") or ""
    # --------------------------------

    converted_items = []
    
    for item in tt_data.get("items", []):
        name = item.get("name", "").strip()
        if not name:
            continue
        
        item_desc = item.get("description", "")
        location_hint = item.get("location_hint", "")
        
        # Infer category from name + description + location
        category_hint = item.get("category_hint") or infer_category_heuristic(
            name, item_desc, location_hint
        )
        
        # Determine scope (use from item, or infer from subtype/category)
        scope = item.get("scope") or subtype
        if not scope or scope not in {"destination", "poi", "natural"}:
            scope = scope_from_category(category_hint) if category_hint else "poi"
        
        # Build query hints
        query_hints = build_query_hints(name, anchor_city, location_hint)
        
        # Extract price from entry_fee_text if available
        price = ""
        entry_fee = item.get("entry_fee_text", "")
        if entry_fee and "free" not in entry_fee.lower():
            price = entry_fee
        
        converted_item = {
            "name": name,
            "section_title": name,
            "section_excerpt": section_excerpt(item_desc),
            "description": item_desc,
            "travel_time": "",  # TravelTriangle doesn't provide this
            "price": price,
            "location_hint": location_hint or anchor_city,
            "category_hint": category_hint,
            "scope": scope,
            "query_hints": query_hints,
            "anchor_city": anchor_city,
            "source_title": title,
            "source_url": primary_url,
            "votes": 1,
            # Preserve TravelTriangle-specific data
            "opening_hours_text": item.get("opening_hours_text", ""),
            "entry_fee_text": item.get("entry_fee_text", "")
        }
        
        converted_items.append(converted_item)
    
    # Return in playlist_items.json format (array of playlists)
    return {
        "playlistTitle": title,
        "placeName": anchor_city,
        "subtype": subtype,
        "source_urls": source_urls,
        "description": description,  # <--- FIX: Pass description to output
        "items": converted_items
    }

# ==================== MAIN ====================
def main():
    parser = argparse.ArgumentParser(
        description="Convert TravelTriangle extraction to playlist_items format"
    )
    parser.add_argument(
        "--in", 
        dest="input_file",
        default=DEFAULT_IN,
        help=f"Input file from traveltriangle_01_extract.py (default: {DEFAULT_IN})"
    )
    parser.add_argument(
        "--out",
        dest="output_file", 
        default=DEFAULT_OUT,
        help=f"Output file for 02_5_resolve_validate.py (default: {DEFAULT_OUT})"
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Process multiple input files (input becomes glob pattern)"
    )
    
    args = parser.parse_args()
    
    input_path = Path(args.input_file)
    output_path = Path(args.output_file)
    
    # Single file mode
    if not args.batch:
        if not input_path.exists():
            print(f"❌ Input file not found: {input_path}")
            return 1
        
        print(f"📥 Reading: {input_path}")
        tt_data = json.loads(input_path.read_text(encoding="utf-8"))
        
        # Handle both single object and array formats
        if isinstance(tt_data, dict):
            converted = convert_tt_to_playlist_items(tt_data)
            output = [converted]  # Wrap in array
        elif isinstance(tt_data, list):
            output = [convert_tt_to_playlist_items(item) for item in tt_data]
        else:
            print(f"❌ Invalid input format (expected dict or array)")
            return 1
        
        print(f"✅ Converted {len(output)} playlist(s)")
        print(f"📤 Writing: {output_path}")
        
        output_path.write_text(
            json.dumps(output, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        
        # Print summary
        total_items = sum(len(p["items"]) for p in output)
        print(f"\n📊 Summary:")
        print(f"   Playlists: {len(output)}")
        print(f"   Total items: {total_items}")
        
        for playlist in output:
            print(f"\n   📁 {playlist['playlistTitle']}")
            print(f"      City: {playlist['placeName']}")
            print(f"      Subtype: {playlist['subtype']}")
            print(f"      Items: {len(playlist['items'])}")
            # Verify description presence in log
            desc_preview = (playlist.get('description') or "")[:50]
            print(f"      Description: {desc_preview}...")
            
            # Category breakdown
            categories = {}
            for item in playlist["items"]:
                cat = item.get("category_hint") or "unknown"
                categories[cat] = categories.get(cat, 0) + 1
            
            print(f"      Categories: {dict(categories)}")
    
    # Batch mode (multiple files)
    else:
        import glob
        pattern = args.input_file
        files = glob.glob(pattern)
        
        if not files:
            print(f"❌ No files found matching pattern: {pattern}")
            return 1
        
        print(f"📥 Found {len(files)} files matching pattern: {pattern}")
        
        all_playlists = []
        
        for file_path in sorted(files):
            print(f"\n   Processing: {file_path}")
            try:
                tt_data = json.loads(Path(file_path).read_text(encoding="utf-8"))
                
                if isinstance(tt_data, dict):
                    converted = convert_tt_to_playlist_items(tt_data)
                    all_playlists.append(converted)
                elif isinstance(tt_data, list):
                    all_playlists.extend([convert_tt_to_playlist_items(item) for item in tt_data])
                
                print(f"   ✅ Converted")
            except Exception as e:
                print(f"   ❌ Failed: {e}")
        
        print(f"\n📤 Writing {len(all_playlists)} playlists to: {output_path}")
        output_path.write_text(
            json.dumps(all_playlists, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        
        total_items = sum(len(p["items"]) for p in all_playlists)
        print(f"\n✅ Done!")
        print(f"   Playlists: {len(all_playlists)}")
        print(f"   Total items: {total_items}")
    
    return 0

if __name__ == "__main__":
    exit(main())


# #!/usr/bin/env python3
# """
# TravelTriangle Step 1.5 — Format Adapter
# Converts tt_extracted.json to playlist_items.json format expected by 02_5_resolve_validate.py

# Input:  tt_extracted.json (from traveltriangle_01_extract.py)
# Output: playlist_items.json (for 02_5_resolve_validate.py)
# """

# import json
# import re
# import argparse
# from pathlib import Path
# from typing import Dict, Any, List, Optional

# # ==================== CONFIG ====================
# DEFAULT_IN = "tt_extracted.json"
# DEFAULT_OUT = "playlist_items.json"

# # Category inference patterns (same as 02_extract_items.py)
# CATEGORY_HINTS_RX = {
#     "beach": re.compile(r"\bbeach(es)?\b", re.I),
#     "island": re.compile(r"\bisland(s)?\b", re.I),
#     "waterfall": re.compile(r"\bwaterfall(s)?|falls\b", re.I),
#     "national_park": re.compile(r"\bnational\s+park|tiger\s+reserve|sanctuary\b", re.I),
#     "trek": re.compile(r"\btrek(s)?|trail(s)?|pass\b", re.I),
#     "lake": re.compile(r"\blake\b", re.I),
#     "fort": re.compile(r"\bfort\b", re.I),
#     "temple": re.compile(r"\btemple\b", re.I),
#     "hill_station": re.compile(r"\bhill\s*station\b", re.I),
#     "park": re.compile(r"\bpark\b", re.I),
#     "city": re.compile(r"\bcity\b", re.I),
#     "town": re.compile(r"\btown\b", re.I),
#     "resort": re.compile(r"\b(resort|hotel|homestay|villa|lodge|camp|hostel)\b", re.I),
# }

# CATEGORY_TO_SCOPE = {
#     # Natural
#     "waterfall": "natural", "beach": "natural", "island": "natural", 
#     "lake": "natural", "peak": "natural", "mountain": "natural",
#     "national_park": "natural", "park": "natural", "sanctuary": "natural", 
#     "reserve": "natural", "valley": "natural", "cave": "natural", 
#     "trek": "natural", "trail": "natural",
    
#     # Destination
#     "city": "destination", "town": "destination", "district": "destination", 
#     "region": "destination", "state": "destination", "hill_station": "destination",
    
#     # POI
#     "temple": "poi", "fort": "poi", "monument": "poi", "museum": "poi", 
#     "zoo": "poi", "church": "poi", "mosque": "poi", "synagogue": "poi",
#     "palace": "poi", "viewpoint": "poi", "dam": "poi", "bridge": "poi", 
#     "garden": "poi", "market": "poi", "street": "poi", "neighborhood": "poi",
#     "resort": "poi", "hotel": "poi", "camp": "poi", "homestay": "poi", 
#     "villa": "poi", "lodge": "poi", "hostel": "poi"
# }

# # ==================== HELPERS ====================
# def infer_category_heuristic(name: str, description: str, location_hint: str = "") -> Optional[str]:
#     """Infer category from text patterns."""
#     blob = " ".join([name or "", description or "", location_hint or ""])
    
#     for cat, rx in CATEGORY_HINTS_RX.items():
#         if rx.search(blob):
#             return cat
    
#     return None

# def scope_from_category(cat: Optional[str]) -> str:
#     """Get scope from category, default to 'poi'."""
#     if not cat:
#         return "poi"
#     return CATEGORY_TO_SCOPE.get(cat.lower(), "poi")

# def build_query_hints(name: str, anchor_city: str, location_hint: str = "") -> List[str]:
#     """Build search query variations."""
#     hints = {name}
    
#     if location_hint:
#         hints.add(f"{name} {location_hint}")
    
#     if anchor_city and anchor_city.lower() not in name.lower():
#         hints.add(f"{name} {anchor_city}")
    
#     return sorted(hints)

# def section_excerpt(text: str, max_chars: int = 400) -> str:
#     """Create excerpt from description."""
#     text = (text or "").strip()
#     if len(text) <= max_chars:
#         return text
#     return text[:max_chars] + "…"

# # ==================== CONVERSION ====================
# def convert_tt_to_playlist_items(tt_data: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Convert TravelTriangle extraction format to playlist_items format.
    
#     TravelTriangle format:
#     {
#       "playlistTitle": "...",
#       "placeName": "Karnataka",
#       "country": "India",
#       "subtype": "poi",
#       "source_urls": ["..."],
#       "description": "...",
#       "items": [
#         {
#           "name": "...",
#           "description": "...",
#           "location_hint": "...",
#           "opening_hours_text": "...",
#           "entry_fee_text": "..."
#         }
#       ]
#     }
    
#     playlist_items format:
#     {
#       "playlistTitle": "...",
#       "placeName": "Karnataka",
#       "subtype": "poi",
#       "source_urls": ["..."],
#       "items": [
#         {
#           "name": "...",
#           "section_title": "...",
#           "section_excerpt": "...",
#           "description": "...",
#           "travel_time": "",
#           "price": "",
#           "location_hint": "...",
#           "category_hint": "waterfall",
#           "scope": "natural",
#           "query_hints": ["..."],
#           "anchor_city": "Karnataka",
#           "source_title": "...",
#           "source_url": "...",
#           "votes": 1
#         }
#       ]
#     }
#     """
    
#     title = tt_data.get("playlistTitle", "Untitled")
#     anchor_city = tt_data.get("placeName", "")
#     subtype = tt_data.get("subtype", "poi")
#     source_urls = tt_data.get("source_urls", [])
#     primary_url = source_urls[0] if source_urls else ""
    
#     converted_items = []
    
#     for item in tt_data.get("items", []):
#         name = item.get("name", "").strip()
#         if not name:
#             continue
        
#         description = item.get("description", "")
#         location_hint = item.get("location_hint", "")
        
#         # Infer category from name + description + location
#         category_hint = item.get("category_hint") or infer_category_heuristic(
#             name, description, location_hint
#         )
        
#         # Determine scope (use from item, or infer from subtype/category)
#         scope = item.get("scope") or subtype
#         if not scope or scope not in {"destination", "poi", "natural"}:
#             scope = scope_from_category(category_hint) if category_hint else "poi"
        
#         # Build query hints
#         query_hints = build_query_hints(name, anchor_city, location_hint)
        
#         # Extract price from entry_fee_text if available
#         price = ""
#         entry_fee = item.get("entry_fee_text", "")
#         if entry_fee and "free" not in entry_fee.lower():
#             price = entry_fee
        
#         converted_item = {
#             "name": name,
#             "section_title": name,
#             "section_excerpt": section_excerpt(description),
#             "description": description,
#             "travel_time": "",  # TravelTriangle doesn't provide this
#             "price": price,
#             "location_hint": location_hint or anchor_city,
#             "category_hint": category_hint,
#             "scope": scope,
#             "query_hints": query_hints,
#             "anchor_city": anchor_city,
#             "source_title": title,
#             "source_url": primary_url,
#             "votes": 1,
#             # Preserve TravelTriangle-specific data
#             "opening_hours_text": item.get("opening_hours_text", ""),
#             "entry_fee_text": item.get("entry_fee_text", "")
#         }
        
#         converted_items.append(converted_item)
    
#     # Return in playlist_items.json format (array of playlists)
#     return {
#         "playlistTitle": title,
#         "placeName": anchor_city,
#         "subtype": subtype,
#         "source_urls": source_urls,
#         "items": converted_items
#     }

# # ==================== MAIN ====================
# def main():
#     parser = argparse.ArgumentParser(
#         description="Convert TravelTriangle extraction to playlist_items format"
#     )
#     parser.add_argument(
#         "--in", 
#         dest="input_file",
#         default=DEFAULT_IN,
#         help=f"Input file from traveltriangle_01_extract.py (default: {DEFAULT_IN})"
#     )
#     parser.add_argument(
#         "--out",
#         dest="output_file", 
#         default=DEFAULT_OUT,
#         help=f"Output file for 02_5_resolve_validate.py (default: {DEFAULT_OUT})"
#     )
#     parser.add_argument(
#         "--batch",
#         action="store_true",
#         help="Process multiple input files (input becomes glob pattern)"
#     )
    
#     args = parser.parse_args()
    
#     input_path = Path(args.input_file)
#     output_path = Path(args.output_file)
    
#     # Single file mode
#     if not args.batch:
#         if not input_path.exists():
#             print(f"❌ Input file not found: {input_path}")
#             return 1
        
#         print(f"📥 Reading: {input_path}")
#         tt_data = json.loads(input_path.read_text(encoding="utf-8"))
        
#         # Handle both single object and array formats
#         if isinstance(tt_data, dict):
#             converted = convert_tt_to_playlist_items(tt_data)
#             output = [converted]  # Wrap in array
#         elif isinstance(tt_data, list):
#             output = [convert_tt_to_playlist_items(item) for item in tt_data]
#         else:
#             print(f"❌ Invalid input format (expected dict or array)")
#             return 1
        
#         print(f"✅ Converted {len(output)} playlist(s)")
#         print(f"📤 Writing: {output_path}")
        
#         output_path.write_text(
#             json.dumps(output, ensure_ascii=False, indent=2),
#             encoding="utf-8"
#         )
        
#         # Print summary
#         total_items = sum(len(p["items"]) for p in output)
#         print(f"\n📊 Summary:")
#         print(f"   Playlists: {len(output)}")
#         print(f"   Total items: {total_items}")
        
#         for playlist in output:
#             print(f"\n   📁 {playlist['playlistTitle']}")
#             print(f"      City: {playlist['placeName']}")
#             print(f"      Subtype: {playlist['subtype']}")
#             print(f"      Items: {len(playlist['items'])}")
            
#             # Category breakdown
#             categories = {}
#             for item in playlist["items"]:
#                 cat = item.get("category_hint") or "unknown"
#                 categories[cat] = categories.get(cat, 0) + 1
            
#             print(f"      Categories: {dict(categories)}")
    
#     # Batch mode (multiple files)
#     else:
#         import glob
#         pattern = args.input_file
#         files = glob.glob(pattern)
        
#         if not files:
#             print(f"❌ No files found matching pattern: {pattern}")
#             return 1
        
#         print(f"📥 Found {len(files)} files matching pattern: {pattern}")
        
#         all_playlists = []
        
#         for file_path in sorted(files):
#             print(f"\n   Processing: {file_path}")
#             try:
#                 tt_data = json.loads(Path(file_path).read_text(encoding="utf-8"))
                
#                 if isinstance(tt_data, dict):
#                     converted = convert_tt_to_playlist_items(tt_data)
#                     all_playlists.append(converted)
#                 elif isinstance(tt_data, list):
#                     all_playlists.extend([convert_tt_to_playlist_items(item) for item in tt_data])
                
#                 print(f"   ✅ Converted")
#             except Exception as e:
#                 print(f"   ❌ Failed: {e}")
        
#         print(f"\n📤 Writing {len(all_playlists)} playlists to: {output_path}")
#         output_path.write_text(
#             json.dumps(all_playlists, ensure_ascii=False, indent=2),
#             encoding="utf-8"
#         )
        
#         total_items = sum(len(p["items"]) for p in all_playlists)
#         print(f"\n✅ Done!")
#         print(f"   Playlists: {len(all_playlists)}")
#         print(f"   Total items: {total_items}")
    
#     return 0

# if __name__ == "__main__":
#     exit(main())