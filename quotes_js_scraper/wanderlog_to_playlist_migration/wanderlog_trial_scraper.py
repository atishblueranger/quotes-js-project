# Cachin logic added
import os
import re
import json
import time
import math
import argparse
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import scrapy
from scrapy.crawler import CrawlerProcess

# --- .env (next to this script) ---
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
except Exception:
    pass

# --- slugify with fallback ---
try:
    from slugify import slugify  # pip install python-slugify
except Exception:
    import re as _re
    def slugify(s: str) -> str:
        s = (s or "").lower().strip()
        s = _re.sub(r"[^\w\s-]", "", s)
        s = _re.sub(r"[\s_-]+", "-", s)
        return _re.sub(r"^-+|-+$", "", s)

# --- Optional Selenium (only if you turn it on) ---
try:
    from scrapy_selenium import SeleniumRequest
except Exception:
    SeleniumRequest = None

# --- Model selection ---
OPENAI_MODEL = os.getenv("LC_MODEL", "gpt-4o-mini")
SCRIPT_VERSION = "0.6.0"

# ---------- Cache Management ----------
class CacheManager:
    """Manages various caches for API calls and expensive operations"""
    
    def __init__(self, cache_dir: Path):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize caches
        self.llm_validation_cache = self.load_cache("llm_validation_cache.json")
        self.llm_title_cache = self.load_cache("llm_title_cache.json") 
        self.google_places_cache = self.load_cache("google_places_cache.json")
        self.mobx_data_cache = self.load_cache("mobx_data_cache.json")  # Cache scraped data
        
        # Track API call stats
        self.api_stats = {
            "llm_validation_calls": 0,
            "llm_title_calls": 0,
            "google_places_calls": 0,
            "cache_hits": 0,
            "cache_misses": 0
        }
    
    def load_cache(self, filename: str) -> Dict:
        """Load cache from JSON file"""
        cache_file = self.cache_dir / filename
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load cache {filename}: {e}")
                return {}
        return {}
    
    def save_cache(self, cache_data: Dict, filename: str):
        """Save cache to JSON file"""
        cache_file = self.cache_dir / filename
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Warning: Failed to save cache {filename}: {e}")
    
    def save_all_caches(self):
        """Save all caches to disk"""
        self.save_cache(self.llm_validation_cache, "llm_validation_cache.json")
        self.save_cache(self.llm_title_cache, "llm_title_cache.json")
        self.save_cache(self.google_places_cache, "google_places_cache.json")
        self.save_cache(self.mobx_data_cache, "mobx_data_cache.json")
    
    def get_cache_key(self, **kwargs) -> str:
        """Generate a cache key from keyword arguments"""
        key_data = json.dumps(kwargs, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(key_data.encode('utf-8')).hexdigest()
    
    def get_url_cache_key(self, url: str) -> str:
        """Generate cache key for URL-based data"""
        return hashlib.md5(url.encode('utf-8')).hexdigest()

# ---------- Category patterns ----------
CATEGORY_PATTERNS = {
    "beaches": {
        "pos": r"\b(beach|sea\s*face|seaface|seafront|shore|coast|bay|chowpatty|sand|sands)\b",
        "neg": r"\b(temple|mandir|church|mosque|museum|mall|market|fort|palace|playground|bank|school|hospital|crocodile|tower|bridge|station|cinema|theatre|theater|atm|office|court|college|university|monument)\b",
    },
    "national parks": {
        "pos": r"\b(national\s+park|wildlife\s+sanctuary|tiger\s+reserve|biosphere\s+reserve|safari|national\s+forest|conservation\s+area)\b",
        "neg": r"\b(mall|market|museum|temple|church|mosque|fort|palace|beach|cinema|theatre|theater|atm|office|court|school|college|university)\b",
    },
    "waterfalls": {
        "pos": r"\b(waterfall|falls|cascade|cascades)\b",
        "neg": r"\b(mall|market|museum|temple|church|mosque|fort|palace|cinema|theatre|theater)\b",
    },
    "castles": {
        "pos": r"\b(castle|fortress|citadel)\b",
        "neg": r"\b(mall|market|museum|temple|church|mosque|beach|cinema|theatre|theater)\b",
    },
    "photo spots": {
        "pos": r"\b(viewpoint|view\s*point|lookout|photo\s*spot|sunset\s*point|sunrise\s*point|scenic|panorama|photograph|photogenic)\b",
        "neg": r"\b(atm|office|court|bank)\b",
    },
    "romantic places": {
        "pos": r"\b(honeymoon|romantic|couple|love|sunset\s*point|secluded|candlelight)\b",
        "neg": r"\b(atm|office|court|bank)\b",
    },
    "architecture": {
        "pos": r"\b(architecture|architectural|heritage|historic|monument|cathedral|basilica|temple|mosque|church|fort|palace|colonial)\b",
        "neg": r"\b(beach|waterfall)\b",
    },
}

def detect_category_from_title(page_title: str) -> str:
    t = (page_title or "").lower()
    if "beach" in t: return "beaches"
    if "national park" in t or "wildlife" in t or "reserve" in t: return "national parks"
    if "waterfall" in t: return "waterfalls"
    if "castle" in t or "fortress" in t or "fort " in t: return "castles"
    if "photo spot" in t or "photo" in t: return "photo spots"
    if "romantic" in t: return "romantic places"
    if "architecture" in t: return "architecture"
    return "beaches"

# ---------- Utilities ----------
def iso_to_epoch_seconds(iso_str: str) -> Optional[int]:
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None

def clean_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    return text.replace("\u2019", "'").replace("\u2014", "-").strip()

def default_description(title: str) -> str:
    return (f'Dive into "{title}" — a handpicked list of places with quick notes, links, '
            f'and essentials for fast trip planning and discovery.')

def build_slug(title: str, city: str, subtype: str, url: str) -> str:
    m = re.search(r"/(\d+)(?:$|[?#])", url) or re.search(r"/(\d+)$", url)
    tid = m.group(1) if m else "list"
    return f"{slugify(title)}-{slugify(city)}-{slugify(subtype)}-{tid}"

def explain_hits(pat: re.Pattern, text: str) -> List[str]:
    try:
        hits = pat.findall(text)
        if not hits:
            return []
        if isinstance(hits[0], tuple):
            hits = [h for tup in hits for h in tup if h]
        uniq = sorted({h.lower() for h in hits if isinstance(h, str) and h.strip()})
        return uniq
    except Exception:
        return []

# ---------- LLM context (uses generalDescription) ----------
def build_llm_context(it: Dict[str, Any], category: str, max_len: int = 900) -> str:
    parts: List[str] = []
    if it.get("generalDescription"):
        parts.append(f"General: {clean_text(it['generalDescription'])}")
    if it.get("block_desc"):
        parts.append(f"Board note: {clean_text(it['block_desc'])}")
    cats = it.get("categories") or []
    if cats:
        parts.append("Tags: " + ", ".join([str(c) for c in cats][:6]))
    for r in (it.get("raw_reviews") or [])[:2]:
        txt = clean_text(r.get("reviewText"))
        if txt:
            parts.append(f"Review: {txt}")
    parts.insert(0, f"Target category: {category}")
    ctx = " ".join(parts).strip()
    if len(ctx) > max_len:
        ctx = ctx[:max_len].rstrip() + "…"
    return ctx

# ---------- Reviews mapping (alternate picks) ----------
def pick_alternate_reviews(reviews: List[Dict[str, Any]], k: int = 3) -> List[Dict[str, Any]]:
    if not reviews:
        return []
    def _key(r):
        rating = r.get("rating", 0)
        ts = iso_to_epoch_seconds(r.get("time") or "") or 0
        return (rating, ts)
    sorted_reviews = sorted(reviews, key=_key, reverse=True)
    picked = []
    for i, r in enumerate(sorted_reviews):
        if len(picked) >= k:
            break
        if i % 2 == 0:
            picked.append({
                "rating": int(r.get("rating", 0)),
                "text": clean_text(r.get("reviewText")),
                "author_name": r.get("reviewerName") or "",
                "relative_time_description": "",
                "time": iso_to_epoch_seconds(r.get("time")) or 0,
                "profile_photo_url": ""
            })
    return picked

# ---------- Heuristic + LLM validation ----------
def heuristic_is_category(category: str, name: str, desc: str, cats: List[str], page_title: str)\
        -> Tuple[Optional[bool], str, Dict[str, Any]]:

    patt = CATEGORY_PATTERNS.get(category, CATEGORY_PATTERNS["beaches"])
    pos = re.compile(patt["pos"], re.IGNORECASE)
    neg = re.compile(patt["neg"], re.IGNORECASE)

    blob = " ".join([name or "", desc or "", " ".join(cats or []), page_title or ""])
    pos_hits = explain_hits(pos, blob)
    neg_hits = explain_hits(neg, blob)

    if neg_hits and not pos_hits:
        return False, "heuristic_neg", {"pos_hits": pos_hits, "neg_hits": neg_hits}
    if pos_hits and not neg_hits:
        return True, "heuristic_pos", {"pos_hits": pos_hits, "neg_hits": neg_hits}
    return None, "heuristic_uncertain", {"pos_hits": pos_hits, "neg_hits": neg_hits}

def llm_validate(cache_manager: CacheManager, category: str, name: str, context: str, city: str, page_title: str) -> Optional[bool]:
    """
    Return True/False/None (None on failure). Uses caching to avoid repeated API calls.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    # Create cache key
    cache_key = cache_manager.get_cache_key(
        category=category,
        name=name,
        context=context[:500],  # Limit context length for cache key
        city=city,
        page_title=page_title
    )
    
    # Check cache first
    if cache_key in cache_manager.llm_validation_cache:
        cache_manager.api_stats["cache_hits"] += 1
        cached_result = cache_manager.llm_validation_cache[cache_key]
        if cached_result == "true":
            return True
        elif cached_result == "false":
            return False
        else:
            return None
    
    cache_manager.api_stats["cache_misses"] += 1
    cache_manager.api_stats["llm_validation_calls"] += 1

    prompt = f"""You are validating inclusion for a travel list titled "{page_title}".
City/Region: {city}
Place: "{name}"
Decision category: {category}

Context:
{context}

Question: Does this place CLEARLY belong in the "{category}" category?
Answer with only one token: YES or NO."""

    result = None
    
    # Try new SDK (openai>=1.0)
    try:
        from openai import OpenAI
        client = OpenAI()
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        out = (resp.choices[0].message.content or "").strip().lower()
        if out.startswith("yes"): 
            result = True
        elif out.startswith("no"):  
            result = False
    except Exception:
        pass

    # Fallback to old SDK (openai<1.0)
    if result is None:
        try:
            import openai as _openai
            _openai.api_key = api_key
            resp = _openai.ChatCompletion.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
            )
            out = resp.choices[0].message["content"].strip().lower()
            if out.startswith("yes"): 
                result = True
            elif out.startswith("no"):  
                result = False
        except Exception:
            result = None
    
    # Cache the result
    if result is True:
        cache_manager.llm_validation_cache[cache_key] = "true"
    elif result is False:
        cache_manager.llm_validation_cache[cache_key] = "false"
    else:
        cache_manager.llm_validation_cache[cache_key] = "none"
    
    return result

def generate_playlist_title(cache_manager: CacheManager, city: str, category: str, sample_names: List[str], page_title: str) -> str:
    """Generate playlist title with caching and improved logic from Script 2"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return create_fallback_title(city, category)
    
    # Create cache key
    cache_key = cache_manager.get_cache_key(
        city=city,
        category=category,
        sample_names=sample_names[:3],  # Only use first 3 names
        page_title=page_title[:100]  # Limit page title for cache key
    )
    
    # Check cache first
    if cache_key in cache_manager.llm_title_cache:
        cache_manager.api_stats["cache_hits"] += 1
        return cache_manager.llm_title_cache[cache_key]
    
    cache_manager.api_stats["cache_misses"] += 1
    cache_manager.api_stats["llm_title_calls"] += 1
    
    # Enhanced prompt that excludes numbers and provides better context
    prompt = f"""Create an engaging travel playlist title for this curated collection.

Context:
- City/Region: {city}
- Category: {category}
- Original page title: "{page_title}"
- Featured places: {", ".join(sample_names[:3])}

Requirements:
- 4-8 words maximum
- Travel and discovery focused
- Engaging and memorable
- NO numbers or counts (avoid "5 Best", "Top 10", etc.)
- Capture the essence of {category} in {city}

Examples of good titles:
- "Hidden Beaches of Mumbai"  
- "Ancient Castles of Rajasthan"
- "Mystical Waterfalls Near Goa"
- "Romantic Sunsets in Kerala"

Return only the title without quotes:"""

    result = create_fallback_title(city, category)  # Default fallback
    
    # Try new SDK (openai>=1.0)
    try:
        from openai import OpenAI
        client = OpenAI()
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,  # Slightly higher for more creativity
        )
        generated_title = (resp.choices[0].message.content or "").strip().strip('"\'')
        
        # Validate the generated title
        if is_valid_title(generated_title, city, category):
            result = generated_title
    except Exception:
        pass

    # Fallback to old SDK (openai<1.0)
    if result == create_fallback_title(city, category):  # If still default
        try:
            import openai as _openai
            _openai.api_key = api_key
            resp = _openai.ChatCompletion.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.4,
            )
            generated_title = resp.choices[0].message["content"].strip().strip('"\'')
            
            # Validate the generated title
            if is_valid_title(generated_title, city, category):
                result = generated_title
        except Exception:
            pass
    
    # Cache the result
    cache_manager.llm_title_cache[cache_key] = result
    return result

def create_fallback_title(city: str, category: str) -> str:
    """Create a fallback title without numbers"""
    category_map = {
        "beaches": f"Beautiful Beaches of {city}",
        "national parks": f"Wild {city} Parks",
        "waterfalls": f"Stunning {city} Waterfalls", 
        "castles": f"Historic {city} Castles",
        "photo spots": f"Picture Perfect {city}",
        "romantic places": f"Romantic {city} Escapes",
        "architecture": f"Architectural Gems of {city}"
    }
    return category_map.get(category.lower(), f"Discover {city}")

def is_valid_title(title: str, city: str, category: str) -> bool:
    """Validate generated title meets our criteria"""
    if not title or len(title.strip()) < 3:
        return False
    
    title = title.strip()
    
    # Check length (roughly 4-8 words, allowing some flexibility)
    word_count = len(title.split())
    if word_count < 3 or word_count > 10:
        return False
    
    # Check for numbers (reject titles with digits)
    if re.search(r'\d', title):
        return False
    
    # Check for undesirable patterns
    undesirable_patterns = [
        r'\b(top|best|\d+|#|number|first|second|third)\b',  # Ranking terms
        r'\b(guide|list|collection)\b',  # Generic list terms  
        r'^(the\s+)?(ultimate|complete|definitive)\b',  # Clickbait terms
    ]
    
    for pattern in undesirable_patterns:
        if re.search(pattern, title.lower()):
            return False
    
    # Should contain city name or category reference (flexible check)
    contains_location = city.lower() in title.lower()
    contains_category = any(cat_word in title.lower() 
                          for cat_word in category.lower().split())
    
    if not (contains_location or contains_category):
        return False
    
    # Reasonable character length
    if len(title) < 10 or len(title) > 80:
        return False
    
    return True

# ---------- Merge MOBX sources ----------
def merge_metadata_and_blocks(place_meta: List[Dict[str, Any]],
                              blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    meta_map: Dict[str, Dict[str, Any]] = {}
    for p in place_meta or []:
        pid = str(p.get("placeId") or p.get("id") or "")
        if not pid:
            pid = f"NAME::{(p.get('name') or '').strip()}"
        meta_map[pid] = {
            "placeId": p.get("placeId") or p.get("id"),
            "name": p.get("name"),
            "rating": p.get("rating"),
            "numRatings": p.get("numRatings"),
            "priceLevel": p.get("priceLevel"),
            "openingPeriods": p.get("openingPeriods") or [],
            "internationalPhoneNumber": p.get("internationalPhoneNumber"),
            "address": p.get("address"),
            "utcOffset": p.get("utcOffset"),
            "categories": p.get("categories") or [],
            "generalDescription": p.get("generatedDescription") or p.get("description"),
            "raw_reviews": p.get("reviews") or [],
            "ratingDistribution": p.get("ratingDistribution") or {},
            "permanentlyClosed": bool(p.get("permanentlyClosed")),
            "imageKeys": p.get("imageKeys") or [],
            "sources": p.get("sources") or [],
            "minMinutesSpent": p.get("minMinutesSpent"),
            "maxMinutesSpent": p.get("maxMinutesSpent"),
            "website": p.get("website"),
        }
    for b in blocks or []:
        if b.get("type") != "place":
            continue
        place = b.get("place") or {}
        pid = str(place.get("placeId") or "")
        key = pid if pid else f"NAME::{place.get('name') or ''}"
        if key not in meta_map:
            meta_map[key] = {"placeId": pid or None, "name": place.get("name")}
        text_ops = ((b.get("text") or {}).get("ops") or [])
        addendum = "".join([t.get("insert", "") for t in text_ops if isinstance(t, dict)])
        meta_map[key].update({
            "latitude": place.get("latitude"),
            "longitude": place.get("longitude"),
            "block_id": b.get("id"),
            "block_desc": addendum.strip() or None,
            "block_imageKeys": b.get("imageKeys") or [],
            "selectedImageKey": b.get("selectedImageKey"),
        })
    merged = list(meta_map.values())
    merged.sort(key=lambda r: (999999 if r.get("block_id") is None else int(r.get("block_id")),
                               -(r.get("rating") or 0)))
    return merged

# ---------- Scoring/Trim ----------
def score_item(it: Dict[str, Any]) -> float:
    rating = float(it.get("rating") or 0.0)
    num = float(it.get("numRatings") or 0.0)
    desc_bonus = 0.2 if it.get("generalDescription") else 0.0
    vol = math.log10(max(1.0, num + 1.0))
    return 0.6 * rating + 0.3 * vol + 0.1 * desc_bonus

def trim_and_light_shuffle(items: List[Dict[str, Any]], keep_ratio: float = 0.7, seed: int = 42, max_displacement: int = 2) -> List[Dict[str, Any]]:
    import random
    rng = random.Random(seed)
    n = len(items)
    k = max(1, int(math.ceil(n * keep_ratio))) if n else 0
    ranked = sorted(items, key=score_item, reverse=True)[:k]
    for i in range(len(ranked)):
        j = min(len(ranked) - 1, max(0, i + rng.randint(-max_displacement, max_displacement)))
        if i != j:
            ranked[i], ranked[j] = ranked[j], ranked[i]
    for idx, it in enumerate(ranked, start=1):
        it["_final_index"] = idx
    return ranked

# ---------- Playlist builder ----------
def build_playlist_doc(title: str, city: str, url: str, items: List[Dict[str, Any]], subtype: str = "destination") -> Dict[str, Any]:
    m = re.search(r"/(\d+)(?:$|[?#])", url)
    geo_id = m.group(1) if m else "1000"
    list_id = f"TEMP-{geo_id}"

    playlist = {
        "list_id": list_id,
        "imageUrl": f"https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg",
        "description": default_description(title),
        "source_urls": [url],
        "source": "original",
        "category": "Travel",
        "title": title,
        "city_id": city,
        "subtype": subtype,
        "city": city,
        "created_ts": int(time.time()),
        "slug": build_slug(title, city, subtype, url),
        "subcollections": {"places": []}
    }
    for it in items:
        pid = it.get("placeId")
        reviews = pick_alternate_reviews(it.get("raw_reviews") or [], k=3)
        place_doc = {
            "_id": pid or (it.get("name") or "unknown"),
            "generalDescription": it.get("generalDescription"),
            "utcOffset": int(it.get("utcOffset") if it.get("utcOffset") is not None else 330),
            "maxMinutesSpent": it.get("maxMinutesSpent"),
            "longitude": it.get("longitude"),
            "rating": it.get("rating") or 0,
            "numRatings": it.get("numRatings") or 0,
            "sources": it.get("sources") or [],
            "imageKeys": (it.get("imageKeys") or []) + (it.get("block_imageKeys") or []),
            "openingPeriods": it.get("openingPeriods") or [],
            "name": it.get("name"),
            "placeId": pid,
            "internationalPhoneNumber": it.get("internationalPhoneNumber"),
            "reviews": reviews,
            "permanentlyClosed": bool(it.get("permanentlyClosed")),
            "priceLevel": it.get("priceLevel"),
            "tripadvisorRating": 0,
            "description": None,
            "website": it.get("website"),
            "index": it.get("_final_index") or 1,
            "id": "",
            "categories": it.get("categories") or [],
            "tripadvisorNumRatings": 0,
            "g_image_urls": [],
            "ratingDistribution": it.get("ratingDistribution") or {},
            "minMinutesSpent": it.get("minMinutesSpent"),
            "latitude": it.get("latitude"),
            "address": it.get("address"),
            "travel_time": it.get("travel_time")
        }
        playlist["subcollections"]["places"].append(place_doc)
    playlist["subcollections"]["places"].sort(key=lambda x: x["index"])
    return playlist

# ---------- Dataset loader ----------
def build_jobs_from_args(url: Optional[str], city: Optional[str], dataset_path: Optional[str]) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    if dataset_path:
        ds = json.loads(Path(dataset_path).read_text(encoding="utf-8"))
        for city_name, cats in ds.items():
            for category_name, entries in (cats or {}).items():
                for entry in entries or []:
                    jobs.append({
                        "city": city_name,
                        "category_hint": category_name.lower(),
                        "title": entry.get("title") or "",
                        "url": entry.get("url") or ""
                    })
    elif url and city:
        jobs.append({"city": city, "category_hint": None, "title": "", "url": url})
    return [j for j in jobs if j.get("url")]

def job_out_basename(job: Dict[str, Any], page_title: str) -> str:
    title = page_title or job.get("title") or "playlist"
    city = job.get("city") or "city"
    m = re.search(r"/(\d+)(?:$|[?#])", job.get("url") or "")
    geo_id = m.group(1) if m else "id"
    return f"{slugify(city)}_{slugify(detect_category_from_title(title))}_{geo_id}"

# ---------- Spider ----------
class WanderlogPlaylistTrialSpider(scrapy.Spider):
    name = "wanderlog_playlist_trial"
    custom_settings = {"LOG_LEVEL": "INFO"}

    def __init__(self, jobs: List[Dict[str, Any]], out_dir: str, keep_ratio: float, use_llm: bool, use_selenium: bool, cache_dir: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.jobs = jobs
        self.out_dir = Path(out_dir)
        self.keep_ratio = float(keep_ratio)
        self.use_llm = bool(use_llm and bool(os.getenv("OPENAI_API_KEY")))
        self.use_selenium = bool(use_selenium and SeleniumRequest is not None)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize cache manager
        cache_dir_path = Path(cache_dir) if cache_dir else self.out_dir / "cache"
        self.cache_manager = CacheManager(cache_dir_path)
        
        self.aggregate_report: Dict[str, Any] = {
            "script_version": SCRIPT_VERSION,
            "start_ts": int(time.time()),
            "llm_enabled_flag": bool(use_llm),
            "llm_client_loaded": bool(os.getenv("OPENAI_API_KEY")),
            "model": OPENAI_MODEL if self.use_llm else None,
            "cache_directory": str(cache_dir_path),
            "jobs": []
        }

    def start_requests(self):
        for idx, job in enumerate(self.jobs):
            url = job["url"]
            
            # Check if we have cached MOBX data for this URL
            url_cache_key = self.cache_manager.get_url_cache_key(url)
            if url_cache_key in self.cache_manager.mobx_data_cache:
                self.logger.info(f"[{idx}] Using cached data for: {url}")
                self.cache_manager.api_stats["cache_hits"] += 1
                # Process cached data directly
                self.parse_cached_data(job, idx, url_cache_key)
            else:
                self.cache_manager.api_stats["cache_misses"] += 1
                meta = {"job": job, "job_index": idx, "url_cache_key": url_cache_key}
                if self.use_selenium:
                    yield SeleniumRequest(url=url, callback=self.parse_page, meta=meta, wait_time=2)
                else:
                    yield scrapy.Request(url=url, callback=self.parse_page, meta=meta)

    def parse_cached_data(self, job: Dict[str, Any], job_index: int, url_cache_key: str):
        """Process cached MOBX data without making a new request"""
        cached_data = self.cache_manager.mobx_data_cache[url_cache_key]
        
        data = cached_data.get("data", {})
        page_title = cached_data.get("page_title", job.get("title", "Untitled"))
        url = job.get("url", "")
        
        self.process_mobx_data(job, job_index, data, page_title, url, from_cache=True)

    def parse_page(self, response):
        job = response.meta["job"]
        job_index = response.meta["job_index"]
        url_cache_key = response.meta["url_cache_key"]

        script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
        if not script_text:
            self.logger.error(f"[{job_index}] MOBX not found: {job['url']}")
            return
        
        m = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text, re.DOTALL)
        if not m:
            self.logger.error(f"[{job_index}] MOBX parse failed: {job['url']}")
            return
        
        try:
            mobx_json = json.loads(m.group(1))
        except json.JSONDecodeError:
            self.logger.error(f"[{job_index}] MOBX JSON decode failed: {job['url']}")
            return

        data = (mobx_json.get("placesListPage") or {}).get("data") or {}
        page_title = data.get("title") or (job.get("title") or "Untitled")
        
        # Cache the MOBX data for future runs
        self.cache_manager.mobx_data_cache[url_cache_key] = {
            "data": data,
            "page_title": page_title,
            "cached_ts": int(time.time())
        }
        
        self.process_mobx_data(job, job_index, data, page_title, response.url, from_cache=False)

    def process_mobx_data(self, job: Dict[str, Any], job_index: int, data: Dict[str, Any], page_title: str, url: str, from_cache: bool = False):
        """Process MOBX data (either fresh or cached)"""
        place_meta = data.get("placeMetadata") or []
        blocks = []
        for sec in (data.get("boardSections") or []):
            for b in (sec.get("blocks") or []):
                if b.get("type") == "place":
                    blocks.append(b)

        # Detect category (prefer dataset hint)
        category = (job.get("category_hint") or "").lower().strip() or detect_category_from_title(page_title)

        # Merge
        merged = merge_metadata_and_blocks(place_meta, blocks)

        # QC: heuristic first, then LLM only if uncertain
        accepted: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []
        llm_calls = 0
        place_reports: List[Dict[str, Any]] = []

        for it in merged:
            name = it.get("name") or ""
            cats = it.get("categories") or []
            ctx = build_llm_context(it, category=category)

            verdict, reason, hits = heuristic_is_category(category, name, ctx, cats, page_title)

            decided_by = "heuristic"
            if verdict is None and self.use_llm:
                llm_ans = llm_validate(
                    cache_manager=self.cache_manager,
                    category=category, 
                    name=name, 
                    context=ctx, 
                    city=job.get("city") or "", 
                    page_title=page_title
                )
                if not from_cache:  # Only count as new API call if not from cache
                    llm_calls += 1
                decided_by = "llm"
                if llm_ans is True:
                    verdict, reason = True, "llm_yes"
                elif llm_ans is False:
                    verdict, reason = False, "llm_no"
                else:
                    verdict, reason = False, "llm_failed"

            record = {
                "placeId": it.get("placeId"),
                "name": name,
                "accepted": bool(verdict),
                "decided_by": decided_by,
                "reason": reason,
                "pos_hits": hits.get("pos_hits", []),
                "neg_hits": hits.get("neg_hits", []),
                "context_used_excerpt": ctx[:220]
            }
            place_reports.append(record)

            if verdict:
                accepted.append(it)
            else:
                rejected.append(it)

        # Trim + shuffle
        curated = trim_and_light_shuffle(accepted, keep_ratio=self.keep_ratio)

        # Generate better title if LLM is enabled
        final_title = page_title
        title_calls = 0
        if self.use_llm and curated:
            sample_names = [p.get("name", "") for p in curated[:3]]
            generated_title = generate_playlist_title(
                cache_manager=self.cache_manager,
                city=job.get("city") or "India",
                category=category,
                sample_names=sample_names,
                page_title=page_title
            )
            if generated_title and generated_title != f"{category.title()} in {job.get('city', 'India')}":
                final_title = generated_title
            if not from_cache:  # Only count as new API call if not from cache
                title_calls += 1

        # Build playlist JSON
        playlist = build_playlist_doc(
            title=final_title,
            city=job.get("city") or "India",
            url=url,
            items=curated
        )

        # Write playlist + report
        base = job_out_basename(job, page_title)
        playlist_path = self.out_dir / f"{base}.json"
        report_path = self.out_dir / f"{base}.report.json"

        playlist_path.write_text(json.dumps(playlist, ensure_ascii=False, indent=2), encoding="utf-8")

        job_report = {
            "job_index": job_index,
            "city": job.get("city"),
            "category_used": category,
            "url": job.get("url"),
            "title": page_title,
            "final_title": final_title,
            "from_cache": from_cache,
            "counts": {
                "meta": len(place_meta),
                "blocks": len(blocks),
                "merged": len(merged),
                "accepted_before_trim": len(accepted),
                "rejected": len(rejected),
                "accepted_final": len(curated),
            },
            "llm": {
                "enabled": self.use_llm,
                "model": OPENAI_MODEL if self.use_llm else None,
                "validation_calls": llm_calls,
                "title_calls": title_calls
            },
            "decisions": place_reports,
            "out_files": {
                "playlist_json": str(playlist_path),
                "report_json": str(report_path)
            },
            "created_ts": int(time.time())
        }
        report_path.write_text(json.dumps(job_report, ensure_ascii=False, indent=2), encoding="utf-8")

        # aggregate
        self.aggregate_report["jobs"].append(job_report)

        cache_status = " [CACHED]" if from_cache else ""
        self.logger.info(f"[{job_index}] {final_title} → kept {len(curated)}/{len(merged)} | LLM calls={llm_calls + title_calls}{cache_status}")
        self.logger.info(f"  wrote: {playlist_path}")
        self.logger.info(f"  wrote: {report_path}")

    def closed(self, reason):
        # Save all caches before closing
        self.cache_manager.save_all_caches()
        
        # Add cache statistics to aggregate report
        self.aggregate_report.update({
            "end_ts": int(time.time()),
            "finish_reason": reason,
            "cache_stats": self.cache_manager.api_stats,
            "cache_sizes": {
                "llm_validation_cache": len(self.cache_manager.llm_validation_cache),
                "llm_title_cache": len(self.cache_manager.llm_title_cache),
                "google_places_cache": len(self.cache_manager.google_places_cache),
                "mobx_data_cache": len(self.cache_manager.mobx_data_cache)
            }
        })
        
        agg_path = self.out_dir / "_aggregate_report.json"
        agg_path.write_text(json.dumps(self.aggregate_report, ensure_ascii=False, indent=2), encoding="utf-8")
        
        # Log cache statistics
        cache_hit_rate = (self.cache_manager.api_stats["cache_hits"] / 
                         max(1, self.cache_manager.api_stats["cache_hits"] + self.cache_manager.api_stats["cache_misses"]) * 100)
        
        self.logger.info(f"Cache statistics:")
        self.logger.info(f"  Cache hit rate: {cache_hit_rate:.1f}%")
        self.logger.info(f"  LLM validation calls (new): {self.cache_manager.api_stats['llm_validation_calls']}")
        self.logger.info(f"  LLM title calls (new): {self.cache_manager.api_stats['llm_title_calls']}")
        self.logger.info(f"  Google Places calls (new): {self.cache_manager.api_stats['google_places_calls']}")
        
        self.logger.info(f"Aggregate report → {agg_path}")

# ---------- Runner ----------
def main():
    parser = argparse.ArgumentParser(description="Trial: Build curated playlists from Wanderlog with heuristic+LLM QC and caching.")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--url", help="Single Wanderlog list URL")
    g.add_argument("--dataset-file", help="Path to JSON dataset (city -> category -> [{title,url}])")

    parser.add_argument("--city", help="City name (required if using --url)")
    parser.add_argument("--out-dir", default="trial_playlists", help="Output directory for JSONs and reports")
    parser.add_argument("--cache-dir", help="Cache directory (defaults to out-dir/cache)")
    parser.add_argument("--keep-ratio", type=float, default=0.7, help="Fraction to keep after trimming (0..1)")
    parser.add_argument("--use-llm", action="store_true", help="Enable LLM for uncertain cases (requires OPENAI_API_KEY)")
    parser.add_argument("--use-selenium", action="store_true", help="(Optional) Use selenium; usually not needed")
    parser.add_argument("--clear-cache", action="store_true", help="Clear all caches before starting")

    args = parser.parse_args()

    if args.url and not args.city:
        parser.error("--city is required when using --url")

    # Handle cache clearing
    if args.clear_cache:
        cache_dir_path = Path(args.cache_dir) if args.cache_dir else Path(args.out_dir) / "cache"
        if cache_dir_path.exists():
            import shutil
            shutil.rmtree(cache_dir_path)
            print(f"Cleared cache directory: {cache_dir_path}")

    jobs = build_jobs_from_args(args.url, args.city, args.dataset_file)
    if not jobs:
        print("No jobs to process (check --url/--city or --dataset-file).")
        return

    # Dynamically enable Selenium if requested
    settings = {"LOG_LEVEL": "INFO"}
    if args.use_selenium:
        if SeleniumRequest is None:
            raise RuntimeError("scrapy-selenium not installed or import failed.")
        from webdriver_manager.chrome import ChromeDriverManager
        driver_path = ChromeDriverManager().install()
        settings.update({
            "DOWNLOADER_MIDDLEWARES": {"scrapy_selenium.SeleniumMiddleware": 800},
            "SELENIUM_DRIVER_NAME": "chrome",
            "SELENIUM_DRIVER_EXECUTABLE_PATH": driver_path,
            "SELENIUM_DRIVER_ARGUMENTS": ["--headless=new", "--no-sandbox", "--disable-gpu", "--window-size=1600,1200"],
        })

    process = CrawlerProcess(settings=settings)
    process.crawl(
        WanderlogPlaylistTrialSpider,
        jobs=jobs,
        out_dir=args.out_dir,
        keep_ratio=args.keep_ratio,
        use_llm=args.use_llm,
        use_selenium=args.use_selenium,
        cache_dir=args.cache_dir
    )
    process.start()

if __name__ == "__main__":
    main()

# import os
# import re
# import json
# import time
# import math
# import argparse
# from pathlib import Path
# from typing import Any, Dict, List, Optional, Tuple
# from datetime import datetime

# import scrapy
# from scrapy.crawler import CrawlerProcess

# # --- .env (next to this script) ---
# try:
#     from dotenv import load_dotenv
#     load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
# except Exception:
#     pass

# # --- slugify with fallback ---
# try:
#     from slugify import slugify  # pip install python-slugify
# except Exception:
#     import re as _re
#     def slugify(s: str) -> str:
#         s = (s or "").lower().strip()
#         s = _re.sub(r"[^\w\s-]", "", s)
#         s = _re.sub(r"[\s_-]+", "-", s)
#         return _re.sub(r"^-+|-+$", "", s)

# # --- Optional Selenium (only if you turn it on) ---
# try:
#     from scrapy_selenium import SeleniumRequest
# except Exception:
#     SeleniumRequest = None

# # --- Model selection ---
# OPENAI_MODEL = os.getenv("LC_MODEL", "gpt-4o-mini")
# SCRIPT_VERSION = "0.5.0"

# # ---------- Category patterns ----------
# CATEGORY_PATTERNS = {
#     "beaches": {
#         "pos": r"\b(beach|sea\s*face|seaface|seafront|shore|coast|bay|chowpatty|sand|sands)\b",
#         "neg": r"\b(temple|mandir|church|mosque|museum|mall|market|fort|palace|playground|bank|school|hospital|crocodile|tower|bridge|station|cinema|theatre|theater|atm|office|court|college|university|monument)\b",
#     },
#     "national parks": {
#         "pos": r"\b(national\s+park|wildlife\s+sanctuary|tiger\s+reserve|biosphere\s+reserve|safari|national\s+forest|conservation\s+area)\b",
#         "neg": r"\b(mall|market|museum|temple|church|mosque|fort|palace|beach|cinema|theatre|theater|atm|office|court|school|college|university)\b",
#     },
#     "waterfalls": {
#         "pos": r"\b(waterfall|falls|cascade|cascades)\b",
#         "neg": r"\b(mall|market|museum|temple|church|mosque|fort|palace|cinema|theatre|theater)\b",
#     },
#     "castles": {
#         "pos": r"\b(castle|fortress|citadel)\b",
#         "neg": r"\b(mall|market|museum|temple|church|mosque|beach|cinema|theatre|theater)\b",
#     },
#     "photo spots": {
#         "pos": r"\b(viewpoint|view\s*point|lookout|photo\s*spot|sunset\s*point|sunrise\s*point|scenic|panorama|photograph|photogenic)\b",
#         "neg": r"\b(atm|office|court|bank)\b",
#     },
#     "romantic places": {
#         "pos": r"\b(honeymoon|romantic|couple|love|sunset\s*point|secluded|candlelight)\b",
#         "neg": r"\b(atm|office|court|bank)\b",
#     },
#     "architecture": {
#         "pos": r"\b(architecture|architectural|heritage|historic|monument|cathedral|basilica|temple|mosque|church|fort|palace|colonial)\b",
#         "neg": r"\b(beach|waterfall)\b",
#     },
# }

# def detect_category_from_title(page_title: str) -> str:
#     t = (page_title or "").lower()
#     if "beach" in t: return "beaches"
#     if "national park" in t or "wildlife" in t or "reserve" in t: return "national parks"
#     if "waterfall" in t: return "waterfalls"
#     if "castle" in t or "fortress" in t or "fort " in t: return "castles"
#     if "photo spot" in t or "photo" in t: return "photo spots"
#     if "romantic" in t: return "romantic places"
#     if "architecture" in t: return "architecture"
#     return "beaches"

# # ---------- Utilities ----------
# def iso_to_epoch_seconds(iso_str: str) -> Optional[int]:
#     if not iso_str:
#         return None
#     try:
#         dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
#         return int(dt.timestamp())
#     except Exception:
#         return None

# def clean_text(text: Optional[str]) -> Optional[str]:
#     if not text:
#         return text
#     return text.replace("\u2019", "'").replace("\u2014", "-").strip()

# def default_description(title: str) -> str:
#     return (f'Dive into “{title}” — a handpicked list of places with quick notes, links, '
#             f'and essentials for fast trip planning and discovery.')

# def build_slug(title: str, city: str, subtype: str, url: str) -> str:
#     m = re.search(r"/(\d+)(?:$|[?#])", url) or re.search(r"/(\d+)$", url)
#     tid = m.group(1) if m else "list"
#     return f"{slugify(title)}-{slugify(city)}-{slugify(subtype)}-{tid}"

# def explain_hits(pat: re.Pattern, text: str) -> List[str]:
#     try:
#         hits = pat.findall(text)
#         if not hits:
#             return []
#         if isinstance(hits[0], tuple):
#             hits = [h for tup in hits for h in tup if h]
#         uniq = sorted({h.lower() for h in hits if isinstance(h, str) and h.strip()})
#         return uniq
#     except Exception:
#         return []

# # ---------- LLM context (uses generalDescription) ----------
# def build_llm_context(it: Dict[str, Any], category: str, max_len: int = 900) -> str:
#     parts: List[str] = []
#     if it.get("generalDescription"):
#         parts.append(f"General: {clean_text(it['generalDescription'])}")
#     if it.get("block_desc"):
#         parts.append(f"Board note: {clean_text(it['block_desc'])}")
#     cats = it.get("categories") or []
#     if cats:
#         parts.append("Tags: " + ", ".join([str(c) for c in cats][:6]))
#     for r in (it.get("raw_reviews") or [])[:2]:
#         txt = clean_text(r.get("reviewText"))
#         if txt:
#             parts.append(f"Review: {txt}")
#     parts.insert(0, f"Target category: {category}")
#     ctx = " ".join(parts).strip()
#     if len(ctx) > max_len:
#         ctx = ctx[:max_len].rstrip() + "…"
#     return ctx

# # ---------- Reviews mapping (alternate picks) ----------
# def pick_alternate_reviews(reviews: List[Dict[str, Any]], k: int = 3) -> List[Dict[str, Any]]:
#     if not reviews:
#         return []
#     def _key(r):
#         rating = r.get("rating", 0)
#         ts = iso_to_epoch_seconds(r.get("time") or "") or 0
#         return (rating, ts)
#     sorted_reviews = sorted(reviews, key=_key, reverse=True)
#     picked = []
#     for i, r in enumerate(sorted_reviews):
#         if len(picked) >= k:
#             break
#         if i % 2 == 0:
#             picked.append({
#                 "rating": int(r.get("rating", 0)),
#                 "text": clean_text(r.get("reviewText")),
#                 "author_name": r.get("reviewerName") or "",
#                 "relative_time_description": "",
#                 "time": iso_to_epoch_seconds(r.get("time")) or 0,
#                 "profile_photo_url": ""
#             })
#     return picked

# # ---------- Heuristic + LLM validation ----------
# def heuristic_is_category(category: str, name: str, desc: str, cats: List[str], page_title: str)\
#         -> Tuple[Optional[bool], str, Dict[str, Any]]:

#     patt = CATEGORY_PATTERNS.get(category, CATEGORY_PATTERNS["beaches"])
#     pos = re.compile(patt["pos"], re.IGNORECASE)
#     neg = re.compile(patt["neg"], re.IGNORECASE)

#     blob = " ".join([name or "", desc or "", " ".join(cats or []), page_title or ""])
#     pos_hits = explain_hits(pos, blob)
#     neg_hits = explain_hits(neg, blob)

#     if neg_hits and not pos_hits:
#         return False, "heuristic_neg", {"pos_hits": pos_hits, "neg_hits": neg_hits}
#     if pos_hits and not neg_hits:
#         return True, "heuristic_pos", {"pos_hits": pos_hits, "neg_hits": neg_hits}
#     return None, "heuristic_uncertain", {"pos_hits": pos_hits, "neg_hits": neg_hits}

# def llm_validate(category: str, name: str, context: str, city: str, page_title: str) -> Optional[bool]:
#     """
#     Return True/False/None (None on failure). Supports both new and old OpenAI SDKs.
#     """
#     api_key = os.getenv("OPENAI_API_KEY")
#     if not api_key:
#         return None

#     prompt = f"""You are validating inclusion for a travel list titled "{page_title}".
# City/Region: {city}
# Place: "{name}"
# Decision category: {category}

# Context:
# {context}

# Question: Does this place CLEARLY belong in the "{category}" category?
# Answer with only one token: YES or NO."""

#     # Try new SDK (openai>=1.0)
#     try:
#         from openai import OpenAI
#         client = OpenAI()
#         resp = client.chat.completions.create(
#             model=OPENAI_MODEL,
#             messages=[{"role": "user", "content": prompt}],
#             temperature=0,
#         )
#         out = (resp.choices[0].message.content or "").strip().lower()
#         if out.startswith("yes"): return True
#         if out.startswith("no"):  return False
#     except Exception:
#         pass

#     # Fallback to old SDK (openai<1.0)
#     try:
#         import openai as _openai
#         _openai.api_key = api_key
#         resp = _openai.ChatCompletion.create(
#             model=OPENAI_MODEL,
#             messages=[{"role": "user", "content": prompt}],
#             temperature=0,
#         )
#         out = resp.choices[0].message["content"].strip().lower()
#         if out.startswith("yes"): return True
#         if out.startswith("no"):  return False
#     except Exception:
#         return None

#     return None

# # ---------- Merge MOBX sources ----------
# def merge_metadata_and_blocks(place_meta: List[Dict[str, Any]],
#                               blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
#     meta_map: Dict[str, Dict[str, Any]] = {}
#     for p in place_meta or []:
#         pid = str(p.get("placeId") or p.get("id") or "")
#         if not pid:
#             pid = f"NAME::{(p.get('name') or '').strip()}"
#         meta_map[pid] = {
#             "placeId": p.get("placeId") or p.get("id"),
#             "name": p.get("name"),
#             "rating": p.get("rating"),
#             "numRatings": p.get("numRatings"),
#             "priceLevel": p.get("priceLevel"),
#             "openingPeriods": p.get("openingPeriods") or [],
#             "internationalPhoneNumber": p.get("internationalPhoneNumber"),
#             "address": p.get("address"),
#             "utcOffset": p.get("utcOffset"),
#             "categories": p.get("categories") or [],
#             "generalDescription": p.get("generatedDescription") or p.get("description"),
#             "raw_reviews": p.get("reviews") or [],
#             "ratingDistribution": p.get("ratingDistribution") or {},
#             "permanentlyClosed": bool(p.get("permanentlyClosed")),
#             "imageKeys": p.get("imageKeys") or [],
#             "sources": p.get("sources") or [],
#             "minMinutesSpent": p.get("minMinutesSpent"),
#             "maxMinutesSpent": p.get("maxMinutesSpent"),
#             "website": p.get("website"),
#         }
#     for b in blocks or []:
#         if b.get("type") != "place":
#             continue
#         place = b.get("place") or {}
#         pid = str(place.get("placeId") or "")
#         key = pid if pid else f"NAME::{place.get('name') or ''}"
#         if key not in meta_map:
#             meta_map[key] = {"placeId": pid or None, "name": place.get("name")}
#         text_ops = ((b.get("text") or {}).get("ops") or [])
#         addendum = "".join([t.get("insert", "") for t in text_ops if isinstance(t, dict)])
#         meta_map[key].update({
#             "latitude": place.get("latitude"),
#             "longitude": place.get("longitude"),
#             "block_id": b.get("id"),
#             "block_desc": addendum.strip() or None,
#             "block_imageKeys": b.get("imageKeys") or [],
#             "selectedImageKey": b.get("selectedImageKey"),
#         })
#     merged = list(meta_map.values())
#     merged.sort(key=lambda r: (999999 if r.get("block_id") is None else int(r.get("block_id")),
#                                -(r.get("rating") or 0)))
#     return merged

# # ---------- Scoring/Trim ----------
# def score_item(it: Dict[str, Any]) -> float:
#     rating = float(it.get("rating") or 0.0)
#     num = float(it.get("numRatings") or 0.0)
#     desc_bonus = 0.2 if it.get("generalDescription") else 0.0
#     vol = math.log10(max(1.0, num + 1.0))
#     return 0.6 * rating + 0.3 * vol + 0.1 * desc_bonus

# def trim_and_light_shuffle(items: List[Dict[str, Any]], keep_ratio: float = 0.7, seed: int = 42, max_displacement: int = 2) -> List[Dict[str, Any]]:
#     import random
#     rng = random.Random(seed)
#     n = len(items)
#     k = max(1, int(math.ceil(n * keep_ratio))) if n else 0
#     ranked = sorted(items, key=score_item, reverse=True)[:k]
#     for i in range(len(ranked)):
#         j = min(len(ranked) - 1, max(0, i + rng.randint(-max_displacement, max_displacement)))
#         if i != j:
#             ranked[i], ranked[j] = ranked[j], ranked[i]
#     for idx, it in enumerate(ranked, start=1):
#         it["_final_index"] = idx
#     return ranked

# # ---------- Playlist builder ----------
# def build_playlist_doc(title: str, city: str, url: str, items: List[Dict[str, Any]], subtype: str = "destination") -> Dict[str, Any]:
#     m = re.search(r"/(\d+)(?:$|[?#])", url)
#     geo_id = m.group(1) if m else "1000"
#     list_id = f"TEMP-{geo_id}"

#     playlist = {
#         "list_id": list_id,
#         "imageUrl": f"https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg",
#         "description": default_description(title),
#         "source_urls": [url],
#         "source": "original",
#         "category": "Travel",
#         "title": title,
#         "city_id": city,
#         "subtype": subtype,
#         "city": city,
#         "created_ts": int(time.time()),
#         "slug": build_slug(title, city, subtype, url),
#         "subcollections": {"places": []}
#     }
#     for it in items:
#         pid = it.get("placeId")
#         reviews = pick_alternate_reviews(it.get("raw_reviews") or [], k=3)
#         place_doc = {
#             "_id": pid or (it.get("name") or "unknown"),
#             "generalDescription": it.get("generalDescription"),
#             "utcOffset": int(it.get("utcOffset") if it.get("utcOffset") is not None else 330),
#             "maxMinutesSpent": it.get("maxMinutesSpent"),
#             "longitude": it.get("longitude"),
#             "rating": it.get("rating") or 0,
#             "numRatings": it.get("numRatings") or 0,
#             "sources": it.get("sources") or [],
#             "imageKeys": (it.get("imageKeys") or []) + (it.get("block_imageKeys") or []),
#             "openingPeriods": it.get("openingPeriods") or [],
#             "name": it.get("name"),
#             "placeId": pid,
#             "internationalPhoneNumber": it.get("internationalPhoneNumber"),
#             "reviews": reviews,
#             "permanentlyClosed": bool(it.get("permanentlyClosed")),
#             "priceLevel": it.get("priceLevel"),
#             "tripadvisorRating": 0,
#             "description": None,
#             "website": it.get("website"),
#             "index": it.get("_final_index") or 1,
#             "id": "",
#             "categories": it.get("categories") or [],
#             "tripadvisorNumRatings": 0,
#             "g_image_urls": [],
#             "ratingDistribution": it.get("ratingDistribution") or {},
#             "minMinutesSpent": it.get("minMinutesSpent"),
#             "latitude": it.get("latitude"),
#             "address": it.get("address"),
#             "travel_time": it.get("travel_time")
#         }
#         playlist["subcollections"]["places"].append(place_doc)
#     playlist["subcollections"]["places"].sort(key=lambda x: x["index"])
#     return playlist

# # ---------- Dataset loader ----------
# def build_jobs_from_args(url: Optional[str], city: Optional[str], dataset_path: Optional[str]) -> List[Dict[str, Any]]:
#     jobs: List[Dict[str, Any]] = []
#     if dataset_path:
#         ds = json.loads(Path(dataset_path).read_text(encoding="utf-8"))
#         for city_name, cats in ds.items():
#             for category_name, entries in (cats or {}).items():
#                 for entry in entries or []:
#                     jobs.append({
#                         "city": city_name,
#                         "category_hint": category_name.lower(),
#                         "title": entry.get("title") or "",
#                         "url": entry.get("url") or ""
#                     })
#     elif url and city:
#         jobs.append({"city": city, "category_hint": None, "title": "", "url": url})
#     return [j for j in jobs if j.get("url")]

# def job_out_basename(job: Dict[str, Any], page_title: str) -> str:
#     title = page_title or job.get("title") or "playlist"
#     city = job.get("city") or "city"
#     m = re.search(r"/(\d+)(?:$|[?#])", job.get("url") or "")
#     geo_id = m.group(1) if m else "id"
#     return f"{slugify(city)}_{slugify(detect_category_from_title(title))}_{geo_id}"

# # ---------- Spider ----------
# class WanderlogPlaylistTrialSpider(scrapy.Spider):
#     name = "wanderlog_playlist_trial"
#     custom_settings = {"LOG_LEVEL": "INFO"}

#     def __init__(self, jobs: List[Dict[str, Any]], out_dir: str, keep_ratio: float, use_llm: bool, use_selenium: bool, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.jobs = jobs
#         self.out_dir = Path(out_dir)
#         self.keep_ratio = float(keep_ratio)
#         self.use_llm = bool(use_llm and bool(os.getenv("OPENAI_API_KEY")))
#         self.use_selenium = bool(use_selenium and SeleniumRequest is not None)
#         self.out_dir.mkdir(parents=True, exist_ok=True)
#         self.aggregate_report: Dict[str, Any] = {
#             "script_version": SCRIPT_VERSION,
#             "start_ts": int(time.time()),
#             "llm_enabled_flag": bool(use_llm),
#             "llm_client_loaded": bool(os.getenv("OPENAI_API_KEY")),
#             "model": OPENAI_MODEL if self.use_llm else None,
#             "jobs": []
#         }

#     def start_requests(self):
#         for idx, job in enumerate(self.jobs):
#             meta = {"job": job, "job_index": idx}
#             if self.use_selenium:
#                 yield SeleniumRequest(url=job["url"], callback=self.parse_page, meta=meta, wait_time=2)
#             else:
#                 yield scrapy.Request(url=job["url"], callback=self.parse_page, meta=meta)

#     def parse_page(self, response):
#         job = response.meta["job"]
#         job_index = response.meta["job_index"]

#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
#         if not script_text:
#             self.logger.error(f"[{job_index}] MOBX not found: {job['url']}")
#             return
#         m = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text, re.DOTALL)
#         if not m:
#             self.logger.error(f"[{job_index}] MOBX parse failed: {job['url']}")
#             return
#         mobx_json = json.loads(m.group(1))

#         data = (mobx_json.get("placesListPage") or {}).get("data") or {}
#         page_title = data.get("title") or (job.get("title") or "Untitled")
#         place_meta = data.get("placeMetadata") or []
#         blocks = []
#         for sec in (data.get("boardSections") or []):
#             for b in (sec.get("blocks") or []):
#                 if b.get("type") == "place":
#                     blocks.append(b)

#         # Detect category (prefer dataset hint)
#         category = (job.get("category_hint") or "").lower().strip() or detect_category_from_title(page_title)

#         # Merge
#         merged = merge_metadata_and_blocks(place_meta, blocks)

#         # QC: heuristic first, then LLM only if uncertain
#         accepted: List[Dict[str, Any]] = []
#         rejected: List[Dict[str, Any]] = []
#         llm_calls = 0
#         place_reports: List[Dict[str, Any]] = []

#         for it in merged:
#             name = it.get("name") or ""
#             cats = it.get("categories") or []
#             ctx = build_llm_context(it, category=category)

#             verdict, reason, hits = heuristic_is_category(category, name, ctx, cats, page_title)

#             decided_by = "heuristic"
#             if verdict is None and self.use_llm:
#                 llm_ans = llm_validate(category=category, name=name, context=ctx, city=job.get("city") or "", page_title=page_title)
#                 llm_calls += 1
#                 decided_by = "llm"
#                 if llm_ans is True:
#                     verdict, reason = True, "llm_yes"
#                 elif llm_ans is False:
#                     verdict, reason = False, "llm_no"
#                 else:
#                     verdict, reason = False, "llm_failed"

#             record = {
#                 "placeId": it.get("placeId"),
#                 "name": name,
#                 "accepted": bool(verdict),
#                 "decided_by": decided_by,
#                 "reason": reason,
#                 "pos_hits": hits.get("pos_hits", []),
#                 "neg_hits": hits.get("neg_hits", []),
#                 "context_used_excerpt": ctx[:220]
#             }
#             place_reports.append(record)

#             if verdict:
#                 accepted.append(it)
#             else:
#                 rejected.append(it)

#         # Trim + shuffle
#         curated = trim_and_light_shuffle(accepted, keep_ratio=self.keep_ratio)

#         # Build playlist JSON
#         playlist = build_playlist_doc(
#             title=page_title,
#             city=job.get("city") or "India",
#             url=response.url,
#             items=curated
#         )

#         # Write playlist + report
#         base = job_out_basename(job, page_title)
#         playlist_path = self.out_dir / f"{base}.json"
#         report_path = self.out_dir / f"{base}.report.json"

#         playlist_path.write_text(json.dumps(playlist, ensure_ascii=False, indent=2), encoding="utf-8")

#         job_report = {
#             "job_index": job_index,
#             "city": job.get("city"),
#             "category_used": category,
#             "url": job.get("url"),
#             "title": page_title,
#             "counts": {
#                 "meta": len(place_meta),
#                 "blocks": len(blocks),
#                 "merged": len(merged),
#                 "accepted_before_trim": len(accepted),
#                 "rejected": len(rejected),
#                 "accepted_final": len(curated),
#             },
#             "llm": {
#                 "enabled": self.use_llm,
#                 "model": OPENAI_MODEL if self.use_llm else None,
#                 "calls": llm_calls
#             },
#             "decisions": place_reports,
#             "out_files": {
#                 "playlist_json": str(playlist_path),
#                 "report_json": str(report_path)
#             },
#             "created_ts": int(time.time())
#         }
#         report_path.write_text(json.dumps(job_report, ensure_ascii=False, indent=2), encoding="utf-8")

#         # aggregate
#         self.aggregate_report["jobs"].append(job_report)

#         self.logger.info(f"[{job_index}] {page_title} → kept {len(curated)}/{len(merged)} | LLM calls={llm_calls}")
#         self.logger.info(f"  wrote: {playlist_path}")
#         self.logger.info(f"  wrote: {report_path}")

#     def closed(self, reason):
#         self.aggregate_report.update({
#             "end_ts": int(time.time()),
#             "finish_reason": reason
#         })
#         agg_path = self.out_dir / "_aggregate_report.json"
#         agg_path.write_text(json.dumps(self.aggregate_report, ensure_ascii=False, indent=2), encoding="utf-8")
#         self.logger.info(f"Aggregate report → {agg_path}")

# # ---------- Runner ----------
# def main():
#     parser = argparse.ArgumentParser(description="Trial: Build curated playlists from Wanderlog with heuristic+LLM QC.")
#     g = parser.add_mutually_exclusive_group(required=True)
#     g.add_argument("--url", help="Single Wanderlog list URL")
#     g.add_argument("--dataset-file", help="Path to JSON dataset (city -> category -> [{title,url}])")

#     parser.add_argument("--city", help="City name (required if using --url)")
#     parser.add_argument("--out-dir", default="trial_playlists", help="Output directory for JSONs and reports")
#     parser.add_argument("--keep-ratio", type=float, default=0.7, help="Fraction to keep after trimming (0..1)")
#     parser.add_argument("--use-llm", action="store_true", help="Enable LLM for uncertain cases (requires OPENAI_API_KEY)")
#     parser.add_argument("--use-selenium", action="store_true", help="(Optional) Use selenium; usually not needed")

#     args = parser.parse_args()

#     if args.url and not args.city:
#         parser.error("--city is required when using --url")

#     jobs = build_jobs_from_args(args.url, args.city, args.dataset_file)
#     if not jobs:
#         print("No jobs to process (check --url/--city or --dataset-file).")
#         return

#     # Dynamically enable Selenium if requested
#     settings = {"LOG_LEVEL": "INFO"}
#     if args.use_selenium:
#         if SeleniumRequest is None:
#             raise RuntimeError("scrapy-selenium not installed or import failed.")
#         from webdriver_manager.chrome import ChromeDriverManager
#         driver_path = ChromeDriverManager().install()
#         settings.update({
#             "DOWNLOADER_MIDDLEWARES": {"scrapy_selenium.SeleniumMiddleware": 800},
#             "SELENIUM_DRIVER_NAME": "chrome",
#             "SELENIUM_DRIVER_EXECUTABLE_PATH": driver_path,
#             "SELENIUM_DRIVER_ARGUMENTS": ["--headless=new", "--no-sandbox", "--disable-gpu", "--window-size=1600,1200"],
#         })

#     process = CrawlerProcess(settings=settings)
#     process.crawl(
#         WanderlogPlaylistTrialSpider,
#         jobs=jobs,
#         out_dir=args.out_dir,
#         keep_ratio=args.keep_ratio,
#         use_llm=args.use_llm,
#         use_selenium=args.use_selenium
#     )
#     process.start()

# if __name__ == "__main__":
#     main()



# -*- coding: utf-8 -*-
# Working version with addition of reports
# """
# Trial: Wanderlog -> curated playlist JSONs with heuristic+LLM gating
# - Single URL or dataset-file with multiple city/category jobs
# - Heuristic decides first; LLM only for 'uncertain' cases
# - Exports one playlist JSON per job + a per-job report + an aggregate report
# - No Firestore/Storage writes (g_image_urls = [])
# """

# import os
# import re
# import json
# import time
# import math
# import argparse
# from pathlib import Path
# from typing import Any, Dict, List, Optional, Tuple
# from datetime import datetime, timezone

# import scrapy
# from scrapy.crawler import CrawlerProcess
# from pathlib import Path
# try:
#     from dotenv import load_dotenv
#     # Load .env that sits next to this script
#     load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
# except Exception:
#     pass


# try:
#     from slugify import slugify
# except Exception:
#     # minimal fallback
#     import re as _re
#     def slugify(s: str) -> str:
#         s = (s or "").lower().strip()
#         s = _re.sub(r"[^\w\s-]", "", s)
#         s = _re.sub(r"[\s_-]+", "-", s)
#         return _re.sub(r"^-+|-+$", "", s)

# # ---------- OPTIONAL LLM (OpenAI) ----------
# OPENAI = None
# OPENAI_MODEL = "gpt-4o-mini"  # cheap/fast; change if you prefer
# if os.environ.get("OPENAI_API_KEY"):
#     try:
#         import openai
#         OPENAI = openai
#         OPENAI.api_key = os.environ["OPENAI_API_KEY"]
#     except Exception:
#         OPENAI = None  # fallback to heuristic silently

# SCRIPT_VERSION = "0.4.0"

# # ---------- Category patterns ----------
# CATEGORY_PATTERNS = {
#     "beaches": {
#         "pos": r"\b(beach|sea\s*face|seaface|seafront|shore|coast|bay|chowpatty|sand|sands)\b",
#         "neg": r"\b(temple|mandir|church|mosque|museum|mall|market|fort|palace|playground|bank|school|hospital|crocodile|tower|bridge|station|cinema|theatre|theater|atm|office|court|college|university|monument)\b",
#     },
#     "national parks": {
#         "pos": r"\b(national\s+park|wildlife\s+sanctuary|tiger\s+reserve|biosphere\s+reserve|safari|national\s+forest|conservation\s+area)\b",
#         "neg": r"\b(mall|market|museum|temple|church|mosque|fort|palace|beach|cinema|theatre|theater|atm|office|court|school|college|university)\b",
#     },
#     "waterfalls": {
#         "pos": r"\b(waterfall|falls|cascade|cascades)\b",
#         "neg": r"\b(mall|market|museum|temple|church|mosque|fort|palace|cinema|theatre|theater)\b",
#     },
#     "castles": {
#         "pos": r"\b(castle|fortress|citadel)\b",
#         "neg": r"\b(mall|market|museum|temple|church|mosque|beach|cinema|theatre|theater)\b",
#     },
#     "photo spots": {
#         "pos": r"\b(viewpoint|view\s*point|lookout|photo\s*spot|sunset\s*point|sunrise\s*point|scenic|panorama|photograph|photogenic)\b",
#         "neg": r"\b(atm|office|court|bank)\b",
#     },
#     "romantic places": {
#         "pos": r"\b(honeymoon|romantic|couple|love|sunset\s*point|secluded|candlelight)\b",
#         "neg": r"\b(atm|office|court|bank)\b",
#     },
#     "architecture": {
#         "pos": r"\b(architecture|architectural|heritage|historic|monument|cathedral|basilica|temple|mosque|church|fort|palace|colonial)\b",
#         "neg": r"\b(beach|waterfall)\b",
#     },
# }

# def detect_category_from_title(page_title: str) -> str:
#     t = (page_title or "").lower()
#     if "beach" in t: return "beaches"
#     if "national park" in t or "wildlife" in t or "reserve" in t: return "national parks"
#     if "waterfall" in t: return "waterfalls"
#     if "castle" in t or "fortress" in t or "fort " in t: return "castles"
#     if "photo spot" in t or "photo" in t: return "photo spots"
#     if "romantic" in t: return "romantic places"
#     if "architecture" in t: return "architecture"
#     # default to 'beaches' if truly unknown
#     return "beaches"

# # ---------- Utilities ----------
# def iso_to_epoch_seconds(iso_str: str) -> Optional[int]:
#     if not iso_str:
#         return None
#     try:
#         dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
#         return int(dt.timestamp())
#     except Exception:
#         return None

# def clean_text(text: Optional[str]) -> Optional[str]:
#     if not text:
#         return text
#     return (text.replace("\u2019", "'").replace("\u2014", "-").strip())

# def default_description(title: str) -> str:
#     return (f'Dive into “{title}” — a handpicked list of places with quick notes, links, '
#             f'and essentials for fast trip planning and discovery.')

# def build_slug(title: str, city: str, subtype: str, url: str) -> str:
#     m = re.search(r"/(\d+)(?:$|[?#])", url) or re.search(r"/(\d+)$", url)
#     tid = m.group(1) if m else "list"
#     base = f"{slugify(title)}-{slugify(city)}-{slugify(subtype)}-{tid}"
#     return base

# def explain_hits(pat: re.Pattern, text: str) -> List[str]:
#     try:
#         hits = pat.findall(text)
#         if not hits:
#             return []
#         if isinstance(hits[0], tuple):
#             hits = [h for tup in hits for h in tup if h]
#         uniq = sorted({h.lower() for h in hits if isinstance(h, str) and h.strip()})
#         return uniq
#     except Exception:
#         return []


# def build_llm_context(it: Dict[str, Any], category: str, max_len: int = 900) -> str:
#     """
#     Build a compact text context for the LLM from:
#       - generalDescription (primary)
#       - board block description (secondary)
#       - categories / tags (light)
#       - up to 2 review snippets (from placeMetadata.reviews)
#     """
#     parts: List[str] = []
#     if it.get("generalDescription"):
#         parts.append(f"General: {clean_text(it['generalDescription'])}")
#     if it.get("block_desc"):
#         parts.append(f"Board note: {clean_text(it['block_desc'])}")
#     cats = it.get("categories") or []
#     if cats:
#         parts.append("Tags: " + ", ".join([str(c) for c in cats][:6]))

#     # a couple of reviews from placeMetadata (if present)
#     for r in (it.get("raw_reviews") or [])[:2]:
#         txt = clean_text(r.get("reviewText"))
#         if txt:
#             parts.append(f"Review: {txt}")

#     # strong reminder to the model of the category we care about
#     parts.insert(0, f"Target category: {category}")

#     ctx = " ".join(parts).strip()
#     if len(ctx) > max_len:
#         ctx = ctx[:max_len].rstrip() + "…"
#     return ctx




# # ---------- Reviews mapping (alternate picks) ----------
# def pick_alternate_reviews(reviews: List[Dict[str, Any]], k: int = 3) -> List[Dict[str, Any]]:
#     if not reviews:
#         return []
#     def _key(r):
#         rating = r.get("rating", 0)
#         ts = iso_to_epoch_seconds(r.get("time") or "") or 0
#         return (rating, ts)
#     sorted_reviews = sorted(reviews, key=_key, reverse=True)
#     picked = []
#     for i, r in enumerate(sorted_reviews):
#         if len(picked) >= k:
#             break
#         if i % 2 == 0:
#             picked.append({
#                 "rating": int(r.get("rating", 0)),
#                 "text": clean_text(r.get("reviewText")),
#                 "author_name": r.get("reviewerName") or "",
#                 "relative_time_description": "",
#                 "time": iso_to_epoch_seconds(r.get("time")) or 0,
#                 "profile_photo_url": ""
#             })
#     return picked

# # ---------- Heuristic + LLM validation ----------
# def heuristic_is_category(category: str, name: str, desc: str, cats: List[str], page_title: str)\
#         -> Tuple[Optional[bool], str, Dict[str, Any]]:
#     patt = CATEGORY_PATTERNS.get(category, CATEGORY_PATTERNS["beaches"])
#     pos = re.compile(patt["pos"], re.IGNORECASE)
#     neg = re.compile(patt["neg"], re.IGNORECASE)

#     blob = " ".join([name or "", desc or "", " ".join(cats or []), page_title or ""])
#     pos_hits = explain_hits(pos, blob)
#     neg_hits = explain_hits(neg, blob)

#     if neg_hits and not pos_hits:
#         return False, "heuristic_neg", {"pos_hits": pos_hits, "neg_hits": neg_hits}
#     if pos_hits and not neg_hits:
#         return True, "heuristic_pos", {"pos_hits": pos_hits, "neg_hits": neg_hits}
#     # both or neither → uncertain
#     return None, "heuristic_uncertain", {"pos_hits": pos_hits, "neg_hits": neg_hits}

# def llm_validate(category: str, name: str, context: str, city: str, page_title: str) -> Optional[bool]:
#     """
#     Return True/False/None (None on failure). Uses both new and old OpenAI SDKs.
#     The `context` already includes generalDescription + board text + tags + review snippets.
#     """
#     api_key = os.getenv("OPENAI_API_KEY")
#     if not api_key:
#         return None

#     prompt = f"""You are validating inclusion for a travel list titled "{page_title}".
# City/Region: {city}
# Place: "{name}"
# Decision category: {category}

# Context:
# {context}

# Question: Does this place CLEARLY belong in the "{category}" category?
# Answer with only one token: YES or NO."""

#     # Try new SDK (openai>=1.0)
#     try:
#         from openai import OpenAI
#         client = OpenAI()
#         resp = client.chat.completions.create(
#             model=OPENAI_MODEL,
#             messages=[{"role": "user", "content": prompt}],
#             temperature=0,
#         )
#         out = (resp.choices[0].message.content or "").strip().lower()
#         if out.startswith("yes"):
#             return True
#         if out.startswith("no"):
#             return False
#     except Exception:
#         pass

#     # Fallback to old SDK (openai<1.0)
#     try:
#         import openai as _openai
#         _openai.api_key = api_key
#         resp = _openai.ChatCompletion.create(
#             model=OPENAI_MODEL,
#             messages=[{"role": "user", "content": prompt}],
#             temperature=0,
#         )
#         out = resp.choices[0].message["content"].strip().lower()
#         if out.startswith("yes"):
#             return True
#         if out.startswith("no"):
#             return False
#     except Exception:
#         return None

#     return None


# # ---------- Merge MOBX sources ----------
# def merge_metadata_and_blocks(place_meta: List[Dict[str, Any]],
#                               blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
#     meta_map: Dict[str, Dict[str, Any]] = {}
#     for p in place_meta or []:
#         pid = str(p.get("placeId") or p.get("id") or "")
#         if not pid:
#             pid = f"NAME::{(p.get('name') or '').strip()}"
#         meta_map[pid] = {
#             "placeId": p.get("placeId") or p.get("id"),
#             "name": p.get("name"),
#             "rating": p.get("rating"),
#             "numRatings": p.get("numRatings"),
#             "priceLevel": p.get("priceLevel"),
#             "openingPeriods": p.get("openingPeriods") or [],
#             "internationalPhoneNumber": p.get("internationalPhoneNumber"),
#             "address": p.get("address"),
#             "utcOffset": p.get("utcOffset"),
#             "categories": p.get("categories") or [],
#             "generalDescription": p.get("generatedDescription") or p.get("description"),
#             "raw_reviews": p.get("reviews") or [],
#             "ratingDistribution": p.get("ratingDistribution") or {},
#             "permanentlyClosed": bool(p.get("permanentlyClosed")),
#             "imageKeys": p.get("imageKeys") or [],
#             "sources": p.get("sources") or [],
#             "minMinutesSpent": p.get("minMinutesSpent"),
#             "maxMinutesSpent": p.get("maxMinutesSpent"),
#             "website": p.get("website"),
#         }
#     # enrich with blocks
#     for b in blocks or []:
#         if b.get("type") != "place":
#             continue
#         place = b.get("place") or {}
#         pid = str(place.get("placeId") or "")
#         key = pid if pid else f"NAME::{place.get('name') or ''}"
#         if key not in meta_map:
#             meta_map[key] = {"placeId": pid or None, "name": place.get("name")}
#         text_ops = ((b.get("text") or {}).get("ops") or [])
#         addendum = "".join([t.get("insert", "") for t in text_ops if isinstance(t, dict)])
#         meta_map[key].update({
#             "latitude": place.get("latitude"),
#             "longitude": place.get("longitude"),
#             "block_id": b.get("id"),
#             "block_desc": addendum.strip() or None,
#             "block_imageKeys": b.get("imageKeys") or [],
#             "selectedImageKey": b.get("selectedImageKey"),
#         })
#     merged = list(meta_map.values())
#     merged.sort(key=lambda r: (999999 if r.get("block_id") is None else int(r.get("block_id")), -(r.get("rating") or 0)))
#     return merged

# # ---------- Scoring/Trim ----------
# def score_item(it: Dict[str, Any]) -> float:
#     rating = float(it.get("rating") or 0.0)
#     num = float(it.get("numRatings") or 0.0)
#     desc_bonus = 0.2 if it.get("generalDescription") else 0.0
#     vol = math.log10(max(1.0, num + 1.0))
#     return 0.6 * rating + 0.3 * vol + 0.1 * desc_bonus

# def trim_and_light_shuffle(items: List[Dict[str, Any]], keep_ratio: float = 0.7, seed: int = 42, max_displacement: int = 2) -> List[Dict[str, Any]]:
#     import random
#     rng = random.Random(seed)
#     n = len(items)
#     k = max(1, int(math.ceil(n * keep_ratio))) if n else 0
#     ranked = sorted(items, key=score_item, reverse=True)[:k]
#     for i in range(len(ranked)):
#         j = min(len(ranked) - 1, max(0, i + rng.randint(-max_displacement, max_displacement)))
#         if i != j:
#             ranked[i], ranked[j] = ranked[j], ranked[i]
#     for idx, it in enumerate(ranked, start=1):
#         it["_final_index"] = idx
#     return ranked

# # ---------- Playlist builder ----------
# def build_playlist_doc(title: str, city: str, url: str, items: List[Dict[str, Any]], subtype: str = "destination") -> Dict[str, Any]:
#     m = re.search(r"/(\d+)(?:$|[?#])", url)
#     geo_id = m.group(1) if m else "1000"
#     list_id = f"TEMP-{geo_id}"

#     playlist = {
#         "list_id": list_id,
#         "imageUrl": f"https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg",
#         "description": default_description(title),
#         "source_urls": [url],
#         "source": "original",
#         "category": "Travel",
#         "title": title,
#         "city_id": city,
#         "subtype": subtype,
#         "city": city,
#         "created_ts": int(time.time()),
#         "slug": build_slug(title, city, subtype, url),
#         "subcollections": {"places": []}
#     }
#     for it in items:
#         pid = it.get("placeId")
#         reviews = pick_alternate_reviews(it.get("raw_reviews") or [], k=3)
#         place_doc = {
#             "_id": pid or (it.get("name") or "unknown"),
#             "generalDescription": it.get("generalDescription"),
#             "utcOffset": int(it.get("utcOffset") if it.get("utcOffset") is not None else 330),
#             "maxMinutesSpent": it.get("maxMinutesSpent"),
#             "longitude": it.get("longitude"),
#             "rating": it.get("rating") or 0,
#             "numRatings": it.get("numRatings") or 0,
#             "sources": it.get("sources") or [],
#             "imageKeys": (it.get("imageKeys") or []) + (it.get("block_imageKeys") or []),
#             "openingPeriods": it.get("openingPeriods") or [],
#             "name": it.get("name"),
#             "placeId": pid,
#             "internationalPhoneNumber": it.get("internationalPhoneNumber"),
#             "reviews": reviews,
#             "permanentlyClosed": bool(it.get("permanentlyClosed")),
#             "priceLevel": it.get("priceLevel"),
#             "tripadvisorRating": 0,
#             "description": None,
#             "website": it.get("website"),
#             "index": it.get("_final_index") or 1,
#             "id": "",
#             "categories": it.get("categories") or [],
#             "tripadvisorNumRatings": 0,
#             "g_image_urls": [],  # trial: not uploading
#             "ratingDistribution": it.get("ratingDistribution") or {},
#             "minMinutesSpent": it.get("minMinutesSpent"),
#             "latitude": it.get("latitude"),
#             "address": it.get("address"),
#             "travel_time": it.get("travel_time")
#         }
#         playlist["subcollections"]["places"].append(place_doc)
#     playlist["subcollections"]["places"].sort(key=lambda x: x["index"])
#     return playlist

# # ---------- Dataset loader ----------
# def build_jobs_from_args(url: Optional[str], city: Optional[str], dataset_path: Optional[str]) -> List[Dict[str, Any]]:
#     jobs: List[Dict[str, Any]] = []
#     if dataset_path:
#         ds = json.loads(Path(dataset_path).read_text(encoding="utf-8"))
#         for city_name, cats in ds.items():
#             for category_name, entries in (cats or {}).items():
#                 for entry in entries or []:
#                     jobs.append({
#                         "city": city_name,
#                         "category_hint": category_name.lower(),
#                         "title": entry.get("title") or "",
#                         "url": entry.get("url") or ""
#                     })
#     elif url and city:
#         jobs.append({"city": city, "category_hint": None, "title": "", "url": url})
#     return [j for j in jobs if j.get("url")]

# def job_out_basename(job: Dict[str, Any], page_title: str) -> str:
#     title = page_title or job.get("title") or "playlist"
#     city = job.get("city") or "city"
#     m = re.search(r"/(\d+)(?:$|[?#])", job.get("url") or "")
#     geo_id = m.group(1) if m else "id"
#     return f"{slugify(city)}_{slugify(detect_category_from_title(title))}_{geo_id}"

# # ---------- Spider ----------
# class WanderlogPlaylistTrialSpider(scrapy.Spider):
#     name = "wanderlog_playlist_trial"

#     custom_settings = {
#         "LOG_LEVEL": "INFO",
#     }

#     def __init__(self, jobs: List[Dict[str, Any]], out_dir: str, keep_ratio: float, use_llm: bool, use_selenium: bool, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.jobs = jobs
#         self.out_dir = Path(out_dir)
#         self.keep_ratio = float(keep_ratio)
#         self.use_llm = bool(use_llm and (OPENAI is not None))
#         self.use_selenium = bool(use_selenium)
#         self.out_dir.mkdir(parents=True, exist_ok=True)
#         self.aggregate_report: Dict[str, Any] = {
#         "script_version": SCRIPT_VERSION,
#         "start_ts": int(time.time()),
#         "llm_enabled_flag": bool(use_llm),
#         "llm_client_loaded": bool(os.getenv("OPENAI_API_KEY")),  # <— changed
#         "model": OPENAI_MODEL if (use_llm and os.getenv("OPENAI_API_KEY")) else None,
#         "jobs": []
#         }


#     def start_requests(self):
#         # Selenium optional: we just use normal Request (MOBX is server-side)
#         for idx, job in enumerate(self.jobs):
#             meta = {"job": job, "job_index": idx}
#             yield scrapy.Request(url=job["url"], callback=self.parse_page, meta=meta)

#     def parse_page(self, response):
#         job = response.meta["job"]
#         job_index = response.meta["job_index"]

#         # Extract MOBX
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
#         if not script_text:
#             self.logger.error(f"[{job_index}] MOBX not found: {job['url']}")
#             return
#         m = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text, re.DOTALL)
#         if not m:
#             self.logger.error(f"[{job_index}] MOBX parse failed: {job['url']}")
#             return
#         mobx_json = json.loads(m.group(1))

#         data = (mobx_json.get("placesListPage") or {}).get("data") or {}
#         page_title = data.get("title") or (job.get("title") or "Untitled")
#         place_meta = data.get("placeMetadata") or []
#         blocks = []
#         for sec in (data.get("boardSections") or []):
#             for b in (sec.get("blocks") or []):
#                 if b.get("type") == "place":
#                     blocks.append(b)

#         # Detect category
#         category = (job.get("category_hint") or "").lower().strip() or detect_category_from_title(page_title)

#         # Merge
#         merged = merge_metadata_and_blocks(place_meta, blocks)

#         # QC pass: heuristic then LLM (for uncertain only)
#         accepted, rejected = [], []
#         llm_calls = 0
#         place_reports: List[Dict[str, Any]] = []

#         # Determine category from page title (or from dataset key if you already pass it)
# category_used = self.category or infer_category_from_title(title)

# for it in merged:
#     name = it.get("name") or ""
#     cats = it.get("categories") or []

#     # Build rich context — this is where generalDescription is leveraged
#     context_for_llm = build_llm_context(it, category=category_used)

#     # Heuristic check stays as-is (beach-focused). It uses the same context so it
#     # can also match keywords found inside generalDescription.
#     verdict, reason = heuristic_is_beach(name, context_for_llm, cats, title)

#     llm_used = False
#     llm_answer: Optional[bool] = None

#     # Only call LLM if heuristic is uncertain
#     if verdict is None and self.use_llm:
#         llm_answer = llm_validate(
#             category=category_used,
#             name=name,
#             context=context_for_llm,
#             city=self.city,
#             page_title=title
#         )
#         llm_used = llm_answer is not None
#         if llm_answer is True:
#             verdict, reason = True, "llm_yes"
#         elif llm_answer is False:
#             verdict, reason = False, "llm_no"
#         else:
#             verdict, reason = False, "llm_failed"

#     # Collect reporting (you already have a decisions list; add context_used_excerpt)
#     decision_record = {
#         "placeId": it.get("placeId"),
#         "name": name,
#         "decided_by": ("heuristic" if verdict is not None and not llm_used else
#                        "llm" if llm_used else "fallback"),
#         "accepted": bool(verdict),
#         "reason": reason,
#         "pos_hits": [],  # if you track your keyword hits
#         "neg_hits": [],
#         "context_used_excerpt": context_for_llm[:220]  # show what we fed the model
#     }
#     decisions.append(decision_record)

#     if verdict is True:
#         cleaned.append(it)

     
#         # Trim + light shuffle
#         curated = trim_and_light_shuffle(accepted, keep_ratio=self.keep_ratio)

#         # Build playlist JSON
#         playlist = build_playlist_doc(title=page_title, city=job.get("city") or "India", url=response.url, items=curated)

#         # Write playlist + report
#         base = job_out_basename(job, page_title)
#         playlist_path = self.out_dir / f"{base}.json"
#         report_path = self.out_dir / f"{base}.report.json"

#         playlist_path.write_text(json.dumps(playlist, ensure_ascii=False, indent=2), encoding="utf-8")

#         job_report = {
#             "job_index": job_index,
#             "city": job.get("city"),
#             "category_used": category,
#             "url": job.get("url"),
#             "title": page_title,
#             "counts": {
#                 "meta": len(place_meta),
#                 "blocks": len(blocks),
#                 "merged": len(merged),
#                 "accepted_before_trim": len(accepted),
#                 "rejected": len(rejected),
#                 "accepted_final": len(curated),
#             },
#             "llm": {
#                 "enabled": self.use_llm,
#                 "model": OPENAI_MODEL if self.use_llm else None,
#                 "calls": llm_calls
#             },
#             "decisions": place_reports,
#             "out_files": {
#                 "playlist_json": str(playlist_path),
#                 "report_json": str(report_path)
#             },
#             "created_ts": int(time.time())
#         }
#         report_path.write_text(json.dumps(job_report, ensure_ascii=False, indent=2), encoding="utf-8")

#         # add to aggregate
#         self.aggregate_report["jobs"].append(job_report)

#         self.logger.info(f"[{job_index}] {page_title} → kept {len(curated)}/{len(merged)} | LLM calls={llm_calls}")
#         self.logger.info(f"  wrote: {playlist_path}")
#         self.logger.info(f"  wrote: {report_path}")

#     def closed(self, reason):
#         self.aggregate_report.update({
#             "end_ts": int(time.time()),
#             "finish_reason": reason
#         })
#         agg_path = self.out_dir / "_aggregate_report.json"
#         agg_path.write_text(json.dumps(self.aggregate_report, ensure_ascii=False, indent=2), encoding="utf-8")
#         self.logger.info(f"Aggregate report → {agg_path}")

# # ---------- Runner ----------
# def main():
#     parser = argparse.ArgumentParser(description="Trial: Build curated playlists from Wanderlog with heuristic+LLM QC.")
#     g = parser.add_mutually_exclusive_group(required=True)
#     g.add_argument("--url", help="Single Wanderlog list URL")
#     g.add_argument("--dataset-file", help="Path to JSON dataset (city -> category -> [{title,url}])")

#     parser.add_argument("--city", help="City name (required if using --url)")
#     parser.add_argument("--out-dir", default="trial_playlists", help="Output directory for JSONs and reports")
#     parser.add_argument("--keep-ratio", type=float, default=0.7, help="Fraction to keep after trimming (0..1)")
#     parser.add_argument("--use-llm", action="store_true", help="Enable LLM for uncertain cases (requires OPENAI_API_KEY)")
#     parser.add_argument("--use-selenium", action="store_true", help="(Optional) Use selenium; typically not needed for this site")

#     args = parser.parse_args()

#     if args.url and not args.city:
#         parser.error("--city is required when using --url")

#     jobs = build_jobs_from_args(args.url, args.city, args.dataset_file)
#     if not jobs:
#         print("No jobs to process (check --url/--city or --dataset-file).")
#         return

#     # If you decide to wire Selenium later, you can pass settings into CrawlerProcess
#     process = CrawlerProcess(settings={
#         "LOG_LEVEL": "INFO",
#         # (optional) If you truly want Selenium:
#         # "DOWNLOADER_MIDDLEWARES": {"scrapy_selenium.SeleniumMiddleware": 800},
#         # "SELENIUM_DRIVER_NAME": "chrome",
#         # "SELENIUM_DRIVER_EXECUTABLE_PATH": ChromeDriverManager().install(),
#         # "SELENIUM_DRIVER_ARGUMENTS": ["--headless=new","--no-sandbox","--disable-gpu","--window-size=1600,1200"],
#     })
#     process.crawl(
#         WanderlogPlaylistTrialSpider,
#         jobs=jobs,
#         out_dir=args.out_dir,
#         keep_ratio=args.keep_ratio,
#         use_llm=args.use_llm,
#         use_selenium=args.use_selenium
#     )
#     process.start()

# if __name__ == "__main__":
#     main()


#Version working
# import os
# import re
# import json
# import time
# import math
# import argparse
# from pathlib import Path
# from typing import Any, Dict, List, Optional, Tuple
# from datetime import datetime, timezone
# from slugify import slugify

# import scrapy
# from scrapy.crawler import CrawlerProcess
# from scrapy_selenium import SeleniumRequest

# # ---------- OPTIONAL LLM (OpenAI) ----------
# OPENAI = None
# if os.environ.get("OPENAI_API_KEY"):
#     try:
#         import openai
#         OPENAI = openai
#         OPENAI.api_key = os.environ["OPENAI_API_KEY"]
#     except Exception:
#         OPENAI = None  # fallback to heuristic silently

# # ---------- Utility helpers ----------

# def iso_to_epoch_seconds(iso_str: str) -> Optional[int]:
#     """Convert ISO time (e.g., 2025-08-28T04:38:33.000Z) -> epoch seconds (int)."""
#     if not iso_str:
#         return None
#     try:
#         dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
#         return int(dt.timestamp())
#     except Exception:
#         return None

# def default_description(title: str) -> str:
#     return (f'Dive into “{title}” — a handpicked list of places with quick notes, links, '
#             f'and essentials for fast trip planning and discovery.')

# def build_slug(title: str, city: str, subtype: str, url: str) -> str:
#     # try to grab the numeric id from URL
#     m = re.search(r"/(\d+)(?:$|[?#])", url) or re.search(r"/(\d+)$", url)
#     tid = m.group(1) if m else "list"
#     base = f"{slugify(title)}-{slugify(city)}-{slugify(subtype)}-{tid}"
#     return base

# def clean_text(text: Optional[str]) -> Optional[str]:
#     if not text:
#         return text
#     return text.replace("\u2019", "'").replace("\u2014", "-").strip()

# def pick_alternate_reviews(reviews: List[Dict[str, Any]], k: int = 3) -> List[Dict[str, Any]]:
#     """
#     Sort by rating DESC then time DESC (if present), then pick alternating indices (0,2,4) up to k.
#     Map to target schema. profile_photo_url & relative_time_description not available -> empty.
#     """
#     if not reviews:
#         return []
#     def _key(r):
#         rating = r.get("rating", 0)
#         t = r.get("time", "")
#         ts = iso_to_epoch_seconds(t) or 0
#         return (rating, ts)
#     sorted_reviews = sorted(reviews, key=_key, reverse=True)
#     picked = []
#     for i, r in enumerate(sorted_reviews):
#         if len(picked) >= k:
#             break
#         if i % 2 == 0:  # alternate
#             mapped = {
#                 "rating": int(r.get("rating", 0)),
#                 "text": clean_text(r.get("reviewText")),
#                 "author_name": r.get("reviewerName") or "",
#                 "relative_time_description": "",
#                 "time": iso_to_epoch_seconds(r.get("time")) or 0,
#                 "profile_photo_url": ""
#             }
#             picked.append(mapped)
#     return picked

# # ---------- Heuristic category filter (beach) ----------

# POSITIVE_BEACH = re.compile(
#     r"\b(beach|seaface|sea\s*face|sea\s*front|shore|coast|bay|chowpatty|sand|sands)\b",
#     re.IGNORECASE
# )
# NEGATIVE_BEACH = re.compile(
#     r"\b(temple|mandir|church|museum|mall|market|fort|playground|bank|school|hospital|"
#     r"crocodile|palace|tower|bridge|station|asylum|asphalt|cinema|theater|theatre|"
#     r"atm|office|court|college|university|monument)\b",
#     re.IGNORECASE
# )

# def heuristic_is_beach(name: str, desc: str, cats: List[str], page_title: str) -> Tuple[bool, str]:
#     blob = " ".join([name or "", desc or "", " ".join(cats or []), page_title or ""])
#     if NEGATIVE_BEACH.search(blob) and not POSITIVE_BEACH.search(blob):
#         return False, "neg-keyword"
#     if POSITIVE_BEACH.search(blob):
#         return True, "pos-keyword"
#     # uncertain
#     return None, "uncertain"

# def llm_is_beach(name: str, desc: str, city: str, page_title: str) -> Optional[bool]:
#     """Optional LLM validator; returns True/False/None (None on failure)."""
#     if not OPENAI:
#         return None
#     prompt = f"""You are validating category inclusion for a travel playlist titled "{page_title}".
# Question: Is the place "{name}" a beach or directly relevant to beaches around {city}? 
# Consider the description: "{desc or ''}".
# Respond with just 'YES' or 'NO'."""
#     try:
#         # Support both old and new clients; simplest compat path:
#         resp = OPENAI.ChatCompletion.create(
#             model="gpt-4o-mini",
#             messages=[{"role":"user","content": prompt}],
#             temperature=0
#         )
#         out = resp.choices[0].message["content"].strip().lower()
#         if "yes" in out[:5]:
#             return True
#         if "no" in out[:5]:
#             return False
#     except Exception:
#         return None
#     return None

# # ---------- Merging logic ----------

# def merge_metadata_and_blocks(place_meta: List[Dict[str, Any]],
#                               blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
#     """
#     Return list of items with merged fields.
#     - prefer place_meta for factuals (rating, counts, descriptions, opening)
#     - use blocks for order/index, coords, selectedImageKey, imageKeys
#     """
#     meta_map: Dict[str, Dict[str, Any]] = {}

#     for p in place_meta:
#         pid = str(p.get("placeId") or p.get("id") or "")
#         if not pid:
#             # fallback on name, rarely needed
#             pid = f"NAME::{p.get('name','').strip()}"
#         meta_map[pid] = {
#             "placeId": p.get("placeId") or p.get("id"),
#             "name": p.get("name"),
#             "rating": p.get("rating"),
#             "numRatings": p.get("numRatings"),
#             "priceLevel": p.get("priceLevel"),
#             "openingPeriods": p.get("openingPeriods") or [],
#             "internationalPhoneNumber": p.get("internationalPhoneNumber"),
#             "address": p.get("address"),
#             "utcOffset": p.get("utcOffset"),
#             "categories": p.get("categories") or [],
#             "generalDescription": p.get("generatedDescription") or p.get("description"),
#             "raw_reviews": p.get("reviews") or [],  # input list with (time, reviewerName, rating, reviewText)
#             "ratingDistribution": p.get("ratingDistribution") or {},
#             "permanentlyClosed": bool(p.get("permanentlyClosed")),
#             "imageKeys": p.get("imageKeys") or [],
#             "sources": p.get("sources") or [],
#             "minMinutesSpent": p.get("minMinutesSpent"),
#             "maxMinutesSpent": p.get("maxMinutesSpent"),
#         }

#     # enrich with blocks (coords, order, description from text.ops)
#     for b in blocks:
#         if b.get("type") != "place":
#             continue
#         place = b.get("place") or {}
#         pid = str(place.get("placeId") or "")
#         name = place.get("name")
#         lat = place.get("latitude")
#         lng = place.get("longitude")
#         # fallback key if pid missing
#         key = pid if pid else f"NAME::{name or ''}"
#         if key not in meta_map:
#             meta_map[key] = {"placeId": pid or None, "name": name}
#         # text.ops -> description addendum
#         text_ops = ((b.get("text") or {}).get("ops") or [])
#         addendum = "".join([t.get("insert","") for t in text_ops if isinstance(t, dict)])
#         meta_map[key].update({
#             "latitude": lat,
#             "longitude": lng,
#             "block_id": b.get("id"),
#             "block_desc": addendum.strip() or None,
#             "selectedImageKey": b.get("selectedImageKey"),
#             "block_imageKeys": b.get("imageKeys") or []
#         })

#     # merge block desc if generalDescription absent
#     merged = []
#     for pid, row in meta_map.items():
#         if not row.get("generalDescription") and row.get("block_desc"):
#             row["generalDescription"] = row["block_desc"]
#         merged.append(row)

#     # initial order: by block id if present, else by rating desc
#     merged.sort(key=lambda r: (999999 if r.get("block_id") is None else int(r.get("block_id")), -(r.get("rating") or 0)))
#     return merged

# # ---------- Trimming & light shuffle ----------

# def score_item(it: Dict[str, Any]) -> float:
#     rating = float(it.get("rating") or 0.0)
#     num = float(it.get("numRatings") or 0.0)
#     desc_bonus = 0.2 if it.get("generalDescription") else 0.0
#     # log boost for volume
#     vol = math.log10(max(1.0, num + 1.0))
#     return 0.6 * rating + 0.3 * vol + 0.1 * desc_bonus

# def trim_and_light_shuffle(items: List[Dict[str, Any]], keep_ratio: float = 0.7, seed: int = 42, max_displacement: int = 2) -> List[Dict[str, Any]]:
#     import random
#     rng = random.Random(seed)
#     n = len(items)
#     k = max(1, int(math.ceil(n * keep_ratio)))
#     # rank by score
#     ranked = sorted(items, key=score_item, reverse=True)[:k]
#     # light shuffle within ±max_displacement
#     for i in range(len(ranked)):
#         j = min(len(ranked) - 1, max(0, i + rng.randint(-max_displacement, max_displacement)))
#         if i != j:
#             ranked[i], ranked[j] = ranked[j], ranked[i]
#     # reindex 1-based
#     for idx, it in enumerate(ranked, start=1):
#         it["_final_index"] = idx
#     return ranked

# # ---------- Playlist builder ----------

# def build_playlist_doc(title: str, city: str, url: str, items: List[Dict[str, Any]], subtype: str = "destination") -> Dict[str, Any]:
#     # derive a TEMP list id from geoCategory numeric id if available
#     m = re.search(r"/(\d+)(?:$|[?#])", url)
#     geo_id = m.group(1) if m else "1000"
#     list_id = f"TEMP-{geo_id}"

#     playlist = {
#         "list_id": list_id,
#         "imageUrl": f"https://storage.googleapis.com/mycasavsc.appspot.com/playlistsNew_images/{list_id}/1.jpg",
#         "description": default_description(title),
#         "source_urls": [url],
#         "source": "original",
#         "category": "Travel",
#         "title": title,
#         "city_id": city,           # trial: use city name directly
#         "subtype": subtype,
#         "city": city,
#         "created_ts": int(time.time()),
#         "slug": build_slug(title, city, subtype, url),
#         "subcollections": {"places": []}
#     }

#     for it in items:
#         pid = it.get("placeId")
#         reviews = pick_alternate_reviews(it.get("raw_reviews") or [], k=3)

#         place_doc = {
#             "_id": pid or (it.get("name") or "unknown"),
#             "generalDescription": it.get("generalDescription"),
#             "utcOffset": int(it.get("utcOffset") if it.get("utcOffset") is not None else 330),
#             "maxMinutesSpent": it.get("maxMinutesSpent"),
#             "longitude": it.get("longitude"),
#             "rating": it.get("rating") or 0,
#             "numRatings": it.get("numRatings") or 0,
#             "sources": it.get("sources") or [],
#             "imageKeys": (it.get("imageKeys") or []) + (it.get("block_imageKeys") or []),
#             "openingPeriods": it.get("openingPeriods") or [],
#             "name": it.get("name"),
#             "placeId": pid,
#             "internationalPhoneNumber": it.get("internationalPhoneNumber"),
#             "reviews": reviews,
#             "permanentlyClosed": bool(it.get("permanentlyClosed")),
#             "priceLevel": it.get("priceLevel"),
#             "tripadvisorRating": 0,
#             "description": None,
#             "website": it.get("website"),
#             "index": it.get("_final_index") or 1,
#             "id": "",  # per your schema
#             "categories": it.get("categories") or ["beach", "point_of_interest", "establishment"],
#             "tripadvisorNumRatings": 0,
#             "g_image_urls": [],  # TRIAL: empty (no storage writes)
#             "ratingDistribution": it.get("ratingDistribution") or {},
#             "minMinutesSpent": it.get("minMinutesSpent"),
#             "latitude": it.get("latitude"),
#             "address": it.get("address"),
#             "travel_time": it.get("travel_time")
#         }
#         playlist["subcollections"]["places"].append(place_doc)

#     # sort by index just in case
#     playlist["subcollections"]["places"].sort(key=lambda x: x["index"])
#     return playlist

# # ---------- Spider ----------

# class WanderlogPlaylistTrialSpider(scrapy.Spider):
#     name = "wanderlog_playlist_trial"

#     custom_settings = {
#         "LOG_LEVEL": "INFO",
#         # Selenium middleware
#         "DOWNLOADER_MIDDLEWARES": {
#             "scrapy_selenium.SeleniumMiddleware": 800
#         },
#         # Headless Chrome config (webdriver-manager finds the binary)
#         "SELENIUM_DRIVER_NAME": "chrome",
#         "SELENIUM_DRIVER_ARGUMENTS": ["--headless=new", "--no-sandbox", "--disable-gpu", "--window-size=1600,1200"],
#     }

#     def __init__(self, url: str, city: str, out_path: str, keep_ratio: float = 0.7, use_llm: bool = False, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.start_url = url
#         self.city = city
#         self.out_path = out_path
#         self.keep_ratio = float(keep_ratio)
#         self.use_llm = bool(use_llm)
#         Path(self.out_path).parent.mkdir(parents=True, exist_ok=True)

#     def start_requests(self):
#         yield SeleniumRequest(
#             url=self.start_url,
#             callback=self.parse_page,
#             wait_time=3,
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse_page(self, response):
#         # 1) Extract MOBX
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
#         if not script_text:
#             self.logger.error("Could not find window.__MOBX_STATE__ on page")
#             return
#         m = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text, re.DOTALL)
#         if not m:
#             self.logger.error("Failed to parse __MOBX_STATE__ JSON blob")
#             return
#         mobx_json = json.loads(m.group(1))

#         data = (mobx_json.get("placesListPage") or {}).get("data") or {}
#         title = data.get("title") or "Untitled"
#         place_meta = data.get("placeMetadata") or []
#         board_sections = data.get("boardSections") or []
#         blocks = []
#         for sec in board_sections:
#             bl = sec.get("blocks") or []
#             for b in bl:
#                 if b.get("type") == "place":
#                     blocks.append(b)

#         self.logger.info(f"Found meta={len(place_meta)} places, blocks={len(blocks)}")

#         # 2) Merge
#         merged = merge_metadata_and_blocks(place_meta, blocks)

#         # 3) Category QC (beaches)
#         cleaned: List[Dict[str, Any]] = []
#         for it in merged:
#             name = it.get("name") or ""
#             cats = it.get("categories") or []
#             desc = f"{it.get('generalDescription') or ''} {it.get('block_desc') or ''}"
#             verdict, reason = heuristic_is_beach(name, desc, cats, title)
#             if verdict is None and self.use_llm:
#                 v = llm_is_beach(name=name, desc=desc, city=self.city, page_title=title)
#                 verdict = v
#                 reason = "llm" if v is not None else "uncertain-fallback"
#             if verdict is True:
#                 cleaned.append(it)
#             else:
#                 self.logger.debug(f"Filtered out (reason={reason}): {name}")

#         self.logger.info(f"After beach QC: {len(cleaned)} kept")

#         # 4) Reviews mapping (limit to 3 alternates) handled later in build

#         # 5) Trim & lightly shuffle
#         curated = trim_and_light_shuffle(cleaned, keep_ratio=self.keep_ratio)

#         # 6) Build playlist JSON (trial: no storage writes, g_image_urls empty)
#         playlist = build_playlist_doc(title=title, city=self.city, url=response.url, items=curated)

#         # 7) Write to disk
#         Path(self.out_path).write_text(json.dumps(playlist, ensure_ascii=False, indent=2), encoding="utf-8")
#         self.logger.info(f"Wrote trial playlist → {self.out_path}")

# # ---------- Runner ----------

# def main():
#     parser = argparse.ArgumentParser(description="Trial: build playlist JSON from Wanderlog (Scrapy + Selenium).")
#     parser.add_argument("--url", required=True, help="Wanderlog list URL (e.g., https://wanderlog.com/list/geoCategory/109807)")
#     parser.add_argument("--city", required=True, help="City name for playlist doc (e.g., Mumbai)")
#     parser.add_argument("--out", default="trial_playlists/playlist_trial.json", help="Output JSON path")
#     parser.add_argument("--keep-ratio", type=float, default=0.7, help="Fraction to keep after trimming (0..1)")
#     parser.add_argument("--use-llm", action="store_true", help="Enable LLM validation if OPENAI_API_KEY is set")
#     args = parser.parse_args()

#     # Selenium driver path via webdriver-manager (programmatic)
#     from webdriver_manager.chrome import ChromeDriverManager
#     ChromeDriverManager().install()
#     # driver_path = ChromeDriverManager().install()
#     # # pass executable path to Scrapy settings at runtime
#     # settings = {
#     #     "SELENIUM_DRIVER_EXECUTABLE_PATH": driver_path,
#     # }

#     process = CrawlerProcess()
#     process.crawl(
#         WanderlogPlaylistTrialSpider,
#         url=args.url,
#         city=args.city,
#         out_path=args.out,
#         keep_ratio=args.keep_ratio,
#         use_llm=args.use_llm and (OPENAI is not None)
#     )
#     process.start()

# if __name__ == "__main__":
#     main()
