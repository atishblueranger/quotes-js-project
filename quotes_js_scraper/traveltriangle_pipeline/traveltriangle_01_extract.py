#!/usr/bin/env python3
"""
TravelTriangle Step 1 — Extract & AI Optimize
Extracts playlist data from TravelTriangle blog URLs with AI-enhanced titles/descriptions.
"""

import os, re, json, time, hashlib, argparse
from pathlib import Path
from typing import Dict, Any, List, Optional
import requests
from bs4 import BeautifulSoup

# ==================== CONFIGURATION ====================
# 🔴 HARDCODED KEY (Paste your full key here)
OPENAI_API_KEY = "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"
OPENAI_MODEL = "gpt-5-mini"

SOURCE_TAG = "traveltriangle"
CATEGORY_DEFAULT = "Travel"

# AI parameters
MIN_TITLE_LENGTH = 2
MAX_TITLE_LENGTH = 5
DESCRIPTION_TARGET = 90
DESCRIPTION_MIN = 60
DESCRIPTION_MAX = 110

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0 Safari/537.36"
)

# ==================== AI SETUP & DIAGNOSTICS ====================
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

def check_ai_status():
    print(f"\n🤖 AI DIAGNOSTICS:")
    if not OPENAI_AVAILABLE:
        print(f"   ❌ OpenAI Library: NOT FOUND (Run: 'pip install openai')")
        return False
    else:
        print(f"   ✅ OpenAI Library: Installed")

    if not OPENAI_API_KEY or "sk-" not in OPENAI_API_KEY:
        print(f"   ❌ API Key: MISSING or INVALID")
        return False
    else:
        print(f"   ✅ API Key: Loaded ({OPENAI_API_KEY[:10]}...)")
    
    return True

# ==================== CACHE SETUP ====================
CACHE_DIR = Path("cache")
HTML_CACHE = CACHE_DIR / "html"
AI_CACHE = CACHE_DIR / "ai_cache.json"

CACHE_DIR.mkdir(exist_ok=True, parents=True)
HTML_CACHE.mkdir(exist_ok=True, parents=True)

# ==================== HELPERS ====================
def clean_txt(s: Optional[str]) -> str:
    if not s: return ""
    s = s.replace("\u2019", "'").replace("\u2014", "-")
    return re.sub(r"\s+", " ", s).strip()

def strip_number_prefix(s: str) -> str:
    s = re.sub(r"^\s*\d+[\.\)]\s*", "", s or "")
    return re.sub(r"\s+", " ", s).strip()

def cached_html_path(url: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
    return HTML_CACHE / f"{safe[:150]}.html"

def load_page_html(url: str) -> str:
    cache_file = cached_html_path(url)
    if cache_file.exists():
        return cache_file.read_text(encoding="utf-8")
    
    print(f"  📥 Fetching HTML...")
    resp = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=30)
    resp.raise_for_status()
    html = resp.text
    cache_file.write_text(html, encoding="utf-8")
    return html

# ==================== GOOGLE PLACES ====================
def gp_find_place(name: str, city: str, api_key: Optional[str], location_hint: Optional[str] = None) -> Optional[str]:
    if not api_key: return None
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    queries = []
    if location_hint: queries.append(f"{name}, {location_hint}, {city}".strip(", "))
    queries.append(f"{name}, {city}".strip(", "))
    queries.append(name)
    
    for q in queries:
        try:
            r = requests.get(
                base_url,
                params={
                    "input": q,
                    "inputtype": "textquery",
                    "fields": "place_id",
                    "key": api_key,
                },
                timeout=15,
            )
            cands = r.json().get("candidates") or []
            if cands:
                return cands[0].get("place_id")
        except Exception:
            continue
    return None

def gp_place_details(place_id: str, api_key: str) -> Dict[str, Any]:
    if not (api_key and place_id): return {}
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = "rating,user_ratings_total,formatted_address,geometry/location,website"
    try:
        r = requests.get(
            url,
            params={"place_id": place_id, "fields": fields, "key": api_key},
            timeout=20,
        )
        return r.json().get("result", {}) or {}
    except Exception:
        return {}

# ==================== AI OPTIMIZATION LOGIC ====================
def _load_ai_cache() -> Dict[str, Any]:
    if AI_CACHE.exists():
        try:
            return json.loads(AI_CACHE.read_text(encoding="utf-8"))
        except:
            return {}
    return {}

def _save_ai_cache(cache: Dict[str, Any]):
    try:
        AI_CACHE.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except:
        pass

def remove_numbers_from_title(title: str) -> str:
    patterns = [r'\b\d+\b\s*', r'^\d+\s+', r'\s+\d+\s+']
    result = title
    for pattern in patterns:
        result = re.sub(pattern, ' ', result)
    return ' '.join(result.split()).strip()

def optimize_title_with_ai(
    original: str,
    city: str,
    country: str,
    use_catchy: bool = True,
) -> Dict[str, Any]:
    simple = remove_numbers_from_title(original)
    
    if not (OPENAI_AVAILABLE and OPENAI_API_KEY):
        return {"simple": simple, "catchy": simple, "confidence": 0.5}
    
    cache = _load_ai_cache()
    cache_key = json.dumps(
        {
            "orig": original,
            "city": city,
            "country": country,
            "mode": "catchy" if use_catchy else "simple",
        },
        sort_keys=True,
    )
    
    if cache_key in cache:
        return cache[cache_key]
    
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        system = (
            "You are a travel content editor creating catchy, memorable playlist titles.\n"
            "- Return ONLY valid JSON.\n"
            '- Use key \"catchy_title\" for the final title.\n'
            "- 2–4 words.\n"
            "- No numbers.\n"
            "- Include the city name only if it fits naturally.\n"
        )
        user = f"ORIGINAL: '{original}'\nCity: {city}\nCountry: {country}"

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
            timeout=30,
        )
        data = json.loads(response.choices[0].message.content.strip())
        catchy = (data.get("catchy_title") or simple).strip()
        
        result = {"simple": simple, "catchy": catchy, "confidence": 0.9}
        cache[cache_key] = result
        _save_ai_cache(cache)
        return result
        
    except Exception as e:
        print(f"  ⚠️ AI title failed: {e}")
        return {"simple": simple, "catchy": simple, "confidence": 0.5}

def optimize_description_with_ai(
    title: str,
    city: str,
    country: str,
    existing: str = "",
) -> Dict[str, Any]:
    if not (OPENAI_AVAILABLE and OPENAI_API_KEY):
        fallback = f"Discover the best of {city}! Handpicked spots and experiences."
        return {"description": fallback, "style": "Fallback", "confidence": 0.5}
    
    cache = _load_ai_cache()
    cache_key = json.dumps(
        {"title": title, "city": city, "existing": existing[:100]},
        sort_keys=True,
    )
    
    if cache_key in cache:
        return cache[cache_key]
    
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        system = (
            f"You are a creative travel writer.\n"
            f"Write an Instagram-style description for the playlist '{title}' in {city}, {country}.\n"
            f"- Length: {DESCRIPTION_MIN}–{DESCRIPTION_MAX} characters, ideally around {DESCRIPTION_TARGET}.\n"
            "- No emojis. No hashtags.\n"
            'Return ONLY valid JSON like: { "description": "...", "confidence": 0.9 }'
        )

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": "Generate description JSON now."},
            ],
            response_format={"type": "json_object"},
            timeout=30,
        )
        data = json.loads(response.choices[0].message.content.strip())
        desc = (data.get("description") or "").strip()
        
        result = {"description": desc, "style": "AI", "confidence": float(data.get("confidence", 0.9))}
        cache[cache_key] = result
        _save_ai_cache(cache)
        return result
        
    except Exception as e:
        print(f"  ⚠️ AI description failed: {e}")
        fallback = f"Discover the best of {city}! Handpicked spots and experiences."
        return {"description": fallback, "style": "Fallback", "confidence": 0.3}

# ==================== PARSING ====================
def parse_tt_article(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    
    h2 = soup.select_one("h2.h2_Waypoints_blogpage")
    title = clean_txt(h2.get_text()) if h2 else ""
    
    desc = ""
    if h2:
        nxt = h2.find_next_sibling()
        while nxt and nxt.name != "p":
            nxt = nxt.find_next_sibling()
        if nxt and nxt.name == "p":
            desc = clean_txt(nxt.get_text())
    
    places: List[Dict[str, Any]] = []
    
    # GARBAGE FILTERS
    SKIP_PATTERNS = [
        r"holiday\s+package",
        r"book\s+(?:my|a|your)?\s*(?:vacation|holiday|package|trip)",
        r"travel\s*triangle",
        r"looking\s+to\s+book",
        r"click\s+here",
        r"read\s+more",
        r"suggested\s+read",
        r"image\s+source",
        r"best\s+places\s+to\s+visit",
        r"places\s+to\s+visit\s+near",
        r"frequently\s+asked\s+questions",
        r"related\s+posts",
        r"common\s+queries",
    ]

    for h3 in soup.select("h3"):
        name = strip_number_prefix(h3.get_text())
        if not name or len(name) < 2:
            continue
        
        # Garbage check
        if any(re.search(pattern, name.lower()) for pattern in SKIP_PATTERNS):
            print(f"  ⏭️  Skipping garbage: '{name}'")
            continue
        if len(name) > 70:
            print(f"  ⏭️  Skipping long header: '{name}'")
            continue
        
        place_desc = ""
        details_p = None
        p = h3.find_next_sibling()
        first_p_taken = False
        
        while p and p.name in ("p", "div"):
            if p.name == "p" and not first_p_taken:
                place_desc = clean_txt(p.get_text())
                first_p_taken = True
            if p.name == "p" and p.find("strong"):
                details_p = p
                break
            p = p.find_next_sibling()
        
        details = {"location": "", "opening_hours": "", "entry_fee": ""}
        if details_p:
            for br in details_p.find_all("br"):
                br.replace_with("\n")
            txt = details_p.get_text("\n", strip=True)
            m_loc = re.search(r"(?i)\bLocation:\s*(.+)", txt)
            m_hrs = re.search(r"(?i)\bOpening\s*hours?:\s*(.+)", txt)
            m_fee = re.search(r"(?i)\bEntry\s*fee:\s*(.+)", txt)
            
            if m_loc:
                details["location"] = clean_txt(m_loc.group(1))
            if m_hrs:
                details["opening_hours"] = clean_txt(m_hrs.group(1))
            if m_fee:
                details["entry_fee"] = clean_txt(m_fee.group(1))
        
        places.append(
            {
                "name": name,
                "description": place_desc,
                "location_hint": details["location"],
                "opening_hours_text": details["opening_hours"],
                "entry_fee_text": details["entry_fee"],
            }
        )
    
    return {
        "playlist_title": title,
        "playlist_description": desc,
        "places": places,
    }

# ==================== MAIN ====================
def main():
    ap = argparse.ArgumentParser("TravelTriangle Step 1 — Extract & AI Optimize")
    ap.add_argument("--url", required=True)
    ap.add_argument("--city", required=True)
    ap.add_argument("--country", default="India")
    ap.add_argument("--category", default=CATEGORY_DEFAULT)
    ap.add_argument("--subtype", default="poi")
    ap.add_argument("--out", default="tt_extracted.json")
    ap.add_argument("--optimize-ai", action="store_true")
    ap.add_argument("--title-mode", choices=["simple", "catchy"], default="catchy")
    ap.add_argument("--enrich-google", action="store_true")
    args = ap.parse_args()
    
    print(f"\n🌐 Fetching: {args.url}")
    
    # DIAGNOSTIC CHECK
    if args.optimize_ai:
        check_ai_status()

    html = load_page_html(args.url)
    parsed = parse_tt_article(html)
    
    raw_title = parsed["playlist_title"] or "TravelTriangle Picks"
    raw_desc = parsed["playlist_description"] or f"Places featured in: {raw_title}"
    
    print(f"📝 Raw title: {raw_title}")
    print(f"📝 Found {len(parsed['places'])} places")
    
    # AI optimization
    final_title = raw_title
    final_desc = raw_desc
    ai_metadata = {}
    
    if args.optimize_ai:
        print(f"\n🤖 AI OPTIMIZATION")
        
        print(f"  ✂️ Optimizing title...")
        title_result = optimize_title_with_ai(
            raw_title, args.city, args.country, args.title_mode == "catchy"
        )
        final_title = (
            title_result["catchy"]
            if args.title_mode == "catchy"
            else title_result["simple"]
        )
        print(f"     ✨ {final_title} (confidence: {title_result['confidence']:.2f})")
        
        print(f"  💬 Optimizing description...")
        desc_result = optimize_description_with_ai(
            final_title, args.city, args.country, raw_desc
        )
        final_desc = desc_result["description"]
        print(f"     ✨ {desc_result['style']}: {final_desc}")
        
        ai_metadata = {
            "title_mode": args.title_mode,
            "title_confidence": title_result["confidence"],
            "desc_style": desc_result["style"],
            "desc_confidence": desc_result["confidence"],
        }
    else:
        print(f"\n⏭️ AI optimization disabled")
    
    # Google Enrich (Placeholder for consistency)
    enriched_places = parsed["places"]
    if args.enrich_google:
        print(f"\n🔍 Enriching (Google)...")
        # Using Step 2.5 for rigorous enrichment later
    
    output = {
        "playlistTitle": final_title,
        "placeName": args.city,
        "country": args.country,
        "category": args.category,
        "subtype": args.subtype,
        "source": SOURCE_TAG,
        "source_urls": [args.url],
        "description": final_desc,
        "items": enriched_places,
        "ai_metadata": ai_metadata if args.optimize_ai else {},
    }
    
    out_path = Path(args.out)
    out_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n✅ Wrote {len(output['items'])} items to {out_path}")

if __name__ == "__main__":
    main()


# #!/usr/bin/env python3
# """
# TravelTriangle Step 1 — Extract & AI Optimize
# Extracts playlist data from TravelTriangle blog URLs with AI-enhanced titles/descriptions.
# """

# import os, re, json, time, hashlib, argparse
# from pathlib import Path
# from typing import Dict, Any, List, Optional
# import requests
# from bs4 import BeautifulSoup

# # ==================== CONFIGURATION ====================
# # 🔴 HARDCODED KEY (Paste your full key here)
# OPENAI_API_KEY = "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"
# OPENAI_MODEL = "gpt-4o-mini"

# SOURCE_TAG = "traveltriangle"
# CATEGORY_DEFAULT = "Travel"

# # AI parameters
# MIN_TITLE_LENGTH = 2
# MAX_TITLE_LENGTH = 5
# DESCRIPTION_TARGET = 90
# DESCRIPTION_MIN = 60
# DESCRIPTION_MAX = 110

# DEFAULT_UA = (
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#     "AppleWebKit/537.36 (KHTML, like Gecko) "
#     "Chrome/126.0 Safari/537.36"
# )

# # ==================== AI SETUP & DIAGNOSTICS ====================
# try:
#     from openai import OpenAI
#     OPENAI_AVAILABLE = True
# except ImportError:
#     OPENAI_AVAILABLE = False

# def check_ai_status():
#     print(f"\n🤖 AI DIAGNOSTICS:")
#     if not OPENAI_AVAILABLE:
#         print(f"   ❌ OpenAI Library: NOT FOUND (Run: 'pip install openai')")
#         return False
#     else:
#         print(f"   ✅ OpenAI Library: Installed")

#     if not OPENAI_API_KEY or "sk-" not in OPENAI_API_KEY:
#         print(f"   ❌ API Key: MISSING or INVALID")
#         return False
#     else:
#         print(f"   ✅ API Key: Loaded ({OPENAI_API_KEY[:10]}...)")
    
#     return True

# # ==================== CACHE SETUP ====================
# CACHE_DIR = Path("cache")
# HTML_CACHE = CACHE_DIR / "html"
# AI_CACHE = CACHE_DIR / "ai_cache.json"

# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# HTML_CACHE.mkdir(exist_ok=True, parents=True)

# # ==================== HELPERS ====================
# def clean_txt(s: Optional[str]) -> str:
#     if not s: return ""
#     s = s.replace("\u2019", "'").replace("\u2014", "-")
#     return re.sub(r"\s+", " ", s).strip()

# def strip_number_prefix(s: str) -> str:
#     s = re.sub(r"^\s*\d+[\.\)]\s*", "", s or "")
#     return re.sub(r"\s+", " ", s).strip()

# def cached_html_path(url: str) -> Path:
#     safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
#     return HTML_CACHE / f"{safe[:150]}.html"

# def load_page_html(url: str) -> str:
#     cache_file = cached_html_path(url)
#     if cache_file.exists():
#         return cache_file.read_text(encoding="utf-8")
    
#     print(f"  📥 Fetching HTML...")
#     resp = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=30)
#     resp.raise_for_status()
#     html = resp.text
#     cache_file.write_text(html, encoding="utf-8")
#     return html

# # ==================== GOOGLE PLACES ====================
# def gp_find_place(name: str, city: str, api_key: Optional[str], location_hint: Optional[str] = None) -> Optional[str]:
#     if not api_key: return None
#     base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
#     queries = []
#     if location_hint: queries.append(f"{name}, {location_hint}, {city}".strip(", "))
#     queries.append(f"{name}, {city}".strip(", "))
#     queries.append(name)
    
#     for q in queries:
#         try:
#             r = requests.get(base_url, params={"input": q, "inputtype": "textquery", "fields": "place_id", "key": api_key}, timeout=15)
#             cands = r.json().get("candidates") or []
#             if cands: return cands[0].get("place_id")
#         except Exception: continue
#     return None

# def gp_place_details(place_id: str, api_key: str) -> Dict[str, Any]:
#     if not (api_key and place_id): return {}
#     url = "https://maps.googleapis.com/maps/api/place/details/json"
#     fields = "rating,user_ratings_total,formatted_address,geometry/location,website"
#     try:
#         r = requests.get(url, params={"place_id": place_id, "fields": fields, "key": api_key}, timeout=20)
#         return r.json().get("result", {}) or {}
#     except Exception: return {}

# # ==================== AI OPTIMIZATION LOGIC ====================
# def _load_ai_cache() -> Dict[str, Any]:
#     if AI_CACHE.exists():
#         try: return json.loads(AI_CACHE.read_text(encoding="utf-8"))
#         except: return {}
#     return {}

# def _save_ai_cache(cache: Dict[str, Any]):
#     try: AI_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
#     except: pass

# def remove_numbers_from_title(title: str) -> str:
#     patterns = [r'\b\d+\b\s*', r'^\d+\s+', r'\s+\d+\s+']
#     result = title
#     for pattern in patterns:
#         result = re.sub(pattern, ' ', result)
#     return ' '.join(result.split()).strip()

# def optimize_title_with_ai(original: str, city: str, country: str, use_catchy: bool = True) -> Dict[str, Any]:
#     simple = remove_numbers_from_title(original)
    
#     if not (OPENAI_AVAILABLE and OPENAI_API_KEY):
#         return {"simple": simple, "catchy": simple, "confidence": 0.5}
    
#     cache = _load_ai_cache()
#     cache_key = json.dumps({"orig": original, "city": city, "country": country, "mode": "catchy" if use_catchy else "simple"}, sort_keys=True)
    
#     if cache_key in cache: return cache[cache_key]
    
#     try:
#         client = OpenAI(api_key=OPENAI_API_KEY)
#         system = "You are a travel content editor creating catchy, memorable playlist titles. No numbers. 2-4 words.Return JSON"
#         user = f"ORIGINAL: '{original}'\nCity: {city}\nCountry: {country}"

#         response = client.chat.completions.create(
#             model=OPENAI_MODEL, temperature=0.7,
#             messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
#             response_format={"type": "json_object"}, timeout=30
#         )
#         data = json.loads(response.choices[0].message.content.strip())
#         catchy = (data.get("catchy_title") or simple).strip()
        
#         result = {"simple": simple, "catchy": catchy, "confidence": 0.9}
#         cache[cache_key] = result
#         _save_ai_cache(cache)
#         return result
        
#     except Exception as e:
#         print(f"  ⚠️ AI title failed: {e}")
#         return {"simple": simple, "catchy": simple, "confidence": 0.5}

# def optimize_description_with_ai(title: str, city: str, country: str, existing: str = "") -> Dict[str, Any]:
#     if not (OPENAI_AVAILABLE and OPENAI_API_KEY):
#         fallback = f"Discover the best of {city}! Handpicked spots and experiences."
#         return {"description": fallback, "style": "Fallback", "confidence": 0.5}
    
#     cache = _load_ai_cache()
#     cache_key = json.dumps({"title": title, "city": city, "existing": existing[:100]}, sort_keys=True)
    
#     if cache_key in cache: return cache[cache_key]
    
#     try:
#         client = OpenAI(api_key=OPENAI_API_KEY)
#         system = f"Creative travel writer. Write engaging description for '{title}' in {city}. Max 100 chars. Instagram style."
#         response = client.chat.completions.create(
#             model=OPENAI_MODEL, temperature=0.75,
#             messages=[{"role": "system", "content": system}, {"role": "user", "content": "Write description."}],
#             response_format={"type": "json_object"}, timeout=30
#         )
#         data = json.loads(response.choices[0].message.content.strip())
#         desc = (data.get("description") or "").strip()
        
#         result = {"description": desc, "style": "AI", "confidence": 0.9}
#         cache[cache_key] = result
#         _save_ai_cache(cache)
#         return result
        
#     except Exception as e:
#         print(f"  ⚠️ AI description failed: {e}")
#         fallback = f"Discover the best of {city}! Handpicked spots and experiences."
#         return {"description": fallback, "style": "Fallback", "confidence": 0.3}

# # ==================== PARSING ====================
# def parse_tt_article(html: str) -> Dict[str, Any]:
#     soup = BeautifulSoup(html, "lxml")
    
#     h2 = soup.select_one("h2.h2_Waypoints_blogpage")
#     title = clean_txt(h2.get_text()) if h2 else ""
    
#     desc = ""
#     if h2:
#         nxt = h2.find_next_sibling()
#         while nxt and nxt.name != "p": nxt = nxt.find_next_sibling()
#         if nxt and nxt.name == "p": desc = clean_txt(nxt.get_text())
    
#     places: List[Dict[str, Any]] = []
    
#     # GARBAGE FILTERS
#     SKIP_PATTERNS = [
#         r"holiday\s+package", r"book\s+(?:my|a|your)?\s*(?:vacation|holiday|package|trip)",
#         r"travel\s*triangle", r"looking\s+to\s+book", r"click\s+here", r"read\s+more",
#         r"suggested\s+read", r"image\s+source", r"best\s+places\s+to\s+visit", 
#         r"places\s+to\s+visit\s+near", r"frequently\s+asked\s+questions", 
#         r"related\s+posts", r"common\s+queries"
#     ]

#     for h3 in soup.select("h3"):
#         name = strip_number_prefix(h3.get_text())
#         if not name or len(name) < 2: continue
        
#         # Garbage check
#         if any(re.search(pattern, name.lower()) for pattern in SKIP_PATTERNS):
#             print(f"  ⏭️  Skipping garbage: '{name}'")
#             continue
#         if len(name) > 70: 
#              print(f"  ⏭️  Skipping long header: '{name}'")
#              continue
        
#         place_desc = ""
#         details_p = None
#         p = h3.find_next_sibling()
#         first_p_taken = False
        
#         while p and p.name in ("p", "div"):
#             if p.name == "p" and not first_p_taken:
#                 place_desc = clean_txt(p.get_text())
#                 first_p_taken = True
#             if p.name == "p" and p.find("strong"):
#                 details_p = p
#                 break
#             p = p.find_next_sibling()
        
#         details = {"location": "", "opening_hours": "", "entry_fee": ""}
#         if details_p:
#             for br in details_p.find_all("br"): br.replace_with("\n")
#             txt = details_p.get_text("\n", strip=True)
#             m_loc = re.search(r"(?i)\bLocation:\s*(.+)", txt)
#             m_hrs = re.search(r"(?i)\bOpening\s*hours?:\s*(.+)", txt)
#             m_fee = re.search(r"(?i)\bEntry\s*fee:\s*(.+)", txt)
            
#             if m_loc: details["location"] = clean_txt(m_loc.group(1))
#             if m_hrs: details["opening_hours"] = clean_txt(m_hrs.group(1))
#             if m_fee: details["entry_fee"] = clean_txt(m_fee.group(1))
        
#         places.append({
#             "name": name, "description": place_desc,
#             "location_hint": details["location"],
#             "opening_hours_text": details["opening_hours"],
#             "entry_fee_text": details["entry_fee"]
#         })
    
#     return {"playlist_title": title, "playlist_description": desc, "places": places}

# # ==================== MAIN ====================
# def main():
#     ap = argparse.ArgumentParser("TravelTriangle Step 1 — Extract & AI Optimize")
#     ap.add_argument("--url", required=True)
#     ap.add_argument("--city", required=True)
#     ap.add_argument("--country", default="India")
#     ap.add_argument("--category", default=CATEGORY_DEFAULT)
#     ap.add_argument("--subtype", default="poi")
#     ap.add_argument("--out", default="tt_extracted.json")
#     ap.add_argument("--optimize-ai", action="store_true")
#     ap.add_argument("--title-mode", choices=["simple", "catchy"], default="catchy")
#     ap.add_argument("--enrich-google", action="store_true")
#     args = ap.parse_args()
    
#     print(f"\n🌐 Fetching: {args.url}")
    
#     # DIAGNOSTIC CHECK
#     if args.optimize_ai:
#         check_ai_status()

#     html = load_page_html(args.url)
#     parsed = parse_tt_article(html)
    
#     raw_title = parsed["playlist_title"] or "TravelTriangle Picks"
#     raw_desc = parsed["playlist_description"] or f"Places featured in: {raw_title}"
    
#     print(f"📝 Raw title: {raw_title}")
#     print(f"📝 Found {len(parsed['places'])} places")
    
#     # AI optimization
#     final_title = raw_title
#     final_desc = raw_desc
#     ai_metadata = {}
    
#     if args.optimize_ai:
#         print(f"\n🤖 AI OPTIMIZATION")
        
#         print(f"  ✂️ Optimizing title...")
#         title_result = optimize_title_with_ai(raw_title, args.city, args.country, args.title_mode == "catchy")
#         final_title = title_result["catchy"] if args.title_mode == "catchy" else title_result["simple"]
#         print(f"     ✨ {final_title} (confidence: {title_result['confidence']:.2f})")
        
#         print(f"  💬 Optimizing description...")
#         desc_result = optimize_description_with_ai(final_title, args.city, args.country, raw_desc)
#         final_desc = desc_result["description"]
#         print(f"     ✨ {desc_result['style']}: {final_desc}")
        
#         ai_metadata = {
#             "title_mode": args.title_mode, "title_confidence": title_result["confidence"],
#             "desc_style": desc_result["style"], "desc_confidence": desc_result["confidence"]
#         }
#     else:
#         print(f"\n⏭️ AI optimization disabled")
    
#     # Google Enrich (Placeholder for consistency)
#     enriched_places = parsed["places"]
#     if args.enrich_google:
#         print(f"\n🔍 Enriching (Google)...")
#         # (Using Step 2.5 for rigorous enrichment now, this is just a lightweight check if needed)
    
#     output = {
#         "playlistTitle": final_title, "placeName": args.city, "country": args.country,
#         "category": args.category, "subtype": args.subtype, "source": SOURCE_TAG,
#         "source_urls": [args.url], "description": final_desc,
#         "items": parsed["places"],
#         "ai_metadata": ai_metadata if args.optimize_ai else {}
#     }
    
#     out_path = Path(args.out)
#     out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
#     print(f"\n✅ Wrote {len(output['items'])} items to {out_path}")

# if __name__ == "__main__":
#     main()











# #!/usr/bin/env python3
# """
# TravelTriangle Step 1 — Extract & AI Optimize
# Extracts playlist data from TravelTriangle blog URLs with AI-enhanced titles/descriptions.
# """

# import os, re, json, time, hashlib, argparse
# from pathlib import Path
# from typing import Dict, Any, List, Optional
# import requests
# from bs4 import BeautifulSoup

# # ==================== CONFIGURATION ====================
# # 🔴 HARDCODED KEY (Paste your full key here)
# OPENAI_API_KEY = "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"
# OPENAI_MODEL = "gpt-4o-mini"

# SOURCE_TAG = "traveltriangle"
# CATEGORY_DEFAULT = "Travel"

# # AI parameters
# MIN_TITLE_LENGTH = 2
# MAX_TITLE_LENGTH = 5
# DESCRIPTION_TARGET = 90
# DESCRIPTION_MIN = 60
# DESCRIPTION_MAX = 110

# DEFAULT_UA = (
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#     "AppleWebKit/537.36 (KHTML, like Gecko) "
#     "Chrome/126.0 Safari/537.36"
# )

# # ==================== AI SETUP & DIAGNOSTICS ====================
# try:
#     from openai import OpenAI
#     OPENAI_AVAILABLE = True
# except ImportError:
#     OPENAI_AVAILABLE = False

# def check_ai_status():
#     print(f"\n🤖 AI DIAGNOSTICS:")
#     if not OPENAI_AVAILABLE:
#         print(f"   ❌ OpenAI Library: NOT FOUND (Run: 'pip install openai')")
#         return False
#     else:
#         print(f"   ✅ OpenAI Library: Installed")

#     if not OPENAI_API_KEY or "sk-" not in OPENAI_API_KEY:
#         print(f"   ❌ API Key: MISSING or INVALID")
#         return False
#     else:
#         print(f"   ✅ API Key: Loaded ({OPENAI_API_KEY[:10]}...)")
    
#     return True

# # ==================== CACHE SETUP ====================
# CACHE_DIR = Path("cache")
# HTML_CACHE = CACHE_DIR / "html"
# AI_CACHE = CACHE_DIR / "ai_cache.json"

# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# HTML_CACHE.mkdir(exist_ok=True, parents=True)

# # ==================== HELPERS ====================
# def clean_txt(s: Optional[str]) -> str:
#     if not s: return ""
#     s = s.replace("\u2019", "'").replace("\u2014", "-")
#     return re.sub(r"\s+", " ", s).strip()

# def strip_number_prefix(s: str) -> str:
#     s = re.sub(r"^\s*\d+[\.\)]\s*", "", s or "")
#     return re.sub(r"\s+", " ", s).strip()

# def cached_html_path(url: str) -> Path:
#     safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
#     return HTML_CACHE / f"{safe[:150]}.html"

# def load_page_html(url: str) -> str:
#     cache_file = cached_html_path(url)
#     if cache_file.exists():
#         return cache_file.read_text(encoding="utf-8")
    
#     print(f"  📥 Fetching HTML...")
#     resp = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=30)
#     resp.raise_for_status()
#     html = resp.text
#     cache_file.write_text(html, encoding="utf-8")
#     return html

# # ==================== GOOGLE PLACES ====================
# def gp_find_place(name: str, city: str, api_key: Optional[str], location_hint: Optional[str] = None) -> Optional[str]:
#     if not api_key: return None
#     base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
#     queries = []
#     if location_hint: queries.append(f"{name}, {location_hint}, {city}".strip(", "))
#     queries.append(f"{name}, {city}".strip(", "))
#     queries.append(name)
    
#     for q in queries:
#         try:
#             r = requests.get(base_url, params={"input": q, "inputtype": "textquery", "fields": "place_id", "key": api_key}, timeout=15)
#             cands = r.json().get("candidates") or []
#             if cands: return cands[0].get("place_id")
#         except Exception: continue
#     return None

# def gp_place_details(place_id: str, api_key: str) -> Dict[str, Any]:
#     if not (api_key and place_id): return {}
#     url = "https://maps.googleapis.com/maps/api/place/details/json"
#     fields = "rating,user_ratings_total,formatted_address,geometry/location,website"
#     try:
#         r = requests.get(url, params={"place_id": place_id, "fields": fields, "key": api_key}, timeout=20)
#         return r.json().get("result", {}) or {}
#     except Exception: return {}

# # ==================== AI OPTIMIZATION LOGIC ====================
# def _load_ai_cache() -> Dict[str, Any]:
#     if AI_CACHE.exists():
#         try: return json.loads(AI_CACHE.read_text(encoding="utf-8"))
#         except: return {}
#     return {}

# def _save_ai_cache(cache: Dict[str, Any]):
#     try: AI_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
#     except: pass

# def remove_numbers_from_title(title: str) -> str:
#     patterns = [r'\b\d+\b\s*', r'^\d+\s+', r'\s+\d+\s+']
#     result = title
#     for pattern in patterns:
#         result = re.sub(pattern, ' ', result)
#     return ' '.join(result.split()).strip()

# def optimize_title_with_ai(original: str, city: str, country: str, use_catchy: bool = True) -> Dict[str, Any]:
#     simple = remove_numbers_from_title(original)
    
#     if not (OPENAI_AVAILABLE and OPENAI_API_KEY):
#         return {"simple": simple, "catchy": simple, "confidence": 0.5}
    
#     cache = _load_ai_cache()
#     cache_key = json.dumps({"orig": original, "city": city, "country": country, "mode": "catchy" if use_catchy else "simple"}, sort_keys=True)
    
#     if cache_key in cache: return cache[cache_key]
    
#     try:
#         client = OpenAI(api_key=OPENAI_API_KEY)
#         system = "You are a travel content editor creating catchy, memorable playlist titles. No numbers. 2-4 words."
#         user = f"ORIGINAL: '{original}'\nCity: {city}\nCountry: {country}"

#         response = client.chat.completions.create(
#             model=OPENAI_MODEL, temperature=0.7,
#             messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
#             response_format={"type": "json_object"}, timeout=30
#         )
#         data = json.loads(response.choices[0].message.content.strip())
#         catchy = (data.get("catchy_title") or simple).strip()
        
#         result = {"simple": simple, "catchy": catchy, "confidence": 0.9}
#         cache[cache_key] = result
#         _save_ai_cache(cache)
#         return result
        
#     except Exception as e:
#         print(f"  ⚠️ AI title failed: {e}")
#         return {"simple": simple, "catchy": simple, "confidence": 0.5}

# def optimize_description_with_ai(title: str, city: str, country: str, existing: str = "") -> Dict[str, Any]:
#     if not (OPENAI_AVAILABLE and OPENAI_API_KEY):
#         fallback = f"Discover the best of {city}! Handpicked spots and experiences."
#         return {"description": fallback, "style": "Fallback", "confidence": 0.5}
    
#     cache = _load_ai_cache()
#     cache_key = json.dumps({"title": title, "city": city, "existing": existing[:100]}, sort_keys=True)
    
#     if cache_key in cache: return cache[cache_key]
    
#     try:
#         client = OpenAI(api_key=OPENAI_API_KEY)
#         system = f"Creative travel writer. Write engaging description for '{title}' in {city}. Max 100 chars. Instagram style."
#         response = client.chat.completions.create(
#             model=OPENAI_MODEL, temperature=0.75,
#             messages=[{"role": "system", "content": system}, {"role": "user", "content": "Write description."}],
#             response_format={"type": "json_object"}, timeout=30
#         )
#         data = json.loads(response.choices[0].message.content.strip())
#         desc = (data.get("description") or "").strip()
        
#         result = {"description": desc, "style": "AI", "confidence": 0.9}
#         cache[cache_key] = result
#         _save_ai_cache(cache)
#         return result
        
#     except Exception as e:
#         print(f"  ⚠️ AI description failed: {e}")
#         fallback = f"Discover the best of {city}! Handpicked spots and experiences."
#         return {"description": fallback, "style": "Fallback", "confidence": 0.3}

# # ==================== PARSING ====================
# def parse_tt_article(html: str) -> Dict[str, Any]:
#     soup = BeautifulSoup(html, "lxml")
    
#     h2 = soup.select_one("h2.h2_Waypoints_blogpage")
#     title = clean_txt(h2.get_text()) if h2 else ""
    
#     desc = ""
#     if h2:
#         nxt = h2.find_next_sibling()
#         while nxt and nxt.name != "p": nxt = nxt.find_next_sibling()
#         if nxt and nxt.name == "p": desc = clean_txt(nxt.get_text())
    
#     places: List[Dict[str, Any]] = []
    
#     # GARBAGE FILTERS
#     SKIP_PATTERNS = [
#         r"holiday\s+package", r"book\s+(?:my|a|your)?\s*(?:vacation|holiday|package|trip)",
#         r"travel\s*triangle", r"looking\s+to\s+book", r"click\s+here", r"read\s+more",
#         r"suggested\s+read", r"image\s+source", r"best\s+places\s+to\s+visit", 
#         r"places\s+to\s+visit\s+near", r"frequently\s+asked\s+questions", 
#         r"related\s+posts", r"common\s+queries"
#     ]

#     for h3 in soup.select("h3"):
#         name = strip_number_prefix(h3.get_text())
#         if not name or len(name) < 2: continue
        
#         # Garbage check
#         if any(re.search(pattern, name.lower()) for pattern in SKIP_PATTERNS):
#             print(f"  ⏭️  Skipping garbage: '{name}'")
#             continue
#         if len(name) > 70: 
#              print(f"  ⏭️  Skipping long header: '{name}'")
#              continue
        
#         place_desc = ""
#         details_p = None
#         p = h3.find_next_sibling()
#         first_p_taken = False
        
#         while p and p.name in ("p", "div"):
#             if p.name == "p" and not first_p_taken:
#                 place_desc = clean_txt(p.get_text())
#                 first_p_taken = True
#             if p.name == "p" and p.find("strong"):
#                 details_p = p
#                 break
#             p = p.find_next_sibling()
        
#         details = {"location": "", "opening_hours": "", "entry_fee": ""}
#         if details_p:
#             for br in details_p.find_all("br"): br.replace_with("\n")
#             txt = details_p.get_text("\n", strip=True)
#             m_loc = re.search(r"(?i)\bLocation:\s*(.+)", txt)
#             m_hrs = re.search(r"(?i)\bOpening\s*hours?:\s*(.+)", txt)
#             m_fee = re.search(r"(?i)\bEntry\s*fee:\s*(.+)", txt)
            
#             if m_loc: details["location"] = clean_txt(m_loc.group(1))
#             if m_hrs: details["opening_hours"] = clean_txt(m_hrs.group(1))
#             if m_fee: details["entry_fee"] = clean_txt(m_fee.group(1))
        
#         places.append({
#             "name": name, "description": place_desc,
#             "location_hint": details["location"],
#             "opening_hours_text": details["opening_hours"],
#             "entry_fee_text": details["entry_fee"]
#         })
    
#     return {"playlist_title": title, "playlist_description": desc, "places": places}

# # ==================== MAIN ====================
# def main():
#     ap = argparse.ArgumentParser("TravelTriangle Step 1 — Extract & AI Optimize")
#     ap.add_argument("--url", required=True)
#     ap.add_argument("--city", required=True)
#     ap.add_argument("--country", default="India")
#     ap.add_argument("--category", default=CATEGORY_DEFAULT)
#     ap.add_argument("--subtype", default="poi")
#     ap.add_argument("--out", default="tt_extracted.json")
#     ap.add_argument("--optimize-ai", action="store_true")
#     ap.add_argument("--title-mode", choices=["simple", "catchy"], default="catchy")
#     ap.add_argument("--enrich-google", action="store_true")
#     args = ap.parse_args()
    
#     print(f"\n🌐 Fetching: {args.url}")
    
#     # DIAGNOSTIC CHECK
#     if args.optimize_ai:
#         check_ai_status()

#     html = load_page_html(args.url)
#     parsed = parse_tt_article(html)
    
#     raw_title = parsed["playlist_title"] or "TravelTriangle Picks"
#     raw_desc = parsed["playlist_description"] or f"Places featured in: {raw_title}"
    
#     print(f"📝 Raw title: {raw_title}")
#     print(f"📝 Found {len(parsed['places'])} places")
    
#     # AI optimization
#     final_title = raw_title
#     final_desc = raw_desc
#     ai_metadata = {}
    
#     if args.optimize_ai:
#         print(f"\n🤖 AI OPTIMIZATION")
        
#         print(f"  ✂️ Optimizing title...")
#         title_result = optimize_title_with_ai(raw_title, args.city, args.country, args.title_mode == "catchy")
#         final_title = title_result["catchy"] if args.title_mode == "catchy" else title_result["simple"]
#         print(f"     ✨ {final_title} (confidence: {title_result['confidence']:.2f})")
        
#         print(f"  💬 Optimizing description...")
#         desc_result = optimize_description_with_ai(final_title, args.city, args.country, raw_desc)
#         final_desc = desc_result["description"]
#         print(f"     ✨ {desc_result['style']}: {final_desc}")
        
#         ai_metadata = {
#             "title_mode": args.title_mode, "title_confidence": title_result["confidence"],
#             "desc_style": desc_result["style"], "desc_confidence": desc_result["confidence"]
#         }
#     else:
#         print(f"\n⏭️ AI optimization disabled")
    
#     # Google Enrich (Placeholder for consistency)
#     enriched_places = parsed["places"]
#     if args.enrich_google:
#         print(f"\n🔍 Enriching (Google)...")
#         # (Using Step 2.5 for rigorous enrichment now, this is just a lightweight check if needed)
    
#     output = {
#         "playlistTitle": final_title, "placeName": args.city, "country": args.country,
#         "category": args.category, "subtype": args.subtype, "source": SOURCE_TAG,
#         "source_urls": [args.url], "description": final_desc,
#         "items": parsed["places"],
#         "ai_metadata": ai_metadata if args.optimize_ai else {}
#     }
    
#     out_path = Path(args.out)
#     out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
#     print(f"\n✅ Wrote {len(output['items'])} items to {out_path}")

# if __name__ == "__main__":
#     main()



# #!/usr/bin/env python3
# """
# TravelTriangle Step 1 — Extract & AI Optimize
# Extracts playlist data from TravelTriangle blog URLs with AI-enhanced titles/descriptions.
# Outputs: tt_extracted.json (for Step 2)
# """

# import os, re, json, time, hashlib, argparse
# from pathlib import Path
# from typing import Dict, Any, List, Optional
# # --- FIX: FORCE LOAD .ENV ---
# from dotenv import load_dotenv

# # Get the folder where THIS script is located
# script_dir = os.path.dirname(os.path.abspath(__file__))
# # Build path to .env in this folder
# env_path = os.path.join(script_dir, '.env')
# # Load it
# load_dotenv(env_path)

# import requests
# from bs4 import BeautifulSoup

# # AI imports
# try:
#     from openai import OpenAI
#     OPENAI_AVAILABLE = True
# except ImportError:
#     OPENAI_AVAILABLE = False

# # ==================== CONFIG ====================
# CACHE_DIR = Path("cache")
# HTML_CACHE = CACHE_DIR / "html"
# AI_CACHE = CACHE_DIR / "ai_cache.json"

# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# HTML_CACHE.mkdir(exist_ok=True, parents=True)

# OPENAI_API_KEY = "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"
# OPENAI_MODEL = "gpt-4o-mini"

# SOURCE_TAG = "traveltriangle"
# CATEGORY_DEFAULT = "Travel"

# # AI parameters
# MIN_TITLE_LENGTH = 2
# MAX_TITLE_LENGTH = 5
# DESCRIPTION_TARGET = 90
# DESCRIPTION_MIN = 60
# DESCRIPTION_MAX = 110

# DEFAULT_UA = (
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#     "AppleWebKit/537.36 (KHTML, like Gecko) "
#     "Chrome/126.0 Safari/537.36"
# )

# # ==================== HELPERS ====================
# def clean_txt(s: Optional[str]) -> str:
#     if not s: return ""
#     s = s.replace("\u2019", "'").replace("\u2014", "-")
#     return re.sub(r"\s+", " ", s).strip()

# def strip_number_prefix(s: str) -> str:
#     s = re.sub(r"^\s*\d+[\.\)]\s*", "", s or "")
#     return re.sub(r"\s+", " ", s).strip()

# def slugify(s: str) -> str:
#     s = (s or "").lower().strip()
#     s = re.sub(r"[^\w\s-]", "", s)
#     s = re.sub(r"[\s_-]+", "-", s)
#     return re.sub(r"^-+|-+$", "", s)

# def cached_html_path(url: str) -> Path:
#     safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
#     return HTML_CACHE / f"{safe[:150]}.html"

# def load_page_html(url: str) -> str:
#     cache_file = cached_html_path(url)
#     if cache_file.exists():
#         return cache_file.read_text(encoding="utf-8")
    
#     print(f"  📥 Fetching HTML...")
#     resp = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=30)
#     resp.raise_for_status()
#     html = resp.text
#     cache_file.write_text(html, encoding="utf-8")
#     return html

# # ==================== GOOGLE PLACES ====================
# def gp_find_place(name: str, city: str, api_key: Optional[str], location_hint: Optional[str] = None) -> Optional[str]:
#     """Find place_id using Google Places API."""
#     if not api_key:
#         return None
    
#     base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
#     queries = []
    
#     if location_hint:
#         queries.append(f"{name}, {location_hint}, {city}".strip(", "))
#     queries.append(f"{name}, {city}".strip(", "))
#     queries.append(name)
    
#     for q in queries:
#         try:
#             r = requests.get(base_url, params={
#                 "input": q,
#                 "inputtype": "textquery",
#                 "fields": "place_id",
#                 "key": api_key
#             }, timeout=15)
#             js = r.json()
#             cands = js.get("candidates") or []
#             if cands:
#                 return cands[0].get("place_id")
#         except Exception:
#             continue
#     return None

# def gp_place_details(place_id: str, api_key: str) -> Dict[str, Any]:
#     """Get place details from Google Places API."""
#     if not (api_key and place_id):
#         return {}
    
#     url = "https://maps.googleapis.com/maps/api/place/details/json"
#     fields = ",".join([
#         "rating", "user_ratings_total", "formatted_address",
#         "geometry/location", "website"
#     ])
    
#     try:
#         r = requests.get(url, params={
#             "place_id": place_id,
#             "fields": fields,
#             "key": api_key
#         }, timeout=20)
#         return r.json().get("result", {}) or {}
#     except Exception:
#         return {}


# # ==================== AI OPTIMIZATION ====================
# def _load_ai_cache() -> Dict[str, Any]:
#     if AI_CACHE.exists():
#         try: return json.loads(AI_CACHE.read_text(encoding="utf-8"))
#         except: return {}
#     return {}

# def _save_ai_cache(cache: Dict[str, Any]):
#     try: AI_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
#     except: pass

# def remove_numbers_from_title(title: str) -> str:
#     """Remove numbers from titles while keeping natural flow."""
#     patterns = [r'\b\d+\b\s*', r'^\d+\s+', r'\s+\d+\s+']
#     result = title
#     for pattern in patterns:
#         result = re.sub(pattern, ' ', result)
#     return ' '.join(result.split()).strip()

# def optimize_title_with_ai(original: str, city: str, country: str, use_catchy: bool = True) -> Dict[str, Any]:
#     """AI-optimize title or just remove numbers if AI unavailable."""
#     simple = remove_numbers_from_title(original)
    
#     if not (OPENAI_AVAILABLE and OPENAI_API_KEY):
#         return {"simple": simple, "catchy": simple, "confidence": 0.5}
    
#     cache = _load_ai_cache()
#     cache_key = json.dumps({"orig": original, "city": city, "country": country, "mode": "catchy" if use_catchy else "simple"}, sort_keys=True)
    
#     if cache_key in cache:
#         return cache[cache_key]
    
#     try:
#         client = OpenAI(api_key=OPENAI_API_KEY)
        
#         system = """You are a travel content editor creating catchy, memorable playlist titles.
# Requirements:
# - 2-4 words maximum (3 ideal)
# - Catchy and memorable
# - Include city/place name
# - Natural sounding

# Examples:
# "Mumbai Eats", "Delhi Gems", "Goa Vibes", "Jaipur Escapes"

# Avoid: "Guide", "List", numbers, being too formal"""

#         user = f"""ORIGINAL: "{original}"
# SIMPLIFIED: "{simple}"
# City: {city or "Unknown"}
# Country: {country or "Unknown"}

# Create a catchy 2-4 word title.

# Return JSON:
# {{
#   "catchy_title": "Your title",
#   "confidence": 0.9
# }}"""

#         response = client.chat.completions.create(
#             model=OPENAI_MODEL,
#             temperature=0.7,
#             messages=[
#                 {"role": "system", "content": system},
#                 {"role": "user", "content": user}
#             ],
#             max_tokens=100,
#             response_format={"type": "json_object"},
#             timeout=30
#         )
        
#         data = json.loads(response.choices[0].message.content.strip())
#         catchy = (data.get("catchy_title") or simple).strip()
#         confidence = float(data.get("confidence", 0.8))
        
#         # Validate word count
#         word_count = len(catchy.split())
#         if word_count < MIN_TITLE_LENGTH or word_count > MAX_TITLE_LENGTH:
#             catchy = simple
#             confidence = 0.6
        
#         result = {"simple": simple, "catchy": catchy, "confidence": confidence}
#         cache[cache_key] = result
#         _save_ai_cache(cache)
        
#         return result
        
#     except Exception as e:
#         print(f"  ⚠️ AI title failed: {e}")
#         result = {"simple": simple, "catchy": simple, "confidence": 0.5}
#         return result

# def optimize_description_with_ai(title: str, city: str, country: str, existing: str = "") -> Dict[str, Any]:
#     """Generate engaging description using AI."""
#     if not (OPENAI_AVAILABLE and OPENAI_API_KEY):
#         fallback = f"Discover the best of {city or 'this destination'}! Handpicked spots and experiences."
#         return {"description": fallback, "style": "Fallback", "confidence": 0.5}
    
#     cache = _load_ai_cache()
#     cache_key = json.dumps({"title": title, "city": city, "country": country, "existing": existing[:100]}, sort_keys=True)
    
#     if cache_key in cache:
#         return cache[cache_key]
    
#     try:
#         client = OpenAI(api_key=OPENAI_API_KEY)
        
#         system = f"""You are a creative travel writer creating Instagram-worthy descriptions.

# Requirements:
# - Target: {DESCRIPTION_TARGET} characters (±20 OK)
# - Catchy and engaging
# - Sensory and vivid
# - Shareable
# - No emojis/hashtags

# Examples:
# "We've curated the best Instagrammable places to help you build a picture-perfect feed!"
# "Hidden gems waiting to be discovered! Experience the authentic soul through local eyes"
# "From sunrise to sunset, these spots capture the magic that makes this place unforgettable"
# """

#         user = f"""PLAYLIST: "{title}"
# City: {city or "Various"}
# Country: {country or "Various"}

# {"CURRENT: " + existing if existing else ""}

# Create an engaging description ({DESCRIPTION_MIN}-{DESCRIPTION_MAX} chars).

# Return JSON:
# {{
#   "description": "Your description",
#   "style": "Style used (e.g., Foodie, Adventure)",
#   "confidence": 0.8
# }}"""

#         response = client.chat.completions.create(
#             model=OPENAI_MODEL,
#             temperature=0.75,
#             messages=[
#                 {"role": "system", "content": system},
#                 {"role": "user", "content": user}
#             ],
#             max_tokens=400,
#             response_format={"type": "json_object"},
#             timeout=30
#         )
        
#         data = json.loads(response.choices[0].message.content.strip())
#         desc = (data.get("description") or "").strip()
#         style = (data.get("style") or "General").strip()
#         confidence = float(data.get("confidence", 0.8))
        
#         # Clean up
#         desc = " ".join(desc.split())
        
#         # Validate length
#         if len(desc) < DESCRIPTION_MIN:
#             desc = f"Discover the best of {city or 'this destination'}! Handpicked spots and experiences."
#             confidence = 0.5
#             style = "Fallback"
#         elif len(desc) > DESCRIPTION_MAX + 20:
#             desc = desc[:DESCRIPTION_MAX] + "..."
        
#         result = {"description": desc, "style": style, "confidence": confidence}
#         cache[cache_key] = result
#         _save_ai_cache(cache)
        
#         return result
        
#     except Exception as e:
#         print(f"  ⚠️ AI description failed: {e}")
#         fallback = f"Discover the best of {city or 'this destination'}! Handpicked spots and experiences."
#         return {"description": fallback, "style": "Fallback", "confidence": 0.3}

# # ==================== PARSING ====================
# def parse_tt_article(html: str) -> Dict[str, Any]:
#     """Extract structured data from TravelTriangle article."""
#     soup = BeautifulSoup(html, "lxml")
    
#     # Title
#     h2 = soup.select_one("h2.h2_Waypoints_blogpage")
#     title = clean_txt(h2.get_text()) if h2 else ""
    
#     # Description
#     desc = ""
#     if h2:
#         nxt = h2.find_next_sibling()
#         while nxt and nxt.name != "p":
#             nxt = nxt.find_next_sibling()
#         if nxt and nxt.name == "p":
#             desc = clean_txt(nxt.get_text())
    
#     # Places
#     places: List[Dict[str, Any]] = []
    
#     # GARBAGE FILTERS
#     # 1. "Skip" patterns: specific items to ignore (ads, buttons)
#     SKIP_PATTERNS = [
#         r"holiday\s+package",
#         r"book\s+(?:my|a|your)?\s*(?:vacation|holiday|package|trip)",
#         r"travel\s*triangle",
#         r"looking\s+to\s+book",
#         r"click\s+here",
#         r"read\s+more",
#         r"suggested\s+read",
#         r"image\s+source",
#     ]

#     # 2. "Stop" patterns: usually signal the start of the footer/SEO section
#     #    (We treat them as skips here for safety, but they usually mean the list is done)
#     FOOTER_PATTERNS = [
#         r"best\s+places\s+to\s+visit",  # This matches the garbage you saw in your log
#         r"places\s+to\s+visit\s+near",
#         r"frequently\s+asked\s+questions",
#         r"common\s+queries",
#         r"related\s+posts",
#     ]
    
#     # Combine them
#     ALL_SKIP_PATTERNS = SKIP_PATTERNS + FOOTER_PATTERNS

#     for h3 in soup.select("h3"):
#         raw_name = h3.get_text()
#         name = strip_number_prefix(raw_name)
        
#         if not name or len(name) < 2:
#             continue
        
#         # --- IMPROVED FILTERING ---
#         name_lower = name.lower()
        
#         # Check regex patterns
#         if any(re.search(pattern, name_lower) for pattern in ALL_SKIP_PATTERNS):
#             print(f"  ⏭️  Skipping garbage: '{name}'")
#             continue
            
#         # Heuristic: If the name is too long (e.g., a full sentence question), skip it
#         # Valid places are rarely > 60 chars long
#         if len(name) > 70: 
#              print(f"  ⏭️  Skipping long header: '{name}'")
#              continue
#         # ---------------------------
        
#         place_desc = ""
#         details_p = None
#         p = h3.find_next_sibling()
#         first_p_taken = False
        
#         while p and p.name in ("p", "div"):
#             if p.name == "p" and not first_p_taken:
#                 place_desc = clean_txt(p.get_text())
#                 first_p_taken = True
#             if p.name == "p" and p.find("strong"):
#                 details_p = p
#                 break
#             p = p.find_next_sibling()
        
#         details = {"location": "", "opening_hours": "", "entry_fee": ""}
#         if details_p:
#             for br in details_p.find_all("br"):
#                 br.replace_with("\n")
#             txt = details_p.get_text("\n", strip=True)
            
#             m_loc = re.search(r"(?i)\bLocation:\s*(.+)", txt)
#             m_hrs = re.search(r"(?i)\bOpening\s*hours?:\s*(.+)", txt)
#             m_fee = re.search(r"(?i)\bEntry\s*fee:\s*(.+)", txt)
            
#             if m_loc: details["location"] = clean_txt(m_loc.group(1))
#             if m_hrs: details["opening_hours"] = clean_txt(m_hrs.group(1))
#             if m_fee: details["entry_fee"] = clean_txt(m_fee.group(1))
        
#         places.append({
#             "name": name,
#             "description": place_desc,
#             "location_hint": details["location"],
#             "opening_hours_text": details["opening_hours"],
#             "entry_fee_text": details["entry_fee"]
#         })
    
#     return {
#         "playlist_title": title,
#         "playlist_description": desc,
#         "places": places
#     }
# # ==================== MAIN ====================
# def main():
#     ap = argparse.ArgumentParser("TravelTriangle Step 1 — Extract & AI Optimize")
#     ap.add_argument("--url", required=True, help="TravelTriangle blog URL")
#     ap.add_argument("--city", required=True, help="City/region name")
#     ap.add_argument("--country", default="India", help="Country name")
#     ap.add_argument("--category", default=CATEGORY_DEFAULT, help="Category")
#     ap.add_argument("--subtype", default="poi", choices=["poi", "destination"], help="Playlist subtype")
#     ap.add_argument("--out", default="tt_extracted.json", help="Output JSON file")
#     ap.add_argument("--optimize-ai", action="store_true", help="Use AI for title/description")
#     ap.add_argument("--title-mode", choices=["simple", "catchy"], default="catchy")
#     ap.add_argument("--enrich-google", action="store_true", help="Enrich with Google Places data (place_id, rating, etc)")
#     args = ap.parse_args()
    
#     print(f"\n🌐 Fetching: {args.url}")
#     html = load_page_html(args.url)
    
#     parsed = parse_tt_article(html)
#     soup = BeautifulSoup(html, "lxml")
    
#     raw_title = parsed["playlist_title"] or clean_txt(soup.title.get_text() if soup.title else "") or "TravelTriangle Picks"
#     raw_desc = parsed["playlist_description"] or f"Places featured in: {raw_title}"
    
#     print(f"📝 Raw title: {raw_title}")
#     print(f"📝 Found {len(parsed['places'])} places")
    
#     # AI optimization
#     final_title = raw_title
#     final_desc = raw_desc
#     ai_metadata = {}
    
#     if args.optimize_ai:
#         print(f"\n🤖 AI OPTIMIZATION")
        
#         # Title
#         print(f"  ✂️ Optimizing title...")
#         title_result = optimize_title_with_ai(raw_title, args.city, args.country, args.title_mode == "catchy")
#         final_title = title_result["catchy"] if args.title_mode == "catchy" else title_result["simple"]
#         print(f"     ✨ {final_title} (confidence: {title_result['confidence']:.2f})")
        
#         # Description
#         print(f"  💬 Optimizing description...")
#         desc_result = optimize_description_with_ai(final_title, args.city, args.country, raw_desc)
#         final_desc = desc_result["description"]
#         print(f"     ✨ {desc_result['style']}: {final_desc}")
#         print(f"     📊 Confidence: {desc_result['confidence']:.2f}")
        
#         ai_metadata = {
#             "title_mode": args.title_mode,
#             "title_confidence": title_result["confidence"],
#             "desc_style": desc_result["style"],
#             "desc_confidence": desc_result["confidence"],
#             "raw_title": raw_title,
#             "raw_description": raw_desc
#         }
#     else:
#         print(f"\n⏭️ AI optimization disabled")
    
#     # ===== GOOGLE PLACES ENRICHMENT (Optional) =====
#     enriched_places = []
    
#     if args.enrich_google:
#         print(f"\n🔍 Enriching {len(parsed['places'])} places with Google Places...")
#         google_api_key = "AIzaSyDEuy2i8AbSR-jnstZG22ndFqy71jtKbgg"
        
#         if not google_api_key:
#             print("   ⚠️  GOOGLE_MAPS_API_KEY not set, skipping enrichment")
#             enriched_places = parsed["places"]
#         else:
#             for idx, p in enumerate(parsed["places"], 1):
#                 name = p["name"]
#                 location_hint = p["location_hint"]
                
#                 print(f"   [{idx}/{len(parsed['places'])}] {name}...", end=" ", flush=True)
                
#                 # Find place_id
#                 place_id = gp_find_place(name, args.city, google_api_key, location_hint)
                
#                 if place_id:
#                     print(f"✅")
#                     # Get details
#                     details = gp_place_details(place_id, google_api_key)
#                     loc = (details.get("geometry") or {}).get("location") or {}
                    
#                     p["place_id"] = place_id
#                     p["lat"] = loc.get("lat")
#                     p["lng"] = loc.get("lng")
#                     p["rating"] = details.get("rating", 0)
#                     p["reviews"] = details.get("user_ratings_total", 0)
#                     p["address"] = details.get("formatted_address")
#                     p["website"] = details.get("website")
#                 else:
#                     print(f"⚠️  (no Place ID)")
#                     p["place_id"] = None
#                     p["lat"] = None
#                     p["lng"] = None
#                     p["rating"] = 0
#                     p["reviews"] = 0
#                     p["address"] = None
#                     p["website"] = None
                
#                 enriched_places.append(p)
#                 time.sleep(0.05)  # Rate limiting
#     else:
#         enriched_places = parsed["places"]
#         print(f"\n⏭️  Google Places enrichment disabled (use --enrich-google to enable)")
    
#     # Build output
#     output = {
#         "playlistTitle": final_title,
#         "placeName": args.city,
#         "country": args.country,
#         "category": args.category,
#         "subtype": args.subtype,
#         "source": SOURCE_TAG,
#         "source_urls": [args.url],
#         "description": final_desc,
#         "items": [
#             {
#                 "name": p["name"],
#                 "description": p["description"],
#                 "location_hint": p["location_hint"],
#                 "opening_hours_text": p["opening_hours_text"],
#                 "entry_fee_text": p["entry_fee_text"],
#                 "category_hint": None,  # Will be classified in Step 2
#                 "scope": args.subtype,
#                 # Google Places data (if enriched)
#                 "place_id": p.get("place_id"),
#                 "lat": p.get("lat"),
#                 "lng": p.get("lng"),
#                 "rating": p.get("rating", 0),
#                 "reviews": p.get("reviews", 0),
#                 "address": p.get("address"),
#                 "website": p.get("website")
#             }
#             for p in enriched_places
#         ],
#         "ai_metadata": ai_metadata if args.optimize_ai else {}
#     }
    
#     # Write output
#     out_path = Path(args.out)
#     out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
#     print(f"\n✅ Wrote {len(output['items'])} items to {out_path}")

# if __name__ == "__main__":
#     main()