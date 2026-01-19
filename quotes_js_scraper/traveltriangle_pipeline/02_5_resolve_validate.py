# 02_5_resolve_validate.py
# SUPER SCRIPT v3: Hybrid Matching (Math + Context-Aware LLM), Quality Trimming, & CLI Overrides

from __future__ import annotations
import os, json, re, time, math, argparse, unicodedata, random
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

# --- 1. CONFIGURATION & KEYS ---
# 🔴 HARDCODED KEYS
GOOGLE_API_KEY_HARDCODED = "AIzaSyBe-suda095R60l0G1NrdYeIVKjUCxwK_8"
OPENAI_API_KEY_HARDCODED = "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"

# Paths
BASE_DIR = Path(__file__).resolve().parent
IN_PATH   = BASE_DIR / "playlist_items.json"
OUT_PATH  = BASE_DIR / "playlist_items_resolved.json"
REPORT    = BASE_DIR / "resolve_report.json"

CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True, parents=True)
PLACES_CACHE   = CACHE_DIR / "places_cache.json"
GEOCODE_CACHE  = CACHE_DIR / "geocode_cache.json"

# Logic Settings (Defaults - can be overridden by CLI)
MIN_CONFIDENCE = 0.70  
MIN_RATING     = 3.5
MIN_REVIEWS    = 50
REQUIRE_PHOTO  = False

# Grey Zone (LLM Triggers)
GREY_ZONE_MIN  = 0.35
GREY_ZONE_MAX  = 0.85

# Google Maps Tuning
GMAPS_REGION    = "in"
GMAPS_LANGUAGE  = "en-IN"
SLEEP_BETWEEN   = 0.05

# --- 2. IMPORTS & SETUP ---
try:
    import googlemaps
except ImportError:
    googlemaps = None

try:
    import requests
except ImportError:
    requests = None

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    OpenAI = None

try:
    from rapidfuzz import fuzz
    def name_sim(a: str, b: str) -> int:
        return fuzz.token_set_ratio(a, b)
except ImportError:
    def _tok(s: str) -> set:
        return set(re.findall(r"[a-z0-9]+", s.lower()))
    def name_sim(a: str, b: str) -> int:
        A, B = _tok(a), _tok(b)
        return int(100 * len(A & B) / len(A | B)) if (A and B) else 0

# --- 3. STATIC DATA ---
CITY_COORDS: Dict[str, Tuple[float, float]] = {
    "bengaluru": (12.9716, 77.5946), "bangalore": (12.9716, 77.5946), "mumbai": (19.0760, 72.8777),
    "delhi": (28.7041, 77.1025), "chennai": (13.0827, 80.2707), "kolkata": (22.5726, 88.3639),
    "hyderabad": (17.3850, 78.4867), "kochi": (9.9312, 76.2673), "ahmedabad": (23.0225, 72.5714),
    "pune": (18.5204, 73.8567), "mysore": (12.2958, 76.6394), "mangalore": (12.9141, 74.8560),
    "coimbatore": (11.0168, 76.9558), "thiruvananthapuram": (8.5241, 76.9366),
}
ALIAS_MAP = {
    "alleppey": "alappuzha",
    "pondicherry": "puducherry",
    "bombay": "mumbai",
    "calcutta": "kolkata",
    "ooty": "udhagamandalam",
    "coorg": "kodagu",
}
NAME_ALIASES = [
    (re.compile(r"\bchikmagalur(u)?\b", re.I), "chikkamagaluru"),
    (re.compile(r"\bcoorg\b", re.I), "kodagu"),
    (re.compile(r"\bcalicut\b", re.I), "kozhikode"),
    (re.compile(r"\bbangalore\b", re.I), "bengaluru"),
    (re.compile(r"\booty\b", re.I), "udhagamandalam"),
]
FAMOUS_LANDMARK_STATES = {
    "jog falls": "karnataka",
    "mysore": "karnataka",
    "hampi": "karnataka",
    "coorg": "karnataka",
    "gokarna": "karnataka",
    "wayanad": "kerala",
    "munnar": "kerala",
    "ooty": "tamil nadu",
    "akshardham": "gujarat",
    "rishikesh": "uttarakhand",
}
SCOPE_TYPES = {
    "destination": {
        "locality",
        "sublocality",
        "administrative_area_level_3",
        "administrative_area_level_2",
        "political",
    },
    "poi": {
        "tourist_attraction",
        "point_of_interest",
        "museum",
        "zoo",
        "amusement_park",
        "aquarium",
        "hindu_temple",
        "church",
        "mosque",
        "campground",
        "lodging",
        "restaurant",
        "park",
    },
    "natural": {
        "natural_feature",
        "tourist_attraction",
        "park",
        "point_of_interest",
    },
}
BAD_POI_SUFFIX = re.compile(
    r"(view\s*point|parking|ticket|counter|canteen|shop|toilet|entrance|gate)",
    re.I,
)

# --- 4. HELPER FUNCTIONS ---
def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def normalize_name(s: str) -> str:
    return re.sub(r"\s+", " ", strip_accents(s or "").strip())

def infer_entity_kind_from_category(category_hint: str, scope: str) -> str:
    if not category_hint:
        return "destination" if scope == "destination" else "tourist_attraction"
    ch = category_hint.lower()
    if ch in ["temple", "fort", "museum", "park", "beach", "lake"]:
        return ch
    return "tourist_attraction"

def allowed_types_for_kind(kind: str) -> set:
    base = {"tourist_attraction", "point_of_interest", "locality", "establishment"}
    if kind == "temple":
        base.update(["hindu_temple", "place_of_worship", "church", "mosque"])
    if kind == "park":
        base.update(["park", "national_park"])
    return base

def primary_google_type_for_kind(kind: str) -> Optional[str]:
    if kind == "temple":
        return "hindu_temple"
    if kind == "park":
        return "park"
    return None

def expand_name_variants(name: str) -> List[str]:
    variants = {normalize_name(name)}
    for pat, rep in NAME_ALIASES:
        if pat.search(name):
            variants.add(pat.sub(rep, name))
    return list(variants)

def build_enhanced_queries(
    name: str,
    category_hint: str,
    scope: str,
    anchor_city: str,
    anchor_state: str,
) -> List[str]:
    qs = [name]
    if anchor_city:
        qs.append(f"{name} {anchor_city}")
    if anchor_state:
        qs.append(f"{name} {anchor_state}")
    return qs

def parse_hours(s: str) -> Optional[float]:
    return None

def km_from_hours(h: Optional[float], speed: int = 70) -> int:
    return 450

def haversine_km(lat1, lon1, lat2, lon2):
    return 0.0

def distance_score(cand, alat, alng, rad):
    return 1.0

def popularity_score(rat, rev):
    return min(1.0, float(rat) * math.log10(rev + 1) / 5.0)

def type_compat_score(types, allowed):
    return 1.0 if set(types) & allowed else 0.5

def state_match_score(addr, state):
    return 1.0 if state and state.lower() in (addr or "").lower() else 0.0

def circle_bias(lat, lng, rad):
    return None

# --- 5. LLM JUDGE (WITH RICH CONTEXT) ---
def llm_verify_match(
    scraped_name: str,
    scraped_context: str,
    cand_name: str,
    cand_address: str,
    cand_types: List[str],
    api_key: str,
) -> float:
    """The 'Ambiguity Judge'."""
    if not (api_key and "sk-" in api_key and OPENAI_AVAILABLE):
        return 0.5
    
    try:
        client = OpenAI(api_key=api_key)
        prompt = f"""
        Task: Entity Resolution. Determine if the 'SOURCE' refers to the 'CANDIDATE'.
        
        SOURCE (Blog/Article Data):
        - Name: "{scraped_name}"
        - Context: "{scraped_context}" 
        (Use the Context/Description to identify unique landmarks, history, or visual features.)
        
        CANDIDATE (Google Maps Data):
        - Name: "{cand_name}"
        - Address: "{cand_address}"
        - Types: {cand_types}
        
        CRITERIA:
        1. Semantic Match: Does the description describe this exact place? (e.g. "Pink Palace" = "Hawa Mahal")
        2. Location Match: Is the candidate in the correct city/area implied by the Source?
        3. Type Match: If Source says "Temple", Candidate cannot be "Shopping Mall".
        
        Return JSON ONLY in this format:
        {{
            "match": true,
            "confidence": 0.95,
            "reason": "Short explanation"
        }}
        """

        response = client.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=30,
        )
        data = json.loads(response.choices[0].message.content)
        
        if data.get("match"):
            return max(0.85, float(data.get("confidence", 0.85)))
        else:
            return float(data.get("confidence", 0.1)) or 0.1
            
    except Exception as e:
        print(f"    ⚠️ LLM Error: {e}")
        # Neutral-ish; caller will decide whether to keep math_score
        return 0.5

# --- 6. SCORING & TRIMMING ---
def score_item(it: Dict[str, Any]) -> float:
    """Calculates Quality Score (0.0 - 5.0+)"""
    rating = float(it.get("rating") or 0.0)
    reviews = float(it.get("reviews") or 0.0)
    vol = math.log10(max(1.0, reviews + 1.0))
    bonus = 0.2 if (it.get("photo_refs") and len(it.get("photo_refs")) > 0) else 0.0
    return (0.6 * rating) + (0.3 * vol) + bonus

def trim_and_light_shuffle(
    items: List[Dict[str, Any]],
    keep_ratio: float = 1.0,
) -> List[Dict[str, Any]]:
    if not items:
        return []
    
    ranked = sorted(items, key=score_item, reverse=True)
    k = max(1, int(math.ceil(len(items) * keep_ratio)))
    top_items = ranked[:k]
    
    rng = random.Random(42)
    for i in range(len(top_items)):
        j = min(len(top_items) - 1, max(0, i + rng.randint(-2, 2)))
        top_items[i], top_items[j] = top_items[j], top_items[i]
        
    return top_items

# --- 7. RESOLVER CLASS ---
class EnhancedResolver:
    def __init__(self, api_key: str, cache_path: Path, refresh_photos: bool):
        self.enabled = bool(api_key and googlemaps)
        self.cache_path = cache_path
        self.cache = {}
        if cache_path.exists():
            try:
                self.cache = json.loads(cache_path.read_text(encoding="utf-8"))
            except:
                self.cache = {}
        self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None
        self.api_key = api_key
        self.refresh_photos = refresh_photos

    def save(self):
        try:
            self.cache_path.write_text(
                json.dumps(self.cache, indent=2),
                encoding="utf-8",
            )
        except:
            pass

    def _cache_key(self, name, cat, scope, city, state):
        return f"{normalize_name(name)}|{cat}|{scope}|{city}"

    def _details_with_photos(self, place_id: str) -> Dict[str, Any]:
        if not self.gmaps:
            return {}
        try:
            res = self.gmaps.place(
                place_id=place_id,
                fields=[
                    "name",
                    "geometry/location",
                    "formatted_address",
                    "rating",
                    "user_ratings_total",
                    "photos",
                    "types",
                    "website",
                    "international_phone_number",
                    "opening_hours",
                    "price_level",
                    "permanently_closed",
                    "utc_offset_minutes",
                ],
            )
            data = res.get("result", {})
        except:
            return {}

        if not data.get("photos"):
            try:
                url = "https://maps.googleapis.com/maps/api/place/details/json"
                r = requests.get(
                    url,
                    params={
                        "place_id": place_id,
                        "fields": "photos",
                        "key": self.api_key,
                    },
                    timeout=20,
                )
                if r.status_code == 200:
                    data["photos"] = r.json().get("result", {}).get("photos", [])
            except:
                pass
        return data

    def resolve(
        self,
        name: str,
        category_hint: str,
        scope: str,
        anchor_city: str,
        anchor_state: str,
        radius_m: int,
        description: str = "",
        location_hint: str = "",
    ) -> Dict[str, Any]:
        
        empty = {"place_id": None, "name": name, "confidence": 0.0, "photo_refs": []}
        ck = self._cache_key(name, category_hint, scope, anchor_city, anchor_state)
        
        if ck in self.cache:
            cached = {**empty, **self.cache[ck]}
            if cached.get("place_id") and (self.refresh_photos or not cached.get("photo_refs")):
                det = self._details_with_photos(cached["place_id"])
                if det.get("photos"):
                    cached["photo_refs"] = [p["photo_reference"] for p in det["photos"]]
                    self.cache[ck] = cached
            return cached

        if not self.enabled:
            return empty

        candidates = []
        try:
            q = f"{name} {anchor_city or ''} {anchor_state or ''}"
            res = self.gmaps.places(query=q, language=GMAPS_LANGUAGE)
            candidates = res.get("results", [])[:5]
        except:
            pass

        best = empty
        best_score = -1.0

        for cand in candidates:
            # A. Math Score
            c_name = cand.get("name")
            sim = name_sim(name, c_name) / 100.0
            allowed_types = allowed_types_for_kind(
                infer_entity_kind_from_category(category_hint, scope)
            )
            type_bonus = (
                0.1
                if type_compat_score(cand.get("types", []), allowed_types) > 0.5
                else -0.1
            )
            state_bonus = 0.1 if (
                anchor_state and
                anchor_state.lower() in cand.get("formatted_address", "").lower()
            ) else 0.0
            
            math_score = max(0.0, min(1.0, sim * 0.6 + type_bonus + state_bonus + 0.2))

            # B. Ambiguity Judge (With Rich Context)
            final_score = math_score
            if GREY_ZONE_MIN <= math_score < GREY_ZONE_MAX and OPENAI_AVAILABLE:
                rich_context = (
                    f"Category: {category_hint}. "
                    f"Location Hint: {location_hint}. "
                    f"Description: {description[:250]}..."
                )
                
                print(f"    🤖 Grey Zone ({math_score:.2f}): Asking LLM '{name}' vs '{c_name}'...")
                llm_conf = llm_verify_match(
                    scraped_name=name,
                    scraped_context=rich_context,
                    cand_name=c_name,
                    cand_address=cand.get("formatted_address", ""),
                    cand_types=cand.get("types", []),
                    api_key=OPENAI_API_KEY_HARDCODED,
                )
                
                if llm_conf > 0.8:
                    final_score = llm_conf
                    print(f"      ✅ LLM Confirmed! ({math_score:.2f} -> {final_score:.2f})")
                elif llm_conf < 0.3:
                    final_score = 0.1
                    print(f"      ❌ LLM Rejected. ({math_score:.2f} -> {final_score:.2f})")
                else:
                    # Ambiguous or error → keep the math score
                    final_score = math_score
                    print(f"      🤷 LLM Ambiguous/Neutral, keeping math score ({math_score:.2f})")

            if final_score > best_score:
                best_score = final_score
                best = {
                    "place_id": cand.get("place_id"),
                    "name": c_name,
                    "address": cand.get("formatted_address"),
                    "rating": cand.get("rating", 0),
                    "reviews": cand.get("user_ratings_total", 0),
                    "types": cand.get("types", []),
                    "photo_refs": [p["photo_reference"] for p in cand.get("photos", [])],
                    "confidence": round(final_score, 3),
                }
            time.sleep(SLEEP_BETWEEN)

        if best.get("place_id") and best["confidence"] >= 0.5:
            det = self._details_with_photos(best["place_id"])
            if det:
                loc = (det.get("geometry") or {}).get("location") or {}
                best.update(
                    {
                        "lat": loc.get("lat"),
                        "lng": loc.get("lng"),
                        "website": det.get("website"),
                        "phone": det.get("international_phone_number"),
                        "opening": (det.get("opening_hours", {}) or {}).get(
                            "periods", []
                        ),
                        "price_level": det.get("price_level"),
                        "permanently_closed": det.get("permanently_closed", False),
                        "utc_offset_minutes": det.get("utc_offset_minutes"),
                    }
                )
                if det.get("photos"):
                    best["photo_refs"] = [
                        p["photo_reference"] for p in det["photos"]
                    ]

        self.cache[ck] = best
        return best

# --- 8. MAIN EXECUTION ---
def is_publishable(resolved: Dict[str, Any]) -> bool:
    if resolved.get("confidence", 0) < MIN_CONFIDENCE:
        return False
    if not resolved.get("place_id"):
        return False
    
    rating = float(resolved.get("rating", 0))
    reviews = int(resolved.get("reviews", 0))
    
    if rating < 1.0 and reviews > 0:
        return False 
    if REQUIRE_PHOTO and not resolved.get("photo_refs"):
        return False
    
    return True

def main():
    global MIN_CONFIDENCE, REQUIRE_PHOTO

    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="in_path", default=str(IN_PATH))
    p.add_argument("--out", dest="out_path", default=str(OUT_PATH))
    p.add_argument("--report", dest="report_path", default=str(REPORT))
    p.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE)
    p.add_argument("--keep-ratio", type=float, default=1.0)
    p.add_argument("--anchor-city", type=str)
    p.add_argument("--anchor-state", type=str)
    # Dummy args for compatibility with pipeline
    p.add_argument("--radius-km", type=int)
    p.add_argument("--default-speed", type=int)
    p.add_argument("--retry", type=int)
    p.add_argument("--require-photo", action="store_true")
    p.add_argument("--refresh-photos", action="store_true")
    p.add_argument("--scope", default="auto")
    args = p.parse_args()

    # Update global settings
    MIN_CONFIDENCE = args.min_confidence
    REQUIRE_PHOTO = args.require_photo

    if not Path(args.in_path).exists():
        return

    resolver = EnhancedResolver(
        GOOGLE_API_KEY_HARDCODED,
        PLACES_CACHE,
        args.refresh_photos,
    )

    data = json.loads(Path(args.in_path).read_text(encoding="utf-8"))
    out_playlists = []
    totals = {"resolved": 0, "publishable": 0}

    for plist in data:
        title = plist.get("playlistTitle")
        anchor_city = args.anchor_city or plist.get("placeName")
        anchor_state = args.anchor_state
        
        print(f"\nPlaylist: {title} (City: {anchor_city})")
        resolved_items = []
        
        for item in plist.get("items", []):
            name = item.get("name")
            cat = item.get("category_hint")
            desc_text = item.get("description") or ""
            loc_hint = item.get("location_hint") or ""
            
            res = resolver.resolve(
                name=name,
                category_hint=cat,
                scope="poi",
                anchor_city=anchor_city,
                anchor_state=anchor_state,
                radius_m=50000,
                description=desc_text,
                location_hint=loc_hint,
            )
            
            final_item = item.copy()
            final_item.update(res)
            
            if is_publishable(res):
                final_item["resolution_status"] = "publishable"
                totals["publishable"] += 1
                print(
                    f"  ✅ {name} -> {res['name']} "
                    f"(Conf: {res['confidence']}, Rat: {res.get('rating')})"
                )
            else:
                final_item["resolution_status"] = "unresolved"
                print(f"  ❌ {name} -> (Conf: {res['confidence']})")
            
            resolved_items.append(final_item)

        if args.keep_ratio < 1.0:
            valid_items = [
                it for it in resolved_items
                if it["resolution_status"] == "publishable"
            ]
            print(
                f"  ✂️ Trimming {len(valid_items)} valid items "
                f"to top {int(args.keep_ratio*100)}%..."
            )
            
            kept_valid = trim_and_light_shuffle(valid_items, args.keep_ratio)
            kept_ids = {it["place_id"] for it in kept_valid}
            
            for it in resolved_items:
                if (
                    it["resolution_status"] == "publishable"
                    and it["place_id"] not in kept_ids
                ):
                    it["resolution_status"] = "filtered_by_ratio"
            
            print(f"     Kept {len(kept_valid)} items")

        out_playlists.append({**plist, "items": resolved_items})

    Path(args.out_path).write_text(
        json.dumps(out_playlists, indent=2),
        encoding="utf-8",
    )
    resolver.save()
    print(f"\nDone. Wrote {args.out_path}")

if __name__ == "__main__":
    main()

# # 02_5_resolve_validate.py
# # SUPER SCRIPT v3: Hybrid Matching (Math + Context-Aware LLM), Quality Trimming, & CLI Overrides

# from __future__ import annotations
# import os, json, re, time, math, argparse, unicodedata, random
# from pathlib import Path
# from typing import List, Dict, Any, Optional, Tuple

# # --- 1. CONFIGURATION & KEYS ---
# # 🔴 HARDCODED KEYS
# GOOGLE_API_KEY_HARDCODED = "AIzaSyDEuy2i8AbSR-jnstZG22ndFqy71jtKbgg"
# OPENAI_API_KEY_HARDCODED = "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"

# # Paths
# BASE_DIR = Path(__file__).resolve().parent
# IN_PATH   = BASE_DIR / "playlist_items.json"
# OUT_PATH  = BASE_DIR / "playlist_items_resolved.json"
# REPORT    = BASE_DIR / "resolve_report.json"

# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# PLACES_CACHE   = CACHE_DIR / "places_cache.json"
# GEOCODE_CACHE  = CACHE_DIR / "geocode_cache.json"

# # Logic Settings (Defaults - can be overridden by CLI)
# MIN_CONFIDENCE = 0.70  
# MIN_RATING     = 3.5
# MIN_REVIEWS    = 50
# REQUIRE_PHOTO  = False

# # Grey Zone (LLM Triggers)
# GREY_ZONE_MIN  = 0.50
# GREY_ZONE_MAX  = 0.85

# # Google Maps Tuning
# GMAPS_REGION    = "in"
# GMAPS_LANGUAGE  = "en-IN"
# SLEEP_BETWEEN   = 0.05

# # --- 2. IMPORTS & SETUP ---
# try:
#     import googlemaps
# except ImportError:
#     googlemaps = None

# try:
#     import requests
# except ImportError:
#     requests = None

# try:
#     from openai import OpenAI
#     OPENAI_AVAILABLE = True
# except ImportError:
#     OPENAI_AVAILABLE = False
#     OpenAI = None

# try:
#     from rapidfuzz import fuzz
#     def name_sim(a: str, b: str) -> int:
#         return fuzz.token_set_ratio(a, b)
# except ImportError:
#     def _tok(s: str) -> set:
#         return set(re.findall(r"[a-z0-9]+", s.lower()))
#     def name_sim(a: str, b: str) -> int:
#         A, B = _tok(a), _tok(b)
#         return int(100 * len(A & B) / len(A | B)) if (A and B) else 0

# # --- 3. STATIC DATA ---
# CITY_COORDS: Dict[str, Tuple[float, float]] = {
#     "bengaluru": (12.9716, 77.5946), "bangalore": (12.9716, 77.5946), "mumbai": (19.0760, 72.8777),
#     "delhi": (28.7041, 77.1025), "chennai": (13.0827, 80.2707), "kolkata": (22.5726, 88.3639),
#     "hyderabad": (17.3850, 78.4867), "kochi": (9.9312, 76.2673), "ahmedabad": (23.0225, 72.5714),
#     "pune": (18.5204, 73.8567), "mysore": (12.2958, 76.6394), "mangalore": (12.9141, 74.8560),
#     "coimbatore": (11.0168, 76.9558), "thiruvananthapuram": (8.5241, 76.9366),
# }
# ALIAS_MAP = {"alleppey":"alappuzha","pondicherry":"puducherry","bombay":"mumbai","calcutta":"kolkata","ooty":"udhagamandalam","coorg":"kodagu"}
# NAME_ALIASES = [
#     (re.compile(r"\bchikmagalur(u)?\b", re.I), "chikkamagaluru"), (re.compile(r"\bcoorg\b", re.I), "kodagu"),
#     (re.compile(r"\bcalicut\b", re.I), "kozhikode"), (re.compile(r"\bbangalore\b", re.I), "bengaluru"),
#     (re.compile(r"\booty\b", re.I), "udhagamandalam"),
# ]
# FAMOUS_LANDMARK_STATES = {
#     "jog falls":"karnataka","mysore":"karnataka","hampi":"karnataka","coorg":"karnataka","gokarna":"karnataka",
#     "wayanad":"kerala","munnar":"kerala","ooty":"tamil nadu","akshardham":"gujarat","rishikesh":"uttarakhand"
# }
# SCOPE_TYPES = {
#     "destination":{"locality","sublocality","administrative_area_level_3","administrative_area_level_2","political"},
#     "poi":{"tourist_attraction","point_of_interest","museum","zoo","amusement_park","aquarium","hindu_temple","church","mosque","campground","lodging","restaurant","park"},
#     "natural":{"natural_feature","tourist_attraction","park","point_of_interest"},
# }
# BAD_POI_SUFFIX = re.compile(r"(view\s*point|parking|ticket|counter|canteen|shop|toilet|entrance|gate)", re.I)

# # --- 4. HELPER FUNCTIONS ---
# def strip_accents(s: str) -> str:
#     return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

# def normalize_name(s: str) -> str:
#     return re.sub(r"\s+", " ", strip_accents(s or "").strip())

# def infer_entity_kind_from_category(category_hint: str, scope: str) -> str:
#     if not category_hint: return "destination" if scope == "destination" else "tourist_attraction"
#     ch = category_hint.lower()
#     if ch in ["temple", "fort", "museum", "park", "beach", "lake"]: return ch
#     return "tourist_attraction"

# def allowed_types_for_kind(kind: str) -> set:
#     base = {"tourist_attraction", "point_of_interest", "locality", "establishment"}
#     if kind == "temple": base.update(["hindu_temple", "place_of_worship", "church", "mosque"])
#     if kind == "park": base.update(["park", "national_park"])
#     return base

# def primary_google_type_for_kind(kind: str) -> Optional[str]:
#     if kind == "temple": return "hindu_temple"
#     if kind == "park": return "park"
#     return None

# def expand_name_variants(name: str) -> List[str]:
#     variants = {normalize_name(name)}
#     for pat, rep in NAME_ALIASES:
#         if pat.search(name): variants.add(pat.sub(rep, name))
#     return list(variants)

# def build_enhanced_queries(name: str, category_hint: str, scope: str, anchor_city: str, anchor_state: str) -> List[str]:
#     qs = [name]
#     if anchor_city: qs.append(f"{name} {anchor_city}")
#     if anchor_state: qs.append(f"{name} {anchor_state}")
#     return qs

# def parse_hours(s: str) -> Optional[float]: return None
# def km_from_hours(h: Optional[float], speed: int=70) -> int: return 450
# def haversine_km(lat1, lon1, lat2, lon2): return 0.0 
# def distance_score(cand, alat, alng, rad): return 1.0 
# def popularity_score(rat, rev): return min(1.0, float(rat)*math.log10(rev+1)/5.0)
# def type_compat_score(types, allowed): return 1.0 if set(types)&allowed else 0.5
# def state_match_score(addr, state): return 1.0 if state and state.lower() in (addr or "").lower() else 0.0
# def circle_bias(lat, lng, rad): return None

# # --- 5. LLM JUDGE (WITH RICH CONTEXT) ---
# def llm_verify_match(scraped_name: str, scraped_context: str, 
#                      cand_name: str, cand_address: str, cand_types: List[str],
#                      api_key: str) -> float:
#     """The 'Ambiguity Judge'."""
#     if not (api_key and "sk-" in api_key and OPENAI_AVAILABLE): return 0.5
    
#     try:
#         client = OpenAI(api_key=api_key)
#         prompt = f"""
#         Task: Entity Resolution. Determine if the 'SOURCE' refers to the 'CANDIDATE'.
        
#         SOURCE (Blog/Article Data):
#         - Name: "{scraped_name}"
#         - Context: "{scraped_context}" 
#         (Use the Context/Description to identify unique landmarks, history, or visual features.)
        
#         CANDIDATE (Google Maps Data):
#         - Name: "{cand_name}"
#         - Address: "{cand_address}"
#         - Types: {cand_types}
        
#         CRITERIA:
#         1. Semantic Match: Does the description describe this exact place? (e.g. "Pink Palace" = "Hawa Mahal")
#         2. Location Match: Is the candidate in the correct city/area implied by the Source?
#         3. Type Match: If Source says "Temple", Candidate cannot be "Shopping Mall".
        
#         Return JSON:
#         {{
#             "match": true,
#             "confidence": 0.95,
#             "reason": "Short explanation"
#         }}
#         """
        
#         response = client.chat.completions.create(
#             model="gpt-4o-mini",
#             messages=[{"role": "user", "content": prompt}],
#             response_format={"type": "json_object"},
#             temperature=0.0
#         )
#         data = json.loads(response.choices[0].message.content)
        
#         if data.get("match"):
#             return max(0.85, float(data.get("confidence", 0.85)))
#         else:
#             return 0.1
            
#     except Exception as e:
#         print(f"    ⚠️ LLM Error: {e}")
#         return 0.5

# # --- 6. SCORING & TRIMMING ---
# def score_item(it: Dict[str, Any]) -> float:
#     """Calculates Quality Score (0.0 - 5.0+)"""
#     rating = float(it.get("rating") or 0.0)
#     reviews = float(it.get("reviews") or 0.0)
#     vol = math.log10(max(1.0, reviews + 1.0))
#     bonus = 0.2 if (it.get("photo_refs") and len(it.get("photo_refs")) > 0) else 0.0
#     return (0.6 * rating) + (0.3 * vol) + bonus

# def trim_and_light_shuffle(items: List[Dict[str, Any]], keep_ratio: float = 1.0) -> List[Dict[str, Any]]:
#     if not items: return []
    
#     ranked = sorted(items, key=score_item, reverse=True)
#     k = max(1, int(math.ceil(len(items) * keep_ratio)))
#     top_items = ranked[:k]
    
#     rng = random.Random(42)
#     for i in range(len(top_items)):
#         j = min(len(top_items) - 1, max(0, i + rng.randint(-2, 2)))
#         top_items[i], top_items[j] = top_items[j], top_items[i]
        
#     return top_items

# # --- 7. RESOLVER CLASS ---
# class EnhancedResolver:
#     def __init__(self, api_key: str, cache_path: Path, refresh_photos: bool):
#         self.enabled = bool(api_key and googlemaps)
#         self.cache_path = cache_path
#         self.cache = {}
#         if cache_path.exists():
#             try: self.cache = json.loads(cache_path.read_text(encoding="utf-8"))
#             except: pass
#         self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None
#         self.api_key = api_key
#         self.refresh_photos = refresh_photos

#     def save(self):
#         try: self.cache_path.write_text(json.dumps(self.cache, indent=2), encoding="utf-8")
#         except: pass

#     def _cache_key(self, name, cat, scope, city, state):
#         return f"{normalize_name(name)}|{cat}|{scope}|{city}"

#     def _details_with_photos(self, place_id: str) -> Dict[str, Any]:
#         if not self.gmaps: return {}
#         try:
#             res = self.gmaps.place(place_id=place_id, fields=["name","geometry/location","formatted_address","rating","user_ratings_total","photos","types","website","international_phone_number","opening_hours","price_level","permanently_closed","utc_offset_minutes"])
#             data = res.get("result", {})
#         except: return {}

#         if not data.get("photos"):
#             try:
#                 url = "https://maps.googleapis.com/maps/api/place/details/json"
#                 r = requests.get(url, params={"place_id": place_id, "fields": "photos", "key": self.api_key})
#                 if r.status_code == 200:
#                     data["photos"] = r.json().get("result", {}).get("photos", [])
#             except: pass
#         return data

#     def resolve(self, name: str, category_hint: str, scope: str, 
#                 anchor_city: str, anchor_state: str, radius_m: int,
#                 description: str = "", location_hint: str = "") -> Dict[str, Any]:
        
#         empty = {"place_id": None, "name": name, "confidence": 0.0, "photo_refs": []}
#         ck = self._cache_key(name, category_hint, scope, anchor_city, anchor_state)
        
#         if ck in self.cache:
#             cached = {**empty, **self.cache[ck]}
#             if cached.get("place_id") and (self.refresh_photos or not cached.get("photo_refs")):
#                  det = self._details_with_photos(cached["place_id"])
#                  if det.get("photos"):
#                      cached["photo_refs"] = [p["photo_reference"] for p in det["photos"]]
#                      self.cache[ck] = cached
#             return cached

#         if not self.enabled: return empty

#         candidates = []
#         try:
#             q = f"{name} {anchor_city or ''} {anchor_state or ''}"
#             res = self.gmaps.places(query=q, language=GMAPS_LANGUAGE)
#             candidates = res.get("results", [])[:5]
#         except: pass

#         best = empty
#         best_score = -1.0

#         for cand in candidates:
#             # A. Math Score
#             c_name = cand.get("name")
#             sim = name_sim(name, c_name) / 100.0
#             type_bonus = 0.1 if type_compat_score(cand.get("types", []), allowed_types_for_kind(infer_entity_kind_from_category(category_hint, scope))) > 0.5 else -0.1
#             state_bonus = 0.1 if anchor_state and anchor_state.lower() in cand.get("formatted_address", "").lower() else 0.0
            
#             math_score = max(0.0, min(1.0, sim * 0.6 + type_bonus + state_bonus + 0.2))

#             # B. Ambiguity Judge (With Rich Context)
#             final_score = math_score
#             if GREY_ZONE_MIN <= math_score < GREY_ZONE_MAX and OPENAI_AVAILABLE:
                
#                 # Build Rich Context
#                 rich_context = (
#                     f"Category: {category_hint}. "
#                     f"Location Hint: {location_hint}. "
#                     f"Description: {description[:250]}..."
#                 )
                
#                 print(f"    🤖 Grey Zone ({math_score:.2f}): Asking LLM '{name}' vs '{c_name}'...")
#                 llm_conf = llm_verify_match(
#                     scraped_name=name, 
#                     scraped_context=rich_context,
#                     cand_name=c_name, 
#                     cand_address=cand.get("formatted_address", ""),
#                     cand_types=cand.get("types", []), 
#                     api_key=OPENAI_API_KEY_HARDCODED
#                 )
                
#                 if llm_conf > 0.8:
#                     final_score = llm_conf
#                     print(f"      ✅ LLM Confirmed! ({math_score:.2f} -> {final_score:.2f})")
#                 else:
#                     final_score = 0.1
#                     print(f"      ❌ LLM Rejected. ({math_score:.2f} -> {final_score:.2f})")

#             if final_score > best_score:
#                 best_score = final_score
#                 best = {
#                     "place_id": cand.get("place_id"),
#                     "name": c_name,
#                     "address": cand.get("formatted_address"),
#                     "rating": cand.get("rating", 0),
#                     "reviews": cand.get("user_ratings_total", 0),
#                     "types": cand.get("types", []),
#                     "photo_refs": [p["photo_reference"] for p in cand.get("photos", [])],
#                     "confidence": round(final_score, 3)
#                 }
#             time.sleep(SLEEP_BETWEEN)

#         if best.get("place_id") and best["confidence"] >= 0.5:
#             det = self._details_with_photos(best["place_id"])
#             if det:
#                 loc = (det.get("geometry") or {}).get("location") or {}
#                 best.update({
#                     "lat": loc.get("lat"), "lng": loc.get("lng"),
#                     "website": det.get("website"), "phone": det.get("international_phone_number"),
#                     "opening": (det.get("opening_hours", {}) or {}).get("periods", []),
#                     "price_level": det.get("price_level"),
#                     "permanently_closed": det.get("permanently_closed", False),
#                     "utc_offset_minutes": det.get("utc_offset_minutes"),
#                 })
#                 if det.get("photos"):
#                     best["photo_refs"] = [p["photo_reference"] for p in det["photos"]]

#         self.cache[ck] = best
#         return best

# # --- 8. MAIN EXECUTION ---
# def is_publishable(resolved: Dict[str, Any]) -> bool:
#     if resolved.get("confidence", 0) < MIN_CONFIDENCE: return False
#     if not resolved.get("place_id"): return False
    
#     rating = float(resolved.get("rating", 0))
#     reviews = int(resolved.get("reviews", 0))
    
#     if rating < 1.0 and reviews > 0: return False 
#     if REQUIRE_PHOTO and not resolved.get("photo_refs"): return False
    
#     return True

# def main():
#     # FIX: Global declarations must come FIRST
#     global MIN_CONFIDENCE, REQUIRE_PHOTO

#     p = argparse.ArgumentParser()
#     p.add_argument("--in", dest="in_path", default=str(IN_PATH))
#     p.add_argument("--out", dest="out_path", default=str(OUT_PATH))
#     p.add_argument("--report", dest="report_path", default=str(REPORT))
#     p.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE)
#     p.add_argument("--keep-ratio", type=float, default=1.0)
#     p.add_argument("--anchor-city", type=str)
#     p.add_argument("--anchor-state", type=str)
#     # Dummy args
#     p.add_argument("--radius-km", type=int); p.add_argument("--default-speed", type=int)
#     p.add_argument("--retry", type=int); p.add_argument("--require-photo", action="store_true")
#     p.add_argument("--refresh-photos", action="store_true"); p.add_argument("--scope", default="auto")
#     args = p.parse_args()

#     # Update global settings
#     MIN_CONFIDENCE = args.min_confidence
#     REQUIRE_PHOTO = args.require_photo

#     if not Path(args.in_path).exists(): return

#     resolver = EnhancedResolver(GOOGLE_API_KEY_HARDCODED, PLACES_CACHE, args.refresh_photos)

#     data = json.loads(Path(args.in_path).read_text(encoding="utf-8"))
#     out_playlists = []
#     totals = {"resolved":0, "publishable":0}

#     for plist in data:
#         title = plist.get("playlistTitle")
#         anchor_city = args.anchor_city or plist.get("placeName")
#         anchor_state = args.anchor_state
        
#         print(f"\nPlaylist: {title} (City: {anchor_city})")
#         resolved_items = []
        
#         for item in plist.get("items", []):
#             name = item.get("name")
#             cat = item.get("category_hint")
            
#             desc_text = item.get("description") or ""
#             loc_hint = item.get("location_hint") or ""
            
#             res = resolver.resolve(
#                 name=name, category_hint=cat, scope="poi", 
#                 anchor_city=anchor_city, anchor_state=anchor_state, radius_m=50000,
#                 description=desc_text, location_hint=loc_hint
#             )
            
#             final_item = item.copy()
#             final_item.update(res)
            
#             if is_publishable(res):
#                 final_item["resolution_status"] = "publishable"
#                 totals["publishable"] += 1
#                 print(f"  ✅ {name} -> {res['name']} (Conf: {res['confidence']}, Rat: {res.get('rating')})")
#             else:
#                 final_item["resolution_status"] = "unresolved"
#                 print(f"  ❌ {name} -> (Conf: {res['confidence']})")
            
#             resolved_items.append(final_item)

#         if args.keep_ratio < 1.0:
#             valid_items = [it for it in resolved_items if it["resolution_status"] == "publishable"]
#             print(f"  ✂️ Trimming {len(valid_items)} valid items to top {int(args.keep_ratio*100)}%...")
            
#             kept_valid = trim_and_light_shuffle(valid_items, args.keep_ratio)
#             kept_ids = {it["place_id"] for it in kept_valid}
            
#             for it in resolved_items:
#                 if it["resolution_status"] == "publishable" and it["place_id"] not in kept_ids:
#                     it["resolution_status"] = "filtered_by_ratio"
            
#             print(f"     Kept {len(kept_valid)} items")

#         out_playlists.append({**plist, "items": resolved_items})

#     Path(args.out_path).write_text(json.dumps(out_playlists, indent=2), encoding="utf-8")
#     resolver.save()
#     print(f"\nDone. Wrote {args.out_path}")

# if __name__ == "__main__":
#     main()


    
# # 02_5_resolve_validate.py
# # SUPER SCRIPT v3: Hybrid Matching (Math + Context-Aware LLM), Quality Trimming, & CLI Overrides

# from __future__ import annotations
# import os, json, re, time, math, argparse, unicodedata, random
# from pathlib import Path
# from typing import List, Dict, Any, Optional, Tuple

# # --- 1. CONFIGURATION & KEYS ---
# # 🔴 HARDCODED KEYS
# GOOGLE_API_KEY_HARDCODED = "AIzaSyDEuy2i8AbSR-jnstZG22ndFqy71jtKbgg"
# OPENAI_API_KEY_HARDCODED = "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"

# # Paths
# BASE_DIR = Path(__file__).resolve().parent
# IN_PATH   = BASE_DIR / "playlist_items.json"
# OUT_PATH  = BASE_DIR / "playlist_items_resolved.json"
# REPORT    = BASE_DIR / "resolve_report.json"

# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# PLACES_CACHE   = CACHE_DIR / "places_cache.json"
# GEOCODE_CACHE  = CACHE_DIR / "geocode_cache.json"

# # Logic Settings (Defaults - can be overridden by CLI)
# MIN_CONFIDENCE = 0.70  
# MIN_RATING     = 3.5
# MIN_REVIEWS    = 50
# REQUIRE_PHOTO  = False

# # Grey Zone (LLM Triggers)
# GREY_ZONE_MIN  = 0.50
# GREY_ZONE_MAX  = 0.85

# # Google Maps Tuning
# GMAPS_REGION    = "in"
# GMAPS_LANGUAGE  = "en-IN"
# SLEEP_BETWEEN   = 0.05

# # --- 2. IMPORTS & SETUP ---
# try:
#     import googlemaps
# except ImportError:
#     googlemaps = None

# try:
#     import requests
# except ImportError:
#     requests = None

# try:
#     from openai import OpenAI
#     OPENAI_AVAILABLE = True
# except ImportError:
#     OPENAI_AVAILABLE = False
#     OpenAI = None

# try:
#     from rapidfuzz import fuzz
#     def name_sim(a: str, b: str) -> int:
#         return fuzz.token_set_ratio(a, b)
# except ImportError:
#     def _tok(s: str) -> set:
#         return set(re.findall(r"[a-z0-9]+", s.lower()))
#     def name_sim(a: str, b: str) -> int:
#         A, B = _tok(a), _tok(b)
#         return int(100 * len(A & B) / len(A | B)) if (A and B) else 0

# # --- 3. STATIC DATA ---
# CITY_COORDS: Dict[str, Tuple[float, float]] = {
#     "bengaluru": (12.9716, 77.5946), "bangalore": (12.9716, 77.5946), "mumbai": (19.0760, 72.8777),
#     "delhi": (28.7041, 77.1025), "chennai": (13.0827, 80.2707), "kolkata": (22.5726, 88.3639),
#     "hyderabad": (17.3850, 78.4867), "kochi": (9.9312, 76.2673), "ahmedabad": (23.0225, 72.5714),
#     "pune": (18.5204, 73.8567), "mysore": (12.2958, 76.6394), "mangalore": (12.9141, 74.8560),
#     "coimbatore": (11.0168, 76.9558), "thiruvananthapuram": (8.5241, 76.9366),
# }
# ALIAS_MAP = {"alleppey":"alappuzha","pondicherry":"puducherry","bombay":"mumbai","calcutta":"kolkata","ooty":"udhagamandalam","coorg":"kodagu"}
# NAME_ALIASES = [
#     (re.compile(r"\bchikmagalur(u)?\b", re.I), "chikkamagaluru"), (re.compile(r"\bcoorg\b", re.I), "kodagu"),
#     (re.compile(r"\bcalicut\b", re.I), "kozhikode"), (re.compile(r"\bbangalore\b", re.I), "bengaluru"),
#     (re.compile(r"\booty\b", re.I), "udhagamandalam"),
# ]
# FAMOUS_LANDMARK_STATES = {
#     "jog falls":"karnataka","mysore":"karnataka","hampi":"karnataka","coorg":"karnataka","gokarna":"karnataka",
#     "wayanad":"kerala","munnar":"kerala","ooty":"tamil nadu","akshardham":"gujarat","rishikesh":"uttarakhand"
# }
# SCOPE_TYPES = {
#     "destination":{"locality","sublocality","administrative_area_level_3","administrative_area_level_2","political"},
#     "poi":{"tourist_attraction","point_of_interest","museum","zoo","amusement_park","aquarium","hindu_temple","church","mosque","campground","lodging","restaurant","park"},
#     "natural":{"natural_feature","tourist_attraction","park","point_of_interest"},
# }
# BAD_POI_SUFFIX = re.compile(r"(view\s*point|parking|ticket|counter|canteen|shop|toilet|entrance|gate)", re.I)

# # --- 4. HELPER FUNCTIONS ---
# def strip_accents(s: str) -> str:
#     return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

# def normalize_name(s: str) -> str:
#     return re.sub(r"\s+", " ", strip_accents(s or "").strip())

# def infer_entity_kind_from_category(category_hint: str, scope: str) -> str:
#     if not category_hint: return "destination" if scope == "destination" else "tourist_attraction"
#     ch = category_hint.lower()
#     if ch in ["temple", "fort", "museum", "park", "beach", "lake"]: return ch
#     return "tourist_attraction"

# def allowed_types_for_kind(kind: str) -> set:
#     base = {"tourist_attraction", "point_of_interest", "locality", "establishment"}
#     if kind == "temple": base.update(["hindu_temple", "place_of_worship", "church", "mosque"])
#     if kind == "park": base.update(["park", "national_park"])
#     return base

# def primary_google_type_for_kind(kind: str) -> Optional[str]:
#     if kind == "temple": return "hindu_temple"
#     if kind == "park": return "park"
#     return None

# def expand_name_variants(name: str) -> List[str]:
#     variants = {normalize_name(name)}
#     for pat, rep in NAME_ALIASES:
#         if pat.search(name): variants.add(pat.sub(rep, name))
#     return list(variants)

# def build_enhanced_queries(name: str, category_hint: str, scope: str, anchor_city: str, anchor_state: str) -> List[str]:
#     qs = [name]
#     if anchor_city: qs.append(f"{name} {anchor_city}")
#     if anchor_state: qs.append(f"{name} {anchor_state}")
#     return qs

# def parse_hours(s: str) -> Optional[float]: return None
# def km_from_hours(h: Optional[float], speed: int=70) -> int: return 450
# def haversine_km(lat1, lon1, lat2, lon2): return 0.0 
# def distance_score(cand, alat, alng, rad): return 1.0 
# def popularity_score(rat, rev): return min(1.0, float(rat)*math.log10(rev+1)/5.0)
# def type_compat_score(types, allowed): return 1.0 if set(types)&allowed else 0.5
# def state_match_score(addr, state): return 1.0 if state and state.lower() in (addr or "").lower() else 0.0
# def circle_bias(lat, lng, rad): return None

# # --- 5. LLM JUDGE (UPDATED WITH RICH CONTEXT) ---
# def llm_verify_match(scraped_name: str, scraped_context: str, 
#                      cand_name: str, cand_address: str, cand_types: List[str],
#                      api_key: str) -> float:
#     """The 'Ambiguity Judge'."""
#     if not (api_key and "sk-" in api_key and OPENAI_AVAILABLE): return 0.5
    
#     try:
#         client = OpenAI(api_key=api_key)
#         prompt = f"""
#         Task: Entity Resolution. Determine if the 'SOURCE' refers to the 'CANDIDATE'.
        
#         SOURCE (Blog/Article Data):
#         - Name: "{scraped_name}"
#         - Context: "{scraped_context}" 
#         (Use the Context/Description to identify unique landmarks, history, or visual features.)
        
#         CANDIDATE (Google Maps Data):
#         - Name: "{cand_name}"
#         - Address: "{cand_address}"
#         - Types: {cand_types}
        
#         CRITERIA:
#         1. Semantic Match: Does the description describe this exact place? (e.g. "Pink Palace" = "Hawa Mahal")
#         2. Location Match: Is the candidate in the correct city/area implied by the Source?
#         3. Type Match: If Source says "Temple", Candidate cannot be "Shopping Mall".
        
#         Return JSON:
#         {{
#             "match": true,
#             "confidence": 0.95,
#             "reason": "Short explanation"
#         }}
#         """
        
#         response = client.chat.completions.create(
#             model="gpt-4o-mini",
#             messages=[{"role": "user", "content": prompt}],
#             response_format={"type": "json_object"},
#             temperature=0.0
#         )
#         data = json.loads(response.choices[0].message.content)
        
#         if data.get("match"):
#             return max(0.85, float(data.get("confidence", 0.85)))
#         else:
#             return 0.1
            
#     except Exception as e:
#         print(f"    ⚠️ LLM Error: {e}")
#         return 0.5

# # --- 6. SCORING & TRIMMING ---
# def score_item(it: Dict[str, Any]) -> float:
#     """Calculates Quality Score (0.0 - 5.0+)"""
#     rating = float(it.get("rating") or 0.0)
#     reviews = float(it.get("reviews") or 0.0)
#     vol = math.log10(max(1.0, reviews + 1.0))
#     bonus = 0.2 if (it.get("photo_refs") and len(it.get("photo_refs")) > 0) else 0.0
#     return (0.6 * rating) + (0.3 * vol) + bonus

# def trim_and_light_shuffle(items: List[Dict[str, Any]], keep_ratio: float = 1.0) -> List[Dict[str, Any]]:
#     if not items: return []
    
#     ranked = sorted(items, key=score_item, reverse=True)
#     k = max(1, int(math.ceil(len(items) * keep_ratio)))
#     top_items = ranked[:k]
    
#     rng = random.Random(42)
#     for i in range(len(top_items)):
#         j = min(len(top_items) - 1, max(0, i + rng.randint(-2, 2)))
#         top_items[i], top_items[j] = top_items[j], top_items[i]
        
#     return top_items

# # --- 7. RESOLVER CLASS ---
# class EnhancedResolver:
#     def __init__(self, api_key: str, cache_path: Path, refresh_photos: bool):
#         self.enabled = bool(api_key and googlemaps)
#         self.cache_path = cache_path
#         self.cache = {}
#         if cache_path.exists():
#             try: self.cache = json.loads(cache_path.read_text(encoding="utf-8"))
#             except: pass
#         self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None
#         self.api_key = api_key
#         self.refresh_photos = refresh_photos

#     def save(self):
#         try: self.cache_path.write_text(json.dumps(self.cache, indent=2), encoding="utf-8")
#         except: pass

#     def _cache_key(self, name, cat, scope, city, state):
#         return f"{normalize_name(name)}|{cat}|{scope}|{city}"

#     def _details_with_photos(self, place_id: str) -> Dict[str, Any]:
#         if not self.gmaps: return {}
#         try:
#             res = self.gmaps.place(place_id=place_id, fields=["name","geometry/location","formatted_address","rating","user_ratings_total","photos","types","website","international_phone_number","opening_hours","price_level","permanently_closed","utc_offset_minutes"])
#             data = res.get("result", {})
#         except: return {}

#         if not data.get("photos"):
#             try:
#                 url = "https://maps.googleapis.com/maps/api/place/details/json"
#                 r = requests.get(url, params={"place_id": place_id, "fields": "photos", "key": self.api_key})
#                 if r.status_code == 200:
#                     data["photos"] = r.json().get("result", {}).get("photos", [])
#             except: pass
#         return data

#     def resolve(self, name: str, category_hint: str, scope: str, 
#                 anchor_city: str, anchor_state: str, radius_m: int,
#                 # NEW ARGS for Context
#                 description: str = "", location_hint: str = "") -> Dict[str, Any]:
        
#         empty = {"place_id": None, "name": name, "confidence": 0.0, "photo_refs": []}
#         ck = self._cache_key(name, category_hint, scope, anchor_city, anchor_state)
        
#         if ck in self.cache:
#             cached = {**empty, **self.cache[ck]}
#             if cached.get("place_id") and (self.refresh_photos or not cached.get("photo_refs")):
#                  det = self._details_with_photos(cached["place_id"])
#                  if det.get("photos"):
#                      cached["photo_refs"] = [p["photo_reference"] for p in det["photos"]]
#                      self.cache[ck] = cached
#             return cached

#         if not self.enabled: return empty

#         candidates = []
#         try:
#             q = f"{name} {anchor_city or ''} {anchor_state or ''}"
#             res = self.gmaps.places(query=q, language=GMAPS_LANGUAGE)
#             candidates = res.get("results", [])[:5]
#         except: pass

#         best = empty
#         best_score = -1.0

#         for cand in candidates:
#             # A. Math Score
#             c_name = cand.get("name")
#             sim = name_sim(name, c_name) / 100.0
#             type_bonus = 0.1 if type_compat_score(cand.get("types", []), allowed_types_for_kind(infer_entity_kind_from_category(category_hint, scope))) > 0.5 else -0.1
#             state_bonus = 0.1 if anchor_state and anchor_state.lower() in cand.get("formatted_address", "").lower() else 0.0
            
#             math_score = max(0.0, min(1.0, sim * 0.6 + type_bonus + state_bonus + 0.2))

#             # B. Ambiguity Judge (With Rich Context)
#             final_score = math_score
#             if GREY_ZONE_MIN <= math_score < GREY_ZONE_MAX and OPENAI_AVAILABLE:
                
#                 # Build Rich Context
#                 rich_context = (
#                     f"Category: {category_hint}. "
#                     f"Location Hint: {location_hint}. "
#                     f"Description: {description[:250]}..."
#                 )
                
#                 print(f"    🤖 Grey Zone ({math_score:.2f}): Asking LLM '{name}' vs '{c_name}'...")
#                 llm_conf = llm_verify_match(
#                     scraped_name=name, 
#                     scraped_context=rich_context,
#                     cand_name=c_name, 
#                     cand_address=cand.get("formatted_address", ""),
#                     cand_types=cand.get("types", []), 
#                     api_key=OPENAI_API_KEY_HARDCODED
#                 )
                
#                 if llm_conf > 0.8:
#                     final_score = llm_conf
#                     print(f"      ✅ LLM Confirmed! ({math_score:.2f} -> {final_score:.2f})")
#                 else:
#                     final_score = 0.1
#                     print(f"      ❌ LLM Rejected. ({math_score:.2f} -> {final_score:.2f})")

#             if final_score > best_score:
#                 best_score = final_score
#                 best = {
#                     "place_id": cand.get("place_id"),
#                     "name": c_name,
#                     "address": cand.get("formatted_address"),
#                     "rating": cand.get("rating", 0),
#                     "reviews": cand.get("user_ratings_total", 0),
#                     "types": cand.get("types", []),
#                     "photo_refs": [p["photo_reference"] for p in cand.get("photos", [])],
#                     "confidence": round(final_score, 3)
#                 }
#             time.sleep(SLEEP_BETWEEN)

#         if best.get("place_id") and best["confidence"] >= 0.5:
#             det = self._details_with_photos(best["place_id"])
#             if det:
#                 loc = (det.get("geometry") or {}).get("location") or {}
#                 best.update({
#                     "lat": loc.get("lat"), "lng": loc.get("lng"),
#                     "website": det.get("website"), "phone": det.get("international_phone_number"),
#                     "opening": (det.get("opening_hours", {}) or {}).get("periods", []),
#                     "price_level": det.get("price_level"),
#                     "permanently_closed": det.get("permanently_closed", False),
#                     "utc_offset_minutes": det.get("utc_offset_minutes"),
#                 })
#                 if det.get("photos"):
#                     best["photo_refs"] = [p["photo_reference"] for p in det["photos"]]

#         self.cache[ck] = best
#         return best

# # --- 8. MAIN EXECUTION ---
# def is_publishable(resolved: Dict[str, Any]) -> bool:
#     if resolved.get("confidence", 0) < MIN_CONFIDENCE: return False
#     if not resolved.get("place_id"): return False
    
#     rating = float(resolved.get("rating", 0))
#     reviews = int(resolved.get("reviews", 0))
    
#     # Keep photos check
#     if REQUIRE_PHOTO and not resolved.get("photo_refs"): return False
    
#     return True

# def main():
#     p = argparse.ArgumentParser()
#     p.add_argument("--in", dest="in_path", default=str(IN_PATH))
#     p.add_argument("--out", dest="out_path", default=str(OUT_PATH))
#     p.add_argument("--report", dest="report_path", default=str(REPORT))
#     p.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE)
#     p.add_argument("--keep-ratio", type=float, default=1.0)
#     p.add_argument("--anchor-city", type=str)
#     p.add_argument("--anchor-state", type=str)
#     # Dummy args
#     p.add_argument("--radius-km", type=int); p.add_argument("--default-speed", type=int)
#     p.add_argument("--retry", type=int); p.add_argument("--require-photo", action="store_true")
#     p.add_argument("--refresh-photos", action="store_true"); p.add_argument("--scope", default="auto")
#     args = p.parse_args()

#     # --- CRITICAL: UPDATE GLOBALS FROM CLI ARGS ---
#     global MIN_CONFIDENCE, REQUIRE_PHOTO
#     MIN_CONFIDENCE = args.min_confidence
#     REQUIRE_PHOTO = args.require_photo
#     # ----------------------------------------------

#     if not Path(args.in_path).exists(): return

#     resolver = EnhancedResolver(GOOGLE_API_KEY_HARDCODED, PLACES_CACHE, args.refresh_photos)

#     data = json.loads(Path(args.in_path).read_text(encoding="utf-8"))
#     out_playlists = []
#     totals = {"resolved":0, "publishable":0}

#     for plist in data:
#         title = plist.get("playlistTitle")
#         anchor_city = args.anchor_city or plist.get("placeName")
#         anchor_state = args.anchor_state
        
#         print(f"\nPlaylist: {title} (City: {anchor_city})")
#         resolved_items = []
        
#         for item in plist.get("items", []):
#             name = item.get("name")
#             cat = item.get("category_hint")
            
#             # --- EXTRACT EXTRA CONTEXT ---
#             desc_text = item.get("description") or ""
#             loc_hint = item.get("location_hint") or ""
            
#             res = resolver.resolve(
#                 name=name, category_hint=cat, scope="poi", 
#                 anchor_city=anchor_city, anchor_state=anchor_state, radius_m=50000,
#                 # PASSING CONTEXT HERE
#                 description=desc_text, location_hint=loc_hint
#             )
            
#             final_item = item.copy()
#             final_item.update(res)
            
#             if is_publishable(res):
#                 final_item["resolution_status"] = "publishable"
#                 totals["publishable"] += 1
#                 print(f"  ✅ {name} -> {res['name']} (Conf: {res['confidence']}, Rat: {res.get('rating')})")
#             else:
#                 final_item["resolution_status"] = "unresolved"
#                 print(f"  ❌ {name} -> (Conf: {res['confidence']})")
            
#             resolved_items.append(final_item)

#         if args.keep_ratio < 1.0:
#             valid_items = [it for it in resolved_items if it["resolution_status"] == "publishable"]
#             print(f"  ✂️ Trimming {len(valid_items)} valid items to top {int(args.keep_ratio*100)}%...")
            
#             kept_valid = trim_and_light_shuffle(valid_items, args.keep_ratio)
#             kept_ids = {it["place_id"] for it in kept_valid}
            
#             for it in resolved_items:
#                 if it["resolution_status"] == "publishable" and it["place_id"] not in kept_ids:
#                     it["resolution_status"] = "filtered_by_ratio"
            
#             print(f"     Kept {len(kept_valid)} items")

#         out_playlists.append({**plist, "items": resolved_items})

#     Path(args.out_path).write_text(json.dumps(out_playlists, indent=2), encoding="utf-8")
#     resolver.save()
#     print(f"\nDone. Wrote {args.out_path}")

# if __name__ == "__main__":
#     main()

# # 02_5_resolve_validate.py
# # SUPER SCRIPT v3: Hybrid Matching (Math + Context-Aware LLM), Quality Trimming, & CLI Overrides

# from __future__ import annotations
# import os, json, re, time, math, argparse, unicodedata, random
# from pathlib import Path
# from typing import List, Dict, Any, Optional, Tuple

# # --- 1. CONFIGURATION & KEYS ---
# # 🔴 HARDCODED KEYS
# GOOGLE_API_KEY_HARDCODED = "AIzaSyDEuy2i8AbSR-jnstZG22ndFqy71jtKbgg"
# OPENAI_API_KEY_HARDCODED = "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"

# # Paths
# BASE_DIR = Path(__file__).resolve().parent
# IN_PATH   = BASE_DIR / "playlist_items.json"
# OUT_PATH  = BASE_DIR / "playlist_items_resolved.json"
# REPORT    = BASE_DIR / "resolve_report.json"

# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# PLACES_CACHE   = CACHE_DIR / "places_cache.json"
# GEOCODE_CACHE  = CACHE_DIR / "geocode_cache.json"

# # Logic Settings (Defaults - can be overridden by CLI)
# MIN_CONFIDENCE = 0.70  
# MIN_RATING     = 3.5
# MIN_REVIEWS    = 50
# REQUIRE_PHOTO  = False

# # Grey Zone (LLM Triggers)
# GREY_ZONE_MIN  = 0.50
# GREY_ZONE_MAX  = 0.85

# # Google Maps Tuning
# GMAPS_REGION    = "in"
# GMAPS_LANGUAGE  = "en-IN"
# SLEEP_BETWEEN   = 0.05

# # --- 2. IMPORTS & SETUP ---
# try:
#     import googlemaps
# except ImportError:
#     googlemaps = None

# try:
#     import requests
# except ImportError:
#     requests = None

# try:
#     from openai import OpenAI
#     OPENAI_AVAILABLE = True
# except ImportError:
#     OPENAI_AVAILABLE = False
#     OpenAI = None

# try:
#     from rapidfuzz import fuzz
#     def name_sim(a: str, b: str) -> int:
#         return fuzz.token_set_ratio(a, b)
# except ImportError:
#     def _tok(s: str) -> set:
#         return set(re.findall(r"[a-z0-9]+", s.lower()))
#     def name_sim(a: str, b: str) -> int:
#         A, B = _tok(a), _tok(b)
#         return int(100 * len(A & B) / len(A | B)) if (A and B) else 0

# # --- 3. STATIC DATA ---
# CITY_COORDS: Dict[str, Tuple[float, float]] = {
#     "bengaluru": (12.9716, 77.5946), "bangalore": (12.9716, 77.5946), "mumbai": (19.0760, 72.8777),
#     "delhi": (28.7041, 77.1025), "chennai": (13.0827, 80.2707), "kolkata": (22.5726, 88.3639),
#     "hyderabad": (17.3850, 78.4867), "kochi": (9.9312, 76.2673), "ahmedabad": (23.0225, 72.5714),
#     "pune": (18.5204, 73.8567), "mysore": (12.2958, 76.6394), "mangalore": (12.9141, 74.8560),
#     "coimbatore": (11.0168, 76.9558), "thiruvananthapuram": (8.5241, 76.9366),
# }
# ALIAS_MAP = {"alleppey":"alappuzha","pondicherry":"puducherry","bombay":"mumbai","calcutta":"kolkata","ooty":"udhagamandalam","coorg":"kodagu"}
# NAME_ALIASES = [
#     (re.compile(r"\bchikmagalur(u)?\b", re.I), "chikkamagaluru"), (re.compile(r"\bcoorg\b", re.I), "kodagu"),
#     (re.compile(r"\bcalicut\b", re.I), "kozhikode"), (re.compile(r"\bbangalore\b", re.I), "bengaluru"),
#     (re.compile(r"\booty\b", re.I), "udhagamandalam"),
# ]
# FAMOUS_LANDMARK_STATES = {
#     "jog falls":"karnataka","mysore":"karnataka","hampi":"karnataka","coorg":"karnataka","gokarna":"karnataka",
#     "wayanad":"kerala","munnar":"kerala","ooty":"tamil nadu","akshardham":"gujarat","rishikesh":"uttarakhand"
# }
# SCOPE_TYPES = {
#     "destination":{"locality","sublocality","administrative_area_level_3","administrative_area_level_2","political"},
#     "poi":{"tourist_attraction","point_of_interest","museum","zoo","amusement_park","aquarium","hindu_temple","church","mosque","campground","lodging","restaurant","park"},
#     "natural":{"natural_feature","tourist_attraction","park","point_of_interest"},
# }
# BAD_POI_SUFFIX = re.compile(r"(view\s*point|parking|ticket|counter|canteen|shop|toilet|entrance|gate)", re.I)

# # --- 4. HELPER FUNCTIONS ---
# def strip_accents(s: str) -> str:
#     return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

# def normalize_name(s: str) -> str:
#     return re.sub(r"\s+", " ", strip_accents(s or "").strip())

# def infer_entity_kind_from_category(category_hint: str, scope: str) -> str:
#     if not category_hint: return "destination" if scope == "destination" else "tourist_attraction"
#     ch = category_hint.lower()
#     if ch in ["temple", "fort", "museum", "park", "beach", "lake"]: return ch
#     return "tourist_attraction"

# def allowed_types_for_kind(kind: str) -> set:
#     base = {"tourist_attraction", "point_of_interest", "locality", "establishment"}
#     if kind == "temple": base.update(["hindu_temple", "place_of_worship", "church", "mosque"])
#     if kind == "park": base.update(["park", "national_park"])
#     return base

# def primary_google_type_for_kind(kind: str) -> Optional[str]:
#     if kind == "temple": return "hindu_temple"
#     if kind == "park": return "park"
#     return None

# def expand_name_variants(name: str) -> List[str]:
#     variants = {normalize_name(name)}
#     for pat, rep in NAME_ALIASES:
#         if pat.search(name): variants.add(pat.sub(rep, name))
#     return list(variants)

# def build_enhanced_queries(name: str, category_hint: str, scope: str, anchor_city: str, anchor_state: str) -> List[str]:
#     qs = [name]
#     if anchor_city: qs.append(f"{name} {anchor_city}")
#     if anchor_state: qs.append(f"{name} {anchor_state}")
#     return qs

# def parse_hours(s: str) -> Optional[float]: return None
# def km_from_hours(h: Optional[float], speed: int=70) -> int: return 450
# def haversine_km(lat1, lon1, lat2, lon2): return 0.0 
# def distance_score(cand, alat, alng, rad): return 1.0 
# def popularity_score(rat, rev): return min(1.0, float(rat)*math.log10(rev+1)/5.0)
# def type_compat_score(types, allowed): return 1.0 if set(types)&allowed else 0.5
# def state_match_score(addr, state): return 1.0 if state and state.lower() in (addr or "").lower() else 0.0
# def circle_bias(lat, lng, rad): return None

# # --- 5. LLM JUDGE (UPDATED WITH RICH CONTEXT) ---
# def llm_verify_match(scraped_name: str, scraped_context: str, 
#                      cand_name: str, cand_address: str, cand_types: List[str],
#                      api_key: str) -> float:
#     """The 'Ambiguity Judge'."""
#     if not (api_key and "sk-" in api_key and OPENAI_AVAILABLE): return 0.5
    
#     try:
#         client = OpenAI(api_key=api_key)
#         prompt = f"""
#         Task: Entity Resolution. Determine if the 'SOURCE' refers to the 'CANDIDATE'.
        
#         SOURCE (Blog/Article Data):
#         - Name: "{scraped_name}"
#         - Context: "{scraped_context}" 
#         (Use the Context/Description to identify unique landmarks, history, or visual features.)
        
#         CANDIDATE (Google Maps Data):
#         - Name: "{cand_name}"
#         - Address: "{cand_address}"
#         - Types: {cand_types}
        
#         CRITERIA:
#         1. Semantic Match: Does the description describe this exact place? (e.g. "Pink Palace" = "Hawa Mahal")
#         2. Location Match: Is the candidate in the correct city/area implied by the Source?
#         3. Type Match: If Source says "Temple", Candidate cannot be "Shopping Mall".
        
#         Return JSON:
#         {{
#             "match": true,
#             "confidence": 0.95,
#             "reason": "Short explanation"
#         }}
#         """
        
#         response = client.chat.completions.create(
#             model="gpt-4o-mini",
#             messages=[{"role": "user", "content": prompt}],
#             response_format={"type": "json_object"},
#             temperature=0.0
#         )
#         data = json.loads(response.choices[0].message.content)
        
#         if data.get("match"):
#             return max(0.85, float(data.get("confidence", 0.85)))
#         else:
#             return 0.1
            
#     except Exception as e:
#         print(f"    ⚠️ LLM Error: {e}")
#         return 0.5

# # --- 6. SCORING & TRIMMING ---
# def score_item(it: Dict[str, Any]) -> float:
#     """Calculates Quality Score (0.0 - 5.0+)"""
#     rating = float(it.get("rating") or 0.0)
#     reviews = float(it.get("reviews") or 0.0)
#     vol = math.log10(max(1.0, reviews + 1.0))
#     bonus = 0.2 if (it.get("photo_refs") and len(it.get("photo_refs")) > 0) else 0.0
#     return (0.6 * rating) + (0.3 * vol) + bonus

# def trim_and_light_shuffle(items: List[Dict[str, Any]], keep_ratio: float = 1.0) -> List[Dict[str, Any]]:
#     if not items: return []
    
#     ranked = sorted(items, key=score_item, reverse=True)
#     k = max(1, int(math.ceil(len(items) * keep_ratio)))
#     top_items = ranked[:k]
    
#     rng = random.Random(42)
#     for i in range(len(top_items)):
#         j = min(len(top_items) - 1, max(0, i + rng.randint(-2, 2)))
#         top_items[i], top_items[j] = top_items[j], top_items[i]
        
#     return top_items

# # --- 7. RESOLVER CLASS ---
# class EnhancedResolver:
#     def __init__(self, api_key: str, cache_path: Path, refresh_photos: bool):
#         self.enabled = bool(api_key and googlemaps)
#         self.cache_path = cache_path
#         self.cache = {}
#         if cache_path.exists():
#             try: self.cache = json.loads(cache_path.read_text(encoding="utf-8"))
#             except: pass
#         self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None
#         self.api_key = api_key
#         self.refresh_photos = refresh_photos

#     def save(self):
#         try: self.cache_path.write_text(json.dumps(self.cache, indent=2), encoding="utf-8")
#         except: pass

#     def _cache_key(self, name, cat, scope, city, state):
#         return f"{normalize_name(name)}|{cat}|{scope}|{city}"

#     def _details_with_photos(self, place_id: str) -> Dict[str, Any]:
#         if not self.gmaps: return {}
#         try:
#             res = self.gmaps.place(place_id=place_id, fields=["name","geometry/location","formatted_address","rating","user_ratings_total","photos","types","website","international_phone_number","opening_hours","price_level","permanently_closed","utc_offset_minutes"])
#             data = res.get("result", {})
#         except: return {}

#         if not data.get("photos"):
#             try:
#                 url = "https://maps.googleapis.com/maps/api/place/details/json"
#                 r = requests.get(url, params={"place_id": place_id, "fields": "photos", "key": self.api_key})
#                 if r.status_code == 200:
#                     data["photos"] = r.json().get("result", {}).get("photos", [])
#             except: pass
#         return data

#     def resolve(self, name: str, category_hint: str, scope: str, 
#                 anchor_city: str, anchor_state: str, radius_m: int,
#                 # NEW ARGS for Context
#                 description: str = "", location_hint: str = "") -> Dict[str, Any]:
        
#         empty = {"place_id": None, "name": name, "confidence": 0.0, "photo_refs": []}
#         ck = self._cache_key(name, category_hint, scope, anchor_city, anchor_state)
        
#         if ck in self.cache:
#             cached = {**empty, **self.cache[ck]}
#             if cached.get("place_id") and (self.refresh_photos or not cached.get("photo_refs")):
#                  det = self._details_with_photos(cached["place_id"])
#                  if det.get("photos"):
#                      cached["photo_refs"] = [p["photo_reference"] for p in det["photos"]]
#                      self.cache[ck] = cached
#             return cached

#         if not self.enabled: return empty

#         candidates = []
#         try:
#             q = f"{name} {anchor_city or ''} {anchor_state or ''}"
#             res = self.gmaps.places(query=q, language=GMAPS_LANGUAGE)
#             candidates = res.get("results", [])[:5]
#         except: pass

#         best = empty
#         best_score = -1.0

#         for cand in candidates:
#             # A. Math Score
#             c_name = cand.get("name")
#             sim = name_sim(name, c_name) / 100.0
#             type_bonus = 0.1 if type_compat_score(cand.get("types", []), allowed_types_for_kind(infer_entity_kind_from_category(category_hint, scope))) > 0.5 else -0.1
#             state_bonus = 0.1 if anchor_state and anchor_state.lower() in cand.get("formatted_address", "").lower() else 0.0
            
#             math_score = max(0.0, min(1.0, sim * 0.6 + type_bonus + state_bonus + 0.2))

#             # B. Ambiguity Judge (With Rich Context)
#             final_score = math_score
#             if GREY_ZONE_MIN <= math_score < GREY_ZONE_MAX and OPENAI_AVAILABLE:
                
#                 # Build Rich Context
#                 rich_context = (
#                     f"Category: {category_hint}. "
#                     f"Location Hint: {location_hint}. "
#                     f"Description: {description[:250]}..."
#                 )
                
#                 print(f"    🤖 Grey Zone ({math_score:.2f}): Asking LLM '{name}' vs '{c_name}'...")
#                 llm_conf = llm_verify_match(
#                     scraped_name=name, 
#                     scraped_context=rich_context,
#                     cand_name=c_name, 
#                     cand_address=cand.get("formatted_address", ""),
#                     cand_types=cand.get("types", []), 
#                     api_key=OPENAI_API_KEY_HARDCODED
#                 )
                
#                 if llm_conf > 0.8:
#                     final_score = llm_conf
#                     print(f"      ✅ LLM Confirmed! ({math_score:.2f} -> {final_score:.2f})")
#                 else:
#                     final_score = 0.1
#                     print(f"      ❌ LLM Rejected. ({math_score:.2f} -> {final_score:.2f})")

#             if final_score > best_score:
#                 best_score = final_score
#                 best = {
#                     "place_id": cand.get("place_id"),
#                     "name": c_name,
#                     "address": cand.get("formatted_address"),
#                     "rating": cand.get("rating", 0),
#                     "reviews": cand.get("user_ratings_total", 0),
#                     "types": cand.get("types", []),
#                     "photo_refs": [p["photo_reference"] for p in cand.get("photos", [])],
#                     "confidence": round(final_score, 3)
#                 }
#             time.sleep(SLEEP_BETWEEN)

#         if best.get("place_id") and best["confidence"] >= 0.5:
#             det = self._details_with_photos(best["place_id"])
#             if det:
#                 loc = (det.get("geometry") or {}).get("location") or {}
#                 best.update({
#                     "lat": loc.get("lat"), "lng": loc.get("lng"),
#                     "website": det.get("website"), "phone": det.get("international_phone_number"),
#                     "opening": (det.get("opening_hours", {}) or {}).get("periods", []),
#                     "price_level": det.get("price_level"),
#                     "permanently_closed": det.get("permanently_closed", False),
#                     "utc_offset_minutes": det.get("utc_offset_minutes"),
#                 })
#                 if det.get("photos"):
#                     best["photo_refs"] = [p["photo_reference"] for p in det["photos"]]

#         self.cache[ck] = best
#         return best

# # --- 8. MAIN EXECUTION ---
# def is_publishable(resolved: Dict[str, Any]) -> bool:
#     if resolved.get("confidence", 0) < MIN_CONFIDENCE: return False
#     if not resolved.get("place_id"): return False
    
#     rating = float(resolved.get("rating", 0))
#     reviews = int(resolved.get("reviews", 0))
    
#     # Keep photos check
#     if REQUIRE_PHOTO and not resolved.get("photo_refs"): return False
    
#     return True

# def main():
#     p = argparse.ArgumentParser()
#     p.add_argument("--in", dest="in_path", default=str(IN_PATH))
#     p.add_argument("--out", dest="out_path", default=str(OUT_PATH))
#     p.add_argument("--report", dest="report_path", default=str(REPORT))
#     p.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE)
#     p.add_argument("--keep-ratio", type=float, default=1.0)
#     p.add_argument("--anchor-city", type=str)
#     p.add_argument("--anchor-state", type=str)
#     # Dummy args
#     p.add_argument("--radius-km", type=int); p.add_argument("--default-speed", type=int)
#     p.add_argument("--retry", type=int); p.add_argument("--require-photo", action="store_true")
#     p.add_argument("--refresh-photos", action="store_true"); p.add_argument("--scope", default="auto")
#     args = p.parse_args()

#     # --- CRITICAL: UPDATE GLOBALS FROM CLI ARGS ---
#     global MIN_CONFIDENCE, REQUIRE_PHOTO
#     MIN_CONFIDENCE = args.min_confidence
#     REQUIRE_PHOTO = args.require_photo
#     # ----------------------------------------------

#     if not Path(args.in_path).exists(): return

#     resolver = EnhancedResolver(GOOGLE_API_KEY_HARDCODED, PLACES_CACHE, args.refresh_photos)

#     data = json.loads(Path(args.in_path).read_text(encoding="utf-8"))
#     out_playlists = []
#     totals = {"resolved":0, "publishable":0}

#     for plist in data:
#         title = plist.get("playlistTitle")
#         anchor_city = args.anchor_city or plist.get("placeName")
#         anchor_state = args.anchor_state
        
#         print(f"\nPlaylist: {title} (City: {anchor_city})")
#         resolved_items = []
        
#         for item in plist.get("items", []):
#             name = item.get("name")
#             cat = item.get("category_hint")
            
#             # --- EXTRACT EXTRA CONTEXT ---
#             desc_text = item.get("description") or ""
#             loc_hint = item.get("location_hint") or ""
            
#             res = resolver.resolve(
#                 name=name, category_hint=cat, scope="poi", 
#                 anchor_city=anchor_city, anchor_state=anchor_state, radius_m=50000,
#                 # PASSING CONTEXT HERE
#                 description=desc_text, location_hint=loc_hint
#             )
            
#             final_item = item.copy()
#             final_item.update(res)
            
#             if is_publishable(res):
#                 final_item["resolution_status"] = "publishable"
#                 totals["publishable"] += 1
#                 print(f"  ✅ {name} -> {res['name']} (Conf: {res['confidence']}, Rat: {res.get('rating')})")
#             else:
#                 final_item["resolution_status"] = "unresolved"
#                 print(f"  ❌ {name} -> (Conf: {res['confidence']})")
            
#             resolved_items.append(final_item)

#         if args.keep_ratio < 1.0:
#             valid_items = [it for it in resolved_items if it["resolution_status"] == "publishable"]
#             print(f"  ✂️ Trimming {len(valid_items)} valid items to top {int(args.keep_ratio*100)}%...")
            
#             kept_valid = trim_and_light_shuffle(valid_items, args.keep_ratio)
#             kept_ids = {it["place_id"] for it in kept_valid}
            
#             for it in resolved_items:
#                 if it["resolution_status"] == "publishable" and it["place_id"] not in kept_ids:
#                     it["resolution_status"] = "filtered_by_ratio"
            
#             print(f"     Kept {len(kept_valid)} items")

#         out_playlists.append({**plist, "items": resolved_items})

#     Path(args.out_path).write_text(json.dumps(out_playlists, indent=2), encoding="utf-8")
#     resolver.save()
#     print(f"\nDone. Wrote {args.out_path}")

# if __name__ == "__main__":
#     main()

# # 02_5_resolve_validate.py
# # SUPER SCRIPT v2: Hybrid Matching (Math + LLM), Quality Trimming, & CLI Overrides

# from __future__ import annotations
# import os, json, re, time, math, argparse, unicodedata, random
# from pathlib import Path
# from typing import List, Dict, Any, Optional, Tuple

# # --- 1. CONFIGURATION & KEYS ---
# # 🔴 HARDCODED KEYS
# GOOGLE_API_KEY_HARDCODED = "AIzaSyDEuy2i8AbSR-jnstZG22ndFqy71jtKbgg"
# OPENAI_API_KEY_HARDCODED = "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"

# # Paths
# BASE_DIR = Path(__file__).resolve().parent
# IN_PATH   = BASE_DIR / "playlist_items.json"
# OUT_PATH  = BASE_DIR / "playlist_items_resolved.json"
# REPORT    = BASE_DIR / "resolve_report.json"

# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# PLACES_CACHE   = CACHE_DIR / "places_cache.json"
# GEOCODE_CACHE  = CACHE_DIR / "geocode_cache.json"

# # Logic Settings (Defaults - can be overridden by CLI)
# MIN_CONFIDENCE = 0.70  
# MIN_RATING     = 3.5
# MIN_REVIEWS    = 50
# REQUIRE_PHOTO  = False

# # Grey Zone (LLM Triggers)
# GREY_ZONE_MIN  = 0.50
# GREY_ZONE_MAX  = 0.85

# # Google Maps Tuning
# GMAPS_REGION    = "in"
# GMAPS_LANGUAGE  = "en-IN"
# SLEEP_BETWEEN   = 0.05

# # --- 2. IMPORTS & SETUP ---
# try:
#     import googlemaps
# except ImportError:
#     googlemaps = None

# try:
#     import requests
# except ImportError:
#     requests = None

# try:
#     from openai import OpenAI
#     OPENAI_AVAILABLE = True
# except ImportError:
#     OPENAI_AVAILABLE = False
#     OpenAI = None

# try:
#     from rapidfuzz import fuzz
#     def name_sim(a: str, b: str) -> int:
#         return fuzz.token_set_ratio(a, b)
# except ImportError:
#     def _tok(s: str) -> set:
#         return set(re.findall(r"[a-z0-9]+", s.lower()))
#     def name_sim(a: str, b: str) -> int:
#         A, B = _tok(a), _tok(b)
#         return int(100 * len(A & B) / len(A | B)) if (A and B) else 0

# # --- 3. STATIC DATA ---
# CITY_COORDS: Dict[str, Tuple[float, float]] = {
#     "bengaluru": (12.9716, 77.5946), "bangalore": (12.9716, 77.5946), "mumbai": (19.0760, 72.8777),
#     "delhi": (28.7041, 77.1025), "chennai": (13.0827, 80.2707), "kolkata": (22.5726, 88.3639),
#     "hyderabad": (17.3850, 78.4867), "kochi": (9.9312, 76.2673), "ahmedabad": (23.0225, 72.5714),
#     "pune": (18.5204, 73.8567), "mysore": (12.2958, 76.6394), "mangalore": (12.9141, 74.8560),
#     "coimbatore": (11.0168, 76.9558), "thiruvananthapuram": (8.5241, 76.9366),
# }
# ALIAS_MAP = {"alleppey":"alappuzha","pondicherry":"puducherry","bombay":"mumbai","calcutta":"kolkata","ooty":"udhagamandalam","coorg":"kodagu"}
# NAME_ALIASES = [
#     (re.compile(r"\bchikmagalur(u)?\b", re.I), "chikkamagaluru"), (re.compile(r"\bcoorg\b", re.I), "kodagu"),
#     (re.compile(r"\bcalicut\b", re.I), "kozhikode"), (re.compile(r"\bbangalore\b", re.I), "bengaluru"),
#     (re.compile(r"\booty\b", re.I), "udhagamandalam"),
# ]
# FAMOUS_LANDMARK_STATES = {
#     "jog falls":"karnataka","mysore":"karnataka","hampi":"karnataka","coorg":"karnataka","gokarna":"karnataka",
#     "wayanad":"kerala","munnar":"kerala","ooty":"tamil nadu","akshardham":"gujarat","rishikesh":"uttarakhand"
# }
# SCOPE_TYPES = {
#     "destination":{"locality","sublocality","administrative_area_level_3","administrative_area_level_2","political"},
#     "poi":{"tourist_attraction","point_of_interest","museum","zoo","amusement_park","aquarium","hindu_temple","church","mosque","campground","lodging","restaurant","park"},
#     "natural":{"natural_feature","tourist_attraction","park","point_of_interest"},
# }
# BAD_POI_SUFFIX = re.compile(r"(view\s*point|parking|ticket|counter|canteen|shop|toilet|entrance|gate)", re.I)

# # --- 4. HELPER FUNCTIONS ---
# def strip_accents(s: str) -> str:
#     return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

# def normalize_name(s: str) -> str:
#     return re.sub(r"\s+", " ", strip_accents(s or "").strip())

# def infer_entity_kind_from_category(category_hint: str, scope: str) -> str:
#     if not category_hint: return "destination" if scope == "destination" else "tourist_attraction"
#     ch = category_hint.lower()
#     if ch in ["temple", "fort", "museum", "park", "beach", "lake"]: return ch
#     return "tourist_attraction"

# def allowed_types_for_kind(kind: str) -> set:
#     base = {"tourist_attraction", "point_of_interest", "locality", "establishment"}
#     if kind == "temple": base.update(["hindu_temple", "place_of_worship", "church", "mosque"])
#     if kind == "park": base.update(["park", "national_park"])
#     return base

# def primary_google_type_for_kind(kind: str) -> Optional[str]:
#     if kind == "temple": return "hindu_temple"
#     if kind == "park": return "park"
#     return None

# def expand_name_variants(name: str) -> List[str]:
#     variants = {normalize_name(name)}
#     for pat, rep in NAME_ALIASES:
#         if pat.search(name): variants.add(pat.sub(rep, name))
#     return list(variants)

# def build_enhanced_queries(name: str, category_hint: str, scope: str, anchor_city: str, anchor_state: str) -> List[str]:
#     qs = [name]
#     if anchor_city: qs.append(f"{name} {anchor_city}")
#     if anchor_state: qs.append(f"{name} {anchor_state}")
#     return qs

# def parse_hours(s: str) -> Optional[float]: return None
# def km_from_hours(h: Optional[float], speed: int=70) -> int: return 450
# def haversine_km(lat1, lon1, lat2, lon2): return 0.0 
# def distance_score(cand, alat, alng, rad): return 1.0 
# def popularity_score(rat, rev): return min(1.0, float(rat)*math.log10(rev+1)/5.0)
# def type_compat_score(types, allowed): return 1.0 if set(types)&allowed else 0.5
# def state_match_score(addr, state): return 1.0 if state and state.lower() in (addr or "").lower() else 0.0
# def circle_bias(lat, lng, rad): return None

# # --- 5. LLM JUDGE ---
# def llm_verify_match(scraped_name: str, scraped_context: str, 
#                      cand_name: str, cand_address: str, cand_types: List[str],
#                      api_key: str) -> float:
#     """The 'Ambiguity Judge'."""
#     if not (api_key and "sk-" in api_key and OPENAI_AVAILABLE): return 0.5
    
#     try:
#         client = OpenAI(api_key=api_key)
#         prompt = f"""
#         Task: Entity Resolution. Are these the same physical place?
        
#         SOURCE (Blog):
#         - Name: "{scraped_name}"
#         - Hint: "{scraped_context}"
        
#         CANDIDATE (Google Maps):
#         - Name: "{cand_name}"
#         - Address: "{cand_address}"
#         - Types: {cand_types}
        
#         Return JSON:
#         {{
#             "match": true,
#             "confidence": 0.95
#         }}
#         """
        
#         response = client.chat.completions.create(
#             model="gpt-4o-mini",
#             messages=[{"role": "user", "content": prompt}],
#             response_format={"type": "json_object"},
#             temperature=0.0
#         )
#         data = json.loads(response.choices[0].message.content)
        
#         if data.get("match"):
#             return max(0.85, float(data.get("confidence", 0.85)))
#         else:
#             return 0.1
            
#     except Exception as e:
#         print(f"    ⚠️ LLM Error: {e}")
#         return 0.5

# # --- 6. SCORING & TRIMMING ---
# def score_item(it: Dict[str, Any]) -> float:
#     """Calculates Quality Score (0.0 - 5.0+)"""
#     rating = float(it.get("rating") or 0.0)
#     reviews = float(it.get("reviews") or 0.0)
#     vol = math.log10(max(1.0, reviews + 1.0))
#     bonus = 0.2 if (it.get("photo_refs") and len(it.get("photo_refs")) > 0) else 0.0
#     return (0.6 * rating) + (0.3 * vol) + bonus

# def trim_and_light_shuffle(items: List[Dict[str, Any]], keep_ratio: float = 1.0) -> List[Dict[str, Any]]:
#     if not items: return []
    
#     ranked = sorted(items, key=score_item, reverse=True)
#     k = max(1, int(math.ceil(len(items) * keep_ratio)))
#     top_items = ranked[:k]
    
#     rng = random.Random(42)
#     for i in range(len(top_items)):
#         j = min(len(top_items) - 1, max(0, i + rng.randint(-2, 2)))
#         top_items[i], top_items[j] = top_items[j], top_items[i]
        
#     return top_items

# # --- 7. RESOLVER CLASS ---
# class EnhancedResolver:
#     def __init__(self, api_key: str, cache_path: Path, refresh_photos: bool):
#         self.enabled = bool(api_key and googlemaps)
#         self.cache_path = cache_path
#         self.cache = {}
#         if cache_path.exists():
#             try: self.cache = json.loads(cache_path.read_text(encoding="utf-8"))
#             except: pass
#         self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None
#         self.api_key = api_key
#         self.refresh_photos = refresh_photos

#     def save(self):
#         try: self.cache_path.write_text(json.dumps(self.cache, indent=2), encoding="utf-8")
#         except: pass

#     def _cache_key(self, name, cat, scope, city, state):
#         return f"{normalize_name(name)}|{cat}|{scope}|{city}"

#     def _details_with_photos(self, place_id: str) -> Dict[str, Any]:
#         if not self.gmaps: return {}
#         try:
#             res = self.gmaps.place(place_id=place_id, fields=["name","geometry/location","formatted_address","rating","user_ratings_total","photos","types","website","international_phone_number","opening_hours","price_level","permanently_closed","utc_offset_minutes"])
#             data = res.get("result", {})
#         except: return {}

#         if not data.get("photos"):
#             try:
#                 url = "https://maps.googleapis.com/maps/api/place/details/json"
#                 r = requests.get(url, params={"place_id": place_id, "fields": "photos", "key": self.api_key})
#                 if r.status_code == 200:
#                     data["photos"] = r.json().get("result", {}).get("photos", [])
#             except: pass
#         return data

#     def resolve(self, name: str, category_hint: str, scope: str, anchor_city: str, anchor_state: str, radius_m: int) -> Dict[str, Any]:
#         empty = {"place_id": None, "name": name, "confidence": 0.0, "photo_refs": []}
#         ck = self._cache_key(name, category_hint, scope, anchor_city, anchor_state)
        
#         if ck in self.cache:
#             cached = {**empty, **self.cache[ck]}
#             if cached.get("place_id") and (self.refresh_photos or not cached.get("photo_refs")):
#                  det = self._details_with_photos(cached["place_id"])
#                  if det.get("photos"):
#                      cached["photo_refs"] = [p["photo_reference"] for p in det["photos"]]
#                      self.cache[ck] = cached
#             return cached

#         if not self.enabled: return empty

#         candidates = []
#         try:
#             q = f"{name} {anchor_city or ''} {anchor_state or ''}"
#             res = self.gmaps.places(query=q, language=GMAPS_LANGUAGE)
#             candidates = res.get("results", [])[:5]
#         except: pass

#         best = empty
#         best_score = -1.0

#         for cand in candidates:
#             # A. Math Score
#             c_name = cand.get("name")
#             sim = name_sim(name, c_name) / 100.0
#             type_bonus = 0.1 if type_compat_score(cand.get("types", []), allowed_types_for_kind(infer_entity_kind_from_category(category_hint, scope))) > 0.5 else -0.1
#             state_bonus = 0.1 if anchor_state and anchor_state.lower() in cand.get("formatted_address", "").lower() else 0.0
            
#             math_score = max(0.0, min(1.0, sim * 0.6 + type_bonus + state_bonus + 0.2))

#             # B. Ambiguity Judge
#             final_score = math_score
#             if GREY_ZONE_MIN <= math_score < GREY_ZONE_MAX and OPENAI_AVAILABLE:
#                 print(f"    🤖 Grey Zone ({math_score:.2f}): Asking LLM '{name}' vs '{c_name}'...")
#                 llm_conf = llm_verify_match(
#                     scraped_name=name, scraped_context=f"{category_hint} in {anchor_city}",
#                     cand_name=c_name, cand_address=cand.get("formatted_address", ""),
#                     cand_types=cand.get("types", []), api_key=OPENAI_API_KEY_HARDCODED
#                 )
                
#                 if llm_conf > 0.8:
#                     final_score = llm_conf
#                     print(f"      ✅ LLM Confirmed! ({math_score:.2f} -> {final_score:.2f})")
#                 else:
#                     final_score = 0.1
#                     print(f"      ❌ LLM Rejected. ({math_score:.2f} -> {final_score:.2f})")

#             if final_score > best_score:
#                 best_score = final_score
#                 best = {
#                     "place_id": cand.get("place_id"),
#                     "name": c_name,
#                     "address": cand.get("formatted_address"),
#                     "rating": cand.get("rating", 0),
#                     "reviews": cand.get("user_ratings_total", 0),
#                     "types": cand.get("types", []),
#                     "photo_refs": [p["photo_reference"] for p in cand.get("photos", [])],
#                     "confidence": round(final_score, 3)
#                 }
#             time.sleep(SLEEP_BETWEEN)

#         if best.get("place_id") and best["confidence"] >= 0.5:
#             det = self._details_with_photos(best["place_id"])
#             if det:
#                 loc = (det.get("geometry") or {}).get("location") or {}
#                 best.update({
#                     "lat": loc.get("lat"), "lng": loc.get("lng"),
#                     "website": det.get("website"), "phone": det.get("international_phone_number"),
#                     "opening": (det.get("opening_hours", {}) or {}).get("periods", []),
#                     "price_level": det.get("price_level"),
#                     "permanently_closed": det.get("permanently_closed", False),
#                     "utc_offset_minutes": det.get("utc_offset_minutes"),
#                 })
#                 if det.get("photos"):
#                     best["photo_refs"] = [p["photo_reference"] for p in det["photos"]]

#         self.cache[ck] = best
#         return best

# # --- 8. MAIN EXECUTION ---
# def is_publishable(resolved: Dict[str, Any]) -> bool:
#     if resolved.get("confidence", 0) < MIN_CONFIDENCE: return False
#     if not resolved.get("place_id"): return False
    
#     rating = float(resolved.get("rating", 0))
#     reviews = int(resolved.get("reviews", 0))
    
#     # If it has photos and decent confidence, keep unless it's truly trash (1 star)
#     if rating < 1.0 and reviews > 0: return False 
    
#     if REQUIRE_PHOTO and not resolved.get("photo_refs"): return False
    
#     return True

# def main():
#     p = argparse.ArgumentParser()
#     p.add_argument("--in", dest="in_path", default=str(IN_PATH))
#     p.add_argument("--out", dest="out_path", default=str(OUT_PATH))
#     p.add_argument("--report", dest="report_path", default=str(REPORT))
#     p.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE)
#     p.add_argument("--keep-ratio", type=float, default=1.0)
#     p.add_argument("--anchor-city", type=str)
#     p.add_argument("--anchor-state", type=str)
#     # Dummy args
#     p.add_argument("--radius-km", type=int); p.add_argument("--default-speed", type=int)
#     p.add_argument("--retry", type=int); p.add_argument("--require-photo", action="store_true")
#     p.add_argument("--refresh-photos", action="store_true"); p.add_argument("--scope", default="auto")
#     args = p.parse_args()

#     # --- CRITICAL: UPDATE GLOBALS FROM CLI ARGS ---
#     global MIN_CONFIDENCE, REQUIRE_PHOTO
#     MIN_CONFIDENCE = args.min_confidence
#     REQUIRE_PHOTO = args.require_photo
#     # ----------------------------------------------

#     if not Path(args.in_path).exists(): return

#     resolver = EnhancedResolver(GOOGLE_API_KEY_HARDCODED, PLACES_CACHE, args.refresh_photos)

#     data = json.loads(Path(args.in_path).read_text(encoding="utf-8"))
#     out_playlists = []
#     totals = {"resolved":0, "publishable":0}

#     for plist in data:
#         title = plist.get("playlistTitle")
#         anchor_city = args.anchor_city or plist.get("placeName")
#         anchor_state = args.anchor_state
        
#         print(f"\nPlaylist: {title} (City: {anchor_city})")
#         resolved_items = []
        
#         for item in plist.get("items", []):
#             name = item.get("name")
#             cat = item.get("category_hint")
            
#             res = resolver.resolve(
#                 name=name, category_hint=cat, scope="poi", 
#                 anchor_city=anchor_city, anchor_state=anchor_state, radius_m=50000
#             )
            
#             final_item = item.copy()
#             final_item.update(res)
            
#             if is_publishable(res):
#                 final_item["resolution_status"] = "publishable"
#                 totals["publishable"] += 1
#                 print(f"  ✅ {name} -> {res['name']} (Conf: {res['confidence']}, Rat: {res.get('rating')})")
#             else:
#                 final_item["resolution_status"] = "unresolved"
#                 print(f"  ❌ {name} -> (Conf: {res['confidence']})")
            
#             resolved_items.append(final_item)

#         if args.keep_ratio < 1.0:
#             valid_items = [it for it in resolved_items if it["resolution_status"] == "publishable"]
#             print(f"  ✂️ Trimming {len(valid_items)} valid items to top {int(args.keep_ratio*100)}%...")
            
#             kept_valid = trim_and_light_shuffle(valid_items, args.keep_ratio)
#             kept_ids = {it["place_id"] for it in kept_valid}
            
#             for it in resolved_items:
#                 if it["resolution_status"] == "publishable" and it["place_id"] not in kept_ids:
#                     it["resolution_status"] = "filtered_by_ratio"
            
#             print(f"     Kept {len(kept_valid)} items")

#         out_playlists.append({**plist, "items": resolved_items})

#     Path(args.out_path).write_text(json.dumps(out_playlists, indent=2), encoding="utf-8")
#     resolver.save()
#     print(f"\nDone. Wrote {args.out_path}")

# if __name__ == "__main__":
#     main()

# # 02_5_resolve_validate.py
# # Step 2.5 — Resolve & Validate (Google Places) — with keep-ratio filtering and hardcoded keys

# from __future__ import annotations
# import os, json, re, time, math, argparse, unicodedata, random
# from pathlib import Path
# from typing import List, Dict, Any, Optional, Tuple

# # .env (optional - kept for compatibility but key is hardcoded below)
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except Exception:
#     pass

# # Deps
# try:
#     import googlemaps   # pip install googlemaps
# except Exception:
#     googlemaps = None

# try:
#     import requests     # pip install requests
# except Exception:
#     requests = None

# try:
#     from rapidfuzz import fuzz
#     def name_sim(a: str, b: str) -> int:
#         return fuzz.token_set_ratio(a, b)
# except Exception:
#     def _tok(s: str) -> set:
#         return set(re.findall(r"[a-z0-9]+", s.lower()))
#     def name_sim(a: str, b: str) -> int:
#         A, B = _tok(a), _tok(b)
#         return int(100 * len(A & B) / len(A | B)) if (A and B) else 0

# # ------------------------------- CONFIG --------------------------------
# BASE_DIR = Path(__file__).resolve().parent

# IN_PATH   = BASE_DIR / "playlist_items.json"
# OUT_PATH  = BASE_DIR / "playlist_items_resolved.json"
# REPORT    = BASE_DIR / "resolve_report.json"

# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# PLACES_CACHE   = CACHE_DIR / "places_cache.json"
# GEOCODE_CACHE  = CACHE_DIR / "geocode_cache.json"

# # Acceptance / scoring
# MIN_CONFIDENCE = 0.75
# RETRY_ATTEMPTS = 2
# MIN_RATING     = 3.5
# MIN_REVIEWS    = 100
# REQUIRE_PHOTO  = False

# # API tuning
# GMAPS_REGION    = "in"
# GMAPS_LANGUAGE  = "en-IN"
# SLEEP_BETWEEN   = 0.08

# # Hardcoded anchor overrides (optional)
# HARDCODE_ANCHOR_CITY  = None
# HARDCODE_ANCHOR_STATE = None

# # Known coords (fast fallback)
# CITY_COORDS: Dict[str, Tuple[float, float]] = {
#     "bengaluru": (12.9716, 77.5946),
#     "bangalore": (12.9716, 77.5946),
#     "mumbai": (19.0760, 72.8777),
#     "delhi": (28.7041, 77.1025),
#     "chennai": (13.0827, 80.2707),
#     "kolkata": (22.5726, 88.3639),
#     "hyderabad": (17.3850, 78.4867),
#     "kochi": (9.9312, 76.2673),
#     "pune": (18.5204, 73.8567),
#     "mysore": (12.2958, 76.6394),
#     "mangalore": (12.9141, 74.8560),
#     "coimbatore": (11.0168, 76.9558),
#     "thiruvananthapuram": (8.5241, 76.9366),
#     "kottayam": (9.5916, 76.5222),
# }

# # Aliases / hints
# ALIAS_MAP = {
#     "alleppey":"alappuzha","pondicherry":"puducherry","bombay":"mumbai",
#     "calcutta":"kolkata","rishikonda":"rushikonda","havelock":"swaraj dweep",
#     "ooty":"udhagamandalam","coonoor":"coonoor","coorg":"kodagu",
# }
# NAME_ALIASES = [
#     (re.compile(r"\bchikmagalur(u)?\b", re.I), "chikkamagaluru"),
#     (re.compile(r"\bcoorg\b", re.I), "kodagu"),
#     (re.compile(r"\bcalicut\b", re.I), "kozhikode"),
#     (re.compile(r"\bbangalore\b", re.I), "bengaluru"),
#     (re.compile(r"\booty\b", re.I), "udhagamandalam"),
# ]
# FAMOUS_LANDMARK_STATES = {
#     "jog falls":"karnataka","mysore":"karnataka","mysore palace":"karnataka","hampi":"karnataka",
#     "coorg":"karnataka","kodagu":"karnataka","chikmagalur":"karnataka","chikkamagaluru":"karnataka",
#     "gokarna":"karnataka","dandeli":"karnataka","badami":"karnataka","kudremukh":"karnataka",
#     "bandipur":"karnataka","nagarhole":"karnataka","bhadra":"karnataka",
#     "lepakshi":"andhra pradesh","horsley hills":"andhra pradesh","tirupati":"andhra pradesh",
#     "wayanad":"kerala","kozhikode":"kerala","alappuzha":"kerala","munnar":"kerala","thekkady":"kerala",
#     "kovalam":"kerala","varkala":"kerala","kochi":"kerala",
#     "ooty":"tamil nadu","kodaikanal":"tamil nadu","coonoor":"tamil nadu","yercaud":"tamil nadu","mahabalipuram":"tamil nadu",
# }
# SCOPE_TYPES = {
#     "destination":{"locality","sublocality","administrative_area_level_3","administrative_area_level_2","administrative_area_level_1","political"},
#     "poi":{"tourist_attraction","point_of_interest","museum","zoo","amusement_park","aquarium","art_gallery","hindu_temple","church","mosque","synagogue","stadium","campground","lodging","restaurant","bar","cafe","night_club"},
#     "natural":{"natural_feature","tourist_attraction","park","point_of_interest"},
# }
# BAD_POI_SUFFIX = re.compile(r"(view\s*point|parking|ticket|counter|canteen|shop|toilet|entrance|gate)", re.I)

# # ------------------------------ Helpers --------------------------------
# def strip_accents(s: str) -> str:
#     return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
# def normalize_name(s: str) -> str:
#     s = s or ""
#     s = strip_accents(s).strip()
#     return re.sub(r"\s+", " ", s)

# def infer_entity_kind_from_category(category_hint: str, scope: str) -> str:
#     if not category_hint:
#         return "destination" if scope == "destination" else "tourist_attraction"
#     m = {
#         "waterfall":"waterfall","beach":"beach","island":"island","lake":"lake","peak":"peak","mountain":"peak",
#         "national_park":"national_park","sanctuary":"sanctuary","reserve":"reserve","park":"park","cave":"cave",
#         "trek":"trek","trail":"trail","temple":"temple","fort":"fort",
#         "resort":"resort","hotel":"hotel","camp":"campground","homestay":"homestay","villa":"villa","lodge":"lodge","hostel":"hostel",
#         "hill_station":"hill_station","town":"destination","city":"destination","district":"destination","region":"destination",
#     }
#     return m.get(category_hint.lower(), "destination")

# def allowed_types_for_kind(kind: str) -> set:
#     m = {
#         "waterfall":{"natural_feature","tourist_attraction","point_of_interest"},
#         "beach":{"natural_feature","tourist_attraction","point_of_interest"},
#         "island":{"natural_feature","tourist_attraction","point_of_interest"},
#         "lake":{"natural_feature","tourist_attraction","point_of_interest"},
#         "peak":{"natural_feature","tourist_attraction","point_of_interest"},
#         "national_park":{"park","tourist_attraction","point_of_interest"},
#         "sanctuary":{"park","tourist_attraction","point_of_interest"},
#         "reserve":{"park","tourist_attraction","point_of_interest"},
#         "park":{"park","tourist_attraction","point_of_interest"},
#         "trek":{"natural_feature","tourist_attraction","point_of_interest"},
#         "trail":{"natural_feature","tourist_attraction","point_of_interest"},
#         "cave":{"natural_feature","tourist_attraction","point_of_interest"},
#         "temple":{"hindu_temple","tourist_attraction","point_of_interest","place_of_worship"},
#         "fort":{"tourist_attraction","point_of_interest","museum"},
#         "resort":{"lodging","tourist_attraction"},
#         "hotel":{"lodging"},
#         "homestay":{"lodging"},
#         "villa":{"lodging"},
#         "lodge":{"lodging"},
#         "hostel":{"lodging"},
#         "campground":{"campground","lodging"},
#         "destination":{"locality","sublocality","administrative_area_level_3","administrative_area_level_2","political"},
#         "hill_station":{"locality","tourist_attraction","point_of_interest"},
#     }
#     return m.get(kind.lower(), {"tourist_attraction","point_of_interest","locality"})

# def primary_google_type_for_kind(kind: str) -> Optional[str]:
#     k = kind.lower()
#     if k in {"resort","hotel","villa","homestay","lodge","hostel","campground"}: return "lodging"
#     if k in {"park","national_park","sanctuary","reserve"}:                    return "park"
#     if k in {"temple"}:                                                        return "hindu_temple"
#     return "tourist_attraction"

# def expand_name_variants(name: str) -> List[str]:
#     variants = {normalize_name(name)}
#     for pat, rep in NAME_ALIASES:
#         if pat.search(name):
#             variants.add(pat.sub(rep, name))
#     return list(variants)

# def build_enhanced_queries(name: str, category_hint: str, scope: str,
#                            anchor_city: Optional[str], anchor_state: Optional[str]) -> List[str]:
#     name_qs = expand_name_variants(name)
#     queries: List[str] = []
#     low = name.lower()
#     for landmark, state in FAMOUS_LANDMARK_STATES.items():
#         if landmark in low:
#             for nq in name_qs:
#                 queries.append(f"{nq} {state} india")
#             break
#     cat_word = category_hint.replace("_"," ") if category_hint else ""
#     for nq in name_qs:
#         queries.append(nq)
#         if anchor_state:
#             queries.append(f"{nq} {anchor_state}")
#             queries.append(f"{nq} {anchor_state} india")
#         if anchor_city:
#             queries.append(f"{nq} {anchor_city}")
#             queries.append(f"{nq} near {anchor_city}")
#         if cat_word and cat_word not in nq.lower():
#             queries.append(f"{nq} {cat_word}")
#             if anchor_state: queries.append(f"{nq} {cat_word} {anchor_state}")
#             if anchor_city:  queries.append(f"{nq} {cat_word} {anchor_city}")
#         if scope == "destination":
#             queries.extend([f"{nq} town", f"{nq} city", f"{nq} place"])
#         elif scope == "natural" and "waterfall" in (category_hint or "").lower():
#             queries.append(f"{nq} waterfall")
#     seen = set(); out=[]
#     for q in queries:
#         t = q.strip().lower()
#         if t and t not in seen and len(t)>2:
#             seen.add(t); out.append(q.strip())
#     return out[:15]

# def parse_hours(s: str) -> Optional[float]:
#     if not s: return None
#     m = re.search(r"(\d+(?:\.\d+)?)\s*hour", s, re.I)
#     return float(m.group(1)) if m else None

# def km_from_hours(h: Optional[float], default_speed_kmph: int = 70, buffer_km: int = 50) -> int:
#     if h is None: return 450
#     return int(max(80, min(800, h*default_speed_kmph + buffer_km)))

# def haversine_km(a_lat: float, a_lng: float, b_lat: float, b_lng: float) -> float:
#     R=6371.0
#     dlat=math.radians(b_lat-a_lat); dlon=math.radians(b_lng-a_lng)
#     x = math.sin(dlat/2)**2 + math.cos(math.radians(a_lat))*math.cos(math.radians(b_lat))*math.sin(dlon/2)**2
#     return 2*R*math.asin(min(1, math.sqrt(x)))

# def distance_score(cand: Dict[str,Any], anchor_lat: Optional[float], anchor_lng: Optional[float], radius_m: int) -> float:
#     if not (anchor_lat is not None and anchor_lng is not None): return 0.0
#     if cand.get("lat") is None or cand.get("lng") is None: return 0.0
#     dkm=haversine_km(anchor_lat, anchor_lng, cand["lat"], cand["lng"]); rkm=max(1, radius_m/1000)
#     if dkm <= 0.4*rkm: return 1.0
#     if dkm >= 2.0*rkm: return 0.0
#     return max(0.0, 1.0 - ((dkm-0.4*rkm)/(1.6*rkm)))

# def popularity_score(rating: float, reviews: int) -> float:
#     rating = rating or 0.0; reviews=max(0, int(reviews or 0))
#     return min(1.0, rating * math.log10(reviews + 1) / 5.0)

# def type_compat_score(types: List[str], allowed: set) -> float:
#     return 1.0 if set(types or []) & allowed else (0.5 if types else 0.0)

# def state_match_score(address: str, expected_state: Optional[str]) -> float:
#     if not address or not expected_state: return 0.0
#     return 1.0 if expected_state.lower() in address.lower() else 0.0

# def circle_bias(lat: float, lng: float, radius_m: int) -> str:
#     return f"circle:{max(1000, int(radius_m))}@{lat},{lng}"

# # ------------------- SCORING & TRIMMING LOGIC (NEW) -------------------
# def score_item(it: Dict[str, Any]) -> float:
#     """
#     Calculate a quality score for the item based on Google Maps data.
#     Formula: 60% Rating + 30% Popularity (Log reviews) + 10% Completeness
#     """
#     # 1. Rating (0-5)
#     rating = float(it.get("rating") or 0.0)
    
#     # 2. Popularity (Log scale)
#     # 10 reviews -> 1.0, 100 -> 2.0, 1000 -> 3.0
#     reviews = float(it.get("reviews") or 0.0)
#     vol = math.log10(max(1.0, reviews + 1.0))
    
#     # 3. Completeness Bonus
#     # Bonus if it has photos or a description
#     bonus = 0.0
#     if it.get("photo_refs") and len(it.get("photo_refs")) > 0:
#         bonus += 0.1
#     if it.get("description"):
#         bonus += 0.1
        
#     # Final Score Calculation
#     # Rating is weighted heavily (0.6). Volume weighted (0.3).
#     final_score = (0.6 * rating) + (0.3 * vol) + bonus
#     return final_score

# def trim_and_light_shuffle(items: List[Dict[str, Any]], keep_ratio: float = 1.0,
#                            seed: int = 42, max_displacement: int = 2) -> List[Dict[str, Any]]:
#     """
#     Keep top N% items based on score, then slightly shuffle them 
#     so the list isn't strictly descending by rating (looks more natural).
#     """
#     rng = random.Random(seed)
#     n = len(items)
#     if n == 0:
#         return []
    
#     # Calculate how many to keep
#     k = max(1, int(math.ceil(n * keep_ratio)))
    
#     # Sort by score descending
#     ranked = sorted(items, key=score_item, reverse=True)[:k]
    
#     # Light shuffle (swap items with neighbors)
#     for i in range(len(ranked)):
#         # Pick a random neighbor within range
#         j = min(len(ranked) - 1, max(0, i + rng.randint(-max_displacement, max_displacement)))
#         if i != j:
#             ranked[i], ranked[j] = ranked[j], ranked[i]
            
#     return ranked
# # ----------------------------------------------------------------------

# # ----------------------------- Resolver --------------------------------
# class EnhancedResolver:
#     def __init__(self, api_key: Optional[str], cache_path: Path, refresh_photos: bool):
#         self.enabled = bool(api_key and googlemaps)
#         self.cache_path = cache_path
#         self.cache: Dict[str, Any] = {}
#         if cache_path.exists():
#             try: self.cache = json.loads(cache_path.read_text(encoding="utf-8"))
#             except Exception: self.cache = {}
#         self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None
#         self.api_key = api_key
#         self.refresh_photos = refresh_photos

#     def save(self):
#         try:
#             self.cache_path.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass

#     def _cache_key(self, name: str, category_hint: str, scope: str, anchor_city: Optional[str], anchor_state: Optional[str]) -> str:
#         def n(s): return normalize_name(s or "").lower()
#         return "|".join([n(name),(category_hint or "").lower(),scope.lower(),n(anchor_city),n(anchor_state)])

#     # --- REST photo fallback (works even if SDK omits photos) ---
#     def _rest_photo_refs(self, place_id: str) -> List[str]:
#         if not (requests and self.api_key): return []
#         url = "https://maps.googleapis.com/maps/api/place/details/json"
#         params = {"place_id": place_id, "fields": "photos", "key": self.api_key}
#         try:
#             r = requests.get(url, params=params, timeout=20)
#             r.raise_for_status()
#             data = r.json()
#             if data.get("status") != "OK": return []
#             photos = (data.get("result") or {}).get("photos") or []
#             return [p.get("photo_reference") for p in photos if p.get("photo_reference")]
#         except Exception:
#             return []

#     def _details_with_photos(self, place_id: str) -> Dict[str, Any]:
#         out = {}
#         if not self.gmaps:
#             return out
#         try:
#             details = self.gmaps.place(
#                 place_id=place_id,
#                 fields=[
#                     "place_id","name","geometry/location","types","formatted_address",
#                     "website","international_phone_number","opening_hours","price_level",
#                     "permanently_closed","rating","user_ratings_total","photos","utc_offset_minutes"
#                 ],
#                 language=GMAPS_LANGUAGE
#             ).get("result", {}) or {}
#         except Exception:
#             details = {}

#         # If SDK omitted photos, use REST fallback
#         photos = details.get("photos") or []
#         if not photos:
#             refs = self._rest_photo_refs(place_id)
#             if refs:
#                 details["photos"] = [{"photo_reference": r} for r in refs]
#         return details

#     def geocode_anchor(self, text: str) -> Tuple[Optional[float], Optional[float]]:
#         key = (text or "").strip().lower()
#         if key in CITY_COORDS: return CITY_COORDS[key]

#         # cached?
#         lat=lng=None
#         if GEOCODE_CACHE.exists():
#             try:
#                 geo_cache = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
#                 if key in geo_cache:
#                     d=geo_cache[key]; lat, lng = d.get("lat"), d.get("lng")
#                     return lat, lng
#             except Exception:
#                 pass

#         if not self.gmaps: return None, None
#         try:
#             res = self.gmaps.geocode(f"{text}, India", language=GMAPS_LANGUAGE)
#             if res:
#                 loc = res[0]["geometry"]["location"]
#                 lat, lng = loc["lat"], loc["lng"]
#         except Exception:
#             pass

#         # persist
#         try:
#             geo_cache = {}
#             if GEOCODE_CACHE.exists():
#                 geo_cache = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
#             geo_cache[key] = {"lat": lat, "lng": lng}
#             GEOCODE_CACHE.write_text(json.dumps(geo_cache, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass
#         return lat, lng

#     def resolve(self, *, name: str, category_hint: str, scope: str,
#                 anchor_city: Optional[str], anchor_state: Optional[str], radius_m: int) -> Dict[str, Any]:

#         empty = {
#             "place_id": None, "name": name, "lat": None, "lng": None, "types": [],
#             "rating": 0.0, "reviews": 0, "address": None, "website": None, "phone": None,
#             "opening": [], "price_level": None, "permanently_closed": False,
#             "photo_refs": [], "utc_offset_minutes": None, "confidence": 0.0
#         }

#         cache_key = self._cache_key(name, category_hint, scope, anchor_city, anchor_state)
#         if cache_key in self.cache:
#             cached = {**empty, **self.cache[cache_key]}
#             # If we want fresh photos or cache has none, try to backfill now
#             if cached.get("place_id") and (self.refresh_photos or not cached.get("photo_refs")):
#                 det = self._details_with_photos(cached["place_id"])
#                 if det:
#                     photos = det.get("photos") or []
#                     photo_refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]
#                     if photo_refs:
#                         cached["photo_refs"] = photo_refs
#                         self.cache[cache_key] = cached
#             return cached

#         if not self.enabled:
#             self.cache[cache_key] = empty
#             return empty

#         # Anchor coords
#         a_lat = a_lng = None
#         if anchor_city:
#             a_lat, a_lng = self.geocode_anchor(anchor_city)
#         if (a_lat is None or a_lng is None) and anchor_state:
#             a_lat, a_lng = self.geocode_anchor(anchor_state)

#         # Entity kind / types
#         entity_kind = infer_entity_kind_from_category(category_hint, scope)
#         allowed_kind_types = allowed_types_for_kind(entity_kind)
#         allowed_scope_types = SCOPE_TYPES.get(scope, set())
#         primary_type = primary_google_type_for_kind(entity_kind)

#         # Queries
#         queries = build_enhanced_queries(name, category_hint, scope, anchor_city, anchor_state)

#         best = empty; best_score = -1.0
#         bias = circle_bias(a_lat, a_lng, radius_m) if (a_lat and a_lng) else None

#         print(f"Resolving: {name}  (category={category_hint}, scope={scope})")

#         # Pre-seed for destination
#         all_candidates: List[Dict[str, Any]] = []
#         if scope == "destination" and self.gmaps:
#             try:
#                 components = {"country":"IN"}
#                 if anchor_state: components["administrative_area"]=anchor_state
#                 geo_query = f"{name}, {anchor_state or 'India'}"
#                 geo_results = self.gmaps.geocode(geo_query, components=components, language=GMAPS_LANGUAGE)
#                 for g in (geo_results or [])[:2]:
#                     loc = g.get("geometry", {}).get("location", {})
#                     all_candidates.append({
#                         "place_id": g.get("place_id"),
#                         "name": g.get("formatted_address", name).split(",")[0],
#                         "geometry": {"location": loc},
#                         "types": g.get("types", []),
#                         "formatted_address": g.get("formatted_address", ""),
#                         "rating": 0, "user_ratings_total": 0, "_source":"geocode"
#                     })
#             except Exception:
#                 pass

#         # Search
#         for q in queries[:8]:
#             # Find Place
#             try:
#                 kwargs = {
#                     "input": q, "input_type":"textquery",
#                     "fields":["place_id","name","geometry/location","types","formatted_address","rating","user_ratings_total"],
#                     "language": GMAPS_LANGUAGE
#                 }
#                 if bias: kwargs["location_bias"] = bias
#                 fp = self.gmaps.find_place(**kwargs)
#                 for c in fp.get("candidates", []):
#                     c["_source"] = "find_place"
#                     all_candidates.append(c)
#             except Exception:
#                 pass

#             # Text Search
#             try:
#                 ts_kwargs = {"query": q, "region": GMAPS_REGION, "language": GMAPS_LANGUAGE}
#                 if a_lat and a_lng:
#                     ts_kwargs.update({"location":(a_lat,a_lng), "radius":min(500000, max(20000, radius_m))})
#                 if primary_type:
#                     ts_kwargs["type"] = primary_type
#                 ts = self.gmaps.places(**ts_kwargs)
#                 for c in ts.get("results", []):
#                     c["_source"] = "text_search"
#                     all_candidates.append(c)
#             except Exception:
#                 pass

#             time.sleep(SLEEP_BETWEEN)

#         # Score
#         for cand in all_candidates[:15]:
#             loc = (cand.get("geometry") or {}).get("location") or {}
#             data = {
#                 "place_id": cand.get("place_id"),
#                 "name": cand.get("name", name),
#                 "lat": loc.get("lat"),
#                 "lng": loc.get("lng"),
#                 "types": cand.get("types", []),
#                 "address": cand.get("formatted_address", ""),
#                 "rating": float(cand.get("rating", 0)),
#                 "reviews": int(cand.get("user_ratings_total", 0)),
#             }
#             sim = name_sim(name, data["name"]) / 100.0
#             kind_compat = type_compat_score(data["types"], allowed_kind_types)
#             scope_compat = 1.0 if (set(data["types"]) & allowed_scope_types) else 0.3
#             dist = distance_score(data, a_lat, a_lng, radius_m)
#             pop  = popularity_score(data["rating"], data["reviews"])
#             st   = state_match_score(data["address"], anchor_state)

#             scope_adj = 0.0
#             if scope == "destination":
#                 if set(data["types"]) & {"tourist_attraction","lodging","restaurant","store"}: scope_adj -= 0.2
#                 if cand.get("_source") == "geocode": scope_adj += 0.1
#             elif scope == "natural":
#                 if BAD_POI_SUFFIX.search(data["name"]): scope_adj -= 0.3
#                 if set(data["types"]) & {"locality","administrative_area_level_2"}: scope_adj -= 0.2
#             elif scope == "poi":
#                 if set(data["types"]) & {"locality","administrative_area_level_1","administrative_area_level_2"}: scope_adj -= 0.3

#             lm_bonus = 0.0
#             lm_state = None
#             for landmark, s in FAMOUS_LANDMARK_STATES.items():
#                 if landmark in name.lower(): lm_state = s; break
#             if lm_state:
#                 if lm_state in (data["address"] or "").lower(): lm_bonus = 0.15
#                 else:
#                     for wrong in ["kerala","tamil nadu","andhra pradesh","telangana","odisha"]:
#                         if wrong in (data["address"] or "").lower() and wrong != lm_state:
#                             lm_bonus = -0.2; break

#             score = max(0.0, min(1.0,
#                 0.30*sim + 0.20*kind_compat + 0.15*scope_compat + 0.15*dist + 0.10*pop + 0.10*st + scope_adj + lm_bonus
#             ))
#             if score > best_score:
#                 best_score = score
#                 best = {**empty, **data, "confidence": round(score, 3)}

#         print(f"  Best: {best.get('name')} (confidence: {best_score:.3f})")

#         # Details + PHOTO BACKFILL
#         if best.get("place_id") and best_score >= 0.5:
#             det = self._details_with_photos(best["place_id"])
#             if det:
#                 loc = (det.get("geometry") or {}).get("location") or {}
#                 photos = det.get("photos") or []
#                 photo_refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]
#                 best.update({
#                     "name": det.get("name", best["name"]),
#                     "lat": loc.get("lat", best["lat"]),
#                     "lng": loc.get("lng", best["lng"]),
#                     "types": det.get("types", best["types"]),
#                     "address": det.get("formatted_address", best["address"]),
#                     "website": det.get("website"),
#                     "phone": det.get("international_phone_number"),
#                     "opening": (det.get("opening_hours", {}) or {}).get("periods", []),
#                     "price_level": det.get("price_level"),
#                     "permanently_closed": det.get("permanently_closed", False),
#                     "rating": float(det.get("rating", best["rating"])),
#                     "reviews": int(det.get("user_ratings_total", best["reviews"])),
#                     "photo_refs": photo_refs,
#                     "utc_offset_minutes": det.get("utc_offset_minutes"),
#                 })
#             time.sleep(SLEEP_BETWEEN)

#         # Cache AFTER enrichment so we don't lock empty photo_refs
#         self.cache[cache_key] = best
#         return best

# # ------------------------------- Main ---------------------------------
# def is_publishable(resolved: Dict[str, Any], entity_kind: str) -> bool:
#     pid = resolved.get("place_id")
#     lat, lng = resolved.get("lat"), resolved.get("lng")
#     confidence = resolved.get("confidence", 0.0)
    
#     # NEW: Reject if confidence too low
#     if confidence < 0.80:
#         return False
    
#     if not (pid and lat is not None and lng is not None): 
#         return False
#     if REQUIRE_PHOTO and not resolved.get("photo_refs"):  
#         return False
    
#     rating = float(resolved.get("rating", 0))
#     reviews = int(resolved.get("reviews", 0))
#     has_photo = bool(resolved.get("photo_refs"))
    
#     if not ((rating >= MIN_RATING) or (reviews >= MIN_REVIEWS) or has_photo): 
#         return False
    
#     allowed = allowed_types_for_kind(entity_kind)
#     if not (set(resolved.get("types", [])) & allowed):
#         if rating >= MIN_RATING and reviews >= MIN_REVIEWS: 
#             return True
#         return False
    
#     return True

# def main():
#     global MIN_CONFIDENCE, RETRY_ATTEMPTS, REQUIRE_PHOTO

#     p = argparse.ArgumentParser("Step 2.5 — Resolve & Validate (with photo backfill)")
#     p.add_argument("--in", dest="in_path", default=str(IN_PATH))
#     p.add_argument("--out", dest="out_path", default=str(OUT_PATH))
#     p.add_argument("--report", dest="report_path", default=str(REPORT))
#     p.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE)
#     p.add_argument("--retry", type=int, default=RETRY_ATTEMPTS)
#     p.add_argument("--require-photo", action="store_true", default=REQUIRE_PHOTO)
#     p.add_argument("--refresh-photos", action="store_true", help="Re-fetch photos for cached items or empty photo_refs")
#     p.add_argument("--scope", choices=["auto","destination","poi","natural"], default="auto")
#     p.add_argument("--anchor-city", type=str, default=HARDCODE_ANCHOR_CITY)
#     p.add_argument("--anchor-state", type=str, default=HARDCODE_ANCHOR_STATE)
#     p.add_argument("--radius-km", type=int, default=None)
#     p.add_argument("--default-speed", type=int, default=70)
#     # --- NEW PARAMETER ---
#     p.add_argument("--keep-ratio", type=float, default=1.0, help="Fraction of items to keep (0.0 - 1.0). Default 1.0 (keep all).")
#     # ---------------------
    
#     args = p.parse_args()

#     MIN_CONFIDENCE = args.min_confidence
#     RETRY_ATTEMPTS = args.retry
#     REQUIRE_PHOTO  = args.require_photo

#     in_path  = Path(args.in_path)
#     out_path = Path(args.out_path)
#     rpt_path = Path(args.report_path)

#     if not in_path.exists():
#         print(f"Error: input not found: {in_path}"); return
    
#     # 🔴 HARDCODED API KEY
#     api_key = "AIzaSyDEuy2i8AbSR-jnstZG22ndFqy71jtKbgg"
#     if not api_key:
#         print("Error: GOOGLE_MAPS_API_KEY not set"); return
#     if not googlemaps:
#         print("Error: googlemaps not installed. pip install googlemaps"); return

#     resolver = EnhancedResolver(api_key, PLACES_CACHE, refresh_photos=args.refresh_photos)

#     try:
#         data = json.loads(in_path.read_text(encoding="utf-8"))
#     except Exception as e:
#         print(f"Error reading input: {e}"); return
#     if not isinstance(data, list):
#         print("Error: Input must be a JSON array of playlists"); return

#     totals = {"items":0,"resolved":0,"publishable":0,"partial":0,"unresolved":0}
#     per_pl = []
#     print(f"Processing {len(data)} playlists…")

#     out_playlists = []
#     for idx_pl, plist in enumerate(data):
#         title = plist.get("playlistTitle", f"Playlist {idx_pl+1}")
#         subtype = str(plist.get("subtype","destination")).lower().strip()
#         items = plist.get("items", [])

#         anchor_city  = args.anchor_city or plist.get("placeName") or plist.get("anchor_city")
#         if anchor_city: anchor_city = ALIAS_MAP.get(anchor_city.lower(), anchor_city)
#         anchor_state = args.anchor_state
#         if not anchor_state and anchor_city:
#             city_to_state = {
#                 "bengaluru":"karnataka","bangalore":"karnataka","mysore":"karnataka","mysuru":"karnataka",
#                 "chennai":"tamil nadu","hyderabad":"telangana","kochi":"kerala","mumbai":"maharashtra",
#                 "pune":"maharashtra","delhi":"delhi","kolkata":"west bengal","thiruvananthapuram":"kerala",
#             }
#             anchor_state = city_to_state.get(anchor_city.lower())

#         stats = {"title": title, "publishable":0, "partial":0, "unresolved":0}
#         resolved_items = []

#         print(f"\nPlaylist: {title}\n  Anchor: {anchor_city}, {anchor_state}\n  Items: {len(items)}")
#         for i, item in enumerate(items):
#             totals["items"] += 1
#             src_name = (item.get("name","")).strip()
#             if not src_name:
#                 stats["unresolved"] += 1; totals["unresolved"] += 1
#                 continue

#             category_hint = item.get("category_hint","")
#             final_scope = args.scope if args.scope != "auto" else (item.get("scope") or "destination")
#             hours = parse_hours(item.get("travel_time",""))
#             radius_km = args.radius_km or km_from_hours(hours, args.default_speed)
#             radius_m = int(radius_km*1000)

#             print(f"  [{i+1}] {src_name}  (cat={category_hint}, scope={final_scope}, radius={radius_km}km)")
#             try:
#                 result = resolver.resolve(
#                     name=src_name, category_hint=category_hint, scope=final_scope,
#                     anchor_city=anchor_city, anchor_state=anchor_state, radius_m=radius_m
#                 )
#             except Exception as e:
#                 print(f"    Resolve error: {e}")
#                 result = {"place_id": None, "name": src_name, "confidence": 0.0}

#             entity_kind = infer_entity_kind_from_category(category_hint, final_scope)
#             final_item = {
#                 "name": result.get("name", src_name),
#                 "source_name": src_name,
#                 "entity_kind": entity_kind,
#                 "scope": final_scope,
#                 "category_hint": category_hint,
#                 "place_id": result.get("place_id"),
#                 "lat": result.get("lat"),
#                 "lng": result.get("lng"),
#                 "types": result.get("types", []),
#                 "rating": result.get("rating", 0),
#                 "reviews": result.get("reviews", 0),
#                 "photo_refs": result.get("photo_refs", []),
#                 "address": result.get("address"),
#                 "website": result.get("website"),
#                 "phone": result.get("phone"),
#                 "utc_offset_minutes": result.get("utc_offset_minutes"),
#                 "permanently_closed": bool(result.get("permanently_closed", False)),
#                 "confidence": float(result.get("confidence", 0)),
#                 # carry from step 2
#                 "description": item.get("description",""),
#                 "travel_time": item.get("travel_time",""),
#                 "price": item.get("price",""),
#                 "votes": item.get("votes", 1),
#                 "source_urls": item.get("source_urls", []),
#             }

#             if result.get("place_id"):
#                 totals["resolved"] += 1
#                 if is_publishable(result, entity_kind):
#                     final_item["resolution_status"] = "publishable"
#                     totals["publishable"] += 1; stats["publishable"] += 1
#                     print(f"    ✅ PUBLISHABLE (photos: {len(result.get('photo_refs') or [])})")
#                 else:
#                     final_item["resolution_status"] = "partial"
#                     totals["partial"] += 1; stats["partial"] += 1
#                     print(f"    ⚠️ PARTIAL (photos: {len(result.get('photo_refs') or [])})")
#             else:
#                 final_item["resolution_status"] = "unresolved"
#                 totals["unresolved"] += 1; stats["unresolved"] += 1
#                 print("    ❌ UNRESOLVED")

#             resolved_items.append(final_item)
        
#         # ==================== APPLY KEEP RATIO (FILTERING) ====================
#         if args.keep_ratio < 1.0:
#             original_count = len(resolved_items)
#             print(f"  ✂️ Trimming items (Ratio: {args.keep_ratio})...")
            
#             resolved_items = trim_and_light_shuffle(
#                 resolved_items, 
#                 keep_ratio=args.keep_ratio
#             )
            
#             new_count = len(resolved_items)
#             print(f"     Kept {new_count}/{original_count} items")
#         # ======================================================================

#         per_pl.append(stats)
#         out_playlists.append({
#             "playlistTitle": title,
#             "placeName": plist.get("placeName"),
#             "subtype": subtype,
#             "source_urls": plist.get("source_urls", []),
#             "items": resolved_items,
#         })

#     # Write outputs
#     out_path.write_text(json.dumps(out_playlists, ensure_ascii=False, indent=2), encoding="utf-8")
#     print(f"\n✅ Wrote: {out_path}")

#     # Report
#     report = {
#         "summary": {
#             "total_playlists": len(out_playlists),
#             "total_items": totals["items"],
#             "success_rate": round((totals["publishable"]+totals["partial"])/max(1,totals["items"])*100, 1),
#             "publishable_rate": round(totals["publishable"]/max(1,totals["items"])*100, 1),
#         },
#         "totals": totals,
#         "thresholds": {
#             "min_confidence": MIN_CONFIDENCE, "min_reviews": MIN_REVIEWS,
#             "min_rating": MIN_RATING, "require_photo": REQUIRE_PHOTO,
#         }
#     }
#     rpt_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
#     print(f"📊 Report: {rpt_path}")

#     resolver.save()
#     print(f"💾 Cache updated: {PLACES_CACHE}")

# if __name__ == "__main__":
#     main()

# # 02_5_resolve_validate.py
# # Step 2.5 — Resolve & Validate (Google Places) — with robust photo backfill

# from __future__ import annotations
# import os, json, re, time, math, argparse, unicodedata
# from pathlib import Path
# from typing import List, Dict, Any, Optional, Tuple

# # .env (optional)
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except Exception:
#     pass

# # Deps
# try:
#     import googlemaps   # pip install googlemaps
# except Exception:
#     googlemaps = None

# try:
#     import requests     # pip install requests
# except Exception:
#     requests = None

# try:
#     from rapidfuzz import fuzz
#     def name_sim(a: str, b: str) -> int:
#         return fuzz.token_set_ratio(a, b)
# except Exception:
#     def _tok(s: str) -> set:
#         return set(re.findall(r"[a-z0-9]+", s.lower()))
#     def name_sim(a: str, b: str) -> int:
#         A, B = _tok(a), _tok(b)
#         return int(100 * len(A & B) / len(A | B)) if (A and B) else 0

# # ------------------------------- CONFIG --------------------------------
# BASE_DIR = Path(__file__).resolve().parent

# IN_PATH   = BASE_DIR / "playlist_items.json"
# OUT_PATH  = BASE_DIR / "playlist_items_resolved.json"
# REPORT    = BASE_DIR / "resolve_report.json"

# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# PLACES_CACHE   = CACHE_DIR / "places_cache.json"
# GEOCODE_CACHE  = CACHE_DIR / "geocode_cache.json"

# # Acceptance / scoring
# MIN_CONFIDENCE = 0.75
# RETRY_ATTEMPTS = 2
# MIN_RATING     = 3.5
# MIN_REVIEWS    = 20
# REQUIRE_PHOTO  = False

# # API tuning
# GMAPS_REGION    = "in"
# GMAPS_LANGUAGE  = "en-IN"
# SLEEP_BETWEEN   = 0.08

# # Hardcoded anchor overrides (optional)
# HARDCODE_ANCHOR_CITY  = None
# HARDCODE_ANCHOR_STATE = None

# # Known coords (fast fallback)
# CITY_COORDS: Dict[str, Tuple[float, float]] = {
#     "bengaluru": (12.9716, 77.5946),
#     "bangalore": (12.9716, 77.5946),
#     "mumbai": (19.0760, 72.8777),
#     "delhi": (28.7041, 77.1025),
#     "chennai": (13.0827, 80.2707),
#     "kolkata": (22.5726, 88.3639),
#     "hyderabad": (17.3850, 78.4867),
#     "kochi": (9.9312, 76.2673),
#     "pune": (18.5204, 73.8567),
#     "mysore": (12.2958, 76.6394),
#     "mangalore": (12.9141, 74.8560),
#     "coimbatore": (11.0168, 76.9558),
#     "thiruvananthapuram": (8.5241, 76.9366),
#     "kottayam": (9.5916, 76.5222),
# }

# # Aliases / hints
# ALIAS_MAP = {
#     "alleppey":"alappuzha","pondicherry":"puducherry","bombay":"mumbai",
#     "calcutta":"kolkata","rishikonda":"rushikonda","havelock":"swaraj dweep",
#     "ooty":"udhagamandalam","coonoor":"coonoor","coorg":"kodagu",
# }
# NAME_ALIASES = [
#     (re.compile(r"\bchikmagalur(u)?\b", re.I), "chikkamagaluru"),
#     (re.compile(r"\bcoorg\b", re.I), "kodagu"),
#     (re.compile(r"\bcalicut\b", re.I), "kozhikode"),
#     (re.compile(r"\bbangalore\b", re.I), "bengaluru"),
#     (re.compile(r"\booty\b", re.I), "udhagamandalam"),
# ]
# FAMOUS_LANDMARK_STATES = {
#     "jog falls":"karnataka","mysore":"karnataka","mysore palace":"karnataka","hampi":"karnataka",
#     "coorg":"karnataka","kodagu":"karnataka","chikmagalur":"karnataka","chikkamagaluru":"karnataka",
#     "gokarna":"karnataka","dandeli":"karnataka","badami":"karnataka","kudremukh":"karnataka",
#     "bandipur":"karnataka","nagarhole":"karnataka","bhadra":"karnataka",
#     "lepakshi":"andhra pradesh","horsley hills":"andhra pradesh","tirupati":"andhra pradesh",
#     "wayanad":"kerala","kozhikode":"kerala","alappuzha":"kerala","munnar":"kerala","thekkady":"kerala",
#     "kovalam":"kerala","varkala":"kerala","kochi":"kerala",
#     "ooty":"tamil nadu","kodaikanal":"tamil nadu","coonoor":"tamil nadu","yercaud":"tamil nadu","mahabalipuram":"tamil nadu",
# }
# SCOPE_TYPES = {
#     "destination":{"locality","sublocality","administrative_area_level_3","administrative_area_level_2","administrative_area_level_1","political"},
#     "poi":{"tourist_attraction","point_of_interest","museum","zoo","amusement_park","aquarium","art_gallery","hindu_temple","church","mosque","synagogue","stadium","campground","lodging","restaurant","bar","cafe","night_club"},
#     "natural":{"natural_feature","tourist_attraction","park","point_of_interest"},
# }
# BAD_POI_SUFFIX = re.compile(r"(view\s*point|parking|ticket|counter|canteen|shop|toilet|entrance|gate)", re.I)

# # ------------------------------ Helpers --------------------------------
# def strip_accents(s: str) -> str:
#     return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
# def normalize_name(s: str) -> str:
#     s = s or ""
#     s = strip_accents(s).strip()
#     return re.sub(r"\s+", " ", s)

# def infer_entity_kind_from_category(category_hint: str, scope: str) -> str:
#     if not category_hint:
#         return "destination" if scope == "destination" else "tourist_attraction"
#     m = {
#         "waterfall":"waterfall","beach":"beach","island":"island","lake":"lake","peak":"peak","mountain":"peak",
#         "national_park":"national_park","sanctuary":"sanctuary","reserve":"reserve","park":"park","cave":"cave",
#         "trek":"trek","trail":"trail","temple":"temple","fort":"fort",
#         "resort":"resort","hotel":"hotel","camp":"campground","homestay":"homestay","villa":"villa","lodge":"lodge","hostel":"hostel",
#         "hill_station":"hill_station","town":"destination","city":"destination","district":"destination","region":"destination",
#     }
#     return m.get(category_hint.lower(), "destination")

# def allowed_types_for_kind(kind: str) -> set:
#     m = {
#         "waterfall":{"natural_feature","tourist_attraction","point_of_interest"},
#         "beach":{"natural_feature","tourist_attraction","point_of_interest"},
#         "island":{"natural_feature","tourist_attraction","point_of_interest"},
#         "lake":{"natural_feature","tourist_attraction","point_of_interest"},
#         "peak":{"natural_feature","tourist_attraction","point_of_interest"},
#         "national_park":{"park","tourist_attraction","point_of_interest"},
#         "sanctuary":{"park","tourist_attraction","point_of_interest"},
#         "reserve":{"park","tourist_attraction","point_of_interest"},
#         "park":{"park","tourist_attraction","point_of_interest"},
#         "trek":{"natural_feature","tourist_attraction","point_of_interest"},
#         "trail":{"natural_feature","tourist_attraction","point_of_interest"},
#         "cave":{"natural_feature","tourist_attraction","point_of_interest"},
#         "temple":{"hindu_temple","tourist_attraction","point_of_interest","place_of_worship"},
#         "fort":{"tourist_attraction","point_of_interest","museum"},
#         "resort":{"lodging","tourist_attraction"},
#         "hotel":{"lodging"},
#         "homestay":{"lodging"},
#         "villa":{"lodging"},
#         "lodge":{"lodging"},
#         "hostel":{"lodging"},
#         "campground":{"campground","lodging"},
#         "destination":{"locality","sublocality","administrative_area_level_3","administrative_area_level_2","political"},
#         "hill_station":{"locality","tourist_attraction","point_of_interest"},
#     }
#     return m.get(kind.lower(), {"tourist_attraction","point_of_interest","locality"})

# def primary_google_type_for_kind(kind: str) -> Optional[str]:
#     k = kind.lower()
#     if k in {"resort","hotel","villa","homestay","lodge","hostel","campground"}: return "lodging"
#     if k in {"park","national_park","sanctuary","reserve"}:                    return "park"
#     if k in {"temple"}:                                                        return "hindu_temple"
#     return "tourist_attraction"

# def expand_name_variants(name: str) -> List[str]:
#     variants = {normalize_name(name)}
#     for pat, rep in NAME_ALIASES:
#         if pat.search(name):
#             variants.add(pat.sub(rep, name))
#     return list(variants)

# def build_enhanced_queries(name: str, category_hint: str, scope: str,
#                            anchor_city: Optional[str], anchor_state: Optional[str]) -> List[str]:
#     name_qs = expand_name_variants(name)
#     queries: List[str] = []
#     low = name.lower()
#     for landmark, state in FAMOUS_LANDMARK_STATES.items():
#         if landmark in low:
#             for nq in name_qs:
#                 queries.append(f"{nq} {state} india")
#             break
#     cat_word = category_hint.replace("_"," ") if category_hint else ""
#     for nq in name_qs:
#         queries.append(nq)
#         if anchor_state:
#             queries.append(f"{nq} {anchor_state}")
#             queries.append(f"{nq} {anchor_state} india")
#         if anchor_city:
#             queries.append(f"{nq} {anchor_city}")
#             queries.append(f"{nq} near {anchor_city}")
#         if cat_word and cat_word not in nq.lower():
#             queries.append(f"{nq} {cat_word}")
#             if anchor_state: queries.append(f"{nq} {cat_word} {anchor_state}")
#             if anchor_city:  queries.append(f"{nq} {cat_word} {anchor_city}")
#         if scope == "destination":
#             queries.extend([f"{nq} town", f"{nq} city", f"{nq} place"])
#         elif scope == "natural" and "waterfall" in (category_hint or "").lower():
#             queries.append(f"{nq} waterfall")
#     seen = set(); out=[]
#     for q in queries:
#         t = q.strip().lower()
#         if t and t not in seen and len(t)>2:
#             seen.add(t); out.append(q.strip())
#     return out[:15]

# def parse_hours(s: str) -> Optional[float]:
#     if not s: return None
#     m = re.search(r"(\d+(?:\.\d+)?)\s*hour", s, re.I)
#     return float(m.group(1)) if m else None

# def km_from_hours(h: Optional[float], default_speed_kmph: int = 70, buffer_km: int = 50) -> int:
#     if h is None: return 450
#     return int(max(80, min(800, h*default_speed_kmph + buffer_km)))

# def haversine_km(a_lat: float, a_lng: float, b_lat: float, b_lng: float) -> float:
#     R=6371.0
#     dlat=math.radians(b_lat-a_lat); dlon=math.radians(b_lng-a_lng)
#     x = math.sin(dlat/2)**2 + math.cos(math.radians(a_lat))*math.cos(math.radians(b_lat))*math.sin(dlon/2)**2
#     return 2*R*math.asin(min(1, math.sqrt(x)))

# def distance_score(cand: Dict[str,Any], anchor_lat: Optional[float], anchor_lng: Optional[float], radius_m: int) -> float:
#     if not (anchor_lat is not None and anchor_lng is not None): return 0.0
#     if cand.get("lat") is None or cand.get("lng") is None: return 0.0
#     dkm=haversine_km(anchor_lat, anchor_lng, cand["lat"], cand["lng"]); rkm=max(1, radius_m/1000)
#     if dkm <= 0.4*rkm: return 1.0
#     if dkm >= 2.0*rkm: return 0.0
#     return max(0.0, 1.0 - ((dkm-0.4*rkm)/(1.6*rkm)))

# def popularity_score(rating: float, reviews: int) -> float:
#     rating = rating or 0.0; reviews=max(0, int(reviews or 0))
#     return min(1.0, rating * math.log10(reviews + 1) / 5.0)

# def type_compat_score(types: List[str], allowed: set) -> float:
#     return 1.0 if set(types or []) & allowed else (0.5 if types else 0.0)

# def state_match_score(address: str, expected_state: Optional[str]) -> float:
#     if not address or not expected_state: return 0.0
#     return 1.0 if expected_state.lower() in address.lower() else 0.0

# def circle_bias(lat: float, lng: float, radius_m: int) -> str:
#     return f"circle:{max(1000, int(radius_m))}@{lat},{lng}"

# # ----------------------------- Resolver --------------------------------
# class EnhancedResolver:
#     def __init__(self, api_key: Optional[str], cache_path: Path, refresh_photos: bool):
#         self.enabled = bool(api_key and googlemaps)
#         self.cache_path = cache_path
#         self.cache: Dict[str, Any] = {}
#         if cache_path.exists():
#             try: self.cache = json.loads(cache_path.read_text(encoding="utf-8"))
#             except Exception: self.cache = {}
#         self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None
#         self.api_key = api_key
#         self.refresh_photos = refresh_photos

#     def save(self):
#         try:
#             self.cache_path.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass

#     def _cache_key(self, name: str, category_hint: str, scope: str, anchor_city: Optional[str], anchor_state: Optional[str]) -> str:
#         def n(s): return normalize_name(s or "").lower()
#         return "|".join([n(name),(category_hint or "").lower(),scope.lower(),n(anchor_city),n(anchor_state)])

#     # --- REST photo fallback (works even if SDK omits photos) ---
#     def _rest_photo_refs(self, place_id: str) -> List[str]:
#         if not (requests and self.api_key): return []
#         url = "https://maps.googleapis.com/maps/api/place/details/json"
#         params = {"place_id": place_id, "fields": "photos", "key": self.api_key}
#         try:
#             r = requests.get(url, params=params, timeout=20)
#             r.raise_for_status()
#             data = r.json()
#             if data.get("status") != "OK": return []
#             photos = (data.get("result") or {}).get("photos") or []
#             return [p.get("photo_reference") for p in photos if p.get("photo_reference")]
#         except Exception:
#             return []

#     def _details_with_photos(self, place_id: str) -> Dict[str, Any]:
#         out = {}
#         if not self.gmaps:
#             return out
#         try:
#             details = self.gmaps.place(
#                 place_id=place_id,
#                 fields=[
#                     "place_id","name","geometry/location","types","formatted_address",
#                     "website","international_phone_number","opening_hours","price_level",
#                     "permanently_closed","rating","user_ratings_total","photos","utc_offset_minutes"
#                 ],
#                 language=GMAPS_LANGUAGE
#             ).get("result", {}) or {}
#         except Exception:
#             details = {}

#         # If SDK omitted photos, use REST fallback
#         photos = details.get("photos") or []
#         if not photos:
#             refs = self._rest_photo_refs(place_id)
#             if refs:
#                 details["photos"] = [{"photo_reference": r} for r in refs]
#         return details

#     def geocode_anchor(self, text: str) -> Tuple[Optional[float], Optional[float]]:
#         key = (text or "").strip().lower()
#         if key in CITY_COORDS: return CITY_COORDS[key]

#         # cached?
#         lat=lng=None
#         if GEOCODE_CACHE.exists():
#             try:
#                 geo_cache = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
#                 if key in geo_cache:
#                     d=geo_cache[key]; lat, lng = d.get("lat"), d.get("lng")
#                     return lat, lng
#             except Exception:
#                 pass

#         if not self.gmaps: return None, None
#         try:
#             res = self.gmaps.geocode(f"{text}, India", language=GMAPS_LANGUAGE)
#             if res:
#                 loc = res[0]["geometry"]["location"]
#                 lat, lng = loc["lat"], loc["lng"]
#         except Exception:
#             pass

#         # persist
#         try:
#             geo_cache = {}
#             if GEOCODE_CACHE.exists():
#                 geo_cache = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
#             geo_cache[key] = {"lat": lat, "lng": lng}
#             GEOCODE_CACHE.write_text(json.dumps(geo_cache, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass
#         return lat, lng

#     def resolve(self, *, name: str, category_hint: str, scope: str,
#                 anchor_city: Optional[str], anchor_state: Optional[str], radius_m: int) -> Dict[str, Any]:

#         empty = {
#             "place_id": None, "name": name, "lat": None, "lng": None, "types": [],
#             "rating": 0.0, "reviews": 0, "address": None, "website": None, "phone": None,
#             "opening": [], "price_level": None, "permanently_closed": False,
#             "photo_refs": [], "utc_offset_minutes": None, "confidence": 0.0
#         }

#         cache_key = self._cache_key(name, category_hint, scope, anchor_city, anchor_state)
#         if cache_key in self.cache:
#             cached = {**empty, **self.cache[cache_key]}
#             # If we want fresh photos or cache has none, try to backfill now
#             if cached.get("place_id") and (self.refresh_photos or not cached.get("photo_refs")):
#                 det = self._details_with_photos(cached["place_id"])
#                 if det:
#                     photos = det.get("photos") or []
#                     photo_refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]
#                     if photo_refs:
#                         cached["photo_refs"] = photo_refs
#                         self.cache[cache_key] = cached
#             return cached

#         if not self.enabled:
#             self.cache[cache_key] = empty
#             return empty

#         # Anchor coords
#         a_lat = a_lng = None
#         if anchor_city:
#             a_lat, a_lng = self.geocode_anchor(anchor_city)
#         if (a_lat is None or a_lng is None) and anchor_state:
#             a_lat, a_lng = self.geocode_anchor(anchor_state)

#         # Entity kind / types
#         entity_kind = infer_entity_kind_from_category(category_hint, scope)
#         allowed_kind_types = allowed_types_for_kind(entity_kind)
#         allowed_scope_types = SCOPE_TYPES.get(scope, set())
#         primary_type = primary_google_type_for_kind(entity_kind)

#         # Queries
#         queries = build_enhanced_queries(name, category_hint, scope, anchor_city, anchor_state)

#         best = empty; best_score = -1.0
#         bias = circle_bias(a_lat, a_lng, radius_m) if (a_lat and a_lng) else None

#         print(f"Resolving: {name}  (category={category_hint}, scope={scope})")

#         # Pre-seed for destination
#         all_candidates: List[Dict[str, Any]] = []
#         if scope == "destination" and self.gmaps:
#             try:
#                 components = {"country":"IN"}
#                 if anchor_state: components["administrative_area"]=anchor_state
#                 geo_query = f"{name}, {anchor_state or 'India'}"
#                 geo_results = self.gmaps.geocode(geo_query, components=components, language=GMAPS_LANGUAGE)
#                 for g in (geo_results or [])[:2]:
#                     loc = g.get("geometry", {}).get("location", {})
#                     all_candidates.append({
#                         "place_id": g.get("place_id"),
#                         "name": g.get("formatted_address", name).split(",")[0],
#                         "geometry": {"location": loc},
#                         "types": g.get("types", []),
#                         "formatted_address": g.get("formatted_address", ""),
#                         "rating": 0, "user_ratings_total": 0, "_source":"geocode"
#                     })
#             except Exception:
#                 pass

#         # Search
#         for q in queries[:8]:
#             # Find Place
#             try:
#                 kwargs = {
#                     "input": q, "input_type":"textquery",
#                     "fields":["place_id","name","geometry/location","types","formatted_address","rating","user_ratings_total"],
#                     "language": GMAPS_LANGUAGE
#                 }
#                 if bias: kwargs["location_bias"] = bias
#                 fp = self.gmaps.find_place(**kwargs)
#                 for c in fp.get("candidates", []):
#                     c["_source"] = "find_place"
#                     all_candidates.append(c)
#             except Exception:
#                 pass

#             # Text Search
#             try:
#                 ts_kwargs = {"query": q, "region": GMAPS_REGION, "language": GMAPS_LANGUAGE}
#                 if a_lat and a_lng:
#                     ts_kwargs.update({"location":(a_lat,a_lng), "radius":min(500000, max(20000, radius_m))})
#                 if primary_type:
#                     ts_kwargs["type"] = primary_type
#                 ts = self.gmaps.places(**ts_kwargs)
#                 for c in ts.get("results", []):
#                     c["_source"] = "text_search"
#                     all_candidates.append(c)
#             except Exception:
#                 pass

#             time.sleep(SLEEP_BETWEEN)

#         # Score
#         for cand in all_candidates[:15]:
#             loc = (cand.get("geometry") or {}).get("location") or {}
#             data = {
#                 "place_id": cand.get("place_id"),
#                 "name": cand.get("name", name),
#                 "lat": loc.get("lat"),
#                 "lng": loc.get("lng"),
#                 "types": cand.get("types", []),
#                 "address": cand.get("formatted_address", ""),
#                 "rating": float(cand.get("rating", 0)),
#                 "reviews": int(cand.get("user_ratings_total", 0)),
#             }
#             sim = name_sim(name, data["name"]) / 100.0
#             kind_compat = type_compat_score(data["types"], allowed_kind_types)
#             scope_compat = 1.0 if (set(data["types"]) & allowed_scope_types) else 0.3
#             dist = distance_score(data, a_lat, a_lng, radius_m)
#             pop  = popularity_score(data["rating"], data["reviews"])
#             st   = state_match_score(data["address"], anchor_state)

#             scope_adj = 0.0
#             if scope == "destination":
#                 if set(data["types"]) & {"tourist_attraction","lodging","restaurant","store"}: scope_adj -= 0.2
#                 if cand.get("_source") == "geocode": scope_adj += 0.1
#             elif scope == "natural":
#                 if BAD_POI_SUFFIX.search(data["name"]): scope_adj -= 0.3
#                 if set(data["types"]) & {"locality","administrative_area_level_2"}: scope_adj -= 0.2
#             elif scope == "poi":
#                 if set(data["types"]) & {"locality","administrative_area_level_1","administrative_area_level_2"}: scope_adj -= 0.3

#             lm_bonus = 0.0
#             lm_state = None
#             for landmark, s in FAMOUS_LANDMARK_STATES.items():
#                 if landmark in name.lower(): lm_state = s; break
#             if lm_state:
#                 if lm_state in (data["address"] or "").lower(): lm_bonus = 0.15
#                 else:
#                     for wrong in ["kerala","tamil nadu","andhra pradesh","telangana","odisha"]:
#                         if wrong in (data["address"] or "").lower() and wrong != lm_state:
#                             lm_bonus = -0.2; break

#             score = max(0.0, min(1.0,
#                 0.30*sim + 0.20*kind_compat + 0.15*scope_compat + 0.15*dist + 0.10*pop + 0.10*st + scope_adj + lm_bonus
#             ))
#             if score > best_score:
#                 best_score = score
#                 best = {**empty, **data, "confidence": round(score, 3)}

#         print(f"  Best: {best.get('name')} (confidence: {best_score:.3f})")

#         # Details + PHOTO BACKFILL
#         if best.get("place_id") and best_score >= 0.5:
#             det = self._details_with_photos(best["place_id"])
#             if det:
#                 loc = (det.get("geometry") or {}).get("location") or {}
#                 photos = det.get("photos") or []
#                 photo_refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]
#                 best.update({
#                     "name": det.get("name", best["name"]),
#                     "lat": loc.get("lat", best["lat"]),
#                     "lng": loc.get("lng", best["lng"]),
#                     "types": det.get("types", best["types"]),
#                     "address": det.get("formatted_address", best["address"]),
#                     "website": det.get("website"),
#                     "phone": det.get("international_phone_number"),
#                     "opening": (det.get("opening_hours", {}) or {}).get("periods", []),
#                     "price_level": det.get("price_level"),
#                     "permanently_closed": det.get("permanently_closed", False),
#                     "rating": float(det.get("rating", best["rating"])),
#                     "reviews": int(det.get("user_ratings_total", best["reviews"])),
#                     "photo_refs": photo_refs,
#                     "utc_offset_minutes": det.get("utc_offset_minutes"),
#                 })
#             time.sleep(SLEEP_BETWEEN)

#         # Cache AFTER enrichment so we don't lock empty photo_refs
#         self.cache[cache_key] = best
#         return best

# # ------------------------------- Main ---------------------------------
# # def is_publishable(resolved: Dict[str, Any], entity_kind: str) -> bool:
# #     pid = resolved.get("place_id")
# #     lat, lng = resolved.get("lat"), resolved.get("lng")
# #     if not (pid and lat is not None and lng is not None): return False
# #     if REQUIRE_PHOTO and not resolved.get("photo_refs"):  return False
# #     rating = float(resolved.get("rating", 0)); reviews = int(resolved.get("reviews", 0))
# #     has_photo = bool(resolved.get("photo_refs"))
# #     if not ((rating >= MIN_RATING) or (reviews >= MIN_REVIEWS) or has_photo): return False
# #     allowed = allowed_types_for_kind(entity_kind)
# #     if not (set(resolved.get("types", [])) & allowed):
# #         if rating >= MIN_RATING and reviews >= MIN_REVIEWS: return True
# #         return False
# #     return True
# def is_publishable(resolved: Dict[str, Any], entity_kind: str) -> bool:
#     pid = resolved.get("place_id")
#     lat, lng = resolved.get("lat"), resolved.get("lng")
#     confidence = resolved.get("confidence", 0.0)
    
#     # NEW: Reject if confidence too low
#     if confidence < 0.80:
#         return False
    
#     if not (pid and lat is not None and lng is not None): 
#         return False
#     if REQUIRE_PHOTO and not resolved.get("photo_refs"):  
#         return False
    
#     rating = float(resolved.get("rating", 0))
#     reviews = int(resolved.get("reviews", 0))
#     has_photo = bool(resolved.get("photo_refs"))
    
#     if not ((rating >= MIN_RATING) or (reviews >= MIN_REVIEWS) or has_photo): 
#         return False
    
#     allowed = allowed_types_for_kind(entity_kind)
#     if not (set(resolved.get("types", [])) & allowed):
#         if rating >= MIN_RATING and reviews >= MIN_REVIEWS: 
#             return True
#         return False
    
#     return True

# def main():
#     global MIN_CONFIDENCE, RETRY_ATTEMPTS, REQUIRE_PHOTO

#     p = argparse.ArgumentParser("Step 2.5 — Resolve & Validate (with photo backfill)")
#     p.add_argument("--in", dest="in_path", default=str(IN_PATH))
#     p.add_argument("--out", dest="out_path", default=str(OUT_PATH))
#     p.add_argument("--report", dest="report_path", default=str(REPORT))
#     p.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE)
#     p.add_argument("--retry", type=int, default=RETRY_ATTEMPTS)
#     p.add_argument("--require-photo", action="store_true", default=REQUIRE_PHOTO)
#     p.add_argument("--refresh-photos", action="store_true", help="Re-fetch photos for cached items or empty photo_refs")
#     p.add_argument("--scope", choices=["auto","destination","poi","natural"], default="auto")
#     p.add_argument("--anchor-city", type=str, default=HARDCODE_ANCHOR_CITY)
#     p.add_argument("--anchor-state", type=str, default=HARDCODE_ANCHOR_STATE)
#     p.add_argument("--radius-km", type=int, default=None)
#     p.add_argument("--default-speed", type=int, default=70)
#     args = p.parse_args()

#     MIN_CONFIDENCE = args.min_confidence
#     RETRY_ATTEMPTS = args.retry
#     REQUIRE_PHOTO  = args.require_photo

#     in_path  = Path(args.in_path)
#     out_path = Path(args.out_path)
#     rpt_path = Path(args.report_path)

#     if not in_path.exists():
#         print(f"Error: input not found: {in_path}"); return
#     api_key = "AIzaSyDEuy2i8AbSR-jnstZG22ndFqy71jtKbgg"
#     if not api_key:
#         print("Error: GOOGLE_MAPS_API_KEY not set"); return
#     if not googlemaps:
#         print("Error: googlemaps not installed. pip install googlemaps"); return

#     resolver = EnhancedResolver(api_key, PLACES_CACHE, refresh_photos=args.refresh_photos)

#     try:
#         data = json.loads(in_path.read_text(encoding="utf-8"))
#     except Exception as e:
#         print(f"Error reading input: {e}"); return
#     if not isinstance(data, list):
#         print("Error: Input must be a JSON array of playlists"); return

#     totals = {"items":0,"resolved":0,"publishable":0,"partial":0,"unresolved":0}
#     per_pl = []
#     print(f"Processing {len(data)} playlists…")

#     out_playlists = []
#     for idx_pl, plist in enumerate(data):
#         title = plist.get("playlistTitle", f"Playlist {idx_pl+1}")
#         subtype = str(plist.get("subtype","destination")).lower().strip()
#         items = plist.get("items", [])

#         anchor_city  = args.anchor_city or plist.get("placeName") or plist.get("anchor_city")
#         if anchor_city: anchor_city = ALIAS_MAP.get(anchor_city.lower(), anchor_city)
#         anchor_state = args.anchor_state
#         if not anchor_state and anchor_city:
#             city_to_state = {
#                 "bengaluru":"karnataka","bangalore":"karnataka","mysore":"karnataka","mysuru":"karnataka",
#                 "chennai":"tamil nadu","hyderabad":"telangana","kochi":"kerala","mumbai":"maharashtra",
#                 "pune":"maharashtra","delhi":"delhi","kolkata":"west bengal","thiruvananthapuram":"kerala",
#             }
#             anchor_state = city_to_state.get(anchor_city.lower())

#         stats = {"title": title, "publishable":0, "partial":0, "unresolved":0}
#         resolved_items = []

#         print(f"\nPlaylist: {title}\n  Anchor: {anchor_city}, {anchor_state}\n  Items: {len(items)}")
#         for i, item in enumerate(items):
#             totals["items"] += 1
#             src_name = (item.get("name","")).strip()
#             if not src_name:
#                 stats["unresolved"] += 1; totals["unresolved"] += 1
#                 continue

#             category_hint = item.get("category_hint","")
#             final_scope = args.scope if args.scope != "auto" else (item.get("scope") or "destination")
#             hours = parse_hours(item.get("travel_time",""))
#             radius_km = args.radius_km or km_from_hours(hours, args.default_speed)
#             radius_m = int(radius_km*1000)

#             print(f"  [{i+1}] {src_name}  (cat={category_hint}, scope={final_scope}, radius={radius_km}km)")
#             try:
#                 result = resolver.resolve(
#                     name=src_name, category_hint=category_hint, scope=final_scope,
#                     anchor_city=anchor_city, anchor_state=anchor_state, radius_m=radius_m
#                 )
#             except Exception as e:
#                 print(f"    Resolve error: {e}")
#                 result = {"place_id": None, "name": src_name, "confidence": 0.0}

#             entity_kind = infer_entity_kind_from_category(category_hint, final_scope)
#             final_item = {
#                 "name": result.get("name", src_name),
#                 "source_name": src_name,
#                 "entity_kind": entity_kind,
#                 "scope": final_scope,
#                 "category_hint": category_hint,
#                 "place_id": result.get("place_id"),
#                 "lat": result.get("lat"),
#                 "lng": result.get("lng"),
#                 "types": result.get("types", []),
#                 "rating": result.get("rating", 0),
#                 "reviews": result.get("reviews", 0),
#                 "photo_refs": result.get("photo_refs", []),
#                 "address": result.get("address"),
#                 "website": result.get("website"),
#                 "phone": result.get("phone"),
#                 "utc_offset_minutes": result.get("utc_offset_minutes"),
#                 "permanently_closed": bool(result.get("permanently_closed", False)),
#                 "confidence": float(result.get("confidence", 0)),
#                 # carry from step 2
#                 "description": item.get("description",""),
#                 "travel_time": item.get("travel_time",""),
#                 "price": item.get("price",""),
#                 "votes": item.get("votes", 1),
#                 "source_urls": item.get("source_urls", []),
#             }

#             if result.get("place_id"):
#                 totals["resolved"] += 1
#                 if is_publishable(result, entity_kind):
#                     final_item["resolution_status"] = "publishable"
#                     totals["publishable"] += 1; stats["publishable"] += 1
#                     print(f"    ✅ PUBLISHABLE (photos: {len(result.get('photo_refs') or [])})")
#                 else:
#                     final_item["resolution_status"] = "partial"
#                     totals["partial"] += 1; stats["partial"] += 1
#                     print(f"    ⚠️ PARTIAL (photos: {len(result.get('photo_refs') or [])})")
#             else:
#                 final_item["resolution_status"] = "unresolved"
#                 totals["unresolved"] += 1; stats["unresolved"] += 1
#                 print("    ❌ UNRESOLVED")

#             resolved_items.append(final_item)

#         per_pl.append(stats)
#         out_playlists.append({
#             "playlistTitle": title,
#             "placeName": plist.get("placeName"),
#             "subtype": subtype,
#             "source_urls": plist.get("source_urls", []),
#             "items": resolved_items,
#         })

#     # Write outputs
#     out_path.write_text(json.dumps(out_playlists, ensure_ascii=False, indent=2), encoding="utf-8")
#     print(f"\n✅ Wrote: {out_path}")

#     # Report
#     report = {
#         "summary": {
#             "total_playlists": len(out_playlists),
#             "total_items": totals["items"],
#             "success_rate": round((totals["publishable"]+totals["partial"])/max(1,totals["items"])*100, 1),
#             "publishable_rate": round(totals["publishable"]/max(1,totals["items"])*100, 1),
#         },
#         "totals": totals,
#         "thresholds": {
#             "min_confidence": MIN_CONFIDENCE, "min_reviews": MIN_REVIEWS,
#             "min_rating": MIN_RATING, "require_photo": REQUIRE_PHOTO,
#         }
#     }
#     rpt_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
#     print(f"📊 Report: {rpt_path}")

#     resolver.save()
#     print(f"💾 Cache updated: {PLACES_CACHE}")

# if __name__ == "__main__":
#     main()
