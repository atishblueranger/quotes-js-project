# 02_5_resolve_validate.py
# Step 2.5 — Resolve & Validate (Google Places) — with robust photo backfill

from __future__ import annotations
import os, json, re, time, math, argparse, unicodedata
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

# .env (optional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Deps
try:
    import googlemaps   # pip install googlemaps
except Exception:
    googlemaps = None

try:
    import requests     # pip install requests
except Exception:
    requests = None

try:
    from rapidfuzz import fuzz
    def name_sim(a: str, b: str) -> int:
        return fuzz.token_set_ratio(a, b)
except Exception:
    def _tok(s: str) -> set:
        return set(re.findall(r"[a-z0-9]+", s.lower()))
    def name_sim(a: str, b: str) -> int:
        A, B = _tok(a), _tok(b)
        return int(100 * len(A & B) / len(A | B)) if (A and B) else 0

# ------------------------------- CONFIG --------------------------------
BASE_DIR = Path(__file__).resolve().parent

IN_PATH   = BASE_DIR / "playlist_items.json"
OUT_PATH  = BASE_DIR / "playlist_items_resolved.json"
REPORT    = BASE_DIR / "resolve_report.json"

CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True, parents=True)
PLACES_CACHE   = CACHE_DIR / "places_cache.json"
GEOCODE_CACHE  = CACHE_DIR / "geocode_cache.json"

# Acceptance / scoring
MIN_CONFIDENCE = 0.75
RETRY_ATTEMPTS = 2
MIN_RATING     = 3.5
MIN_REVIEWS    = 20
REQUIRE_PHOTO  = False

# API tuning
GMAPS_REGION    = "in"
GMAPS_LANGUAGE  = "en-IN"
SLEEP_BETWEEN   = 0.08

# Hardcoded anchor overrides (optional)
HARDCODE_ANCHOR_CITY  = None
HARDCODE_ANCHOR_STATE = None

# Known coords (fast fallback)
CITY_COORDS: Dict[str, Tuple[float, float]] = {
    "bengaluru": (12.9716, 77.5946),
    "bangalore": (12.9716, 77.5946),
    "mumbai": (19.0760, 72.8777),
    "delhi": (28.7041, 77.1025),
    "chennai": (13.0827, 80.2707),
    "kolkata": (22.5726, 88.3639),
    "hyderabad": (17.3850, 78.4867),
    "kochi": (9.9312, 76.2673),
    "pune": (18.5204, 73.8567),
    "mysore": (12.2958, 76.6394),
    "mangalore": (12.9141, 74.8560),
    "coimbatore": (11.0168, 76.9558),
    "thiruvananthapuram": (8.5241, 76.9366),
    "kottayam": (9.5916, 76.5222),
}

# Aliases / hints
ALIAS_MAP = {
    "alleppey":"alappuzha","pondicherry":"puducherry","bombay":"mumbai",
    "calcutta":"kolkata","rishikonda":"rushikonda","havelock":"swaraj dweep",
    "ooty":"udhagamandalam","coonoor":"coonoor","coorg":"kodagu",
}
NAME_ALIASES = [
    (re.compile(r"\bchikmagalur(u)?\b", re.I), "chikkamagaluru"),
    (re.compile(r"\bcoorg\b", re.I), "kodagu"),
    (re.compile(r"\bcalicut\b", re.I), "kozhikode"),
    (re.compile(r"\bbangalore\b", re.I), "bengaluru"),
    (re.compile(r"\booty\b", re.I), "udhagamandalam"),
]
FAMOUS_LANDMARK_STATES = {
    "jog falls":"karnataka","mysore":"karnataka","mysore palace":"karnataka","hampi":"karnataka",
    "coorg":"karnataka","kodagu":"karnataka","chikmagalur":"karnataka","chikkamagaluru":"karnataka",
    "gokarna":"karnataka","dandeli":"karnataka","badami":"karnataka","kudremukh":"karnataka",
    "bandipur":"karnataka","nagarhole":"karnataka","bhadra":"karnataka",
    "lepakshi":"andhra pradesh","horsley hills":"andhra pradesh","tirupati":"andhra pradesh",
    "wayanad":"kerala","kozhikode":"kerala","alappuzha":"kerala","munnar":"kerala","thekkady":"kerala",
    "kovalam":"kerala","varkala":"kerala","kochi":"kerala",
    "ooty":"tamil nadu","kodaikanal":"tamil nadu","coonoor":"tamil nadu","yercaud":"tamil nadu","mahabalipuram":"tamil nadu",
}
SCOPE_TYPES = {
    "destination":{"locality","sublocality","administrative_area_level_3","administrative_area_level_2","administrative_area_level_1","political"},
    "poi":{"tourist_attraction","point_of_interest","museum","zoo","amusement_park","aquarium","art_gallery","hindu_temple","church","mosque","synagogue","stadium","campground","lodging","restaurant","bar","cafe","night_club"},
    "natural":{"natural_feature","tourist_attraction","park","point_of_interest"},
}
BAD_POI_SUFFIX = re.compile(r"(view\s*point|parking|ticket|counter|canteen|shop|toilet|entrance|gate)", re.I)

# ------------------------------ Helpers --------------------------------
def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
def normalize_name(s: str) -> str:
    s = s or ""
    s = strip_accents(s).strip()
    return re.sub(r"\s+", " ", s)

def infer_entity_kind_from_category(category_hint: str, scope: str) -> str:
    if not category_hint:
        return "destination" if scope == "destination" else "tourist_attraction"
    m = {
        "waterfall":"waterfall","beach":"beach","island":"island","lake":"lake","peak":"peak","mountain":"peak",
        "national_park":"national_park","sanctuary":"sanctuary","reserve":"reserve","park":"park","cave":"cave",
        "trek":"trek","trail":"trail","temple":"temple","fort":"fort",
        "resort":"resort","hotel":"hotel","camp":"campground","homestay":"homestay","villa":"villa","lodge":"lodge","hostel":"hostel",
        "hill_station":"hill_station","town":"destination","city":"destination","district":"destination","region":"destination",
    }
    return m.get(category_hint.lower(), "destination")

def allowed_types_for_kind(kind: str) -> set:
    m = {
        "waterfall":{"natural_feature","tourist_attraction","point_of_interest"},
        "beach":{"natural_feature","tourist_attraction","point_of_interest"},
        "island":{"natural_feature","tourist_attraction","point_of_interest"},
        "lake":{"natural_feature","tourist_attraction","point_of_interest"},
        "peak":{"natural_feature","tourist_attraction","point_of_interest"},
        "national_park":{"park","tourist_attraction","point_of_interest"},
        "sanctuary":{"park","tourist_attraction","point_of_interest"},
        "reserve":{"park","tourist_attraction","point_of_interest"},
        "park":{"park","tourist_attraction","point_of_interest"},
        "trek":{"natural_feature","tourist_attraction","point_of_interest"},
        "trail":{"natural_feature","tourist_attraction","point_of_interest"},
        "cave":{"natural_feature","tourist_attraction","point_of_interest"},
        "temple":{"hindu_temple","tourist_attraction","point_of_interest","place_of_worship"},
        "fort":{"tourist_attraction","point_of_interest","museum"},
        "resort":{"lodging","tourist_attraction"},
        "hotel":{"lodging"},
        "homestay":{"lodging"},
        "villa":{"lodging"},
        "lodge":{"lodging"},
        "hostel":{"lodging"},
        "campground":{"campground","lodging"},
        "destination":{"locality","sublocality","administrative_area_level_3","administrative_area_level_2","political"},
        "hill_station":{"locality","tourist_attraction","point_of_interest"},
    }
    return m.get(kind.lower(), {"tourist_attraction","point_of_interest","locality"})

def primary_google_type_for_kind(kind: str) -> Optional[str]:
    k = kind.lower()
    if k in {"resort","hotel","villa","homestay","lodge","hostel","campground"}: return "lodging"
    if k in {"park","national_park","sanctuary","reserve"}:                    return "park"
    if k in {"temple"}:                                                        return "hindu_temple"
    return "tourist_attraction"

def expand_name_variants(name: str) -> List[str]:
    variants = {normalize_name(name)}
    for pat, rep in NAME_ALIASES:
        if pat.search(name):
            variants.add(pat.sub(rep, name))
    return list(variants)

def build_enhanced_queries(name: str, category_hint: str, scope: str,
                           anchor_city: Optional[str], anchor_state: Optional[str]) -> List[str]:
    name_qs = expand_name_variants(name)
    queries: List[str] = []
    low = name.lower()
    for landmark, state in FAMOUS_LANDMARK_STATES.items():
        if landmark in low:
            for nq in name_qs:
                queries.append(f"{nq} {state} india")
            break
    cat_word = category_hint.replace("_"," ") if category_hint else ""
    for nq in name_qs:
        queries.append(nq)
        if anchor_state:
            queries.append(f"{nq} {anchor_state}")
            queries.append(f"{nq} {anchor_state} india")
        if anchor_city:
            queries.append(f"{nq} {anchor_city}")
            queries.append(f"{nq} near {anchor_city}")
        if cat_word and cat_word not in nq.lower():
            queries.append(f"{nq} {cat_word}")
            if anchor_state: queries.append(f"{nq} {cat_word} {anchor_state}")
            if anchor_city:  queries.append(f"{nq} {cat_word} {anchor_city}")
        if scope == "destination":
            queries.extend([f"{nq} town", f"{nq} city", f"{nq} place"])
        elif scope == "natural" and "waterfall" in (category_hint or "").lower():
            queries.append(f"{nq} waterfall")
    seen = set(); out=[]
    for q in queries:
        t = q.strip().lower()
        if t and t not in seen and len(t)>2:
            seen.add(t); out.append(q.strip())
    return out[:15]

def parse_hours(s: str) -> Optional[float]:
    if not s: return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*hour", s, re.I)
    return float(m.group(1)) if m else None

def km_from_hours(h: Optional[float], default_speed_kmph: int = 70, buffer_km: int = 50) -> int:
    if h is None: return 450
    return int(max(80, min(800, h*default_speed_kmph + buffer_km)))

def haversine_km(a_lat: float, a_lng: float, b_lat: float, b_lng: float) -> float:
    R=6371.0
    dlat=math.radians(b_lat-a_lat); dlon=math.radians(b_lng-a_lng)
    x = math.sin(dlat/2)**2 + math.cos(math.radians(a_lat))*math.cos(math.radians(b_lat))*math.sin(dlon/2)**2
    return 2*R*math.asin(min(1, math.sqrt(x)))

def distance_score(cand: Dict[str,Any], anchor_lat: Optional[float], anchor_lng: Optional[float], radius_m: int) -> float:
    if not (anchor_lat is not None and anchor_lng is not None): return 0.0
    if cand.get("lat") is None or cand.get("lng") is None: return 0.0
    dkm=haversine_km(anchor_lat, anchor_lng, cand["lat"], cand["lng"]); rkm=max(1, radius_m/1000)
    if dkm <= 0.4*rkm: return 1.0
    if dkm >= 2.0*rkm: return 0.0
    return max(0.0, 1.0 - ((dkm-0.4*rkm)/(1.6*rkm)))

def popularity_score(rating: float, reviews: int) -> float:
    rating = rating or 0.0; reviews=max(0, int(reviews or 0))
    return min(1.0, rating * math.log10(reviews + 1) / 5.0)

def type_compat_score(types: List[str], allowed: set) -> float:
    return 1.0 if set(types or []) & allowed else (0.5 if types else 0.0)

def state_match_score(address: str, expected_state: Optional[str]) -> float:
    if not address or not expected_state: return 0.0
    return 1.0 if expected_state.lower() in address.lower() else 0.0

def circle_bias(lat: float, lng: float, radius_m: int) -> str:
    return f"circle:{max(1000, int(radius_m))}@{lat},{lng}"

# ----------------------------- Resolver --------------------------------
class EnhancedResolver:
    def __init__(self, api_key: Optional[str], cache_path: Path, refresh_photos: bool):
        self.enabled = bool(api_key and googlemaps)
        self.cache_path = cache_path
        self.cache: Dict[str, Any] = {}
        if cache_path.exists():
            try: self.cache = json.loads(cache_path.read_text(encoding="utf-8"))
            except Exception: self.cache = {}
        self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None
        self.api_key = api_key
        self.refresh_photos = refresh_photos

    def save(self):
        try:
            self.cache_path.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _cache_key(self, name: str, category_hint: str, scope: str, anchor_city: Optional[str], anchor_state: Optional[str]) -> str:
        def n(s): return normalize_name(s or "").lower()
        return "|".join([n(name),(category_hint or "").lower(),scope.lower(),n(anchor_city),n(anchor_state)])

    # --- REST photo fallback (works even if SDK omits photos) ---
    def _rest_photo_refs(self, place_id: str) -> List[str]:
        if not (requests and self.api_key): return []
        url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {"place_id": place_id, "fields": "photos", "key": self.api_key}
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            if data.get("status") != "OK": return []
            photos = (data.get("result") or {}).get("photos") or []
            return [p.get("photo_reference") for p in photos if p.get("photo_reference")]
        except Exception:
            return []

    def _details_with_photos(self, place_id: str) -> Dict[str, Any]:
        out = {}
        if not self.gmaps:
            return out
        try:
            details = self.gmaps.place(
                place_id=place_id,
                fields=[
                    "place_id","name","geometry/location","types","formatted_address",
                    "website","international_phone_number","opening_hours","price_level",
                    "permanently_closed","rating","user_ratings_total","photos","utc_offset_minutes"
                ],
                language=GMAPS_LANGUAGE
            ).get("result", {}) or {}
        except Exception:
            details = {}

        # If SDK omitted photos, use REST fallback
        photos = details.get("photos") or []
        if not photos:
            refs = self._rest_photo_refs(place_id)
            if refs:
                details["photos"] = [{"photo_reference": r} for r in refs]
        return details

    def geocode_anchor(self, text: str) -> Tuple[Optional[float], Optional[float]]:
        key = (text or "").strip().lower()
        if key in CITY_COORDS: return CITY_COORDS[key]

        # cached?
        lat=lng=None
        if GEOCODE_CACHE.exists():
            try:
                geo_cache = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
                if key in geo_cache:
                    d=geo_cache[key]; lat, lng = d.get("lat"), d.get("lng")
                    return lat, lng
            except Exception:
                pass

        if not self.gmaps: return None, None
        try:
            res = self.gmaps.geocode(f"{text}, India", language=GMAPS_LANGUAGE)
            if res:
                loc = res[0]["geometry"]["location"]
                lat, lng = loc["lat"], loc["lng"]
        except Exception:
            pass

        # persist
        try:
            geo_cache = {}
            if GEOCODE_CACHE.exists():
                geo_cache = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
            geo_cache[key] = {"lat": lat, "lng": lng}
            GEOCODE_CACHE.write_text(json.dumps(geo_cache, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        return lat, lng

    def resolve(self, *, name: str, category_hint: str, scope: str,
                anchor_city: Optional[str], anchor_state: Optional[str], radius_m: int) -> Dict[str, Any]:

        empty = {
            "place_id": None, "name": name, "lat": None, "lng": None, "types": [],
            "rating": 0.0, "reviews": 0, "address": None, "website": None, "phone": None,
            "opening": [], "price_level": None, "permanently_closed": False,
            "photo_refs": [], "utc_offset_minutes": None, "confidence": 0.0
        }

        cache_key = self._cache_key(name, category_hint, scope, anchor_city, anchor_state)
        if cache_key in self.cache:
            cached = {**empty, **self.cache[cache_key]}
            # If we want fresh photos or cache has none, try to backfill now
            if cached.get("place_id") and (self.refresh_photos or not cached.get("photo_refs")):
                det = self._details_with_photos(cached["place_id"])
                if det:
                    photos = det.get("photos") or []
                    photo_refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]
                    if photo_refs:
                        cached["photo_refs"] = photo_refs
                        self.cache[cache_key] = cached
            return cached

        if not self.enabled:
            self.cache[cache_key] = empty
            return empty

        # Anchor coords
        a_lat = a_lng = None
        if anchor_city:
            a_lat, a_lng = self.geocode_anchor(anchor_city)
        if (a_lat is None or a_lng is None) and anchor_state:
            a_lat, a_lng = self.geocode_anchor(anchor_state)

        # Entity kind / types
        entity_kind = infer_entity_kind_from_category(category_hint, scope)
        allowed_kind_types = allowed_types_for_kind(entity_kind)
        allowed_scope_types = SCOPE_TYPES.get(scope, set())
        primary_type = primary_google_type_for_kind(entity_kind)

        # Queries
        queries = build_enhanced_queries(name, category_hint, scope, anchor_city, anchor_state)

        best = empty; best_score = -1.0
        bias = circle_bias(a_lat, a_lng, radius_m) if (a_lat and a_lng) else None

        print(f"Resolving: {name}  (category={category_hint}, scope={scope})")

        # Pre-seed for destination
        all_candidates: List[Dict[str, Any]] = []
        if scope == "destination" and self.gmaps:
            try:
                components = {"country":"IN"}
                if anchor_state: components["administrative_area"]=anchor_state
                geo_query = f"{name}, {anchor_state or 'India'}"
                geo_results = self.gmaps.geocode(geo_query, components=components, language=GMAPS_LANGUAGE)
                for g in (geo_results or [])[:2]:
                    loc = g.get("geometry", {}).get("location", {})
                    all_candidates.append({
                        "place_id": g.get("place_id"),
                        "name": g.get("formatted_address", name).split(",")[0],
                        "geometry": {"location": loc},
                        "types": g.get("types", []),
                        "formatted_address": g.get("formatted_address", ""),
                        "rating": 0, "user_ratings_total": 0, "_source":"geocode"
                    })
            except Exception:
                pass

        # Search
        for q in queries[:8]:
            # Find Place
            try:
                kwargs = {
                    "input": q, "input_type":"textquery",
                    "fields":["place_id","name","geometry/location","types","formatted_address","rating","user_ratings_total"],
                    "language": GMAPS_LANGUAGE
                }
                if bias: kwargs["location_bias"] = bias
                fp = self.gmaps.find_place(**kwargs)
                for c in fp.get("candidates", []):
                    c["_source"] = "find_place"
                    all_candidates.append(c)
            except Exception:
                pass

            # Text Search
            try:
                ts_kwargs = {"query": q, "region": GMAPS_REGION, "language": GMAPS_LANGUAGE}
                if a_lat and a_lng:
                    ts_kwargs.update({"location":(a_lat,a_lng), "radius":min(500000, max(20000, radius_m))})
                if primary_type:
                    ts_kwargs["type"] = primary_type
                ts = self.gmaps.places(**ts_kwargs)
                for c in ts.get("results", []):
                    c["_source"] = "text_search"
                    all_candidates.append(c)
            except Exception:
                pass

            time.sleep(SLEEP_BETWEEN)

        # Score
        for cand in all_candidates[:15]:
            loc = (cand.get("geometry") or {}).get("location") or {}
            data = {
                "place_id": cand.get("place_id"),
                "name": cand.get("name", name),
                "lat": loc.get("lat"),
                "lng": loc.get("lng"),
                "types": cand.get("types", []),
                "address": cand.get("formatted_address", ""),
                "rating": float(cand.get("rating", 0)),
                "reviews": int(cand.get("user_ratings_total", 0)),
            }
            sim = name_sim(name, data["name"]) / 100.0
            kind_compat = type_compat_score(data["types"], allowed_kind_types)
            scope_compat = 1.0 if (set(data["types"]) & allowed_scope_types) else 0.3
            dist = distance_score(data, a_lat, a_lng, radius_m)
            pop  = popularity_score(data["rating"], data["reviews"])
            st   = state_match_score(data["address"], anchor_state)

            scope_adj = 0.0
            if scope == "destination":
                if set(data["types"]) & {"tourist_attraction","lodging","restaurant","store"}: scope_adj -= 0.2
                if cand.get("_source") == "geocode": scope_adj += 0.1
            elif scope == "natural":
                if BAD_POI_SUFFIX.search(data["name"]): scope_adj -= 0.3
                if set(data["types"]) & {"locality","administrative_area_level_2"}: scope_adj -= 0.2
            elif scope == "poi":
                if set(data["types"]) & {"locality","administrative_area_level_1","administrative_area_level_2"}: scope_adj -= 0.3

            lm_bonus = 0.0
            lm_state = None
            for landmark, s in FAMOUS_LANDMARK_STATES.items():
                if landmark in name.lower(): lm_state = s; break
            if lm_state:
                if lm_state in (data["address"] or "").lower(): lm_bonus = 0.15
                else:
                    for wrong in ["kerala","tamil nadu","andhra pradesh","telangana","odisha"]:
                        if wrong in (data["address"] or "").lower() and wrong != lm_state:
                            lm_bonus = -0.2; break

            score = max(0.0, min(1.0,
                0.30*sim + 0.20*kind_compat + 0.15*scope_compat + 0.15*dist + 0.10*pop + 0.10*st + scope_adj + lm_bonus
            ))
            if score > best_score:
                best_score = score
                best = {**empty, **data, "confidence": round(score, 3)}

        print(f"  Best: {best.get('name')} (confidence: {best_score:.3f})")

        # Details + PHOTO BACKFILL
        if best.get("place_id") and best_score >= 0.5:
            det = self._details_with_photos(best["place_id"])
            if det:
                loc = (det.get("geometry") or {}).get("location") or {}
                photos = det.get("photos") or []
                photo_refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]
                best.update({
                    "name": det.get("name", best["name"]),
                    "lat": loc.get("lat", best["lat"]),
                    "lng": loc.get("lng", best["lng"]),
                    "types": det.get("types", best["types"]),
                    "address": det.get("formatted_address", best["address"]),
                    "website": det.get("website"),
                    "phone": det.get("international_phone_number"),
                    "opening": (det.get("opening_hours", {}) or {}).get("periods", []),
                    "price_level": det.get("price_level"),
                    "permanently_closed": det.get("permanently_closed", False),
                    "rating": float(det.get("rating", best["rating"])),
                    "reviews": int(det.get("user_ratings_total", best["reviews"])),
                    "photo_refs": photo_refs,
                    "utc_offset_minutes": det.get("utc_offset_minutes"),
                })
            time.sleep(SLEEP_BETWEEN)

        # Cache AFTER enrichment so we don't lock empty photo_refs
        self.cache[cache_key] = best
        return best

# ------------------------------- Main ---------------------------------
# def is_publishable(resolved: Dict[str, Any], entity_kind: str) -> bool:
#     pid = resolved.get("place_id")
#     lat, lng = resolved.get("lat"), resolved.get("lng")
#     if not (pid and lat is not None and lng is not None): return False
#     if REQUIRE_PHOTO and not resolved.get("photo_refs"):  return False
#     rating = float(resolved.get("rating", 0)); reviews = int(resolved.get("reviews", 0))
#     has_photo = bool(resolved.get("photo_refs"))
#     if not ((rating >= MIN_RATING) or (reviews >= MIN_REVIEWS) or has_photo): return False
#     allowed = allowed_types_for_kind(entity_kind)
#     if not (set(resolved.get("types", [])) & allowed):
#         if rating >= MIN_RATING and reviews >= MIN_REVIEWS: return True
#         return False
#     return True
def is_publishable(resolved: Dict[str, Any], entity_kind: str) -> bool:
    pid = resolved.get("place_id")
    lat, lng = resolved.get("lat"), resolved.get("lng")
    confidence = resolved.get("confidence", 0.0)
    
    # NEW: Reject if confidence too low
    if confidence < 0.80:
        return False
    
    if not (pid and lat is not None and lng is not None): 
        return False
    if REQUIRE_PHOTO and not resolved.get("photo_refs"):  
        return False
    
    rating = float(resolved.get("rating", 0))
    reviews = int(resolved.get("reviews", 0))
    has_photo = bool(resolved.get("photo_refs"))
    
    if not ((rating >= MIN_RATING) or (reviews >= MIN_REVIEWS) or has_photo): 
        return False
    
    allowed = allowed_types_for_kind(entity_kind)
    if not (set(resolved.get("types", [])) & allowed):
        if rating >= MIN_RATING and reviews >= MIN_REVIEWS: 
            return True
        return False
    
    return True

def main():
    global MIN_CONFIDENCE, RETRY_ATTEMPTS, REQUIRE_PHOTO

    p = argparse.ArgumentParser("Step 2.5 — Resolve & Validate (with photo backfill)")
    p.add_argument("--in", dest="in_path", default=str(IN_PATH))
    p.add_argument("--out", dest="out_path", default=str(OUT_PATH))
    p.add_argument("--report", dest="report_path", default=str(REPORT))
    p.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE)
    p.add_argument("--retry", type=int, default=RETRY_ATTEMPTS)
    p.add_argument("--require-photo", action="store_true", default=REQUIRE_PHOTO)
    p.add_argument("--refresh-photos", action="store_true", help="Re-fetch photos for cached items or empty photo_refs")
    p.add_argument("--scope", choices=["auto","destination","poi","natural"], default="auto")
    p.add_argument("--anchor-city", type=str, default=HARDCODE_ANCHOR_CITY)
    p.add_argument("--anchor-state", type=str, default=HARDCODE_ANCHOR_STATE)
    p.add_argument("--radius-km", type=int, default=None)
    p.add_argument("--default-speed", type=int, default=70)
    args = p.parse_args()

    MIN_CONFIDENCE = args.min_confidence
    RETRY_ATTEMPTS = args.retry
    REQUIRE_PHOTO  = args.require_photo

    in_path  = Path(args.in_path)
    out_path = Path(args.out_path)
    rpt_path = Path(args.report_path)

    if not in_path.exists():
        print(f"Error: input not found: {in_path}"); return
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not api_key:
        print("Error: GOOGLE_MAPS_API_KEY not set"); return
    if not googlemaps:
        print("Error: googlemaps not installed. pip install googlemaps"); return

    resolver = EnhancedResolver(api_key, PLACES_CACHE, refresh_photos=args.refresh_photos)

    try:
        data = json.loads(in_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Error reading input: {e}"); return
    if not isinstance(data, list):
        print("Error: Input must be a JSON array of playlists"); return

    totals = {"items":0,"resolved":0,"publishable":0,"partial":0,"unresolved":0}
    per_pl = []
    print(f"Processing {len(data)} playlists…")

    out_playlists = []
    for idx_pl, plist in enumerate(data):
        title = plist.get("playlistTitle", f"Playlist {idx_pl+1}")
        subtype = str(plist.get("subtype","destination")).lower().strip()
        items = plist.get("items", [])

        anchor_city  = args.anchor_city or plist.get("placeName") or plist.get("anchor_city")
        if anchor_city: anchor_city = ALIAS_MAP.get(anchor_city.lower(), anchor_city)
        anchor_state = args.anchor_state
        if not anchor_state and anchor_city:
            city_to_state = {
                "bengaluru":"karnataka","bangalore":"karnataka","mysore":"karnataka","mysuru":"karnataka",
                "chennai":"tamil nadu","hyderabad":"telangana","kochi":"kerala","mumbai":"maharashtra",
                "pune":"maharashtra","delhi":"delhi","kolkata":"west bengal","thiruvananthapuram":"kerala",
            }
            anchor_state = city_to_state.get(anchor_city.lower())

        stats = {"title": title, "publishable":0, "partial":0, "unresolved":0}
        resolved_items = []

        print(f"\nPlaylist: {title}\n  Anchor: {anchor_city}, {anchor_state}\n  Items: {len(items)}")
        for i, item in enumerate(items):
            totals["items"] += 1
            src_name = (item.get("name","")).strip()
            if not src_name:
                stats["unresolved"] += 1; totals["unresolved"] += 1
                continue

            category_hint = item.get("category_hint","")
            final_scope = args.scope if args.scope != "auto" else (item.get("scope") or "destination")
            hours = parse_hours(item.get("travel_time",""))
            radius_km = args.radius_km or km_from_hours(hours, args.default_speed)
            radius_m = int(radius_km*1000)

            print(f"  [{i+1}] {src_name}  (cat={category_hint}, scope={final_scope}, radius={radius_km}km)")
            try:
                result = resolver.resolve(
                    name=src_name, category_hint=category_hint, scope=final_scope,
                    anchor_city=anchor_city, anchor_state=anchor_state, radius_m=radius_m
                )
            except Exception as e:
                print(f"    Resolve error: {e}")
                result = {"place_id": None, "name": src_name, "confidence": 0.0}

            entity_kind = infer_entity_kind_from_category(category_hint, final_scope)
            final_item = {
                "name": result.get("name", src_name),
                "source_name": src_name,
                "entity_kind": entity_kind,
                "scope": final_scope,
                "category_hint": category_hint,
                "place_id": result.get("place_id"),
                "lat": result.get("lat"),
                "lng": result.get("lng"),
                "types": result.get("types", []),
                "rating": result.get("rating", 0),
                "reviews": result.get("reviews", 0),
                "photo_refs": result.get("photo_refs", []),
                "address": result.get("address"),
                "website": result.get("website"),
                "phone": result.get("phone"),
                "utc_offset_minutes": result.get("utc_offset_minutes"),
                "permanently_closed": bool(result.get("permanently_closed", False)),
                "confidence": float(result.get("confidence", 0)),
                # carry from step 2
                "description": item.get("description",""),
                "travel_time": item.get("travel_time",""),
                "price": item.get("price",""),
                "votes": item.get("votes", 1),
                "source_urls": item.get("source_urls", []),
            }

            if result.get("place_id"):
                totals["resolved"] += 1
                if is_publishable(result, entity_kind):
                    final_item["resolution_status"] = "publishable"
                    totals["publishable"] += 1; stats["publishable"] += 1
                    print(f"    ✅ PUBLISHABLE (photos: {len(result.get('photo_refs') or [])})")
                else:
                    final_item["resolution_status"] = "partial"
                    totals["partial"] += 1; stats["partial"] += 1
                    print(f"    ⚠️ PARTIAL (photos: {len(result.get('photo_refs') or [])})")
            else:
                final_item["resolution_status"] = "unresolved"
                totals["unresolved"] += 1; stats["unresolved"] += 1
                print("    ❌ UNRESOLVED")

            resolved_items.append(final_item)

        per_pl.append(stats)
        out_playlists.append({
            "playlistTitle": title,
            "placeName": plist.get("placeName"),
            "subtype": subtype,
            "source_urls": plist.get("source_urls", []),
            "items": resolved_items,
        })

    # Write outputs
    out_path.write_text(json.dumps(out_playlists, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✅ Wrote: {out_path}")

    # Report
    report = {
        "summary": {
            "total_playlists": len(out_playlists),
            "total_items": totals["items"],
            "success_rate": round((totals["publishable"]+totals["partial"])/max(1,totals["items"])*100, 1),
            "publishable_rate": round(totals["publishable"]/max(1,totals["items"])*100, 1),
        },
        "totals": totals,
        "thresholds": {
            "min_confidence": MIN_CONFIDENCE, "min_reviews": MIN_REVIEWS,
            "min_rating": MIN_RATING, "require_photo": REQUIRE_PHOTO,
        }
    }
    rpt_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"📊 Report: {rpt_path}")

    resolver.save()
    print(f"💾 Cache updated: {PLACES_CACHE}")

if __name__ == "__main__":
    main()


# # Claude version.env
# """
# Step 2.5 — Resolve & Validate (Google Places) — FINAL ENHANCED VERSION WITH PHOTO FIXES

# Reads playlist_items.json (Step 2) → resolves items to Google Places → writes:
#   - playlist_items_resolved.json
#   - resolve_report.json

# Uses cache/places_cache.json (and geocode_cache.json) to avoid duplicate calls.

# Key improvements based on our discussions:
# - Uses `scope` field from Step 2 for accurate destination vs POI vs natural feature targeting
# - Enhanced geographic validation with landmark-state checking
# - Circular location bias with dynamic radius from travel_time
# - Better query building with category hints and state context
# - Improved scoring algorithm with scope-specific penalties/boosts
# - Geocoding pre-pass for destination scope to find towns/cities
# - Type compatibility checking with allowed types per scope
# - FIXED: Complete photo processing pipeline

# CLI examples:
#   python step2_5_resolve.py --anchor-city "Bengaluru" --anchor-state "Karnataka" 
#   python step2_5_resolve.py --min-confidence 0.75 --require-photo
#   python step2_5_resolve.py --scope destination  # override all items to destination scope
# """

# from __future__ import annotations
# import os, json, re, time, math, argparse, unicodedata
# from pathlib import Path
# from typing import List, Dict, Any, Optional, Tuple

# # Optional .env
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except Exception:
#     pass

# # External deps
# try:
#     import googlemaps  # pip install googlemaps
# except Exception:
#     googlemaps = None

# try:
#     from rapidfuzz import fuzz  # pip install rapidfuzz
#     def name_sim(a: str, b: str) -> int:
#         return fuzz.token_set_ratio(a, b)
# except Exception:
#     def _norm_tokens(s: str) -> set:
#         return set(re.findall(r"[a-z0-9]+", s.lower()))
#     def name_sim(a: str, b: str) -> int:
#         ta, tb = _norm_tokens(a), _norm_tokens(b)
#         if not ta or not tb: return 0
#         return int(100 * len(ta & tb) / len(ta | tb))

# # ------------------------------- CONFIG -------------------------------

# BASE_DIR = Path(__file__).resolve().parent

# IN_PATH   = BASE_DIR / "playlist_items.json"
# OUT_PATH  = BASE_DIR / "playlist_items_resolved.json"
# REPORT    = BASE_DIR / "resolve_report.json"

# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# PLACES_CACHE = CACHE_DIR / "places_cache.json"
# GEOCODE_CACHE = CACHE_DIR / "geocode_cache.json"

# # Acceptance / scoring
# MIN_CONFIDENCE = 0.75           # accept candidate ≥ 0.75
# RETRY_ATTEMPTS = 2              # additional variants beyond the first
# MIN_RATING     = 3.5            # or reviews >= MIN_REVIEWS or has photo
# MIN_REVIEWS    = 20
# REQUIRE_PHOTO  = False          # set True to require ≥1 photo for publishable

# # API tuning
# GMAPS_REGION   = "in"
# GMAPS_LANGUAGE = "en-IN"
# SLEEP_BETWEEN  = 0.08           # gentle pacing for Places/Details

# # Hardcoded anchor overrides (optional)
# HARDCODE_ANCHOR_CITY  = None  # e.g., "Bengaluru"
# HARDCODE_ANCHOR_STATE = None  # e.g., "Karnataka"

# # Known coordinates for quick lookup (expand as needed)
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

# # --------------------------- Enhanced Mapping ---------------------------

# ALIAS_MAP = {
#     # hint/city/state aliases → canonical
#     "alleppey": "alappuzha",
#     "pondicherry": "puducherry",
#     "bombay": "mumbai",
#     "calcutta": "kolkata",
#     "rishikonda": "rushikonda",
#     "havelock": "swaraj dweep",
#     "ooty": "udhagamandalam",
#     "coonoor": "coonoor",
#     "coorg": "kodagu",
# }

# # Name-level aliases (search expansions)
# NAME_ALIASES = [
#     (re.compile(r"\bchikmagalur(u)?\b", re.I), "chikkamagaluru"),
#     (re.compile(r"\bcoorg\b", re.I), "kodagu"),
#     (re.compile(r"\bcalicut\b", re.I), "kozhikode"),
#     (re.compile(r"\bbangalore\b", re.I), "bengaluru"),
#     (re.compile(r"\booty\b", re.I), "udhagamandalam"),
# ]

# # Famous landmark → correct state (helps kill wrong-state doppelgangers)
# FAMOUS_LANDMARK_STATES = {
#     "jog falls": "karnataka",
#     "mysore": "karnataka",
#     "mysore palace": "karnataka",
#     "hampi": "karnataka",
#     "coorg": "karnataka",
#     "kodagu": "karnataka",
#     "chikmagalur": "karnataka",
#     "chikkamagaluru": "karnataka",
#     "gokarna": "karnataka",
#     "dandeli": "karnataka",
#     "badami": "karnataka",
#     "kudremukh": "karnataka",
#     "bandipur": "karnataka",
#     "nagarhole": "karnataka",
#     "bhadra": "karnataka",
#     "lepakshi": "andhra pradesh",
#     "horsley hills": "andhra pradesh",
#     "tirupati": "andhra pradesh",
#     "wayanad": "kerala",
#     "kozhikode": "kerala",
#     "alappuzha": "kerala",
#     "munnar": "kerala",
#     "thekkady": "kerala",
#     "kovalam": "kerala",
#     "varkala": "kerala",
#     "kochi": "kerala",
#     "ooty": "tamil nadu",
#     "kodaikanal": "tamil nadu",
#     "coonoor": "tamil nadu",
#     "yercaud": "tamil nadu",
#     "mahabalipuram": "tamil nadu",
# }

# # Scope-specific Google Place types
# SCOPE_TYPES = {
#     "destination": {
#         "locality", "sublocality", "administrative_area_level_3",
#         "administrative_area_level_2", "administrative_area_level_1", "political"
#     },
#     "poi": {
#         "tourist_attraction", "point_of_interest", "museum", "zoo", "amusement_park",
#         "aquarium", "art_gallery", "hindu_temple", "church", "mosque", "synagogue",
#         "stadium", "campground", "lodging", "restaurant", "bar", "cafe", "night_club"
#     },
#     "natural": {
#         "natural_feature", "tourist_attraction", "park", "point_of_interest"
#     },
# }

# # Bad POI suffixes to penalize for natural features
# BAD_POI_SUFFIX = re.compile(r"(view\s*point|parking|ticket|counter|canteen|shop|toilet|entrance|gate)", re.I)

# # ----------------------------- Utilities ------------------------------

# def strip_accents(s: str) -> str:
#     return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

# def normalize_name(s: str) -> str:
#     s = s or ""
#     s = strip_accents(s).strip()
#     return re.sub(r"\s+", " ", s)

# def split_name_hint(raw: str) -> Tuple[str, str]:
#     s = normalize_name(raw)
#     s = re.sub(r"\s*\(.*?\)\s*$", "", s)
#     parts = [p.strip() for p in re.split(r",|–|-|—", s) if p.strip()]
#     name = parts[0] if parts else s
#     hint = parts[1] if len(parts) > 1 else ""
#     hint = ALIAS_MAP.get(hint.lower(), hint)
#     return name, hint

# def infer_entity_kind_from_category(category_hint: str, scope: str) -> str:
#     """Infer entity kind from Step 2's category_hint"""
#     if not category_hint:
#         return "destination" if scope == "destination" else "tourist_attraction"
    
#     # Direct mapping from Step 2 categories
#     category_to_kind = {
#         "waterfall": "waterfall",
#         "beach": "beach",
#         "island": "island", 
#         "lake": "lake",
#         "peak": "peak",
#         "mountain": "peak",
#         "national_park": "national_park",
#         "sanctuary": "sanctuary",
#         "reserve": "reserve",
#         "park": "park",
#         "temple": "temple",
#         "fort": "fort",
#         "cave": "cave",
#         "trek": "trek",
#         "trail": "trail",
#         "resort": "resort",
#         "hotel": "hotel",
#         "camp": "campground",
#         "homestay": "homestay",
#         "villa": "villa",
#         "lodge": "lodge",
#         "hostel": "hostel",
#         "hill_station": "hill_station",
#         "town": "destination",
#         "city": "destination",
#         "district": "destination",
#         "region": "destination",
#     }
    
#     return category_to_kind.get(category_hint.lower(), "destination")

# def allowed_types_for_kind(kind: str) -> set:
#     """Get allowed Google Place types for an entity kind"""
#     type_mapping = {
#         # Natural features
#         "waterfall": {"natural_feature", "tourist_attraction", "point_of_interest"},
#         "beach": {"natural_feature", "tourist_attraction", "point_of_interest"}, 
#         "island": {"natural_feature", "tourist_attraction", "point_of_interest"},
#         "lake": {"natural_feature", "tourist_attraction", "point_of_interest"},
#         "peak": {"natural_feature", "tourist_attraction", "point_of_interest"},
#         "national_park": {"park", "tourist_attraction", "point_of_interest"},
#         "sanctuary": {"park", "tourist_attraction", "point_of_interest"},
#         "reserve": {"park", "tourist_attraction", "point_of_interest"},
#         "park": {"park", "tourist_attraction", "point_of_interest"},
#         "trek": {"natural_feature", "tourist_attraction", "point_of_interest"},
#         "trail": {"natural_feature", "tourist_attraction", "point_of_interest"},
#         "cave": {"natural_feature", "tourist_attraction", "point_of_interest"},
#         # POIs
#         "temple": {"hindu_temple", "tourist_attraction", "point_of_interest", "place_of_worship"},
#         "fort": {"tourist_attraction", "point_of_interest", "museum"},
#         "resort": {"lodging", "tourist_attraction"},
#         "hotel": {"lodging"},
#         "homestay": {"lodging"},
#         "villa": {"lodging"},
#         "lodge": {"lodging"}, 
#         "hostel": {"lodging"},
#         "campground": {"campground", "lodging"},
#         # Destinations
#         "destination": {"locality", "sublocality", "administrative_area_level_3", "administrative_area_level_2", "political"},
#         "hill_station": {"locality", "tourist_attraction", "point_of_interest"},
#     }
    
#     return type_mapping.get(kind.lower(), {"tourist_attraction", "point_of_interest", "locality"})

# def primary_google_type_for_kind(kind: str) -> Optional[str]:
#     """Get primary Google type for Places API type parameter"""
#     k = kind.lower()
#     if k in {"resort", "hotel", "villa", "homestay", "lodge", "hostel", "campground"}:
#         return "lodging"
#     elif k in {"park", "national_park", "sanctuary", "reserve"}:
#         return "park"
#     elif k in {"temple"}:
#         return "hindu_temple"
#     else:
#         return "tourist_attraction"

# # -------------------------- Distance & Scores -------------------------

# def parse_hours(s: str) -> Optional[float]:
#     if not s: return None
#     m = re.search(r"(\d+(?:\.\d+)?)\s*hour", s, re.I)
#     return float(m.group(1)) if m else None

# def km_from_hours(h: Optional[float], default_speed_kmph: int = 70, buffer_km: int = 50) -> int:
#     if h is None: return 450
#     return int(max(80, min(800, h * default_speed_kmph + buffer_km)))

# def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
#     R = 6371.0
#     dLat = math.radians(lat2 - lat1)
#     dLon = math.radians(lon2 - lon1)
#     a = math.sin(dLat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2
#     return 2*R*math.asin(min(1, math.sqrt(a)))

# def distance_score(cand: Dict[str, Any], anchor_lat: Optional[float], anchor_lng: Optional[float], radius_m: int) -> float:
#     if anchor_lat is None or anchor_lng is None: return 0.0
#     if cand.get("lat") is None or cand.get("lng") is None: return 0.0
#     dkm = haversine_km(anchor_lat, anchor_lng, cand["lat"], cand["lng"])
#     rkm = max(1, radius_m/1000)
#     # Gentler distance scoring - full score inside half-radius, gradual decay
#     if dkm <= 0.4*rkm: return 1.0
#     if dkm >= 2.0*rkm: return 0.0
#     return max(0.0, 1.0 - ((dkm - 0.4*rkm) / (1.6*rkm)))

# def popularity_score(rating: float, reviews: int) -> float:
#     rating = rating or 0.0
#     reviews = max(0, int(reviews or 0))
#     return min(1.0, rating * math.log10(reviews + 1) / 5.0)

# def type_compat_score(types: List[str], allowed: set) -> float:
#     return 1.0 if set(types or []) & allowed else (0.5 if types else 0.0)

# def state_match_score(address: str, expected_state: Optional[str]) -> float:
#     if not address or not expected_state: return 0.0
#     return 1.0 if expected_state.lower() in address.lower() else 0.0

# # ---------------------------- Query Builder ---------------------------

# def expand_name_variants(name: str) -> List[str]:
#     variants = {normalize_name(name)}
#     for pat, rep in NAME_ALIASES:
#         if pat.search(name):
#             variants.add(pat.sub(rep, name))
#     return list(variants)

# def build_enhanced_queries(name: str, category_hint: str, scope: str, 
#                           anchor_city: Optional[str], anchor_state: Optional[str]) -> List[str]:
#     """Build enhanced search queries using Step 2 data"""
#     name_qs = expand_name_variants(name)
#     queries: List[str] = []

#     # Landmark-state boost if known (highest priority)
#     low = name.lower()
#     for landmark, state in FAMOUS_LANDMARK_STATES.items():
#         if landmark in low:
#             for nq in name_qs:
#                 queries.append(f"{nq} {state} india")
#             break

#     # Category-specific queries  
#     cat_word = category_hint.replace("_", " ") if category_hint else ""
    
#     for nq in name_qs:
#         # Base name query
#         queries.append(nq)
        
#         # State context
#         if anchor_state:
#             queries.append(f"{nq} {anchor_state}")
#             queries.append(f"{nq} {anchor_state} india")
            
#         # City context
#         if anchor_city:
#             queries.append(f"{nq} {anchor_city}")
#             queries.append(f"{nq} near {anchor_city}")
            
#         # Category enhancement
#         if cat_word and cat_word not in nq.lower():
#             queries.append(f"{nq} {cat_word}")
#             if anchor_state:
#                 queries.append(f"{nq} {cat_word} {anchor_state}")
#             if anchor_city:
#                 queries.append(f"{nq} {cat_word} {anchor_city}")
        
#         # Scope-specific enhancements
#         if scope == "destination":
#             # For destinations, try generic location terms
#             queries.extend([
#                 f"{nq} town", f"{nq} city", f"{nq} place"
#             ])
#         elif scope == "natural":
#             # For natural features, avoid POI-like terms
#             if "waterfall" in (category_hint or "").lower():
#                 queries.append(f"{nq} waterfall")

#     # Dedup while preserving order
#     seen = set()
#     unique_queries = []
#     for q in queries:
#         qn = q.strip().lower()
#         if qn and qn not in seen and len(qn) > 2:
#             seen.add(qn)
#             unique_queries.append(q.strip())
    
#     return unique_queries[:15]  # Limit to prevent too many API calls

# # ------------------------------ Biasing -------------------------------

# def circle_bias(lat: float, lng: float, radius_m: int) -> str:
#     return f"circle:{max(1000, int(radius_m))}@{lat},{lng}"

# # ----------------------------- Resolver --------------------------------

# class EnhancedResolver:
#     def __init__(self, api_key: Optional[str], cache_path: Path):
#         self.enabled = bool(api_key and googlemaps)
#         self.cache_path = cache_path
#         self.cache: Dict[str, Any] = {}
#         if cache_path.exists():
#             try:
#                 self.cache = json.loads(cache_path.read_text(encoding="utf-8"))
#             except Exception:
#                 self.cache = {}
#         self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None

#     def save(self):
#         try:
#             self.cache_path.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass

#     def cache_key(self, name: str, category_hint: str, scope: str, 
#                   anchor_city: Optional[str], anchor_state: Optional[str]) -> str:
#         return "|".join([
#             normalize_name(name).lower(),
#             (category_hint or "").lower(),
#             scope.lower(),
#             normalize_name(anchor_city or "").lower(),
#             normalize_name(anchor_state or "").lower()
#         ])

#     def geocode_anchor(self, text: str) -> Tuple[Optional[float], Optional[float]]:
#         key = (text or "").strip().lower()
#         if key in CITY_COORDS:
#             return CITY_COORDS[key]
            
#         # Check geocode cache
#         if GEOCODE_CACHE.exists():
#             try:
#                 geo_cache = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
#                 if key in geo_cache:
#                     d = geo_cache[key]
#                     return d.get("lat"), d.get("lng")
#             except Exception:
#                 pass
                
#         if not self.gmaps:
#             return None, None
            
#         lat = lng = None
#         try:
#             res = self.gmaps.geocode(f"{text}, India", language=GMAPS_LANGUAGE)
#             if res:
#                 loc = res[0]["geometry"]["location"]
#                 lat, lng = loc["lat"], loc["lng"]
#         except Exception:
#             pass
            
#         # Cache result
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
#                 anchor_city: Optional[str], anchor_state: Optional[str], 
#                 radius_m: int) -> Dict[str, Any]:
        
#         empty = {
#             "place_id": None, "name": name, "lat": None, "lng": None, "types": [],
#             "rating": 0.0, "reviews": 0, "address": None, "website": None, "phone": None,
#             "opening": [], "price_level": None, "permanently_closed": False,
#             "photo_refs": [], "utc_offset_minutes": None, "confidence": 0.0
#         }

#         key = self.cache_key(name, category_hint, scope, anchor_city, anchor_state)
#         if key in self.cache:
#             return {**empty, **self.cache[key]}
            
#         if not self.enabled:
#             self.cache[key] = empty
#             return empty

#         # Get anchor coordinates
#         a_lat = a_lng = None
#         if anchor_city:
#             a_lat, a_lng = self.geocode_anchor(anchor_city)
#         if (a_lat is None or a_lng is None) and anchor_state:
#             a_lat, a_lng = self.geocode_anchor(anchor_state)

#         # Infer entity kind and allowed types
#         entity_kind = infer_entity_kind_from_category(category_hint, scope)
#         allowed_kind_types = allowed_types_for_kind(entity_kind)
#         allowed_scope_types = SCOPE_TYPES.get(scope, set())
#         primary_type = primary_google_type_for_kind(entity_kind)

#         # Build enhanced queries
#         queries = build_enhanced_queries(name, category_hint, scope, anchor_city, anchor_state)
        
#         best = empty
#         best_score = -1.0
        
#         bias = circle_bias(a_lat, a_lng, radius_m) if (a_lat and a_lng) else None
        
#         print(f"Resolving: {name} (category: {category_hint}, scope: {scope})")
#         print(f"  Queries: {queries[:3]}...")
        
#         # Pre-seed with geocode results for destination scope
#         all_candidates: List[Dict[str, Any]] = []
#         if scope == "destination" and self.gmaps:
#             try:
#                 components = {"country": "IN"}
#                 if anchor_state:
#                     components["administrative_area"] = anchor_state
#                 geo_query = f"{name}, {anchor_state or 'India'}"
#                 geo_results = self.gmaps.geocode(geo_query, components=components, language=GMAPS_LANGUAGE)
                
#                 for g in (geo_results or [])[:2]:
#                     loc = g.get("geometry", {}).get("location", {})
#                     all_candidates.append({
#                         "place_id": g.get("place_id"),
#                         "name": g.get("formatted_address", name).split(",")[0],  # Take first part
#                         "geometry": {"location": loc},
#                         "types": g.get("types", []),
#                         "formatted_address": g.get("formatted_address", ""),
#                         "rating": 0, "user_ratings_total": 0,
#                         "_source": "geocode"
#                     })
#             except Exception:
#                 pass

#         # Search with multiple query variants
#         for query in queries[:8]:  # Limit API calls
#             try:
#                 # Find Place API
#                 try:
#                     fp_kwargs = {
#                         "input": query,
#                         "input_type": "textquery",
#                         "fields": ["place_id", "name", "geometry/location", "types", 
#                                   "formatted_address", "rating", "user_ratings_total","photos"],
#                         "language": GMAPS_LANGUAGE
#                     }
#                     if bias:
#                         fp_kwargs["location_bias"] = bias
                        
#                     fp_result = self.gmaps.find_place(**fp_kwargs)
#                     candidates = fp_result.get("candidates", [])
#                     for c in candidates:
#                         c["_source"] = "find_place"
#                     all_candidates.extend(candidates)
#                 except Exception:
#                     pass

#                 # Text Search API  
#                 try:
#                     ts_kwargs = {
#                         "query": query,
#                         "region": GMAPS_REGION,
#                         "language": GMAPS_LANGUAGE
#                     }
#                     if a_lat and a_lng:
#                         ts_kwargs.update({
#                             "location": (a_lat, a_lng),
#                             "radius": min(500000, max(20000, radius_m))
#                         })
#                     if primary_type:
#                         ts_kwargs["type"] = primary_type
                        
#                     ts_result = self.gmaps.places(**ts_kwargs)
#                     candidates = ts_result.get("results", [])
#                     for c in candidates:
#                         c["_source"] = "text_search"
#                     all_candidates.extend(candidates)
#                 except Exception:
#                     pass
                    
#                 time.sleep(SLEEP_BETWEEN)
                
#             except Exception:
#                 time.sleep(SLEEP_BETWEEN)
#                 continue

#         # Score all candidates
#         for candidate in all_candidates[:15]:  # Evaluate top candidates
#             loc = (candidate.get("geometry") or {}).get("location") or {}
#             cand_data = {
#                 "place_id": candidate.get("place_id"),
#                 "name": candidate.get("name", name),
#                 "lat": loc.get("lat"),
#                 "lng": loc.get("lng"),
#                 "types": candidate.get("types", []),
#                 "address": candidate.get("formatted_address", ""),
#                 "rating": float(candidate.get("rating", 0)),
#                 "reviews": int(candidate.get("user_ratings_total", 0)),
#             }

#             # Enhanced scoring components
#             sim = name_sim(name, cand_data["name"]) / 100.0
#             kind_compat = type_compat_score(cand_data["types"], allowed_kind_types)
#             scope_compat = 1.0 if (set(cand_data["types"]) & allowed_scope_types) else 0.3
#             pop = popularity_score(cand_data["rating"], cand_data["reviews"])
#             dist = distance_score(cand_data, a_lat, a_lng, radius_m)
#             state_match = state_match_score(cand_data["address"], anchor_state)

#             # Scope-specific penalties and boosts
#             scope_penalty = 0.0
            
#             if scope == "destination":
#                 # Penalize POI-like results for destinations
#                 if set(cand_data["types"]) & {"tourist_attraction", "lodging", "restaurant", "store"}:
#                     scope_penalty -= 0.2
#                 # Boost geocode results for destinations
#                 if candidate.get("_source") == "geocode":
#                     scope_penalty += 0.1
                    
#             elif scope == "natural":
#                 # Penalize viewpoints, parking for natural features
#                 if BAD_POI_SUFFIX.search(cand_data["name"]):
#                     scope_penalty -= 0.3
#                 # Penalize cities/localities for natural features
#                 if set(cand_data["types"]) & {"locality", "administrative_area_level_2"}:
#                     scope_penalty -= 0.2
                    
#             elif scope == "poi":
#                 # Penalize administrative areas for POIs
#                 if set(cand_data["types"]) & {"locality", "administrative_area_level_1", "administrative_area_level_2"}:
#                     scope_penalty -= 0.3

#             # Landmark-state validation
#             landmark_bonus = 0.0
#             landmark_state = None
#             for landmark, state in FAMOUS_LANDMARK_STATES.items():
#                 if landmark in name.lower():
#                     landmark_state = state
#                     break
                    
#             if landmark_state:
#                 if landmark_state in cand_data["address"].lower():
#                     landmark_bonus = 0.15
#                 else:
#                     # Penalty for wrong state
#                     wrong_states = ["kerala", "tamil nadu", "andhra pradesh", "telangana", "odisha"]
#                     for wrong_state in wrong_states:
#                         if wrong_state in cand_data["address"].lower() and wrong_state != landmark_state:
#                             landmark_bonus = -0.2
#                             break

#             # Final weighted score
#             raw_score = (0.30 * sim + 
#                         0.20 * kind_compat + 
#                         0.15 * scope_compat +
#                         0.15 * dist + 
#                         0.10 * pop + 
#                         0.10 * state_match +
#                         scope_penalty + 
#                         landmark_bonus)
            
#             # Clamp to [0,1] range
#             score = max(0.0, min(1.0, raw_score))

#             if score > best_score:
#                 best_score = score
#                 best = {**empty, **cand_data, "confidence": round(score, 3)}

#         print(f"  Best: {best.get('name')} (confidence: {best_score:.3f})")

#         # Get detailed information for best candidate
#         if best.get("place_id") and best_score >= 0.5:
#             try:
#                 details = self.gmaps.place(
#                     place_id=best["place_id"],
#                     fields=[
#                         "place_id", "name", "geometry/location", "types", "formatted_address",
#                         "website", "international_phone_number", "opening_hours", "price_level",
#                         "permanently_closed", "rating", "user_ratings_total",
#                         "photos", "utc_offset_minutes"
#                     ],
#                     language=GMAPS_LANGUAGE
#                 ).get("result", {})

#                 if details:
#                     print(f"    Debug - Details keys: {list(details.keys())}")
#                     photos = details.get("photos", [])
#                     print(f"    Debug - Photos found: {len(photos)}")
#                     if photos:
#                         print(f"    Debug - First photo structure: {photos[0]}")
#                     else:
#                         print(f"    Debug - No photos for {details.get('name')} (types: {details.get('types')})")
        
#                     photo_refs = [p.get("photo_reference") for p in photos[:10] if p.get("photo_reference")]
#                     print(f"    Debug - Extracted photo_refs: {len(photo_refs)}")
#                     loc = details.get("geometry", {}).get("location", {})
#                     # photos = details.get("photos", [])
#                     # photo_refs = [p.get("photo_reference") for p in photos[:10] if p.get("photo_reference")]

#                     best.update({
#                         "name": details.get("name", best["name"]),
#                         "lat": loc.get("lat", best["lat"]),
#                         "lng": loc.get("lng", best["lng"]),
#                         "types": details.get("types", best["types"]),
#                         "address": details.get("formatted_address", best["address"]),
#                         "website": details.get("website"),
#                         "phone": details.get("international_phone_number"),
#                         "opening": (details.get("opening_hours", {}) or {}).get("periods", []),
#                         "price_level": details.get("price_level"),
#                         "permanently_closed": details.get("permanently_closed", False),
#                         "rating": float(details.get("rating", best["rating"])),
#                         "reviews": int(details.get("user_ratings_total", best["reviews"])),
#                         "photo_refs": photo_refs,
#                         "utc_offset_minutes": details.get("utc_offset_minutes"),
#                     })
                    
#                 time.sleep(SLEEP_BETWEEN)
#             except Exception:
#                 pass

#         # Cache and return
#         self.cache[key] = best
#         return best

# # ------------------------------- Main ---------------------------------

# def is_publishable(resolved: Dict[str, Any], entity_kind: str) -> bool:
#     """Enhanced publishability check"""
#     pid = resolved.get("place_id")
#     lat, lng = resolved.get("lat"), resolved.get("lng")
#     if not (pid and lat is not None and lng is not None):
#         return False
        
#     if REQUIRE_PHOTO and not resolved.get("photo_refs"):
#         return False
        
#     rating = float(resolved.get("rating", 0))
#     reviews = int(resolved.get("reviews", 0))
#     has_photo = bool(resolved.get("photo_refs"))
    
#     # Basic quality thresholds
#     if not ((rating >= MIN_RATING) or (reviews >= MIN_REVIEWS) or has_photo):
#         return False
    
#     # Type compatibility check
#     allowed = allowed_types_for_kind(entity_kind)
#     if not (set(resolved.get("types", [])) & allowed):
#         # Allow if very popular despite type mismatch
#         if rating >= MIN_RATING and reviews >= MIN_REVIEWS:
#             return True
#         return False
    
#     return True

# def main():
#     global MIN_CONFIDENCE, RETRY_ATTEMPTS, REQUIRE_PHOTO

#     parser = argparse.ArgumentParser(
#         description="Step 2.5 — Enhanced Google Places Resolver (Final Version)",
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#         epilog="""
# EXAMPLES:
#   python step2_5_resolve.py                              # Use scope from Step 2 data
#   python step2_5_resolve.py --scope destination          # Override all to destination
#   python step2_5_resolve.py --anchor-city "Bengaluru" --anchor-state "Karnataka"
#   python step2_5_resolve.py --min-confidence 0.8         # Higher confidence threshold
#   python step2_5_resolve.py --require-photo             # Require photos for publishable

# SETUP:
#   1. Get Google Maps API key from Google Cloud Console
#   2. Enable Places API (Find Place, Place Details, Text Search)  
#   3. Set environment variable: export GOOGLE_MAPS_API_KEY="your_key"
#         """
#     )
    
#     parser.add_argument("--in", dest="in_path", default=str(IN_PATH))
#     parser.add_argument("--out", dest="out_path", default=str(OUT_PATH))
#     parser.add_argument("--report", dest="report_path", default=str(REPORT))
#     parser.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE)
#     parser.add_argument("--retry", type=int, default=RETRY_ATTEMPTS)
#     parser.add_argument("--require-photo", action="store_true", default=REQUIRE_PHOTO)
    
#     # Enhanced arguments
#     parser.add_argument("--scope", choices=["auto", "destination", "poi", "natural"], default="auto",
#                        help="Override scope for all items (auto uses Step 2 data)")
#     parser.add_argument("--anchor-city", type=str, default=HARDCODE_ANCHOR_CITY)
#     parser.add_argument("--anchor-state", type=str, default=HARDCODE_ANCHOR_STATE)
#     parser.add_argument("--radius-km", type=int, default=None, 
#                        help="Override radius calculation (km)")
#     parser.add_argument("--default-speed", type=int, default=70, 
#                        help="Travel speed for radius calc (km/h)")
    
#     args = parser.parse_args()

#     MIN_CONFIDENCE = args.min_confidence
#     RETRY_ATTEMPTS = args.retry
#     REQUIRE_PHOTO = args.require_photo

#     in_path = Path(args.in_path)
#     out_path = Path(args.out_path)
#     report_path = Path(args.report_path)

#     if not in_path.exists():
#         print(f"Error: Input file not found: {in_path}")
#         return

#     api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
#     if not api_key:
#         print("Error: GOOGLE_MAPS_API_KEY environment variable not set")
#         return
        
#     resolver = EnhancedResolver(api_key, PLACES_CACHE)

#     try:
#         data = json.loads(in_path.read_text(encoding="utf-8"))
#     except Exception as e:
#         print(f"Error reading input file: {e}")
#         return
        
#     if not isinstance(data, list):
#         print("Error: Input must be a JSON array of playlists")
#         return

#     totals = {"items": 0, "resolved": 0, "publishable": 0, "partial": 0, "unresolved": 0}
#     per_pl = []

#     print(f"Processing {len(data)} playlists with enhanced scope-aware resolution...")

#     out_playlists = []
#     for plist_idx, plist in enumerate(data):
#         title = plist.get("playlistTitle", f"Playlist {plist_idx + 1}")
#         subtype = str(plist.get("subtype", "destination")).lower().strip()
#         items = plist.get("items", [])

#         # Determine anchor
#         anchor_city = args.anchor_city or plist.get("placeName") or plist.get("anchor_city")
#         if anchor_city:
#             # Apply alias mapping
#             anchor_city = ALIAS_MAP.get(anchor_city.lower(), anchor_city)
            
#         anchor_state = args.anchor_state
#         if not anchor_state and anchor_city:
#             # Infer state from city
#             city_to_state = {
#                 "bengaluru": "karnataka", "bangalore": "karnataka",
#                 "mysore": "karnataka", "mysuru": "karnataka",
#                 "chennai": "tamil nadu", "hyderabad": "telangana",
#                 "kochi": "kerala", "mumbai": "maharashtra",
#                 "pune": "maharashtra", "delhi": "delhi",
#                 "kolkata": "west bengal", "thiruvananthapuram": "kerala",
#             }
#             anchor_state = city_to_state.get(anchor_city.lower())

#         resolved_items = []
#         stats = {"title": title, "publishable": 0, "partial": 0, "unresolved": 0}

#         print(f"\nProcessing playlist: {title}")
#         print(f"  Anchor: {anchor_city}, {anchor_state}")
#         print(f"  Items: {len(items)}")

#         for item_idx, item in enumerate(items):
#             totals["items"] += 1
#             src_name = (item.get("name", "")).strip()
#             if not src_name:
#                 print(f"  Skipping empty item {item_idx + 1}")
#                 totals["unresolved"] += 1
#                 stats["unresolved"] += 1
#                 continue

#             # Get enhanced data from Step 2
#             category_hint = item.get("category_hint", "")
#             step2_scope = item.get("scope", "")
            
#             # Determine final scope
#             final_scope = args.scope if args.scope != "auto" else (step2_scope or "destination")
            
#             # Calculate search radius
#             hours = parse_hours(item.get("travel_time", ""))
#             radius_km = args.radius_km or km_from_hours(hours, args.default_speed)
#             radius_m = int(radius_km * 1000)

#             print(f"\n  [{item_idx + 1}] {src_name}")
#             print(f"    Category: {category_hint}, Scope: {final_scope}")
#             print(f"    Radius: {radius_km}km")

#             # Resolve with enhanced parameters
#             try:
#                 result = resolver.resolve(
#                     name=src_name,
#                     category_hint=category_hint,
#                     scope=final_scope,
#                     anchor_city=anchor_city,
#                     anchor_state=anchor_state,
#                     radius_m=radius_m
#                 )
#             except Exception as e:
#                 print(f"    Error resolving: {e}")
#                 result = {"place_id": None, "name": src_name, "confidence": 0.0}

#             # Build final item
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
#                 # Carry forward from Step 2
#                 "description": item.get("description", ""),
#                 "travel_time": item.get("travel_time", ""),
#                 "price": item.get("price", ""),
#                 "votes": item.get("votes", 1),
#                 "source_urls": item.get("source_urls", []),
#             }

#             # Determine status
#             if result.get("place_id"):
#                 totals["resolved"] += 1
#                 if is_publishable(result, entity_kind):
#                     final_item["resolution_status"] = "publishable"
#                     totals["publishable"] += 1
#                     stats["publishable"] += 1
#                     print(f"    → ✅ PUBLISHABLE: {result.get('name')} (confidence: {result.get('confidence')})")
#                 else:
#                     final_item["resolution_status"] = "partial" 
#                     totals["partial"] += 1
#                     stats["partial"] += 1
#                     print(f"    → ⚠️ PARTIAL: {result.get('name')} (confidence: {result.get('confidence')})")
#             else:
#                 final_item["resolution_status"] = "unresolved"
#                 totals["unresolved"] += 1
#                 stats["unresolved"] += 1
#                 print(f"    → ❌ UNRESOLVED: {src_name}")

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
#     try:
#         out_path.write_text(json.dumps(out_playlists, ensure_ascii=False, indent=2), encoding="utf-8")
#         print(f"\n✅ Wrote resolved data to: {out_path}")
#     except Exception as e:
#         print(f"Error writing output: {e}")
#         return

#     # Generate report
#     report_data = {
#         "summary": {
#             "total_playlists": len(out_playlists),
#             "total_items": totals["items"],
#             "success_rate": round((totals["publishable"] + totals["partial"]) / max(1, totals["items"]) * 100, 1),
#             "publishable_rate": round(totals["publishable"] / max(1, totals["items"]) * 100, 1),
#         },
#         "totals": totals,
#         "thresholds": {
#             "min_confidence": MIN_CONFIDENCE,
#             "min_reviews": MIN_REVIEWS,
#             "min_rating": MIN_RATING,
#             "require_photo": REQUIRE_PHOTO,
#         },
#         "config": {
#             "anchor_city": anchor_city,
#             "anchor_state": anchor_state,
#             "language": GMAPS_LANGUAGE,
#             "region": GMAPS_REGION,
#         },
#         "playlists": per_pl
#     }
    
#     try:
#         report_path.write_text(json.dumps(report_data, ensure_ascii=False, indent=2), encoding="utf-8")
#         print(f"📊 Wrote report to: {report_path}")
#     except Exception as e:
#         print(f"Warning: Could not write report: {e}")

#     # Save cache
#     resolver.save()
#     print(f"💾 Updated cache: {PLACES_CACHE}")

#     # Print summary
#     print(f"\n{'='*60}")
#     print(f"🎯 RESOLUTION SUMMARY")
#     print(f"{'='*60}")
#     print(f"Total Items:      {totals['items']}")
#     print(f"Publishable:      {totals['publishable']} ({totals['publishable']/max(1,totals['items'])*100:.1f}%)")
#     print(f"Partial:          {totals['partial']} ({totals['partial']/max(1,totals['items'])*100:.1f}%)")
#     print(f"Unresolved:       {totals['unresolved']} ({totals['unresolved']/max(1,totals['items'])*100:.1f}%)")
#     print(f"Overall Success:  {(totals['publishable'] + totals['partial'])/max(1,totals['items'])*100:.1f}%")
    
#     print(f"\n🔧 Key Enhancements:")
#     print(f"• Scope-aware resolution (destination/poi/natural)")
#     print(f"• Enhanced query building with category hints")
#     print(f"• Landmark-state validation for accuracy")
#     print(f"• Dynamic radius calculation from travel times")
#     print(f"• Geocoding pre-pass for destination scope")

# if __name__ == "__main__":
#     main()













# """
# Step 2.5 — Resolve & Validate (Google Places) — NOT MOST ACCURATE VERSION

# Reads playlist_items.json (Step 2) → resolves items to Google Places → writes:
#   - playlist_items_resolved.json
#   - resolve_report.json

# Uses cache/places_cache.json (and geocode_cache.json) to avoid duplicate calls.

# Key improvements over previous versions:
# - Circle bias around an **anchor city** (configurable or hardcoded), with radius derived from travel_time
# - Optional **hardcoded anchor state/city** overrides (no geocoding required)
# - Landmark/state-aware query building (e.g., "Jog Falls Karnataka India")
# - Correct entity kind normalization (e.g., "falls" → "waterfall")
# - Combined scoring: name similarity + type + popularity + distance-to-anchor + state match + hint-in-address
# - Dual API strategy (Find Place + Text Search) with `language="en-IN"` and `region="in"`
# - Clean caching and idempotent behavior

# CLI examples:
#   python step2_5_resolve.py --anchor-city "Bengaluru" --anchor-state "Karnataka" --radius-km 450
#   python step2_5_resolve.py --min-confidence 0.7 --retry 3

# """

# from __future__ import annotations
# import os, json, re, time, math, argparse, unicodedata
# from pathlib import Path
# from typing import List, Dict, Any, Optional, Tuple

# # Optional .env
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except Exception:
#     pass

# # External deps
# try:
#     import googlemaps  # pip install googlemaps
# except Exception:
#     googlemaps = None

# try:
#     from rapidfuzz import fuzz  # pip install rapidfuzz
#     def name_sim(a: str, b: str) -> int:
#         return fuzz.token_set_ratio(a, b)
# except Exception:
#     def _norm_tokens(s: str) -> set:
#         return set(re.findall(r"[a-z0-9]+", s.lower()))
#     def name_sim(a: str, b: str) -> int:
#         ta, tb = _norm_tokens(a), _norm_tokens(b)
#         if not ta or not tb: return 0
#         return int(100 * len(ta & tb) / len(ta | tb))

# # ------------------------------- CONFIG -------------------------------

# BASE_DIR = Path(__file__).resolve().parent

# IN_PATH   = BASE_DIR / "playlist_items.json"
# OUT_PATH  = BASE_DIR / "playlist_items_resolved.json"
# REPORT    = BASE_DIR / "resolve_report.json"

# CACHE_DIR = BASE_DIR / "cache"
# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# PLACES_CACHE = CACHE_DIR / "places_cache.json"
# GEOCODE_CACHE = CACHE_DIR / "geocode_cache.json"

# # Acceptance / scoring
# MIN_CONFIDENCE = 0.75           # accept candidate ≥ 0.75
# RETRY_ATTEMPTS = 2              # additional variants beyond the first
# MIN_RATING     = 3.5            # or reviews >= MIN_REVIEWS or has photo
# MIN_REVIEWS    = 20
# REQUIRE_PHOTO  = False          # set True to require ≥1 photo for publishable

# # API tuning
# GMAPS_REGION   = "in"
# GMAPS_LANGUAGE = "en-IN"
# SLEEP_BETWEEN  = 0.08           # gentle pacing for Places/Details

# # Hardcoded anchor overrides (optional)
# # You can set these at the top OR pass via CLI flags --anchor-city / --anchor-state.
# HARDCODE_ANCHOR_CITY  = None  # e.g., "Bengaluru"
# HARDCODE_ANCHOR_STATE = None  # e.g., "Karnataka"

# # Known coordinates for quick lookup (expand as needed)
# CITY_COORDS: Dict[str, Tuple[float, float]] = {
#     "bengaluru": (12.9716, 77.5946),
#     "bangalore": (12.9716, 77.5946),  # alias
#     "mumbai": (19.0760, 72.8777),
#     "delhi": (28.7041, 77.1025),
#     "chennai": (13.0827, 80.2707),
#     "kolkata": (22.5726, 88.3639),
#     "hyderabad": (17.3850, 78.4867),
#     "kochi": (9.9312, 76.2673),
#     "pune": (18.5204, 73.8567),
# }

# # --------------------------- Heuristics/Maps ---------------------------

# ALIAS_MAP = {
#     # hint/city/state aliases → canonical
#     "alleppey": "alappuzha",
#     "pondicherry": "puducherry",
#     "bombay": "mumbai",
#     "calcutta": "kolkata",
#     "rishikonda": "rushikonda",
#     "havelock": "swaraj dweep",
# }

# # Name-level aliases (search expansions)
# NAME_ALIASES = [
#     (re.compile(r"\bchikmagalur(u)?\b", re.I), "chikkamagaluru"),
#     (re.compile(r"\bcoorg\b", re.I), "kodagu"),
#     (re.compile(r"\bcalicut\b", re.I), "kozhikode"),
#     (re.compile(r"\bbangalore\b", re.I), "bengaluru"),
#     (re.compile(r"\booty\b", re.I), "udhagamandalam"),
# ]

# # Famous landmark → correct state (helps kill wrong-state doppelgangers)
# FAMOUS_LANDMARK_STATES = {
#     "jog falls": "karnataka",
#     "mysore": "karnataka",
#     "mysore palace": "karnataka",
#     "hampi": "karnataka",
#     "coorg": "karnataka",
#     "kodagu": "karnataka",
#     "chikmagalur": "karnataka",
#     "chikkamagaluru": "karnataka",
#     "gokarna": "karnataka",
#     "dandeli": "karnataka",
#     "badami": "karnataka",
#     "kudremukh": "karnataka",
#     "lepakshi": "andhra pradesh",
#     "horsley hills": "andhra pradesh",
#     "wayanad": "kerala",
#     "kozhikode": "kerala",
#     "alappuzha": "kerala",
# }

# DESTINATION_HINTS = re.compile(
#     r"\b(beach|island|bay|cove|lagoon|coast(line)?|shore|dunes?|desert|"
#     r"national\s+park|tiger\s+reserve|sanctuary|biosphere|rainforest|"
#     r"lake|falls?|waterfall|valley|pass|peak|summit|cave|gorge|"
#     r"trail|trek|meadow|plateau)\b",
#     re.I,
# )

# POI_HINTS = re.compile(
#     r"\b(resort|hotel|villa|homestay|guest\s?house|hostel|lodge|camp|campground|"
#     r"glamp(ing)?|spa|retreat|bungalow|treehouse|houseboat|beach\s?club)\b",
#     re.I,
# )

# ALLOWED_TYPES_BY_KIND = {
#     # destinations
#     "waterfall": {"natural_feature", "tourist_attraction", "point_of_interest"},
#     "lake": {"natural_feature", "tourist_attraction", "point_of_interest"},
#     "island": {"natural_feature", "tourist_attraction", "point_of_interest"},
#     "park": {"park", "tourist_attraction", "point_of_interest"},
#     "national_park": {"park", "tourist_attraction", "point_of_interest"},
#     "sanctuary": {"park", "tourist_attraction", "point_of_interest"},
#     "reserve": {"park", "tourist_attraction", "point_of_interest"},
#     "valley": {"natural_feature", "tourist_attraction", "point_of_interest"},
#     "peak": {"natural_feature", "tourist_attraction", "point_of_interest"},
#     "pass": {"natural_feature", "tourist_attraction", "point_of_interest"},
#     "trek": {"natural_feature", "tourist_attraction", "point_of_interest"},
#     "trail": {"natural_feature", "tourist_attraction", "point_of_interest"},
#     "cave": {"natural_feature", "tourist_attraction", "point_of_interest"},
#     # lodging
#     "resort": {"lodging"},
#     "hotel": {"lodging"},
#     "villa": {"lodging"},
#     "homestay": {"lodging"},
#     "lodge": {"lodging"},
#     "camp": {"campground", "lodging"},
#     "hostel": {"lodging"},
#     "guesthouse": {"lodging"},
#     "beach_club": {"bar", "restaurant", "tourist_attraction", "point_of_interest"},
# }

# # ----------------------------- Utilities ------------------------------

# def strip_accents(s: str) -> str:
#     return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

# def normalize_name(s: str) -> str:
#     s = s or ""
#     s = strip_accents(s).strip()
#     return re.sub(r"\s+", " ", s)

# def split_name_hint(raw: str) -> Tuple[str, str]:
#     s = normalize_name(raw)
#     s = re.sub(r"\s*\(.*?\)\s*$", "", s)
#     parts = [p.strip() for p in re.split(r",|–|-|—", s) if p.strip()]
#     name = parts[0] if parts else s
#     hint = parts[1] if len(parts) > 1 else ""
#     hint = ALIAS_MAP.get(hint.lower(), hint)
#     return name, hint

# def infer_entity_kind(name: str, playlist_subtype: str) -> str:
#     n = name.lower()
#     if POI_HINTS.search(n):
#         for k in ["resort","hotel","villa","homestay","guest house","hostel","lodge","camp","glamp","spa","retreat","treehouse","houseboat","beach club"]:
#             if k in n: return k.replace(" ", "_")
#         return "resort"
#     if DESTINATION_HINTS.search(n):
#         # Normalize 'falls'/'waterfalls' → 'waterfall'
#         if re.search(r"\bwaterfall\b", n) or re.search(r"\bfalls?\b", n):
#             return "waterfall"
#         for k in [
#             "island","beach","national park","tiger reserve","sanctuary","reserve","rainforest",
#             "lake","valley","pass","peak","trek","trail","cave","park","bay","cove","lagoon","coast","shore","desert","dunes"
#         ]:
#             if k in n:
#                 return k.replace(" ", "_").rstrip("s")
#     # fallback based on playlist subtype
#     return "resort" if playlist_subtype == "poi" else "waterfall"  # neutral, works for many destinations

# def allowed_types_for(kind: str) -> set:
#     return ALLOWED_TYPES_BY_KIND.get(kind.lower(), {"tourist_attraction","point_of_interest","natural_feature","park","lodging","campground"})

# def primary_google_type(kind: str) -> Optional[str]:
#     k = kind.lower()
#     if k in {"resort","hotel","villa","homestay","lodge","hostel","guesthouse","camp"}:
#         return "lodging"
#     if k in {"park","national_park","sanctuary","reserve"}:
#         return "park"
#     # natural destinations behave like attractions
#     return "tourist_attraction"

# # -------------------------- Distance & Scores -------------------------

# def parse_hours(s: str) -> Optional[float]:
#     if not s: return None
#     m = re.search(r"(\d+(?:\.\d+)?)\s*hour", s, re.I)
#     return float(m.group(1)) if m else None

# def km_from_hours(h: Optional[float], default_speed_kmph: int = 70, buffer_km: int = 50) -> int:
#     if h is None: return 450  # default getaway radius
#     return int(max(80, min(800, h * default_speed_kmph + buffer_km)))

# def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
#     R = 6371.0
#     dLat = math.radians(lat2 - lat1)
#     dLon = math.radians(lon2 - lon1)
#     a = math.sin(dLat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2
#     return 2*R*math.asin(min(1, math.sqrt(a)))

# def distance_score(cand: Dict[str, Any], anchor_lat: Optional[float], anchor_lng: Optional[float], radius_m: int) -> float:
#     if anchor_lat is None or anchor_lng is None: return 0.0
#     if cand.get("lat") is None or cand.get("lng") is None: return 0.0
#     dkm = haversine_km(anchor_lat, anchor_lng, cand["lat"], cand["lng"])
#     rkm = max(1, radius_m/1000)
#     # full score inside half-radius, then linear decay to zero at 1.5x radius
#     if dkm <= 0.5*rkm: return 1.0
#     if dkm >= 1.5*rkm: return 0.0
#     return 1.0 - ((dkm - 0.5*rkm) / (1.0*rkm))

# def popularity_score(rating: float, reviews: int) -> float:
#     rating = rating or 0.0
#     reviews = max(0, int(reviews or 0))
#     return rating * math.log10(reviews + 1) / 5.0

# def type_compat_score(types: List[str], allowed: set) -> float:
#     return 1.0 if set(types or []) & allowed else (0.5 if types else 0.0)

# def state_match_score(address: str, expected_state: Optional[str]) -> float:
#     if not address or not expected_state: return 0.0
#     return 1.0 if expected_state.lower() in address.lower() else 0.0

# # ---------------------------- Query Builder ---------------------------

# def expand_name_variants(name: str) -> List[str]:
#     variants = {normalize_name(name)}
#     for pat, rep in NAME_ALIASES:
#         if pat.search(name):
#             variants.add(pat.sub(rep, name))
#     return list(variants)

# def build_queries(name: str, kind: str, anchor_city: Optional[str], anchor_state: Optional[str]) -> List[str]:
#     name_qs = expand_name_variants(name)
#     cat = kind.replace("_", " ")
#     queries: List[str] = []

#     # Landmark-state boost if known (e.g., Jog Falls → Karnataka)
#     low = name.lower()
#     for landmark, state in FAMOUS_LANDMARK_STATES.items():
#         if landmark in low:
#             for nq in name_qs:
#                 queries.append(f"{nq} {state} india")
#             break

#     for nq in name_qs:
#         queries.append(nq)
#         if anchor_state:
#             queries.append(f"{nq} {anchor_state}")
#             queries.append(f"{nq} {anchor_state} india")
#         if anchor_city:
#             queries.append(f"{nq} {anchor_city}")
#             queries.append(f"{nq} near {anchor_city}")
#         if cat and cat not in nq.lower():
#             if anchor_state:
#                 queries.append(f"{nq} {cat} {anchor_state}")
#             if anchor_city:
#                 queries.append(f"{nq} {cat} {anchor_city}")
#             queries.append(f"{nq} {cat}")

#     # de-dup preserving order
#     seen = set(); out = []
#     for q in queries:
#         qn = q.strip().lower()
#         if qn and qn not in seen:
#             seen.add(qn); out.append(q.strip())
#     return out[:12]

# # ------------------------------ Biasing -------------------------------

# def circle_bias(lat: float, lng: float, radius_m: int) -> str:
#     return f"circle:{max(1000, int(radius_m))}@{lat},{lng}"

# # ----------------------------- Resolver --------------------------------

# class Resolver:
#     def __init__(self, api_key: Optional[str], cache_path: Path):
#         self.enabled = bool(api_key and googlemaps)
#         self.cache_path = cache_path
#         self.cache: Dict[str, Any] = {}
#         if cache_path.exists():
#             try:
#                 self.cache = json.loads(cache_path.read_text(encoding="utf-8"))
#             except Exception:
#                 self.cache = {}
#         self.gmaps = googlemaps.Client(key=api_key) if self.enabled else None

#     # ---------- cache IO ----------
#     def save(self):
#         try:
#             self.cache_path.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass

#     def cache_key(self, name: str, hint: str, kind: str, anchor_city: Optional[str], anchor_state: Optional[str]) -> str:
#         return "|".join([
#             normalize_name(name).lower(), normalize_name(hint).lower(), kind.lower(),
#             normalize_name(anchor_city or "").lower(), normalize_name(anchor_state or "").lower()
#         ])

#     # ---------- geocode helpers ----------
#     def geocode_anchor(self, text: str) -> Tuple[Optional[float], Optional[float]]:
#         # check local dict first
#         key = text.strip().lower()
#         if key in CITY_COORDS:
#             return CITY_COORDS[key]
#         # then cache
#         if GEOCODE_CACHE.exists():
#             try:
#                 geo_cache = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
#                 if key in geo_cache:
#                     d = geo_cache[key]; return d.get("lat"), d.get("lng")
#             except Exception:
#                 pass
#         if not self.gmaps:
#             return None, None
#         lat = lng = None
#         try:
#             res = self.gmaps.geocode(f"{text}, India", language=GMAPS_LANGUAGE)
#             if res:
#                 loc = res[0]["geometry"]["location"]
#                 lat, lng = loc["lat"], loc["lng"]
#         except Exception:
#             pass
#         try:
#             geo_cache = {}
#             if GEOCODE_CACHE.exists():
#                 geo_cache = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
#             geo_cache[key] = {"lat": lat, "lng": lng}
#             GEOCODE_CACHE.write_text(json.dumps(geo_cache, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass
#         return lat, lng

#     # ---------- resolving ----------
#     def resolve(self, *, name: str, hint: str, kind: str, anchor_city: Optional[str], anchor_state: Optional[str], radius_m: int) -> Dict[str, Any]:
#         empty = {
#             "place_id": None, "name": name, "lat": None, "lng": None, "types": [],
#             "rating": 0.0, "reviews": 0, "address": None, "website": None, "phone": None,
#             "opening": [], "price_level": None, "permanently_closed": False,
#             "photo_refs": [], "utc_offset_minutes": None, "confidence": 0.0
#         }

#         key = self.cache_key(name, hint, kind, anchor_city, anchor_state)
#         if key in self.cache:
#             return {**empty, **self.cache[key]}
#         if not self.enabled:
#             self.cache[key] = empty
#             return empty

#         # Anchor coordinates
#         a_lat = a_lng = None
#         if anchor_city:
#             a_lat, a_lng = self.geocode_anchor(anchor_city)
#         # if not found and city is absent but state exists, try geocoding state center
#         if (a_lat is None or a_lng is None) and anchor_state:
#             a_lat, a_lng = self.geocode_anchor(anchor_state)

#         # Build queries
#         queries = build_queries(name=name, kind=kind, anchor_city=anchor_city, anchor_state=anchor_state)
#         allowed = allowed_types_for(kind)
#         ptype = primary_google_type(kind)

#         best = empty
#         best_score = -1.0

#         # Bias string for Find Place; location+radius for Text Search
#         bias = circle_bias(a_lat, a_lng, radius_m) if (a_lat is not None and a_lng is not None) else None

#         for idx, query in enumerate(queries[: 1 + RETRY_ATTEMPTS + 4]):
#             try:
#                 # 1) Find Place (with bias if available)
#                 cands: List[Dict[str, Any]] = []
#                 try:
#                     fp_kwargs = dict(input=query, input_type="textquery",
#                                      fields=["place_id","name","geometry/location","types","formatted_address","rating","user_ratings_total"],
#                                      language=GMAPS_LANGUAGE)
#                     if bias:
#                         fp_kwargs["location_bias"] = bias
#                     res = self.gmaps.find_place(**fp_kwargs)
#                     cands.extend(res.get("candidates", []) or [])
#                 except Exception:
#                     pass

#                 # 2) Text Search (wider net)
#                 try:
#                     ts_kwargs = dict(query=query, region=GMAPS_REGION, language=GMAPS_LANGUAGE)
#                     if a_lat is not None and a_lng is not None:
#                         ts_kwargs.update(location=(a_lat, a_lng), radius=min(500000, max(20000, radius_m)))
#                     if ptype:
#                         ts_kwargs.update(type=ptype)
#                     ts = self.gmaps.places(**ts_kwargs)
#                     cands.extend(ts.get("results", []) or [])
#                 except Exception:
#                     pass

#                 # Score candidates
#                 for c in cands[:10]:
#                     loc = (c.get("geometry") or {}).get("location") or {}
#                     cand = {
#                         "place_id": c.get("place_id"),
#                         "name": c.get("name") or name,
#                         "lat": loc.get("lat"),
#                         "lng": loc.get("lng"),
#                         "types": c.get("types", []) or [],
#                         "address": c.get("formatted_address") or "",
#                         "rating": float(c.get("rating") or 0.0),
#                         "reviews": int(c.get("user_ratings_total") or 0),
#                     }

#                     # components for score
#                     sim  = name_sim(name, cand["name"]) / 100.0
#                     tsc  = type_compat_score(cand["types"], allowed)
#                     pop  = popularity_score(cand["rating"], cand["reviews"]) 
#                     dsc  = distance_score(cand, a_lat, a_lng, radius_m)
#                     sm   = state_match_score(cand["address"], anchor_state)

#                     # Also apply landmark-state check boost/penalty
#                     lm_state = None
#                     for landmark, state in FAMOUS_LANDMARK_STATES.items():
#                         if landmark in name.lower():
#                             lm_state = state; break
#                     lm = 0.0
#                     if lm_state:
#                         if lm_state in (cand["address"].lower()):
#                             lm = 0.6
#                         else:
#                             # small penalty if clearly a different south-Indian state
#                             if any(s in (cand["address"].lower()) for s in ["kerala","tamil nadu","andhra pradesh","telangana"]):
#                                 lm = -0.3

#                     score = 0.32*sim + 0.20*tsc + 0.13*pop + 0.20*dsc + 0.10*sm + 0.05*max(0.0, lm)

#                     if score > best_score:
#                         best_score = score
#                         best = {**empty, **cand, "confidence": round(score, 3)}

#                 time.sleep(SLEEP_BETWEEN)
#                 if best_score >= MIN_CONFIDENCE:
#                     break
#             except Exception:
#                 time.sleep(SLEEP_BETWEEN)
#                 continue

#         # Details fetch for top candidate
#         if best.get("place_id"):
#             try:
#                 det = self.gmaps.place(place_id=best["place_id"], fields=[
#                     "place_id","name","geometry/location","types","formatted_address",
#                     "website","international_phone_number","opening_hours","price_level",
#                     "permanently_closed","rating","user_ratings_total","photos","utc_offset_minutes"
#                 ], language=GMAPS_LANGUAGE).get("result", {}) or {}

#                 loc = (det.get("geometry") or {}).get("location") or {}
#                 photos = det.get("photos") or []
#                 photo_refs = []
#                 for p in photos[:10]:
#                     ref = p.get("photo_reference") or p.get("photoReference")
#                     if ref: photo_refs.append(ref)

#                 best.update({
#                     "name": det.get("name") or best["name"],
#                     "lat": loc.get("lat", best["lat"]),
#                     "lng": loc.get("lng", best["lng"]),
#                     "types": det.get("types") or best["types"],
#                     "address": det.get("formatted_address") or best["address"],
#                     "website": det.get("website"),
#                     "phone": det.get("international_phone_number"),
#                     "opening": ((det.get("opening_hours") or {}).get("periods")) or [],
#                     "price_level": det.get("price_level"),
#                     "permanently_closed": bool(det.get("permanently_closed", False)),
#                     "rating": float(det.get("rating") or best.get("rating") or 0.0),
#                     "reviews": int(det.get("user_ratings_total") or best.get("reviews") or 0),
#                     "photo_refs": photo_refs,
#                     "utc_offset_minutes": det.get("utc_offset_minutes"),
#                 })
#                 time.sleep(SLEEP_BETWEEN)
#             except Exception:
#                 pass

#         # cache & return
#         self.cache[key] = best
#         return best

# # ------------------------------- Main ---------------------------------

# def is_publishable(resolved: Dict[str, Any], kind: str) -> bool:
#     pid = resolved.get("place_id")
#     lat, lng = resolved.get("lat"), resolved.get("lng")
#     if not (pid and lat is not None and lng is not None):
#         return False
#     if REQUIRE_PHOTO and not resolved.get("photo_refs"):
#         return False
#     rating = float(resolved.get("rating") or 0)
#     reviews = int(resolved.get("reviews") or 0)
#     if (rating >= MIN_RATING) or (reviews >= MIN_REVIEWS) or resolved.get("photo_refs"):
#         pass
#     else:
#         return False
#     allowed = allowed_types_for(kind)
#     if not (set(resolved.get("types", [])) & allowed):
#         if rating >= MIN_RATING and reviews >= MIN_REVIEWS:
#             return True
#         return False
#     return True

# def main():
#     global MIN_CONFIDENCE, RETRY_ATTEMPTS, REQUIRE_PHOTO

#     parser = argparse.ArgumentParser(description="Step 2.5 — Resolve & Validate (Google Places) — Most Accurate")
#     parser.add_argument("--in", dest="in_path", default=str(IN_PATH))
#     parser.add_argument("--out", dest="out_path", default=str(OUT_PATH))
#     parser.add_argument("--report", dest="report_path", default=str(REPORT))
#     parser.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE)
#     parser.add_argument("--retry", type=int, default=RETRY_ATTEMPTS)
#     parser.add_argument("--require-photo", action="store_true", default=REQUIRE_PHOTO)
#     # New tuning
#     parser.add_argument("--anchor-city", type=str, default=HARDCODE_ANCHOR_CITY)
#     parser.add_argument("--anchor-state", type=str, default=HARDCODE_ANCHOR_STATE)
#     parser.add_argument("--radius-km", type=int, default=None, help="Override per-item computed radius (km)")
#     parser.add_argument("--default-speed", type=int, default=70, help="Travel speed used for radius calc (km/h)")
#     args = parser.parse_args()

#     MIN_CONFIDENCE = args.min_confidence
#     RETRY_ATTEMPTS = args.retry
#     REQUIRE_PHOTO  = args.require_photo

#     in_path = Path(args.in_path)
#     out_path = Path(args.out_path)
#     report_path = Path(args.report_path)

#     if not in_path.exists():
#         print(f"❌ Input not found: {in_path}")
#         return

#     api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
#     resolver = Resolver(api_key, PLACES_CACHE)

#     data = json.loads(in_path.read_text(encoding="utf-8"))
#     if not isinstance(data, list):
#         print("❌ Input must be a JSON array of playlists.")
#         return

#     totals = {"items": 0, "resolved": 0, "publishable": 0, "partial": 0, "unresolved": 0}
#     per_pl = []

#     out_playlists = []
#     for plist in data:
#         title = plist.get("playlistTitle") or plist.get("title") or "Untitled"
#         subtype = str(plist.get("subtype","destination")).lower().strip()
#         items: List[Dict[str, Any]] = plist.get("items", [])

#         # Determine anchor inputs
#         anchor_city = args.anchor_city or plist.get("placeName") or plist.get("anchor_city")
#         # Normalize and alias
#         if anchor_city:
#             alias = ALIAS_MAP.get(anchor_city.lower(), None)
#             if alias:
#                 anchor_city = alias
#         anchor_state = args.anchor_state  # explicit override wins

#         # If state override is not provided, attempt a cheap guess from known landmark list or common mapping by city
#         if not anchor_state and anchor_city:
#             low = anchor_city.lower()
#             city_to_state = {
#                 "bengaluru": "karnataka", "bangalore": "karnataka",
#                 "mysuru": "karnataka", "mysore": "karnataka",
#                 "chennai": "tamil nadu", "hyderabad": "telangana",
#                 "kochi": "kerala", "mumbai": "maharashtra",
#             }
#             anchor_state = city_to_state.get(low)

#         resolved_items = []
#         stats = {"title": title, "publishable": 0, "partial": 0, "unresolved": 0}

#         for it in items:
#             totals["items"] += 1
#             src_name = (it.get("name") or "").strip()
#             if not src_name:
#                 totals["unresolved"] += 1
#                 stats["unresolved"] += 1
#                 continue

#             kind = infer_entity_kind(src_name, subtype)
#             base_name, hint = split_name_hint(src_name)

#             # Radius: from travel_time or CLI override
#             hours = parse_hours(it.get("travel_time", ""))
#             radius_km = args.radius_km if args.radius_km is not None else km_from_hours(hours, default_speed_kmph=args.default_speed)
#             radius_m = int(radius_km * 1000)

#             res = resolver.resolve(
#                 name=base_name, hint=hint, kind=kind,
#                 anchor_city=anchor_city, anchor_state=anchor_state, radius_m=radius_m
#             )

#             final = {
#                 "name": res.get("name") or base_name,
#                 "source_name": src_name,
#                 "entity_kind": kind,
#                 "place_id": res.get("place_id"),
#                 "lat": res.get("lat"),
#                 "lng": res.get("lng"),
#                 "types": res.get("types", []),
#                 "rating": res.get("rating", 0),
#                 "reviews": res.get("reviews", 0),
#                 "photo_refs": res.get("photo_refs", []),
#                 "address": res.get("address"),
#                 "website": res.get("website"),
#                 "phone": res.get("phone"),
#                 "utc_offset_minutes": res.get("utc_offset_minutes"),
#                 "permanently_closed": bool(res.get("permanently_closed", False)),
#                 "confidence": float(res.get("confidence", 0)),
#                 # carry forward from Step 2
#                 "description": it.get("description", ""),
#                 "travel_time": it.get("travel_time", ""),
#                 "price": it.get("price", ""),
#                 "votes": it.get("votes", 1),
#                 "source_urls": it.get("source_urls", []),
#             }

#             if res.get("place_id"):
#                 totals["resolved"] += 1
#                 if is_publishable(res, kind):
#                     final["resolution_status"] = "publishable"
#                     totals["publishable"] += 1
#                     stats["publishable"] += 1
#                 else:
#                     final["resolution_status"] = "partial"
#                     totals["partial"] += 1
#                     stats["partial"] += 1
#             else:
#                 final["resolution_status"] = "unresolved"
#                 totals["unresolved"] += 1
#                 stats["unresolved"] += 1

#             resolved_items.append(final)

#         per_pl.append(stats)
#         out_playlists.append({
#             "playlistTitle": title,
#             "placeName": plist.get("placeName"),
#             "subtype": subtype,
#             "source_urls": plist.get("source_urls", []),
#             "items": resolved_items,
#         })

#     # write outputs
#     out_path.write_text(json.dumps(out_playlists, ensure_ascii=False, indent=2), encoding="utf-8")

#     report_payload = {
#         "totals": totals,
#         "thresholds": {
#             "min_confidence": MIN_CONFIDENCE,
#             "min_reviews": MIN_REVIEWS,
#             "min_rating": MIN_RATING,
#             "require_photo": REQUIRE_PHOTO,
#         },
#         "playlists": per_pl,
#         "config": {
#             "anchor_city": HARDCODE_ANCHOR_CITY,
#             "anchor_state": HARDCODE_ANCHOR_STATE,
#             "language": GMAPS_LANGUAGE,
#             "region": GMAPS_REGION,
#         }
#     }
#     report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")

#     resolver.save()

#     print(f"✅ Wrote {len(out_playlists)} playlists to {out_path}")
#     print(f"🧾 Report → {report_path}")
#     if not resolver.enabled:
#         print("ℹ️ GOOGLE_MAPS_API_KEY not set or googlemaps not installed — ran in validation-only mode.")

# if __name__ == "__main__":
#     main()















