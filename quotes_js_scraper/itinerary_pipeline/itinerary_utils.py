#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shared utilities for the 1-day itinerary pipeline:
- caching (html/json)
- LLM JSON helper (Structured Outputs / JSON Schema)
- extraction helpers
- Google Places resolve helpers (fuzzy + optional LLM judge)
- routing (haversine, nearest-neighbor, 2-opt)
- scoring utilities
"""

from __future__ import annotations
import os, re, json, time, math, hashlib, random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# --------------------------- Optional deps ---------------------------
try:
    import requests
except Exception:
    requests = None

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

try:
    from rapidfuzz import fuzz
    def name_sim(a: str, b: str) -> int:
        return fuzz.token_set_ratio(a or "", b or "")
except Exception:
    def _tok(s: str) -> set:
        return set(re.findall(r"[a-z0-9]+", (s or "").lower()))
    def name_sim(a: str, b: str) -> int:
        A, B = _tok(a), _tok(b)
        return int(100 * len(A & B) / len(A | B)) if (A and B) else 0

try:
    import googlemaps
except Exception:
    googlemaps = None

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except Exception:
    OpenAI = None
    OPENAI_AVAILABLE = False


# --------------------------- Paths & Cache ---------------------------
BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR / "cache"
HTML_CACHE_DIR = CACHE_DIR / "html"
JSON_CACHE_DIR = CACHE_DIR / "json"
CACHE_DIR.mkdir(exist_ok=True, parents=True)
HTML_CACHE_DIR.mkdir(exist_ok=True, parents=True)
JSON_CACHE_DIR.mkdir(exist_ok=True, parents=True)


# --------------------------- Env helpers -----------------------------
def env(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key)
    return v if v not in (None, "") else default

def require_env(key: str) -> str:
    v = env(key)
    if not v:
        raise RuntimeError(f"Missing required env var: {key}")
    return v


# --------------------------- JSON helpers ----------------------------
def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def stable_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def safe_filename(s: str, max_len: int = 160) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s or "").strip("_")
    return (s[:max_len] or "file")


# --------------------------- HTTP / HTML -----------------------------
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0 Safari/537.36"
)

def fetch_html(url: str, use_cache: bool = True, timeout: int = 30) -> str:
    if not requests:
        raise RuntimeError("requests not installed. pip install requests")
    if use_cache:
        cache_path = HTML_CACHE_DIR / f"{safe_filename(url)}.html"
        if cache_path.exists():
            return cache_path.read_text(encoding="utf-8", errors="ignore")

    resp = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=timeout)
    resp.raise_for_status()
    html = resp.text

    if use_cache:
        cache_path.write_text(html, encoding="utf-8")
    return html

def clean_txt(s: str) -> str:
    s = (s or "").replace("\u2019", "'").replace("\u2014", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def html_to_text(html: str, max_chars: int = 18000) -> str:
    if not BeautifulSoup:
        return clean_txt(html)[:max_chars]

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()

    # Try to focus on article-ish content
    main = soup.select_one("article") or soup.select_one("main") or soup.body or soup
    text = clean_txt(main.get_text(" "))
    return text[:max_chars]


# --------------------------- Extraction ------------------------------
SKIP_PATTERNS = [
    r"frequently\s+asked\s+questions",
    r"faq",
    r"related\s+posts",
    r"common\s+queries",
    r"read\s+more",
    r"click\s+here",
    r"book\s+now",
    r"packages?",
    r"travel\s*triangle",
    r"makemytrip",
    r"holidify",
    r"subscribe",
]

FOOD_HINTS = re.compile(
    r"\b(restaurant|cafe|coffee|breakfast|brunch|lunch|dinner|street\s+food|food\s+market|dhaba|eatery|bistro|bar|pub)\b",
    re.I,
)

def heuristic_extract_candidates(html: str, max_items: int = 60) -> List[Dict[str, Any]]:
    """
    Best-effort heuristic extraction from headings + list items.
    Returns MANY candidates; later steps resolve + dedupe + LLM pick the final 8.
    """
    if not BeautifulSoup:
        return []

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    candidates: List[Dict[str, Any]] = []

    # Headings first
    for h in soup.select("h2, h3, h4"):
        name = clean_txt(h.get_text(" "))
        if len(name) < 2 or len(name) > 80:
            continue
        low = name.lower()
        if any(re.search(p, low) for p in SKIP_PATTERNS):
            continue
        # Remove numbering prefix like "1. Amber Fort"
        name2 = re.sub(r"^\s*\d+[\.\)]\s*", "", name).strip()
        if len(name2) < 2:
            continue

        # Take nearby paragraph as context
        desc = ""
        p = h.find_next("p")
        if p:
            desc = clean_txt(p.get_text(" "))[:260]

        kind_hint = "eating" if FOOD_HINTS.search(name2 + " " + desc) else "sightseeing"
        candidates.append({
            "name": name2,
            "description": desc,
            "kind_hint": kind_hint,
            "raw_context": desc,
        })

        if len(candidates) >= max_items:
            break

    # If headings are thin, scan list items
    if len(candidates) < 10:
        for li in soup.select("li"):
            t = clean_txt(li.get_text(" "))
            if 3 <= len(t) <= 70:
                low = t.lower()
                if any(re.search(p, low) for p in SKIP_PATTERNS):
                    continue
                kind_hint = "eating" if FOOD_HINTS.search(t) else "sightseeing"
                candidates.append({
                    "name": t,
                    "description": "",
                    "kind_hint": kind_hint,
                    "raw_context": "",
                })
            if len(candidates) >= max_items:
                break

    # Dedup by normalized name
    seen = set()
    out = []
    for c in candidates:
        key = re.sub(r"\s+", " ", c["name"].lower()).strip()
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out[:max_items]


# --------------------------- LLM JSON helper -------------------------
def llm_client_from_env() -> Optional[Any]:
    api_key = env("OPENAI_API_KEY")
    if not api_key or not OPENAI_AVAILABLE:
        return None
    return OpenAI(api_key=api_key)

def llm_json(
    client: Any,
    model: str,
    schema: Dict[str, Any],
    system: str,
    user: str,
    timeout: int = 45,
    cache_key_obj: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Uses Structured Outputs (json_schema) when available.
    Falls back to json_object style if needed.
    Caches in JSON_CACHE_DIR to save cost.
    """
    cache_key = None
    if cache_key_obj is not None:
        cache_key = stable_hash({"schema": schema, "system": system, "user": user, "k": cache_key_obj})
        cache_path = JSON_CACHE_DIR / f"llm_{cache_key}.json"
        cached = load_json(cache_path)
        if isinstance(cached, dict):
            return cached

    # Primary: Structured Outputs
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_schema", "json_schema": {"name": "out", "schema": schema}},
            timeout=timeout,
        )
        data = json.loads(resp.choices[0].message.content)
        if cache_key_obj is not None:
            save_json(cache_path, data)
        return data
    except Exception:
        pass

    # Fallback: json_object mode
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system + "\nReturn ONLY valid JSON."},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
            timeout=timeout,
        )
        data = json.loads(resp.choices[0].message.content)
        if cache_key_obj is not None:
            save_json(cache_path, data)
        return data
    except Exception as e:
        raise RuntimeError(f"LLM call failed: {e}")


# --------------------------- Google Places ---------------------------
@dataclass
class PlacesConfig:
    google_api_key: Optional[str]
    language: str = "en"
    region: Optional[str] = None  # e.g. "in"
    sleep_sec: float = 0.05
    min_confidence: float = 0.80
    grey_min: float = 0.35
    grey_max: float = 0.85

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    p = math.pi / 180.0
    dlat = (lat2 - lat1) * p
    dlon = (lon2 - lon1) * p
    a = (math.sin(dlat/2)**2
         + math.cos(lat1*p)*math.cos(lat2*p)*math.sin(dlon/2)**2)
    return 2 * R * math.asin(math.sqrt(a))

def popularity_score(rating: float, reviews: int) -> float:
    rating = float(rating or 0)
    reviews = int(reviews or 0)
    return (rating * math.log10(reviews + 10))  # stable-ish

FOOD_GOOGLE_TYPES = {
    "restaurant", "cafe", "bakery", "bar", "meal_takeaway", "meal_delivery", "food"
}
SIGHT_GOOGLE_TYPES = {
    "tourist_attraction", "museum", "park", "point_of_interest", "zoo",
    "amusement_park", "aquarium", "church", "mosque", "hindu_temple",
    "place_of_worship", "natural_feature", "art_gallery", "shopping_mall",
    "landmark"
}

def infer_place_type_from_google(types: List[str]) -> str:
    tset = set(types or [])
    if tset & FOOD_GOOGLE_TYPES:
        return "eating"
    return "sightseeing"

def allowed_types_for_kind(kind: str) -> set:
    base = {"point_of_interest", "tourist_attraction", "establishment"}
    if kind == "eating":
        base |= FOOD_GOOGLE_TYPES
    else:
        base |= SIGHT_GOOGLE_TYPES
    return base

class PlacesResolver:
    """
    Resolves a candidate name to a Google Place (place_id + details).
    Uses:
    - text search
    - fuzzy similarity
    - type compatibility
    - optional LLM judge in grey-zone
    """
    def __init__(self, cfg: PlacesConfig, llm_client: Optional[Any] = None, llm_model: str = "gpt-5-mini"):
        self.cfg = cfg
        self.llm_client = llm_client
        self.llm_model = llm_model

        self.enabled = bool(cfg.google_api_key and (requests or googlemaps))
        self.gmaps = googlemaps.Client(key=cfg.google_api_key) if (googlemaps and cfg.google_api_key) else None

        self.cache_path = JSON_CACHE_DIR / "places_resolve_cache.json"
        self.cache: Dict[str, Any] = load_json(self.cache_path, default={}) or {}

    def save(self):
        save_json(self.cache_path, self.cache)

    def _cache_key(self, name: str, city: str, state: str, kind_hint: str) -> str:
        return stable_hash({"n": name, "c": city, "s": state, "k": kind_hint, "lang": self.cfg.language, "reg": self.cfg.region})

    def _text_search(self, query: str) -> List[Dict[str, Any]]:
        if self.gmaps:
            try:
                res = self.gmaps.places(
                    query=query,
                    language=self.cfg.language,
                    region=self.cfg.region,
                )
                return (res.get("results") or [])[:6]
            except Exception:
                return []

        # REST fallback
        if not requests or not self.cfg.google_api_key:
            return []
        try:
            url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
            params = {
                "query": query,
                "key": self.cfg.google_api_key,
                "language": self.cfg.language,
            }
            if self.cfg.region:
                params["region"] = self.cfg.region
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return (r.json().get("results") or [])[:6]
        except Exception:
            pass
        return []

    def _place_details(self, place_id: str) -> Dict[str, Any]:
        if not place_id:
            return {}
        if self.gmaps:
            try:
                res = self.gmaps.place(
                    place_id=place_id,
                    fields=[
                        "place_id", "name", "formatted_address",
                        "geometry/location", "types",
                        "rating", "user_ratings_total",
                        "photos", "website",
                        "international_phone_number",
                        "opening_hours",
                        "price_level",
                        "business_status",
                        "utc_offset_minutes",
                    ],
                    language=self.cfg.language,
                )
                return res.get("result") or {}
            except Exception:
                pass

        if not requests or not self.cfg.google_api_key:
            return {}
        try:
            url = "https://maps.googleapis.com/maps/api/place/details/json"
            params = {
                "place_id": place_id,
                "fields": ",".join([
                    "place_id", "name", "formatted_address",
                    "geometry/location", "types",
                    "rating", "user_ratings_total",
                    "photos", "website",
                    "international_phone_number",
                    "opening_hours",
                    "price_level",
                    "business_status",
                    "utc_offset_minutes",
                ]),
                "key": self.cfg.google_api_key,
                "language": self.cfg.language,
            }
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200 and r.json().get("status") == "OK":
                return r.json().get("result") or {}
        except Exception:
            pass
        return {}

    def _llm_judge(self, source_name: str, source_context: str, cand: Dict[str, Any], kind_hint: str) -> float:
        """
        Returns confidence (0..1). Neutral 0.5 if unavailable.
        """
        if not (self.llm_client and OPENAI_AVAILABLE):
            return 0.5

        schema = {
            "type": "object",
            "properties": {
                "match": {"type": "boolean"},
                "confidence": {"type": "number"},
                "reason": {"type": "string"},
            },
            "required": ["match", "confidence", "reason"],
            "additionalProperties": False,
        }

        system = (
            "You are an entity-resolution judge for travel places.\n"
            "Decide whether SOURCE refers to CANDIDATE.\n"
            "Be strict about city/area/type mismatches.\n"
        )
        user = (
            f"SOURCE:\n- name: {source_name}\n- context: {source_context[:240]}\n"
            f"- kind_hint: {kind_hint}\n\n"
            f"CANDIDATE:\n- name: {cand.get('name')}\n"
            f"- address: {cand.get('formatted_address') or cand.get('vicinity') or ''}\n"
            f"- types: {cand.get('types') or []}\n\n"
            "Return JSON with match/confidence/reason."
        )

        try:
            out = llm_json(
                self.llm_client, self.llm_model, schema,
                system=system, user=user,
                timeout=30,
                cache_key_obj={"judge": True, "src": source_name, "cand": cand.get("place_id")},
            )
            conf = float(out.get("confidence", 0.5))
            if bool(out.get("match")):
                return max(0.80, min(1.0, conf))
            return max(0.0, min(1.0, conf))
        except Exception:
            return 0.5

    def resolve_one(
        self,
        name: str,
        city: str,
        state: str = "",
        kind_hint: str = "sightseeing",
        context: str = "",
    ) -> Dict[str, Any]:
        """
        Output normalized dict:
        {
          place_id, resolved_name, address, lat, lng, types,
          rating, reviews, photo_refs, website, phone, opening_periods,
          price_level, utc_offset_minutes, business_status,
          confidence, resolution_status
        }
        """
        empty = {
            "place_id": None,
            "resolved_name": name,
            "address": "",
            "lat": None,
            "lng": None,
            "types": [],
            "rating": 0.0,
            "reviews": 0,
            "photo_refs": [],
            "website": None,
            "phone": None,
            "opening_periods": [],
            "price_level": None,
            "utc_offset_minutes": None,
            "business_status": None,
            "confidence": 0.0,
            "resolution_status": "unresolved",
        }

        if not self.enabled:
            return empty

        ck = self._cache_key(name, city, state, kind_hint)
        if ck in self.cache:
            return {**empty, **self.cache[ck]}

        # Query variations
        queries = []
        if city:
            queries.append(f"{name} {city}".strip())
        if state:
            queries.append(f"{name} {city} {state}".strip())
        queries.append(name)

        allowed = allowed_types_for_kind(kind_hint)

        best = None
        best_score = -1.0

        for q in queries:
            cands = self._text_search(q)
            for cand in cands:
                c_name = cand.get("name") or ""
                sim = name_sim(name, c_name) / 100.0

                types = cand.get("types") or []
                type_ok = 1.0 if (set(types) & allowed) else 0.6

                addr = cand.get("formatted_address") or cand.get("vicinity") or ""
                state_bonus = 0.08 if (state and state.lower() in addr.lower()) else 0.0

                rat = float(cand.get("rating") or 0.0)
                rev = int(cand.get("user_ratings_total") or 0)
                pop = popularity_score(rat, rev)
                pop_norm = min(1.0, pop / 12.0)  # rough normalization

                math_score = max(0.0, min(1.0, 0.55*sim + 0.25*type_ok + 0.15*pop_norm + state_bonus))

                final_score = math_score
                if self.cfg.grey_min <= math_score < self.cfg.grey_max:
                    llm_conf = self._llm_judge(name, context, cand, kind_hint)
                    # If LLM is strongly positive/negative, override.
                    if llm_conf >= 0.85:
                        final_score = llm_conf
                    elif llm_conf <= 0.25:
                        final_score = 0.10

                if final_score > best_score:
                    best_score = final_score
                    best = cand

            time.sleep(self.cfg.sleep_sec)

            if best_score >= 0.92:
                break  # good enough

        if not best:
            self.cache[ck] = empty
            return empty

        place_id = best.get("place_id")
        det = self._place_details(place_id)

        loc = ((det.get("geometry") or {}).get("location") or {})
        photos = det.get("photos") or []
        photo_refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]

        out = {
            "place_id": place_id,
            "resolved_name": det.get("name") or best.get("name") or name,
            "address": det.get("formatted_address") or best.get("formatted_address") or "",
            "lat": loc.get("lat"),
            "lng": loc.get("lng"),
            "types": det.get("types") or best.get("types") or [],
            "rating": float(det.get("rating") or best.get("rating") or 0.0),
            "reviews": int(det.get("user_ratings_total") or best.get("user_ratings_total") or 0),
            "photo_refs": photo_refs,
            "website": det.get("website"),
            "phone": det.get("international_phone_number"),
            "opening_periods": (det.get("opening_hours") or {}).get("periods") or [],
            "price_level": det.get("price_level"),
            "utc_offset_minutes": det.get("utc_offset_minutes"),
            "business_status": det.get("business_status"),
            "confidence": round(float(best_score), 3),
            "resolution_status": "publishable" if float(best_score) >= self.cfg.min_confidence else "unresolved",
        }

        self.cache[ck] = out
        return out


# --------------------------- Routing ---------------------------------
def route_length_km(points: List[Tuple[float, float]]) -> float:
    if len(points) < 2:
        return 0.0
    total = 0.0
    for i in range(1, len(points)):
        total += haversine_km(points[i-1][0], points[i-1][1], points[i][0], points[i][1])
    return total

def nearest_neighbor_order(points: List[Tuple[float, float]], start_idx: int = 0) -> List[int]:
    n = len(points)
    if n <= 1:
        return list(range(n))
    unused = set(range(n))
    order = [start_idx]
    unused.remove(start_idx)
    cur = start_idx
    while unused:
        nxt = min(unused, key=lambda j: haversine_km(points[cur][0], points[cur][1], points[j][0], points[j][1]))
        order.append(nxt)
        unused.remove(nxt)
        cur = nxt
    return order

def two_opt(points: List[Tuple[float, float]], order: List[int], iters: int = 60) -> List[int]:
    if len(order) < 4:
        return order
    best = order[:]
    best_len = route_length_km([points[i] for i in best])
    n = len(best)
    for _ in range(iters):
        improved = False
        for i in range(1, n-2):
            for k in range(i+1, n-1):
                new = best[:i] + best[i:k+1][::-1] + best[k+1:]
                new_len = route_length_km([points[i] for i in new])
                if new_len + 1e-6 < best_len:
                    best = new
                    best_len = new_len
                    improved = True
        if not improved:
            break
    return best

def walkability_percent(total_km: float) -> int:
    # crude but useful UI metric
    if total_km <= 7:
        return 70
    if total_km <= 12:
        return 60
    if total_km <= 18:
        return 40
    return 20

def pick_anchor_idx(items: List[Dict[str, Any]]) -> int:
    """
    Choose anchor among sightseeing stops: highest frequency then popularity.
    """
    best_i = 0
    best_score = -1.0
    for i, it in enumerate(items):
        freq = int(it.get("frequency") or 0)
        pop = popularity_score(it.get("rating") or 0, it.get("reviews") or 0)
        score = (2.0 * freq) + pop
        if score > best_score:
            best_score = score
            best_i = i
    return best_i


# --------------------------- Misc ------------------------------------
def slugify(text: str) -> str:
    t = (text or "").strip().lower()
    t = re.sub(r"[^\w\s-]+", "", t)
    t = re.sub(r"[\s_-]+", "-", t)
    t = re.sub(r"^-+|-+$", "", t)
    return t or "untitled"

def now_ts() -> int:
    return int(time.time())
