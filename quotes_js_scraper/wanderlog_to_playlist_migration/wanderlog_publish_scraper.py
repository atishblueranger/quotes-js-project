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

import requests
import scrapy
from scrapy.crawler import CrawlerProcess

# ---------------- Configuration / constants ----------------
SCRIPT_VERSION = "1.3.0"

# Presentation / metadata (paths/limits)
GCS_BUCKET_DEFAULT = os.getenv("GCS_BUCKET", "mycasavsc.appspot.com")
IMAGE_BASE = "https://storage.googleapis.com/{bucket}/playlistsNew_images/{list_id}/1.jpg"
G_IMAGE_TEMPLATE = "https://storage.googleapis.com/{bucket}/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
G_IMAGE_COUNT = 4  # max uploaded photos per place

# Publish-only tags
SUBTYPE_TAG = "poi"
SOURCE_TAG = "wanderlog"

# City map (manual; extend as you go city-by-city)
CITY_ID_MAP = {
   "Jodhpur": "143",
  "Jaisalmer": "183",
  "Gurugram": "207",
  "Mysuru": "280",
  "Darjeeling": "349",
  "Lucknow": "375",
  "Amritsar": "384",
  "Ooty": "480",
  "Nashik": "449",
  "Bhopal": "485",
  "Indore": "558",
  "Mussoorie": "687",
  "Lonavala": "701",
  "Shillong": "728",
  "Kodaikanal": "842",
  "Nainital": "895",
  "Tirupati": "914",
  "Amer": "146238"
    ######Thailand
    # "Bangkok": "4",
    # "Chiang Mai": "19",
    # "Phuket Town": "51",
    # "Kathu": "53",
    # "Pattaya": "80",
    # "Bophut": "98",
    # "Hua Hin": "128",
    # "Khao Lak": "173",
    # "Karon": "176",
    # "Chiang Rai": "257",
    # "Rawai": "269",
    # "Ayutthaya": "468",
    # "Pai": "546",
    # "Kanchanaburi": "584",
    # "Mae Rim": "828",
    # "Chalong": "146242",
    # "Kamala": "146243",
    # "Patong": "146244",
    # "Ao Nang": "146246",
    # "Railay Beach": "146249",
    # "Chaweng": "146251",
    # "Kata Beach": "146257"
    # "Colombo": "146257",
    # "Negombo": "71",
    # # "Kandy": "146257",
    # "Galle": "190",
    # "Nuwara Eliya": "638",
    # "Ella": "146257",
    # "Anuradhapura": "146257",
    # "Sigiriya": "146257",
    # "Polonnaruwa": "146257",
    # "Dambulla": "146257",
    # "Singapore": "7",
    # "Manila": "175",
    # "Cebu City": "188",
    # # "Malay": "146257",
    # "Makati": "244",
    # "El Nido": "320",
    # "Puerto Princesa": "355",
    # "Davao City": "394",
    # "Baguio": "495",
    # "Coron": "509"
    # "Kathmandu": "17",
    # "Pokhara": "117"
    # "Kuala Lumpur": "33",
    # "George Town": "137",
    # "Kota Kinabalu": "145",
    # "Melaka": "187",
    # "Kuching": "223",
    # "Petaling Jaya": "272",
    # "Johor Bahru": "273",
    # "Ipoh": "376",
    # "Sandakan": "1160",
    # "Genting Highlands": "1688",
    # "Batu Caves": "4713",
    # "Pantai Cenang": "3121",

    # "Kyoto": "2",
    # "Tokyo": "1",
    # "Osaka": "3",
    # "Yokohama": "20",
    # "Minato": "23",
    # "Chuo": "26",
    # "Taito": "28",
    # "Sapporo": "29",
    # "Nagoya": "30",
    # "Shinjuku": "32",
    # "Fukuoka": "31",
    # "Kobe": "34",
    # "Shibuya": "38",
    # "Chiyoda": "42",
    # "Sendai": "56",
    # "Hiroshima": "59",
    # "Kanazawa": "62",
    # "Nagasaki": "75",
    # "Kitakyushu": "86",
    # "Okayama": "87",
    # "Bunkyo": "94",
    # "Kumamoto": "95",
    # "Nara": "99",
    # "Koto": "100",
    # "Ishigaki": "102",
    # "Toshima": "104",
    # "Hakodate": "110",
    # "Sumida": "113",
    # "Ota": "114",
    # "Kamakura": "121",
    # "Takayama": "124",
    # "Miyakojima": "140",
    # "Otaru": "142",
    # "Taketomi-cho": "163",
    # "Hakone-machi": "215",
    # "Beppu": "228",
    # "Biei-cho": "934",
    # "Urayasu": "892",

#   "Istanbul": "9622",
#   "Ankara": "9705",
#   "Antalya": "9721",
#   "Izmir": "9740",

#   "Kusadasi": "9853",
#   "Marmaris": "9857",

#   "Bodrum City": "9882",

#   "Fethiye": "9893",
#   "Goreme": "9896",
#   "Alanya": "9947",

#   "Kas": "9993",
#   "Bursa": "10030",
#   "Oludeniz": "10066",
#   "Dalyan": "10784",
#   "Pamukkale": "12573"

    #   "Birmingham": "9666",
    #   "Edinburgh": "9636",
    #   "Manchester": "9662",
    #   "Liverpool": "9660",
    #   "Dublin": "9633",
    #   "Cardiff": "9712",
    #   "Belfast": "9688",
    #   "Glasgow": "9651",
    #   "Oxford": "9723",
    #   "Bath": "9730",
    #   "York": "9707",
    #   "Bristol": "9692",
    #    "Brighton": "9702",
    #   "Newcastle upon Tyne": "9713",
    #   "Leeds": "9689"

    # "Rome": "9616",
    # "Venice": "9634",
    # "Florence": "9630",
    # "Milan": "9624",
    # "Vatican City": "10705",
    # "Naples": "9635",
    # "Pompeii": "9950",
    # "Pisa": "9767",
    # "Sorrento": "9743",
    # "Amalfi": "10198",
    # "Ravello": "10806",
    # "Turin": "9650",
    # "Bologna": "9672",
    # "Verona": "9711"
###### Netherlands
#   "Amsterdam": "9625",
#   "Rotterdam": "9719",
#   "The Hague": "9725",
#   "Utrecht": "164184",
#   "Maastricht": "164279",
#   "Haarlem": "164311",
#   "Delft": "164414",
#   "Zaandam": "164598"
###### FRANCE
#   "Paris": "9614",
#      "Nice": "9674",
#      "Lyon": "9670",
#   "Strasbourg": "9751",
#   "Chamonix": "9982",
#     "Cannes": "9827",
#     "Avignon": "9919",
#     "Bordeaux": "9663",
#     "Marseille": "9675",
#     "Annecy": "10085",
#     "Colmar": "10157",
#     "Toulouse": "9709",
#     "Aix-en-Provence": "9919"
######### 
## Swizerland

#   "Zurich": "9668",
#   "Geneva": "9724",
#     "Basel": "9817",
#   "Lausanne": "9979",
#     "Bern": "9984",
#   "Lucerne": "10008",
#     "Zermatt": "10136"
 ######Spain
#   "Barcelona": "9617",
#   "Madrid": "9621",
#   "Seville": "9641",
#   "Granada": "9693",
#   "Malaga": "9681",
#   "Marbella": "164108",
#   "Valencia": "9657",
#   "Palma de Mallorca": "9680",
#   "Cordoba": "9812"
# Germany

# "Berlin": "9623",
#   "Munich": "9645",
#   "Frankfurt": "9655",
#   "Hamburg": "9647",
#   "Cologne": "9682",
#   "Heidelberg": "9942",
#   "Stuttgart": "9737",
#   "Dusseldorf": "9738",
#   "Dresden": "9717",
#   "Nuremberg": "9739",
# # Portugal
#   "Lisbon": "9626",
#   "Sintra": "9915",
#   "Porto": "9653",
#   "Cascais": "9858",
#   "Faro": "9910",
#   "Albufeira": "9765",
#   "Lagos": "9831",
#   "Portimao": "9868",
#   "Funchal": "9735"

# Austria
#   "Vienna": "9631",
#   "Salzburg": "9750",
#   "Innsbruck": "9861",
#   "Graz": "9791"

# # Croatia
#   "Zagreb": "9669",
#   "Split": "9694",
#   "Dubrovnik": "9704",
#   "Zadar": "9904",

# Greece

#   "Athens": "9637",
#     "Oia": "10698",
#     "Mykonos Town": "9956",
#     "Heraklion": "9830",
#     "Rhodes Town": "9819",




}

# Optional .env
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
except Exception:
    pass

# Optional Selenium (off by default)
try:
    from scrapy_selenium import SeleniumRequest
except Exception:
    SeleniumRequest = None

# OpenAI model selection (if LLM features used)
OPENAI_MODEL = os.getenv("LC_MODEL", "gpt-4o-mini")

# Lazy GCP clients
FIRESTORE = None
STORAGE = None


# ---------------- Slug helpers / Firestore ID ----------------
def _slugify_basic(text: str) -> str:
    t = (text or "").strip().lower()
    t = re.sub(r"[^\w\s-]+", "", t)
    t = re.sub(r"[\s_-]+", "-", t)
    return re.sub(r"^-+|-+$", "", t) or "untitled"


def build_unique_slug(title: str, city: str, subtype: str, source_urls: List[str]) -> str:
    base = f"{_slugify_basic(title)}-{_slugify_basic(city)}-{subtype}"
    urls_str = str(sorted([u for u in source_urls if isinstance(u, str)])) if source_urls else ""
    h = hashlib.md5(urls_str.encode("utf-8")).hexdigest()[:6] if urls_str else "na"
    return f"{base}-{h}"


class FirestoreIdAssigner:
    """
    - Reuse existing numeric doc ID if a doc with the same slug exists
    - Otherwise assign the next numeric ID (max(existing numeric ids) + 1)
    """
    def __init__(self, collection: str, project: Optional[str] = None):
        from google.cloud import firestore as _firestore
        self.client = _firestore.Client(project=project) if project else _firestore.Client()
        self.col = self.client.collection(collection)
        self.next_id = self._compute_start_id()

    def _compute_start_id(self) -> int:
        max_id = 0
        try:
            for doc in self.col.select([]).stream():
                try:
                    v = int(doc.id)
                    if v > max_id:
                        max_id = v
                except ValueError:
                    continue
        except Exception:
            pass
        return max_id + 1

    def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, bool]:
        try:
            try:
                from google.cloud.firestore_v1 import FieldFilter
                q = self.col.where(filter=FieldFilter("slug", "==", slug)).limit(1)
            except Exception:
                q = self.col.where("slug", "==", slug).limit(1)
            existing = list(q.stream())
        except Exception:
            existing = []
        if existing:
            return existing[0].id, True
        new_id = str(self.next_id)
        self.next_id += 1
        return new_id, False


# --------------- slugify fallback ---------------
try:
    from slugify import slugify  # pip install python-slugify
except Exception:
    def slugify(s: str) -> str:
        s = (s or "").lower().strip()
        s = re.sub(r"[^\w\s-]", "", s)
        s = re.sub(r"[\s_-]+", "-", s)
        return re.sub(r"^-+|-+$", "", s)


# ---------------- Cache Management ----------------
class CacheManager:
    def __init__(self, cache_dir: Path):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.llm_validation_cache = self._load("llm_validation_cache.json")
        self.llm_title_cache = self._load("llm_title_cache.json")
        self.google_places_cache = self._load("google_places_cache.json")
        self.mobx_data_cache = self._load("mobx_data_cache.json")
        self.api_stats = {
            "llm_validation_calls": 0,
            "llm_title_calls": 0,
            "google_places_calls": 0,
            "cache_hits": 0,
            "cache_misses": 0
        }

    def _load(self, filename: str) -> Dict:
        p = self.cache_dir / filename
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save(self, data: Dict, filename: str):
        p = self.cache_dir / filename
        try:
            with p.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def save_all(self):
        self._save(self.llm_validation_cache, "llm_validation_cache.json")
        self._save(self.llm_title_cache, "llm_title_cache.json")
        self._save(self.google_places_cache, "google_places_cache.json")
        self._save(self.mobx_data_cache, "mobx_data_cache.json")

    def get_cache_key(self, **kwargs) -> str:
        key_data = json.dumps(kwargs, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(key_data.encode("utf-8")).hexdigest()

    def get_url_cache_key(self, url: str) -> str:
        return hashlib.md5(url.encode("utf-8")).hexdigest()


# ---------------- Category detection ----------------
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
    "malls": {
        "pos": r"\b(mall|shopping\s*centre|shopping\s*center|shopping\s*mall|galleria|plaza|souq|souk|bazaar|city\s*centre|city\s*center)\b",
        "neg": r"\b(beach|park|museum|temple|church|mosque|fort|palace|waterfall|viewpoint|skyline|tower)\b",
    },
    "skyline": {
        "pos": r"\b(skyline|skyscraper|observation\s*deck|view\s*deck|lookout|panorama|city\s*view|rooftop|tower|skyview|frame|wheel|ferris\s*wheel|eye|burj)\b",
        "neg": r"\b(mall|market|souq|souk|bazaar|museum|school|hospital)\b",
    },
}


def detect_category_from_title(page_title: str) -> str:
    t = (page_title or "").lower()
    if "mall" in t or "shopping" in t or "souq" in t or "souk" in t: return "malls"
    if "skyline" in t or "skyscraper" in t or "observation" in t or "view" in t: return "skyline"
    if "beach" in t: return "beaches"
    if "national park" in t or "wildlife" in t or "reserve" in t: return "national parks"
    if "waterfall" in t: return "waterfalls"
    if "castle" in t or "fortress" in t or "fort " in t: return "castles"
    if "photo spot" in t or "photo" in t: return "photo spots"
    if "romantic" in t: return "romantic places"
    if "architecture" in t: return "architecture"
    return "beaches"


# ---------------- Utilities ----------------
def iso_to_epoch_seconds(iso_str: str) -> Optional[int]:
    if not iso_str: return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None


def clean_text(text: Optional[str]) -> Optional[str]:
    if not text: return text
    return text.replace("\u2019", "'").replace("\u2014", "-").strip()


def default_description(title: str) -> str:
    return (f'Dive into "{title}" — a handpicked list of places with quick notes, links, '
            f'and essentials for fast trip planning and discovery.')


def build_slug_for_localfile(title: str, city: str, subtype: str, url: str) -> str:
    m = re.search(r"/(\d+)(?:$|[?#])", url) or re.search(r"/(\d+)$", url)
    tid = m.group(1) if m else "list"
    return f"{slugify(title)}-{slugify(city)}-{slugify(subtype)}-{tid}"


def explain_hits(pat: re.Pattern, text: str) -> List[str]:
    try:
        hits = pat.findall(text)
        if not hits: return []
        if isinstance(hits[0], tuple):
            hits = [h for tup in hits for h in tup if h]
        uniq = sorted({h.lower() for h in hits if isinstance(h, str) and h.strip()} )
        return uniq
    except Exception:
        return []


# ---------------- LLM helpers ----------------
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
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key: return None
    cache_key = cache_manager.get_cache_key(
        category=category, name=name, context=context[:500], city=city, page_title=page_title
    )
    if cache_key in cache_manager.llm_validation_cache:
        cache_manager.api_stats["cache_hits"] += 1
        v = cache_manager.llm_validation_cache[cache_key]
        return True if v == "true" else False if v == "false" else None
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
    try:
        from openai import OpenAI
        client = OpenAI()
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        out = (resp.choices[0].message.content or "").strip().lower()
        if out.startswith("yes"): result = True
        elif out.startswith("no"): result = False
    except Exception:
        pass
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
            if out.startswith("yes"): result = True
            elif out.startswith("no"): result = False
        except Exception:
            result = None
    cache_manager.llm_validation_cache[cache_key] = "true" if result is True else "false" if result is False else "none"
    return result


def is_valid_title(title: str, city: str, category: str) -> bool:
    if not title or len(title.strip()) < 3: return False
    title = title.strip()
    words = len(title.split())
    if words < 3 or words > 10: return False
    if re.search(r"\d", title): return False
    bad = [r"\b(top|best|#|number|first|second|third)\b",
           r"\b(guide|list|collection)\b",
           r"^(the\s+)?(ultimate|complete|definitive)\b"]
    for pat in bad:
        if re.search(pat, title.lower()): return False
    contains_location = city.lower() in title.lower()
    contains_category = any(w in title.lower() for w in category.lower().split())
    if not (contains_location or contains_category): return False
    if len(title) < 10 or len(title) > 80: return False
    return True


def create_fallback_title(city: str, category: str) -> str:
    m = {
        "beaches": f"Beautiful Beaches of {city}",
        "national parks": f"Wild {city} Parks",
        "waterfalls": f"Stunning {city} Waterfalls",
        "castles": f"Historic {city} Castles",
        "photo spots": f"Picture Perfect {city}",
        "romantic places": f"Romantic {city} Escapes",
        "architecture": f"Architectural Gems of {city}",
        "malls": f"Top Shopping in {city}",
        "skyline": f"{city} Skyline & Best Views",
    }
    return m.get(category.lower(), f"Discover {city}")


def generate_playlist_title(cache_manager: CacheManager, city: str, category: str, sample_names: List[str], page_title: str) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return create_fallback_title(city, category)
    cache_key = cache_manager.get_cache_key(city=city, category=category, sample_names=sample_names[:3], page_title=page_title[:100])
    if cache_key in cache_manager.llm_title_cache:
        cache_manager.api_stats["cache_hits"] += 1
        return cache_manager.llm_title_cache[cache_key]
    cache_manager.api_stats["cache_misses"] += 1
    cache_manager.api_stats["llm_title_calls"] += 1
    prompt = f"""Create an engaging travel playlist title.

Context:
- City/Region: {city}
- Category: {category}
- Original page title: "{page_title}"
- Featured places: {", ".join(sample_names[:3])}

Rules:
- 4–8 words
- Travel/discovery focused
- NO numbers or counts
- Memorable & specific to {city} or {category}

Return only the title:"""
    result = create_fallback_title(city, category)
    try:
        from openai import OpenAI
        client = OpenAI()
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
        )
        t = (resp.choices[0].message.content or "").strip().strip('"\'')
        if is_valid_title(t, city, category):
            result = t
    except Exception:
        pass
    if result == create_fallback_title(city, category):
        try:
            import openai as _openai
            _openai.api_key = api_key
            resp = _openai.ChatCompletion.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.4,
            )
            t = resp.choices[0].message["content"].strip().strip('"\'')
            if is_valid_title(t, city, category):
                result = t
        except Exception:
            pass
    cache_manager.llm_title_cache[cache_key] = result
    return result


# ---------------- Merge MOBX data ----------------
def merge_metadata_and_blocks(place_meta: List[Dict[str, Any]], blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
            "block_desc": (addendum.strip() or None),
            "block_imageKeys": b.get("imageKeys") or [],
            "selectedImageKey": b.get("selectedImageKey"),
        })
    merged = list(meta_map.values())
    merged.sort(key=lambda r: (999999 if r.get("block_id") is None else int(r.get("block_id")), -(r.get("rating") or 0)))
    return merged


# ---------------- Score / trim ----------------
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


# ---------------- Google Photos + GCS ----------------
def get_place_photo_refs(cache: CacheManager, place_id: str, api_key: str) -> List[str]:
    if not place_id: return []
    cache_key = f"refs::{place_id}"
    if cache_key in cache.google_places_cache:
        cache.api_stats["cache_hits"] += 1
        return cache.google_places_cache[cache_key] or []
    cache.api_stats["cache_misses"] += 1
    cache.api_stats["google_places_calls"] += 1
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {"place_id": place_id, "fields": "photo", "key": api_key}
    try:
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()
        js = r.json()
        photos = (js.get("result") or {}).get("photos") or []
        refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]
        refs = refs[:10]
    except Exception:
        refs = []
    cache.google_places_cache[cache_key] = refs
    return refs


def fetch_photo_bytes(photo_ref: str, api_key: str, maxwidth: int = 1600) -> Optional[bytes]:
    if not photo_ref: return None
    url = "https://maps.googleapis.com/maps/api/place/photo"
    params = {"maxwidth": str(maxwidth), "photo_reference": photo_ref, "key": api_key}
    try:
        r = requests.get(url, params=params, timeout=30, allow_redirects=True)
        r.raise_for_status()
        return r.content
    except Exception:
        return None


def gcs_upload_bytes(storage_client, bucket_name: str, blob_path: str, data: bytes, content_type: str = "image/jpeg", make_public: bool = True) -> str:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.cache_control = "public, max-age=31536000"
    blob.upload_from_string(data, content_type=content_type)
    try:
        if make_public:
            blob.make_public()
    except Exception:
        pass
    return f"https://storage.googleapis.com/{bucket_name}/{blob_path}"


def upload_photos_and_cover(list_id: str,
                            places_docs: List[Dict[str, Any]],
                            bucket_name: str,
                            max_photos: int = G_IMAGE_COUNT,
                            skip_photos: bool = False,
                            cache: Optional[CacheManager] = None) -> Optional[str]:
    if skip_photos or not places_docs:
        return None
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        return None

    global STORAGE
    if STORAGE is None:
        from google.cloud import storage as _storage
        STORAGE = _storage.Client()

    cover_done = False
    cover_url: Optional[str] = None

    for place in places_docs:
        pid = place.get("placeId")
        if not pid:
            place["g_image_urls"] = []
            continue

        refs = get_place_photo_refs(cache, pid, api_key) if cache else []
        uploaded_count = 0
        for i, ref in enumerate(refs[:max_photos], start=1):
            data = fetch_photo_bytes(ref, api_key)
            if not data:
                continue
            blob_path = f"playlistsPlaces/{list_id}/{pid}/{i}.jpg"
            _ = gcs_upload_bytes(STORAGE, bucket_name, blob_path, data, "image/jpeg", make_public=True)
            uploaded_count += 1

            if not cover_done and i == 1:
                cover_blob = f"playlistsNew_images/{list_id}/1.jpg"
                gcs_upload_bytes(STORAGE, bucket_name, cover_blob, data, "image/jpeg", make_public=True)
                cover_url = IMAGE_BASE.format(bucket=bucket_name, list_id=list_id)
                cover_done = True

        if uploaded_count:
            place["g_image_urls"] = [
                G_IMAGE_TEMPLATE.format(bucket=bucket_name, list_id=list_id, placeId=pid, n=n)
                for n in range(1, uploaded_count + 1)
            ]
        else:
            place["g_image_urls"] = []

    return cover_url


# ---------------- Firestore publish ----------------
def publish_playlist_to_firestore(playlist_doc: Dict[str, Any],
                                  places_docs: List[Dict[str, Any]],
                                  collection: str,
                                  project: Optional[str],
                                  assigned_doc_id: str) -> str:
    global FIRESTORE
    if FIRESTORE is None:
        from google.cloud import firestore as _firestore
        FIRESTORE = _firestore.Client(project=project) if project else _firestore.Client()

    col = FIRESTORE.collection(collection)
    doc_ref = col.document(assigned_doc_id)

    doc_to_write = dict(playlist_doc)
    doc_to_write.pop("subcollections", None)
    doc_ref.set(doc_to_write, merge=False)

    sub = doc_ref.collection("places")
    try:
        old = list(sub.stream())
        for i in range(0, len(old), 200):
            batch = FIRESTORE.batch()
            for doc in old[i:i+200]:
                batch.delete(doc.reference)
            batch.commit()
    except Exception:
        pass

    for i in range(0, len(places_docs), 450):
        batch = FIRESTORE.batch()
        for item in places_docs[i:i+450]:
            sub_id = item.get("placeId") or item.get("_id")
            batch.set(sub.document(sub_id), item)
        batch.commit()

    return assigned_doc_id


# ---------------- Dataset loader ----------------
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


# ---------------- Spider ----------------
class WanderlogPublishSpider(scrapy.Spider):
    name = "wanderlog_publish"
    custom_settings = {"LOG_LEVEL": "INFO"}

    # ---- Scrapy 2.13+ supports async start(); keep start_requests() for older versions ----
    async def start(self):
        for req in self._gen_start_requests():
            yield req

    def start_requests(self):
        # Deprecated in Scrapy 2.13+, but we keep it for compatibility
        yield from self._gen_start_requests()

    def __init__(self,
                 jobs: List[Dict[str, Any]],
                 out_dir: str,
                 keep_ratio: float,
                 use_llm: bool,
                 use_selenium: bool,
                 cache_dir: str = None,
                 publish: bool = False,
                 min_items: int = 7,
                 gcs_bucket: str = None,
                 collection: str = "playlistsNew",
                 project: Optional[str] = None,
                 max_photos: int = G_IMAGE_COUNT,
                 skip_photos: bool = False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.jobs = jobs
        self.out_dir = Path(out_dir)
        self.keep_ratio = float(keep_ratio)
        self.use_llm = bool(use_llm and bool(os.getenv("OPENAI_API_KEY")))
        self.use_selenium = bool(use_selenium and SeleniumRequest is not None)
        self.publish = bool(publish)
        self.min_items = int(min_items)
        self.gcs_bucket = gcs_bucket or GCS_BUCKET_DEFAULT
        self.collection = collection
        self.project = project
        self.max_photos = int(max_photos)
        self.skip_photos = bool(skip_photos)

        self.out_dir.mkdir(parents=True, exist_ok=True)
        cache_dir_path = Path(cache_dir) if cache_dir else self.out_dir / "cache"
        self.cache_manager = CacheManager(cache_dir_path)

        # Firestore numeric ID assigner (only if publishing)
        self.id_assigner = None
        if self.publish:
            self.id_assigner = FirestoreIdAssigner(collection=self.collection, project=self.project)

        self.failed_jobs: List[Dict[str, Any]] = []

        self.aggregate_report: Dict[str, Any] = {
            "script_version": SCRIPT_VERSION,
            "start_ts": int(time.time()),
            "llm_enabled_flag": bool(use_llm),
            "llm_client_loaded": bool(os.getenv("OPENAI_API_KEY")),
            "model": OPENAI_MODEL if self.use_llm else None,
            "cache_directory": str(cache_dir_path),
            "publish_enabled": self.publish,
            "min_items": self.min_items,
            "gcs_bucket": self.gcs_bucket,
            "collection": self.collection,
            "jobs": []
        }

    # ---- request generator used by both start() & start_requests() ----
    def _gen_start_requests(self):
        for idx, job in enumerate(self.jobs):
            url = job["url"]
            url_cache_key = self.cache_manager.get_url_cache_key(url)
            if url_cache_key in self.cache_manager.mobx_data_cache:
                self.logger.info(f"[{idx}] Using cached data for: {url}")
                self.cache_manager.api_stats["cache_hits"] += 1
                # Parse immediately from cache (no network)
                self.parse_cached_data(job, idx, url_cache_key)
                continue

            self.cache_manager.api_stats["cache_misses"] += 1
            meta = {"job": job, "job_index": idx, "url_cache_key": url_cache_key}
            if self.use_selenium:
                if SeleniumRequest is None:
                    self.logger.error("Selenium selected but scrapy_selenium is not installed.")
                    self.record_failure(job, "selenium_not_available", "bootstrap", url)
                    continue
                yield SeleniumRequest(url=url, callback=self.parse_page, meta=meta, wait_time=2, errback=self.on_fail)
            else:
                yield scrapy.Request(url=url, callback=self.parse_page, meta=meta, errback=self.on_fail)

    # ---- common failure recorder ----
    def record_failure(self, job: Dict[str, Any], error: str, stage: str, url: str):
        self.failed_jobs.append({
            "job_index": job.get("_idx"),
            "city": job.get("city"),
            "category": job.get("category_hint"),
            "title": job.get("title"),
            "url": url,
            "stage": stage,
            "error": error
        })

    # ---- errback for downloader/network errors ----
    def on_fail(self, failure):
        req = failure.request
        job = req.meta.get("job", {})
        err = repr(getattr(failure, "value", failure))
        self.record_failure(job, err, "download", req.url)

    def parse_cached_data(self, job: Dict[str, Any], job_index: int, url_cache_key: str):
        cached = self.cache_manager.mobx_data_cache.get(url_cache_key) or {}
        data = cached.get("data", {})
        page_title = cached.get("page_title", job.get("title", "Untitled"))
        url = job.get("url", "")
        if not data:
            self.logger.error(f"[{job_index}] Cached entry missing data: {url}")
            self.record_failure(job, "cached_data_missing", "cache", url)
            return
        self.process_mobx_data(job, job_index, data, page_title, url, from_cache=True)

    def parse_page(self, response):
        job = response.meta["job"]
        job_index = response.meta["job_index"]
        url_cache_key = response.meta["url_cache_key"]
        script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
        if not script_text:
            self.logger.error(f"[{job_index}] MOBX not found: {response.url}")
            self.record_failure(job, "mobx_script_not_found", "parse", response.url)
            return
        m = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text, re.DOTALL)
        if not m:
            self.logger.error(f"[{job_index}] MOBX parse failed: {response.url}")
            self.record_failure(job, "mobx_regex_no_match", "parse", response.url)
            return
        try:
            mobx_json = json.loads(m.group(1))
        except json.JSONDecodeError as e:
            self.logger.error(f"[{job_index}] MOBX JSON decode failed: {response.url}")
            self.record_failure(job, f"json_decode_error:{e}", "parse", response.url)
            return
        data = (mobx_json.get("placesListPage") or {}).get("data") or {}
        if not data:
            self.logger.error(f"[{job_index}] MOBX data empty: {response.url}")
            self.record_failure(job, "mobx_data_empty", "parse", response.url)
            return
        page_title = data.get("title") or (job.get("title") or "Untitled")
        self.cache_manager.mobx_data_cache[url_cache_key] = {"data": data, "page_title": page_title, "cached_ts": int(time.time())}
        self.process_mobx_data(job, job_index, data, page_title, response.url, from_cache=False)

    def process_mobx_data(self, job: Dict[str, Any], job_index: int, data: Dict[str, Any], page_title: str, url: str, from_cache: bool = False):
        place_meta = data.get("placeMetadata") or []
        blocks = []
        for sec in (data.get("boardSections") or []):
            for b in (sec.get("blocks") or []):
                if b.get("type") == "place":
                    blocks.append(b)

        category = (job.get("category_hint") or "").lower().strip() or detect_category_from_title(page_title)
        merged = merge_metadata_and_blocks(place_meta, blocks)

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
                ans = llm_validate(cache_manager=self.cache_manager, category=category, name=name, context=ctx,
                                   city=job.get("city") or "", page_title=page_title)
                if not from_cache:
                    llm_calls += 1
                decided_by = "llm"
                if ans is True:
                    verdict, reason = True, "llm_yes"
                elif ans is False:
                    verdict, reason = False, "llm_no"
                else:
                    verdict, reason = False, "llm_failed"

            place_reports.append({
                "placeId": it.get("placeId"),
                "name": name,
                "accepted": bool(verdict),
                "decided_by": decided_by,
                "reason": reason,
                "pos_hits": hits.get("pos_hits", []),
                "neg_hits": hits.get("neg_hits", []),
                "context_used_excerpt": ctx[:220]
            })
            if verdict: accepted.append(it)
            else: rejected.append(it)

        curated = trim_and_light_shuffle(accepted, keep_ratio=self.keep_ratio)

        # Title generation (numbers disallowed)
        final_title = page_title
        title_calls = 0
        if self.use_llm and curated:
            sample_names = [p.get("name", "") for p in curated[:3]]
            gen_title = generate_playlist_title(
                cache_manager=self.cache_manager,
                city=job.get("city") or "India",
                category=category,
                sample_names=sample_names,
                page_title=page_title
            )
            if gen_title and is_valid_title(gen_title, job.get("city", "India"), category):
                final_title = gen_title
            if not from_cache:
                title_calls += 1

        # Build slug (for Firestore numeric doc id assignment)
        source_urls = [url] if url else []
        city_name = job.get("city") or "India"
        slug = build_unique_slug(final_title, city_name, SUBTYPE_TAG, source_urls)

        # Determine Firestore doc id (numeric) if publishing
        list_id_str = None
        existed = False
        if self.publish:
            if not self.id_assigner:
                self.id_assigner = FirestoreIdAssigner(collection=self.collection, project=self.project)
            list_id_str, existed = self.id_assigner.assign_doc_id_for_slug(slug)
        else:
            m = re.search(r"/(\d+)(?:$|[?#])", url or "")
            list_id_str = m.group(1) if m else f"{slugify(city_name)}-local"

        # Build parent playlist doc (no 'subcollections' field here)
        playlist_doc = {
            "list_id": str(list_id_str),
            "imageUrl": IMAGE_BASE.format(bucket=self.gcs_bucket, list_id=list_id_str),
            "description": default_description(final_title),
            "source_urls": source_urls,
            "source": SOURCE_TAG,
            "category": "Travel",
            "title": final_title,
            "city_id": CITY_ID_MAP.get(city_name, city_name),
            "subtype": SUBTYPE_TAG,
            "city": city_name,
            "created_ts": int(time.time()),
            "slug": slug
        }

        # Build places docs
        places_docs: List[Dict[str, Any]] = []
        for it in curated:
            pid = it.get("placeId")
            reviews = []
            for r in (it.get("raw_reviews") or [])[:3]:
                reviews.append({
                    "rating": int(r.get("rating", 0)),
                    "text": clean_text(r.get("reviewText")),
                    "author_name": r.get("reviewerName") or "",
                    "relative_time_description": "",
                    "time": iso_to_epoch_seconds(r.get("time") or "") or 0,
                    "profile_photo_url": ""
                })
            places_docs.append({
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
            })

        # Local JSON artifact: include places for inspection
        base = job_out_basename(job, page_title)
        playlist_local = dict(playlist_doc)
        playlist_local["subcollections"] = {"places": sorted(places_docs, key=lambda x: x["index"])}
        playlist_path = self.out_dir / f"{base}.json"
        report_path = self.out_dir / f"{base}.report.json"
        playlist_path.write_text(json.dumps(playlist_local, ensure_ascii=False, indent=2), encoding="utf-8")

        published = False
        firestore_id = None

        # PUBLISH only if we have >= min_items
        if self.publish and len(places_docs) >= self.min_items:
            # 1) Upload photos & set cover
            try:
                cover = upload_photos_and_cover(
                    list_id=str(list_id_str),
                    places_docs=places_docs,
                    bucket_name=self.gcs_bucket,
                    max_photos=self.max_photos,
                    skip_photos=self.skip_photos,
                    cache=self.cache_manager
                )
                if cover:
                    playlist_doc["imageUrl"] = cover
            except Exception as e:
                self.logger.warning(f"[{job_index}] image upload step failed: {e}")

            # 2) Upsert into Firestore with assigned numeric ID
            try:
                firestore_id = publish_playlist_to_firestore(
                    playlist_doc=playlist_doc,
                    places_docs=places_docs,
                    collection=self.collection,
                    project=self.project,
                    assigned_doc_id=str(list_id_str)
                )
                published = True
            except Exception as e:
                self.logger.warning(f"[{job_index}] firestore publish failed: {e}")
        else:
            self.logger.info(f"[{job_index}] Not published (keep={len(places_docs)} < min={self.min_items}).")

        # Report
        job_report = {
            "job_index": job_index,
            "city": city_name,
            "category_used": category,
            "url": url,
            "title": page_title,
            "final_title": final_title,
            "from_cache": from_cache,
            "counts": {
                "meta": len(place_meta),
                "blocks": len(blocks),
                "merged": len(merged),
                "accepted_before_trim": len(accepted),
                "rejected": len(rejected),
                "accepted_final": len(places_docs),
            },
            "llm": {
                "enabled": self.use_llm,
                "model": OPENAI_MODEL if self.use_llm else None,
                "validation_calls": llm_calls,
                "title_calls": title_calls
            },
            "publish": {
                "enabled": self.publish,
                "min_items": self.min_items,
                "published": published,
                "firestore_doc_id": firestore_id,
                "bucket": self.gcs_bucket if published else None
            },
            "out_files": {
                "playlist_json": str(playlist_path),
                "report_json": str(report_path)
            },
            "created_ts": int(time.time())
        }
        report_path.write_text(json.dumps(job_report, ensure_ascii=False, indent=2), encoding="utf-8")
        self.aggregate_report["jobs"].append(job_report)

        pub_msg = "PUBLISHED" if published else "DRYRUN"
        self.logger.info(f"[{job_index}] {final_title} → kept {len(places_docs)}/{len(merged)} | LLM calls={llm_calls + title_calls} [{pub_msg}]")
        self.logger.info(f"  wrote: {playlist_path}")
        self.logger.info(f"  wrote: {report_path}")

    def closed(self, reason):
        self.cache_manager.save_all()

        # Write failures summary and rerun dataset
        failed_path = self.out_dir / "_failed_jobs.json"
        rerun_ds_path = self.out_dir / "_rerun_dataset.json"
        try:
            failed: List[Dict[str, Any]] = self.failed_jobs
            failed_path.write_text(json.dumps(failed, ensure_ascii=False, indent=2), encoding="utf-8")

            # Build rerun dataset in the same schema as the input dataset
            rerun: Dict[str, Dict[str, List[Dict[str, str]]]] = {}
            for f in failed:
                city = f.get("city") or "Unknown"
                cat = (f.get("category") or "misc").lower()
                rerun.setdefault(city, {}).setdefault(cat, []).append({"title": f.get("title") or "", "url": f.get("url") or ""})
            rerun_ds_path.write_text(json.dumps(rerun, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            self.logger.warning(f"Failed to write failure artifacts: {e}")

        self.aggregate_report.update({
            "end_ts": int(time.time()),
            "finish_reason": reason,
            "failures_count": len(self.failed_jobs),
            "failures_file": str(failed_path),
            "rerun_dataset_file": str(rerun_ds_path),
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
        hit = self.cache_manager.api_stats["cache_hits"]
        miss = self.cache_manager.api_stats["cache_misses"]
        rate = (hit / max(1, hit + miss)) * 100
        self.logger.info(f"Cache hit rate: {rate:.1f}%")
        self.logger.info(f"Aggregate report → {agg_path}")
        self.logger.info(f"Failures → {failed_path}")
        self.logger.info(f"Rerun dataset → {rerun_ds_path}")


# ---------------- Runner ----------------
def main():
    parser = argparse.ArgumentParser(description="Wanderlog → curated playlists → (optional) publish to Firestore + GCS.")
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

    # Publish knobs
    parser.add_argument("--publish", action="store_true", help="If set, publish to Firestore + GCS")
    parser.add_argument("--min-items", type=int, default=7, help="Only publish if >= this many places")
    parser.add_argument("--bucket", dest="gcs_bucket", default=GCS_BUCKET_DEFAULT, help="GCS bucket name")
    parser.add_argument("--collection", default="playlistsNew", help="Firestore collection name")
    parser.add_argument("--project", default=os.getenv("GOOGLE_CLOUD_PROJECT"), help="GCP project (optional)")
    parser.add_argument("--max-photos", type=int, default=G_IMAGE_COUNT, help=f"Max photos per place to upload (default {G_IMAGE_COUNT})")
    parser.add_argument("--skip-photos", action="store_true", help="Skip photo fetching/upload (still publishes doc)")

    # Fail if a city in the dataset is missing from CITY_ID_MAP
    parser.add_argument("--strict-city-id", action="store_true",
                        help="Error if any city in dataset lacks a CITY_ID_MAP entry")

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

    # Validate city-id mappings
    missing_cities = sorted({j["city"] for j in jobs if j.get("city") and j["city"] not in CITY_ID_MAP})
    if missing_cities:
        msg = "Missing city_id mapping for: " + ", ".join(missing_cities)
        if args.strict_city_id:
            raise SystemExit("ERROR: " + msg)
        else:
            print(f"⚠️ {msg} (will write 'city_id' as city name)")

    # --- Hardened Scrapy settings for unstable/chunked pages & backoff ---
    settings = {
        "LOG_LEVEL": "INFO",
        "TELNETCONSOLE_ENABLED": False,
        "ROBOTSTXT_OBEY": False,

        # Timeouts / TLS / DNS
        "DOWNLOAD_TIMEOUT": 300,
        "DNS_TIMEOUT": 30,

        # Make broken chunked responses non-fatal
        "DOWNLOAD_FAIL_ON_DATALOSS": False,

        # Concurrency & politeness (helps avoid rate limits)
        "CONCURRENT_REQUESTS": 6,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 3,
        "DOWNLOAD_DELAY": 0.75,
        "RANDOMIZE_DOWNLOAD_DELAY": True,

        # AutoThrottle
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 30,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,

        # Retries with exponential backoff
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
        "RETRY_BACKOFF_BASE": 2,
        "RETRY_BACKOFF_MAX": 60,

        # No cookies, stable UA
        "COOKIES_ENABLED": False,
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0 Safari/537.36"
        ),

        # Stats cadence
        "LOGSTATS_INTERVAL": 60,
    }

    # Selenium (optional)
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
        WanderlogPublishSpider,
        jobs=jobs,
        out_dir=args.out_dir,
        keep_ratio=args.keep_ratio,
        use_llm=args.use_llm,
        use_selenium=args.use_selenium,
        cache_dir=args.cache_dir,
        publish=args.publish,
        min_items=args.min_items,
        gcs_bucket=args.gcs_bucket,
        collection=args.collection, 
        project=args.project,
        max_photos=args.max_photos,
        skip_photos=args.skip_photos
    )
    process.start()


if __name__ == "__main__":
    main()




# # wanderlog_publish_scraper.py
# # ------------------------------------------------------------
# # Scrape Wanderlog lists → curate → (optionally) publish to
# # Firestore + GCS with numeric doc IDs (slug reuse), subtype=poi,
# # source=wanderlog, and Google Photos (up to G_IMAGE_COUNT).
# #
# # Requirements (examples):
# #   pip install scrapy requests python-slugify google-cloud-firestore google-cloud-storage python-dotenv
# #
# # Env you may want:
# #   GOOGLE_CLOUD_PROJECT=...
# #   GOOGLE_APPLICATION_CREDENTIALS=... (or gcloud auth application-default login)
# #   GOOGLE_MAPS_API_KEY=...
# #   OPENAI_API_KEY=... (if --use-llm)
# #   GCS_BUCKET=mycasavsc.appspot.com (defaults below)
# #
# # Usage (dry-run local JSON only):
# #   python wanderlog_publish_scraper.py --url "https://wanderlog.com/list/geoCategory/109820" --city "Chennai"
# #
# # Publish (only if >= min-items):
# #   python wanderlog_publish_scraper.py --dataset-file datasets/trial_dataset.json --publish --min-items 7
# #
# # ------------------------------------------------------------


# import os
# import re
# import json
# import time
# import math
# import argparse
# import hashlib
# from pathlib import Path
# from typing import Any, Dict, List, Optional, Tuple
# from datetime import datetime

# import requests
# import scrapy
# from scrapy.crawler import CrawlerProcess

# # ---------------- Configuration / constants ----------------
# SCRIPT_VERSION = "1.2.1"

# # Presentation / metadata (paths/limits)
# GCS_BUCKET_DEFAULT = os.getenv("GCS_BUCKET", "mycasavsc.appspot.com")
# IMAGE_BASE = "https://storage.googleapis.com/{bucket}/playlistsNew_images/{list_id}/1.jpg"
# G_IMAGE_TEMPLATE = "https://storage.googleapis.com/{bucket}/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
# G_IMAGE_COUNT = 4  # max uploaded photos per place

# # Publish-only tags
# SUBTYPE_TAG = "poi"
# SOURCE_TAG = "wanderlog"

# # City map (manual; extend as you go city-by-city)
# CITY_ID_MAP = {
#     # #Mumbai
#     # # "Chennai(Madras)": "40",
#     # # "Kolkata": "69",
#     # # "Thiruvananthapuram": "200",
#     # # "India": "86661",
#     # # "Goa": "86853",
#     # # "Pondicherry":"334",
#     # # "Bhubaneswar":"444",
#     # # "Visakhapatnam": "518",
#     # # "Port Blair": "589",
#     # "Haridwar": "783",
#     # # "Kanyakumari": "926",
#     # # "Puri": "975",
#     # # "Mahabalipuram": "1055",
#     # # "New Delhi": "13",
#     # # "Jaipur": "24",
#     # # "Bengaluru": "35",
#     # # "Agra": "60",
#     # # "Hyderabad": "78",
#     # # "Udaipur": "91",
#     # # "Jodhpur": "143",
#     # # "Ahmedabad": "161",
#     # # "Jaisalmer": "183",
#     # # "Gurugram": "207",
#     # # "Srinagar": "256",
#     # # "Mysuru": "280",
#     # # "Leh": "16704", # Check for Id 297
#     # "Chandigarh":"304" #Check for Id 304
#     # # "Vadodara": "348"  
#     # # "Darjeeling": "13485" # Check for Id
#     # # "Coimbatore": "5169" # CHeck for Ids 
#     # # "Lucknow": "375"
#     # # "Amritsar": "384"
#     # # "Shimla": "13910" # Wrong id
#     # # "Ooty":"480"
#     # # "Nashik": "449"
#     # # "Bhopal":"485"
#     # # "Gangtok":"503"
#     # # "Indore":"558"
#     # # "Madurai":"615"  #CHEck inn firestore 621
#     # # "Mussoorie":"687"
#     # # "Hampi":"696"
#     # # "Lonavala":"701"
#     # #  "Shillong":"728"
#     # # "Kodaikanal":"842"
#     # # "Nainital":"895"
#     # # "Tirupati":"914"
#     # # "Mahabaleshwar":"978"
#     # # "Amer":"146238"
#     # "Dubai": "85939",
#     # "Abu Dhabi": "85942",
#     # "Sharjah": "85948",
#     # "Al Ain": "85962",
#     ######Vietnam
# #   "Hanoi": "8",
# #   "Ho Chi Minh City": "16",
# #   "Hoi An": "21",
# #   "Da Nang": "49",
# #   "Hue": "92",
# #   "Nha Trang": "93",
# #   "Da Lat": "174",
# #   "Hạ Long Bay": "180",
# #   "Sapa": "221",
# #   "Phan Thiet": "277",
# #   "Ninh Binh": "724",
# #   "Mui Ne": "146264",
# ######Thailand
#     "Bangkok": "4",
#     "Chiang Mai": "19",
#     "Phuket Town": "51",
#     "Kathu": "53",
#     "Pattaya": "80",
#     "Bophut": "98",
#     "Hua Hin": "128",
#     "Khao Lak": "173",
#     "Karon": "176",
#     "Chiang Rai": "257",
#     "Rawai": "269",
#     "Ayutthaya": "468",
#     "Pai": "546",
#     "Kanchanaburi": "584",
#     "Mae Rim": "828",
#     "Chalong": "146242",
#     "Kamala": "146243",
#      "Patong": "146244",
#     "Ao Nang": "146246",
#     "Railay Beach": "146249",
#     "Chaweng": "146251",
#     "Kata Beach": "146257"
# }




# # Optional .env
# try:
#     from dotenv import load_dotenv
#     load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
# except Exception:
#     pass

# # Optional Selenium (off by default)
# try:
#     from scrapy_selenium import SeleniumRequest
# except Exception:
#     SeleniumRequest = None

# # OpenAI model selection (if LLM features used)
# OPENAI_MODEL = os.getenv("LC_MODEL", "gpt-4o-mini")

# # Lazy GCP clients
# FIRESTORE = None
# STORAGE = None


# # ---------------- Slug helpers / Firestore ID ----------------
# def _slugify_basic(text: str) -> str:
#     t = (text or "").strip().lower()
#     t = re.sub(r"[^\w\s-]+", "", t)
#     t = re.sub(r"[\s_-]+", "-", t)
#     return re.sub(r"^-+|-+$", "", t) or "untitled"


# def build_unique_slug(title: str, city: str, subtype: str, source_urls: List[str]) -> str:
#     base = f"{_slugify_basic(title)}-{_slugify_basic(city)}-{subtype}"
#     urls_str = str(sorted([u for u in source_urls if isinstance(u, str)])) if source_urls else ""
#     h = hashlib.md5(urls_str.encode("utf-8")).hexdigest()[:6] if urls_str else "na"
#     return f"{base}-{h}"


# class FirestoreIdAssigner:
#     """
#     Matches the reference Step 3 behavior:
#     - Reuse existing numeric doc ID if a doc with the same slug exists
#     - Otherwise assign the next numeric ID (max(existing numeric ids) + 1)
#     """
#     def __init__(self, collection: str, project: Optional[str] = None):
#         from google.cloud import firestore as _firestore
#         self.client = _firestore.Client(project=project) if project else _firestore.Client()
#         self.col = self.client.collection(collection)
#         self.next_id = self._compute_start_id()

#     def _compute_start_id(self) -> int:
#         max_id = 0
#         try:
#             for doc in self.col.select([]).stream():
#                 try:
#                     v = int(doc.id)
#                     if v > max_id:
#                         max_id = v
#                 except ValueError:
#                     continue
#         except Exception:
#             pass
#         return max_id + 1

#     def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, bool]:
#         try:
#             try:
#                 from google.cloud.firestore_v1 import FieldFilter
#                 q = self.col.where(filter=FieldFilter("slug", "==", slug)).limit(1)
#             except Exception:
#                 q = self.col.where("slug", "==", slug).limit(1)
#             existing = list(q.stream())
#         except Exception:
#             existing = []
#         if existing:
#             return existing[0].id, True
#         new_id = str(self.next_id)
#         self.next_id += 1
#         return new_id, False


# # --------------- slugify fallback for other uses ---------------
# try:
#     from slugify import slugify  # pip install python-slugify
# except Exception:
#     def slugify(s: str) -> str:
#         s = (s or "").lower().strip()
#         s = re.sub(r"[^\w\s-]", "", s)
#         s = re.sub(r"[\s_-]+", "-", s)
#         return re.sub(r"^-+|-+$", "", s)


# # ---------------- Cache Management ----------------
# class CacheManager:
#     def __init__(self, cache_dir: Path):
#         self.cache_dir = Path(cache_dir)
#         self.cache_dir.mkdir(parents=True, exist_ok=True)
#         self.llm_validation_cache = self._load("llm_validation_cache.json")
#         self.llm_title_cache = self._load("llm_title_cache.json")
#         self.google_places_cache = self._load("google_places_cache.json")
#         self.mobx_data_cache = self._load("mobx_data_cache.json")
#         self.api_stats = {
#             "llm_validation_calls": 0,
#             "llm_title_calls": 0,
#             "google_places_calls": 0,
#             "cache_hits": 0,
#             "cache_misses": 0
#         }

#     def _load(self, filename: str) -> Dict:
#         p = self.cache_dir / filename
#         if p.exists():
#             try:
#                 return json.loads(p.read_text(encoding="utf-8"))
#             except Exception:
#                 return {}
#         return {}

#     def _save(self, data: Dict, filename: str):
#         p = self.cache_dir / filename
#         try:
#             with p.open("w", encoding="utf-8") as f:
#                 json.dump(data, f, ensure_ascii=False, indent=2)
#         except Exception:
#             pass

#     def save_all(self):
#         self._save(self.llm_validation_cache, "llm_validation_cache.json")
#         self._save(self.llm_title_cache, "llm_title_cache.json")
#         self._save(self.google_places_cache, "google_places_cache.json")
#         self._save(self.mobx_data_cache, "mobx_data_cache.json")

#     def get_cache_key(self, **kwargs) -> str:
#         key_data = json.dumps(kwargs, sort_keys=True, ensure_ascii=False)
#         return hashlib.md5(key_data.encode("utf-8")).hexdigest()

#     def get_url_cache_key(self, url: str) -> str:
#         return hashlib.md5(url.encode("utf-8")).hexdigest()


# # ---------------- Category detection ----------------
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
#     "malls": {
#         "pos": r"\b(mall|shopping\s*centre|shopping\s*center|shopping\s*mall|galleria|plaza|souq|souk|bazaar|city\s*centre|city\s*center)\b",
#         "neg": r"\b(beach|park|museum|temple|church|mosque|fort|palace|waterfall|viewpoint|skyline|tower)\b",
#     },
#     "skyline": {
#         "pos": r"\b(skyline|skyscraper|observation\s*deck|view\s*deck|lookout|panorama|city\s*view|rooftop|tower|skyview|frame|wheel|ferris\s*wheel|eye|burj)\b",
#         "neg": r"\b(mall|market|souq|souk|bazaar|museum|school|hospital)\b",
#     },
# }


# def detect_category_from_title(page_title: str) -> str:
#     t = (page_title or "").lower()
#     if "mall" in t or "shopping" in t or "souq" in t or "souk" in t: return "malls"
#     if "skyline" in t or "skyscraper" in t or "observation" in t or "view" in t: return "skyline"
#     if "beach" in t: return "beaches"
#     if "national park" in t or "wildlife" in t or "reserve" in t: return "national parks"
#     if "waterfall" in t: return "waterfalls"
#     if "castle" in t or "fortress" in t or "fort " in t: return "castles"
#     if "photo spot" in t or "photo" in t: return "photo spots"
#     if "romantic" in t: return "romantic places"
#     if "architecture" in t: return "architecture"
#     return "beaches"



# # ---------------- Utilities ----------------
# def iso_to_epoch_seconds(iso_str: str) -> Optional[int]:
#     if not iso_str: return None
#     try:
#         dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
#         return int(dt.timestamp())
#     except Exception:
#         return None


# def clean_text(text: Optional[str]) -> Optional[str]:
#     if not text: return text
#     return text.replace("\u2019", "'").replace("\u2014", "-").strip()


# def default_description(title: str) -> str:
#     return (f'Dive into "{title}" — a handpicked list of places with quick notes, links, '
#             f'and essentials for fast trip planning and discovery.')


# def build_slug_for_localfile(title: str, city: str, subtype: str, url: str) -> str:
#     m = re.search(r"/(\d+)(?:$|[?#])", url) or re.search(r"/(\d+)$", url)
#     tid = m.group(1) if m else "list"
#     return f"{slugify(title)}-{slugify(city)}-{slugify(subtype)}-{tid}"


# def explain_hits(pat: re.Pattern, text: str) -> List[str]:
#     try:
#         hits = pat.findall(text)
#         if not hits: return []
#         if isinstance(hits[0], tuple):
#             hits = [h for tup in hits for h in tup if h]
#         uniq = sorted({h.lower() for h in hits if isinstance(h, str) and h.strip()} )
#         return uniq
#     except Exception:
#         return []


# # ---------------- LLM helpers ----------------
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


# def llm_validate(cache_manager: CacheManager, category: str, name: str, context: str, city: str, page_title: str) -> Optional[bool]:
#     api_key = os.getenv("OPENAI_API_KEY")
#     if not api_key: return None
#     cache_key = cache_manager.get_cache_key(
#         category=category, name=name, context=context[:500], city=city, page_title=page_title
#     )
#     if cache_key in cache_manager.llm_validation_cache:
#         cache_manager.api_stats["cache_hits"] += 1
#         v = cache_manager.llm_validation_cache[cache_key]
#         return True if v == "true" else False if v == "false" else None
#     cache_manager.api_stats["cache_misses"] += 1
#     cache_manager.api_stats["llm_validation_calls"] += 1
#     prompt = f"""You are validating inclusion for a travel list titled "{page_title}".
# City/Region: {city}
# Place: "{name}"
# Decision category: {category}

# Context:
# {context}

# Question: Does this place CLEARLY belong in the "{category}" category?
# Answer with only one token: YES or NO."""
#     result = None
#     try:
#         from openai import OpenAI
#         client = OpenAI()
#         resp = client.chat.completions.create(
#             model=OPENAI_MODEL,
#             messages=[{"role": "user", "content": prompt}],
#             temperature=0,
#         )
#         out = (resp.choices[0].message.content or "").strip().lower()
#         if out.startswith("yes"): result = True
#         elif out.startswith("no"): result = False
#     except Exception:
#         pass
#     if result is None:
#         try:
#             import openai as _openai
#             _openai.api_key = api_key
#             resp = _openai.ChatCompletion.create(
#                 model=OPENAI_MODEL,
#                 messages=[{"role": "user", "content": prompt}],
#                 temperature=0,
#             )
#             out = resp.choices[0].message["content"].strip().lower()
#             if out.startswith("yes"): result = True
#             elif out.startswith("no"): result = False
#         except Exception:
#             result = None
#     cache_manager.llm_validation_cache[cache_key] = "true" if result is True else "false" if result is False else "none"
#     return result


# def is_valid_title(title: str, city: str, category: str) -> bool:
#     if not title or len(title.strip()) < 3: return False
#     title = title.strip()
#     words = len(title.split())
#     if words < 3 or words > 10: return False
#     if re.search(r"\d", title): return False
#     bad = [r"\b(top|best|#|number|first|second|third)\b",
#            r"\b(guide|list|collection)\b",
#            r"^(the\s+)?(ultimate|complete|definitive)\b"]
#     for pat in bad:
#         if re.search(pat, title.lower()): return False
#     contains_location = city.lower() in title.lower()
#     contains_category = any(w in title.lower() for w in category.lower().split())
#     if not (contains_location or contains_category): return False
#     if len(title) < 10 or len(title) > 80: return False
#     return True


# def create_fallback_title(city: str, category: str) -> str:
#     m = {
#         "beaches": f"Beautiful Beaches of {city}",
#         "national parks": f"Wild {city} Parks",
#         "waterfalls": f"Stunning {city} Waterfalls",
#         "castles": f"Historic {city} Castles",
#         "photo spots": f"Picture Perfect {city}",
#         "romantic places": f"Romantic {city} Escapes",
#         "architecture": f"Architectural Gems of {city}",
#         "malls": f"Top Shopping in {city}",
#         "skyline": f"{city} Skyline & Best Views",
#     }
#     return m.get(category.lower(), f"Discover {city}")



# def generate_playlist_title(cache_manager: CacheManager, city: str, category: str, sample_names: List[str], page_title: str) -> str:
#     api_key = os.getenv("OPENAI_API_KEY")
#     if not api_key:
#         return create_fallback_title(city, category)
#     cache_key = cache_manager.get_cache_key(city=city, category=category, sample_names=sample_names[:3], page_title=page_title[:100])
#     if cache_key in cache_manager.llm_title_cache:
#         cache_manager.api_stats["cache_hits"] += 1
#         return cache_manager.llm_title_cache[cache_key]
#     cache_manager.api_stats["cache_misses"] += 1
#     cache_manager.api_stats["llm_title_calls"] += 1
#     prompt = f"""Create an engaging travel playlist title.

# Context:
# - City/Region: {city}
# - Category: {category}
# - Original page title: "{page_title}"
# - Featured places: {", ".join(sample_names[:3])}

# Rules:
# - 4–8 words
# - Travel/discovery focused
# - NO numbers or counts
# - Memorable & specific to {city} or {category}

# Return only the title:"""
#     result = create_fallback_title(city, category)
#     try:
#         from openai import OpenAI
#         client = OpenAI()
#         resp = client.chat.completions.create(
#             model=OPENAI_MODEL,
#             messages=[{"role": "user", "content": prompt}],
#             temperature=0.4,
#         )
#         t = (resp.choices[0].message.content or "").strip().strip('"\'')
#         if is_valid_title(t, city, category):
#             result = t
#     except Exception:
#         pass
#     if result == create_fallback_title(city, category):
#         try:
#             import openai as _openai
#             _openai.api_key = api_key
#             resp = _openai.ChatCompletion.create(
#                 model=OPENAI_MODEL,
#                 messages=[{"role": "user", "content": prompt}],
#                 temperature=0.4,
#             )
#             t = resp.choices[0].message["content"].strip().strip('"\'')
#             if is_valid_title(t, city, category):
#                 result = t
#         except Exception:
#             pass
#     cache_manager.llm_title_cache[cache_key] = result
#     return result


# # ---------------- Merge MOBX data ----------------
# def merge_metadata_and_blocks(place_meta: List[Dict[str, Any]], blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
#             "block_desc": (addendum.strip() or None),
#             "block_imageKeys": b.get("imageKeys") or [],
#             "selectedImageKey": b.get("selectedImageKey"),
#         })
#     merged = list(meta_map.values())
#     merged.sort(key=lambda r: (999999 if r.get("block_id") is None else int(r.get("block_id")), -(r.get("rating") or 0)))
#     return merged


# # ---------------- Score / trim ----------------
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


# # ---------------- Google Photos + GCS ----------------
# def get_place_photo_refs(cache: CacheManager, place_id: str, api_key: str) -> List[str]:
#     """
#     Lightweight Place Details REST call to retrieve photo refs; cached per place_id.
#     """
#     if not place_id: return []
#     cache_key = f"refs::{place_id}"
#     if cache_key in cache.google_places_cache:
#         cache.api_stats["cache_hits"] += 1
#         return cache.google_places_cache[cache_key] or []
#     cache.api_stats["cache_misses"] += 1
#     cache.api_stats["google_places_calls"] += 1
#     url = "https://maps.googleapis.com/maps/api/place/details/json"
#     params = {"place_id": place_id, "fields": "photo", "key": api_key}
#     try:
#         r = requests.get(url, params=params, timeout=12)
#         r.raise_for_status()
#         js = r.json()
#         photos = (js.get("result") or {}).get("photos") or []
#         refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]
#         refs = refs[:10]
#     except Exception:
#         refs = []
#     cache.google_places_cache[cache_key] = refs
#     return refs


# def fetch_photo_bytes(photo_ref: str, api_key: str, maxwidth: int = 1600) -> Optional[bytes]:
#     if not photo_ref: return None
#     url = "https://maps.googleapis.com/maps/api/place/photo"
#     params = {"maxwidth": str(maxwidth), "photo_reference": photo_ref, "key": api_key}
#     try:
#         r = requests.get(url, params=params, timeout=30, allow_redirects=True)
#         r.raise_for_status()
#         return r.content
#     except Exception:
#         return None


# def gcs_upload_bytes(storage_client, bucket_name: str, blob_path: str, data: bytes, content_type: str = "image/jpeg", make_public: bool = True) -> str:
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(blob_path)
#     blob.cache_control = "public, max-age=31536000"
#     blob.upload_from_string(data, content_type=content_type)
#     try:
#         if make_public:
#             blob.make_public()
#     except Exception:
#         pass
#     return f"https://storage.googleapis.com/{bucket_name}/{blob_path}"


# def upload_photos_and_cover(list_id: str,
#                             places_docs: List[Dict[str, Any]],
#                             bucket_name: str,
#                             max_photos: int = G_IMAGE_COUNT,
#                             skip_photos: bool = False,
#                             cache: Optional[CacheManager] = None) -> Optional[str]:
#     """
#     Upload Google Photos for each place and return a cover URL if created.
#     - Upload up to `max_photos` to: playlistsPlaces/{list_id}/{placeId}/{i}.jpg
#     - Returns IMAGE_BASE cover URL if 1st photo uploaded anywhere, else None.
#     Also fills each place's 'g_image_urls' field with canonical template URLs.
#     """
#     if skip_photos or not places_docs:
#         return None
#     api_key = os.getenv("GOOGLE_MAPS_API_KEY")
#     if not api_key:
#         return None

#     global STORAGE
#     if STORAGE is None:
#         from google.cloud import storage as _storage
#         STORAGE = _storage.Client()

#     cover_done = False
#     cover_url: Optional[str] = None

#     for place in places_docs:
#         pid = place.get("placeId")
#         if not pid:
#             place["g_image_urls"] = []
#             continue

#         refs = get_place_photo_refs(cache, pid, api_key) if cache else []
#         uploaded_count = 0
#         for i, ref in enumerate(refs[:max_photos], start=1):
#             data = fetch_photo_bytes(ref, api_key)
#             if not data:
#                 continue
#             blob_path = f"playlistsPlaces/{list_id}/{pid}/{i}.jpg"
#             _ = gcs_upload_bytes(STORAGE, bucket_name, blob_path, data, "image/jpeg", make_public=True)
#             uploaded_count += 1

#             # set cover from first successfully uploaded photo (across all places)
#             if not cover_done and i == 1:
#                 cover_blob = f"playlistsNew_images/{list_id}/1.jpg"
#                 gcs_upload_bytes(STORAGE, bucket_name, cover_blob, data, "image/jpeg", make_public=True)
#                 cover_url = IMAGE_BASE.format(bucket=bucket_name, list_id=list_id)
#                 cover_done = True

#         # canonical template URLs for uploaded_count
#         if uploaded_count:
#             place["g_image_urls"] = [
#                 G_IMAGE_TEMPLATE.format(bucket=bucket_name, list_id=list_id, placeId=pid, n=n)
#                 for n in range(1, uploaded_count + 1)
#             ]
#         else:
#             place["g_image_urls"] = []

#     return cover_url


# # ---------------- Firestore publish (doc + subcollection) ----------------
# def publish_playlist_to_firestore(playlist_doc: Dict[str, Any],
#                                   places_docs: List[Dict[str, Any]],
#                                   collection: str,
#                                   project: Optional[str],
#                                   assigned_doc_id: str) -> str:
#     """
#     Upsert the playlist (without 'subcollections') and replace 'places' subcollection.
#     """
#     global FIRESTORE
#     if FIRESTORE is None:
#         from google.cloud import firestore as _firestore
#         FIRESTORE = _firestore.Client(project=project) if project else _firestore.Client()

#     col = FIRESTORE.collection(collection)
#     doc_ref = col.document(assigned_doc_id)

#     # Write main doc (ensure no redundant embedded 'subcollections')
#     doc_to_write = dict(playlist_doc)
#     doc_to_write.pop("subcollections", None)
#     doc_ref.set(doc_to_write, merge=False)

#     # Replace subcollection "places"
#     sub = doc_ref.collection("places")
#     try:
#         old = list(sub.stream())
#         for i in range(0, len(old), 200):
#             batch = FIRESTORE.batch()
#             for doc in old[i:i+200]:
#                 batch.delete(doc.reference)
#             batch.commit()
#     except Exception:
#         pass

#     for i in range(0, len(places_docs), 450):
#         batch = FIRESTORE.batch()
#         for item in places_docs[i:i+450]:
#             sub_id = item.get("placeId") or item.get("_id")
#             batch.set(sub.document(sub_id), item)
#         batch.commit()

#     return assigned_doc_id


# # ---------------- Dataset loader ----------------
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


# # ---------------- Spider ----------------
# class WanderlogPublishSpider(scrapy.Spider):
#     name = "wanderlog_publish"
#     custom_settings = {"LOG_LEVEL": "INFO"}

#     def __init__(self,
#                  jobs: List[Dict[str, Any]],
#                  out_dir: str,
#                  keep_ratio: float,
#                  use_llm: bool,
#                  use_selenium: bool,
#                  cache_dir: str = None,
#                  publish: bool = False,
#                  min_items: int = 7,
#                  gcs_bucket: str = None,
#                  collection: str = "playlistsNew",
#                  project: Optional[str] = None,
#                  max_photos: int = G_IMAGE_COUNT,
#                  skip_photos: bool = False,
#                  *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.jobs = jobs
#         self.out_dir = Path(out_dir)
#         self.keep_ratio = float(keep_ratio)
#         self.use_llm = bool(use_llm and bool(os.getenv("OPENAI_API_KEY")))
#         self.use_selenium = bool(use_selenium and SeleniumRequest is not None)
#         self.publish = bool(publish)
#         self.min_items = int(min_items)
#         self.gcs_bucket = gcs_bucket or GCS_BUCKET_DEFAULT
#         self.collection = collection
#         self.project = project
#         self.max_photos = int(max_photos)
#         self.skip_photos = bool(skip_photos)

#         self.out_dir.mkdir(parents=True, exist_ok=True)
#         cache_dir_path = Path(cache_dir) if cache_dir else self.out_dir / "cache"
#         self.cache_manager = CacheManager(cache_dir_path)

#         # Firestore numeric ID assigner (only if publishing)
#         self.id_assigner = None
#         if self.publish:
#             self.id_assigner = FirestoreIdAssigner(collection=self.collection, project=self.project)

#         self.aggregate_report: Dict[str, Any] = {
#             "script_version": SCRIPT_VERSION,
#             "start_ts": int(time.time()),
#             "llm_enabled_flag": bool(use_llm),
#             "llm_client_loaded": bool(os.getenv("OPENAI_API_KEY")),
#             "model": OPENAI_MODEL if self.use_llm else None,
#             "cache_directory": str(cache_dir_path),
#             "publish_enabled": self.publish,
#             "min_items": self.min_items,
#             "gcs_bucket": self.gcs_bucket,
#             "collection": self.collection,
#             "jobs": []
#         }

#     def start_requests(self):
#         for idx, job in enumerate(self.jobs):
#             url = job["url"]
#             url_cache_key = self.cache_manager.get_url_cache_key(url)
#             if url_cache_key in self.cache_manager.mobx_data_cache:
#                 self.logger.info(f"[{idx}] Using cached data for: {url}")
#                 self.cache_manager.api_stats["cache_hits"] += 1
#                 self.parse_cached_data(job, idx, url_cache_key)
#             else:
#                 self.cache_manager.api_stats["cache_misses"] += 1
#                 meta = {"job": job, "job_index": idx, "url_cache_key": url_cache_key}
#                 if self.use_selenium:
#                     yield SeleniumRequest(url=url, callback=self.parse_page, meta=meta, wait_time=2)
#                 else:
#                     yield scrapy.Request(url=url, callback=self.parse_page, meta=meta)

#     def parse_cached_data(self, job: Dict[str, Any], job_index: int, url_cache_key: str):
#         cached = self.cache_manager.mobx_data_cache[url_cache_key]
#         data = cached.get("data", {})
#         page_title = cached.get("page_title", job.get("title", "Untitled"))
#         url = job.get("url", "")
#         self.process_mobx_data(job, job_index, data, page_title, url, from_cache=True)

#     def parse_page(self, response):
#         job = response.meta["job"]
#         job_index = response.meta["job_index"]
#         url_cache_key = response.meta["url_cache_key"]
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
#         if not script_text:
#             self.logger.error(f"[{job_index}] MOBX not found: {job['url']}")
#             return
#         m = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text, re.DOTALL)
#         if not m:
#             self.logger.error(f"[{job_index}] MOBX parse failed: {job['url']}")
#             return
#         try:
#             mobx_json = json.loads(m.group(1))
#         except json.JSONDecodeError:
#             self.logger.error(f"[{job_index}] MOBX JSON decode failed: {job['url']}")
#             return
#         data = (mobx_json.get("placesListPage") or {}).get("data") or {}
#         page_title = data.get("title") or (job.get("title") or "Untitled")
#         self.cache_manager.mobx_data_cache[url_cache_key] = {"data": data, "page_title": page_title, "cached_ts": int(time.time())}
#         self.process_mobx_data(job, job_index, data, page_title, response.url, from_cache=False)

#     def process_mobx_data(self, job: Dict[str, Any], job_index: int, data: Dict[str, Any], page_title: str, url: str, from_cache: bool = False):
#         place_meta = data.get("placeMetadata") or []
#         blocks = []
#         for sec in (data.get("boardSections") or []):
#             for b in (sec.get("blocks") or []):
#                 if b.get("type") == "place":
#                     blocks.append(b)

#         category = (job.get("category_hint") or "").lower().strip() or detect_category_from_title(page_title)
#         merged = merge_metadata_and_blocks(place_meta, blocks)

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
#                 ans = llm_validate(cache_manager=self.cache_manager, category=category, name=name, context=ctx,
#                                    city=job.get("city") or "", page_title=page_title)
#                 if not from_cache:
#                     llm_calls += 1
#                 decided_by = "llm"
#                 if ans is True:
#                     verdict, reason = True, "llm_yes"
#                 elif ans is False:
#                     verdict, reason = False, "llm_no"
#                 else:
#                     verdict, reason = False, "llm_failed"

#             place_reports.append({
#                 "placeId": it.get("placeId"),
#                 "name": name,
#                 "accepted": bool(verdict),
#                 "decided_by": decided_by,
#                 "reason": reason,
#                 "pos_hits": hits.get("pos_hits", []),
#                 "neg_hits": hits.get("neg_hits", []),
#                 "context_used_excerpt": ctx[:220]
#             })
#             if verdict: accepted.append(it)
#             else: rejected.append(it)

#         curated = trim_and_light_shuffle(accepted, keep_ratio=self.keep_ratio)

#         # Title generation (numbers disallowed)
#         final_title = page_title
#         title_calls = 0
#         if self.use_llm and curated:
#             sample_names = [p.get("name", "") for p in curated[:3]]
#             gen_title = generate_playlist_title(
#                 cache_manager=self.cache_manager,
#                 city=job.get("city") or "India",
#                 category=category,
#                 sample_names=sample_names,
#                 page_title=page_title
#             )
#             if gen_title and gen_title != create_fallback_title(job.get("city", "India"), category):
#                 final_title = gen_title
#             if not from_cache:
#                 title_calls += 1

#         # Build slug (for Firestore numeric doc id assignment)
#         source_urls = [url] if url else []
#         city_name = job.get("city") or "India"
#         slug = build_unique_slug(final_title, city_name, SUBTYPE_TAG, source_urls)

#         # Determine Firestore doc id (numeric) if publishing
#         list_id_str = None
#         existed = False
#         if self.publish:
#             if not self.id_assigner:
#                 self.id_assigner = FirestoreIdAssigner(collection=self.collection, project=self.project)
#             list_id_str, existed = self.id_assigner.assign_doc_id_for_slug(slug)
#         else:
#             # Not publishing: make a stable-ish local-only id from URL
#             m = re.search(r"/(\d+)(?:$|[?#])", url or "")
#             list_id_str = m.group(1) if m else f"{slugify(city_name)}-local"

#         # Build parent playlist doc (no 'subcollections' field here)
#         playlist_doc = {
#             "list_id": str(list_id_str),
#             "imageUrl": IMAGE_BASE.format(bucket=self.gcs_bucket, list_id=list_id_str),
#             "description": default_description(final_title),
#             "source_urls": source_urls,
#             "source": SOURCE_TAG,
#             "category": "Travel",
#             "title": final_title,
#             "city_id": CITY_ID_MAP.get(city_name, city_name),
#             "subtype": SUBTYPE_TAG,
#             "city": city_name,
#             "created_ts": int(time.time()),
#             "slug": slug
#         }

#         # Build places docs separately
#         places_docs: List[Dict[str, Any]] = []
#         for it in curated:
#             pid = it.get("placeId")
#             reviews = []
#             for r in (it.get("raw_reviews") or [])[:3]:
#                 reviews.append({
#                     "rating": int(r.get("rating", 0)),
#                     "text": clean_text(r.get("reviewText")),
#                     "author_name": r.get("reviewerName") or "",
#                     "relative_time_description": "",
#                     "time": iso_to_epoch_seconds(r.get("time") or "") or 0,
#                     "profile_photo_url": ""
#                 })
#             places_docs.append({
#                 "_id": pid or (it.get("name") or "unknown"),
#                 "generalDescription": it.get("generalDescription"),
#                 "utcOffset": int(it.get("utcOffset") if it.get("utcOffset") is not None else 330),
#                 "maxMinutesSpent": it.get("maxMinutesSpent"),
#                 "longitude": it.get("longitude"),
#                 "rating": it.get("rating") or 0,
#                 "numRatings": it.get("numRatings") or 0,
#                 "sources": it.get("sources") or [],
#                 "imageKeys": (it.get("imageKeys") or []) + (it.get("block_imageKeys") or []),
#                 "openingPeriods": it.get("openingPeriods") or [],
#                 "name": it.get("name"),
#                 "placeId": pid,
#                 "internationalPhoneNumber": it.get("internationalPhoneNumber"),
#                 "reviews": reviews,
#                 "permanentlyClosed": bool(it.get("permanentlyClosed")),
#                 "priceLevel": it.get("priceLevel"),
#                 "tripadvisorRating": 0,
#                 "description": None,
#                 "website": it.get("website"),
#                 "index": it.get("_final_index") or 1,
#                 "id": "",
#                 "categories": it.get("categories") or [],
#                 "tripadvisorNumRatings": 0,
#                 "g_image_urls": [],
#                 "ratingDistribution": it.get("ratingDistribution") or {},
#                 "minMinutesSpent": it.get("minMinutesSpent"),
#                 "latitude": it.get("latitude"),
#                 "address": it.get("address"),
#                 "travel_time": it.get("travel_time")
#             })

#         # Local JSON artifact: include places for inspection
#         base = job_out_basename(job, page_title)
#         playlist_local = dict(playlist_doc)
#         playlist_local["subcollections"] = {"places": sorted(places_docs, key=lambda x: x["index"])}
#         playlist_path = self.out_dir / f"{base}.json"
#         report_path = self.out_dir / f"{base}.report.json"
#         playlist_path.write_text(json.dumps(playlist_local, ensure_ascii=False, indent=2), encoding="utf-8")

#         published = False
#         firestore_id = None

#         # PUBLISH only if we have >= min_items
#         if self.publish and len(places_docs) >= self.min_items:
#             # 1) Upload photos & set cover
#             try:
#                 cover = upload_photos_and_cover(
#                     list_id=str(list_id_str),
#                     places_docs=places_docs,
#                     bucket_name=self.gcs_bucket,
#                     max_photos=self.max_photos,
#                     skip_photos=self.skip_photos,
#                     cache=self.cache_manager
#                 )
#                 if cover:
#                     playlist_doc["imageUrl"] = cover
#             except Exception as e:
#                 self.logger.warning(f"[{job_index}] image upload step failed: {e}")

#             # 2) Upsert into Firestore with assigned numeric ID
#             try:
#                 firestore_id = publish_playlist_to_firestore(
#                     playlist_doc=playlist_doc,
#                     places_docs=places_docs,
#                     collection=self.collection,
#                     project=self.project,
#                     assigned_doc_id=str(list_id_str)
#                 )
#                 published = True
#             except Exception as e:
#                 self.logger.warning(f"[{job_index}] firestore publish failed: {e}")
#         else:
#             self.logger.info(f"[{job_index}] Not published (keep={len(places_docs)} < min={self.min_items}).")

#         # Report
#         job_report = {
#             "job_index": job_index,
#             "city": city_name,
#             "category_used": category,
#             "url": url,
#             "title": page_title,
#             "final_title": final_title,
#             "from_cache": from_cache,
#             "counts": {
#                 "meta": len(place_meta),
#                 "blocks": len(blocks),
#                 "merged": len(merged),
#                 "accepted_before_trim": len(accepted),
#                 "rejected": len(rejected),
#                 "accepted_final": len(places_docs),
#             },
#             "llm": {
#                 "enabled": self.use_llm,
#                 "model": OPENAI_MODEL if self.use_llm else None,
#                 "validation_calls": llm_calls,
#                 "title_calls": title_calls
#             },
#             "publish": {
#                 "enabled": self.publish,
#                 "min_items": self.min_items,
#                 "published": published,
#                 "firestore_doc_id": firestore_id,
#                 "bucket": self.gcs_bucket if published else None
#             },
#             "out_files": {
#                 "playlist_json": str(playlist_path),
#                 "report_json": str(report_path)
#             },
#             "created_ts": int(time.time())
#         }
#         report_path.write_text(json.dumps(job_report, ensure_ascii=False, indent=2), encoding="utf-8")
#         self.aggregate_report["jobs"].append(job_report)

#         pub_msg = "PUBLISHED" if published else "DRYRUN"
#         self.logger.info(f"[{job_index}] {final_title} → kept {len(places_docs)}/{len(merged)} | LLM calls={llm_calls + title_calls} [{pub_msg}]")
#         self.logger.info(f"  wrote: {playlist_path}")
#         self.logger.info(f"  wrote: {report_path}")

#     def closed(self, reason):
#         self.cache_manager.save_all()
#         self.aggregate_report.update({
#             "end_ts": int(time.time()),
#             "finish_reason": reason,
#             "cache_stats": self.cache_manager.api_stats,
#             "cache_sizes": {
#                 "llm_validation_cache": len(self.cache_manager.llm_validation_cache),
#                 "llm_title_cache": len(self.cache_manager.llm_title_cache),
#                 "google_places_cache": len(self.cache_manager.google_places_cache),
#                 "mobx_data_cache": len(self.cache_manager.mobx_data_cache)
#             }
#         })
#         agg_path = self.out_dir / "_aggregate_report.json"
#         agg_path.write_text(json.dumps(self.aggregate_report, ensure_ascii=False, indent=2), encoding="utf-8")
#         hit = self.cache_manager.api_stats["cache_hits"]
#         miss = self.cache_manager.api_stats["cache_misses"]
#         rate = (hit / max(1, hit + miss)) * 100
#         self.logger.info(f"Cache hit rate: {rate:.1f}%")
#         self.logger.info(f"Aggregate report → {agg_path}")


# # ---------------- Runner ----------------
# def main():
#     parser = argparse.ArgumentParser(description="Wanderlog → curated playlists → (optional) publish to Firestore + GCS.")
#     g = parser.add_mutually_exclusive_group(required=True)
#     g.add_argument("--url", help="Single Wanderlog list URL")
#     g.add_argument("--dataset-file", help="Path to JSON dataset (city -> category -> [{title,url}])")

#     parser.add_argument("--city", help="City name (required if using --url)")
#     parser.add_argument("--out-dir", default="trial_playlists", help="Output directory for JSONs and reports")
#     parser.add_argument("--cache-dir", help="Cache directory (defaults to out-dir/cache)")
#     parser.add_argument("--keep-ratio", type=float, default=0.7, help="Fraction to keep after trimming (0..1)")
#     parser.add_argument("--use-llm", action="store_true", help="Enable LLM for uncertain cases (requires OPENAI_API_KEY)")
#     parser.add_argument("--use-selenium", action="store_true", help="(Optional) Use selenium; usually not needed")
#     parser.add_argument("--clear-cache", action="store_true", help="Clear all caches before starting")

#     # Publish knobs
#     parser.add_argument("--publish", action="store_true", help="If set, publish to Firestore + GCS")
#     parser.add_argument("--min-items", type=int, default=7, help="Only publish if >= this many places")
#     parser.add_argument("--bucket", dest="gcs_bucket", default=GCS_BUCKET_DEFAULT, help="GCS bucket name")
#     parser.add_argument("--collection", default="playlistsNew", help="Firestore collection name")
#     parser.add_argument("--project", default=os.getenv("GOOGLE_CLOUD_PROJECT"), help="GCP project (optional)")
#     parser.add_argument("--max-photos", type=int, default=G_IMAGE_COUNT, help=f"Max photos per place to upload (default {G_IMAGE_COUNT})")
#     parser.add_argument("--skip-photos", action="store_true", help="Skip photo fetching/upload (still publishes doc)")

#     # NEW: fail/warn if a city in the dataset is missing from CITY_ID_MAP
#     parser.add_argument("--strict-city-id", action="store_true",
#                         help="Error if any city in dataset lacks a CITY_ID_MAP entry")

#     args = parser.parse_args()

#     if args.url and not args.city:
#         parser.error("--city is required when using --url")

#     # Handle cache clearing
#     if args.clear_cache:
#         cache_dir_path = Path(args.cache_dir) if args.cache_dir else Path(args.out_dir) / "cache"
#         if cache_dir_path.exists():
#             import shutil
#             shutil.rmtree(cache_dir_path)
#             print(f"Cleared cache directory: {cache_dir_path}")

#     jobs = build_jobs_from_args(args.url, args.city, args.dataset_file)
#     if not jobs:
#         print("No jobs to process (check --url/--city or --dataset-file).")
#         return

#     # NEW: validate city-id mappings
#     missing_cities = sorted({j["city"] for j in jobs if j.get("city") and j["city"] not in CITY_ID_MAP})
#     if missing_cities:
#         msg = "Missing city_id mapping for: " + ", ".join(missing_cities)
#         if args.strict_city_id:
#             raise SystemExit("ERROR: " + msg)
#         else:
#             print(f"⚠️ {msg} (will write 'city_id' as city name)")

#     # Selenium (optional)
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
#         WanderlogPublishSpider,
#         jobs=jobs,
#         out_dir=args.out_dir,
#         keep_ratio=args.keep_ratio,
#         use_llm=args.use_llm,
#         use_selenium=args.use_selenium,
#         cache_dir=args.cache_dir,
#         publish=args.publish,
#         min_items=args.min_items,
#         gcs_bucket=args.gcs_bucket,
#         collection=args.collection,
#         project=args.project,
#         max_photos=args.max_photos,
#         skip_photos=args.skip_photos
#     )
#     process.start()


# # def main():
# #     parser = argparse.ArgumentParser(description="Wanderlog → curated playlists → (optional) publish to Firestore + GCS.")
# #     g = parser.add_mutually_exclusive_group(required=True)
# #     g.add_argument("--url", help="Single Wanderlog list URL")
# #     g.add_argument("--dataset-file", help="Path to JSON dataset (city -> category -> [{title,url}])")

# #     parser.add_argument("--city", help="City name (required if using --url)")
# #     parser.add_argument("--out-dir", default="trial_playlists", help="Output directory for JSONs and reports")
# #     parser.add_argument("--cache-dir", help="Cache directory (defaults to out-dir/cache)")
# #     parser.add_argument("--keep-ratio", type=float, default=0.7, help="Fraction to keep after trimming (0..1)")
# #     parser.add_argument("--use-llm", action="store_true", help="Enable LLM for uncertain cases (requires OPENAI_API_KEY)")
# #     parser.add_argument("--use-selenium", action="store_true", help="(Optional) Use selenium; usually not needed")
# #     parser.add_argument("--clear-cache", action="store_true", help="Clear all caches before starting")

# #     # Publish knobs
# #     parser.add_argument("--publish", action="store_true", help="If set, publish to Firestore + GCS")
# #     parser.add_argument("--min-items", type=int, default=7, help="Only publish if >= this many places")
# #     parser.add_argument("--bucket", dest="gcs_bucket", default=GCS_BUCKET_DEFAULT, help="GCS bucket name")
# #     parser.add_argument("--collection", default="playlistsNew", help="Firestore collection name")
# #     parser.add_argument("--project", default=os.getenv("GOOGLE_CLOUD_PROJECT"), help="GCP project (optional)")
# #     parser.add_argument("--max-photos", type=int, default=G_IMAGE_COUNT, help=f"Max photos per place to upload (default {G_IMAGE_COUNT})")
# #     parser.add_argument("--skip-photos", action="store_true", help="Skip photo fetching/upload (still publishes doc)")

# #     args = parser.parse_args()

# #     if args.url and not args.city:
# #         parser.error("--city is required when using --url")

# #     # Handle cache clearing
# #     if args.clear_cache:
# #         cache_dir_path = Path(args.cache_dir) if args.cache_dir else Path(args.out_dir) / "cache"
# #         if cache_dir_path.exists():
# #             import shutil
# #             shutil.rmtree(cache_dir_path)
# #             print(f"Cleared cache directory: {cache_dir_path}")

# #     jobs = build_jobs_from_args(args.url, args.city, args.dataset_file)
# #     if not jobs:
# #         print("No jobs to process (check --url/--city or --dataset-file).")
# #         return

# #     # Selenium (optional)
# #     settings = {"LOG_LEVEL": "INFO"}
# #     if args.use_selenium:
# #         if SeleniumRequest is None:
# #             raise RuntimeError("scrapy-selenium not installed or import failed.")
# #         from webdriver_manager.chrome import ChromeDriverManager
# #         driver_path = ChromeDriverManager().install()
# #         settings.update({
# #             "DOWNLOADER_MIDDLEWARES": {"scrapy_selenium.SeleniumMiddleware": 800},
# #             "SELENIUM_DRIVER_NAME": "chrome",
# #             "SELENIUM_DRIVER_EXECUTABLE_PATH": driver_path,
# #             "SELENIUM_DRIVER_ARGUMENTS": ["--headless=new", "--no-sandbox", "--disable-gpu", "--window-size=1600,1200"],
# #         })

# #     process = CrawlerProcess(settings=settings)
# #     process.crawl(
# #         WanderlogPublishSpider,
# #         jobs=jobs,
# #         out_dir=args.out_dir,
# #         keep_ratio=args.keep_ratio,
# #         use_llm=args.use_llm,
# #         use_selenium=args.use_selenium,
# #         cache_dir=args.cache_dir,
# #         publish=args.publish,
# #         min_items=args.min_items,
# #         gcs_bucket=args.gcs_bucket,
# #         collection=args.collection,
# #         project=args.project,
# #         max_photos=args.max_photos,
# #         skip_photos=args.skip_photos
# #     )
# #     process.start()


# if __name__ == "__main__":
#     main()



# Repeated subcollection of places in collectionn
# import os
# import re
# import json
# import time
# import math
# import argparse
# import hashlib
# from pathlib import Path
# from typing import Any, Dict, List, Optional, Tuple
# from datetime import datetime

# import requests
# import scrapy
# from scrapy.crawler import CrawlerProcess

# # ---------------- Configuration / constants ----------------
# SCRIPT_VERSION = "1.2.0"

# # Presentation / metadata (paths/limits)
# GCS_BUCKET_DEFAULT = os.getenv("GCS_BUCKET", "mycasavsc.appspot.com")
# IMAGE_BASE = "https://storage.googleapis.com/{bucket}/playlistsNew_images/{list_id}/1.jpg"
# G_IMAGE_TEMPLATE = "https://storage.googleapis.com/{bucket}/playlistsPlaces/{list_id}/{placeId}/{n}.jpg"
# G_IMAGE_COUNT = 4  # max uploaded photos per place

# # Publish-only tags
# SUBTYPE_TAG = "poi"
# SOURCE_TAG = "wanderlog"

# # City map (manual; extend as you go city-by-city)
# CITY_ID_MAP = {
#     "Mumbai": "25",
#     # "India": "86661",
# }

# # Optional .env
# try:
#     from dotenv import load_dotenv
#     load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
# except Exception:
#     pass

# # Optional Selenium (off by default)
# try:
#     from scrapy_selenium import SeleniumRequest
# except Exception:
#     SeleniumRequest = None

# # OpenAI model selection (if LLM features used)
# OPENAI_MODEL = os.getenv("LC_MODEL", "gpt-4o-mini")

# # Lazy GCP clients
# FIRESTORE = None
# STORAGE = None


# # ---------------- Slug helpers / Firestore ID ----------------
# def _slugify_basic(text: str) -> str:
#     t = (text or "").strip().lower()
#     t = re.sub(r"[^\w\s-]+", "", t)
#     t = re.sub(r"[\s_-]+", "-", t)
#     return re.sub(r"^-+|-+$", "", t) or "untitled"


# def build_unique_slug(title: str, city: str, subtype: str, source_urls: List[str]) -> str:
#     base = f"{_slugify_basic(title)}-{_slugify_basic(city)}-{subtype}"
#     urls_str = str(sorted([u for u in source_urls if isinstance(u, str)])) if source_urls else ""
#     h = hashlib.md5(urls_str.encode("utf-8")).hexdigest()[:6] if urls_str else "na"
#     return f"{base}-{h}"


# class FirestoreIdAssigner:
#     """
#     Matches the reference Step 3 behavior:
#     - Reuse existing numeric doc ID if a doc with the same slug exists
#     - Otherwise assign the next numeric ID (max(existing numeric ids) + 1)
#     """
#     def __init__(self, collection: str, project: Optional[str] = None):
#         from google.cloud import firestore as _firestore
#         self.client = _firestore.Client(project=project) if project else _firestore.Client()
#         self.col = self.client.collection(collection)
#         self.next_id = self._compute_start_id()

#     def _compute_start_id(self) -> int:
#         max_id = 0
#         try:
#             for doc in self.col.select([]).stream():
#                 try:
#                     v = int(doc.id)
#                     if v > max_id:
#                         max_id = v
#                 except ValueError:
#                     continue
#         except Exception:
#             pass
#         return max_id + 1

#     def assign_doc_id_for_slug(self, slug: str) -> Tuple[str, bool]:
#         try:
#             try:
#                 from google.cloud.firestore_v1 import FieldFilter
#                 q = self.col.where(filter=FieldFilter("slug", "==", slug)).limit(1)
#             except Exception:
#                 q = self.col.where("slug", "==", slug).limit(1)
#             existing = list(q.stream())
#         except Exception:
#             existing = []
#         if existing:
#             return existing[0].id, True
#         new_id = str(self.next_id)
#         self.next_id += 1
#         return new_id, False


# # --------------- slugify fallback for other uses ---------------
# try:
#     from slugify import slugify  # pip install python-slugify
# except Exception:
#     def slugify(s: str) -> str:
#         s = (s or "").lower().strip()
#         s = re.sub(r"[^\w\s-]", "", s)
#         s = re.sub(r"[\s_-]+", "-", s)
#         return re.sub(r"^-+|-+$", "", s)


# # ---------------- Cache Management ----------------
# class CacheManager:
#     def __init__(self, cache_dir: Path):
#         self.cache_dir = Path(cache_dir)
#         self.cache_dir.mkdir(parents=True, exist_ok=True)
#         self.llm_validation_cache = self._load("llm_validation_cache.json")
#         self.llm_title_cache = self._load("llm_title_cache.json")
#         self.google_places_cache = self._load("google_places_cache.json")
#         self.mobx_data_cache = self._load("mobx_data_cache.json")
#         self.api_stats = {
#             "llm_validation_calls": 0,
#             "llm_title_calls": 0,
#             "google_places_calls": 0,
#             "cache_hits": 0,
#             "cache_misses": 0
#         }

#     def _load(self, filename: str) -> Dict:
#         p = self.cache_dir / filename
#         if p.exists():
#             try:
#                 return json.loads(p.read_text(encoding="utf-8"))
#             except Exception:
#                 return {}
#         return {}

#     def _save(self, data: Dict, filename: str):
#         p = self.cache_dir / filename
#         try:
#             p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass

#     def save_all(self):
#         self._save(self.llm_validation_cache, "llm_validation_cache.json")
#         self._save(self.llm_title_cache, "llm_title_cache.json")
#         self._save(self.google_places_cache, "google_places_cache.json")
#         self._save(self.mobx_data_cache, "mobx_data_cache.json")

#     def get_cache_key(self, **kwargs) -> str:
#         key_data = json.dumps(kwargs, sort_keys=True, ensure_ascii=False)
#         return hashlib.md5(key_data.encode("utf-8")).hexdigest()

#     def get_url_cache_key(self, url: str) -> str:
#         return hashlib.md5(url.encode("utf-8")).hexdigest()


# # ---------------- Category detection ----------------
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


# # ---------------- Utilities ----------------
# def iso_to_epoch_seconds(iso_str: str) -> Optional[int]:
#     if not iso_str: return None
#     try:
#         dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
#         return int(dt.timestamp())
#     except Exception:
#         return None


# def clean_text(text: Optional[str]) -> Optional[str]:
#     if not text: return text
#     return text.replace("\u2019", "'").replace("\u2014", "-").strip()


# def default_description(title: str) -> str:
#     return (f'Dive into "{title}" — a handpicked list of places with quick notes, links, '
#             f'and essentials for fast trip planning and discovery.')


# def build_slug_for_localfile(title: str, city: str, subtype: str, url: str) -> str:
#     m = re.search(r"/(\d+)(?:$|[?#])", url) or re.search(r"/(\d+)$", url)
#     tid = m.group(1) if m else "list"
#     return f"{slugify(title)}-{slugify(city)}-{slugify(subtype)}-{tid}"


# def explain_hits(pat: re.Pattern, text: str) -> List[str]:
#     try:
#         hits = pat.findall(text)
#         if not hits: return []
#         if isinstance(hits[0], tuple):
#             hits = [h for tup in hits for h in tup if h]
#         uniq = sorted({h.lower() for h in hits if isinstance(h, str) and h.strip()} )
#         return uniq
#     except Exception:
#         return []


# # ---------------- LLM helpers ----------------
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


# def llm_validate(cache_manager: CacheManager, category: str, name: str, context: str, city: str, page_title: str) -> Optional[bool]:
#     api_key = os.getenv("OPENAI_API_KEY")
#     if not api_key: return None
#     cache_key = cache_manager.get_cache_key(
#         category=category, name=name, context=context[:500], city=city, page_title=page_title
#     )
#     if cache_key in cache_manager.llm_validation_cache:
#         cache_manager.api_stats["cache_hits"] += 1
#         v = cache_manager.llm_validation_cache[cache_key]
#         return True if v == "true" else False if v == "false" else None
#     cache_manager.api_stats["cache_misses"] += 1
#     cache_manager.api_stats["llm_validation_calls"] += 1
#     prompt = f"""You are validating inclusion for a travel list titled "{page_title}".
# City/Region: {city}
# Place: "{name}"
# Decision category: {category}

# Context:
# {context}

# Question: Does this place CLEARLY belong in the "{category}" category?
# Answer with only one token: YES or NO."""
#     result = None
#     try:
#         from openai import OpenAI
#         client = OpenAI()
#         resp = client.chat.completions.create(
#             model=OPENAI_MODEL,
#             messages=[{"role": "user", "content": prompt}],
#             temperature=0,
#         )
#         out = (resp.choices[0].message.content or "").strip().lower()
#         if out.startswith("yes"): result = True
#         elif out.startswith("no"): result = False
#     except Exception:
#         pass
#     if result is None:
#         try:
#             import openai as _openai
#             _openai.api_key = api_key
#             resp = _openai.ChatCompletion.create(
#                 model=OPENAI_MODEL,
#                 messages=[{"role": "user", "content": prompt}],
#                 temperature=0,
#             )
#             out = resp.choices[0].message["content"].strip().lower()
#             if out.startswith("yes"): result = True
#             elif out.startswith("no"): result = False
#         except Exception:
#             result = None
#     cache_manager.llm_validation_cache[cache_key] = "true" if result is True else "false" if result is False else "none"
#     return result


# def is_valid_title(title: str, city: str, category: str) -> bool:
#     if not title or len(title.strip()) < 3: return False
#     title = title.strip()
#     words = len(title.split())
#     if words < 3 or words > 10: return False
#     if re.search(r"\d", title): return False
#     bad = [r"\b(top|best|#|number|first|second|third)\b",
#            r"\b(guide|list|collection)\b",
#            r"^(the\s+)?(ultimate|complete|definitive)\b"]
#     for pat in bad:
#         if re.search(pat, title.lower()): return False
#     contains_location = city.lower() in title.lower()
#     contains_category = any(w in title.lower() for w in category.lower().split())
#     if not (contains_location or contains_category): return False
#     if len(title) < 10 or len(title) > 80: return False
#     return True


# def create_fallback_title(city: str, category: str) -> str:
#     m = {
#         "beaches": f"Beautiful Beaches of {city}",
#         "national parks": f"Wild {city} Parks",
#         "waterfalls": f"Stunning {city} Waterfalls",
#         "castles": f"Historic {city} Castles",
#         "photo spots": f"Picture Perfect {city}",
#         "romantic places": f"Romantic {city} Escapes",
#         "architecture": f"Architectural Gems of {city}"
#     }
#     return m.get(category.lower(), f"Discover {city}")


# def generate_playlist_title(cache_manager: CacheManager, city: str, category: str, sample_names: List[str], page_title: str) -> str:
#     api_key = os.getenv("OPENAI_API_KEY")
#     if not api_key:
#         return create_fallback_title(city, category)
#     cache_key = cache_manager.get_cache_key(city=city, category=category, sample_names=sample_names[:3], page_title=page_title[:100])
#     if cache_key in cache_manager.llm_title_cache:
#         cache_manager.api_stats["cache_hits"] += 1
#         return cache_manager.llm_title_cache[cache_key]
#     cache_manager.api_stats["cache_misses"] += 1
#     cache_manager.api_stats["llm_title_calls"] += 1
#     prompt = f"""Create an engaging travel playlist title.

# Context:
# - City/Region: {city}
# - Category: {category}
# - Original page title: "{page_title}"
# - Featured places: {", ".join(sample_names[:3])}

# Rules:
# - 4–8 words
# - Travel/discovery focused
# - NO numbers or counts
# - Memorable & specific to {city} or {category}

# Return only the title:"""
#     result = create_fallback_title(city, category)
#     try:
#         from openai import OpenAI
#         client = OpenAI()
#         resp = client.chat.completions.create(
#             model=OPENAI_MODEL,
#             messages=[{"role": "user", "content": prompt}],
#             temperature=0.4,
#         )
#         t = (resp.choices[0].message.content or "").strip().strip('"\'')
#         if is_valid_title(t, city, category):
#             result = t
#     except Exception:
#         pass
#     if result == create_fallback_title(city, category):
#         try:
#             import openai as _openai
#             _openai.api_key = api_key
#             resp = _openai.ChatCompletion.create(
#                 model=OPENAI_MODEL,
#                 messages=[{"role": "user", "content": prompt}],
#                 temperature=0.4,
#             )
#             t = resp.choices[0].message["content"].strip().strip('"\'')
#             if is_valid_title(t, city, category):
#                 result = t
#         except Exception:
#             pass
#     cache_manager.llm_title_cache[cache_key] = result
#     return result


# # ---------------- Merge MOBX data ----------------
# def merge_metadata_and_blocks(place_meta: List[Dict[str, Any]], blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
#             "block_desc": (addendum.strip() or None),
#             "block_imageKeys": b.get("imageKeys") or [],
#             "selectedImageKey": b.get("selectedImageKey"),
#         })
#     merged = list(meta_map.values())
#     merged.sort(key=lambda r: (999999 if r.get("block_id") is None else int(r.get("block_id")), -(r.get("rating") or 0)))
#     return merged


# # ---------------- Score / trim ----------------
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


# # ---------------- Google Photos + GCS ----------------
# def get_place_photo_refs(cache: CacheManager, place_id: str, api_key: str) -> List[str]:
#     """
#     Lightweight Place Details REST call to retrieve photo refs; cached per place_id.
#     """
#     if not place_id: return []
#     cache_key = f"refs::{place_id}"
#     if cache_key in cache.google_places_cache:
#         cache.api_stats["cache_hits"] += 1
#         return cache.google_places_cache[cache_key] or []
#     cache.api_stats["cache_misses"] += 1
#     cache.api_stats["google_places_calls"] += 1
#     url = "https://maps.googleapis.com/maps/api/place/details/json"
#     params = {"place_id": place_id, "fields": "photo", "key": api_key}
#     try:
#         r = requests.get(url, params=params, timeout=12)
#         r.raise_for_status()
#         js = r.json()
#         photos = (js.get("result") or {}).get("photos") or []
#         refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]
#         refs = refs[:10]
#     except Exception:
#         refs = []
#     cache.google_places_cache[cache_key] = refs
#     return refs


# def fetch_photo_bytes(photo_ref: str, api_key: str, maxwidth: int = 1600) -> Optional[bytes]:
#     if not photo_ref: return None
#     url = "https://maps.googleapis.com/maps/api/place/photo"
#     params = {"maxwidth": str(maxwidth), "photo_reference": photo_ref, "key": api_key}
#     try:
#         r = requests.get(url, params=params, timeout=30, allow_redirects=True)
#         r.raise_for_status()
#         return r.content
#     except Exception:
#         return None


# def gcs_upload_bytes(storage_client, bucket_name: str, blob_path: str, data: bytes, content_type: str = "image/jpeg", make_public: bool = True) -> str:
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(blob_path)
#     blob.cache_control = "public, max-age=31536000"
#     blob.upload_from_string(data, content_type=content_type)
#     try:
#         if make_public:
#             blob.make_public()
#     except Exception:
#         pass
#     return f"https://storage.googleapis.com/{bucket_name}/{blob_path}"


# def fill_place_images_and_cover(playlist: Dict[str, Any],
#                                 bucket_name: str,
#                                 max_photos: int = G_IMAGE_COUNT,
#                                 skip_photos: bool = False,
#                                 cache: Optional[CacheManager] = None) -> None:
#     """
#     Populate g_image_urls for each place (using Maps Photo API). Also ensure a cover image exists.
#     - Upload up to `max_photos` per place to: playlistsPlaces/{list_id}/{placeId}/{i}.jpg
#     - Set playlist['imageUrl'] to playlistsNew_images/{list_id}/1.jpg from first uploaded photo
#     """
#     if skip_photos: return
#     api_key = os.getenv("GOOGLE_MAPS_API_KEY")
#     if not api_key: return

#     global STORAGE
#     if STORAGE is None:
#         from google.cloud import storage as _storage
#         STORAGE = _storage.Client()

#     list_id = playlist["list_id"]
#     cover_done = False

#     for place in playlist["subcollections"]["places"]:
#         pid = place.get("placeId")
#         if not pid:
#             place["g_image_urls"] = []
#             continue

#         refs = get_place_photo_refs(cache, pid, api_key) if cache else []
#         uploaded_urls: List[str] = []
#         idx_used = 0

#         for i, ref in enumerate(refs[:max_photos], start=1):
#             data = fetch_photo_bytes(ref, api_key)
#             if not data:
#                 continue
#             idx_used += 1
#             blob_path = f"playlistsPlaces/{list_id}/{pid}/{i}.jpg"
#             url = gcs_upload_bytes(STORAGE, bucket_name, blob_path, data, "image/jpeg", make_public=True)
#             uploaded_urls.append(url)

#             if not cover_done and i == 1:
#                 cover_blob = f"playlistsNew_images/{list_id}/1.jpg"
#                 gcs_upload_bytes(STORAGE, bucket_name, cover_blob, data, "image/jpeg", make_public=True)
#                 playlist["imageUrl"] = IMAGE_BASE.format(bucket=bucket_name, list_id=list_id)
#                 cover_done = True

#         # Prefer the canonical template URLs (match frontend expectations)
#         if idx_used:
#             place["g_image_urls"] = [
#                 G_IMAGE_TEMPLATE.format(bucket=bucket_name, list_id=list_id, placeId=pid, n=n)
#                 for n in range(1, idx_used + 1)
#             ]
#         else:
#             place["g_image_urls"] = []


# # ---------------- Firestore publish (doc + subcollection) ----------------
# def publish_playlist_to_firestore(playlist: Dict[str, Any],
#                                   collection: str,
#                                   project: Optional[str],
#                                   assigned_doc_id: str) -> str:
#     """
#     Upsert the playlist to the given doc id and replace 'places' subcollection.
#     """
#     global FIRESTORE
#     if FIRESTORE is None:
#         from google.cloud import firestore as _firestore
#         FIRESTORE = _firestore.Client(project=project) if project else _firestore.Client()

#     col = FIRESTORE.collection(collection)
#     doc_ref = col.document(assigned_doc_id)

#     # Write main doc
#     doc_ref.set(playlist, merge=False)

#     # Replace subcollection "places"
#     sub = doc_ref.collection("places")
#     try:
#         old = list(sub.stream())
#         for i in range(0, len(old), 200):
#             batch = FIRESTORE.batch()
#             for doc in old[i:i+200]:
#                 batch.delete(doc.reference)
#             batch.commit()
#     except Exception:
#         pass

#     places = playlist.get("subcollections", {}).get("places", [])
#     for i in range(0, len(places), 450):
#         batch = FIRESTORE.batch()
#         for item in places[i:i+450]:
#             sub_id = item.get("placeId") or item.get("_id")
#             batch.set(sub.document(sub_id), item)
#         batch.commit()

#     return assigned_doc_id


# # ---------------- Dataset loader ----------------
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


# # ---------------- Spider ----------------
# class WanderlogPublishSpider(scrapy.Spider):
#     name = "wanderlog_publish"
#     custom_settings = {"LOG_LEVEL": "INFO"}

#     def __init__(self,
#                  jobs: List[Dict[str, Any]],
#                  out_dir: str,
#                  keep_ratio: float,
#                  use_llm: bool,
#                  use_selenium: bool,
#                  cache_dir: str = None,
#                  publish: bool = False,
#                  min_items: int = 7,
#                  gcs_bucket: str = None,
#                  collection: str = "playlistsNew",
#                  project: Optional[str] = None,
#                  max_photos: int = G_IMAGE_COUNT,
#                  skip_photos: bool = False,
#                  *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.jobs = jobs
#         self.out_dir = Path(out_dir)
#         self.keep_ratio = float(keep_ratio)
#         self.use_llm = bool(use_llm and bool(os.getenv("OPENAI_API_KEY")))
#         self.use_selenium = bool(use_selenium and SeleniumRequest is not None)
#         self.publish = bool(publish)
#         self.min_items = int(min_items)
#         self.gcs_bucket = gcs_bucket or GCS_BUCKET_DEFAULT
#         self.collection = collection
#         self.project = project
#         self.max_photos = int(max_photos)
#         self.skip_photos = bool(skip_photos)

#         self.out_dir.mkdir(parents=True, exist_ok=True)
#         cache_dir_path = Path(cache_dir) if cache_dir else self.out_dir / "cache"
#         self.cache_manager = CacheManager(cache_dir_path)

#         # Firestore numeric ID assigner (only if publishing)
#         self.id_assigner = None
#         if self.publish:
#             self.id_assigner = FirestoreIdAssigner(collection=self.collection, project=self.project)

#         self.aggregate_report: Dict[str, Any] = {
#             "script_version": SCRIPT_VERSION,
#             "start_ts": int(time.time()),
#             "llm_enabled_flag": bool(use_llm),
#             "llm_client_loaded": bool(os.getenv("OPENAI_API_KEY")),
#             "model": OPENAI_MODEL if self.use_llm else None,
#             "cache_directory": str(cache_dir_path),
#             "publish_enabled": self.publish,
#             "min_items": self.min_items,
#             "gcs_bucket": self.gcs_bucket,
#             "collection": self.collection,
#             "jobs": []
#         }

#     def start_requests(self):
#         for idx, job in enumerate(self.jobs):
#             url = job["url"]
#             url_cache_key = self.cache_manager.get_url_cache_key(url)
#             if url_cache_key in self.cache_manager.mobx_data_cache:
#                 self.logger.info(f"[{idx}] Using cached data for: {url}")
#                 self.cache_manager.api_stats["cache_hits"] += 1
#                 self.parse_cached_data(job, idx, url_cache_key)
#             else:
#                 self.cache_manager.api_stats["cache_misses"] += 1
#                 meta = {"job": job, "job_index": idx, "url_cache_key": url_cache_key}
#                 if self.use_selenium:
#                     yield SeleniumRequest(url=url, callback=self.parse_page, meta=meta, wait_time=2)
#                 else:
#                     yield scrapy.Request(url=url, callback=self.parse_page, meta=meta)

#     def parse_cached_data(self, job: Dict[str, Any], job_index: int, url_cache_key: str):
#         cached = self.cache_manager.mobx_data_cache[url_cache_key]
#         data = cached.get("data", {})
#         page_title = cached.get("page_title", job.get("title", "Untitled"))
#         url = job.get("url", "")
#         self.process_mobx_data(job, job_index, data, page_title, url, from_cache=True)

#     def parse_page(self, response):
#         job = response.meta["job"]
#         job_index = response.meta["job_index"]
#         url_cache_key = response.meta["url_cache_key"]
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
#         if not script_text:
#             self.logger.error(f"[{job_index}] MOBX not found: {job['url']}")
#             return
#         m = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text, re.DOTALL)
#         if not m:
#             self.logger.error(f"[{job_index}] MOBX parse failed: {job['url']}")
#             return
#         try:
#             mobx_json = json.loads(m.group(1))
#         except json.JSONDecodeError:
#             self.logger.error(f"[{job_index}] MOBX JSON decode failed: {job['url']}")
#             return
#         data = (mobx_json.get("placesListPage") or {}).get("data") or {}
#         page_title = data.get("title") or (job.get("title") or "Untitled")
#         self.cache_manager.mobx_data_cache[url_cache_key] = {"data": data, "page_title": page_title, "cached_ts": int(time.time())}
#         self.process_mobx_data(job, job_index, data, page_title, response.url, from_cache=False)

#     def process_mobx_data(self, job: Dict[str, Any], job_index: int, data: Dict[str, Any], page_title: str, url: str, from_cache: bool = False):
#         place_meta = data.get("placeMetadata") or []
#         blocks = []
#         for sec in (data.get("boardSections") or []):
#             for b in (sec.get("blocks") or []):
#                 if b.get("type") == "place":
#                     blocks.append(b)

#         category = (job.get("category_hint") or "").lower().strip() or detect_category_from_title(page_title)
#         merged = merge_metadata_and_blocks(place_meta, blocks)

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
#                 ans = llm_validate(cache_manager=self.cache_manager, category=category, name=name, context=ctx,
#                                    city=job.get("city") or "", page_title=page_title)
#                 if not from_cache:
#                     llm_calls += 1
#                 decided_by = "llm"
#                 if ans is True:
#                     verdict, reason = True, "llm_yes"
#                 elif ans is False:
#                     verdict, reason = False, "llm_no"
#                 else:
#                     verdict, reason = False, "llm_failed"

#             place_reports.append({
#                 "placeId": it.get("placeId"),
#                 "name": name,
#                 "accepted": bool(verdict),
#                 "decided_by": decided_by,
#                 "reason": reason,
#                 "pos_hits": hits.get("pos_hits", []),
#                 "neg_hits": hits.get("neg_hits", []),
#                 "context_used_excerpt": ctx[:220]
#             })
#             if verdict: accepted.append(it)
#             else: rejected.append(it)

#         curated = trim_and_light_shuffle(accepted, keep_ratio=self.keep_ratio)

#         # Title generation (numbers disallowed)
#         final_title = page_title
#         title_calls = 0
#         if self.use_llm and curated:
#             sample_names = [p.get("name", "") for p in curated[:3]]
#             gen_title = generate_playlist_title(
#                 cache_manager=self.cache_manager,
#                 city=job.get("city") or "India",
#                 category=category,
#                 sample_names=sample_names,
#                 page_title=page_title
#             )
#             if gen_title and gen_title != create_fallback_title(job.get("city", "India"), category):
#                 final_title = gen_title
#             if not from_cache:
#                 title_calls += 1

#         # Build slug (for Firestore numeric doc id assignment)
#         source_urls = [url] if url else []
#         city_name = job.get("city") or "India"
#         slug = build_unique_slug(final_title, city_name, SUBTYPE_TAG, source_urls)

#         # Determine Firestore doc id (numeric) if publishing
#         list_id_str = None
#         existed = False
#         if self.publish:
#             if not self.id_assigner:
#                 self.id_assigner = FirestoreIdAssigner(collection=self.collection, project=self.project)
#             list_id_str, existed = self.id_assigner.assign_doc_id_for_slug(slug)
#         else:
#             # Not publishing: make a stable-ish local-only id from URL
#             m = re.search(r"/(\d+)(?:$|[?#])", url or "")
#             list_id_str = m.group(1) if m else f"{slugify(city_name)}-local"

#         # Build playlist JSON (we will override imageUrl after uploads)
#         playlist = {
#             "list_id": str(list_id_str),
#             "imageUrl": IMAGE_BASE.format(bucket=self.gcs_bucket, list_id=list_id_str),
#             "description": default_description(final_title),
#             "source_urls": source_urls,
#             "source": SOURCE_TAG,
#             "category": "Travel",
#             "title": final_title,
#             "city_id": CITY_ID_MAP.get(city_name, city_name),
#             "subtype": SUBTYPE_TAG,
#             "city": city_name,
#             "created_ts": int(time.time()),
#             "slug": slug,
#             "subcollections": {"places": []}
#         }

#         # Places
#         for it in curated:
#             pid = it.get("placeId")
#             reviews = []
#             for r in (it.get("raw_reviews") or [])[:3]:
#                 reviews.append({
#                     "rating": int(r.get("rating", 0)),
#                     "text": clean_text(r.get("reviewText")),
#                     "author_name": r.get("reviewerName") or "",
#                     "relative_time_description": "",
#                     "time": iso_to_epoch_seconds(r.get("time") or "") or 0,
#                     "profile_photo_url": ""
#                 })

#             place_doc = {
#                 "_id": pid or (it.get("name") or "unknown"),
#                 "generalDescription": it.get("generalDescription"),
#                 "utcOffset": int(it.get("utcOffset") if it.get("utcOffset") is not None else 330),
#                 "maxMinutesSpent": it.get("maxMinutesSpent"),
#                 "longitude": it.get("longitude"),
#                 "rating": it.get("rating") or 0,
#                 "numRatings": it.get("numRatings") or 0,
#                 "sources": it.get("sources") or [],
#                 "imageKeys": (it.get("imageKeys") or []) + (it.get("block_imageKeys") or []),
#                 "openingPeriods": it.get("openingPeriods") or [],
#                 "name": it.get("name"),
#                 "placeId": pid,
#                 "internationalPhoneNumber": it.get("internationalPhoneNumber"),
#                 "reviews": reviews,
#                 "permanentlyClosed": bool(it.get("permanentlyClosed")),
#                 "priceLevel": it.get("priceLevel"),
#                 "tripadvisorRating": 0,
#                 "description": None,
#                 "website": it.get("website"),
#                 "index": it.get("_final_index") or 1,
#                 "id": "",
#                 "categories": it.get("categories") or [],
#                 "tripadvisorNumRatings": 0,
#                 "g_image_urls": [],
#                 "ratingDistribution": it.get("ratingDistribution") or {},
#                 "minMinutesSpent": it.get("minMinutesSpent"),
#                 "latitude": it.get("latitude"),
#                 "address": it.get("address"),
#                 "travel_time": it.get("travel_time")
#             }
#             playlist["subcollections"]["places"].append(place_doc)

#         playlist["subcollections"]["places"].sort(key=lambda x: x["index"])

#         # Write local artifacts always
#         base = job_out_basename(job, page_title)
#         playlist_path = self.out_dir / f"{base}.json"
#         report_path = self.out_dir / f"{base}.report.json"
#         playlist_path.write_text(json.dumps(playlist, ensure_ascii=False, indent=2), encoding="utf-8")

#         published = False
#         firestore_id = None

#         # PUBLISH only if we have >= min_items
#         if self.publish and len(playlist["subcollections"]["places"]) >= self.min_items:
#             # 1) Upload photos & set cover
#             try:
#                 fill_place_images_and_cover(
#                     playlist=playlist,
#                     bucket_name=self.gcs_bucket,
#                     max_photos=self.max_photos,
#                     skip_photos=self.skip_photos,
#                     cache=self.cache_manager
#                 )
#             except Exception as e:
#                 self.logger.warning(f"[{job_index}] image upload step failed: {e}")

#             # 2) Upsert into Firestore with assigned numeric ID
#             try:
#                 firestore_id = publish_playlist_to_firestore(
#                     playlist=playlist,
#                     collection=self.collection,
#                     project=self.project,
#                     assigned_doc_id=str(list_id_str)
#                 )
#                 published = True
#             except Exception as e:
#                 self.logger.warning(f"[{job_index}] firestore publish failed: {e}")
#         else:
#             self.logger.info(f"[{job_index}] Not published (keep={len(playlist['subcollections']['places'])} < min={self.min_items}).")

#         # Report
#         job_report = {
#             "job_index": job_index,
#             "city": city_name,
#             "category_used": category,
#             "url": url,
#             "title": page_title,
#             "final_title": final_title,
#             "from_cache": from_cache,
#             "counts": {
#                 "meta": len(place_meta),
#                 "blocks": len(blocks),
#                 "merged": len(merged),
#                 "accepted_before_trim": len(accepted),
#                 "rejected": len(rejected),
#                 "accepted_final": len(playlist['subcollections']['places']),
#             },
#             "llm": {
#                 "enabled": self.use_llm,
#                 "model": OPENAI_MODEL if self.use_llm else None,
#                 "validation_calls": llm_calls,
#                 "title_calls": title_calls
#             },
#             "publish": {
#                 "enabled": self.publish,
#                 "min_items": self.min_items,
#                 "published": published,
#                 "firestore_doc_id": firestore_id,
#                 "bucket": self.gcs_bucket if published else None
#             },
#             "out_files": {
#                 "playlist_json": str(playlist_path),
#                 "report_json": str(report_path)
#             },
#             "created_ts": int(time.time())
#         }
#         report_path.write_text(json.dumps(job_report, ensure_ascii=False, indent=2), encoding="utf-8")
#         self.aggregate_report["jobs"].append(job_report)

#         pub_msg = "PUBLISHED" if published else "DRYRUN"
#         self.logger.info(f"[{job_index}] {final_title} → kept {len(playlist['subcollections']['places'])}/{len(merged)} | LLM calls={llm_calls + title_calls} [{pub_msg}]")
#         self.logger.info(f"  wrote: {playlist_path}")
#         self.logger.info(f"  wrote: {report_path}")

#     def closed(self, reason):
#         self.cache_manager.save_all()
#         self.aggregate_report.update({
#             "end_ts": int(time.time()),
#             "finish_reason": reason,
#             "cache_stats": self.cache_manager.api_stats,
#             "cache_sizes": {
#                 "llm_validation_cache": len(self.cache_manager.llm_validation_cache),
#                 "llm_title_cache": len(self.cache_manager.llm_title_cache),
#                 "google_places_cache": len(self.cache_manager.google_places_cache),
#                 "mobx_data_cache": len(self.cache_manager.mobx_data_cache)
#             }
#         })
#         agg_path = self.out_dir / "_aggregate_report.json"
#         agg_path.write_text(json.dumps(self.aggregate_report, ensure_ascii=False, indent=2), encoding="utf-8")
#         hit = self.cache_manager.api_stats["cache_hits"]
#         miss = self.cache_manager.api_stats["cache_misses"]
#         rate = (hit / max(1, hit + miss)) * 100
#         self.logger.info(f"Cache hit rate: {rate:.1f}%")
#         self.logger.info(f"Aggregate report → {agg_path}")


# # ---------------- Runner ----------------
# def main():
#     parser = argparse.ArgumentParser(description="Wanderlog → curated playlists → (optional) publish to Firestore + GCS.")
#     g = parser.add_mutually_exclusive_group(required=True)
#     g.add_argument("--url", help="Single Wanderlog list URL")
#     g.add_argument("--dataset-file", help="Path to JSON dataset (city -> category -> [{title,url}])")

#     parser.add_argument("--city", help="City name (required if using --url)")
#     parser.add_argument("--out-dir", default="trial_playlists", help="Output directory for JSONs and reports")
#     parser.add_argument("--cache-dir", help="Cache directory (defaults to out-dir/cache)")
#     parser.add_argument("--keep-ratio", type=float, default=0.7, help="Fraction to keep after trimming (0..1)")
#     parser.add_argument("--use-llm", action="store_true", help="Enable LLM for uncertain cases (requires OPENAI_API_KEY)")
#     parser.add_argument("--use-selenium", action="store_true", help="(Optional) Use selenium; usually not needed")
#     parser.add_argument("--clear-cache", action="store_true", help="Clear all caches before starting")

#     # Publish knobs
#     parser.add_argument("--publish", action="store_true", help="If set, publish to Firestore + GCS")
#     parser.add_argument("--min-items", type=int, default=7, help="Only publish if >= this many places")
#     parser.add_argument("--bucket", dest="gcs_bucket", default=GCS_BUCKET_DEFAULT, help="GCS bucket name")
#     parser.add_argument("--collection", default="playlistsNew", help="Firestore collection name")
#     parser.add_argument("--project", default=os.getenv("GOOGLE_CLOUD_PROJECT"), help="GCP project (optional)")
#     parser.add_argument("--max-photos", type=int, default=G_IMAGE_COUNT, help=f"Max photos per place to upload (default {G_IMAGE_COUNT})")
#     parser.add_argument("--skip-photos", action="store_true", help="Skip photo fetching/upload (still publishes doc)")

#     args = parser.parse_args()

#     if args.url and not args.city:
#         parser.error("--city is required when using --url")

#     # Handle cache clearing
#     if args.clear_cache:
#         cache_dir_path = Path(args.cache_dir) if args.cache_dir else Path(args.out_dir) / "cache"
#         if cache_dir_path.exists():
#             import shutil
#             shutil.rmtree(cache_dir_path)
#             print(f"Cleared cache directory: {cache_dir_path}")

#     jobs = build_jobs_from_args(args.url, args.city, args.dataset_file)
#     if not jobs:
#         print("No jobs to process (check --url/--city or --dataset-file).")
#         return

#     # Selenium (optional)
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
#         WanderlogPublishSpider,
#         jobs=jobs,
#         out_dir=args.out_dir,
#         keep_ratio=args.keep_ratio,
#         use_llm=args.use_llm,
#         use_selenium=args.use_selenium,
#         cache_dir=args.cache_dir,
#         publish=args.publish,
#         min_items=args.min_items,
#         gcs_bucket=args.gcs_bucket,
#         collection=args.collection,
#         project=args.project,
#         max_photos=args.max_photos,
#         skip_photos=args.skip_photos
#     )
#     process.start()


# if __name__ == "__main__":
#     main()








# import os
# import re
# import json
# import time
# import math
# import argparse
# import hashlib
# from pathlib import Path
# from typing import Any, Dict, List, Optional, Tuple
# from datetime import datetime

# import requests
# import scrapy
# from scrapy.crawler import CrawlerProcess

# SUBTYPE_TAG = "poi"        # publish-only subtype
# SOURCE_TAG  = "wanderlog"  # publish-only source



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

# # --- GCP clients (lazy import in publish stage) ---
# FIRESTORE = None
# STORAGE = None

# # --- Model selection ---
# OPENAI_MODEL = os.getenv("LC_MODEL", "gpt-4o-mini")  # <= ensure this is valid
# SCRIPT_VERSION = "0.9.0"

# # ---------- Cache Management ----------
# class CacheManager:
#     def __init__(self, cache_dir: Path):
#         self.cache_dir = Path(cache_dir)
#         self.cache_dir.mkdir(parents=True, exist_ok=True)
#         self.llm_validation_cache = self.load_cache("llm_validation_cache.json")
#         self.llm_title_cache = self.load_cache("llm_title_cache.json")
#         self.google_places_cache = self.load_cache("google_places_cache.json")  # stores photo_refs by placeId
#         self.mobx_data_cache = self.load_cache("mobx_data_cache.json")
#         self.api_stats = {
#             "llm_validation_calls": 0,
#             "llm_title_calls": 0,
#             "google_places_calls": 0,
#             "cache_hits": 0,
#             "cache_misses": 0
#         }
#     def load_cache(self, filename: str) -> Dict:
#         p = self.cache_dir / filename
#         if p.exists():
#             try:
#                 return json.loads(p.read_text(encoding="utf-8"))
#             except Exception:
#                 return {}
#         return {}
#     def save_cache(self, data: Dict, filename: str):
#         p = self.cache_dir / filename
#         try:
#             p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
#         except Exception:
#             pass
#     def save_all_caches(self):
#         self.save_cache(self.llm_validation_cache, "llm_validation_cache.json")
#         self.save_cache(self.llm_title_cache, "llm_title_cache.json")
#         self.save_cache(self.google_places_cache, "google_places_cache.json")
#         self.save_cache(self.mobx_data_cache, "mobx_data_cache.json")
#     def get_cache_key(self, **kwargs) -> str:
#         key_data = json.dumps(kwargs, sort_keys=True, ensure_ascii=False)
#         return hashlib.md5(key_data.encode("utf-8")).hexdigest()
#     def get_url_cache_key(self, url: str) -> str:
#         return hashlib.md5(url.encode("utf-8")).hexdigest()

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
#     if not iso_str: return None
#     try:
#         dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
#         return int(dt.timestamp())
#     except Exception:
#         return None

# def clean_text(text: Optional[str]) -> Optional[str]:
#     if not text: return text
#     return text.replace("\u2019", "'").replace("\u2014", "-").strip()

# def default_description(title: str) -> str:
#     return (f'Dive into "{title}" — a handpicked list of places with quick notes, links, '
#             f'and essentials for fast trip planning and discovery.')

# def build_slug(title: str, city: str, subtype: str, url: str) -> str:
#     m = re.search(r"/(\d+)(?:$|[?#])", url) or re.search(r"/(\d+)$", url)
#     tid = m.group(1) if m else "list"
#     return f"{slugify(title)}-{slugify(city)}-{slugify(subtype)}-{tid}"

# def explain_hits(pat: re.Pattern, text: str) -> List[str]:
#     try:
#         hits = pat.findall(text)
#         if not hits: return []
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

# # ---------- Reviews mapping ----------
# def pick_alternate_reviews(reviews: List[Dict[str, Any]], k: int = 3) -> List[Dict[str, Any]]:
#     if not reviews: return []
#     def _key(r):
#         rating = r.get("rating", 0)
#         ts = iso_to_epoch_seconds(r.get("time") or "") or 0
#         return (rating, ts)
#     sorted_reviews = sorted(reviews, key=_key, reverse=True)
#     out = []
#     for i, r in enumerate(sorted_reviews):
#         if len(out) >= k: break
#         if i % 2 == 0:
#             out.append({
#                 "rating": int(r.get("rating", 0)),
#                 "text": clean_text(r.get("reviewText")),
#                 "author_name": r.get("reviewerName") or "",
#                 "relative_time_description": "",
#                 "time": iso_to_epoch_seconds(r.get("time")) or 0,
#                 "profile_photo_url": ""
#             })
#     return out

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

# def llm_validate(cache_manager: CacheManager, category: str, name: str, context: str, city: str, page_title: str) -> Optional[bool]:
#     api_key = os.getenv("OPENAI_API_KEY")
#     if not api_key: return None
#     cache_key = cache_manager.get_cache_key(
#         category=category, name=name, context=context[:500], city=city, page_title=page_title
#     )
#     if cache_key in cache_manager.llm_validation_cache:
#         cache_manager.api_stats["cache_hits"] += 1
#         v = cache_manager.llm_validation_cache[cache_key]
#         return True if v == "true" else False if v == "false" else None
#     cache_manager.api_stats["cache_misses"] += 1
#     cache_manager.api_stats["llm_validation_calls"] += 1
#     prompt = f"""You are validating inclusion for a travel list titled "{page_title}".
# City/Region: {city}
# Place: "{name}"
# Decision category: {category}

# Context:
# {context}

# Question: Does this place CLEARLY belong in the "{category}" category?
# Answer with only one token: YES or NO."""
#     result = None
#     # try new SDK
#     try:
#         from openai import OpenAI
#         client = OpenAI()
#         resp = client.chat.completions.create(
#             model=OPENAI_MODEL,
#             messages=[{"role": "user", "content": prompt}],
#             temperature=0,
#         )
#         out = (resp.choices[0].message.content or "").strip().lower()
#         if out.startswith("yes"): result = True
#         elif out.startswith("no"): result = False
#     except Exception:
#         pass
#     # fallback old SDK
#     if result is None:
#         try:
#             import openai as _openai
#             _openai.api_key = api_key
#             resp = _openai.ChatCompletion.create(
#                 model=OPENAI_MODEL,
#                 messages=[{"role": "user", "content": prompt}],
#                 temperature=0,
#             )
#             out = resp.choices[0].message["content"].strip().lower()
#             if out.startswith("yes"): result = True
#             elif out.startswith("no"): result = False
#         except Exception:
#             result = None
#     cache_manager.llm_validation_cache[cache_key] = "true" if result is True else "false" if result is False else "none"
#     return result

# def is_valid_title(title: str, city: str, category: str) -> bool:
#     if not title or len(title.strip()) < 3: return False
#     title = title.strip()
#     words = len(title.split())
#     if words < 3 or words > 10: return False
#     if re.search(r"\d", title): return False
#     bad = [r"\b(top|best|#|number|first|second|third)\b",
#            r"\b(guide|list|collection)\b",
#            r"^(the\s+)?(ultimate|complete|definitive)\b"]
#     for pat in bad:
#         if re.search(pat, title.lower()): return False
#     contains_location = city.lower() in title.lower()
#     contains_category = any(w in title.lower() for w in category.lower().split())
#     if not (contains_location or contains_category): return False
#     if len(title) < 10 or len(title) > 80: return False
#     return True

# def create_fallback_title(city: str, category: str) -> str:
#     m = {
#         "beaches": f"Beautiful Beaches of {city}",
#         "national parks": f"Wild {city} Parks",
#         "waterfalls": f"Stunning {city} Waterfalls",
#         "castles": f"Historic {city} Castles",
#         "photo spots": f"Picture Perfect {city}",
#         "romantic places": f"Romantic {city} Escapes",
#         "architecture": f"Architectural Gems of {city}"
#     }
#     return m.get(category.lower(), f"Discover {city}")

# def generate_playlist_title(cache_manager: CacheManager, city: str, category: str, sample_names: List[str], page_title: str) -> str:
#     api_key = os.getenv("OPENAI_API_KEY")
#     if not api_key:
#         return create_fallback_title(city, category)
#     cache_key = cache_manager.get_cache_key(city=city, category=category, sample_names=sample_names[:3], page_title=page_title[:100])
#     if cache_key in cache_manager.llm_title_cache:
#         cache_manager.api_stats["cache_hits"] += 1
#         return cache_manager.llm_title_cache[cache_key]
#     cache_manager.api_stats["cache_misses"] += 1
#     cache_manager.api_stats["llm_title_calls"] += 1
#     prompt = f"""Create an engaging travel playlist title.

# Context:
# - City/Region: {city}
# - Category: {category}
# - Original page title: "{page_title}"
# - Featured places: {", ".join(sample_names[:3])}

# Rules:
# - 4–8 words
# - Travel/discovery focused
# - NO numbers or counts
# - Memorable & specific to {city} or {category}

# Return only the title:"""
#     result = create_fallback_title(city, category)
#     try:
#         from openai import OpenAI
#         client = OpenAI()
#         resp = client.chat.completions.create(
#             model=OPENAI_MODEL,
#             messages=[{"role": "user", "content": prompt}],
#             temperature=0.4,
#         )
#         t = (resp.choices[0].message.content or "").strip().strip('"\'')
#         if is_valid_title(t, city, category):
#             result = t
#     except Exception:
#         pass
#     if result == create_fallback_title(city, category):
#         try:
#             import openai as _openai
#             _openai.api_key = api_key
#             resp = _openai.ChatCompletion.create(
#                 model=OPENAI_MODEL,
#                 messages=[{"role": "user", "content": prompt}],
#                 temperature=0.4,
#             )
#             t = resp.choices[0].message["content"].strip().strip('"\'')
#             if is_valid_title(t, city, category):
#                 result = t
#         except Exception:
#             pass
#     cache_manager.llm_title_cache[cache_key] = result
#     return result

# # ---------- Merge MOBX sources ----------
# def merge_metadata_and_blocks(place_meta: List[Dict[str, Any]], blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
#     meta_map: Dict[str, Dict[str, Any]] = {}
#     for p in place_meta or []:
#         pid = str(p.get("placeId") or p.get("id") or "")
#         if not pid: pid = f"NAME::{(p.get('name') or '').strip()}"
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
#         if b.get("type") != "place": continue
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
#             "block_desc": (addendum.strip() or None),
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

# # ---------- List ID ----------
# def compute_list_id(city: str, url: str) -> str:
#     m = re.search(r"/(\d+)(?:$|[?#])", url or "")
#     geo_id = m.group(1) if m else "id"
#     return f"{slugify(city)}-{geo_id}"

# # ---------- Playlist builder ----------
# def build_playlist_doc(list_id: str, title: str, city: str, url: str, items: List[Dict[str, Any]], subtype: str = "destination") -> Dict[str, Any]:
#     playlist = {
#         "list_id": list_id,
#         "imageUrl": f"https://storage.googleapis.com/{os.getenv('GCS_BUCKET', 'mycasavsc.appspot.com')}/playlistsNew_images/{list_id}/1.jpg",
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

# # ---------- Google Places Photos ----------
# def get_place_photo_refs(cache: CacheManager, place_id: str, api_key: str) -> List[str]:
#     if not place_id: return []
#     cache_key = f"refs::{place_id}"
#     if cache_key in cache.google_places_cache:
#         cache.api_stats["cache_hits"] += 1
#         return cache.google_places_cache[cache_key] or []
#     cache.api_stats["cache_misses"] += 1
#     cache.api_stats["google_places_calls"] += 1
#     url = "https://maps.googleapis.com/maps/api/place/details/json"
#     params = {"place_id": place_id, "fields": "photo", "key": api_key}
#     try:
#         r = requests.get(url, params=params, timeout=12)
#         r.raise_for_status()
#         js = r.json()
#         photos = (js.get("result") or {}).get("photos") or []
#         refs = [p.get("photo_reference") for p in photos if p.get("photo_reference")]
#         refs = refs[:5]  # keep a few
#     except Exception:
#         refs = []
#     cache.google_places_cache[cache_key] = refs
#     return refs

# def fetch_photo_bytes(photo_ref: str, api_key: str, maxwidth: int = 1600) -> Optional[bytes]:
#     if not photo_ref: return None
#     url = "https://maps.googleapis.com/maps/api/place/photo"
#     params = {"maxwidth": str(maxwidth), "photo_reference": photo_ref, "key": api_key}
#     try:
#         r = requests.get(url, params=params, timeout=20, allow_redirects=True)
#         r.raise_for_status()
#         return r.content
#     except Exception:
#         return None

# def gcs_upload_bytes(storage_client, bucket_name: str, blob_path: str, data: bytes, content_type: str = "image/jpeg", make_public: bool = True) -> str:
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(blob_path)
#     blob.upload_from_string(data, content_type=content_type)
#     if make_public:
#         try:
#             blob.make_public()
#         except Exception:
#             pass
#     return blob.public_url or f"https://storage.googleapis.com/{bucket_name}/{blob_path}"

# def fill_place_images_and_cover(playlist: Dict[str, Any], bucket_name: str, max_photos: int = 3, skip_photos: bool = False, cache: Optional[CacheManager] = None) -> None:
#     """Populate g_image_urls for each place and upload a cover image for playlist."""
#     if skip_photos: return
#     api_key = os.getenv("GOOGLE_MAPS_API_KEY")
#     if not api_key: return  # silently skip if no key

#     global STORAGE
#     if STORAGE is None:
#         from google.cloud import storage as _storage
#         STORAGE = _storage.Client()

#     list_id = playlist["list_id"]
#     cover_uploaded = False
#     for place in playlist["subcollections"]["places"]:
#         pid = place.get("placeId")
#         if not pid:
#             place["g_image_urls"] = []
#             continue
#         refs = get_place_photo_refs(cache, pid, api_key) if cache else []
#         gcs_urls: List[str] = []
#         for idx, ref in enumerate(refs[:max_photos], start=1):
#             data = fetch_photo_bytes(ref, api_key)
#             if not data: continue
#             blob_path = f"playlistsPlaces/{list_id}/{pid}/{idx}.jpg"
#             url = gcs_upload_bytes(STORAGE, bucket_name, blob_path, data, "image/jpeg", make_public=True)
#             gcs_urls.append(url)
#             # set cover from the first successfully uploaded photo (once)
#             if not cover_uploaded and idx == 1:
#                 cover_blob = f"playlistsNew_images/{list_id}/1.jpg"
#                 gcs_upload_bytes(STORAGE, bucket_name, cover_blob, data, "image/jpeg", make_public=True)
#                 playlist["imageUrl"] = f"https://storage.googleapis.com/{bucket_name}/{cover_blob}"
#                 cover_uploaded = True
#         place["g_image_urls"] = gcs_urls

# # ---------- Firestore publish ----------
# def publish_playlist_to_firestore(playlist: Dict[str, Any], collection: str = "playlistsNew", project: Optional[str] = None) -> str:
#     global FIRESTORE
#     if FIRESTORE is None:
#         from google.cloud import firestore as _firestore
#         FIRESTORE = _firestore.Client(project=project) if project else _firestore.Client()
#     doc_id = playlist["list_id"]
#     col = FIRESTORE.collection(collection)
#     col.document(doc_id).set(playlist)
#     return doc_id

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

#     def __init__(self, jobs: List[Dict[str, Any]], out_dir: str, keep_ratio: float, use_llm: bool, use_selenium: bool,
#                  cache_dir: str = None, publish: bool = False, min_items: int = 7, gcs_bucket: str = None,
#                  collection: str = "playlistsNew", project: Optional[str] = None, max_photos: int = 3, skip_photos: bool = False, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.jobs = jobs
#         self.out_dir = Path(out_dir)
#         self.keep_ratio = float(keep_ratio)
#         self.use_llm = bool(use_llm and bool(os.getenv("OPENAI_API_KEY")))
#         self.use_selenium = bool(use_selenium and SeleniumRequest is not None)
#         self.publish = bool(publish)
#         self.min_items = int(min_items)
#         self.gcs_bucket = gcs_bucket or os.getenv("GCS_BUCKET", "mycasavsc.appspot.com")
#         self.collection = collection
#         self.project = project
#         self.max_photos = int(max_photos)
#         self.skip_photos = bool(skip_photos)

#         self.out_dir.mkdir(parents=True, exist_ok=True)
#         cache_dir_path = Path(cache_dir) if cache_dir else self.out_dir / "cache"
#         self.cache_manager = CacheManager(cache_dir_path)
#         self.aggregate_report: Dict[str, Any] = {
#             "script_version": SCRIPT_VERSION,
#             "start_ts": int(time.time()),
#             "llm_enabled_flag": bool(use_llm),
#             "llm_client_loaded": bool(os.getenv("OPENAI_API_KEY")),
#             "model": OPENAI_MODEL if self.use_llm else None,
#             "cache_directory": str(cache_dir_path),
#             "publish_enabled": self.publish,
#             "min_items": self.min_items,
#             "gcs_bucket": self.gcs_bucket,
#             "collection": self.collection,
#             "jobs": []
#         }

#     def start_requests(self):
#         for idx, job in enumerate(self.jobs):
#             url = job["url"]
#             url_cache_key = self.cache_manager.get_url_cache_key(url)
#             if url_cache_key in self.cache_manager.mobx_data_cache:
#                 self.logger.info(f"[{idx}] Using cached data for: {url}")
#                 self.cache_manager.api_stats["cache_hits"] += 1
#                 self.parse_cached_data(job, idx, url_cache_key)
#             else:
#                 self.cache_manager.api_stats["cache_misses"] += 1
#                 meta = {"job": job, "job_index": idx, "url_cache_key": url_cache_key}
#                 if self.use_selenium:
#                     yield SeleniumRequest(url=url, callback=self.parse_page, meta=meta, wait_time=2)
#                 else:
#                     yield scrapy.Request(url=url, callback=self.parse_page, meta=meta)

#     def parse_cached_data(self, job: Dict[str, Any], job_index: int, url_cache_key: str):
#         cached = self.cache_manager.mobx_data_cache[url_cache_key]
#         data = cached.get("data", {})
#         page_title = cached.get("page_title", job.get("title", "Untitled"))
#         url = job.get("url", "")
#         self.process_mobx_data(job, job_index, data, page_title, url, from_cache=True)

#     def parse_page(self, response):
#         job = response.meta["job"]
#         job_index = response.meta["job_index"]
#         url_cache_key = response.meta["url_cache_key"]
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
#         if not script_text:
#             self.logger.error(f"[{job_index}] MOBX not found: {job['url']}")
#             return
#         m = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text, re.DOTALL)
#         if not m:
#             self.logger.error(f"[{job_index}] MOBX parse failed: {job['url']}")
#             return
#         try:
#             mobx_json = json.loads(m.group(1))
#         except json.JSONDecodeError:
#             self.logger.error(f"[{job_index}] MOBX JSON decode failed: {job['url']}")
#             return
#         data = (mobx_json.get("placesListPage") or {}).get("data") or {}
#         page_title = data.get("title") or (job.get("title") or "Untitled")
#         self.cache_manager.mobx_data_cache[url_cache_key] = {"data": data, "page_title": page_title, "cached_ts": int(time.time())}
#         self.process_mobx_data(job, job_index, data, page_title, response.url, from_cache=False)

#     def process_mobx_data(self, job: Dict[str, Any], job_index: int, data: Dict[str, Any], page_title: str, url: str, from_cache: bool = False):
#         place_meta = data.get("placeMetadata") or []
#         blocks = []
#         for sec in (data.get("boardSections") or []):
#             for b in (sec.get("blocks") or []):
#                 if b.get("type") == "place":
#                     blocks.append(b)

#         category = (job.get("category_hint") or "").lower().strip() or detect_category_from_title(page_title)
#         merged = merge_metadata_and_blocks(place_meta, blocks)

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
#                 ans = llm_validate(cache_manager=self.cache_manager, category=category, name=name, context=ctx,
#                                    city=job.get("city") or "", page_title=page_title)
#                 if not from_cache:
#                     llm_calls += 1
#                 decided_by = "llm"
#                 if ans is True:
#                     verdict, reason = True, "llm_yes"
#                 elif ans is False:
#                     verdict, reason = False, "llm_no"
#                 else:
#                     verdict, reason = False, "llm_failed"

#             place_reports.append({
#                 "placeId": it.get("placeId"),
#                 "name": name,
#                 "accepted": bool(verdict),
#                 "decided_by": decided_by,
#                 "reason": reason,
#                 "pos_hits": hits.get("pos_hits", []),
#                 "neg_hits": hits.get("neg_hits", []),
#                 "context_used_excerpt": ctx[:220]
#             })
#             if verdict: accepted.append(it)
#             else: rejected.append(it)

#         curated = trim_and_light_shuffle(accepted, keep_ratio=self.keep_ratio)

#         # Generate title (no numbers)
#         final_title = page_title
#         title_calls = 0
#         if self.use_llm and curated:
#             sample_names = [p.get("name", "") for p in curated[:3]]
#             gen_title = generate_playlist_title(
#                 cache_manager=self.cache_manager,
#                 city=job.get("city") or "India",
#                 category=category,
#                 sample_names=sample_names,
#                 page_title=page_title
#             )
#             if gen_title and gen_title != create_fallback_title(job.get("city", "India"), category):
#                 final_title = gen_title
#             if not from_cache:
#                 title_calls += 1

#         # Build playlist JSON with stable id
#         list_id = compute_list_id(job.get("city") or "city", url)
#         playlist = build_playlist_doc(
#             list_id=list_id, title=final_title, city=job.get("city") or "India", url=url, items=curated
#         )

#         # Prepare output files (always write trial artifacts)
#         base = job_out_basename(job, page_title)
#         playlist_path = self.out_dir / f"{base}.json"
#         report_path = self.out_dir / f"{base}.report.json"
#         playlist_path.write_text(json.dumps(playlist, ensure_ascii=False, indent=2), encoding="utf-8")

#         published = False
#         firestore_id = None
#         cover_done = False

#         # PUBLISH only if we have >= min_items
#         if self.publish and len(playlist["subcollections"]["places"]) >= self.min_items:
#             # 1) Fill images (GCS)
#             try:
#                 fill_place_images_and_cover(
#                     playlist=playlist,
#                     bucket_name=self.gcs_bucket,
#                     max_photos=self.max_photos,
#                     skip_photos=self.skip_photos,
#                     cache=self.cache_manager
#                 )
#             except Exception as e:
#                 self.logger.warning(f"[{job_index}] image upload step failed: {e}")

#             # 2) Write to Firestore
#             try:
#                 firestore_id = publish_playlist_to_firestore(
#                     playlist=playlist,
#                     collection=self.collection,
#                     project=self.project
#                 )
#                 published = True
#             except Exception as e:
#                 self.logger.warning(f"[{job_index}] firestore publish failed: {e}")
#         else:
#             self.logger.info(f"[{job_index}] Not published (keep={len(playlist['subcollections']['places'])} < min={self.min_items}).")

#         # Write report
#         job_report = {
#             "job_index": job_index,
#             "city": job.get("city"),
#             "category_used": category,
#             "url": job.get("url"),
#             "title": page_title,
#             "final_title": final_title,
#             "from_cache": from_cache,
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
#                 "validation_calls": llm_calls,
#                 "title_calls": title_calls
#             },
#             "publish": {
#                 "enabled": self.publish,
#                 "min_items": self.min_items,
#                 "published": published,
#                 "firestore_doc_id": firestore_id,
#                 "bucket": self.gcs_bucket if published else None
#             },
#             "out_files": {
#                 "playlist_json": str(playlist_path),
#                 "report_json": str(report_path)
#             },
#             "created_ts": int(time.time())
#         }
#         report_path.write_text(json.dumps(job_report, ensure_ascii=False, indent=2), encoding="utf-8")
#         self.aggregate_report["jobs"].append(job_report)

#         pub_msg = "PUBLISHED" if published else "DRYRUN"
#         self.logger.info(f"[{job_index}] {final_title} → kept {len(curated)}/{len(merged)} | LLM calls={llm_calls + title_calls} [{pub_msg}]")
#         self.logger.info(f"  wrote: {playlist_path}")
#         self.logger.info(f"  wrote: {report_path}")

#     def closed(self, reason):
#         self.cache_manager.save_all_caches()
#         self.aggregate_report.update({
#             "end_ts": int(time.time()),
#             "finish_reason": reason,
#             "cache_stats": self.cache_manager.api_stats,
#             "cache_sizes": {
#                 "llm_validation_cache": len(self.cache_manager.llm_validation_cache),
#                 "llm_title_cache": len(self.cache_manager.llm_title_cache),
#                 "google_places_cache": len(self.cache_manager.google_places_cache),
#                 "mobx_data_cache": len(self.cache_manager.mobx_data_cache)
#             }
#         })
#         agg_path = self.out_dir / "_aggregate_report.json"
#         agg_path.write_text(json.dumps(self.aggregate_report, ensure_ascii=False, indent=2), encoding="utf-8")
#         cache_hit_rate = (self.cache_manager.api_stats["cache_hits"] /
#                           max(1, self.cache_manager.api_stats["cache_hits"] + self.cache_manager.api_stats["cache_misses"]) * 100)
#         self.logger.info(f"Cache hit rate: {cache_hit_rate:.1f}%")
#         self.logger.info(f"Aggregate report → {agg_path}")

# # ---------- Runner ----------
# def main():
#     parser = argparse.ArgumentParser(description="Publish curated playlists to Firestore + GCS (heuristic + LLM + caching).")
#     g = parser.add_mutually_exclusive_group(required=True)
#     g.add_argument("--url", help="Single Wanderlog list URL")
#     g.add_argument("--dataset-file", help="Path to JSON dataset (city -> category -> [{title,url}])")

#     parser.add_argument("--city", help="City name (required if using --url)")
#     parser.add_argument("--out-dir", default="trial_playlists", help="Output directory for JSONs and reports")
#     parser.add_argument("--cache-dir", help="Cache directory (defaults to out-dir/cache)")
#     parser.add_argument("--keep-ratio", type=float, default=0.7, help="Fraction to keep after trimming (0..1)")
#     parser.add_argument("--use-llm", action="store_true", help="Enable LLM for uncertain cases (requires OPENAI_API_KEY)")
#     parser.add_argument("--use-selenium", action="store_true", help="(Optional) Use selenium; usually not needed")
#     parser.add_argument("--clear-cache", action="store_true", help="Clear all caches before starting")

#     # Publish knobs
#     parser.add_argument("--publish", action="store_true", help="If set, publish to Firestore + GCS")
#     parser.add_argument("--min-items", type=int, default=7, help="Only publish if >= this many places")
#     parser.add_argument("--bucket", dest="gcs_bucket", default=os.getenv("GCS_BUCKET", "mycasavsc.appspot.com"), help="GCS bucket name")
#     parser.add_argument("--collection", default="playlistsNew", help="Firestore collection name")
#     parser.add_argument("--project", default=os.getenv("GOOGLE_CLOUD_PROJECT"), help="GCP project (optional)")
#     parser.add_argument("--max-photos", type=int, default=3, help="Max photos per place to upload")
#     parser.add_argument("--skip-photos", action="store_true", help="Skip photo fetching/upload (still publishes doc)")

#     args = parser.parse_args()

#     if args.url and not args.city:
#         parser.error("--city is required when using --url")

#     # Handle cache clearing
#     if args.clear_cache:
#         cache_dir_path = Path(args.cache_dir) if args.cache_dir else Path(args.out_dir) / "cache"
#         if cache_dir_path.exists():
#             import shutil
#             shutil.rmtree(cache_dir_path)
#             print(f"Cleared cache directory: {cache_dir_path}")

#     jobs = build_jobs_from_args(args.url, args.city, args.dataset_file)
#     if not jobs:
#         print("No jobs to process (check --url/--city or --dataset-file).")
#         return

#     # Selenium (optional)
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
#         use_selenium=args.use_selenium,
#         cache_dir=args.cache_dir,
#         publish=args.publish,
#         min_items=args.min_items,
#         gcs_bucket=args.gcs_bucket,
#         collection=args.collection,
#         project=args.project,
#         max_photos=args.max_photos,
#         skip_photos=args.skip_photos
#     )
#     process.start()

# if __name__ == "__main__":
#     main()
