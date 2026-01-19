"""
Step 2 — Extract playlist items from grouped article URLs (LLM-first + Category)

Input:   groups.json  (from Step 1)
Output:  playlist_items.json  (for Step 2.5)

Fixes in this version:
- Fetch REAL HTML via requests; keep Selenium fallback
- Force Selenium for holidify.com and traveltriangle.com (JS/anti-bot)
- Remove temperature=0 (some models only accept default=1)
- Harden load_page_html to detect non-HTML and fallback
- Add lightweight logging around LLM fallbacks
"""

import os, json, re, time, traceback, requests
from typing import List, Dict, Any, Optional
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
from tqdm import tqdm
from rapidfuzz import fuzz
from bs4 import BeautifulSoup

# LangChain
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate

# Selenium fallback
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

load_dotenv()

# ── CONFIG ─────────────────────────────────────────────────────────────────────
GROUPS_IN   = "groups.json"
OUT_PATH    = "playlist_items.json"

CACHE_DIR   = Path("cache")
HTML_CACHE  = CACHE_DIR / "html"
LLM_CACHE   = CACHE_DIR / "llm_category_cache.json"

MODEL       = os.getenv("LC_MODEL", "gpt-4.1-mini")  # used for LLM extraction & classification
# NOTE: do NOT pass temperature param; some models only allow default (=1)

# Trim sizes
SHORTLIST_SIZE = 10
FINAL_ITEMS    = 10

# Domains that often need JS rendering / anti-bot
FORCE_SELENIUM_DOMAINS = {"tripoto.com", "lbb.in", "holidify.com", "traveltriangle.com"}

# User-Agent
DEFAULT_UA = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)
os.environ["USER_AGENT"] = DEFAULT_UA

CACHE_DIR.mkdir(exist_ok=True, parents=True)
HTML_CACHE.mkdir(exist_ok=True, parents=True)
if not LLM_CACHE.exists():
    LLM_CACHE.write_text("{}", encoding="utf-8")

# ── Category taxonomy ──────────────────────────────────────────────────────────
NATURAL_CATS = {
    "waterfall","beach","island","lake","peak","mountain",
    "national_park","park","sanctuary","reserve","valley","cave","trek","trail"
}
DESTINATION_CATS = {"city","town","district","region","state","hill_station"}
POI_CATS = {"temple","fort","monument","museum","zoo","church","mosque","synagogue",
            "palace","viewpoint","dam","bridge","garden","market","street","neighborhood",
            "resort","hotel","camp","homestay","villa","lodge","hostel"}

CATEGORY_NORMALIZER = {
    "falls": "waterfall", "national park": "national_park", "hill station": "hill_station",
    "city/town": "town", "cities": "city", "towns": "town", "temples": "temple"
}
CATEGORY_ALLOWLIST = NATURAL_CATS | DESTINATION_CATS | POI_CATS
CATEGORY_TO_SCOPE = {**{c: "natural" for c in NATURAL_CATS},
                     **{c: "destination" for c in DESTINATION_CATS},
                     **{c: "poi" for c in POI_CATS}}

def normalize_category(v: Optional[str]) -> Optional[str]:
    if not v: return None
    v = CATEGORY_NORMALIZER.get(v.strip().lower(), v.strip().lower())
    return v if v in CATEGORY_ALLOWLIST else None

def scope_from_category(cat: Optional[str]) -> Optional[str]:
    return CATEGORY_TO_SCOPE.get(cat) if cat else None

# ── Prompts ────────────────────────────────────────────────────────────────────
EXTRACT_PROMPT = PromptTemplate.from_template("""
Extract recommended items from this travel collection page.

Return ONLY a JSON array. Each element:
{
  "name": "<place or trek name>",
  "description": "<1-3 concise sentences from page>",
  "travel_time": "<e.g., '2 hours 50 minutes' or ''>",
  "price": "<e.g., 'Starting at INR 5,300 per night' or ''>"
}

Guidelines:
- Prefer items that are actually listed as recommendations on this page.
- Strip HTML tags/entities; keep concise human text.
- If price or travel time appears near the item, include it; else "".
- If the page lists fewer than 15, return as many as it truly has.

HTML:
```html
{html}
```""")

NAMES_ONLY_PROMPT = PromptTemplate.from_template("""
Extract up to 25 recommended items from this page.
Return ONLY a JSON array of strings (names only, no objects, no markdown).

HTML:
```html
{html}
```""")

CLASSIFY_PROMPT = PromptTemplate.from_template("""
You classify a place into a compact taxonomy to help a maps resolver choose the right Google Place.

Allowed category_hint values (pick ONE that best fits):
- NATURAL: waterfall, beach, island, lake, peak, mountain, national_park, park, sanctuary, reserve, valley, cave, trek, trail
- DESTINATION: city, town, district, region, state, hill_station
- POI: temple, fort, monument, museum, zoo, church, mosque, synagogue, palace, viewpoint, dam, bridge, garden, market, street, neighborhood, resort, hotel, camp, homestay, villa, lodge, hostel

Output a single JSON object:
{
  "category_hint": "<one from the list above>",
  "scope": "<destination|poi|natural>"
}

Choose "destination" for cities/towns/regions, "natural" for nature features, "poi" for single attractions or lodging.

Name: {name}
Anchor city: {anchor_city}
Location hint: {location_hint}
Page title: {page_title}
Source URL: {url}

Short description (may be empty):
\"\"\"{desc}\"\"\"""")

# ── Helpers (text + hints) ─────────────────────────────────────────────────────
def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def strip_fences(text: str) -> str:
    m = re.search(r"```(?:json)?\s*(\[.*?\]|\{.*?\})\s*```", text, re.S)
    return m.group(1) if m else text.strip()

def sanitize_html(raw_html: str, max_chars: int = 120_000) -> str:
    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()
    for tag in soup.select("nav, footer, header, aside"):
        tag.decompose()
    return str(soup)[:max_chars]

def extract_page_title(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "html.parser")
    return clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")

NEAR_RE = re.compile(r"\bnear\s+([A-Za-z][A-Za-z\s&.-]{2,})\b", re.I)
IN_RE   = re.compile(r"\bin\s+([A-Za-z][A-Za-z\s&.-]{2,})\b", re.I)

def infer_location_hint_from_title(title: str, fallback_anchor: str) -> str:
    if not title: return fallback_anchor or "India"
    m = NEAR_RE.search(title)
    if m: return clean_text(m.group(1))
    m = IN_RE.search(title)
    if m: return clean_text(m.group(1))
    return fallback_anchor or "India"

# Extract durations/prices
TIME_PAT = re.compile(
    r"\b((?:\d{1,2}\s*(?:days?|d))|(?:\d{1,2}\s*(?:hours?|hrs?|h)(?:\s*\d{1,2}\s*(?:minutes?|mins?|m))?)|(?:\d{1,2}\s*-\s*\d{1,2}\s*(?:hours?|hrs?)))\b",
    re.I,
)
PRICE_PAT = re.compile(
    r"(?:₹|INR|Rs\.?)\s?[\d,]+(?:\s?(?:per\s?(?:night|day|person)|pp|/night|/day))?",
    re.I,
)

def parse_travel_time(text: str) -> str:
    m = TIME_PAT.search(text or "")
    return clean_text(m.group(0)) if m else ""

def parse_price(text: str) -> str:
    m = PRICE_PAT.search(text or "")
    return clean_text(m.group(0)) if m else ""

def section_excerpt(text: str, max_chars: int = 400) -> str:
    t = clean_text(text)
    return (t[:max_chars] + "…") if len(t) > max_chars else t

# ── Loaders ────────────────────────────────────────────────────────────────────
def fetch_http(url: str) -> str:
    headers = {
        "User-Agent": DEFAULT_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return r.text or ""

def fetch_selenium(url: str) -> str:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"--user-agent={DEFAULT_UA}")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(lambda d: d.execute_script("return document.readyState") == "complete")
        for _ in range(3):
            ActionChains(driver).scroll_by_amount(0, 1400).perform()
            time.sleep(0.6)
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article, .content, .post, .list"))
            )
        except Exception:
            pass
        return driver.page_source
    finally:
        driver.quit()

def cached_html_path(url: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
    return HTML_CACHE / f"{safe[:150]}.html"

def load_page_html(url: str) -> str:
    cache_file = cached_html_path(url)
    if cache_file.exists():
        return cache_file.read_text(encoding="utf-8")

    domain = urlparse(url).netloc.replace("www.", "").lower()
    html = ""
    try:
        if domain in FORCE_SELENIUM_DOMAINS:
            html = fetch_selenium(url)
        else:
            html = fetch_http(url)
            # If it looks like text-only or too short, fall back to Selenium
            blob = (html or "")[:2000].lower()
            if (len(html) < 2000) or ("<html" not in blob and "<body" not in blob):
                html = fetch_selenium(url)
    except Exception:
        html = fetch_selenium(url)

    cache_file.write_text(html or "", encoding="utf-8")
    return html

# ── LLM extraction ─────────────────────────────────────────────────────────────
def extract_items_llm(llm: ChatOpenAI, raw_html: str) -> List[Dict[str, Any]]:
    snippet = sanitize_html(raw_html)
    # 1) try full schema
    try:
        msg = llm.invoke(EXTRACT_PROMPT.format(html=snippet))
        text = strip_fences(getattr(msg, "content", str(msg)))
        arr = json.loads(text)
        out = []
        for it in arr:
            name = (it.get("name") or "").strip()
            if not name:
                continue
            out.append({
                "name": name,
                "description": clean_text(it.get("description") or ""),
                "travel_time": clean_text(it.get("travel_time") or ""),
                "price": clean_text(it.get("price") or ""),
            })
        if out:
            return out
    except Exception as e:
        print(f"[LLM extract] falling back → {e}")

    # 2) names-only fallback
    try:
        msg = llm.invoke(NAMES_ONLY_PROMPT.format(html=snippet))
        text = strip_fences(getattr(msg, "content", str(msg)))
        names = json.loads(text)
        out = []
        for n in names:
            if isinstance(n, str) and n.strip():
                out.append({
                    "name": n.strip(),
                    "description": "",
                    "travel_time": "",
                    "price": "",
                })
        if out:
            return out
    except Exception as e:
        print(f"[LLM names] falling back → {e}")

    # 3) heuristic scrape fallback on REAL HTML
    return heuristic_extract_items(raw_html)

def heuristic_extract_items(raw_html: str, limit: int = 25) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(raw_html, "html.parser")
    texts: List[str] = []
    selectors = [
        "article h2", "article h3", "section h2", "section h3",
        "ol li", "ul li", "h2", "h3", "h4"
    ]
    for sel in selectors:
        for el in soup.select(sel):
            t = el.get_text(" ", strip=True)
            if 2 <= len(t) <= 120:
                texts.append(t)
    seen = set()
    names = []
    for t in texts:
        k = re.sub(r"\s+", " ", t).strip().lower()
        if k not in seen:
            seen.add(k); names.append(t)
        if len(names) >= limit:
            break
    return [{"name": n, "description": "", "travel_time": "", "price": ""} for n in names]

# ── LLM category classification ────────────────────────────────────────────────
def _load_llm_cache():
    try: return json.loads(LLM_CACHE.read_text(encoding="utf-8"))
    except Exception: return {}

def _save_llm_cache(d):
    try: LLM_CACHE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception: pass

def classify_category_llm(llm, *, name: str, desc: str, page_title: str,
                          anchor_city: str, location_hint: str, url: str):
    cache = _load_llm_cache()
    key = json.dumps({"n": name, "t": page_title, "a": anchor_city, "l": location_hint, "u": url, "d": (desc or "")[:300]}, sort_keys=True)
    if key in cache:
        obj = cache[key]
        return normalize_category(obj.get("category_hint")), obj.get("scope")

    category_hint = scope = None
    try:
        msg  = llm.invoke(CLASSIFY_PROMPT.format(
            name=name, page_title=page_title, anchor_city=anchor_city or "",
            location_hint=location_hint or "", url=url, desc=desc or ""
        ))
        raw  = getattr(msg, "content", str(msg)).strip()
        raw  = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.S).strip()
        obj  = json.loads(raw)
        category_hint = normalize_category(obj.get("category_hint"))
        scope = obj.get("scope", "").strip().lower() if obj.get("scope") else None
        if scope not in {"destination","poi","natural"}: scope = None
    except Exception as e:
        print(f"[LLM classify] fallback → {e}")

    cache[key] = {"category_hint": category_hint, "scope": scope}
    _save_llm_cache(cache)
    return category_hint, scope

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

def infer_category_heuristic(name: str, description: str, page_title: str) -> Optional[str]:
    blob = " ".join([name or "", description or "", page_title or ""])
    for cat, rx in CATEGORY_HINTS_RX.items():
        if rx.search(blob):
            return normalize_category(cat)
    return None

# ── Merge / voting ────────────────────────────────────────────────────────────
def fuzzy_merge(items: List[Dict[str, Any]], threshold: int = 92) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for it in items:
        found = False
        for m in merged:
            if fuzz.token_set_ratio(it["name"], m["name"]) >= threshold:
                m["votes"] = m.get("votes", 1) + 1
                if len(it.get("description","")) > len(m.get("description","")):
                    m["description"] = it["description"]
                    m["section_excerpt"] = it.get("section_excerpt", m.get("section_excerpt"))
                for k in ("travel_time","price","location_hint","category_hint","scope"):
                    if not m.get(k) and it.get(k):
                        m[k] = it[k]
                m.setdefault("source_urls", set()).add(it["source_url"])
                m.setdefault("query_hints", set()).update(it.get("query_hints", []))
                found = True
                break
        if not found:
            it["votes"] = 1
            it["source_urls"] = {it["source_url"]}
            it["query_hints"] = set(it.get("query_hints", []))
            merged.append(it)
    for m in merged:
        m["source_urls"] = sorted(list(m["source_urls"]))
        m["query_hints"] = sorted(list(m["query_hints"]))
    return merged

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    playlists = json.load(open(GROUPS_IN, encoding="utf-8"))
    llm = ChatOpenAI(model=MODEL)  # no temperature arg

    output = []
    for plist in tqdm(playlists, desc="Playlists"):
        title   = plist["playlistTitle"]
        anchor  = plist.get("placeName", "")
        subtype = (plist.get("subtype") or "").lower().strip()
        urls    = [u["url"] for u in plist["ClubbedArticles"]]

        raw_items: List[Dict[str, Any]] = []

        for u in urls:
            html = load_page_html(u)
            print(f"[fetch] {u} → has_html_tag={('<html' in (html[:2000].lower()))} len={len(html)}")
            page_title = extract_page_title(html)
            loc_hint_from_title = infer_location_hint_from_title(page_title, anchor)

            # LLM-driven extraction
            items = []
            try:
                items = extract_items_llm(llm, html)
            except Exception:
                traceback.print_exc()

            # If LLM yielded nothing, try a tiny heuristic so we don’t return empty
            if not items:
                print(f"[heuristic] Falling back to headings for {u}")
                soup = BeautifulSoup(html, "html.parser")
                for el in soup.select("h2, h3, h4"):
                    n = clean_text(el.get_text(" ", strip=True))
                    if not n: continue
                    items.append({"name": n, "description": "", "travel_time": "", "price": ""})

            for it in items[:SHORTLIST_SIZE]:
                name = it["name"]
                desc = it.get("description","")
                tt = it.get("travel_time") or parse_travel_time(desc)
                pp = it.get("price") or parse_price(desc)

                qhints = {name}
                if loc_hint_from_title:
                    qhints.add(f"{name} {loc_hint_from_title}")
                if anchor and anchor.lower() not in name.lower():
                    qhints.add(f"{name} {anchor}")

                # LLM category + scope (uses description for context)
                cat_llm, scope_llm = classify_category_llm(
                    llm,
                    name=name,
                    desc=desc,
                    page_title=page_title,
                    anchor_city=anchor or "",
                    location_hint=loc_hint_from_title or "",
                    url=u
                )
                cat_hint = cat_llm or infer_category_heuristic(name, desc, page_title)
                scope_suggested = scope_llm or scope_from_category(cat_hint)

                raw_items.append({
                    "name": name,
                    "section_title": name,
                    "section_excerpt": section_excerpt(desc) if desc else "",
                    "description": desc,
                    "travel_time": tt or "",
                    "price": pp or "",
                    "location_hint": loc_hint_from_title,
                    "category_hint": cat_hint,
                    "scope": scope_suggested,
                    "query_hints": sorted(qhints),
                    "anchor_city": anchor,
                    "source_title": page_title,
                    "source_url": u,
                })

        if not raw_items:
            output.append({
                "playlistTitle": title,
                "placeName": anchor,
                "subtype": subtype,
                "source_urls": urls,
                "items": []
            })
            continue

        merged = fuzzy_merge(raw_items, threshold=92)

        merged.sort(
            key=lambda x: (
                -x.get("votes", 0),
                -(1 if x.get("description") else 0),
                -len((x.get("description") or "")),
                (x.get("name") or "").lower()
            )
        )
        final = merged[:FINAL_ITEMS]

        output.append({
            "playlistTitle": title,
            "placeName": anchor,
            "subtype": subtype,
            "source_urls": urls,
            "items": final
        })

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote {len(output)} playlists to {OUT_PATH}")

if __name__ == "__main__":
    main()


# """
# Step 2 — Extract playlist items from grouped article URLs (LLM-first + Category)

# Input:   groups.json  (from Step 1)
# Output:  playlist_items.json  (for Step 2.5)

# What’s new:
# - LLM classification per item → robust `category_hint` + suggested `scope`
# - Uses description + page title + anchor + location hint for better accuracy
# - Caches LLM classifications in cache/llm_category_cache.json
# - Keeps fuzzy-merge, Selenium fallback, and no Google Places here
# """

# import os, json, re, time, traceback
# from typing import List, Dict, Any, Optional, Tuple
# from pathlib import Path
# from urllib.parse import urlparse

# from dotenv import load_dotenv
# from tqdm import tqdm
# from rapidfuzz import fuzz
# from bs4 import BeautifulSoup

# # LangChain
# from langchain_openai import ChatOpenAI
# from langchain_core.prompts import PromptTemplate
# from langchain_community.document_loaders import WebBaseLoader

# # Selenium fallback
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.chrome.service import Service

# load_dotenv()

# # ── CONFIG ─────────────────────────────────────────────────────────────────────
# GROUPS_IN   = "groups.json"
# OUT_PATH    = "playlist_items.json"

# CACHE_DIR   = Path("cache")
# HTML_CACHE  = CACHE_DIR / "html"
# LLM_CACHE   = CACHE_DIR / "llm_category_cache.json"

# MODEL       = os.getenv("LC_MODEL", "gpt-4o-mini")  # used for LLM extraction & classification
# TEMPERATURE = 0.0

# # Trim sizes
# SHORTLIST_SIZE = 15
# FINAL_ITEMS    = 20

# # Domains that often need JS rendering
# FORCE_SELENIUM_DOMAINS = {"tripoto.com", "lbb.in"}

# # User-Agent
# DEFAULT_UA = os.getenv(
#     "USER_AGENT",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#     "AppleWebKit/537.36 (KHTML, like Gecko) "
#     "Chrome/125.0.0.0 Safari/537.36"
# )
# os.environ["USER_AGENT"] = DEFAULT_UA

# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# HTML_CACHE.mkdir(exist_ok=True, parents=True)
# if not LLM_CACHE.exists():
#     LLM_CACHE.write_text("{}", encoding="utf-8")

# # ── Category taxonomy ──────────────────────────────────────────────────────────
# NATURAL_CATS = {
#     "waterfall","beach","island","lake","peak","mountain",
#     "national_park","park","sanctuary","reserve","valley","cave","trek","trail"
# }
# DESTINATION_CATS = {"city","town","district","region","state","hill_station"}
# POI_CATS = {"temple","fort","monument","museum","zoo","church","mosque","synagogue",
#             "palace","viewpoint","dam","bridge","garden","market","street","neighborhood",
#             "resort","hotel","camp","homestay","villa","lodge","hostel"}

# CATEGORY_NORMALIZER = {
#     "falls": "waterfall", "national park": "national_park", "hill station": "hill_station",
#     "city/town": "town", "cities": "city", "towns": "town", "temples": "temple"
# }
# CATEGORY_ALLOWLIST = NATURAL_CATS | DESTINATION_CATS | POI_CATS
# CATEGORY_TO_SCOPE = {**{c: "natural" for c in NATURAL_CATS},
#                      **{c: "destination" for c in DESTINATION_CATS},
#                      **{c: "poi" for c in POI_CATS}}

# def normalize_category(v: Optional[str]) -> Optional[str]:
#     if not v: return None
#     v = CATEGORY_NORMALIZER.get(v.strip().lower(), v.strip().lower())
#     return v if v in CATEGORY_ALLOWLIST else None

# def scope_from_category(cat: Optional[str]) -> Optional[str]:
#     return CATEGORY_TO_SCOPE.get(cat) if cat else None

# # ── Prompts ────────────────────────────────────────────────────────────────────
# EXTRACT_PROMPT = PromptTemplate.from_template("""
# Extract recommended items from this travel collection page.

# Return ONLY a JSON array. Each element:
# {{
#   "name": "<place or trek name>",
#   "description": "<1-3 concise sentences from page>",
#   "travel_time": "<e.g., '2 hours 50 minutes' or ''>",
#   "price": "<e.g., 'Starting at INR 5,300 per night' or ''>"
# }}

# Guidelines:
# - Prefer items that are actually listed as recommendations on this page.
# - Strip HTML tags/entities; keep concise human text.
# - If price or travel time appears near the item, include it; else "".
# - If the page lists fewer than 15, return as many as it truly has.

# HTML:
# ```html
# {html}
# ```""")

# NAMES_ONLY_PROMPT = PromptTemplate.from_template("""
# Extract up to 25 recommended items from this page.
# Return ONLY a JSON array of strings (names only, no objects, no markdown).

# HTML:
# ```html
# {html}
# ```""")

# CLASSIFY_PROMPT = PromptTemplate.from_template("""
# You classify a place into a compact taxonomy to help a maps resolver choose the right Google Place.

# Allowed category_hint values (pick ONE that best fits):
# - NATURAL: waterfall, beach, island, lake, peak, mountain, national_park, park, sanctuary, reserve, valley, cave, trek, trail
# - DESTINATION: city, town, district, region, state, hill_station
# - POI: temple, fort, monument, museum, zoo, church, mosque, synagogue, palace, viewpoint, dam, bridge, garden, market, street, neighborhood, resort, hotel, camp, homestay, villa, lodge, hostel

# Output a single JSON object:
# {{
#   "category_hint": "<one from the list above>",
#   "scope": "<destination|poi|natural>"
# }}

# Choose "destination" for cities/towns/regions, "natural" for nature features, "poi" for single attractions or lodging.

# Name: {name}
# Anchor city: {anchor_city}
# Location hint: {location_hint}
# Page title: {page_title}
# Source URL: {url}

# Short description (may be empty):
# \"\"\"{desc}\"\"\"""")

# # ── Helpers (text + hints) ─────────────────────────────────────────────────────
# def clean_text(s: str) -> str:
#     return re.sub(r"\s+", " ", (s or "").strip())

# def strip_fences(text: str) -> str:
#     m = re.search(r"```(?:json)?\s*(\[.*?\]|\{.*?\})\s*```", text, re.S)
#     return m.group(1) if m else text.strip()

# def sanitize_html(raw_html: str, max_chars: int = 120_000) -> str:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
#         tag.decompose()
#     for tag in soup.select("nav, footer, header, aside"):
#         tag.decompose()
#     return str(soup)[:max_chars]

# def extract_page_title(raw_html: str) -> str:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     return clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")

# # Heuristics for “near <city>” or “in <state>”
# NEAR_RE = re.compile(r"\bnear\s+([A-Za-z][A-Za-z\s&.-]{2,})\b", re.I)
# IN_RE   = re.compile(r"\bin\s+([A-Za-z][A-Za-z\s&.-]{2,})\b", re.I)

# def infer_location_hint_from_title(title: str, fallback_anchor: str) -> str:
#     if not title: return fallback_anchor or "India"
#     m = NEAR_RE.search(title)
#     if m: return clean_text(m.group(1))
#     m = IN_RE.search(title)
#     if m: return clean_text(m.group(1))
#     return fallback_anchor or "India"

# # Extract durations/prices
# TIME_PAT = re.compile(
#     r"\b((?:\d{1,2}\s*(?:days?|d))|(?:\d{1,2}\s*(?:hours?|hrs?|h)(?:\s*\d{1,2}\s*(?:minutes?|mins?|m))?)|(?:\d{1,2}\s*-\s*\d{1,2}\s*(?:hours?|hrs?)))\b",
#     re.I,
# )
# PRICE_PAT = re.compile(
#     r"(?:₹|INR|Rs\.?)\s?[\d,]+(?:\s?(?:per\s?(?:night|day|person)|pp|/night|/day))?",
#     re.I,
# )

# def parse_travel_time(text: str) -> str:
#     m = TIME_PAT.search(text or "")
#     return clean_text(m.group(0)) if m else ""

# def parse_price(text: str) -> str:
#     m = PRICE_PAT.search(text or "")
#     return clean_text(m.group(0)) if m else ""

# def section_excerpt(text: str, max_chars: int = 400) -> str:
#     t = clean_text(text)
#     return (t[:max_chars] + "…") if len(t) > max_chars else t

# # ── Loaders ────────────────────────────────────────────────────────────────────
# def fetch_http(url: str) -> str:
#     docs = WebBaseLoader(url, header_template={"User-Agent": DEFAULT_UA}).load()
#     return docs[0].page_content if docs else ""

# # def fetch_selenium(url: str) -> str:
# #     opts = Options()
# #     opts.add_argument("--headless=new")
# #     opts.add_argument("--no-sandbox")
# #     opts.add_argument("--disable-dev-shm-usage")
# #     opts.add_argument(f"--user-agent={DEFAULT_UA}")
# #     driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
# #     try:
# #         driver.get(url)
# #         WebDriverWait(driver, 12).until(lambda d: d.execute_script("return document.readyState") == "complete")
# #         # gentle scroll for lazy content
# #         for _ in range(3):
# #             ActionChains(driver).scroll_by_amount(0, 1400).perform()
# #             time.sleep(0.6)
# #         try:
# #             WebDriverWait(driver, 5).until(
# #                 EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article, .content, .post, .list"))
# #             )
# #         except Exception:
# #             pass
# #         return driver.page_source
# #     finally:
# #         driver.quit()
# def fetch_selenium(url: str) -> str:
#     opts = Options()
#     opts.add_argument("--headless=new")
#     opts.add_argument("--no-sandbox")
#     opts.add_argument("--disable-dev-shm-usage")
#     opts.add_argument(f"--user-agent={DEFAULT_UA}")
    
#     # Fix: Use Service class for newer Selenium versions
#     from selenium.webdriver.chrome.service import Service
#     service = Service(ChromeDriverManager().install())
#     driver = webdriver.Chrome(service=service, options=opts)
    
#     try:
#         driver.get(url)
#         WebDriverWait(driver, 12).until(lambda d: d.execute_script("return document.readyState") == "complete")
#         # gentle scroll for lazy content
#         for _ in range(3):
#             ActionChains(driver).scroll_by_amount(0, 1400).perform()
#             time.sleep(0.6)
#         try:
#             WebDriverWait(driver, 5).until(
#                 EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article, .content, .post, .list"))
#             )
#         except Exception:
#             pass
#         return driver.page_source
#     finally:
#         driver.quit()

# def cached_html_path(url: str) -> Path:
#     safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
#     return HTML_CACHE / f"{safe[:150]}.html"

# def load_page_html(url: str) -> str:
#     cache_file = cached_html_path(url)
#     if cache_file.exists():
#         return cache_file.read_text(encoding="utf-8")

#     domain = urlparse(url).netloc.replace("www.", "").lower()
#     html = ""
#     try:
#         if domain in FORCE_SELENIUM_DOMAINS:
#             html = fetch_selenium(url)
#         else:
#             html = fetch_http(url)
#             if len(html) < 2000 or "enable javascript" in html.lower():
#                 html = fetch_selenium(url)
#     except Exception:
#         html = fetch_selenium(url)

#     cache_file.write_text(html or "", encoding="utf-8")
#     return html

# # ── LLM extraction ─────────────────────────────────────────────────────────────
# def extract_items_llm(llm: ChatOpenAI, raw_html: str) -> List[Dict[str, Any]]:
#     snippet = sanitize_html(raw_html)
#     # 1) try full schema
#     try:
#         msg = llm.invoke(EXTRACT_PROMPT.format(html=snippet))
#         text = strip_fences(getattr(msg, "content", str(msg)))
#         arr = json.loads(text)
#         out = []
#         for it in arr:
#             name = (it.get("name") or "").strip()
#             if not name:
#                 continue
#             out.append({
#                 "name": name,
#                 "description": clean_text(it.get("description") or ""),
#                 "travel_time": clean_text(it.get("travel_time") or ""),
#                 "price": clean_text(it.get("price") or ""),
#             })
#         if out:
#             return out
#     except Exception:
#         pass
#     # 2) names-only fallback
#     try:
#         msg = llm.invoke(NAMES_ONLY_PROMPT.format(html=snippet))
#         text = strip_fences(getattr(msg, "content", str(msg)))
#         names = json.loads(text)
#         out = []
#         for n in names:
#             if isinstance(n, str) and n.strip():
#                 out.append({
#                     "name": n.strip(),
#                     "description": "",
#                     "travel_time": "",
#                     "price": "",
#                 })
#         if out:
#             return out
#     except Exception:
#         pass
#     # 3) heuristic scrape fallback
#     return heuristic_extract_items(raw_html)

# def heuristic_extract_items(raw_html: str, limit: int = 25) -> List[Dict[str, Any]]:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     texts: List[str] = []
#     for sel in ["h2", "h3", "h4", "li strong", "li b"]:
#         for el in soup.select(sel):
#             t = el.get_text(" ", strip=True)
#             if 2 <= len(t) <= 100:
#                 texts.append(t)
#     seen = set()
#     names = []
#     for t in texts:
#         k = re.sub(r"\s+", " ", t).strip().lower()
#         if k not in seen:
#             seen.add(k); names.append(t)
#         if len(names) >= limit:
#             break
#     return [{"name": n, "description": "", "travel_time": "", "price": ""} for n in names]

# # ── LLM category classification ────────────────────────────────────────────────
# def _load_llm_cache():
#     try: return json.loads(LLM_CACHE.read_text(encoding="utf-8"))
#     except Exception: return {}

# def _save_llm_cache(d):
#     try: LLM_CACHE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
#     except Exception: pass

# def classify_category_llm(llm, *, name: str, desc: str, page_title: str,
#                           anchor_city: str, location_hint: str, url: str):
#     cache = _load_llm_cache()
#     key = json.dumps({"n": name, "t": page_title, "a": anchor_city, "l": location_hint, "u": url, "d": desc[:300]}, sort_keys=True)
#     if key in cache:
#         obj = cache[key]
#         return normalize_category(obj.get("category_hint")), obj.get("scope")

#     category_hint = scope = None
#     try:
#         msg  = llm.invoke(CLASSIFY_PROMPT.format(
#             name=name, page_title=page_title, anchor_city=anchor_city or "",
#             location_hint=location_hint or "", url=url, desc=desc or ""
#         ))
#         raw  = getattr(msg, "content", str(msg)).strip()
#         raw  = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.S).strip()
#         obj  = json.loads(raw)
#         category_hint = normalize_category(obj.get("category_hint"))
#         scope = obj.get("scope", "").strip().lower() if obj.get("scope") else None
#         if scope not in {"destination","poi","natural"}: scope = None
#     except Exception:
#         # swallow & fallback later
#         pass

#     cache[key] = {"category_hint": category_hint, "scope": scope}
#     _save_llm_cache(cache)
#     return category_hint, scope

# # conservative regex fallback if LLM unsure
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

# def infer_category_heuristic(name: str, description: str, page_title: str) -> Optional[str]:
#     blob = " ".join([name or "", description or "", page_title or ""])
#     for cat, rx in CATEGORY_HINTS_RX.items():
#         if rx.search(blob):
#             return normalize_category(cat)
#     return None

# # ── Merge / voting ────────────────────────────────────────────────────────────
# def fuzzy_merge(items: List[Dict[str, Any]], threshold: int = 92) -> List[Dict[str, Any]]:
#     merged: List[Dict[str, Any]] = []
#     for it in items:
#         found = False
#         for m in merged:
#             if fuzz.token_set_ratio(it["name"], m["name"]) >= threshold:
#                 m["votes"] = m.get("votes", 1) + 1
#                 # prefer longer/better description
#                 if len(it.get("description","")) > len(m.get("description","")):
#                     m["description"] = it["description"]
#                     m["section_excerpt"] = it.get("section_excerpt", m.get("section_excerpt"))
#                 # carry earliest non-empty fields
#                 for k in ("travel_time","price","location_hint","category_hint","scope"):
#                     if not m.get(k) and it.get(k):
#                         m[k] = it[k]
#                 m.setdefault("source_urls", set()).add(it["source_url"])
#                 m.setdefault("query_hints", set()).update(it.get("query_hints", []))
#                 found = True
#                 break
#         if not found:
#             it["votes"] = 1
#             it["source_urls"] = {it["source_url"]}
#             it["query_hints"] = set(it.get("query_hints", []))
#             merged.append(it)
#     for m in merged:
#         m["source_urls"] = sorted(list(m["source_urls"]))
#         m["query_hints"] = sorted(list(m["query_hints"]))
#     return merged

# # ── Main ───────────────────────────────────────────────────────────────────────
# def main():
#     playlists = json.load(open(GROUPS_IN, encoding="utf-8"))
#     llm = ChatOpenAI(model=MODEL, temperature=TEMPERATURE)

#     output = []
#     for plist in tqdm(playlists, desc="Playlists"):
#         title   = plist["playlistTitle"]
#         anchor  = plist.get("placeName", "")
#         subtype = (plist.get("subtype") or "").lower().strip()
#         urls    = [u["url"] for u in plist["ClubbedArticles"]]

#         raw_items: List[Dict[str, Any]] = []

#         for u in urls:
#             html = load_page_html(u)
#             page_title = extract_page_title(html)
#             loc_hint_from_title = infer_location_hint_from_title(page_title, anchor)

#             # LLM-driven extraction
#             items = []
#             try:
#                 items = extract_items_llm(llm, html)
#             except Exception:
#                 traceback.print_exc()

#             # If LLM yielded nothing, try a tiny heuristic so we don’t return empty
#             if not items:
#                 soup = BeautifulSoup(html, "html.parser")
#                 for el in soup.select("h2, h3, h4"):
#                     n = clean_text(el.get_text(" ", strip=True))
#                     if not n: continue
#                     items.append({"name": n, "description": "", "travel_time": "", "price": ""})

#             for it in items[:SHORTLIST_SIZE]:
#                 name = it["name"]
#                 desc = it.get("description","")
#                 # If LLM didn’t capture time/price, try to parse from the desc
#                 tt = it.get("travel_time") or parse_travel_time(desc)
#                 pp = it.get("price") or parse_price(desc)

#                 # query hints for Step 2.5
#                 qhints = {name}
#                 if loc_hint_from_title:
#                     qhints.add(f"{name} {loc_hint_from_title}")
#                 if anchor and anchor.lower() not in name.lower():
#                     qhints.add(f"{name} {anchor}")

#                 # LLM category + scope (uses description for context)
#                 cat_llm, scope_llm = classify_category_llm(
#                     llm,
#                     name=name,
#                     desc=desc,
#                     page_title=page_title,
#                     anchor_city=anchor or "",
#                     location_hint=loc_hint_from_title or "",
#                     url=u
#                 )
#                 # fallback if unsure
#                 cat_hint = cat_llm or infer_category_heuristic(name, desc, page_title)
#                 scope_suggested = scope_llm or scope_from_category(cat_hint)

#                 raw_items.append({
#                     "name": name,
#                     "section_title": name,
#                     "section_excerpt": section_excerpt(desc) if desc else "",
#                     "description": desc,
#                     "travel_time": tt or "",
#                     "price": pp or "",
#                     "location_hint": loc_hint_from_title,  # e.g., "Bangalore"/"Karnataka"
#                     "category_hint": cat_hint,             # e.g., "waterfall","beach","town"
#                     "scope": scope_suggested,              # e.g., "natural","destination","poi"
#                     "query_hints": sorted(qhints),
#                     "anchor_city": anchor,
#                     "source_title": page_title,
#                     "source_url": u,
#                 })

#         # Nothing at all? keep the shape
#         if not raw_items:
#             output.append({
#                 "playlistTitle": title,
#                 "placeName": anchor,
#                 "subtype": subtype,
#                 "source_urls": urls,
#                 "items": []
#             })
#             continue

#         # Dedupe / vote
#         merged = fuzzy_merge(raw_items, threshold=92)

#         # Rank & trim — vote count, then has description, then name alpha
#         merged.sort(
#             key=lambda x: (
#                 -x.get("votes", 0),
#                 -(1 if x.get("description") else 0),
#                 -len((x.get("description") or "")),
#                 (x.get("name") or "").lower()
#             )
#         )
#         final = merged[:FINAL_ITEMS]

#         output.append({
#             "playlistTitle": title,
#             "placeName": anchor,
#             "subtype": subtype,
#             "source_urls": urls,
#             "items": final
#         })

#     with open(OUT_PATH, "w", encoding="utf-8") as f:
#         json.dump(output, f, ensure_ascii=False, indent=2)

#     print(f"✅ Wrote {len(output)} playlists to {OUT_PATH}")

# if __name__ == "__main__":
#     main()


# """
# Step 2 (Tripoto+LLM) — Extract playlist items from grouped article URLs
# Input:  groups.json   (from Step 1)
# Output: playlist_items.json (for Step 2.5)

# What you get:
# - Aggressive Selenium loader (great for Tripoto/JS-heavy pages)
# - Robust text extraction (numbered sections, distance blocks, keyword patterns)
# - LLM classification per item → `category_hint` + suggested `scope`
# - Title-based location hint, time/price parsing, query_hints
# - Caches HTML and LLM classifications (cache/html, cache/llm_category_cache.json)
# - Output shape matches your latest Step 2 (ready for Step 2.5)
# """

# from __future__ import annotations
# import os, json, re, time, traceback
# from typing import List, Dict, Any, Optional, Tuple
# from pathlib import Path

# from dotenv import load_dotenv
# from tqdm import tqdm
# from rapidfuzz import fuzz
# from bs4 import BeautifulSoup

# # LangChain (LLM)
# from langchain_openai import ChatOpenAI
# from langchain_core.prompts import PromptTemplate

# # Selenium (primary loader)
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.chrome import ChromeDriverManager

# load_dotenv()

# # ───────────────────────── CONFIG ─────────────────────────
# GROUPS_IN   = "groups.json"
# OUT_PATH    = "playlist_items.json"

# CACHE_DIR   = Path("cache")
# HTML_CACHE  = CACHE_DIR / "html"
# LLM_CACHE   = CACHE_DIR / "llm_category_cache.json"

# MODEL       = os.getenv("LC_MODEL", "gpt-4o-mini")
# TEMPERATURE = 0.0

# FINAL_ITEMS = 10

# DEFAULT_UA = os.getenv(
#     "USER_AGENT",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
#     "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
# )
# os.environ["USER_AGENT"] = DEFAULT_UA

# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# HTML_CACHE.mkdir(exist_ok=True, parents=True)
# if not LLM_CACHE.exists():
#     LLM_CACHE.write_text("{}", encoding="utf-8")

# # ────────────── Category taxonomy + helpers ───────────────
# NATURAL_CATS = {
#     "waterfall","beach","island","lake","peak","mountain",
#     "national_park","park","sanctuary","reserve","valley","cave","trek","trail"
# }
# DESTINATION_CATS = {"city","town","district","region","state","hill_station"}
# POI_CATS = {"temple","fort","monument","museum","zoo","church","mosque","synagogue",
#             "palace","viewpoint","dam","bridge","garden","market","street","neighborhood",
#             "resort","hotel","camp","homestay","villa","lodge","hostel"}

# CATEGORY_NORMALIZER = {
#     "falls": "waterfall", "national park": "national_park", "hill station": "hill_station",
#     "city/town": "town", "cities": "city", "towns": "town", "temples": "temple"
# }
# CATEGORY_ALLOWLIST = NATURAL_CATS | DESTINATION_CATS | POI_CATS
# CATEGORY_TO_SCOPE = {**{c: "natural" for c in NATURAL_CATS},
#                      **{c: "destination" for c in DESTINATION_CATS},
#                      **{c: "poi" for c in POI_CATS}}

# def normalize_category(v: Optional[str]) -> Optional[str]:
#     if not v: return None
#     v = CATEGORY_NORMALIZER.get(v.strip().lower(), v.strip().lower())
#     return v if v in CATEGORY_ALLOWLIST else None

# def scope_from_category(cat: Optional[str]) -> Optional[str]:
#     return CATEGORY_TO_SCOPE.get(cat) if cat else None

# # ───────────────────────── Prompts ────────────────────────
# CLASSIFY_PROMPT = PromptTemplate.from_template("""
# You classify a place into a compact taxonomy to help a maps resolver choose the right Google Place.

# Allowed category_hint values (pick ONE that best fits):
# - NATURAL: waterfall, beach, island, lake, peak, mountain, national_park, park, sanctuary, reserve, valley, cave, trek, trail
# - DESTINATION: city, town, district, region, state, hill_station
# - POI: temple, fort, monument, museum, zoo, church, mosque, synagogue, palace, viewpoint, dam, bridge, garden, market, street, neighborhood, resort, hotel, camp, homestay, villa, lodge, hostel

# Output a single JSON object:
# {{
#   "category_hint": "<one from the list above>",
#   "scope": "<destination|poi|natural>"
# }}

# Choose "destination" for cities/towns/regions, "natural" for nature features, "poi" for single attractions or lodging.

# Name: {name}
# Anchor city: {anchor_city}
# Location hint: {location_hint}
# Page title: {page_title}
# Source URL: {url}

# Short description (may be empty):
# \"\"\"{desc}\"\"\"""")

# SUMMARIZE_PROMPT = PromptTemplate.from_template("""
# Create a concise travel description for this place. Focus on what makes it special and why someone would visit.
# Keep it factual and 1–2 sentences.

# Place: {name}
# Content: {content}

# Description:""")

# # ────────────── Title/location + parse helpers ────────────
# NEAR_RE = re.compile(r"\bnear\s+([A-Za-z][A-Za-z\s&\.-]{2,})\b", re.I)
# IN_RE   = re.compile(r"\bin\s+([A-Za-z][A-Za-z\s&\.-]{2,})\b", re.I)

# TIME_PAT = re.compile(
#     r"\b((?:\d{1,2}\s*(?:days?|d))|(?:\d{1,2}\s*(?:hours?|hrs?|h)(?:\s*\d{1,2}\s*(?:minutes?|mins?|m))?)|(?:\d{1,2}\s*-\s*\d{1,2}\s*(?:hours?|hrs?)))\b",
#     re.I,
# )
# PRICE_PAT = re.compile(
#     r"(?:₹|INR|Rs\.?)\s?[\d,]+(?:\s?(?:per\s?(?:night|day|person)|pp|/night|/day))?",
#     re.I,
# )

# def clean_text(s: str) -> str:
#     return re.sub(r"\s+", " ", (s or "").strip())

# def extract_page_title(html: str) -> str:
#     soup = BeautifulSoup(html, "html.parser")
#     return clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")

# def infer_location_hint_from_title(title: str, fallback: str) -> str:
#     if not title: return fallback or "India"
#     m = NEAR_RE.search(title);  m2 = IN_RE.search(title)
#     if m:  return clean_text(m.group(1))
#     if m2: return clean_text(m2.group(1))
#     return fallback or "India"

# def parse_travel_time(text: str) -> str:
#     m = TIME_PAT.search(text or "");  return clean_text(m.group(0)) if m else ""

# def parse_price(text: str) -> str:
#     m = PRICE_PAT.search(text or ""); return clean_text(m.group(0)) if m else ""

# # ────────────── Aggressive Selenium loader ────────────────
# def create_driver() -> webdriver.Chrome:
#     opts = Options()
#     opts.add_argument("--headless=new")
#     opts.add_argument("--no-sandbox")
#     opts.add_argument("--disable-dev-shm-usage")
#     opts.add_argument("--disable-blink-features=AutomationControlled")
#     opts.add_experimental_option("excludeSwitches", ["enable-automation"])
#     opts.add_experimental_option("useAutomationExtension", False)
#     opts.add_argument(f"--user-agent={DEFAULT_UA}")
#     service = Service(ChromeDriverManager().install())
#     driver = webdriver.Chrome(service=service, options=opts)
#     driver.set_page_load_timeout(40)
#     try:
#         driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
#     except Exception:
#         pass
#     return driver

# def aggressive_content_load(url: str) -> str:
#     driver = None
#     try:
#         driver = create_driver()
#         driver.get(url)
#         WebDriverWait(driver, 20).until(lambda d: d.execute_script("return document.readyState") == "complete")

#         # quick lazy-load passes
#         for _ in range(6):
#             ActionChains(driver).scroll_by_amount(0, 1600).perform()
#             time.sleep(0.8)

#         # wait for meaningful content
#         try:
#             WebDriverWait(driver, 8).until(
#                 EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article, .content, .post, .list, h2, h3"))
#             )
#         except Exception:
#             pass

#         # If still thin, click "Load/More" like controls a bit
#         for btn in driver.find_elements(By.XPATH, "//*[contains(text(),'Load') or contains(text(),'More') or contains(text(),'Show')]")[:3]:
#             try:
#                 driver.execute_script("arguments[0].click();", btn)
#                 time.sleep(2.0)
#             except Exception:
#                 continue

#         return driver.page_source
#     except Exception as e:
#         print(f"    ❌ Selenium error: {e}")
#         return ""
#     finally:
#         if driver:
#             driver.quit()

# # ──────────────── HTML cache ──────────────────────────────
# def cached_html_path(url: str) -> Path:
#     safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
#     return HTML_CACHE / f"{safe[:150]}.html"

# def load_page_html(url: str, force_reload: bool = False) -> str:
#     cache_file = cached_html_path(url)
#     if not force_reload and cache_file.exists():
#         txt = cache_file.read_text(encoding="utf-8", errors="ignore")
#         if len(BeautifulSoup(txt, "html.parser").get_text()) > 1000:
#             return txt
#     html = aggressive_content_load(url)
#     if html:
#         cache_file.write_text(html, encoding="utf-8")
#     return html

# # ─────────────── Extraction (Tripoto-friendly) ────────────
# def extract_from_continuous_text(html: str) -> List[Tuple[str, str]]:
#     if not html or len(html) < 1000: return []
#     soup = BeautifulSoup(html, "html.parser")
#     all_text = soup.get_text(" ", strip=True)
#     if len(all_text) < 500: return []

#     places: List[Tuple[str,str]] = []

#     # Pattern 1: numbered sections
#     rx1 = re.compile(r'(\d+)\.\s*([A-Z][A-Za-z\s&,-]{3,50}?)(?:SPONSORED)?\s*(.{40,600}?)(?=\d+\.\s*[A-Z]|$)', re.S)
#     for _, name, desc in rx1.findall(all_text):
#         name = clean_text(name); desc = clean_text(desc[:500])
#         if 3 < len(name) < 60: places.append((name, desc))

#     # Pattern 2: "Distance:" blocks
#     rx2 = re.compile(r'([A-Z][A-Za-z\s&,-]{3,50}?)\s*(?:Distance:?|Distance from [A-Za-z ]+:)\s*(.{5,40})(.{40,400}?)(?=[A-Z][A-Za-z\s&,-]{3,50}? Distance:|$)', re.I|re.S)
#     for name, dist, desc in rx2.findall(all_text):
#         name = clean_text(name); dist = clean_text(dist); desc = clean_text(desc)
#         if 3 < len(name) < 60 and not any(fuzz.ratio(name.lower(), n.lower())>87 for n,_ in places):
#             places.append((name, f"{dist}. {desc}"))

#     # Pattern 3: keyword places
#     kw = r'(Falls?|Hills?|Fort|Temple|Beach|Island|Dam|Park|Sanctuary|Palace|Resort|Village|Valley|Cave|Lake|Garden|Museum|Monument|Viewpoint)'
#     rx3 = re.compile(rf'([A-Z][A-Za-z\s&,-]{{3,50}}?\s+{kw})', re.I)
#     for full,_ in rx3.findall(all_text):
#         name = clean_text(full)
#         if 5 < len(name) < 60 and not re.search(r'(advertisement|subscribe|follow)', name, re.I):
#             pos = all_text.find(name)
#             desc = clean_text(all_text[pos+len(name):pos+len(name)+280])
#             if not any(fuzz.ratio(name.lower(), n.lower())>85 for n,_ in places):
#                 places.append((name, desc))

#     # Dedup
#     out, seen = [], set()
#     for n, d in places:
#         k = re.sub(r"\s+", "", n.lower())
#         if k not in seen:
#             seen.add(k); out.append((n, d))
#     return out

# def extract_from_text_blocks(html: str) -> List[Tuple[str,str]]:
#     if not html or len(html) < 1000: return []
#     soup = BeautifulSoup(html, "html.parser")
#     lines = [clean_text(x) for x in soup.get_text("\n").split("\n") if clean_text(x)]
#     places: List[Tuple[str,str]] = []
#     current = None; buf: List[str] = []

#     patterns = [
#         re.compile(r'^\d+\.?\s*([A-Z][a-zA-Z\s&,-]{3,50})$'),
#         re.compile(r'^([A-Z][a-zA-Z\s&,-]{3,50})\s*[-–—:]'),
#         re.compile(r'^##?\s*([A-Z][a-zA-Z\s&,-]{3,50})$'),
#     ]

#     for line in lines:
#         hit = None
#         for rx in patterns:
#             m = rx.match(line)
#             if m: hit = m.group(1); break
#         if not hit and re.match(r'^[A-Z][A-Za-z\s&,-]{3,40}$', line):
#             words = line.split()
#             if len(words) <= 4 and not any(w.lower() in {'the','and','or','with','from','about','more','this','that'} for w in words):
#                 hit = line

#         if hit:
#             if current:
#                 places.append((current, clean_text(" ".join(buf))))
#             current = hit; buf = []
#         elif current and 20 < len(line) < 400 and not re.search(r'(advertisement|cookie|privacy|subscribe|share|contact)', line, re.I):
#             buf.append(line)
#         if len(buf) > 3: buf = buf[:3]

#     if current:
#         places.append((current, clean_text(" ".join(buf))))
#     return places

# # ───────────── LLM helpers (classify + summarize) ─────────
# def _load_llm_cache():
#     try: return json.loads(LLM_CACHE.read_text(encoding="utf-8"))
#     except Exception: return {}

# def _save_llm_cache(d):
#     try: LLM_CACHE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
#     except Exception: pass

# def classify_category_llm(llm: ChatOpenAI, *, name: str, desc: str, page_title: str,
#                           anchor_city: str, location_hint: str, url: str) -> Tuple[Optional[str], Optional[str]]:
#     cache = _load_llm_cache()
#     key = json.dumps({"n":name, "t":page_title, "a":anchor_city, "l":location_hint, "u":url, "d":(desc or "")[:300]}, sort_keys=True)
#     if key in cache:
#         obj = cache[key]
#         return normalize_category(obj.get("category_hint")), obj.get("scope")

#     cat_hint = scope = None
#     try:
#         msg = llm.invoke(CLASSIFY_PROMPT.format(
#             name=name, page_title=page_title, anchor_city=anchor_city or "",
#             location_hint=location_hint or "", url=url, desc=desc or ""
#         ))
#         raw = (getattr(msg, "content", str(msg)) or "").strip()
#         raw = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.S).strip()
#         obj = json.loads(raw)
#         cat_hint = normalize_category(obj.get("category_hint"))
#         scope = (obj.get("scope") or "").strip().lower()
#         if scope not in {"destination","poi","natural"}: scope = None
#     except Exception:
#         pass

#     cache[key] = {"category_hint": cat_hint, "scope": scope}
#     _save_llm_cache(cache)
#     return cat_hint, scope

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
#     "museum": re.compile(r"\bmuseum\b", re.I),
#     "viewpoint": re.compile(r"\bview\s?point\b", re.I),
# }

# def infer_category_heuristic(name: str, desc: str, title: str) -> Optional[str]:
#     blob = " ".join([name or "", desc or "", title or ""])
#     for cat, rx in CATEGORY_HINTS_RX.items():
#         if rx.search(blob):
#             return normalize_category(cat)
#     return None

# def enhance_description(llm: ChatOpenAI, name: str, content: str) -> str:
#     if not content or len(content) < 16: return ""
#     try:
#         msg = llm.invoke(SUMMARIZE_PROMPT.format(name=name, content=content[:1200]))
#         txt = (getattr(msg, "content", "") or "").strip()
#         return txt if len(txt) > 10 else content
#     except Exception:
#         return content

# # ───────────── merge, hints, sectioning ─────────────
# def build_query_hints(name: str, context: str) -> List[str]:
#     out = [name]
#     if context and context.lower() not in name.lower():
#         out.append(f"{name} {context}")
#     out.append(f"{name} India")
#     return out

# def fuzzy_merge(items: List[Dict[str, Any]], threshold: int = 90) -> List[Dict[str, Any]]:
#     merged: List[Dict[str, Any]] = []
#     for it in items:
#         found = False
#         for m in merged:
#             if fuzz.token_set_ratio(it["name"], m["name"]) >= threshold:
#                 m["votes"] = m.get("votes", 1) + 1
#                 if len(it.get("description","")) > len(m.get("description","")):
#                     m["description"] = it["description"]
#                     m["section_excerpt"] = it.get("section_excerpt", m.get("section_excerpt"))
#                 for k in ("travel_time","price","location_hint","category_hint","scope"):
#                     if not m.get(k) and it.get(k):
#                         m[k] = it[k]
#                 m.setdefault("source_urls", set()).add(it["source_url"])
#                 m.setdefault("query_hints", set()).update(it.get("query_hints", []))
#                 found = True
#                 break
#         if not found:
#             it["votes"] = 1
#             it["source_urls"] = {it["source_url"]}
#             it["query_hints"] = set(it.get("query_hints", []))
#             merged.append(it)
#     for m in merged:
#         m["source_urls"] = sorted(list(m["source_urls"]))
#         m["query_hints"] = sorted(list(m["query_hints"]))
#     return merged

# # ─────────────────────────── Main ─────────────────────────
# def main():
#     if not Path(GROUPS_IN).exists():
#         print(f"❌ Input not found: {GROUPS_IN}")
#         return

#     playlists = json.loads(Path(GROUPS_IN).read_text(encoding="utf-8"))
#     llm = ChatOpenAI(model=MODEL, temperature=TEMPERATURE)

#     out_playlists: List[Dict[str, Any]] = []

#     for plist in tqdm(playlists, desc="Playlists"):
#         title   = plist.get("playlistTitle") or "Untitled"
#         anchor  = plist.get("placeName") or "India"
#         subtype = (plist.get("subtype") or "").lower().strip()
#         urls    = [u["url"] for u in plist.get("ClubbedArticles", [])]

#         raw_items: List[Dict[str, Any]] = []

#         for url in urls:
#             print(f"\n🌐 {url}")
#             html = load_page_html(url, force_reload=False)
#             if len(html) < 800:
#                 print("   ⚠️ Thin HTML, refetching aggressively…")
#                 html = load_page_html(url, force_reload=True)

#             if not html:
#                 print("   ❌ No HTML")
#                 continue

#             page_title = extract_page_title(html)
#             loc_hint   = infer_location_hint_from_title(page_title, anchor)

#             # Prefer continuous text extractor, then block extractor
#             sections = extract_from_continuous_text(html)
#             if not sections:
#                 sections = extract_from_text_blocks(html)
#             print(f"   📌 extracted {len(sections)} candidates")

#             for name, content in sections:
#                 # enhance desc
#                 desc = enhance_description(llm, name, content)
#                 # parse quick extras
#                 tt = parse_travel_time(desc) or parse_travel_time(content)
#                 pp = parse_price(desc) or parse_price(content)
#                 # LLM classify → category_hint/scope
#                 cat_llm, scope_llm = classify_category_llm(
#                     llm,
#                     name=name, desc=desc, page_title=page_title,
#                     anchor_city=anchor, location_hint=loc_hint, url=url
#                 )
#                 cat_hint = cat_llm or infer_category_heuristic(name, desc, page_title)
#                 scope    = scope_llm or scope_from_category(cat_hint)

#                 raw_items.append({
#                     "name": name,
#                     "section_title": name,
#                     "section_excerpt": clean_text(content)[:400] if content else "",
#                     "description": desc,
#                     "travel_time": tt or "",
#                     "price": pp or "",
#                     "location_hint": loc_hint,
#                     "category_hint": cat_hint,     # ← NEW
#                     "scope": scope,                 # ← NEW
#                     "query_hints": build_query_hints(name, loc_hint or anchor),
#                     "anchor_city": anchor,
#                     "source_title": page_title,
#                     "source_url": url,
#                 })

#         if not raw_items:
#             out_playlists.append({
#                 "playlistTitle": title,
#                 "placeName": anchor,
#                 "subtype": subtype,
#                 "source_urls": urls,
#                 "items": []
#             })
#             continue

#         merged = fuzzy_merge(raw_items, threshold=90)
#         merged.sort(key=lambda x: (-x.get("votes",0), -(1 if x.get("description") else 0),
#                                    -len(x.get("description","")), x.get("name","").lower()))
#         final = merged[:FINAL_ITEMS]

#         out_playlists.append({
#             "playlistTitle": title,
#             "placeName": anchor,
#             "subtype": subtype,
#             "source_urls": urls,
#             "items": final
#         })

#     Path(OUT_PATH).write_text(json.dumps(out_playlists, ensure_ascii=False, indent=2), encoding="utf-8")
#     total_items = sum(len(p["items"]) for p in out_playlists)
#     print(f"\n✅ Wrote {len(out_playlists)} playlists / {total_items} items → {OUT_PATH}")

# if __name__ == "__main__":
#     main()



















# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# Step 2 — Extract playlist items from grouped article URLs (LLM-first).

# Input:  groups.json  (from Step 1)
# Output: playlist_items.json  (for Step 2.5)

# What’s new vs your “previous version”:
# - Adds fields to help Step 2.5 resolve the right Google Place:
#   location_hint, category_hint, query_hints, anchor_city, source_title
# - Adds section_title + section_excerpt (useful for UI or later QA)
# - Keeps your LLM extraction (full schema → names-only → heuristic)
# - Selenium fallback kept; no Google Places here.
# """

# import os, json, re, time
# from typing import List, Dict, Any, Optional, Tuple
# from pathlib import Path
# from urllib.parse import urlparse

# from dotenv import load_dotenv
# from tqdm import tqdm
# from rapidfuzz import fuzz
# from bs4 import BeautifulSoup

# # LangChain
# from langchain_openai import ChatOpenAI
# from langchain_core.prompts import PromptTemplate
# from langchain_community.document_loaders import WebBaseLoader

# # Selenium fallback
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.chrome import ChromeDriverManager

# load_dotenv()

# # ── CONFIG ─────────────────────────────────────────────────────────────────────
# GROUPS_IN   = "groups.json"
# OUT_PATH    = "playlist_items.json"
# CACHE_DIR   = Path("cache")
# HTML_CACHE  = CACHE_DIR / "html"

# MODEL       = os.getenv("LC_MODEL", "gpt-4o-mini")  # used for LLM extraction

# # Trim sizes
# SHORTLIST_SIZE = 20
# FINAL_ITEMS    = 15

# # Domains that often need JS rendering
# FORCE_SELENIUM_DOMAINS = {"tripoto.com", "lbb.in"}

# # User-Agent
# DEFAULT_UA = os.getenv(
#     "USER_AGENT",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#     "AppleWebKit/537.36 (KHTML, like Gecko) "
#     "Chrome/125.0.0.0 Safari/537.36"
# )
# os.environ["USER_AGENT"] = DEFAULT_UA

# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# HTML_CACHE.mkdir(exist_ok=True, parents=True)

# # ── Prompts ────────────────────────────────────────────────────────────────────
# EXTRACT_PROMPT = PromptTemplate.from_template("""
# Extract recommended items from this travel collection page.

# Return ONLY a JSON array. Each element:
# {{
#   "name": "<place or trek name>",
#   "description": "<1-3 concise sentences from page>",
#   "travel_time": "<e.g., '2 hours 50 minutes' or ''>",
#   "price": "<e.g., 'Starting at INR 5,300 per night' or ''>"
# }}

# Guidelines:
# - Prefer items that are actually listed as recommendations on this page.
# - Strip HTML tags/entities; keep concise human text.
# - If price or travel time appears near the item, include it; else "".
# - If the page lists fewer than 15, return as many as it truly has.

# HTML:
# ```html
# {html}
# ```""")

# NAMES_ONLY_PROMPT = PromptTemplate.from_template("""
# Extract up to 25 recommended items from this page.
# Return ONLY a JSON array of strings (names only, no objects, no markdown).

# HTML:
# ```html
# {html}
# ```""")

# # ── Helpers (text + hints) ─────────────────────────────────────────────────────
# def clean_text(s: str) -> str:
#     return re.sub(r"\s+", " ", (s or "").strip())

# def strip_fences(text: str) -> str:
#     m = re.search(r"```(?:json)?\s*(\[.*?\]|\{.*?\})\s*```", text, re.S)
#     return m.group(1) if m else text.strip()

# def sanitize_html(raw_html: str, max_chars: int = 120_000) -> str:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
#         tag.decompose()
#     for tag in soup.select("nav, footer, header, aside"):
#         tag.decompose()
#     return str(soup)[:max_chars]

# def extract_page_title(raw_html: str) -> str:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     return clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")

# # Heuristics for “near <city>” or “in <state>”
# NEAR_RE = re.compile(r"\bnear\s+([A-Za-z][A-Za-z\s&.-]{2,})\b", re.I)
# IN_RE   = re.compile(r"\bin\s+([A-Za-z][A-Za-z\s&.-]{2,})\b", re.I)

# def infer_location_hint_from_title(title: str, fallback_anchor: str) -> str:
#     if not title: return fallback_anchor or "India"
#     m = NEAR_RE.search(title)
#     if m: return clean_text(m.group(1))
#     m = IN_RE.search(title)
#     if m: return clean_text(m.group(1))
#     return fallback_anchor or "India"

# CATEGORY_HINTS = {
#     "beach":         re.compile(r"\bbeach(es)?\b", re.I),
#     "island":        re.compile(r"\bisland(s)?\b", re.I),
#     "waterfall":     re.compile(r"\bwaterfall(s)?|falls\b", re.I),
#     "national_park": re.compile(r"\bnational\s+park|tiger\s+reserve|sanctuary\b", re.I),
#     "trek":          re.compile(r"\btrek(s)?|trail(s)?|pass\b", re.I),
#     "lake":          re.compile(r"\blake\b", re.I),
#     "fort":          re.compile(r"\bfort\b", re.I),
#     "temple":        re.compile(r"\btemple\b", re.I),
#     "wildlife":      re.compile(r"\bwildlife|safari\b", re.I),
# }

# def infer_category_hint(name: str, description: str, page_title: str) -> Optional[str]:
#     blob = " ".join([name or "", description or "", page_title or ""])
#     for cat, rx in CATEGORY_HINTS.items():
#         if rx.search(blob):
#             return cat
#     return None

# # Extract “2 days”, “4–6 hours”, “3h 30m”, etc.
# TIME_PAT = re.compile(
#     r"\b((?:\d{1,2}\s*(?:days?|d))|(?:\d{1,2}\s*(?:hours?|hrs?|h)(?:\s*\d{1,2}\s*(?:minutes?|mins?|m))?)|(?:\d{1,2}\s*-\s*\d{1,2}\s*(?:hours?|hrs?)))\b",
#     re.I,
# )
# # Extract ₹/INR price-like snippets
# PRICE_PAT = re.compile(
#     r"(?:₹|INR|Rs\.?)\s?[\d,]+(?:\s?(?:per\s?(?:night|day|person)|pp|/night|/day))?",
#     re.I,
# )

# def parse_travel_time(text: str) -> str:
#     m = TIME_PAT.search(text or "")
#     return clean_text(m.group(0)) if m else ""

# def parse_price(text: str) -> str:
#     m = PRICE_PAT.search(text or "")
#     return clean_text(m.group(0)) if m else ""

# def section_excerpt(text: str, max_chars: int = 400) -> str:
#     t = clean_text(text)
#     return (t[:max_chars] + "…") if len(t) > max_chars else t

# # ── Loaders ────────────────────────────────────────────────────────────────────
# def fetch_http(url: str) -> str:
#     docs = WebBaseLoader(url, header_template={"User-Agent": DEFAULT_UA}).load()
#     return docs[0].page_content if docs else ""

# def fetch_selenium(url: str) -> str:
#     opts = Options()
#     opts.add_argument("--headless=new")
#     opts.add_argument("--no-sandbox")
#     opts.add_argument("--disable-dev-shm-usage")
#     opts.add_argument(f"--user-agent={DEFAULT_UA}")
#     driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
#     try:
#         driver.get(url)
#         WebDriverWait(driver, 10).until(lambda d: d.execute_script("return document.readyState") == "complete")
#         # light scroll helps Tripoto/LBB
#         for _ in range(3):
#             ActionChains(driver).scroll_by_amount(0, 1400).perform()
#             time.sleep(0.6)
#         try:
#             WebDriverWait(driver, 5).until(
#                 EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article, .content, .post, .list"))
#             )
#         except Exception:
#             pass
#         return driver.page_source
#     finally:
#         driver.quit()

# def cached_html_path(url: str) -> Path:
#     safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
#     return HTML_CACHE / f"{safe[:150]}.html"

# def load_page_html(url: str) -> str:
#     cache_file = cached_html_path(url)
#     if cache_file.exists():
#         return cache_file.read_text(encoding="utf-8")

#     domain = urlparse(url).netloc.replace("www.", "").lower()
#     html = ""
#     try:
#         if domain in FORCE_SELENIUM_DOMAINS:
#             html = fetch_selenium(url)
#         else:
#             html = fetch_http(url)
#             if len(html) < 2000 or "enable javascript" in html.lower():
#                 html = fetch_selenium(url)
#     except Exception:
#         html = fetch_selenium(url)

#     cache_file.write_text(html or "", encoding="utf-8")
#     return html

# # ── LLM extraction ─────────────────────────────────────────────────────────────
# def extract_items_llm(llm: ChatOpenAI, raw_html: str) -> List[Dict[str, Any]]:
#     snippet = sanitize_html(raw_html)
#     # 1) try full schema
#     try:
#         msg = llm.invoke(EXTRACT_PROMPT.format(html=snippet))
#         text = strip_fences(getattr(msg, "content", str(msg)))
#         arr = json.loads(text)
#         out = []
#         for it in arr:
#             name = (it.get("name") or "").strip()
#             if not name:
#                 continue
#             out.append({
#                 "name": name,
#                 "description": clean_text(it.get("description") or ""),
#                 "travel_time": clean_text(it.get("travel_time") or ""),
#                 "price": clean_text(it.get("price") or ""),
#             })
#         if out:
#             return out
#     except Exception:
#         pass
#     # 2) fallback → names-only prompt
#     try:
#         msg = llm.invoke(NAMES_ONLY_PROMPT.format(html=snippet))
#         text = strip_fences(getattr(msg, "content", str(msg)))
#         names = json.loads(text)
#         out = []
#         for n in names:
#             if isinstance(n, str) and n.strip():
#                 out.append({
#                     "name": n.strip(),
#                     "description": "",
#                     "travel_time": "",
#                     "price": "",
#                 })
#         if out:
#             return out
#     except Exception:
#         pass
#     # 3) last resort → heuristic scrape of headings/lists
#     return heuristic_extract_items(raw_html)

# def heuristic_extract_items(raw_html: str, limit: int = 25) -> List[Dict[str, Any]]:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     texts: List[str] = []
#     for sel in ["h2", "h3", "h4", "li strong", "li b"]:
#         for el in soup.select(sel):
#             t = el.get_text(" ", strip=True)
#             if 2 <= len(t) <= 100:
#                 texts.append(t)
#     seen = set()
#     names = []
#     for t in texts:
#         k = re.sub(r"\s+", " ", t).strip().lower()
#         if k not in seen:
#             seen.add(k); names.append(t)
#         if len(names) >= limit:
#             break
#     return [{"name": n, "description": "", "travel_time": "", "price": ""} for n in names]

# # ── Merge / voting ────────────────────────────────────────────────────────────
# def fuzzy_merge(items: List[Dict[str, Any]], threshold: int = 92) -> List[Dict[str, Any]]:
#     merged: List[Dict[str, Any]] = []
#     for it in items:
#         found = False
#         for m in merged:
#             if fuzz.token_set_ratio(it["name"], m["name"]) >= threshold:
#                 m["votes"] = m.get("votes", 1) + 1
#                 # prefer longer/better description
#                 if len(it.get("description","")) > len(m.get("description","")):
#                     m["description"] = it["description"]
#                     m["section_excerpt"] = it.get("section_excerpt", m.get("section_excerpt"))
#                 # carry earliest non-empty fields
#                 for k in ("travel_time","price","location_hint","category_hint"):
#                     if not m.get(k) and it.get(k):
#                         m[k] = it[k]
#                 m.setdefault("source_urls", set()).add(it["source_url"])
#                 m.setdefault("query_hints", set()).update(it.get("query_hints", []))
#                 found = True
#                 break
#         if not found:
#             it["votes"] = 1
#             it["source_urls"] = {it["source_url"]}
#             it["query_hints"] = set(it.get("query_hints", []))
#             merged.append(it)
#     for m in merged:
#         m["source_urls"] = sorted(list(m["source_urls"]))
#         m["query_hints"] = sorted(list(m["query_hints"]))
#     return merged

# # ── Main ───────────────────────────────────────────────────────────────────────
# def main():
#     playlists = json.load(open(GROUPS_IN, encoding="utf-8"))
#     llm = ChatOpenAI(model=MODEL, temperature=0.0)

#     output = []
#     for plist in tqdm(playlists, desc="Playlists"):
#         title   = plist["playlistTitle"]
#         anchor  = plist.get("placeName", "")
#         subtype = plist.get("subtype", "").lower().strip()
#         urls    = [u["url"] for u in plist["ClubbedArticles"]]

#         raw_items: List[Dict[str, Any]] = []

#         for u in urls:
#             html = load_page_html(u)
#             page_title = extract_page_title(html)
#             loc_hint_from_title = infer_location_hint_from_title(page_title, anchor)

#             # LLM-driven extraction
#             items = extract_items_llm(llm, html)
#             for it in items:
#                 name = it["name"]
#                 desc = it.get("description","")
#                 # If LLM didn’t capture time/price, try to parse from the desc
#                 tt = it.get("travel_time") or parse_travel_time(desc)
#                 pp = it.get("price") or parse_price(desc)
#                 cat_hint = infer_category_hint(name, desc, page_title)

#                 # Places query helpers for Step 2.5
#                 qhints = {name}
#                 if loc_hint_from_title:
#                     qhints.add(f"{name} {loc_hint_from_title}")
#                 if cat_hint and cat_hint not in name.lower():
#                     qhints.add(f"{name} {cat_hint.replace('_',' ')}")
#                 if anchor and anchor.lower() not in name.lower():
#                     qhints.add(f"{name} {anchor}")

#                 raw_items.append({
#                     "name": name,
#                     "section_title": name,
#                     "section_excerpt": section_excerpt(desc) if desc else "",
#                     "description": desc,
#                     "travel_time": tt or "",
#                     "price": pp or "",
#                     "location_hint": loc_hint_from_title,  # e.g., "Bangalore"/"Karnataka"
#                     "category_hint": cat_hint,             # e.g., "waterfall","beach"
#                     "query_hints": sorted(qhints),
#                     "anchor_city": anchor,
#                     "source_title": page_title,
#                     "source_url": u,
#                 })

#             # If LLM yielded nothing, try a tiny heuristic pass so we don’t return empty
#             if not items:
#                 soup = BeautifulSoup(html, "html.parser")
#                 for el in soup.select("h2, h3, h4"):
#                     n = clean_text(el.get_text(" ", strip=True))
#                     if not n: continue
#                     qh = [n]
#                     if loc_hint_from_title: qh.append(f"{n} {loc_hint_from_title}")
#                     if anchor: qh.append(f"{n} {anchor}")
#                     raw_items.append({
#                         "name": n,
#                         "section_title": n,
#                         "section_excerpt": "",
#                         "description": "",
#                         "travel_time": "",
#                         "price": "",
#                         "location_hint": loc_hint_from_title,
#                         "category_hint": infer_category_hint(n, "", page_title),
#                         "query_hints": qh,
#                         "anchor_city": anchor,
#                         "source_title": page_title,
#                         "source_url": u
#                     })

#         # Nothing at all? keep the shape
#         if not raw_items:
#             output.append({
#                 "playlistTitle": title,
#                 "placeName": anchor,
#                 "subtype": subtype,
#                 "source_urls": urls,
#                 "items": []
#             })
#             continue

#         # Dedupe / vote
#         merged = fuzzy_merge(raw_items, threshold=92)

#         # Rank & trim — vote count, then has description, then name alpha
#         merged.sort(
#             key=lambda x: (
#                 -x.get("votes", 0),
#                 -(1 if x.get("description") else 0),
#                 -len((x.get("description") or "")),
#                 (x.get("name") or "").lower()
#             )
#         )
#         final = merged[:FINAL_ITEMS]

#         output.append({
#             "playlistTitle": title,
#             "placeName": anchor,
#             "subtype": subtype,
#             "source_urls": urls,
#             "items": final
#         })

#     with open(OUT_PATH, "w", encoding="utf-8") as f:
#         json.dump(output, f, ensure_ascii=False, indent=2)

#     print(f"✅ Wrote {len(output)} playlists to {OUT_PATH}")

# if __name__ == "__main__":
#     main()




# #Tripoto working,LLB, working but not perfect
# import os, json, re, time
# from typing import List, Dict, Any, Optional, Tuple
# from pathlib import Path
# from dotenv import load_dotenv
# from tqdm import tqdm
# from rapidfuzz import fuzz
# from bs4 import BeautifulSoup

# # LangChain for LLM only
# from langchain_openai import ChatOpenAI
# from langchain_core.prompts import PromptTemplate

# # Selenium - primary content loader
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.keys import Keys
# from webdriver_manager.chrome import ChromeDriverManager

# load_dotenv()

# # ── CONFIG ─────────────────────────────────────────────────────────────────────
# GROUPS_IN   = "groups.json"
# OUT_PATH    = "playlist_items.json"
# CACHE_DIR   = Path("cache")
# HTML_CACHE  = CACHE_DIR / "html"

# MODEL = os.getenv("LC_MODEL", "gpt-5-nano")
# FINAL_ITEMS = 15

# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# HTML_CACHE.mkdir(exist_ok=True, parents=True)

# # ── Aggressive Selenium Content Loading ──────────────────────────────────────
# def create_aggressive_selenium_driver() -> webdriver.Chrome:
#     """Create Selenium driver optimized for JS-heavy sites like Tripoto"""
#     opts = Options()
    
#     # Keep JavaScript enabled for content loading
#     opts.add_argument("--headless=new")
#     opts.add_argument("--no-sandbox")
#     opts.add_argument("--disable-dev-shm-usage")
#     opts.add_argument("--disable-blink-features=AutomationControlled")
#     opts.add_experimental_option("excludeSwitches", ["enable-automation"])
#     opts.add_experimental_option('useAutomationExtension', False)
    
#     # Increase timeouts
#     opts.add_argument("--page-load-strategy=normal")
    
#     # Fixed initialization - use Service instead of direct path
#     from selenium.webdriver.chrome.service import Service
#     service = Service(ChromeDriverManager().install())
#     driver = webdriver.Chrome(service=service, options=opts)
#     driver.set_page_load_timeout(30)
    
#     # Anti-detection
#     driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
#     return driver

# def aggressive_content_load(url: str) -> str:
#     """Aggressively load content from JS-heavy pages"""
#     driver = None
#     try:
#         print(f"    🚀 Aggressive loading: {url}")
#         driver = create_aggressive_selenium_driver()
        
#         # Load page
#         driver.get(url)
        
#         # Wait for basic page load
#         WebDriverWait(driver, 15).until(
#             lambda d: d.execute_script("return document.readyState") == "complete"
#         )
        
#         print(f"    ⏳ Page loaded, waiting for content...")
        
#         # Wait for content to appear - try multiple strategies
#         content_loaded = False
        
#         # Strategy 1: Wait for text content to appear
#         try:
#             WebDriverWait(driver, 10).until(
#                 lambda d: len(d.find_element(By.TAG_NAME, "body").text) > 1000
#             )
#             content_loaded = True
#             print(f"    ✅ Content detected via body text")
#         except:
#             pass
        
#         # Strategy 2: Wait for specific article/content elements
#         if not content_loaded:
#             content_selectors = [
#                 "h1, h2, h3",  # Any headings
#                 "[class*='content']",
#                 "[class*='article']",
#                 "[class*='story']",
#                 "[class*='trip']",
#                 "p",  # Just paragraphs
#                 ".rich-text",  # Common rich text class
#                 "[data-*]"  # Elements with data attributes
#             ]
            
#             for selector in content_selectors:
#                 try:
#                     WebDriverWait(driver, 5).until(
#                         EC.presence_of_element_located((By.CSS_SELECTOR, selector))
#                     )
#                     elements = driver.find_elements(By.CSS_SELECTOR, selector)
#                     total_text = sum(len(el.text) for el in elements[:10])
#                     if total_text > 500:
#                         content_loaded = True
#                         print(f"    ✅ Content detected via selector: {selector} ({total_text} chars)")
#                         break
#                 except:
#                     continue
        
#         # Strategy 3: Aggressive scrolling and waiting
#         if not content_loaded:
#             print(f"    📜 No content detected, trying aggressive scrolling...")
            
#             # Scroll multiple times with longer waits
#             for i in range(8):
#                 # Scroll down
#                 driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {(i+1)/8});")
#                 time.sleep(2)  # Longer wait
                
#                 # Check if content appeared
#                 body_text_length = len(driver.find_element(By.TAG_NAME, "body").text)
#                 if body_text_length > 2000:  # Good amount of text
#                     content_loaded = True
#                     print(f"    ✅ Content appeared after scroll {i+1} ({body_text_length} chars)")
#                     break
                
#                 print(f"    📜 Scroll {i+1}: {body_text_length} chars")
        
#         # Strategy 4: Try interacting with page elements
#         if not content_loaded:
#             print(f"    🖱️ Trying page interactions...")
#             try:
#                 # Try clicking "Load more" or similar buttons
#                 load_buttons = driver.find_elements(By.XPATH, "//*[contains(text(), 'Load') or contains(text(), 'Show') or contains(text(), 'More')]")
#                 for button in load_buttons[:3]:
#                     try:
#                         driver.execute_script("arguments[0].click();", button)
#                         time.sleep(3)
#                         body_text_length = len(driver.find_element(By.TAG_NAME, "body").text)
#                         if body_text_length > 2000:
#                             content_loaded = True
#                             print(f"    ✅ Content loaded after button click ({body_text_length} chars)")
#                             break
#                     except:
#                         continue
#             except:
#                 pass
        
#         # Strategy 5: Wait for any dynamic content loading
#         if not content_loaded:
#             print(f"    ⏰ Final wait for dynamic content...")
#             time.sleep(5)  # Final wait
        
#         # Get final page source
#         html = driver.page_source
#         content_length = len(html)
#         text_length = len(driver.find_element(By.TAG_NAME, "body").text)
        
#         print(f"    📄 Final content: {content_length} HTML chars, {text_length} text chars")
        
#         if text_length < 500:
#             print(f"    ⚠️ Warning: Very little text content found!")
#             # Try one more time with a different approach
#             driver.refresh()
#             time.sleep(10)
#             html = driver.page_source
#             text_length = len(driver.find_element(By.TAG_NAME, "body").text)
#             print(f"    🔄 After refresh: {len(html)} HTML chars, {text_length} text chars")
        
#         return html
        
#     except Exception as e:
#         print(f"    ❌ Selenium error: {e}")
#         return ""
#     finally:
#         if driver:
#             driver.quit()

# # ── Continuous Text Extraction (for single-line content) ──────────────────────
# def extract_from_continuous_text(html: str) -> List[Tuple[str, str]]:
#     """Extract places from continuous text (single line) using regex patterns"""
#     if not html or len(html) < 1000:
#         return []
    
#     soup = BeautifulSoup(html, 'html.parser')
#     all_text = soup.get_text()
    
#     if len(all_text) < 500:
#         print("    ⚠️ Very little text content found")
#         return []
    
#     print(f"    📝 Analyzing {len(all_text)} characters of continuous text")
    
#     places = []
    
#     # Pattern 1: Numbered places with descriptions
#     # "1. Chunchi FallsSPONSOREDChunchi FallsAround 89km from Bangalore..."
#     numbered_pattern = r'(\d+)\.\s*([A-Z][A-Za-z\s&,-]{3,40}?)(?:SPONSORED)?(?:[A-Za-z\s&,-]{0,20}?)(.*?)(?=\d+\.\s*[A-Z]|$)'
    
#     matches = re.findall(numbered_pattern, all_text)
#     for num, name, description in matches:
#         name = clean_text(name)
#         desc = clean_text(description[:500])  # First 500 chars as description
        
#         if (len(name) > 3 and len(name) < 50 and 
#             not re.search(r'(SPONSORED|advertisement|subscribe|follow)', name, re.I)):
#             places.append((name, desc))
    
#     # Pattern 2: Distance patterns - "Place Name Distance: Around X km from Bangalore"
#     distance_pattern = r'([A-Z][A-Za-z\s&,-]{3,40}?)(?:Distance:|Distance from Bangalore:)\s*(?:Around\s*)?(\d+\s*k?m?\s*from\s*Bangalore)(.*?)(?=[A-Z][A-Za-z\s&,-]{3,40}?Distance:|$)'
    
#     matches = re.findall(distance_pattern, all_text, re.IGNORECASE)
#     for name, distance, description in matches:
#         name = clean_text(name)
#         desc = clean_text(description[:400])
        
#         if (len(name) > 3 and len(name) < 50 and
#             not re.search(r'(SPONSORED|advertisement|subscribe|follow)', name, re.I)):
#             # Check if not already added
#             if not any(fuzz.ratio(name.lower(), existing[0].lower()) > 85 for existing in places):
#                 places.append((name, f"{distance}. {desc}"))
    
#     # Pattern 3: Place names followed by descriptions in specific Tripoto format
#     # "PlaceNameSPONSOREDPlaceNameDescription..."
#     tripoto_pattern = r'([A-Z][A-Za-z\s&,-]{3,40}?)SPONSORED\1([^A-Z]{50,400}?)(?=[A-Z][A-Za-z\s&,-]{3,40}?SPONSORED|$)'
    
#     matches = re.findall(tripoto_pattern, all_text)
#     for name, description in matches:
#         name = clean_text(name)
#         desc = clean_text(description)
        
#         if (len(name) > 3 and len(name) < 50):
#             # Check if not already added
#             if not any(fuzz.ratio(name.lower(), existing[0].lower()) > 85 for existing in places):
#                 places.append((name, desc))
    
#     # Pattern 4: Look for common travel place patterns in text
#     # "How to reach PlaceName from Bangalore"
#     reach_pattern = r'How to reach ([A-Z][A-Za-z\s&,-]{3,40}?) from Bangalore[?:]?\s*([^.]{50,300})'
    
#     matches = re.findall(reach_pattern, all_text)
#     for name, description in matches:
#         name = clean_text(name)
#         desc = clean_text(description)
        
#         if len(name) > 3 and len(name) < 50:
#             if not any(fuzz.ratio(name.lower(), existing[0].lower()) > 85 for existing in places):
#                 places.append((name, desc))
    
#     # Pattern 5: Direct place extraction from typical travel content
#     # Look for place names that appear with travel-related keywords
#     place_keywords = r'\b(Falls?|Hills?|Fort|Temple|Beach|Island|Dam|Park|Sanctuary|Palace|Resort|Village|Valley|Gorge|Cave|Lake|Garden)\b'
    
#     # Find potential place names with these keywords
#     place_pattern = rf'([A-Z][A-Za-z\s&,-]{{3,40}}?\s+{place_keywords})'
    
#     potential_places = re.findall(place_pattern, all_text)
#     for match in potential_places:
#         place_name = clean_text(match[0])  # match is a tuple from group capture
        
#         if (len(place_name) > 5 and len(place_name) < 50 and
#             not re.search(r'(SPONSORED|advertisement|subscribe|follow|Get App|Search for)', place_name, re.I)):
            
#             # Try to find description near this place name
#             place_index = all_text.find(place_name)
#             if place_index != -1:
#                 # Get text after the place name
#                 after_text = all_text[place_index + len(place_name):place_index + len(place_name) + 300]
#                 description = clean_text(after_text)
                
#                 # Check if not already added
#                 if not any(fuzz.ratio(place_name.lower(), existing[0].lower()) > 80 for existing in places):
#                     places.append((place_name, description))
    
#     # Clean up and deduplicate results
#     final_places = []
#     seen_names = set()
    
#     for name, desc in places:
#         name_key = name.lower().replace(' ', '')
#         if name_key not in seen_names:
#             seen_names.add(name_key)
#             final_places.append((name, desc))
    
#     print(f"    🏷️ Extracted {len(final_places)} potential places from continuous text")
    
#     # Show first few for debugging
#     for i, (name, desc) in enumerate(final_places[:5]):
#         print(f"      {i+1}. {name}: {desc[:60]}...")
    
#     return final_places

# # ── Fallback Text-Based Extraction ──────────────────────────────────────────
# def extract_from_text_content(html: str) -> List[Tuple[str, str]]:
#     """Extract places from any text content found, even without proper HTML structure"""
#     if not html or len(html) < 1000:
#         return []
    
#     soup = BeautifulSoup(html, 'html.parser')
    
#     # Get all text content
#     all_text = soup.get_text()
    
#     if len(all_text) < 500:
#         print("    ⚠️ Very little text content found")
#         return []
    
#     print(f"    📝 Analyzing {len(all_text)} characters of text content")
    
#     # Split text into lines and look for place-like patterns
#     lines = [line.strip() for line in all_text.split('\n') if line.strip()]
    
#     places = []
#     current_place = None
#     current_description = []
    
#     # Patterns that might indicate place names
#     place_patterns = [
#         r'^\d+\.?\s*([A-Z][a-zA-Z\s&,-]{3,50})$',  # "1. Place Name"
#         r'^([A-Z][a-zA-Z\s&,-]{3,50})\s*[-–—]',    # "Place Name - description"  
#         r'^([A-Z][a-zA-Z\s&,-]{3,50})\s*:',        # "Place Name: description"
#         r'^##?\s*([A-Z][a-zA-Z\s&,-]{3,50})$',     # "## Place Name"
#     ]
    
#     # Look for numbered lists or clear place indicators
#     for line in lines:
#         line = line.strip()
#         if not line or len(line) < 4:
#             continue
        
#         is_place_name = False
#         place_name = None
        
#         # Check if line matches place name patterns
#         for pattern in place_patterns:
#             match = re.match(pattern, line)
#             if match:
#                 place_name = match.group(1).strip()
#                 is_place_name = True
#                 break
        
#         # Also check for lines that look like place names (title case, reasonable length)
#         if not is_place_name and re.match(r'^[A-Z][a-zA-Z\s&,-]{3,40}$', line):
#             # Check if this might be a place name
#             words = line.split()
#             if (len(words) <= 4 and 
#                 not any(word.lower() in ['the', 'and', 'or', 'but', 'with', 'from', 'about', 'more', 'this', 'that'] for word in words) and
#                 not re.search(r'(website|email|phone|contact|follow|subscribe)', line, re.I)):
#                 place_name = line
#                 is_place_name = True
        
#         if is_place_name and place_name:
#             # Save previous place if exists
#             if current_place:
#                 description = ' '.join(current_description).strip()
#                 places.append((current_place, description))
            
#             # Start new place
#             current_place = place_name
#             current_description = []
        
#         elif current_place and len(line) > 20 and len(line) < 500:
#             # This might be description text
#             if not re.search(r'(advertisement|cookie|privacy|subscribe|follow|share|contact)', line, re.I):
#                 current_description.append(line)
        
#         # Limit description length
#         if len(current_description) > 3:
#             current_description = current_description[:3]
    
#     # Add last place
#     if current_place:
#         description = ' '.join(current_description).strip()
#         places.append((current_place, description))
    
#     # Filter and clean results
#     filtered_places = []
#     for name, desc in places:
#         # Skip obvious non-places
#         if (len(name) > 3 and len(name) < 60 and
#             not re.search(r'(advertisement|subscribe|follow|contact|email|website|click|more|read)', name, re.I)):
#             filtered_places.append((name, desc))
    
#     print(f"    🏷️ Extracted {len(filtered_places)} potential places from text")
#     return filtered_places

# # ── Enhanced Caching ─────────────────────────────────────────────────────────
# def cached_html_path(url: str) -> Path:
#     """Generate cache file path"""
#     safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
#     return HTML_CACHE / f"{safe[:150]}.html"

# def load_page_html(url: str, force_reload: bool = False) -> str:
#     """Load HTML with enhanced caching logic"""
#     cache_file = cached_html_path(url)
    
#     if not force_reload and cache_file.exists():
#         cached_html = cache_file.read_text(encoding="utf-8", errors='ignore')
        
#         # Check if cached content is good (has actual text content)
#         soup = BeautifulSoup(cached_html, 'html.parser')
#         text_content = soup.get_text()
        
#         if len(text_content) > 1000:  # Good content threshold
#             print(f"    📦 Using good cached content ({len(cached_html)} chars, {len(text_content)} text)")
#             return cached_html
#         else:
#             print(f"    ⚠️ Cached content is poor ({len(text_content)} text chars), refetching...")
    
#     # Fetch fresh content
#     html = aggressive_content_load(url)
    
#     # Only cache if we got reasonable content
#     if html:
#         soup = BeautifulSoup(html, 'html.parser')
#         text_content = soup.get_text()
#         print(f"    💾 Caching {len(html)} chars ({len(text_content)} text)")
#         cache_file.write_text(html, encoding="utf-8")
    
#     return html

# # ── LLM Enhancement ──────────────────────────────────────────────────────────
# SUMMARIZE_PROMPT = PromptTemplate.from_template("""
# Create a concise travel description for this place. Focus on what makes it special and why someone would visit.
# Keep it factual and 1-2 sentences.

# Place: {name}
# Content: {content}

# Description:""")

# def enhance_description(llm: ChatOpenAI, name: str, content: str) -> str:
#     """Use LLM to enhance extracted content"""
#     if not content or len(content) < 10:
#         return ""
    
#     try:
#         msg = llm.invoke(SUMMARIZE_PROMPT.format(name=name, content=content[:1000]))
#         result = msg.content.strip()
#         return result if len(result) > 10 else content
#     except Exception as e:
#         print(f"    LLM enhancement failed: {e}")
#         return content

# # ── Helper Functions ─────────────────────────────────────────────────────────
# def clean_text(s: str) -> str:
#     """Clean and normalize text"""
#     return re.sub(r"\s+", " ", (s or "").strip())

# def build_query_hints(name: str, context: str = "Karnataka") -> List[str]:
#     """Build query hints for Google Places"""
#     hints = [name]
#     if context and context not in name:
#         hints.append(f"{name} {context}")
#     hints.append(f"{name} India")
#     return hints

# def fuzzy_merge(items: List[Dict[str, Any]], threshold: int = 85) -> List[Dict[str, Any]]:
#     """Merge similar items"""
#     merged = []
    
#     for item in items:
#         found = False
#         for existing in merged:
#             if fuzz.token_set_ratio(item["name"], existing["name"]) >= threshold:
#                 existing["votes"] = existing.get("votes", 1) + 1
#                 if len(item.get("description", "")) > len(existing.get("description", "")):
#                     existing["description"] = item["description"]
#                 existing.setdefault("source_urls", set()).add(item["source_url"])
#                 found = True
#                 break
        
#         if not found:
#             item["votes"] = 1
#             item["source_urls"] = {item["source_url"]}
#             merged.append(item)
    
#     for item in merged:
#         item["source_urls"] = sorted(list(item["source_urls"]))
    
#     return merged

# # ── Main Processing ──────────────────────────────────────────────────────────
# def main():
#     """Main processing function"""
#     if not Path(GROUPS_IN).exists():
#         print(f"Input file not found: {GROUPS_IN}")
#         return
    
#     with open(GROUPS_IN, encoding="utf-8") as f:
#         playlists = json.load(f)
    
#     llm = ChatOpenAI(model=MODEL, temperature=0.0)
#     output = []
    
#     for plist in tqdm(playlists, desc="Processing playlists"):
#         title = plist["playlistTitle"]
#         anchor = plist.get("placeName", "India")
#         subtype = plist.get("subtype", "").lower().strip()
#         urls = [u["url"] for u in plist.get("ClubbedArticles", [])]

#         print(f"\n🎯 Processing: {title}")
#         raw_items = []

#         for url in urls:
#             print(f"  🌐 Processing: {url}")
            
#             # Force reload for debugging - remove 'True' for production
#             html = load_page_html(url, force_reload=True)
            
#             if len(html) < 1000:
#                 print(f"    ❌ Insufficient content ({len(html)} chars)")
#                 continue
            
#             # Try continuous text extraction (handles single-line content)
#             sections = extract_from_continuous_text(html)
#             print(f"    📍 Found {len(sections)} places")
            
#             for name, content in sections:
#                 # Enhanced description
#                 description = enhance_description(llm, name, content)
                
#                 # Build query hints
#                 query_hints = build_query_hints(name, "Karnataka")  # Assume Karnataka for Bangalore-area content
                
#                 raw_items.append({
#                     "name": name,
#                     "description": description,
#                     "travel_time": "",
#                     "price": "",
#                     "location_hint": "Karnataka",
#                     "query_hints": query_hints,
#                     "source_url": url
#                 })

#         if not raw_items:
#             print("  ❌ No items extracted")
#             output.append({
#                 "playlistTitle": title,
#                 "placeName": anchor,
#                 "subtype": subtype,
#                 "source_urls": urls,
#                 "items": []
#             })
#             continue

#         # Merge and dedupe
#         merged = fuzzy_merge(raw_items, threshold=85)
#         print(f"  🔄 After merging: {len(merged)} unique items")

#         # Sort by quality
#         merged.sort(key=lambda x: (
#             -x.get("votes", 0),
#             -len(x.get("description", "")),
#             x.get("name", "").lower()
#         ))

#         final = merged[:FINAL_ITEMS]
        
#         output.append({
#             "playlistTitle": title,
#             "placeName": anchor,
#             "subtype": subtype,
#             "source_urls": urls,
#             "items": final
#         })
        
#         print(f"  ✅ Final: {len(final)} items")
#         # Show first few items for verification
#         for i, item in enumerate(final[:3]):
#             print(f"    {i+1}. {item['name']}: {item['description'][:60]}...")

#     # Write output
#     with open(OUT_PATH, "w", encoding="utf-8") as f:
#         json.dump(output, f, ensure_ascii=False, indent=2)

#     total_items = sum(len(p["items"]) for p in output)
#     items_with_desc = sum(1 for p in output for item in p["items"] if item.get("description"))
    
#     print(f"\n🎉 Completed! Wrote {len(output)} playlists to {OUT_PATH}")
#     print(f"📊 Total items: {total_items}, with descriptions: {items_with_desc}")

# if __name__ == "__main__":
#     main()

# working for tripoto.com  but with error / description capture
# import os, json, re, time
# from typing import List, Dict, Any, Optional, Tuple
# from pathlib import Path
# from dotenv import load_dotenv
# from tqdm import tqdm
# from rapidfuzz import fuzz
# from bs4 import BeautifulSoup

# # LangChain
# from langchain_openai import ChatOpenAI
# from langchain_core.prompts import PromptTemplate
# from langchain_community.document_loaders import WebBaseLoader

# # Selenium fallback
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.chrome import ChromeDriverManager

# load_dotenv()

# # ── CONFIG ─────────────────────────────────────────────────────────────────────
# GROUPS_IN   = "groups.json"
# OUT_PATH    = "playlist_items.json"
# CACHE_DIR   = Path("cache")
# HTML_CACHE  = CACHE_DIR / "html"

# MODEL       = os.getenv("LC_MODEL", "gpt-5-nano")  # used for optional summarization

# # Trim sizes
# SHORTLIST_SIZE = 20
# FINAL_ITEMS    = 15

# FORCE_SELENIUM_DOMAINS = {"tripoto.com", "lbb.in"}
# LONG_SCROLL_DOMAINS = {"tripoto.com", "lbb.in"}
# CLICK_EXPAND_TEXTS = ["read more", "show more", "view more", "load more"]
# COOKIE_BUTTON_TEXTS = ["accept", "agree", "allow all", "i agree"]

# # User-Agent
# DEFAULT_UA = os.getenv(
#     "USER_AGENT",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#     "AppleWebKit/537.36 (KHTML, like Gecko) "
#     "Chrome/125.0.0.0 Safari/537.36"
# )
# os.environ["USER_AGENT"] = DEFAULT_UA

# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# HTML_CACHE.mkdir(exist_ok=True, parents=True)

# # ── Prompts (used only to tidy/condense section text) ──────────────────────────
# SUMMARIZE_PROMPT = PromptTemplate.from_template("""
# You will be given a short section about a single place from a travel list.
# Write 1–3 concise sentences that capture WHY to visit, signature features, and any specifics
# like best season or unique highlights. Do not invent details. Keep it crisp and factual.

# SECTION:
# \"\"\"{section}\"\"\"
# """)

# # ── Helpers ────────────────────────────────────────────────────────────────────
# def clean_text(s: str) -> str:
#     s = re.sub(r"\s+", " ", (s or "").strip())
#     return s

# def strip_fences(text: str) -> str:
#     m = re.search(r"```(?:json)?\s*(\[.*?\]|\{.*?\})\s*```", text, re.S)
#     return m.group(1) if m else text.strip()

# def sanitize_html(raw_html: str, max_chars: int = 120_000) -> str:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
#         tag.decompose()
#     for tag in soup.select("nav, footer, header, aside"):
#         tag.decompose()
#     return str(soup)[:max_chars]

# def extract_page_title(raw_html: str) -> str:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     t = soup.title.get_text(" ", strip=True) if soup.title else ""
#     return clean_text(t)

# # Heuristics for “near <city>” or “in <state>” style hints
# NEAR_RE = re.compile(r"\bnear\s+([A-Za-z][A-Za-z\s&.-]{2,})\b", re.I)
# IN_RE   = re.compile(r"\bin\s+([A-Za-z][A-Za-z\s&.-]{2,})\b", re.I)

# def infer_location_hint_from_title(title: str, fallback_anchor: str) -> str:
#     if not title: return fallback_anchor or "India"
#     m = NEAR_RE.search(title)
#     if m: return clean_text(m.group(1))
#     m = IN_RE.search(title)
#     if m: return clean_text(m.group(1))
#     return fallback_anchor or "India"

# CATEGORY_HINTS = {
#     "beach":       re.compile(r"\bbeach(es)?\b", re.I),
#     "island":      re.compile(r"\bisland(s)?\b", re.I),
#     "waterfall":   re.compile(r"\bwaterfall(s)?|falls\b", re.I),
#     "national_park": re.compile(r"\bnational\s+park|tiger\s+reserve|sanctuary\b", re.I),
#     "trek":        re.compile(r"\btrek(s)?|trail(s)?|pass\b", re.I),
#     "lake":        re.compile(r"\blake\b", re.I),
#     "fort":        re.compile(r"\bfort\b", re.I),
#     "temple":      re.compile(r"\btemple\b", re.I),
#     "wildlife":    re.compile(r"\bwildlife|safari\b", re.I),
# }

# def infer_category_hint(name: str, section_text: str, page_title: str) -> Optional[str]:
#     blob = " ".join([name or "", section_text or "", page_title or ""])
#     for cat, rx in CATEGORY_HINTS.items():
#         if rx.search(blob):
#             return cat
#     return None

# # Extract “2 days”, “4–6 hours”, “3h 30m”, etc.
# TIME_PAT = re.compile(
#     r"\b((?:\d{1,2}\s*(?:days?|d))|(?:\d{1,2}\s*(?:hours?|hrs?|h)(?:\s*\d{1,2}\s*(?:minutes?|mins?|m))?)|(?:\d{1,2}\s*-\s*\d{1,2}\s*(?:hours?|hrs?)))\b",
#     re.I,
# )
# # Extract ₹/INR price-like snippets
# PRICE_PAT = re.compile(
#     r"(?:₹|INR|Rs\.?)\s?[\d,]+(?:\s?(?:per\s?(?:night|day|person)|pp|/night|/day))?",
#     re.I,
# )

# def parse_travel_time(text: str) -> str:
#     m = TIME_PAT.search(text or "")
#     return clean_text(m.group(0)) if m else ""

# def parse_price(text: str) -> str:
#     m = PRICE_PAT.search(text or "")
#     return clean_text(m.group(0)) if m else ""

# def section_excerpt(text: str, max_chars: int = 400) -> str:
#     t = clean_text(text)
#     return (t[:max_chars] + "…") if len(t) > max_chars else t

# # ── Loaders ────────────────────────────────────────────────────────────────────
# def fetch_http(url: str) -> str:
#     docs = WebBaseLoader(url, header_template={"User-Agent": DEFAULT_UA}).load()
#     return docs[0].page_content if docs else ""

# # def fetch_selenium(url: str) -> str:
# #     opts = Options()
# #     opts.add_argument("--headless=new")
# #     opts.add_argument("--no-sandbox")
# #     opts.add_argument("--disable-dev-shm-usage")
# #     driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
# #     try:
# #         driver.get(url)
# #         WebDriverWait(driver, 10).until(
# #             lambda d: d.execute_script("return document.readyState") == "complete"
# #         )
# #         for _ in range(3):
# #             ActionChains(driver).scroll_by_amount(0, 1400).perform()
# #             time.sleep(0.6)
# #         # best-effort: wait for typical containers
# #         try:
# #             WebDriverWait(driver, 5).until(
# #                 EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article, .content, .post, .list"))
# #             )
# #         except Exception:
# #             pass
# #         return driver.page_source
# #     finally:
# #         driver.quit()
# def fetch_selenium(url: str) -> str:
#     opts = Options()
#     opts.add_argument("--headless=new")
#     opts.add_argument("--no-sandbox")
#     opts.add_argument("--disable-dev-shm-usage")
#     opts.add_argument(f"--user-agent={DEFAULT_UA}")
#     driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
#     try:
#         driver.get(url)
#         WebDriverWait(driver, 12).until(lambda d: d.execute_script("return document.readyState") == "complete")

#         # Try to close cookie banners
#         try:
#             for txt in COOKIE_BUTTON_TEXTS:
#                 els = driver.find_elements(By.XPATH, f"//button[translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')='{txt}'] | //*[self::a or self::div or self::span][contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), '{txt}')]")
#                 if els:
#                     try: els[0].click()
#                     except: pass
#         except: pass

#         # Long scroll loop for heavy JS pages
#         max_scrolls = 14 if any(h in url for h in LONG_SCROLL_DOMAINS) else 6
#         last_h = 0
#         for i in range(max_scrolls):
#             ActionChains(driver).scroll_by_amount(0, 1600).perform()
#             time.sleep(0.6)
#             # click any expanders that appear
#             try:
#                 for txt in CLICK_EXPAND_TEXTS:
#                     btns = driver.find_elements(By.XPATH, f"//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), '{txt}')]")
#                     for b in btns[:3]:
#                         try: b.click(); time.sleep(0.3)
#                         except: pass
#             except: pass
#             # stop if we reached the bottom (no height growth)
#             h = driver.execute_script("return document.body.scrollHeight")
#             if h == last_h: break
#             last_h = h

#         # Wait for typical article containers
#         try:
#             WebDriverWait(driver, 5).until(
#                 EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article, .post, [data-testid*='article']"))
#             )
#         except: pass

#         return driver.page_source
#     finally:
#         driver.quit()


# def cached_html_path(url: str) -> Path:
#     safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
#     return HTML_CACHE / f"{safe[:150]}.html"

# # def load_page_html(url: str) -> str:
# #     cache_file = cached_html_path(url)
# #     if cache_file.exists():
# #         return cache_file.read_text(encoding="utf-8")
# #     html = ""
# #     try:
# #         html = fetch_http(url)
# #     except Exception:
# #         html = ""
# #     if len(html) < 1500:
# #         html = fetch_selenium(url)
# #     cache_file.write_text(html or "", encoding="utf-8")
# #     return html
# def load_page_html(url: str) -> str:
#     cache_file = cached_html_path(url)
#     if cache_file.exists():
#         return cache_file.read_text(encoding="utf-8")

#     from urllib.parse import urlparse
#     domain = urlparse(url).netloc.replace("www.", "").lower()
#     html = ""
#     try:
#         if domain in FORCE_SELENIUM_DOMAINS:
#             html = fetch_selenium(url)
#         else:
#             html = fetch_http(url)
#             if len(html) < 2000 or "cf-chl-jschl" in html.lower() or "enable javascript" in html.lower():
#                 html = fetch_selenium(url)
#     except Exception:
#         html = fetch_selenium(url)

#     cache_file.write_text(html or "", encoding="utf-8")
#     return html

# # ── DOM-first section extraction (works well for LBB) ──────────────────────────
# HEADER_SEL = "h2, h3, h4, li > strong, li > b"

# SKIP_PHRASES = re.compile(
#     r"(also read|editor'?s note|disclaimer|how to reach|where to stay|contact:)",
#     re.I,
# )

# def iter_sections(soup: BeautifulSoup) -> List[Tuple[str, str]]:
#     """
#     Yields (name, section_text) pairs by walking headings and their following siblings
#     until the next heading. If list items use <li><strong>Name</strong> ...</li>, we
#     treat the remainder of the <li> as the section text.
#     """
#     out: List[Tuple[str, str]] = []
#     headers = soup.select(HEADER_SEL)
#     used_ids = set()

#     for h in headers:
#         # Get candidate name
#         if h.name in ("strong", "b") and h.parent and h.parent.name == "li":
#             name = clean_text(h.get_text(" ", strip=True))
#             # Section text = rest of the <li> without the <strong> tag text
#             li_text = clean_text(h.parent.get_text(" ", strip=True))
#             sect = li_text[len(name):].strip(" -:—") if li_text.lower().startswith(name.lower()) else li_text
#         else:
#             name = clean_text(h.get_text(" ", strip=True))
#             # Gather next siblings until next header
#             parts = []
#             sib = h.next_sibling
#             steps = 0
#             while sib and steps < 12:
#                 steps += 1
#                 if getattr(sib, "name", None) and re.match(r"^h[2-4]$", sib.name, re.I):
#                     break
#                 if getattr(sib, "name", None) in ("p", "li", "ul", "ol", "div"):
#                     txt = clean_text(BeautifulSoup(str(sib), "html.parser").get_text(" ", strip=True))
#                     if txt and not SKIP_PHRASES.search(txt):
#                         parts.append(txt)
#                 sib = sib.next_sibling
#             sect = clean_text(" ".join(parts))

#         # Filter obvious junk
#         if not name or len(name) < 2:
#             continue
#         # Avoid duplicates if anchors repeat
#         key = (name.lower(), sect[:80].lower())
#         if key in used_ids:
#             continue
#         used_ids.add(key)

#         out.append((name, sect))
#     return out

# # ── LLM summarization on the extracted section (optional) ─────────────────────
# def summarize_section(llm: ChatOpenAI, section_text: str) -> str:
#     if not section_text or len(section_text) < 40:
#         return section_text  # already short; keep as-is
#     try:
#         msg = llm.invoke(SUMMARIZE_PROMPT.format(section=section_text[:3000]))
#         return clean_text(msg.content)
#     except Exception:
#         return section_text

# # ── Merge / voting ────────────────────────────────────────────────────────────
# def fuzzy_merge(items: List[Dict[str, Any]], threshold: int = 92) -> List[Dict[str, Any]]:
#     merged: List[Dict[str, Any]] = []
#     for it in items:
#         found = False
#         for m in merged:
#             if fuzz.token_set_ratio(it["name"], m["name"]) >= threshold:
#                 m["votes"] = m.get("votes", 1) + 1
#                 # prefer longer/better description
#                 if len(it.get("description","")) > len(m.get("description","")):
#                     m["description"] = it["description"]
#                     m["section_excerpt"] = it.get("section_excerpt", m.get("section_excerpt"))
#                 # carry earliest non-empty fields
#                 for k in ("travel_time","price"):
#                     if not m.get(k) and it.get(k):
#                         m[k] = it[k]
#                 m.setdefault("source_urls", set()).add(it["source_url"])
#                 # union hints
#                 m.setdefault("query_hints", set()).update(it.get("query_hints", []))
#                 found = True
#                 break
#         if not found:
#             it["votes"] = 1
#             it["source_urls"] = {it["source_url"]}
#             it["query_hints"] = set(it.get("query_hints", []))
#             merged.append(it)
#     for m in merged:
#         m["source_urls"] = sorted(list(m["source_urls"]))
#         m["query_hints"] = sorted(list(m["query_hints"]))
#     return merged

# # ── Main ───────────────────────────────────────────────────────────────────────
# def main():
#     playlists = json.load(open(GROUPS_IN, encoding="utf-8"))
#     llm = ChatOpenAI(model=MODEL, temperature=0.0)

#     output = []
#     for plist in tqdm(playlists, desc="Playlists"):
#         title   = plist["playlistTitle"]
#         anchor  = plist.get("placeName", "")  # e.g., "India" or "Bangalore"
#         subtype = plist.get("subtype", "").lower().strip()
#         urls    = [u["url"] for u in plist["ClubbedArticles"]]

#         # infer page-wide hints from titles later
#         raw_items: List[Dict[str, Any]] = []

#         for u in urls:
#             html = load_page_html(u)
#             page_title = extract_page_title(html)
#             loc_hint_from_title = infer_location_hint_from_title(page_title, anchor)

#             # DOM-first extraction
#             sec_pairs = iter_sections(BeautifulSoup(sanitize_html(html), "html.parser"))
#             for name, sect in sec_pairs:
#                 # Summaries improve noisy LBB/UGC sections
#                 desc = summarize_section(llm, sect)
#                 tt = parse_travel_time(sect)
#                 pp = parse_price(sect)
#                 cat_hint = infer_category_hint(name, sect, page_title)

#                 # Build Places query helpers
#                 qhints = {name}
#                 if loc_hint_from_title:
#                     qhints.add(f"{name} {loc_hint_from_title}")
#                 if cat_hint and cat_hint not in name.lower():
#                     qhints.add(f"{name} {cat_hint.replace('_',' ')}")
#                 # mild state guess from anchor (if anchor looks like a city/state)
#                 if anchor and anchor.lower() not in (name.lower()):
#                     qhints.add(f"{name} {anchor}")

#                 raw_items.append({
#                     "name": name,
#                     "section_title": name,
#                     "section_excerpt": section_excerpt(sect),
#                     "description": desc,
#                     "travel_time": tt,
#                     "price": pp,
#                     "location_hint": loc_hint_from_title,  # e.g., "Bangalore" or "Karnataka"
#                     "category_hint": cat_hint,             # e.g., "waterfall", "beach"
#                     "query_hints": sorted(qhints),
#                     "anchor_city": anchor,
#                     "source_title": page_title,
#                     "source_url": u,
#                 })

#             # If a page yielded nothing via DOM (rare), last-resort: names only from headings
#             if not sec_pairs:
#                 soup = BeautifulSoup(html, "html.parser")
#                 for el in soup.select("h2, h3, h4"):
#                     n = clean_text(el.get_text(" ", strip=True))
#                     if n:
#                         raw_items.append({
#                             "name": n, "section_title": n, "section_excerpt": "",
#                             "description": "", "travel_time": "", "price": "",
#                             "location_hint": loc_hint_from_title,
#                             "category_hint": infer_category_hint(n, "", page_title),
#                             "query_hints": [n, f"{n} {loc_hint_from_title}", f"{n} {anchor}"] if anchor else [n],
#                             "anchor_city": anchor,
#                             "source_title": page_title,
#                             "source_url": u
#                         })

#         # If page(s) truly produced nothing, push empty playlist to keep shape
#         if not raw_items:
#             output.append({
#                 "playlistTitle": title,
#                 "placeName": anchor,
#                 "subtype": subtype,
#                 "source_urls": urls,
#                 "items": []
#             })
#             continue

#         # Dedupe / vote
#         merged = fuzzy_merge(raw_items, threshold=92)

#         # Rank & trim — prefer vote count, then has description, then name alpha
#         merged.sort(
#             key=lambda x: (
#                 -x.get("votes", 0),
#                 -(1 if x.get("description") else 0),
#                 -len((x.get("description") or "")),
#                 (x.get("name") or "").lower()
#             )
#         )
#         final = merged[:FINAL_ITEMS]

#         output.append({
#             "playlistTitle": title,
#             "placeName": anchor,
#             "subtype": subtype,
#             "source_urls": urls,
#             "items": final
#         })

#     with open(OUT_PATH, "w", encoding="utf-8") as f:
#         json.dump(output, f, ensure_ascii=False, indent=2)

#     print(f"✅ Wrote {len(output)} playlists to {OUT_PATH}")

# if __name__ == "__main__":
#     main()


# Working for holidify.com without description capture
# import os, json, re, time
# from typing import List, Dict, Any
# from pathlib import Path
# from dotenv import load_dotenv
# from tqdm import tqdm
# from rapidfuzz import fuzz
# from bs4 import BeautifulSoup

# # LangChain
# from langchain_openai import ChatOpenAI
# from langchain_core.prompts import PromptTemplate
# from langchain_community.document_loaders import WebBaseLoader

# # Selenium fallback
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.chrome import ChromeDriverManager

# load_dotenv()

# # ── CONFIG ─────────────────────────────────────────────────────────────────────
# GROUPS_IN   = "groups.json"
# OUT_PATH    = "playlist_items.json"
# CACHE_DIR   = Path("cache")
# HTML_CACHE  = CACHE_DIR / "html"

# MODEL       = os.getenv("LC_MODEL", "gpt-5-nano")

# # Trim sizes
# SHORTLIST_SIZE = 20
# FINAL_ITEMS    = 15

# # User-Agent
# DEFAULT_UA = os.getenv(
#     "USER_AGENT",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#     "AppleWebKit/537.36 (KHTML, like Gecko) "
#     "Chrome/125.0.0.0 Safari/537.36"
# )
# os.environ["USER_AGENT"] = DEFAULT_UA

# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# HTML_CACHE.mkdir(exist_ok=True, parents=True)

# # ── Prompts ────────────────────────────────────────────────────────────────────
# EXTRACT_PROMPT = PromptTemplate.from_template("""
# Extract recommended items from this travel collection page.

# Return ONLY a JSON array. Each element:
# {
#   "name": "<place or trek name>",
#   "description": "<1-3 concise sentences from page>",
#   "travel_time": "<e.g., '2 hours 50 minutes' or ''>",
#   "price": "<e.g., 'Starting at INR 5,300 per night' or ''>"
# }

# Guidelines:
# - Prefer items that are actually listed as recommendations.
# - Strip HTML tags/entities; keep concise human text.
# - If price or travel time appears in bullets / bold / near the item, include it; else "".
# - If the page lists fewer than 15, return as many as it truly has.

# HTML:
# ```html
# {html}
# ```""")

# NAMES_ONLY_PROMPT = PromptTemplate.from_template("""
# Extract up to 25 recommended items from this page.
# Return ONLY a JSON array of strings (names only, no objects, no markdown).

# HTML:
# ```html
# {html}
# ```""")

# # ── Helpers ────────────────────────────────────────────────────────────────────
# def strip_fences(text: str) -> str:
#     m = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", text, re.S)
#     return m.group(1) if m else text.strip()

# def sanitize_html(raw_html: str, max_chars: int = 45000) -> str:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
#         tag.decompose()
#     for tag in soup.select("nav, footer, header"):
#         tag.decompose()
#     return str(soup)[:max_chars]

# # ── Loaders ────────────────────────────────────────────────────────────────────
# def fetch_http(url: str) -> str:
#     docs = WebBaseLoader(url, header_template={"User-Agent": DEFAULT_UA}).load()
#     return docs[0].page_content if docs else ""

# def fetch_selenium(url: str) -> str:
#     opts = Options()
#     opts.add_argument("--headless=new")
#     opts.add_argument("--no-sandbox")
#     opts.add_argument("--disable-dev-shm-usage")
#     driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
#     try:
#         driver.get(url)
#         WebDriverWait(driver, 8).until(
#             lambda d: d.execute_script("return document.readyState") == "complete"
#         )
#         for _ in range(2):
#             ActionChains(driver).scroll_by_amount(0, 1200).perform()
#             time.sleep(0.6)
#         try:
#             WebDriverWait(driver, 4).until(
#                 EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article, .content, .post, .list"))
#             )
#         except Exception:
#             pass
#         return driver.page_source
#     finally:
#         driver.quit()

# def cached_html_path(url: str) -> Path:
#     safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
#     return HTML_CACHE / f"{safe[:150]}.html"

# def load_page_html(url: str) -> str:
#     cache_file = cached_html_path(url)
#     if cache_file.exists():
#         return cache_file.read_text(encoding="utf-8")
#     html = ""
#     try:
#         html = fetch_http(url)
#     except Exception:
#         html = ""
#     if len(html) < 1500:
#         html = fetch_selenium(url)
#     cache_file.write_text(html or "", encoding="utf-8")
#     return html

# # ── LLM extraction ─────────────────────────────────────────────────────────────
# def extract_items_llm(llm: ChatOpenAI, raw_html: str) -> List[Dict[str, Any]]:
#     snippet = sanitize_html(raw_html)
#     # 1) structured attempt
#     try:
#         msg = llm.invoke(EXTRACT_PROMPT.format(html=snippet))
#         text = strip_fences(getattr(msg, "content", str(msg)))
#         arr = json.loads(text)
#         out = []
#         for it in arr:
#             name = (it.get("name") or "").strip()
#             if not name:
#                 continue
#             out.append({
#                 "name": name,
#                 "description": (it.get("description") or "").strip(),
#                 "travel_time": (it.get("travel_time") or "").strip(),
#                 "price": (it.get("price") or "").strip(),
#             })
#         if out:
#             return out
#     except Exception:
#         pass
#     # 2) names-only fallback
#     try:
#         msg = llm.invoke(NAMES_ONLY_PROMPT.format(html=snippet))
#         text = strip_fences(getattr(msg, "content", str(msg)))
#         names = json.loads(text)
#         out = []
#         for n in names:
#             if isinstance(n, str) and n.strip():
#                 out.append({"name": n.strip(), "description": "", "travel_time": "", "price": ""})
#         if out:
#             return out
#     except Exception:
#         pass
#     # 3) heuristic scrape (headings/lists)
#     return heuristic_extract_items(raw_html)

# def heuristic_extract_items(raw_html: str, limit: int = 25) -> List[Dict[str, Any]]:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     texts: List[str] = []
#     for sel in ["h2", "h3", "h4", "li strong", "li b"]:
#         for el in soup.select(sel):
#             t = el.get_text(" ", strip=True)
#             if 2 <= len(t) <= 100:
#                 texts.append(t)
#     seen = set()
#     names = []
#     for t in texts:
#         k = re.sub(r"\s+", " ", t).strip().lower()
#         if k not in seen:
#             seen.add(k); names.append(t)
#         if len(names) >= limit: break
#     return [{"name": n, "description": "", "travel_time": "", "price": ""} for n in names]

# # ── Merge / voting ────────────────────────────────────────────────────────────
# def fuzzy_merge(items: List[Dict[str, Any]], threshold: int = 92) -> List[Dict[str, Any]]:
#     merged: List[Dict[str, Any]] = []
#     for it in items:
#         found = False
#         for m in merged:
#             if fuzz.token_set_ratio(it["name"], m["name"]) >= threshold:
#                 m["votes"] = m.get("votes", 1) + 1
#                 if len(it.get("description","")) > len(m.get("description","")):
#                     m["description"] = it["description"]
#                 if not m.get("travel_time") and it.get("travel_time"):
#                     m["travel_time"] = it["travel_time"]
#                 if not m.get("price") and it.get("price"):
#                     m["price"] = it["price"]
#                 m.setdefault("source_urls", set()).add(it["source_url"])
#                 found = True
#                 break
#         if not found:
#             it["votes"] = 1
#             it["source_urls"] = {it["source_url"]}
#             merged.append(it)
#     for m in merged:
#         m["source_urls"] = sorted(list(m["source_urls"]))
#     return merged

# # ── Main ───────────────────────────────────────────────────────────────────────
# def main():
#     playlists = json.load(open(GROUPS_IN, encoding="utf-8"))
#     llm = ChatOpenAI(model=MODEL, temperature=0.0)

#     output = []
#     for plist in tqdm(playlists, desc="Playlists"):
#         title   = plist["playlistTitle"]
#         anchor  = plist.get("placeName", "")
#         subtype = plist.get("subtype", "").lower().strip()
#         urls    = [u["url"] for u in plist["ClubbedArticles"]]

#         # 1) extract per-URL
#         raw_items: List[Dict[str, Any]] = []
#         for u in urls:
#             html = load_page_html(u)
#             items = extract_items_llm(llm, html)
#             for it in items:
#                 it["source_url"] = u
#                 raw_items.append(it)

#         if not raw_items:
#             output.append({
#                 "playlistTitle": title,
#                 "placeName": anchor,
#                 "subtype": subtype,
#                 "source_urls": urls,
#                 "items": []
#             })
#             continue

#         # 2) dedupe / vote
#         merged = fuzzy_merge(raw_items, threshold=92)

#         # 3) rank & trim (no Places fields here)
#         merged.sort(
#             key=lambda x: (
#                 -x.get("votes", 0),
#                 -len((x.get("description") or "").strip()),
#                 (x.get("name") or "").lower()
#             )
#         )
#         final = merged[:FINAL_ITEMS]

#         output.append({
#             "playlistTitle": title,
#             "placeName": anchor,
#             "subtype": subtype,
#             "source_urls": urls,
#             "items": final
#         })

#     with open(OUT_PATH, "w", encoding="utf-8") as f:
#         json.dump(output, f, ensure_ascii=False, indent=2)

#     print(f"✅ Wrote {len(output)} playlists to {OUT_PATH}")

# if __name__ == "__main__":
#     main()

# # Working for holidify & LLB( with description capture)
# #!/usr/bin/env python3
# import os, json, re, time
# from typing import List, Dict, Any, Optional
# from pathlib import Path
# from dotenv import load_dotenv
# from tqdm import tqdm
# from rapidfuzz import fuzz
# from bs4 import BeautifulSoup

# # LangChain
# from langchain_openai import ChatOpenAI
# from langchain_core.prompts import PromptTemplate
# from langchain_core.output_parsers import StrOutputParser
# from langchain_community.document_loaders import WebBaseLoader

# # Selenium fallback
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.chrome import ChromeDriverManager

# # Google Maps (optional)
# try:
#     import googlemaps
# except ImportError:
#     googlemaps = None

# load_dotenv()

# # ── CONFIG ─────────────────────────────────────────────────────────────────────
# GROUPS_IN         = "groups.json"  # adjust if needed
# OUT_PATH          = "playlist_items.json"
# CACHE_DIR         = Path("cache")
# HTML_CACHE        = CACHE_DIR / "html"
# PLACES_CACHE_FILE = CACHE_DIR / "places_cache.json"

# MODEL             = os.getenv("LC_MODEL", "gpt-4o-mini")
# GOOGLE_KEY        = os.getenv("GOOGLE_MAPS_API_KEY")
# RESOLVE_PLACES    = bool(GOOGLE_KEY and googlemaps)

# # Country/region bias for free-text Place search (no anchor city)
# PLACES_QUERY_SUFFIX = ", Bangalore"

# # Trim sizes
# SHORTLIST_SIZE    = 20
# FINAL_ITEMS       = 15

# # User-Agent for HTTP loader (and also exported to env for loaders that read it)
# DEFAULT_UA = os.getenv(
#     "USER_AGENT",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#     "AppleWebKit/537.36 (KHTML, like Gecko) "
#     "Chrome/125.0.0.0 Safari/537.36"
# )
# os.environ["USER_AGENT"] = DEFAULT_UA  # many loaders read this

# CACHE_DIR.mkdir(exist_ok=True, parents=True)
# HTML_CACHE.mkdir(exist_ok=True, parents=True)

# # ── Google client & cache ──────────────────────────────────────────────────────
# if RESOLVE_PLACES:
#     gmaps = googlemaps.Client(key=GOOGLE_KEY)  # type: ignore
# else:
#     gmaps = None

# places_cache: Dict[str, Optional[dict]] = {}
# if PLACES_CACHE_FILE.exists():
#     try:
#         places_cache = json.load(open(PLACES_CACHE_FILE, encoding="utf-8"))
#     except Exception:
#         places_cache = {}

# def save_places_cache():
#     with open(PLACES_CACHE_FILE, "w", encoding="utf-8") as f:
#         json.dump(places_cache, f, ensure_ascii=False, indent=2)

# # ── Prompts ────────────────────────────────────────────────────────────────────
# EXTRACT_PROMPT = PromptTemplate.from_template("""
# Extract recommended items from this travel collection page.

# Return ONLY a JSON array. Each element:
# {{
#   "name": "<place or trek name>",
#   "description": "<1-3 concise sentences from page>",
#   "travel_time": "<e.g., '2 hours 50 minutes' or ''>",
#   "price": "<e.g., 'Starting at INR 5,300 per night' or ''>"
# }}

# Guidelines:
# - Prefer items that are actually listed as recommendations.
# - Strip HTML tags/entities; keep concise human text.
# - If price or travel time appears in bullets / bold / near the item, include it; else "".
# - If the page lists fewer than 15, return as many as it truly has.

# HTML:
# ```html
# {html}
# ```""")

# NAMES_ONLY_PROMPT = PromptTemplate.from_template("""
# Extract up to 25 recommended items from this page.
# Return ONLY a JSON array of strings (names only, no objects, no markdown).

# HTML:
# ```html
# {html}
# ```""")

# # ── Helpers ────────────────────────────────────────────────────────────────────
# def strip_fences(text: str) -> str:
#     m = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", text, re.S)
#     return m.group(1) if m else text.strip()

# def sanitize_html(raw_html: str, max_chars: int = 45000) -> str:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
#         tag.decompose()
#     # drop super noisy navs/footers
#     for tag in soup.select("nav, footer, header"):
#         tag.decompose()
#     return str(soup)[:max_chars]

# # ── Loaders ────────────────────────────────────────────────────────────────────
# def fetch_http(url: str) -> str:
#     # header_template is respected by WebBaseLoader (modern versions)
#     docs = WebBaseLoader(url, header_template={"User-Agent": DEFAULT_UA}).load()
#     return docs[0].page_content if docs else ""

# def fetch_selenium(url: str) -> str:
#     opts = Options()
#     opts.add_argument("--headless=new")
#     opts.add_argument("--no-sandbox")
#     opts.add_argument("--disable-dev-shm-usage")
#     driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
#     try:
#         driver.get(url)
#         WebDriverWait(driver, 8).until(
#             lambda d: d.execute_script("return document.readyState") == "complete"
#         )
#         # light scroll to trigger lazy content
#         for _ in range(2):
#             ActionChains(driver).scroll_by_amount(0, 1200).perform()
#             time.sleep(0.6)
#         # try waiting for common article containers (best-effort)
#         try:
#             WebDriverWait(driver, 4).until(
#                 EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article, .content, .post, .list"))
#             )
#         except Exception:
#             pass
#         return driver.page_source
#     finally:
#         driver.quit()

# def cached_html_path(url: str) -> Path:
#     safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)
#     return HTML_CACHE / f"{safe[:150]}.html"

# def load_page_html(url: str) -> str:
#     cache_file = cached_html_path(url)
#     if cache_file.exists():
#         return cache_file.read_text(encoding="utf-8")

#     html = ""
#     try:
#         html = fetch_http(url)
#     except Exception:
#         html = ""
#     # If it’s suspiciously small, try Selenium
#     if len(html) < 1500:
#         html = fetch_selenium(url)

#     cache_file.write_text(html or "", encoding="utf-8")
#     return html

# # ── LLM extraction ─────────────────────────────────────────────────────────────
# def extract_items_llm(llm: ChatOpenAI, raw_html: str) -> List[Dict[str, Any]]:
#     snippet = sanitize_html(raw_html)
#     # 1) try full schema
#     try:
#         msg = llm.invoke(EXTRACT_PROMPT.format(html=snippet))
#         text = strip_fences(getattr(msg, "content", str(msg)))
#         arr = json.loads(text)
#         out = []
#         for it in arr:
#             name = (it.get("name") or "").strip()
#             if not name:
#                 continue
#             out.append({
#                 "name": name,
#                 "description": (it.get("description") or "").strip(),
#                 "travel_time": (it.get("travel_time") or "").strip(),
#                 "price": (it.get("price") or "").strip(),
#             })
#         if out:
#             return out
#     except Exception:
#         pass

#     # 2) fallback → names-only prompt
#     try:
#         msg = llm.invoke(NAMES_ONLY_PROMPT.format(html=snippet))
#         text = strip_fences(getattr(msg, "content", str(msg)))
#         names = json.loads(text)
#         out = []
#         for n in names:
#             if isinstance(n, str) and n.strip():
#                 out.append({
#                     "name": n.strip(),
#                     "description": "",
#                     "travel_time": "",
#                     "price": "",
#                 })
#         if out:
#             return out
#     except Exception:
#         pass

#     # 3) last resort → heuristic scrape of headings/lists
#     return heuristic_extract_items(raw_html)

# def heuristic_extract_items(raw_html: str, limit: int = 25) -> List[Dict[str, Any]]:
#     soup = BeautifulSoup(raw_html, "html.parser")
#     texts: List[str] = []

#     # Headings often contain the item names
#     for sel in ["h2", "h3", "h4", "li strong", "li b"]:
#         for el in soup.select(sel):
#             t = el.get_text(" ", strip=True)
#             if 2 <= len(t) <= 100:
#                 texts.append(t)

#     # Deduplicate while preserving order
#     seen = set()
#     names = []
#     for t in texts:
#         k = re.sub(r"\s+", " ", t).strip().lower()
#         if k not in seen:
#             seen.add(k)
#             names.append(t)
#         if len(names) >= limit:
#             break

#     return [{"name": n, "description": "", "travel_time": "", "price": ""} for n in names]

# # ── Merge / voting ────────────────────────────────────────────────────────────
# def fuzzy_merge(items: List[Dict[str, Any]], threshold: int = 92) -> List[Dict[str, Any]]:
#     merged: List[Dict[str, Any]] = []
#     for it in items:
#         found = False
#         for m in merged:
#             if fuzz.token_set_ratio(it["name"], m["name"]) >= threshold:
#                 m["votes"] = m.get("votes", 1) + 1
#                 if len(it.get("description","")) > len(m.get("description","")):
#                     m["description"] = it["description"]
#                 if not m.get("travel_time") and it.get("travel_time"):
#                     m["travel_time"] = it["travel_time"]
#                 if not m.get("price") and it.get("price"):
#                     m["price"] = it["price"]
#                 m.setdefault("source_urls", set()).add(it["source_url"])
#                 found = True
#                 break
#         if not found:
#             it["votes"] = 1
#             it["source_urls"] = {it["source_url"]}
#             merged.append(it)
#     for m in merged:
#         m["source_urls"] = sorted(list(m["source_urls"]))
#     return merged

# # ── Place resolution (no anchor city) ─────────────────────────────────────────
# def resolve_place_no_anchor(name: str) -> Optional[Dict[str, Any]]:
#     if not RESOLVE_PLACES or not gmaps:
#         return None
#     key = name.lower()
#     if key in places_cache:
#         return places_cache[key]
#     q = (name + PLACES_QUERY_SUFFIX).strip()
#     try:
#         res = gmaps.find_place(input=q, input_type="textquery")
#         cands = res.get("candidates", [])
#         if not cands:
#             places_cache[key] = None
#             return None
#         pid = cands[0]["place_id"]
#         det = gmaps.place(
#             place_id=pid,
#             fields=["place_id","name","geometry/location","types","rating","user_ratings_total"]
#         )
#         places_cache[key] = det.get("result")
#         time.sleep(0.05)
#         return places_cache[key]
#     except Exception:
#         places_cache[key] = None
#         return None

# # ── Main ───────────────────────────────────────────────────────────────────────
# def main():
#     playlists = json.load(open(GROUPS_IN, encoding="utf-8"))
#     llm = ChatOpenAI(model=MODEL, temperature=0.0)

#     output = []
#     for plist in tqdm(playlists, desc="Playlists"):
#         title   = plist["playlistTitle"]
#         anchor  = plist.get("placeName", "")
#         subtype = plist.get("subtype", "").lower().strip()
#         urls    = [u["url"] for u in plist["ClubbedArticles"]]

#         # 1) extract per-URL
#         raw_items: List[Dict[str, Any]] = []
#         for u in urls:
#             html = load_page_html(u)
#             items = extract_items_llm(llm, html)
#             for it in items:
#                 it["source_url"] = u
#                 raw_items.append(it)

#         if not raw_items:
#             output.append({
#                 "playlistTitle": title,
#                 "placeName": anchor,
#                 "subtype": subtype,
#                 "source_urls": urls,
#                 "items": []
#             })
#             continue

#         # 2) dedupe / vote
#         merged = fuzzy_merge(raw_items, threshold=92)

#         # 3) optional place resolution (no distance)
#         enriched = []
#         for it in merged:
#             det = resolve_place_no_anchor(it["name"])
#             if det and det.get("geometry"):
#                 loc = det["geometry"]["location"]
#                 enriched.append({
#                     "name": det.get("name", it["name"]),
#                     "place_id": det.get("place_id"),
#                     "lat": loc.get("lat"),
#                     "lng": loc.get("lng"),
#                     "types": det.get("types", []),
#                     "rating": det.get("rating", 0),
#                     "reviews": det.get("user_ratings_total", 0),
#                     "votes": it["votes"],
#                     "description": it.get("description",""),
#                     "travel_time": it.get("travel_time",""),
#                     "price": it.get("price",""),
#                     "source_urls": it["source_urls"]
#                 })
#             else:
#                 enriched.append({
#                     "name": it["name"],
#                     "place_id": None,
#                     "lat": None,
#                     "lng": None,
#                     "types": [],
#                     "rating": 0,
#                     "reviews": 0,
#                     "votes": it["votes"],
#                     "description": it.get("description",""),
#                     "travel_time": it.get("travel_time",""),
#                     "price": it.get("price",""),
#                     "source_urls": it["source_urls"]
#                 })

#         # 4) rank & trim
#         enriched.sort(key=lambda x: (-x["votes"], -x.get("rating", 0), -x.get("reviews", 0)))
#         shortlist = enriched[:SHORTLIST_SIZE]
#         final = shortlist[:FINAL_ITEMS]

#         output.append({
#             "playlistTitle": title,
#             "placeName": anchor,
#             "subtype": subtype,
#             "source_urls": urls,
#             "items": final
#         })

#         if RESOLVE_PLACES:
#             save_places_cache()

#     with open(OUT_PATH, "w", encoding="utf-8") as f:
#         json.dump(output, f, ensure_ascii=False, indent=2)

#     print(f"✅ Wrote {len(output)} playlists to {OUT_PATH}")

# if __name__ == "__main__":
#     main()



# Gemini Version
# import os
# import json
# import asyncio
# import re
# from collections import Counter
# from dotenv import load_dotenv
# from tqdm import tqdm
# import googlemaps
# import aiohttp

# from langchain_community.document_loaders.url import UnstructuredURLLoader
# from langchain_openai import ChatOpenAI
# from langchain_core.prompts import PromptTemplate

# load_dotenv()

# # ─── CONFIG ────────────────────────────────────────────────────────────────────
# IN_PATH      = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\scrape_queue\groups.json"
# OUT_PATH     = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\scrape_queue\enriched_playlists.json"
# CACHE_PATH   = os.path.join(os.path.dirname(__file__), "cache", "places_cache.json")
# MODEL        = os.getenv("LC_MODEL", "gpt-4o-mini")
# GOOGLE_KEY   = os.getenv("GOOGLE_MAPS_API_KEY")

# # ─── PROMPTS ───────────────────────────────────────────────────────────────────
# EXTRACT_ALL_PROMPT = PromptTemplate.from_template("""
# Extract ALL distinct place names from the provided HTML of a travel article.
# Return ONLY a JSON array of strings. Do not include markdown, code blocks, or any extra text.
# Example: ["Jaipur", "The Oberoi Amarvilas", "Karma Lakelands", "Shimla", "Manali"]

# HTML:
# ```html
# {html}
# ```""")


# # ─── HELPERS ────────────────────────────────────────────────────────────────────
# def clean_json_from_llm(text: str) -> str:
#     """Extracts a JSON array string from a raw LLM response."""
#     match = re.search(r'\[.*\]', text, re.DOTALL)
#     if match:
#         return match.group(0)
#     return ""

# async def fetch_page_content(session: aiohttp.ClientSession, url: str) -> str:
#     """Fetches HTML content of a URL using aiohttp."""
#     try:
#         async with session.get(url, timeout=30) as response:
#             return await response.text()
#     except Exception as e:
#         print(f"  -> aiohttp fetch failed for {url}: {e}")
#         return ""

# async def extract_all_places(llm, html: str):
#     """Invokes the LLM to extract all place names from HTML."""
#     if not html:
#         return []
#     snippet = html[:20000] # Use a larger snippet for extracting all places
#     prompt_value = EXTRACT_ALL_PROMPT.format(html=snippet)
#     res = await llm.ainvoke(prompt_value)
    
#     json_string = clean_json_from_llm(res.content)
#     try:
#         return json.loads(json_string)
#     except json.JSONDecodeError:
#         print(f"  -> Warning: Could not parse cleaned JSON for all places. Raw: {res.content[:100]}...")
#         return []

# def resolve_place_google(gmaps_client, cache: dict, place_name: str):
#     """Resolves a place name to a Google Place ID with caching."""
#     key = place_name.lower()
#     if key in cache:
#         return cache[key]

#     print(f"  -> Calling Google Places API for: {place_name}")
#     try:
#         # Use find_place to get the best candidate
#         res = gmaps_client.find_place(
#             input=f"{place_name} near Delhi NCR",
#             input_type="textquery",
#             fields=["place_id", "name", "geometry", "rating", "types"]
#         )
#         candidates = res.get("candidates", [])
#         if not candidates:
#             cache[key] = None
#             return None
        
#         # Store the result in the cache and return it
#         cache[key] = candidates[0]
#         return candidates[0]
#     except Exception as e:
#         print(f"  -> Google API error for '{place_name}': {e}")
#         cache[key] = None
#         return None

# # ─── MAIN ORCHESTRATION ───────────────────────────────────────────────────────
# async def main():
#     # Initialize clients and load data
#     llm = ChatOpenAI(model=MODEL, temperature=0)
#     gmaps = googlemaps.Client(key=GOOGLE_KEY)
    
#     try:
#         with open(IN_PATH, 'r', encoding='utf-8') as f:
#             playlists = json.load(f)
#     except FileNotFoundError:
#         print(f"ERROR: Input file not found at {IN_PATH}. Please run Step 1 first.")
#         return

#     # Load Google Places cache
#     os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
#     try:
#         with open(CACHE_PATH, 'r', encoding='utf-8') as f:
#             places_cache = json.load(f)
#     except (FileNotFoundError, json.JSONDecodeError):
#         places_cache = {}

#     enriched_playlists = []
#     async with aiohttp.ClientSession() as session:
#         for playlist in tqdm(playlists, desc="Enriching Playlists"):
#             print(f"\nProcessing playlist: '{playlist['playlistTitle']}'")
            
#             # Step 1: Scrape all places from all articles in the playlist
#             fetch_tasks = [fetch_page_content(session, article['url']) for article in playlist['ClubbedArticles']]
#             html_contents = await asyncio.gather(*fetch_tasks)
            
#             extract_tasks = [extract_all_places(llm, html) for html in html_contents]
#             all_places_lists = await asyncio.gather(*extract_tasks)
            
#             # Step 2: Merge and count all extracted places
#             master_place_list = [place for sublist in all_places_lists for place in sublist]
#             place_votes = Counter(master_place_list)
#             unique_places = list(place_votes.keys())
            
#             # Step 3: Resolve unique places with Google Maps API
#             resolved_places = {}
#             for place_name in unique_places:
#                 resolved_data = resolve_place_google(gmaps, places_cache, place_name)
#                 if resolved_data:
#                     resolved_places[place_name] = resolved_data
            
#             # Step 4: Build the final list of enriched places
#             enriched_places = []
#             for name, data in resolved_places.items():
#                 location = data.get('geometry', {}).get('location', {})
#                 enriched_places.append({
#                     "name": data.get('name', name),
#                     "place_id": data.get('place_id'),
#                     "lat": location.get('lat'),
#                     "lng": location.get('lng'),
#                     "rating": data.get('rating', 0),
#                     "types": data.get('types', []),
#                     "votes": place_votes.get(name, 0)
#                 })

#             # Step 5: Rank and Trim the list
#             enriched_places.sort(key=lambda x: (-x["votes"], -x.get("rating", 0)), reverse=False)
#             final_places = enriched_places[:20] # Keep the top 20
            
#             enriched_playlists.append({
#                 "playlistTitle": playlist["playlistTitle"],
#                 "placeName": playlist["placeName"],
#                 "subtype": playlist["subtype"],
#                 "places": final_places
#             })

#     # Save the final enriched data
#     with open(OUT_PATH, "w", encoding="utf-8") as f:
#         json.dump(enriched_playlists, f, indent=2, ensure_ascii=False)

#     # Save the updated cache
#     with open(CACHE_PATH, "w", encoding="utf-8") as f:
#         json.dump(places_cache, f, indent=2, ensure_ascii=False)

#     print(f"\n✅ Wrote {len(enriched_playlists)} enriched playlists to {OUT_PATH}")

# if __name__ == "__main__":
#     asyncio.run(main())