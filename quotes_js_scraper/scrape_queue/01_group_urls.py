import os, re, json, asyncio
from collections import defaultdict
from dotenv import load_dotenv
from tqdm import tqdm

# LangChain-community imports
from langchain_community.document_loaders.url import UnstructuredURLLoader
from langchain_community.document_loaders.url_selenium import SeleniumURLLoader
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate

load_dotenv()

# ─── CONFIG ────────────────────────────────────────────────────────────────────
IN_PATH    = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\scrape_queue\urls.json"
OUT_PATH   = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\scrape_queue\groups.json"
MODEL      = os.getenv("LC_MODEL", "gpt-4o-mini")
MAX_PER_PL = 3
SEL_OPTS   = ["--headless","--no-sandbox","--disable-dev-shm-usage"]
PLACE_NAME = "Pondicherry"

# ─── PROMPTS ───────────────────────────────────────────────────────────────────
EXTRACT_PROMPT = PromptTemplate.from_template("""\
Extract exactly 5 place names from this travel collection page.
Return ONLY a JSON array of 5 strings (no markdown, no code blocks, no extra text):
["Tijara Fort Palace","Heritage Village Resort","The Hill Fort","Downtown Resort","Fort Unchagaon"]

HTML:
```html
{html}
```""")

CLASSIFY_PROMPT = PromptTemplate.from_template("""\
You are classifying a travel list page into one subtype. Return ONLY one word: "poi" or "destination".
Definition:
- poi = specific stays/forts/resorts/retreats/spas/farms/homestays/parks/attractions (not cities)
- destination = cities/towns/hill-stations/districts/states/regions

Use BOTH inputs: (A) five extracted names from the page, (B) the page title.

(A) places = {places}
(B) title  = {title}
""")

TITLE_PROMPT = PromptTemplate.from_template("""\
Make a concise (6–12 words) playlist title for {place} from these article titles.
Subtype: {subtype}  (poi = unique stays/attractions, destination = towns/cities)
Return ONLY the title text, no quotes/no markdown.

Titles:
- {t1}
- {t2}
- {t3}
""")

# ─── LEXICONS / REGEX ──────────────────────────────────────────────────────────
POI_STRONG = re.compile(
    r"\b(resort|retreat|palace|fort|villa|hotel|spa|homestay|camp|lodge|haveli|treehouse|glamp(?:ing)?|bungalow|boutique|heritage|stay)\b",
    re.I,
)
POI_WEAK = re.compile(
    r"\b(museum|beach|falls?|waterfall|temple|monument|garden|bagh|gate|dam|valley)\b",
    re.I,
)
DEST_HINTS = re.compile(
    r"\b(city|town|village|district|state|pradesh|himachal|uttarakhand|rajasthan|punjab|haryana|delhi|ncr|kashmir|goa|gujarat|maharashtra|bihar|uttar\s?pradesh|madhya\s?pradesh|west\s?bengal)\b",
    re.I,
)
TITLE_POI_HINTS = re.compile(r"\b(hotels?|resorts?|stays?|forts?|palaces?|villas?|homestays?)\b", re.I)
TITLE_DEST_HINTS = re.compile(r"\b(cities|towns|destinations|places to visit|hill[-\s]?stations?)\b", re.I)

STOPWORDS = {
    "the","a","an","and","or","of","for","to","from","near","under","over","within","less","than",
    "in","on","by","with","best","top","places","place","visit","trips","trip","day","weekend",
    "getaways","getaway","hours","away","monsoon","winter","summer","road","you","can","take",
    "under","₹5000","10","15","50","short","&","–","-",":","’","'","(",")","[","]"
}

# ─── UTILS ─────────────────────────────────────────────────────────────────────
def clean_json_response(response_text: str) -> str:
    cleaned = re.sub(r'```json\s*|\s*```', '', response_text)
    cleaned = re.sub(r'```\s*', '', cleaned)
    return cleaned.strip()

def tokenize_title(s: str) -> set:
    s = s.lower()
    s = re.sub(r"[^a-zA-Z0-9\s]", " ", s)
    toks = [t for t in s.split() if t and t not in STOPWORDS and not t.isdigit()]
    return set(toks)

def jaccard(a: set, b: set) -> float:
    if not a or not b: return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0

# ─── LOADERS ───────────────────────────────────────────────────────────────────
async def load_page(url: str) -> str:
    try:
        docs = await UnstructuredURLLoader(urls=[url]).aload()
        content = docs[0].page_content
        if len(content) > 500:
            return content
    except Exception:
        pass
    print(f"  -> Falling back to Selenium for: {url}")
    docs = await SeleniumURLLoader(
        urls=[url],
        browser="chrome",
        headless=True,
        args=SEL_OPTS
    ).aload()
    return docs[0].page_content

# ─── LLM FACTORY (handles temperature restrictions) ────────────────────────────
def make_llm():
    # Do NOT set temperature; some models only allow default(=1)
    return ChatOpenAI(model=MODEL)

# ─── LLM WRAPPERS ──────────────────────────────────────────────────────────────
async def extract_places(llm, html: str) -> list[str]:
    snippet = html[:15000]
    res = await llm.ainvoke(EXTRACT_PROMPT.format(html=snippet))
    content = clean_json_response(res.content)
    try:
        arr = json.loads(content)
        if isinstance(arr, list):
            return [x.strip() for x in arr if isinstance(x, str) and x.strip()]
    except json.JSONDecodeError:
        print(f"  -> Warning: Could not parse JSON for places; got:\n{res.content[:220]}...")
    return []

def label_place(name: str) -> tuple[str, str]:
    s = name.lower()
    left = s.split(",", 1)[0].strip()
    if POI_STRONG.search(left) or POI_STRONG.search(s):
        return ("poi", "strong")
    if POI_WEAK.search(left) or POI_WEAK.search(s):
        return ("poi", "weak")
    if DEST_HINTS.search(s) or (len(left.split()) <= 2 and not (POI_STRONG.search(left) or POI_WEAK.search(left))):
        return ("destination", "weak")
    return ("unknown", "weak")

def aggregate_labels(places: list[str], title: str) -> str:
    poi_strong = poi_weak = dest = 0
    for p in places:
        lbl, strength = label_place(p)
        if lbl == "poi":
            if strength == "strong": poi_strong += 1
            else: poi_weak += 1
        elif lbl == "destination":
            dest += 1

    total_poi = poi_strong + poi_weak

    if TITLE_POI_HINTS.search(title):  poi_strong += 1
    if TITLE_DEST_HINTS.search(title): dest += 1

    total_poi = poi_strong + poi_weak

    if poi_strong >= 1 and total_poi >= dest:
        return "poi"
    if total_poi > dest:
        return "poi"
    if dest > total_poi:
        return "destination"
    if poi_strong >= 1:
        return "poi"
    return "unknown"

async def classify_subtype(llm, places: list[str], title: str) -> str:
    heuristic = aggregate_labels(places, title)
    if heuristic != "unknown":
        return heuristic
    try:
        msg = await llm.ainvoke(CLASSIFY_PROMPT.format(places=places, title=title))
        ans = msg.content.strip().lower()
        return "poi" if ans == "poi" else ("destination" if ans == "destination" else "unknown")
    except Exception:
        return "unknown"

async def make_playlist_title(llm, subtype: str, place: str, titles: list[str]) -> str:
    t1, t2, t3 = (titles + ["", "", ""])[:3]
    if not t1:
        return "Untitled"
    try:
        msg = await llm.ainvoke(TITLE_PROMPT.format(
            place=place, subtype=subtype, t1=t1, t2=t2 or t1, t3=t3 or t1
        ))
        out = re.sub(r"[\r\n]+", " ", msg.content.strip())
        if 6 <= len(out.split()) <= 14:
            return out
    except Exception:
        pass
    return ("Unique Stays & Weekend Getaways" if subtype == "poi" else "Road Trips & Destinations") + f" around {place}"

# ─── CLUSTERING ────────────────────────────────────────────────────────────────
def cluster_by_title_similarity(items: list[dict], max_per: int = 3, min_sim: float = 0.18) -> list[list[dict]]:
    tokens = [tokenize_title(it["title"]) for it in items]
    used = [False] * len(items)
    clusters = []

    for i, it in enumerate(items):
        if used[i]:
            continue
        used[i] = True
        cluster = [it]
        sims = [(j, jaccard(tokens[i], tokens[j])) for j in range(len(items)) if not used[j]]
        sims.sort(key=lambda x: x[1], reverse=True)
        for j, sim in sims:
            if len(cluster) >= max_per:
                break
            if sim >= min_sim or not any(s >= min_sim for _, s in sims):
                cluster.append(items[j]); used[j] = True
        clusters.append(cluster)
    return clusters

# ─── MAIN ───────────────────────────────────────────────────────────────────────
async def main():
    # Create an LLM client WITHOUT temperature (models may force default=1)
    llm = make_llm()

    try:
        with open(IN_PATH, "r", encoding="utf-8") as f:
            rows = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Input file not found at {IN_PATH}")
        return

    enriched = []  # [{title,url, places:[...], subtype:str}]
    for row in tqdm(rows, desc="Classifying URLs"):
        title = row.get("title","").strip()
        url   = row["url"]
        try:
            html = await load_page(url)
            if not html:
                print(f"  -> Warning: No content for {url}; skipping.")
                continue

            # Make the OpenAI calls
            try:
                places = await extract_places(llm, html)
            except Exception as e:
                # If someone reintroduced temperature elsewhere and we get a 400, rebuild once.
                emsg = str(e)
                if "temperature" in emsg and "Unsupported value" in emsg:
                    print("  -> Rebuilding LLM client without temperature and retrying once...")
                    llm = make_llm()
                    places = await extract_places(llm, html)
                else:
                    raise

            if not places:
                print(f"  -> Warning: No places extracted for {url}; skipping.")
                continue

            subtype = await classify_subtype(llm, places, title)
            print(f"  -> {subtype.upper():12s} | {title}")
            if subtype in ("poi","destination"):
                enriched.append({"title": title, "url": url, "places": places, "subtype": subtype})
        except Exception as e:
            print(f"  -> Error processing {url}: {e}")

    output = []
    for subtype in ("poi","destination"):
        bucket = [it for it in enriched if it["subtype"] == subtype]
        if not bucket:
            continue
        clusters = cluster_by_title_similarity(bucket, MAX_PER_PL, min_sim=0.18)
        for cluster in clusters:
            titles = [it["title"] for it in cluster]
            pl_title = await make_playlist_title(llm, subtype, PLACE_NAME, titles)
            output.append({
                "playlistTitle":   pl_title,
                "placeName":       PLACE_NAME,
                "subtype":         subtype,
                "ClubbedArticles": [{"title": it["title"], "url": it["url"]} for it in cluster]
            })

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Wrote {len(output)} playlists to {OUT_PATH}")

if __name__ == "__main__":
    asyncio.run(main())



# #!/usr/bin/env python3
# import os, re, json, asyncio
# from collections import defaultdict
# from dotenv import load_dotenv
# from tqdm import tqdm

# # LangChain-community imports
# from langchain_community.document_loaders.url import UnstructuredURLLoader
# from langchain_community.document_loaders.url_selenium import SeleniumURLLoader
# from langchain_openai import ChatOpenAI
# from langchain_core.prompts import PromptTemplate

# load_dotenv()

# # ─── CONFIG ────────────────────────────────────────────────────────────────────
# IN_PATH    = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\scrape_queue\urls.json"
# OUT_PATH   = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\scrape_queue\groups.json"
# MODEL      = os.getenv("LC_MODEL", "gpt-4o-mini")
# MAX_PER_PL = 3
# SEL_OPTS   = ["--headless","--no-sandbox","--disable-dev-shm-usage"]
# PLACE_NAME = "Phuket"

# # ─── PROMPTS ───────────────────────────────────────────────────────────────────
# EXTRACT_PROMPT = PromptTemplate.from_template("""\
# Extract exactly 5 place names from this travel collection page.
# Return ONLY a JSON array of 5 strings (no markdown, no code blocks, no extra text):
# ["Tijara Fort Palace","Heritage Village Resort","The Hill Fort","Downtown Resort","Fort Unchagaon"]

# HTML:
# ```html
# {html}
# ```""")

# CLASSIFY_PROMPT = PromptTemplate.from_template("""\
# You are classifying a travel list page into one subtype. Return ONLY one word: "poi" or "destination".
# Definition:
# - poi = specific stays/forts/resorts/retreats/spas/farms/homestays/parks/attractions (not cities)
# - destination = cities/towns/hill-stations/districts/states/regions

# Use BOTH inputs: (A) five extracted names from the page, (B) the page title.

# (A) places = {places}
# (B) title  = {title}
# """)

# TITLE_PROMPT = PromptTemplate.from_template("""\
# Make a concise (6–12 words) playlist title for {place} from these article titles.
# Subtype: {subtype}  (poi = unique stays/attractions, destination = towns/cities)
# Return ONLY the title text, no quotes/no markdown.

# Titles:
# - {t1}
# - {t2}
# - {t3}
# """)

# # ─── LEXICONS / REGEX ──────────────────────────────────────────────────────────
# POI_STRONG = re.compile(
#     r"\b(resort|retreat|palace|fort|villa|hotel|spa|homestay|camp|lodge|haveli|treehouse|glamp(?:ing)?|bungalow|boutique|heritage|stay)\b",
#     re.I,
# )
# POI_WEAK = re.compile(
#     r"\b(museum|beach|falls?|waterfall|temple|monument|garden|bagh|gate|dam|valley)\b",
#     re.I,
# )
# DEST_HINTS = re.compile(
#     r"\b(city|town|village|district|state|pradesh|himachal|uttarakhand|rajasthan|punjab|haryana|delhi|ncr|kashmir|goa|gujarat|maharashtra|bihar|uttar\s?pradesh|madhya\s?pradesh|west\s?bengal)\b",
#     re.I,
# )
# TITLE_POI_HINTS = re.compile(r"\b(hotels?|resorts?|stays?|forts?|palaces?|villas?|homestays?)\b", re.I)
# TITLE_DEST_HINTS = re.compile(r"\b(cities|towns|destinations|places to visit|hill[-\s]?stations?)\b", re.I)

# STOPWORDS = {
#     "the","a","an","and","or","of","for","to","from","near","under","over","within","less","than",
#     "in","on","by","with","best","top","places","place","visit","trips","trip","day","weekend",
#     "getaways","getaway","hours","away","monsoon","winter","summer","road","you","can","take",
#     "under","₹5000","10","15","50","short","&","–","-",":","’","'","(",")","[","]"
# }

# # ─── UTILS ─────────────────────────────────────────────────────────────────────
# def clean_json_response(response_text: str) -> str:
#     cleaned = re.sub(r'```json\s*|\s*```', '', response_text)
#     cleaned = re.sub(r'```\s*', '', cleaned)
#     return cleaned.strip()

# def tokenize_title(s: str) -> set:
#     s = s.lower()
#     s = re.sub(r"[^a-zA-Z0-9\s]", " ", s)
#     toks = [t for t in s.split() if t and t not in STOPWORDS and not t.isdigit()]
#     return set(toks)

# def jaccard(a: set, b: set) -> float:
#     if not a or not b: return 0.0
#     inter = len(a & b)
#     union = len(a | b)
#     return inter / union if union else 0.0

# # ─── LOADERS ───────────────────────────────────────────────────────────────────
# async def load_page(url: str) -> str:
#     try:
#         docs = await UnstructuredURLLoader(urls=[url]).aload()
#         content = docs[0].page_content
#         if len(content) > 500:
#             return content
#     except Exception:
#         pass
#     print(f"  -> Falling back to Selenium for: {url}")
#     docs = await SeleniumURLLoader(
#         urls=[url],
#         browser="chrome",
#         headless=True,
#         args=SEL_OPTS
#     ).aload()
#     return docs[0].page_content

# # ─── LLM WRAPPERS ──────────────────────────────────────────────────────────────
# async def extract_places(llm, html: str) -> list[str]:
#     snippet = html[:15000]
#     res = await llm.ainvoke(EXTRACT_PROMPT.format(html=snippet))
#     content = clean_json_response(res.content)
#     try:
#         arr = json.loads(content)
#         if isinstance(arr, list):
#             return [x.strip() for x in arr if isinstance(x, str) and x.strip()]
#     except json.JSONDecodeError:
#         print(f"  -> Warning: Could not parse JSON for places; got:\n{res.content[:220]}...")
#     return []

# def label_place(name: str) -> tuple[str, str]:
#     """
#     Returns (label, strength):
#       label ∈ {'poi','destination','unknown'}
#       strength ∈ {'strong','weak'}
#     """
#     s = name.lower()
#     left = s.split(",", 1)[0].strip()  # left of comma is often the property/brand
#     if POI_STRONG.search(left) or POI_STRONG.search(s):
#         return ("poi", "strong")
#     if POI_WEAK.search(left) or POI_WEAK.search(s):
#         return ("poi", "weak")
#     # destination-looking: short proper name or region/state hints
#     if DEST_HINTS.search(s) or (len(left.split()) <= 2 and not (POI_STRONG.search(left) or POI_WEAK.search(left))):
#         return ("destination", "weak")
#     return ("unknown", "weak")

# def aggregate_labels(places: list[str], title: str) -> str:
#     poi_strong = poi_weak = dest = 0
#     for p in places:
#         lbl, strength = label_place(p)
#         if lbl == "poi":
#             if strength == "strong": poi_strong += 1
#             else: poi_weak += 1
#         elif lbl == "destination":
#             dest += 1

#     total_poi = poi_strong + poi_weak

#     # Title nudges
#     if TITLE_POI_HINTS.search(title):  poi_strong += 1
#     if TITLE_DEST_HINTS.search(title): dest += 1

#     total_poi = poi_strong + poi_weak

#     # 1) POI-strong override if collection is at least as POI as destination
#     if poi_strong >= 1 and total_poi >= dest:
#         return "poi"

#     # 2) Majority vote
#     if total_poi > dest:
#         return "poi"
#     if dest > total_poi:
#         return "destination"

#     # 3) Tie-break
#     if poi_strong >= 1:
#         return "poi"
#     return "unknown"

# async def classify_subtype(llm, places: list[str], title: str) -> str:
#     heuristic = aggregate_labels(places, title)
#     if heuristic != "unknown":
#         return heuristic
#     # fallback to LLM only if needed
#     try:
#         msg = await llm.ainvoke(CLASSIFY_PROMPT.format(places=places, title=title))
#         ans = msg.content.strip().lower()
#         return "poi" if ans == "poi" else ("destination" if ans == "destination" else "unknown")
#     except Exception:
#         return "unknown"

# async def make_playlist_title(llm, subtype: str, place: str, titles: list[str]) -> str:
#     t1, t2, t3 = (titles + ["", "", ""])[:3]
#     if not t1:
#         return "Untitled"
#     try:
#         msg = await llm.ainvoke(TITLE_PROMPT.format(
#             place=place, subtype=subtype, t1=t1, t2=t2 or t1, t3=t3 or t1
#         ))
#         out = re.sub(r"[\r\n]+", " ", msg.content.strip())
#         if 6 <= len(out.split()) <= 14:
#             return out
#     except Exception:
#         pass
#     return ("Unique Stays & Weekend Getaways" if subtype == "poi" else "Road Trips & Destinations") + f" around {place}"

# # ─── CLUSTERING ────────────────────────────────────────────────────────────────
# def cluster_by_title_similarity(items: list[dict], max_per: int = 3, min_sim: float = 0.18) -> list[list[dict]]:
#     tokens = [tokenize_title(it["title"]) for it in items]
#     used = [False] * len(items)
#     clusters = []

#     for i, it in enumerate(items):
#         if used[i]:
#             continue
#         used[i] = True
#         cluster = [it]
#         sims = [(j, jaccard(tokens[i], tokens[j])) for j in range(len(items)) if not used[j]]
#         sims.sort(key=lambda x: x[1], reverse=True)
#         for j, sim in sims:
#             if len(cluster) >= max_per:
#                 break
#             if sim >= min_sim or not any(s >= min_sim for _, s in sims):
#                 cluster.append(items[j]); used[j] = True
#         clusters.append(cluster)
#     return clusters

# # ─── MAIN ───────────────────────────────────────────────────────────────────────
# async def main():
#     llm = ChatOpenAI(model=MODEL, temperature=0)

#     try:
#         with open(IN_PATH, "r", encoding="utf-8") as f:
#             rows = json.load(f)
#     except FileNotFoundError:
#         print(f"ERROR: Input file not found at {IN_PATH}")
#         return

#     enriched = []  # [{title,url, places:[...], subtype:str}]
#     for row in tqdm(rows, desc="Classifying URLs"):
#         title = row.get("title","").strip()
#         url   = row["url"]
#         try:
#             html = await load_page(url)
#             if not html:
#                 print(f"  -> Warning: No content for {url}; skipping.")
#                 continue
#             places = await extract_places(llm, html)
#             if not places:
#                 print(f"  -> Warning: No places extracted for {url}; skipping.")
#                 continue
#             subtype = await classify_subtype(llm, places, title)
#             print(f"  -> {subtype.upper():12s} | {title}")
#             if subtype in ("poi","destination"):
#                 enriched.append({"title": title, "url": url, "places": places, "subtype": subtype})
#         except Exception as e:
#             print(f"  -> Error processing {url}: {e}")

#     output = []
#     for subtype in ("poi","destination"):
#         bucket = [it for it in enriched if it["subtype"] == subtype]
#         if not bucket:
#             continue
#         clusters = cluster_by_title_similarity(bucket, MAX_PER_PL, min_sim=0.18)
#         for cluster in clusters:
#             titles = [it["title"] for it in cluster]
#             pl_title = await make_playlist_title(llm, subtype, PLACE_NAME, titles)
#             output.append({
#                 "playlistTitle":   pl_title,
#                 "placeName":       PLACE_NAME,
#                 "subtype":         subtype,
#                 "ClubbedArticles": [{"title": it["title"], "url": it["url"]} for it in cluster]
#             })

#     with open(OUT_PATH, "w", encoding="utf-8") as f:
#         json.dump(output, f, indent=2, ensure_ascii=False)
#     print(f"\n✅ Wrote {len(output)} playlists to {OUT_PATH}")

# if __name__ == "__main__":
#     asyncio.run(main())





















