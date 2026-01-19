# generate_playlist_images_all.py
# -------------------------------------------------------------
# - Processes ALL (or selected) Firestore docs in playlistsNew
# - New: --ids-json <path.json> with {"document_ids": ["1","10","100",...]}
# - Grounded analysis (city+country) -> OpenAI image -> base/cover/hero
# - Uploads to gs://mycasavsc.appspot.com/playlistsNew_images/<id>/
# - Keeps existing 'imageUrl'. Adds:
#     image_base_url, image_cover_url, image_hero_url, imageUrls{}, images_last_updated_ts
# - Style presets: classic_warm | bright_white | cool_modern | pastel_soft | moody_film
#   * --style <preset> to force, or --style auto (default) to rotate deterministically by playlist_id
#   * Per-doc override via Firestore field: image_style: "<preset>"
# -------------------------------------------------------------
# Requirements: pip install openai firebase-admin google-cloud-storage pillow
# OPENAI KEY: setx OPENAI_API_KEY "sk-..." (don’t hard-code)
# -------------------------------------------------------------

import os, re, argparse, hashlib, unicodedata, json
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Tuple, NamedTuple, Optional

from PIL import Image, ImageDraw, ImageFont
from hashlib import sha1

# --------------- CONFIG ---------------
PROJECT_ID           = "mycasavsc"
SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
COLLECTION           = "playlistsNew"
GCS_BUCKET_NAME      = "mycasavsc.appspot.com"  # same bucket pattern as existing URLs

OPENAI_API_KEY       = "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"
OPENAI_TEXT_MODEL    = "gpt-4.1"
OPENAI_IMAGE_MODEL   = "gpt-image-1-mini"
OPENAI_IMAGE_SIZE    = "1024x1024"

OUTDIR               = Path("trial_out")    # optional local QA
COVER_SIZE           = 1200
HERO_W, HERO_H       = 1920, 1080
TEXT_PADDING         = 48
# --------------------------------------


# ----------------- FONT HELPERS -----------------
FONT_CANDIDATES = [
    r"C:\Windows\Fonts\seguisb.ttf",
    r"C:\Windows\Fonts\segoeui.ttf",
    r"C:\Windows\Fonts\arial.ttf",
    r"C:\Windows\Fonts\calibri.ttf",
    r"C:\Windows\Fonts\tahoma.ttf",
    "/Library/Fonts/Arial.ttf",
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
]

# Optional short names you can use in Firestore (image_font) or CLI (--font seguisb)
FONT_ALIASES = {
    "seguisb": r"C:\Windows\Fonts\seguisb.ttf",          # Segoe UI Semibold
    "segoeui": r"C:\Windows\Fonts\segoeui.ttf",
    "seguib":  r"C:\Windows\Fonts\segoeuib.ttf",         # Segoe UI Bold (if present)
    "arial":   r"C:\Windows\Fonts\arial.ttf",
    "arialbd": r"C:\Windows\Fonts\arialbd.ttf",          # Arial Bold
    "calibri": r"C:\Windows\Fonts\calibri.ttf",
    "calibrib":r"C:\Windows\Fonts\calibrib.ttf",         # Calibri Bold
    "tahoma":  r"C:\Windows\Fonts\tahoma.ttf",
    "tahomabd":r"C:\Windows\Fonts\tahomabd.ttf",         # Tahoma Bold
    "dejavu":  "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "dejavub": "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
}

def _bold_variant_of(path: str) -> Optional[str]:
    if not path: 
        return None
    # Try common bold filename patterns
    candidates = []
    base, ext = os.path.splitext(path)
    candidates += [base + "bd" + ext, base + "bd" + ext.upper()]          # arial -> arialbd.ttf
    candidates += [base + "b" + ext]                                      # tahoma -> tahomab.ttf (less common)
    candidates += [base + "-Bold" + ext, base.replace("Regular","Bold")+ext]
    # Known families
    fam = os.path.basename(path).lower()
    mapping = {
        "segoeui.ttf": "segoeuib.ttf",
        "dejavusans.ttf": "DejaVuSans-Bold.ttf",
    }
    if fam in mapping:
        alt = os.path.join(os.path.dirname(path), mapping[fam])
        candidates.insert(0, alt)

    for c in candidates:
        if os.path.exists(c):
            return c
    return None

def choose_font_path(override: str, firestore_font: str, playlist_id: str, 
                     max_variants: int = 5, weight: str = "normal") -> str:
    # 1) CLI override (alias or path)
    p = _alias_to_path(override)
    if p and weight == "bold":
        return _bold_variant_of(p) or p
    if p:
        return p

    # 2) Firestore override
    p = _alias_to_path(firestore_font)
    if p and weight == "bold":
        return _bold_variant_of(p) or p
    if p:
        return p

    # 3) Deterministic rotation
    avail = _existing_fonts()
    if not avail:
        return ""
    n = max(1, min(max_variants, len(avail)))
    idx = int(sha1(str(playlist_id).encode("utf-8")).hexdigest(), 16) % n
    picked = avail[idx]
    if weight == "bold":
        return _bold_variant_of(picked) or picked
    return picked

def _existing_fonts() -> List[str]:
    """Return FONT_CANDIDATES that exist on this machine."""
    return [p for p in FONT_CANDIDATES if os.path.exists(p)]

def _alias_to_path(alias_or_path: str) -> Optional[str]:
    """If it's an alias, map to a path; if it's a path and exists, return it; else None."""
    if not alias_or_path:
        return None
    s = alias_or_path.strip()
    if s.lower() in FONT_ALIASES:
        p = FONT_ALIASES[s.lower()]
        return p if os.path.exists(p) else None
    return s if os.path.exists(s) else None

# def choose_font_path(override: str, firestore_font: str, playlist_id: str, max_variants: int = 5) -> str:
#     """
#     Priority:
#       1) explicit CLI --font (path or alias)
#       2) Firestore field 'image_font' (alias or path)
#       3) Deterministic rotation across first N existing fonts from FONT_CANDIDATES
#     Returns a path or '' (empty) to let _pick_font fall back.
#     """
#     # 1) CLI override
#     p = _alias_to_path(override)
#     if p:
#         return p

#     # 2) Firestore override
#     p = _alias_to_path(firestore_font)
#     if p:
#         return p

#     # 3) Deterministic rotation
#     avail = _existing_fonts()
#     if not avail:
#         return ""  # _pick_font() will still try FONT_CANDIDATES internally
#     n = max(1, min(max_variants, len(avail)))
#     idx = int(sha1(str(playlist_id).encode("utf-8")).hexdigest(), 16) % n
#     return avail[idx]

def _pick_font(size: int, preferred: str = "", fallback_cfg: str = "") -> ImageFont.FreeTypeFont:
    for p in [preferred, fallback_cfg, *FONT_CANDIDATES]:
        if p and os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                pass
    raise RuntimeError("No scalable TTF font found. Pass --font C:\\Windows\\Fonts\\segoeui.ttf (or seguisb.ttf).")

def draw_text_bottom_left_auto(
    img: Image.Image, text: str, padding: int = 48,
    preferred_font: str = "", max_lines: int = 2,
    width_frac: float = 0.86, max_height_frac: float = 0.32,
    faux_bold: bool = False
    
    
) -> Image.Image:
    if img.mode != "RGBA":
        img = img.convert("RGBA")
    W, H = img.size
    draw = ImageDraw.Draw(img, "RGBA")
    target_w = W * width_frac
    target_h = H * max_height_frac
    size = int(W * 0.14)
    min_size = max(36, int(W * 0.05))
    chosen_font, text_block = None, None

    def wrap_lines(s: str, font: ImageFont.ImageFont) -> Optional[str]:
        words, lines, line = s.split(), [], ""
        for w in words:
            test = (line + " " + w).strip()
            if draw.textlength(test, font=font) <= target_w:
                line = test
            else:
                if line: lines.append(line)
                line = w
                if len(lines) >= max_lines: return None
        if line: lines.append(line)
        return "\n".join(lines)

    while size >= min_size:
        font = _pick_font(size, preferred_font)
        wrapped = wrap_lines(text, font)
        if wrapped is None:
            size -= 4
            continue
        x0, y0, x1, y1 = draw.multiline_textbbox((0,0), wrapped, font=font, spacing=int(size*0.2))
        if (x1-x0) <= target_w and (y1-y0) <= target_h:
            chosen_font, text_block = font, wrapped
            break
        size -= 4

    if chosen_font is None:
        chosen_font = _pick_font(min_size, preferred_font)
        text_block = wrap_lines(text, chosen_font) or text

    _,_,_, block_h = draw.multiline_textbbox((0,0), text_block, font=chosen_font, spacing=int(chosen_font.size*0.2))
    x = padding
    y = H - padding - block_h

    # soft gradient behind text
    panel_h = block_h + max(16, padding // 2)
    overlay = Image.new("RGBA", (W, panel_h), (0,0,0,0))
    ov = ImageDraw.Draw(overlay)
    for i in range(panel_h):
        alpha = int(200 * (i / panel_h))
        ov.line([(0,i),(W,i)], fill=(0,0,0,alpha))
    img.alpha_composite(overlay, (0, H - panel_h))

    draw = ImageDraw.Draw(img, "RGBA")
# subtle shadow
    draw.multiline_text((x+2, y+2), text_block, font=chosen_font,
                    fill=(0,0,0,200), spacing=int(chosen_font.size*0.2))

    if faux_bold:
    # Faux-bold pass: draw a few times around the origin to thicken
        for dx, dy in ((1,0), (-1,0), (0,1), (0,-1)):
            draw.multiline_text((x+dx, y+dy), text_block, font=chosen_font,
                            fill=(255,255,255,255), spacing=int(chosen_font.size*0.2))
    # Final pass
    draw.multiline_text((x, y), text_block, font=chosen_font,
                    fill=(255,255,255,255), spacing=int(chosen_font.size*0.2))

    return img.convert("RGB")


# ----------------- STYLE SYSTEM -----------------
STYLE_PRESETS = {
    "classic_warm": {
        "human": "Classic Warm Golden Hour",
        "suffix": (
            "Warm golden-hour tones, cinematic composition, soft long shadows, earthy palette, slight glow."
        ),
    },
    "bright_white": {
        "human": "Bright & White Sunlit Minimal",
        "suffix": (
            "Bright and sunny image showcasing the beauty of {LOC}, mostly in daylight, "
            "high-key lighting, clean minimal aesthetic, crisp whites, soft natural shadows."
        ),
    },
    "cool_modern": {
        "human": "Cool Modern Blue",
        "suffix": (
            "Cool modern palette with crisp blues, blue-hour influence, neutral whites, "
            "clean lines, contemporary mood, polarizing filter look."
        ),
    },
    "pastel_soft": {
        "human": "Pastel Sunrise/Sunset Soft",
        "suffix": (
            "Pastel sunrise/sunset hues, airy atmosphere, gentle diffusion, soft color wash, subtle vignette."
        ),
    },
    "moody_film": {
        "human": "Moody Filmic",
        "suffix": (
            "Moody desaturated palette, soft contrast, subtle film grain, overcast ambiance, timeless editorial feel."
        ),
    },
}
STYLE_KEYS = list(STYLE_PRESETS.keys())

def choose_style(preset_override: Optional[str], firestore_style: Optional[str], playlist_id: str) -> str:
    cand = (preset_override or "").strip().lower() if preset_override else ""
    if cand in STYLE_PRESETS: return cand
    fs = (firestore_style or "").strip().lower() if firestore_style else ""
    if fs in STYLE_PRESETS: return fs
    idx = int(sha1(str(playlist_id).encode("utf-8")).hexdigest(), 16) % len(STYLE_KEYS)
    return STYLE_KEYS[idx]


# --------- DATA STRUCTS / ANALYSIS ----------
class PlaylistData(NamedTuple):
    playlist_id: str
    title: str
    city: str
    country_name: str
    image_style: str  # optional Firestore override

class TitleAnalysis(NamedTuple):
    region: str
    specific_location: str
    main_subject: str
    photography_style: str
    short_title: str
    confidence_score: float
    city: str
    country: str

def analyze_title_with_ai(title: str, city: str, country: str, pid: str="") -> TitleAnalysis:
    try:
        from openai import OpenAI
    except ImportError:
        raise RuntimeError("pip install openai")
    if not OPENAI_API_KEY:
        raise RuntimeError("Set OPENAI_API_KEY env var.")

    client = OpenAI(api_key=OPENAI_API_KEY)

    loc_hint = f'City="{city}"' if city else ""
    if country: loc_hint = (loc_hint + f', Country="{country}"').strip(", ")

    analysis_prompt = f"""
Analyze this travel playlist title for image generation:

Title: "{title}"
Location hints: {loc_hint}

Return ONLY JSON with keys:
- "region" (e.g., "Campania, Italy" for Naples)
- "specific_location" (must reflect the hinted city if provided)
- "main_subject" (concrete visual subject)
- "photography_style"
- "short_title" (2–4 words; include city if natural)
- "confidence_score" (0.0–1.0)

Rules:
- If a city hint is provided, DO NOT switch to another city.
- Keep analysis geographically accurate for the hinted country.
"""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_TEXT_MODEL,
            messages=[
                {"role":"system","content":"You create grounded, location-accurate analyses for travel imagery."},
                {"role":"user","content":analysis_prompt},
            ],
            temperature=0.2, max_tokens=500
        )
        text = resp.choices[0].message.content.strip()
        if text.startswith("```json"):
            text = text.split("```json",1)[1].split("```",1)[0].strip()
        elif text.startswith("```"):
            text = text.split("```",1)[1].split("```",1)[0].strip()
        data = json.loads(text)
        analysis = TitleAnalysis(
            region=data.get("region",""),
            specific_location=data.get("specific_location",""),
            main_subject=data.get("main_subject","iconic local landmark"),
            photography_style=data.get("photography_style","travel photography"),
            short_title=data.get("short_title", title[:40]),
            confidence_score=float(data.get("confidence_score", 0.7)),
            city=city, country=country,
        )
    except Exception as e:
        print(f"   ⚠️  AI analysis failed ({e}), using fallback...")
        analysis = analyze_title_fallback(title, city, country)

    return enforce_location(analysis)

def analyze_title_fallback(title: str, city: str, country: str) -> TitleAnalysis:
    region = country or ""
    t = (title or "").lower()
    if any(k in t for k in ["love","romance","enchanted","whisper"]):
        subject, style = "romantic cityscape with heritage architecture", "dreamy cinematic travel photography"
    elif any(k in t for k in ["adventure","wild","mountain","trek"]):
        subject, style = "dramatic natural landscapes", "dynamic outdoor photography"
    elif any(k in t for k in ["heritage","historic","ancient","temple","museum","palace"]):
        subject, style = "historic architecture and cultural landmarks", "architectural travel photography"
    else:
        subject, style = "iconic local landmark with golden hour lighting", "travel photography"
    short_title = shorten_title_intelligently(title, city)
    return enforce_location(TitleAnalysis(region, city or country or "Unknown",
                                         subject, style, short_title, 0.6, city, country))

def enforce_location(a: TitleAnalysis) -> TitleAnalysis:
    region = a.region or a.country or ""
    specific = a.specific_location or a.city or a.country or "Unknown"
    if a.city:
        if a.city.lower() not in specific.lower():
            specific = f"{a.city}, {a.country}" if a.country else a.city
        if a.country and a.country.lower() not in region.lower():
            region = f"{region}, {a.country}".strip(", ")
        st = a.short_title or a.city
        if a.city.lower() not in st.lower():
            st = f"{a.city} {st.split()[0] if st else 'Highlights'}"
    else:
        st = a.short_title or (a.country or "Top Picks")
    return TitleAnalysis(region, specific, a.main_subject, a.photography_style,
                         st, a.confidence_score, a.city, a.country)

def shorten_title_intelligently(title: str, city: str) -> str:
    filler = {'in','of','the','a','an','and','to','for'}
    words = [w for w in re.split(r"\s+", title.strip()) if w]
    core = [w for w in words if w.lower() not in filler]
    short = " ".join(core[:3]) if core else title[:20]
    if city and city.lower() not in short.lower():
        short = f"{city} {core[0] if core else 'Highlights'}"
    return short


# ------------- PROMPT (style-aware) -------------
def build_ai_enhanced_image_prompt(a: TitleAnalysis, pid: str, preset_key: str) -> str:
    loc = f"{a.city}, {a.country}" if a.city and a.country else (a.city or a.country or "the destination")
    preset = STYLE_PRESETS[preset_key]
    suffix = preset["suffix"].replace("{LOC}", (a.city or a.country or "the destination"))
    negatives = "Avoid: text overlays, logos, watermarks, crowds, clutter, signage, HDR, artificial colors."
    return (
        f"A stunning {a.photography_style} photograph in {loc}. "
        f"Main subject: {a.main_subject}. "
        f"{suffix} "
        f"{negatives} "
        f"Style_ID: {preset_key}_{a.city or a.country}_{pid}_{a.confidence_score:.1f}"
    )


# ------------- IMAGE GENERATION -------------
def generate_square_image_bytes(prompt: str, quality: str="high") -> bytes:
    try:
        from openai import OpenAI
    except ImportError:
        raise RuntimeError("pip install openai")
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY missing. Set it in your environment.")
    if quality not in {"low","medium","high","auto"}:
        raise ValueError("quality must be one of: low | medium | high | auto")

    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.images.generate(
        model=OPENAI_IMAGE_MODEL,
        prompt=prompt[:1000],
        size=OPENAI_IMAGE_SIZE,
        quality=quality,
        n=1,
    )

    d0 = resp.data[0]
    b64 = getattr(d0, "b64_json", None)
    if b64:
        import base64
        return base64.b64decode(b64)
    url = getattr(d0, "url", None)
    if url:
        import requests
        r = requests.get(url, timeout=30); r.raise_for_status()
        return r.content
    raise RuntimeError("Image generation succeeded but returned no bytes.")


# ------------- POST-PROCESS (cover/hero) -------------
# def derive_cover_and_hero(base_bytes: bytes, analysis: TitleAnalysis,
#                           user_font: str="", debug_print: bool=False, pid: str="",text_weight: str = "normal") -> Dict[str, bytes]:
#     base = Image.open(BytesIO(base_bytes)).convert("RGB")
#     base_w, base_h = base.size
#     if debug_print:
#         print(f"   ✂️  Short Title: '{analysis.short_title}' (confidence: {analysis.confidence_score:.2f})")

#     # Cover: 1:1 crop
#     side = min(base_w, base_h)
#     left = (base_w - side)//2
#     top  = (base_h - side)//2
#     cover_sq = base.crop((left, top, left+side, top+side)).resize((COVER_SIZE, COVER_SIZE), Image.LANCZOS)
#     cover = draw_text_bottom_left_auto(
#     cover_sq,
#     analysis.short_title,
#     TEXT_PADDING,
#     user_font=selected_font_path,
#     faux_bold=(text_weight == "bold")
# )


#     # Hero: 16:9 crop
#     target = HERO_W / HERO_H
#     if base_w / base_h > target:
#         new_w = int(base_h * target)
#         left = (base_w - new_w)//2
#         hero_crop = base.crop((left, 0, left+new_w, base_h))
#     else:
#         new_h = int(base_w / target)
#         top  = (base_h - new_h)//2
#         hero_crop = base.crop((0, top, base_w, top+new_h))
#     hero = hero_crop.resize((HERO_W, HERO_H), Image.LANCZOS)

#     out_base = BytesIO(); base.save(out_base,  format="JPEG", quality=92, optimize=True)
#     out_cover= BytesIO(); cover.save(out_cover, format="JPEG", quality=90, optimize=True)
#     out_hero = BytesIO(); hero.save(out_hero,  format="JPEG", quality=90, optimize=True)
#     return {"base": out_base.getvalue(), "cover": out_cover.getvalue(), "hero": out_hero.getvalue()}

def derive_cover_and_hero(
    base_bytes: bytes,
    analysis: TitleAnalysis,
    user_font: str = "",
    debug_print: bool = False,
    pid: str = "",
    text_weight: str = "normal",
) -> Dict[str, bytes]:
    from io import BytesIO
    from PIL import Image

    base = Image.open(BytesIO(base_bytes)).convert("RGB")
    base_w, base_h = base.size
    if debug_print:
        print(f"   ✂️  Short Title: '{analysis.short_title}' (confidence: {analysis.confidence_score:.2f})")

    # Cover: 1:1 crop
    side = min(base_w, base_h)
    left = (base_w - side) // 2
    top = (base_h - side) // 2
    cover_sq = base.crop((left, top, left + side, top + side)).resize((COVER_SIZE, COVER_SIZE), Image.LANCZOS)

    # IMPORTANT: use the function parameter `user_font` here, not a non-existent local
    cover = draw_text_bottom_left_auto(
        cover_sq,
        analysis.short_title,
        TEXT_PADDING,
        preferred_font=user_font,             # ← correct keyword
        faux_bold=(text_weight == "bold"),    # ← enables bold effect if requested
    )

    # Hero: 16:9 crop
    target = HERO_W / HERO_H
    if base_w / base_h > target:
        new_w = int(base_h * target)
        left = (base_w - new_w) // 2
        hero_crop = base.crop((left, 0, left + new_w, base_h))
    else:
        new_h = int(base_w / target)
        top = (base_h - new_h) // 2
        hero_crop = base.crop((0, top, base_w, top + new_h))
    hero = hero_crop.resize((HERO_W, HERO_H), Image.LANCZOS)

    out_base = BytesIO(); base.save(out_base, format="JPEG", quality=92, optimize=True)
    out_cover = BytesIO(); cover.save(out_cover, format="JPEG", quality=90, optimize=True)
    out_hero = BytesIO(); hero.save(out_hero, format="JPEG", quality=90, optimize=True)
    return {"base": out_base.getvalue(), "cover": out_cover.getvalue(), "hero": out_hero.getvalue()}


# ------------- FIREBASE / GCS -------------
def init_firebase():
    import firebase_admin
    from firebase_admin import credentials
    if not Path(SERVICE_ACCOUNT_JSON).exists():
        raise RuntimeError(f"Service account JSON not found: {SERVICE_ACCOUNT_JSON}")
    if not getattr(firebase_admin, "_apps", None):
        cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
        firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})

def fetch_docs_by_ids(ids: List[str]) -> List:
    from firebase_admin import firestore
    db = firestore.client()
    snaps = []
    for _id in ids:
        snaps.append(db.collection(COLLECTION).document(_id).get())
    return snaps

def fetch_all_docs(start_after: Optional[str]=None) -> List:
    from firebase_admin import firestore
    db = firestore.client()
    snaps = []
    for snap in db.collection(COLLECTION).stream():
        if start_after and str(snap.id) <= str(start_after):
            continue
        snaps.append(snap)
    return snaps

def upload_to_gcs(bytes_data: bytes, bucket_name: str, dest_path: str, public: bool=True) -> str:
    from google.cloud import storage
    client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_path)
    blob.cache_control = "public, max-age=31536000"
    blob.upload_from_string(bytes_data, content_type="image/jpeg")
    if public:
        try: blob.make_public()
        except Exception: pass
    return f"https://storage.googleapis.com/{bucket_name}/{dest_path}"

def update_firestore_urls(doc_id: str, urls: Dict[str,str], style_used: str):
    from firebase_admin import firestore
    db = firestore.client()
    data = {
        # "image_base_url": urls.get("base"),
        # "image_cover_url": urls.get("cover"),
        # "image_hero_url":  urls.get("hero"),
        "imageUrls": urls,
        "image_style": style_used,  # record which preset was applied
        "images_last_updated_ts": firestore.SERVER_TIMESTAMP,
    }
    db.collection(COLLECTION).document(doc_id).set(data, merge=True)


# ------------- PER-DOC PROCESSOR -------------
def process_doc(snap, args, font_path: str, duplicate_hashes: set) -> bool:
    data = snap.to_dict() or {}
    pid    = str(snap.id)
    title  = str(data.get("title") or data.get("name") or f"Untitled-{pid}")
    city   = str(data.get("city") or "")
    country= str(data.get("country_name") or data.get("country") or "")
    fs_style = str(data.get("image_style") or "")
    fs_font  = str(data.get("image_font")  or "")  # NEW: optional per-doc font (alias or full path)
    fs_font_w = (data.get("image_font_weight") or "").strip().lower()
    desired_weight = fs_font_w if fs_font_w in {"normal","bold"} else args.font_weight

    # Skip if already has new URLs
    if args.skip_existing and all(k in data for k in ["image_base_url","image_cover_url","image_hero_url"]):
        print(f"⏭️  Skip {pid} (already has base/cover/hero).")
        return True

    print(f"\n📋 {pid} • {title}  |  {city}, {country}")

    # Analyze
    if args.no_ai_analysis:
        analysis = analyze_title_fallback(title, city, country)
        print("   📏 Using rule-based analysis (--no-ai-analysis)")
    else:
        print("   🧠 Analyzing with city+country-aware AI...")
        analysis = analyze_title_with_ai(title, city, country, pid)

    # Choose style
    preset_key = choose_style(args.style if args.style != "auto" else None, fs_style, pid)

    # Choose font for this doc
    selected_font_path = choose_font_path(args.font, fs_font, pid, 
                                      max_variants=args.font_variants,
                                      weight=desired_weight)

    if args.print_prompts:
        print(f"   🔤 Font: {os.path.basename(selected_font_path) if selected_font_path else '(auto default)'}")

    if args.print_prompts or args.dry_run:
        print(f"   🌍 Region: {analysis.region}")
        print(f"   📌 Location: {analysis.specific_location}")
        print(f"   🎯 Subject: {analysis.main_subject}")
        print(f"   📸 Style (photo): {analysis.photography_style}")
        print(f"   ✂️  Short: '{analysis.short_title}'")
        print(f"   🎨 Style preset: {preset_key} — {STYLE_PRESETS[preset_key]['human']}")
        print(f"   📊 Confidence: {analysis.confidence_score:.2f}")

    if args.dry_run:
        return True

    # Prompt + generate
    prompt = build_ai_enhanced_image_prompt(analysis, pid, preset_key)
    if args.print_prompts:
        print(f"   🤖 Prompt: {prompt[:180]}...")
    print("   🎨 Generating image...")
    base_bytes = generate_square_image_bytes(prompt, quality=args.img_quality)

    # Dedup within run
    h = hashlib.sha1(base_bytes).hexdigest()
    if h in duplicate_hashes:
        if args.print_prompts:
            print("   🔄 Duplicate detected; regenerating with variation...")
        prompt2 = prompt.replace(f"_{analysis.confidence_score:.1f}", f"_retry_{analysis.confidence_score:.1f}")
        base_bytes = generate_square_image_bytes(prompt2, quality=args.img_quality)
        h = hashlib.sha1(base_bytes).hexdigest()
    duplicate_hashes.add(h)

    print("   🖼️  Creating cover and hero...")
    imgs = derive_cover_and_hero(base_bytes, analysis, user_font=selected_font_path, debug_print=args.print_prompts, pid=pid,
    text_weight=desired_weight)

    # Optional local QA save
    if args.save_local:
        pdir = OUTDIR / pid
        pdir.mkdir(parents=True, exist_ok=True)
        (pdir/"base.jpg").write_bytes(imgs["base"])
        (pdir/"cover.jpg").write_bytes(imgs["cover"])
        (pdir/"hero.jpg").write_bytes(imgs["hero"])

    if args.local_only:
        print("   💾 Local-only: skipping upload & Firestore update.")
    else:
        print("   ☁️  Uploading to Cloud Storage...")
        base_path  = f"playlistsNew_images/{pid}/base.jpg"
        cover_path = f"playlistsNew_images/{pid}/cover.jpg"
        hero_path  = f"playlistsNew_images/{pid}/hero.jpg"

        base_url  = upload_to_gcs(imgs["base"],  GCS_BUCKET_NAME, base_path)
        cover_url = upload_to_gcs(imgs["cover"], GCS_BUCKET_NAME, cover_path)
        hero_url  = upload_to_gcs(imgs["hero"],  GCS_BUCKET_NAME, hero_path)

        urls = {"base": base_url, "cover": cover_url, "hero": hero_url}
        print("   📝 Updating Firestore (preserving existing imageUrl)...")
        update_firestore_urls(pid, urls, style_used=preset_key)

        print(f"   ✅ Done {pid}")

    return True


# ------------- CLI / MAIN -------------
def load_ids_from_json(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    ids = data.get("document_ids")
    if not isinstance(ids, list):
        raise ValueError("JSON must contain an array field 'document_ids'.")
    out = []
    for v in ids:
        if v is None: continue
        s = str(v).strip()
        if s: out.append(s)
    return out

def main():
    parser = argparse.ArgumentParser(description="Generate & upload base/cover/hero for playlists; update Firestore")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--all", action="store_true", help="Process all documents in the collection")
    mode.add_argument("--ids", nargs="+", help="Process only these doc IDs")
    mode.add_argument("--ids-json", help="Path to JSON file with {\"document_ids\": [\"1\",\"10\",\"100\",...]}")

    parser.add_argument("--start-after", help="Resume: skip documents <= this doc id")
    parser.add_argument("--font", default="", help="Path to .ttf (e.g., C:\\Windows\\Fonts\\seguisb.ttf)")
    parser.add_argument("--img-quality", choices=["low","medium","high","auto"], default="high")
    parser.add_argument("--style", choices=STYLE_KEYS + ["auto"], default="auto",
                        help="Force style preset for all images, or 'auto' to rotate (default: auto)")
    parser.add_argument("--dry-run", action="store_true", help="Analyze only (no image generation/upload)")
    parser.add_argument("--no-ai-analysis", action="store_true", help="Use fallback rules (no GPT calls)")
    parser.add_argument("--print-prompts", action="store_true", help="Print analysis/prompt logs")
    parser.add_argument("--skip-existing", action="store_true", help="Skip docs that already have new URLs")
    parser.add_argument("--save-local", action="store_true", help="Also save base/cover/hero to trial_out/<id>")
    parser.add_argument("--local-only", action="store_true",
                    help="Generate and save images locally without uploading or updating Firestore")
    parser.add_argument(
    "--font-variants", type=int, default=5,
    help="How many distinct fonts to rotate through automatically (default: 5)."
    )
    parser.add_argument(
    "--font-aliases", action="store_true",
    help="Print available font aliases and exit."
    )
    parser.add_argument("--font-weight", choices=["normal","bold"], default="normal",
                    help="Use bold font weight when available (or simulate).")


    args = parser.parse_args()

    if args.font_aliases:
        print("Font aliases you can use with --font or 'image_font' in Firestore:")
        for k, v in FONT_ALIASES.items():
            print(f"  {k:8s} -> {v} {'(OK)' if os.path.exists(v) else '(missing)'}")
        return


    if not OPENAI_API_KEY and not args.no_ai_analysis and not args.dry_run:
        raise RuntimeError("OPENAI_API_KEY missing. Set it in your environment.")

    init_firebase()

    # Collect doc IDs according to mode
    docs = []
    if args.all:
        docs = fetch_all_docs(start_after=args.start_after)
    else:
        ids_set = set()
        if args.ids:
            ids_set.update(str(x).strip() for x in args.ids if str(x).strip())
        if args.ids_json:
            ids_from_file = load_ids_from_json(args.ids_json)
            ids_set.update(ids_from_file)
        if not ids_set:
            raise SystemExit("No IDs provided. Use --ids or --ids-json.")
        docs = fetch_docs_by_ids(sorted(ids_set))

    if not docs:
        print("No documents matched.")
        return

    duplicate_hashes: set = set()
    ok = 0
    print(f"Found {len(docs)} document(s). Starting...")
    for snap in docs:
        try:
            if process_doc(snap, args, args.font, duplicate_hashes):
                ok += 1
        except KeyboardInterrupt:
            print("\nInterrupted by user.")
            break
        except Exception as e:
            print(f"   ❌ Error on {snap.id}: {e}")
            import traceback; traceback.print_exc()

    print(f"\n🎉 Completed. Successful: {ok}/{len(docs)}")


if __name__ == "__main__":
    main()





# """
# AI-Enhanced Playlist Image Generator (City+Country Grounded)
# - Reads title, city, country_name from Firestore
# - Uses city/country hints in GPT analysis (and enforces them afterward)
# - Generates images with gpt-image-1
# - Produces 1:1 (cover with text) + 16:9 hero

# IMPORTANT:
# - Set your OpenAI key via env var:  setx OPENAI_API_KEY "sk-..."
# - Never hard-code keys in this file.
# """

# import os, re, argparse, hashlib, unicodedata, json
# from io import BytesIO
# from pathlib import Path
# from typing import Dict, List, Tuple, NamedTuple, Optional
# from PIL import Image, ImageDraw, ImageFont

# # ---------------------- CONFIG ----------------------
# PROJECT_ID           = "mycasavsc"
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# COLLECTION           = "playlistsNew"

# OPENAI_API_KEY       = "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"
# OPENAI_TEXT_MODEL    = "gpt-4.1-mini"
# OPENAI_IMAGE_MODEL   = "gpt-image-1-mini"
# OPENAI_IMAGE_SIZE    = "1024x1024"

# OUTDIR               = Path("trial_out")
# COVER_SIZE           = 1200
# HERO_W, HERO_H       = 1920, 1080
# FONT_PATH            = ""
# TEXT_PADDING         = 48
# # ----------------------------------------------------


# # ----------------------------- Font discovery ---------------------------------
# FONT_CANDIDATES = [
#     # Windows
#     r"C:\Windows\Fonts\seguisb.ttf",
#     r"C:\Windows\Fonts\segoeui.ttf",
#     r"C:\Windows\Fonts\arial.ttf",
#     r"C:\Windows\Fonts\calibri.ttf",
#     r"C:\Windows\Fonts\tahoma.ttf",
#     # macOS
#     "/Library/Fonts/Arial.ttf",
#     "/System/Library/Fonts/Supplemental/Arial.ttf",
#     # Linux
#     "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
# ]

# def _pick_font(size: int, preferred: str = "", fallback_cfg: str = "") -> ImageFont.FreeTypeFont:
#     for p in [preferred, fallback_cfg, FONT_PATH, *FONT_CANDIDATES]:
#         if p and os.path.exists(p):
#             try:
#                 return ImageFont.truetype(p, size)
#             except Exception:
#                 pass
#     raise RuntimeError(
#         "No scalable TTF font found. Pass --font path to a .ttf (e.g., C:\\Windows\\Fonts\\segoeui.ttf)."
#     )


# # --------------------------- Text overlay helper ------------------------------
# def draw_text_bottom_left_auto(
#     img: Image.Image,
#     text: str,
#     padding: int = 48,
#     preferred_font: str = "",
#     max_lines: int = 2,
#     width_frac: float = 0.86,
#     max_height_frac: float = 0.32,
# ) -> Image.Image:
#     if img.mode != "RGBA":
#         img = img.convert("RGBA")
#     W, H = img.size
#     draw = ImageDraw.Draw(img, "RGBA")

#     target_w = W * width_frac
#     target_h = H * max_height_frac
#     size = int(W * 0.14)
#     min_size = max(36, int(W * 0.05))
#     chosen_font, text_block = None, None

#     def wrap_lines(s: str, font: ImageFont.ImageFont) -> Optional[str]:
#         words, lines, line = s.split(), [], ""
#         for w in words:
#             test = (line + " " + w).strip()
#             if draw.textlength(test, font=font) <= target_w:
#                 line = test
#             else:
#                 if line:
#                     lines.append(line)
#                 line = w
#                 if len(lines) >= max_lines:
#                     return None
#         if line:
#             lines.append(line)
#         return "\n".join(lines)

#     while size >= min_size:
#         font = _pick_font(size, preferred_font)
#         wrapped = wrap_lines(text, font)
#         if wrapped is None:
#             size -= 4
#             continue
#         x0, y0, x1, y1 = draw.multiline_textbbox((0, 0), wrapped, font=font, spacing=int(size * 0.2))
#         block_w, block_h = x1 - x0, y1 - y0
#         if block_w <= target_w and block_h <= target_h:
#             chosen_font, text_block = font, wrapped
#             break
#         size -= 4

#     if chosen_font is None:
#         chosen_font = _pick_font(min_size, preferred_font)
#         text_block = wrap_lines(text, chosen_font) or text

#     x = padding
#     _, _, _, block_h = draw.multiline_textbbox(
#         (0, 0), text_block, font=chosen_font, spacing=int(chosen_font.size * 0.2)
#     )
#     y = H - padding - block_h

#     # Gradient panel
#     panel_h = block_h + max(16, padding // 2)
#     overlay = Image.new("RGBA", (W, panel_h), (0, 0, 0, 0))
#     ov = ImageDraw.Draw(overlay)
#     for i in range(panel_h):
#         alpha = int(180 * (i / panel_h))
#         ov.line([(0, i), (W, i)], fill=(0, 0, 0, alpha))
#     img.alpha_composite(overlay, (0, H - panel_h))

#     # Text + shadow
#     draw = ImageDraw.Draw(img, "RGBA")
#     draw.multiline_text(
#         (x + 2, y + 2), text_block, font=chosen_font, fill=(0, 0, 0, 200), spacing=int(chosen_font.size * 0.2)
#     )
#     draw.multiline_text(
#         (x, y), text_block, font=chosen_font, fill=(255, 255, 255, 255), spacing=int(chosen_font.size * 0.2)
#     )
#     return img.convert("RGB")


# # ----------------------- Playlist data structure ------------------------------
# class PlaylistData(NamedTuple):
#     playlist_id: str
#     title: str
#     city: str
#     country_name: str


# # ----------------------- Title analysis (AI + fallback) -----------------------
# class TitleAnalysis(NamedTuple):
#     region: str
#     specific_location: str
#     main_subject: str
#     photography_style: str
#     short_title: str
#     confidence_score: float
#     city: str
#     country: str


# def analyze_title_with_ai(title: str, city: str, country: str, pid: str = "") -> TitleAnalysis:
#     """
#     Analyze a playlist title with explicit city/country context.
#     We then enforce the hints so the result cannot drift.
#     """
#     try:
#         from openai import OpenAI
#     except ImportError:
#         raise RuntimeError("OpenAI library not installed. Run: pip install openai")

#     if not OPENAI_API_KEY:
#         raise RuntimeError("OPENAI_API_KEY is missing. Set it in your environment.")

#     client = OpenAI(api_key=OPENAI_API_KEY)

#     loc_hint = f'City="{city}"' if city else ""
#     if country:
#         loc_hint = (loc_hint + f', Country="{country}"').strip(", ")

#     analysis_prompt = f"""
# Analyze this travel playlist title for image generation:

# Title: "{title}"
# Location hints: {loc_hint}

# Return ONLY JSON with keys:
# - "region" (e.g., "Campania, Italy" if city is Naples)
# - "specific_location" (must reflect the hinted city if provided)
# - "main_subject" (concrete visual subject)
# - "photography_style"
# - "short_title" (2–4 words; include the city if natural)
# - "confidence_score" (0.0–1.0)

# Rules:
# - If a city hint is provided, DO NOT change the city.
# - Keep the analysis culturally/geographically accurate for the hinted country.
# """

#     try:
#         resp = client.chat.completions.create(
#             model=OPENAI_TEXT_MODEL,
#             messages=[
#                 {"role": "system", "content": "You create grounded, location-accurate analyses for travel imagery."},
#                 {"role": "user", "content": analysis_prompt},
#             ],
#             temperature=0.2,
#             max_tokens=500,
#         )
#         text = resp.choices[0].message.content.strip()
#         if text.startswith("```json"):
#             text = text.split("```json", 1)[1].split("```", 1)[0].strip()
#         elif text.startswith("```"):
#             text = text.split("```", 1)[1].split("```", 1)[0].strip()
#         data = json.loads(text)
#         analysis = TitleAnalysis(
#             region=data.get("region", ""),
#             specific_location=data.get("specific_location", ""),
#             main_subject=data.get("main_subject", "iconic local landmark"),
#             photography_style=data.get("photography_style", "travel photography"),
#             short_title=data.get("short_title", title[:40]),
#             confidence_score=float(data.get("confidence_score", 0.7)),
#             city=city, country=country,
#         )
#     except Exception as e:
#         print(f"   ⚠️  AI analysis failed ({e}), using fallback...")
#         analysis = analyze_title_fallback(title, city, country)

#     return enforce_location(analysis)


# def analyze_title_fallback(title: str, city: str, country: str) -> TitleAnalysis:
#     """Simple rules with location context."""
#     # region guess by country (minimal mapping)
#     region = ""
#     if country.lower() in {"italy"}:
#         region = "Italy"
#     elif country:
#         region = country

#     # subject/style from keywords
#     t = (title or "").lower()
#     if any(k in t for k in ["love", "romance", "enchanted", "whisper"]):
#         subject = "romantic cityscape with heritage architecture and warm evening light"
#         style = "dreamy cinematic travel photography"
#     elif any(k in t for k in ["adventure","wild","mountain","trek"]):
#         subject = "dramatic natural landscapes with sense of motion"
#         style = "dynamic outdoor photography"
#     elif any(k in t for k in ["heritage","historic","ancient","temple","museum","palace"]):
#         subject = "historic architecture and cultural landmarks"
#         style = "architectural travel photography"
#     else:
#         subject = "iconic local landmark with golden hour lighting"
#         style = "travel photography"

#     short_title = shorten_title_intelligently(title, city)

#     return enforce_location(TitleAnalysis(
#         region=region,
#         specific_location=city or country or "Unknown",
#         main_subject=subject,
#         photography_style=style,
#         short_title=short_title,
#         confidence_score=0.6,
#         city=city, country=country,
#     ))


# def enforce_location(a: TitleAnalysis) -> TitleAnalysis:
#     """Hard guardrails: force city/country correctness and fix short_title."""
#     region = a.region or a.country or ""
#     specific = a.specific_location or a.city or a.country or "Unknown"

#     if a.city:
#         # Ensure specific_location shows the city
#         if a.city.lower() not in specific.lower():
#             specific = a.city if not a.country else f"{a.city}, {a.country}"
#         # Ensure region contains country (if missing)
#         if a.country and a.country.lower() not in region.lower():
#             region = f"{region}, {a.country}".strip(", ")

#         # Ensure short_title contains the city (prepend if missing)
#         st = a.short_title or a.city
#         if a.city.lower() not in st.lower():
#             st = f"{a.city} {st.split()[0] if st else 'Highlights'}"
#     else:
#         st = a.short_title or (a.country or "Top Picks")

#     return TitleAnalysis(
#         region=region,
#         specific_location=specific,
#         main_subject=a.main_subject,
#         photography_style=a.photography_style,
#         short_title=st,
#         confidence_score=a.confidence_score,
#         city=a.city,
#         country=a.country,
#     )


# def shorten_title_intelligently(title: str, city: str) -> str:
#     """Shorten title; include city if provided."""
#     filler = {'in','of','the','a','an','and','to','for'}
#     words = [w for w in re.split(r"\s+", title.strip()) if w]
#     core = [w for w in words if w.lower() not in filler]
#     short = " ".join(core[:3]) if core else title[:20]
#     if city and city.lower() not in short.lower():
#         short = f"{city} {core[0] if core else 'Highlights'}"
#     return short


# # ------------------------- Image prompt builder -------------------------------
# def build_ai_enhanced_image_prompt(a: TitleAnalysis, pid: str = "") -> str:
#     loc_line = f"{a.city}, {a.country}" if a.city and a.country else (a.city or a.country or "the destination")
#     prompt = (
#         f"A stunning {a.photography_style} photograph in {loc_line}. "
#         f"Main subject: {a.main_subject}. "
#         "Cinematic composition, natural warm lighting, shallow depth of field, high detail. "
#         "Avoid: text overlays, logos, watermarks, crowds, signage, clutter, HDR, artificial colors. "
#         f"Style_ID: {a.city or a.country}_{pid}_{a.confidence_score:.1f}"
#     )
#     return prompt


# # ----------------------- Image generation (OpenAI) ----------------------------
# def generate_square_image_bytes(prompt: str, quality: str = "high") -> bytes:
#     """Generate image bytes using gpt-image-1 (no response_format param)."""
#     try:
#         from openai import OpenAI
#     except ImportError:
#         raise RuntimeError("OpenAI library not installed. Run: pip install openai")

#     if not OPENAI_API_KEY:
#         raise RuntimeError("OPENAI_API_KEY missing. Set as environment variable.")

#     if quality not in {"low","medium","high","auto"}:
#         raise ValueError("quality must be one of: low | medium | high | auto")

#     client = OpenAI(api_key=OPENAI_API_KEY)

#     resp = client.images.generate(
#         model=OPENAI_IMAGE_MODEL,
#         prompt=prompt[:1000],
#         size=OPENAI_IMAGE_SIZE,
#         quality=quality,
#         n=1,
#     )

#     d0 = resp.data[0]
#     b64 = getattr(d0, "b64_json", None)
#     if b64:
#         import base64
#         return base64.b64decode(b64)

#     # Fallback to URL if provided
#     url = getattr(d0, "url", None)
#     if url:
#         import requests
#         r = requests.get(url, timeout=30)
#         r.raise_for_status()
#         return r.content

#     raise RuntimeError("Image generation succeeded but returned no bytes.")


# # ----------------------- Cover/Hero processing --------------------------------
# def derive_cover_and_hero(
#     base_bytes: bytes,
#     analysis: TitleAnalysis,
#     user_font: str = "",
#     debug_print: bool = False,
#     pid: str = "",
# ) -> Dict[str, bytes]:
#     base = Image.open(BytesIO(base_bytes)).convert("RGB")
#     base_w, base_h = base.size

#     if debug_print:
#         print(f"   ✂️  Short Title: '{analysis.short_title}' (confidence: {analysis.confidence_score:.2f})")

#     # Cover: center crop to square -> resize -> overlay text
#     side = min(base_w, base_h)
#     left = (base_w - side) // 2
#     top = (base_h - side) // 2
#     cover_square = base.crop((left, top, left + side, top + side))
#     cover_resized = cover_square.resize((COVER_SIZE, COVER_SIZE), Image.LANCZOS)
#     cover = draw_text_bottom_left_auto(
#         cover_resized, analysis.short_title, padding=TEXT_PADDING, preferred_font=user_font, max_lines=2
#     )

#     # Hero: 16:9 crop
#     target_ratio = HERO_W / HERO_H
#     if base_w / base_h > target_ratio:
#         new_w = int(base_h * target_ratio)
#         left = (base_w - new_w) // 2
#         hero_crop = base.crop((left, 0, left + new_w, base_h))
#     else:
#         new_h = int(base_w / target_ratio)
#         top = (base_h - new_h) // 2
#         hero_crop = base.crop((0, top, base_w, top + new_h))
#     hero = hero_crop.resize((HERO_W, HERO_H), Image.LANCZOS)

#     # Encode JPEGs
#     out_base = BytesIO(); base.save(out_base, format="JPEG", quality=92, optimize=True)
#     out_cover = BytesIO(); cover.save(out_cover, format="JPEG", quality=90, optimize=True)
#     out_hero = BytesIO(); hero.save(out_hero, format="JPEG", quality=90, optimize=True)

#     return {"base": out_base.getvalue(), "cover": out_cover.getvalue(), "hero": out_hero.getvalue()}


# # ----------------------------- Firestore (read) -------------------------------
# def fetch_playlists_from_firestore(ids: List[str]) -> Dict[str, PlaylistData]:
#     """Returns {id: PlaylistData(title, city, country_name)}"""
#     try:
#         import firebase_admin
#         from firebase_admin import credentials, firestore
#     except ImportError:
#         raise RuntimeError("Firebase libraries not installed. Run: pip install firebase-admin")

#     if not Path(SERVICE_ACCOUNT_JSON).exists():
#         raise RuntimeError(f"Service account JSON not found: {SERVICE_ACCOUNT_JSON}")

#     if not getattr(__import__("firebase_admin"), "_apps", None):
#         cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#         firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})

#     db = firestore.client()
#     out: Dict[str, PlaylistData] = {}

#     for doc_id in ids:
#         try:
#             doc = db.collection(COLLECTION).document(doc_id).get()
#             if not doc.exists:
#                 print(f"⚠️  Document {doc_id} not found in collection '{COLLECTION}'")
#                 continue
#             data = doc.to_dict() or {}
#             title = str(data.get("title") or data.get("name") or f"Untitled-{doc_id}")
#             city = str(data.get("city") or "")
#             country = str(data.get("country_name") or data.get("country") or "")
#             out[doc_id] = PlaylistData(doc_id, title, city, country)
#         except Exception as e:
#             print(f"⚠️  Error fetching {doc_id}: {e}")

#     return out


# # ---------------------------------- CLI ---------------------------------------
# def main():
#     parser = argparse.ArgumentParser(
#         description="AI-Enhanced playlist image generator (city+country grounded)"
#     )

#     mode_group = parser.add_mutually_exclusive_group(required=True)
#     mode_group.add_argument("--ids", nargs="+", help="Firestore document IDs to fetch")
#     mode_group.add_argument("--id", help="Manual mode: local ID label (requires --title and --city/--country)")

#     parser.add_argument("--title", help="Manual mode: title text")
#     parser.add_argument("--city", help="Manual mode: city (e.g., 'Naples')")
#     parser.add_argument("--country", help="Manual mode: country name (e.g., 'Italy')")
#     parser.add_argument("--outdir", default=str(OUTDIR), help=f"Output directory (default: {OUTDIR})")
#     parser.add_argument("--font", default="", help="Path to a .ttf/.otf font file")
#     parser.add_argument("--print-prompts", action="store_true", help="Print analysis and save ai_analysis.json")
#     parser.add_argument("--dry-run", action="store_true", help="Analyze only (no image generation)")
#     parser.add_argument("--no-ai-analysis", action="store_true", help="Skip GPT analysis (use rules)")
#     parser.add_argument("--img-quality", choices=["low", "medium", "high", "auto"], default="high",
#                         help="Image generation quality (default: high)")

#     args = parser.parse_args()

#     # Manual mode validation
#     if args.id and (not args.title or not (args.city or args.country)):
#         parser.error("Manual mode (--id) requires --title and at least one of --city/--country")

#     output_dir = Path(args.outdir)
#     output_dir.mkdir(parents=True, exist_ok=True)

#     # Build playlist dict
#     if args.ids:
#         print("🔍 Fetching playlists from Firestore (title, city, country)...")
#         playlists = fetch_playlists_from_firestore(args.ids)
#         if not playlists:
#             print("❌ No valid documents found. Exiting.")
#             return
#         print(f"✅ Found {len(playlists)} playlist(s)")
#     else:
#         playlists = {
#             args.id: PlaylistData(
#                 playlist_id=args.id,
#                 title=args.title,
#                 city=args.city or "",
#                 country_name=args.country or "",
#             )
#         }

#     processed_count = 0
#     duplicate_hashes: set = set()

#     print(f"\n🎨 Processing {len(playlists)} playlist(s) with grounded AI analysis...")
#     print("=" * 70)

#     for pid, pl in playlists.items():
#         try:
#             print(f"\n📋 {pid}")
#             print(f"   📍 Title: {pl.title}")
#             print(f"   🗺️  City: {pl.city}   •   Country: {pl.country_name}")

#             # Step 1: Analyze
#             if args.no_ai_analysis:
#                 print("   📏 Using rule-based analysis (--no-ai-analysis)...")
#                 analysis = analyze_title_fallback(pl.title, pl.city, pl.country_name)
#             else:
#                 print("   🧠 Analyzing with city+country-aware AI...")
#                 analysis = analyze_title_with_ai(pl.title, pl.city, pl.country_name, pid)

#             if args.print_prompts or args.dry_run:
#                 print(f"   🌍 Region: {analysis.region}")
#                 print(f"   📌 Location: {analysis.specific_location}")
#                 print(f"   🎯 Subject: {analysis.main_subject}")
#                 print(f"   📸 Style: {analysis.photography_style}")
#                 print(f"   ✂️  Short: '{analysis.short_title}'")
#                 print(f"   📊 Confidence: {analysis.confidence_score:.2f}")

#             if args.dry_run:
#                 continue

#             # Step 2: Build prompt
#             prompt = build_ai_enhanced_image_prompt(analysis, pid)
#             if args.print_prompts:
#                 print(f"   🤖 Prompt: {prompt[:180]}...")

#             # Step 3: Generate base image
#             print("   🎨 Generating image...")
#             base_bytes = generate_square_image_bytes(prompt, quality=args.img_quality)

#             # Step 4: Deduplicate within this run
#             img_hash = hashlib.sha1(base_bytes).hexdigest()
#             if img_hash in duplicate_hashes:
#                 if args.print_prompts:
#                     print("   🔄 Duplicate detected; regenerating with slight variation...")
#                 prompt2 = prompt.replace(f"_{analysis.confidence_score:.1f}", f"_retry_{analysis.confidence_score:.1f}")
#                 base_bytes = generate_square_image_bytes(prompt2, quality=args.img_quality)
#                 img_hash = hashlib.sha1(base_bytes).hexdigest()
#             duplicate_hashes.add(img_hash)

#             # Step 5: Derive cover/hero
#             print("   🖼️  Creating cover and hero...")
#             imgs = derive_cover_and_hero(
#                 base_bytes, analysis, user_font=args.font, debug_print=args.print_prompts, pid=pid
#             )

#             # Step 6: Save
#             pdir = output_dir / str(pid)
#             pdir.mkdir(parents=True, exist_ok=True)
#             (pdir / "base.jpg").write_bytes(imgs["base"])
#             (pdir / "cover.jpg").write_bytes(imgs["cover"])
#             (pdir / "hero.jpg").write_bytes(imgs["hero"])

#             if args.print_prompts:
#                 (pdir / "ai_analysis.json").write_text(json.dumps({
#                     "original_title": pl.title,
#                     "city": pl.city,
#                     "country": pl.country_name,
#                     "region": analysis.region,
#                     "specific_location": analysis.specific_location,
#                     "main_subject": analysis.main_subject,
#                     "photography_style": analysis.photography_style,
#                     "short_title": analysis.short_title,
#                     "confidence_score": analysis.confidence_score,
#                     "generated_prompt": prompt,
#                     "img_quality": args.img_quality,
#                 }, indent=2), encoding="utf-8")
#                 print("   📄 Saved ai_analysis.json")

#             print(f"   ✅ Saved to: {pdir}")
#             processed_count += 1

#         except Exception as e:
#             print(f"   ❌ Failed to process {pid}: {e}")
#             import traceback; traceback.print_exc()

#     print("\n" + "=" * 70)
#     if args.dry_run:
#         print(f"🔍 Dry run complete! Analyzed {len(playlists)} playlist(s)")
#         print("💡 Re-run without --dry-run to generate images")
#     else:
#         print(f"🎉 Processing complete! {processed_count}/{len(playlists)} successful")
#         if processed_count > 0:
#             print(f"📁 Output directory: {Path(args.outdir).absolute()}")


# if __name__ == "__main__":
#     main()



# """
# AI-Enhanced Playlist Image Generator
# - Fetches playlist titles from Firestore (read-only)
# - Analyzes titles with GPT-4o-mini to produce location-aware prompts
# - Generates a base image with gpt-image-1
# - Produces cover (1:1 with text) + hero (16:9) images

# Fixes vs your last version:
# - Removed unsupported `response_format` for Images API
# - Uses allowed quality values: low | medium | high | auto (default: high)
# - Decodes `b64_json` correctly; URL fallback retained
# - Reads OPENAI_API_KEY from environment (do NOT hard-code keys)
# """

# import os, re, argparse, hashlib, unicodedata, json
# from io import BytesIO
# from pathlib import Path
# from typing import Dict, List, Tuple, NamedTuple, Optional
# from PIL import Image, ImageDraw, ImageFont

# # ---------------------- CONFIG (edit paths/IDs as needed) ----------------------
# PROJECT_ID           = "mycasavsc"
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# COLLECTION           = "playlistsNew"

# # Read API key from environment — rotate your leaked key and set it as an env var:
# #   PowerShell:  setx OPENAI_API_KEY "sk-...."
# OPENAI_API_KEY       = "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"
# OPENAI_TEXT_MODEL    = "gpt-4o-mini"
# OPENAI_IMAGE_MODEL   = "gpt-image-1"
# OPENAI_IMAGE_SIZE    = "1024x1024"   # 256x256, 512x512, 1024x1024, etc.

# OUTDIR               = Path("trial_out")
# COVER_SIZE           = 1200
# HERO_W, HERO_H       = 1920, 1080
# FONT_PATH            = ""  # can be overridden with --font
# TEXT_PADDING         = 48
# # -----------------------------------------------------------------------------


# # ----------------------------- Font discovery ---------------------------------
# FONT_CANDIDATES = [
#     # Windows
#     r"C:\Windows\Fonts\segoeui.ttf",
#     r"C:\Windows\Fonts\arial.ttf",
#     r"C:\Windows\Fonts\calibri.ttf",
#     r"C:\Windows\Fonts\tahoma.ttf",
#     # macOS
#     "/Library/Fonts/Arial.ttf",
#     "/System/Library/Fonts/Supplemental/Arial.ttf",
#     "/System/Library/Fonts/SFNS.ttf",
#     # Linux
#     "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
# ]

# def _pick_font(size: int, preferred: str = "", fallback_cfg: str = "") -> ImageFont.FreeTypeFont:
#     for p in [preferred, fallback_cfg, FONT_PATH, *FONT_CANDIDATES]:
#         if p and os.path.exists(p):
#             try:
#                 return ImageFont.truetype(p, size)
#             except Exception:
#                 pass
#     raise RuntimeError(
#         "No scalable TTF font found. Pass --font path to a .ttf (e.g., C:\\Windows\\Fonts\\segoeui.ttf)."
#     )


# # --------------------------- Text overlay helper ------------------------------
# def draw_text_bottom_left_auto(
#     img: Image.Image,
#     text: str,
#     padding: int = 48,
#     preferred_font: str = "",
#     max_lines: int = 2,
#     width_frac: float = 0.86,
#     max_height_frac: float = 0.32,
# ) -> Image.Image:
#     if img.mode != "RGBA":
#         img = img.convert("RGBA")
#     W, H = img.size
#     draw = ImageDraw.Draw(img, "RGBA")

#     target_w = W * width_frac
#     target_h = H * max_height_frac
#     size = int(W * 0.14)
#     min_size = max(36, int(W * 0.05))
#     chosen_font, text_block = None, None

#     def wrap_lines(s: str, font: ImageFont.ImageFont) -> Optional[str]:
#         words, lines, line = s.split(), [], ""
#         for w in words:
#             test = (line + " " + w).strip()
#             if draw.textlength(test, font=font) <= target_w:
#                 line = test
#             else:
#                 if line:
#                     lines.append(line)
#                 line = w
#                 if len(lines) >= max_lines:
#                     return None
#         if line:
#             lines.append(line)
#         return "\n".join(lines)

#     while size >= min_size:
#         font = _pick_font(size, preferred_font)
#         wrapped = wrap_lines(text, font)
#         if wrapped is None:
#             size -= 4
#             continue
#         x0, y0, x1, y1 = draw.multiline_textbbox((0, 0), wrapped, font=font, spacing=int(size * 0.2))
#         block_w, block_h = x1 - x0, y1 - y0
#         if block_w <= target_w and block_h <= target_h:
#             chosen_font, text_block = font, wrapped
#             break
#         size -= 4

#     if chosen_font is None:
#         chosen_font = _pick_font(min_size, preferred_font)
#         text_block = wrap_lines(text, chosen_font) or text

#     x = padding
#     _, _, _, block_h = draw.multiline_textbbox(
#         (0, 0), text_block, font=chosen_font, spacing=int(chosen_font.size * 0.2)
#     )
#     y = H - padding - block_h

#     # Gradient panel behind text
#     panel_h = block_h + max(16, padding // 2)
#     overlay = Image.new("RGBA", (W, panel_h), (0, 0, 0, 0))
#     ov = ImageDraw.Draw(overlay)
#     for i in range(panel_h):
#         alpha = int(180 * (i / panel_h))
#         ov.line([(0, i), (W, i)], fill=(0, 0, 0, alpha))
#     img.alpha_composite(overlay, (0, H - panel_h))

#     # Draw text with subtle shadow
#     draw = ImageDraw.Draw(img, "RGBA")
#     draw.multiline_text(
#         (x + 2, y + 2), text_block, font=chosen_font, fill=(0, 0, 0, 200), spacing=int(chosen_font.size * 0.2)
#     )
#     draw.multiline_text(
#         (x, y), text_block, font=chosen_font, fill=(255, 255, 255, 255), spacing=int(chosen_font.size * 0.2)
#     )
#     return img.convert("RGB")


# # ----------------------- Title analysis (AI + fallback) -----------------------
# class TitleAnalysis(NamedTuple):
#     region: str
#     specific_location: str
#     main_subject: str
#     photography_style: str
#     short_title: str
#     confidence_score: float

# def analyze_title_with_ai(title: str, pid: str = "") -> TitleAnalysis:
#     """Use GPT-4o-mini to analyze a playlist title into an image prompt scaffold."""
#     try:
#         from openai import OpenAI
#     except ImportError:
#         raise RuntimeError("OpenAI library not installed. Run: pip install openai")

#     if not OPENAI_API_KEY:
#         raise RuntimeError("OPENAI_API_KEY is missing. Set it in your environment.")

#     client = OpenAI(api_key=OPENAI_API_KEY)

#     analysis_prompt = f"""
# Analyze this travel playlist title for image generation: "{title}"

# Provide ONLY JSON with:
# 1. "region"
# 2. "specific_location"
# 3. "main_subject"
# 4. "photography_style"
# 5. "short_title"
# 6. "confidence_score" (0.0 to 1.0)

# Context: Indian destinations; emphasize Indian architecture/landscapes/culture.
# """

#     try:
#         response = client.chat.completions.create(
#             model=OPENAI_TEXT_MODEL,
#             messages=[
#                 {
#                     "role": "system",
#                     "content": "You are an expert travel photographer and content analyst specializing in Indian destinations.",
#                 },
#                 {"role": "user", "content": analysis_prompt},
#             ],
#             temperature=0.3,
#             max_tokens=500,
#         )

#         analysis_text = response.choices[0].message.content.strip()
#         if analysis_text.startswith("```json"):
#             analysis_text = analysis_text.split("```json", 1)[1].split("```", 1)[0].strip()
#         elif analysis_text.startswith("```"):
#             analysis_text = analysis_text.split("```", 1)[1].split("```", 1)[0].strip()

#         data = json.loads(analysis_text)
#         return TitleAnalysis(
#             region=data.get("region", "India"),
#             specific_location=data.get("specific_location", "India"),
#             main_subject=data.get("main_subject", "beautiful Indian landmark"),
#             photography_style=data.get("photography_style", "travel photography"),
#             short_title=data.get("short_title", title[:20]),
#             confidence_score=float(data.get("confidence_score", 0.5)),
#         )

#     except Exception as e:
#         print(f"⚠️  AI analysis error (fallback engaged): {e}")
#         return _fallback_analysis(title)

# def _fallback_analysis(title: str) -> TitleAnalysis:
#     region, location = extract_region_hint_fallback(title)
#     subject, style = guess_subject_and_style_fallback(title)
#     short_title = shorten_title_intelligently_fallback(title)
#     return TitleAnalysis(
#         region=region,
#         specific_location=location,
#         main_subject=subject,
#         photography_style=style,
#         short_title=short_title,
#         confidence_score=0.3,
#     )

# def extract_region_hint_fallback(title: str) -> Tuple[str, str]:
#     t = (title or "").lower()
#     city_mappings = {
#         "srinagar": ("Kashmir, India", "Srinagar"),
#         "leh": ("Ladakh, India", "Leh"),
#         "manali": ("Himachal Pradesh, India", "Manali"),
#         "shimla": ("Himachal Pradesh, India", "Shimla"),
#         "dharamshala": ("Himachal Pradesh, India", "Dharamshala"),
#         "jaipur": ("Rajasthan, India", "Jaipur"),
#         "udaipur": ("Rajasthan, India", "Udaipur"),
#         "jodhpur": ("Rajasthan, India", "Jodhpur"),
#         "pushkar": ("Rajasthan, India", "Pushkar"),
#         "kochi": ("Kerala, India", "Kochi"),
#         "munnar": ("Kerala, India", "Munnar"),
#         "thekkady": ("Kerala, India", "Thekkady"),
#         "wayanad": ("Kerala, India", "Wayanad"),
#         "goa": ("Goa, India", "Goa"),
#         "hampi": ("Karnataka, India", "Hampi"),
#         "coorg": ("Karnataka, India", "Coorg"),
#         "mysore": ("Karnataka, India", "Mysore"),
#         "mumbai": ("Maharashtra, India", "Mumbai"),
#         "pune": ("Maharashtra, India", "Pune"),
#         "lonavala": ("Maharashtra, India", "Lonavala"),
#         "darjeeling": ("West Bengal, India", "Darjeeling"),
#         "gangtok": ("Sikkim, India", "Gangtok"),
#         "chandigarh": ("Punjab, India", "Chandigarh"),
#     }
#     for city, (region, location) in city_mappings.items():
#         if city in t:
#             return region, location

#     if "central india" in t:
#         return "Central India", "Central India"
#     if any(x in t for x in ["rajasthan", "desert"]):
#         return "Rajasthan, India", "Rajasthan"
#     if any(x in t for x in ["kerala", "backwater"]):
#         return "Kerala, India", "Kerala"
#     if any(x in t for x in ["ladakh", "kashmir", "himalaya"]):
#         return "Indian Himalayas", "Himalayas"
#     if any(x in t for x in ["konkan", "western ghats"]):
#         return "Western Coast of India", "Western India"
#     if "sundarbans" in t:
#         return "Sundarbans, India", "Sundarbans"
#     if any(x in t for x in ["tamil", "madurai"]):
#         return "Tamil Nadu, India", "Tamil Nadu"
#     if "karnataka" in t:
#         return "Karnataka, India", "Karnataka"
#     if "gujarat" in t:
#         return "Gujarat, India", "Gujarat"
#     if any(x in t for x in ["assam", "meghalaya", "northeast"]):
#         return "Northeast India", "Northeast India"
#     return "India", "India"

# def guess_subject_and_style_fallback(title: str) -> Tuple[str, str]:
#     t = (title or "").lower()
#     if any(k in t for k in ["rooms", "stays", "hotels", "resorts", "view", "accommodation"]):
#         if "srinagar" in t or "kashmir" in t:
#             return (
#                 "a traditional Kashmiri houseboat on Dal Lake with snow-capped mountains",
#                 "luxury travel photography",
#             )
#         elif "kerala" in t or "backwater" in t:
#             return (
#                 "a Kerala backwater resort with traditional architecture and serene water views",
#                 "boutique hotel photography",
#             )
#         elif any(x in t for x in ["palace", "heritage", "royal"]):
#             return (
#                 "an elegant heritage palace hotel with Rajasthani architecture",
#                 "luxury hotel photography",
#             )
#         else:
#             return (
#                 "a boutique hotel room with panoramic valley views",
#                 "luxury accommodation photography",
#             )
#     if any(k in t for k in ["palace", "regal", "royal"]):
#         return (
#             "a Rajasthani palace with ornate jharokhas and chhatris",
#             "architectural photography",
#         )
#     if any(k in t for k in ["fort", "citadel"]):
#         return (
#             "an Indian hill fort with massive stone walls and mountain backdrop",
#             "heritage photography",
#         )
#     if any(k in t for k in ["wildlife", "tiger", "safari"]):
#         return ("a Bengal tiger in dappled forest light", "wildlife photography")
#     if any(k in t for k in ["desert", "dune", "thar"]):
#         return ("golden sand dunes with camel silhouettes at sunset", "landscape photography")
#     if any(k in t for k in ["beach", "coast", "island"]):
#         return ("a pristine Indian beach with palm trees at golden hour", "coastal photography")
#     if any(k in t for k in ["tea", "plantation"]):
#         return ("terraced tea gardens with morning mist", "agricultural photography")
#     if any(k in t for k in ["mountain", "valley", "himalaya"]):
#         return ("majestic Himalayan peaks with alpine scenery", "mountain photography")
#     return (
#         "a stunning Indian landmark showcasing natural beauty or architectural heritage",
#         "travel photography",
#     )

# def shorten_title_intelligently_fallback(full_title: str) -> str:
#     t = _clean(full_title)
#     if ":" in t:
#         left, right = t.split(":", 1)
#         left, right = left.strip(), right.strip()
#         if len(left.split()) >= 2 and len(right.split()) <= 3:
#             loc_words = _extract_key_words(right)
#             theme_words = _extract_key_words(left)
#             if loc_words and theme_words:
#                 key_loc = loc_words[0]
#                 key_theme = _find_best_theme_word(theme_words)
#                 if key_theme:
#                     return f"{key_loc} {key_theme}"
#         return shorten_phrase(left if len(left.split()) >= 2 else right)
#     if "&" in t:
#         parts = [p.strip() for p in t.split("&")]
#         key = []
#         for part in parts[:2]:
#             w = _extract_key_words(part)
#             if w:
#                 key.append(w[0])
#         if len(key) >= 2:
#             return f"{key[0]} & {key[1]}"
#     return shorten_phrase(t)

# def _clean(text: str) -> str:
#     t = unicodedata.normalize("NFKC", text or "").strip()
#     t = re.sub(r"[\"""„‟''ʼ′`´]+", "", t)
#     return t

# def _extract_key_words(phrase: str) -> List[str]:
#     stop = {"the", "and", "of", "in", "on", "at", "to", "for", "with", "by", "a", "an"}
#     drop = {"escapes", "destinations", "getaways", "spots", "places", "trips", "adventures", "guide", "travel"}
#     words = phrase.split()
#     out: List[str] = []
#     for w in words:
#         cw = re.sub(r"[^\w]", "", w).lower()
#         if cw and cw not in stop and cw not in drop:
#             out.append(w)
#     return out

# def _find_best_theme_word(words: List[str]) -> str:
#     prio = {
#         "stays": 1, "hotels": 1, "resorts": 1, "rooms": 2,
#         "palace": 1, "fort": 1, "temple": 1,
#         "beach": 1, "mountain": 1, "desert": 1, "valley": 1,
#         "wildlife": 1, "safari": 1, "trek": 1,
#         "experience": 3, "journey": 3, "adventure": 2,
#     }
#     best, bp = "", 10
#     for w in words:
#         p = prio.get(w.lower(), 5)
#         if p < bp:
#             best, bp = w, p
#     return best or (words[0] if words else "")

# def shorten_phrase(phrase: str) -> str:
#     words = _extract_key_words(phrase)
#     if not words:
#         return phrase[:20]
#     if len(words) == 1:
#         return words[0]
#     if len(words) == 2:
#         return f"{words[0]} {words[1]}"
#     return f"{words[0]} {words[1]}"


# # ----------------------- Prompt builder for images ----------------------------
# def build_ai_enhanced_image_prompt(analysis: TitleAnalysis, pid: str) -> str:
#     prompt = (
#         f"A stunning {analysis.photography_style} shot in {analysis.region}. "
#         f"Main subject: {analysis.main_subject}. "
#         f"Photography style: cinematic composition with warm, natural lighting, "
#         f"shallow depth of field, high detail, professional travel magazine aesthetic. "
#         f"Color palette: warm earth tones with golden hour lighting. "
#         f"Camera: shot with professional DSLR, 85mm lens equivalent, f/2.8 aperture. "
#     )
#     r = analysis.region.lower()
#     sl = analysis.specific_location.lower()
#     if "kashmir" in r or "srinagar" in sl:
#         prompt += "Emphasize Himalayan backdrop and traditional Kashmiri architecture. "
#     elif "rajasthan" in r:
#         prompt += "Emphasize desert scapes and sandstone architecture with warm golden tones. "
#     elif "kerala" in r:
#         prompt += "Emphasize lush tropical vegetation, water elements, and Kerala style. "
#     elif "himalaya" in r:
#         prompt += "Emphasize snow-capped peaks and dramatic alpine lighting. "

#     negatives = (
#         "Avoid: text overlays, logos, watermarks, people, crowded scenes, signage, "
#         "European landmarks, Eiffel Tower, Western skylines, clutter, HDR, artificial colors"
#     )
#     prompt += f"{negatives}. Style_ID: {analysis.specific_location}_{pid}_{analysis.confidence_score:.1f}"
#     return prompt


# # ----------------------- OpenAI Images generation -----------------------------
# def generate_square_image_bytes(prompt: str, quality: str = "high") -> bytes:
#     """
#     Generate an image with gpt-image-1.
#     - No response_format parameter (model returns base64)
#     - quality in {low|medium|high|auto}
#     """
#     try:
#         from openai import OpenAI
#     except ImportError:
#         raise RuntimeError("OpenAI library not installed. Run: pip install openai")

#     if not OPENAI_API_KEY:
#         raise RuntimeError("OPENAI_API_KEY is missing. Set it in your environment.")

#     if quality not in {"low", "medium", "high", "auto"}:
#         raise ValueError("quality must be one of: low | medium | high | auto")

#     client = OpenAI(api_key=OPENAI_API_KEY)

#     try:
#         resp = client.images.generate(
#             model=OPENAI_IMAGE_MODEL,
#             prompt=prompt[:1000],
#             size=OPENAI_IMAGE_SIZE,
#             quality=quality,
#             n=1,
#         )

#         d0 = resp.data[0]
#         # Prefer base64 payload
#         b64 = getattr(d0, "b64_json", None)
#         if not b64 and isinstance(d0, dict):
#             b64 = d0.get("b64_json")
#         if b64:
#             import base64
#             return base64.b64decode(b64)

#         # Fallback to URL (future-proofing)
#         url = getattr(d0, "url", None)
#         if not url and isinstance(d0, dict):
#             url = d0.get("url")
#         if url:
#             import requests
#             r = requests.get(url, timeout=30)
#             r.raise_for_status()
#             return r.content

#         raise RuntimeError("Image generation succeeded but returned no bytes.")

#     except Exception as e:
#         print(f"OpenAI API Error: {e}")
#         raise


# # -------------------------- Image post-processing -----------------------------
# def derive_cover_and_hero(
#     base_bytes: bytes,
#     analysis: TitleAnalysis,
#     user_font: str = "",
#     debug_print: bool = False,
#     pid: str = "",
# ) -> Dict[str, bytes]:
#     base = Image.open(BytesIO(base_bytes)).convert("RGB")

#     # Save base
#     out_base = BytesIO()
#     base.save(out_base, format="JPEG", quality=92, optimize=True)
#     base_jpg = out_base.getvalue()

#     # Hero (16:9 crop)
#     tw, th = base.width, base.height
#     target_ratio = HERO_W / HERO_H
#     if (tw / th) >= target_ratio:
#         new_w = int(th * target_ratio)
#         x_offset = (tw - new_w) // 2
#         crop_box = (x_offset, 0, x_offset + new_w, th)
#     else:
#         new_h = int(tw / target_ratio)
#         y_offset = (th - new_h) // 2
#         crop_box = (0, y_offset, tw, y_offset + new_h)
#     hero = base.crop(crop_box).resize((HERO_W, HERO_H), Image.LANCZOS)

#     # Cover (1:1 + title)
#     cover = base.resize((COVER_SIZE, COVER_SIZE), Image.LANCZOS)
#     if debug_print:
#         print(f"   ✂️  AI Short Title: '{analysis.short_title}' (confidence: {analysis.confidence_score:.2f})")
#     cover = draw_text_bottom_left_auto(
#         cover,
#         analysis.short_title,
#         padding=TEXT_PADDING,
#         preferred_font=user_font,
#         max_lines=2,
#     )

#     out_cover, out_hero = BytesIO(), BytesIO()
#     cover.save(out_cover, format="JPEG", quality=90, optimize=True)
#     hero.save(out_hero, format="JPEG", quality=90, optimize=True)

#     return {"base": base_jpg, "cover": out_cover.getvalue(), "hero": out_hero.getvalue()}


# # ----------------------------- Firestore (read) -------------------------------
# def fetch_titles_from_firestore(ids: List[str]) -> Dict[str, str]:
#     try:
#         import firebase_admin
#         from firebase_admin import credentials, firestore
#     except ImportError:
#         raise RuntimeError("Firebase libraries not installed. Run: pip install firebase-admin")

#     if not Path(SERVICE_ACCOUNT_JSON).exists():
#         raise RuntimeError(f"Service account JSON not found: {SERVICE_ACCOUNT_JSON}")

#     if not firebase_admin._apps:
#         cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#         firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})

#     db = firestore.client()
#     titles: Dict[str, str] = {}

#     for doc_id in ids:
#         try:
#             doc = db.collection(COLLECTION).document(doc_id).get()
#             if doc.exists:
#                 data = doc.to_dict() or {}
#                 titles[doc_id] = data.get("title", f"Untitled-{doc_id}")
#             else:
#                 print(f"⚠️  Document {doc_id} not found in collection '{COLLECTION}'")
#                 titles[doc_id] = f"Missing-{doc_id}"
#         except Exception as e:
#             print(f"⚠️  Error fetching {doc_id}: {e}")
#             titles[doc_id] = f"Error-{doc_id}"

#     return titles


# # ---------------------------------- CLI ---------------------------------------
# def main():
#     parser = argparse.ArgumentParser(
#         description="AI-Enhanced playlist image generator (Firestore → GPT-4o-mini → gpt-image-1)"
#     )

#     mode_group = parser.add_mutually_exclusive_group(required=True)
#     mode_group.add_argument("--ids", nargs="+", help="Firestore document IDs to fetch titles for")
#     mode_group.add_argument("--id", help="Manual mode: local ID label (requires --title)")

#     parser.add_argument("--title", help="Manual mode: title text for the given --id")
#     parser.add_argument("--outdir", default=str(OUTDIR), help=f"Output directory (default: {OUTDIR})")
#     parser.add_argument("--font", default="", help="Path to a .ttf/.otf font file for overlay text")
#     parser.add_argument("--print-prompts", action="store_true", help="Print analysis and prompts; save ai_analysis.json")
#     parser.add_argument("--dry-run", action="store_true", help="Analyze only (no image generation)")
#     parser.add_argument("--no-ai-analysis", action="store_true", help="Skip GPT analysis (use rules)")
#     parser.add_argument(
#         "--img-quality",
#         choices=["low", "medium", "high", "auto"],
#         default="high",
#         help="Image generation quality (default: high)",
#     )

#     args = parser.parse_args()

#     if args.id and not args.title:
#         parser.error("Manual mode (--id) requires --title")

#     output_dir = Path(args.outdir)
#     output_dir.mkdir(parents=True, exist_ok=True)

#     # Build ID->title mapping
#     if args.ids:
#         print("🔍 Fetching titles from Firestore...")
#         id_to_title = fetch_titles_from_firestore(args.ids)
#         if not id_to_title:
#             print("❌ No valid documents found. Exiting.")
#             return
#         print(f"✅ Found {len(id_to_title)} documents")
#     else:
#         id_to_title = {args.id: args.title}

#     processed_count = 0
#     duplicate_hashes: set = set()

#     print(f"\n🎨 Processing {len(id_to_title)} playlist(s) with AI-enhanced analysis...")
#     print("=" * 70)

#     for playlist_id, title in id_to_title.items():
#         try:
#             print(f"\n📋 {playlist_id} • Title: {title}")

#             # Step 1: Title analysis
#             if args.no_ai_analysis:
#                 print("   📏 Using rule-based analysis (--no-ai-analysis)...")
#                 region, location = extract_region_hint_fallback(title)
#                 subject, style = guess_subject_and_style_fallback(title)
#                 short_title = shorten_title_intelligently_fallback(title)
#                 analysis = TitleAnalysis(
#                     region=region,
#                     specific_location=location,
#                     main_subject=subject,
#                     photography_style=style,
#                     short_title=short_title,
#                     confidence_score=0.7,
#                 )
#             else:
#                 print("   🧠 Analyzing title with AI...")
#                 analysis = analyze_title_with_ai(title, playlist_id)

#             if args.print_prompts or args.dry_run:
#                 print(f"   🌍 AI Region: {analysis.region} ({analysis.specific_location})")
#                 print(f"   🎯 AI Subject: {analysis.main_subject}")
#                 print(f"   📸 AI Style: {analysis.photography_style}")
#                 print(f"   ✂️  AI Short Title: '{analysis.short_title}'")
#                 print(f"   📊 Confidence: {analysis.confidence_score:.2f}")

#             if args.dry_run:
#                 continue

#             # Step 2: Prompt
#             prompt = build_ai_enhanced_image_prompt(analysis, playlist_id)
#             if args.print_prompts:
#                 print(f"   🤖 AI-Enhanced Prompt: {prompt[:200]}...")

#             # Step 3: Generate base image
#             print("   🎨 Generating AI-guided image...")
#             base_image_bytes = generate_square_image_bytes(prompt, quality=args.img_quality)

#             # Step 4: Deduplicate
#             image_hash = hashlib.sha1(base_image_bytes).hexdigest()
#             if image_hash in duplicate_hashes:
#                 if args.print_prompts:
#                     print("   🔄 Duplicate image detected; regenerating with variation...")
#                 retry_prompt = prompt.replace(
#                     f"Style_ID: {analysis.specific_location}_{playlist_id}_{analysis.confidence_score:.1f}",
#                     f"Style_ID: {analysis.specific_location}_{playlist_id}_retry_{analysis.confidence_score:.1f}",
#                 )
#                 base_image_bytes = generate_square_image_bytes(retry_prompt, quality=args.img_quality)
#                 image_hash = hashlib.sha1(base_image_bytes).hexdigest()
#             duplicate_hashes.add(image_hash)

#             # Step 5: Derive cover/hero
#             print("   🖼️  Creating cover and hero with AI-optimized titles...")
#             processed = derive_cover_and_hero(
#                 base_image_bytes, analysis, user_font=args.font, debug_print=args.print_prompts, pid=playlist_id
#             )

#             # Step 6: Save files
#             playlist_dir = output_dir / str(playlist_id)
#             playlist_dir.mkdir(parents=True, exist_ok=True)

#             (playlist_dir / "base.jpg").write_bytes(processed["base"])
#             (playlist_dir / "cover.jpg").write_bytes(processed["cover"])
#             (playlist_dir / "hero.jpg").write_bytes(processed["hero"])

#             if args.print_prompts:
#                 analysis_file = playlist_dir / "ai_analysis.json"
#                 analysis_data = {
#                     "original_title": title,
#                     "region": analysis.region,
#                     "specific_location": analysis.specific_location,
#                     "main_subject": analysis.main_subject,
#                     "photography_style": analysis.photography_style,
#                     "short_title": analysis.short_title,
#                     "confidence_score": analysis.confidence_score,
#                     "generated_prompt": prompt,
#                     "img_quality": args.img_quality,
#                 }
#                 analysis_file.write_text(json.dumps(analysis_data, indent=2), encoding="utf-8")
#                 print("   📄 AI analysis saved to: ai_analysis.json")

#             print(f"   ✅ Saved to: {playlist_dir}")
#             processed_count += 1

#         except Exception as error:
#             print(f"   ❌ Failed to process {playlist_id}: {error}")
#             import traceback
#             traceback.print_exc()

#     print("\n" + "=" * 70)
#     if args.dry_run:
#         print(f"🔍 Dry run complete! Analyzed {len(id_to_title)} playlist(s)")
#         print("💡 Re-run without --dry-run to generate images")
#     else:
#         print(f"🎉 AI-enhanced processing complete! {processed_count}/{len(id_to_title)} successful")
#         if processed_count > 0:
#             print(f"📁 Output directory: {Path(args.outdir).absolute()}")


# if __name__ == "__main__":
#     main()










# # Working but put any image is not related to title efficiently
# """
# Local trial image generator (NO Firestore writes, NO GCS uploads).

# For each ID:
#   • Fetch playlist title from Firestore (or use --title)
#   • Build an India-anchored, title-aware prompt (with strong negatives + per-ID style token)
#   • Generate one square base (textless) via OpenAI Images: gpt-image-1, 1024x1024
#   • Derive:
#       - base.jpg  (1:1, no text)
#       - cover.jpg (1:1, SHORT text overlay, auto-fitted)
#       - hero.jpg  (16:9, no text)
#   • Save to ./trial_out/<id>/

# Run examples:
#   python generate_playlist_images.py --ids 565 571 568 --print-prompts --font "C:\\Windows\\Fonts\\segoeui.ttf"
#   python generate_playlist_images.py --id demo --title "Wildlife & Rainforest Escapes across India" --font "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
# """

# import os, re, base64, argparse, hashlib, unicodedata
# from io import BytesIO
# from pathlib import Path
# from typing import Dict, List
# from PIL import Image, ImageDraw, ImageFont

# # ---------------------- HARD-CODED CONFIG (edit me) ----------------------
# PROJECT_ID           = "mycasavsc"
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"  # <— set this for Firestore mode
# COLLECTION           = "playlistsNew"

# OPENAI_API_KEY       = "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"           # <— set this
# OPENAI_IMAGE_MODEL   = "gpt-image-1"                            # valid sizes: 1024x1024, 1536x1024, 1024x1536, auto

# OUTDIR               = Path("trial_out")
# COVER_SIZE           = 1200
# HERO_W, HERO_H       = 1920, 1080
# FONT_PATH            = ""  # optional default .ttf; can also pass --font
# TEXT_PADDING         = 48
# # -------------------------------------------------------------------------

# # ----------------------- Font discovery & overlay ------------------------
# FONT_CANDIDATES = [
#     # Windows
#     r"C:\Windows\Fonts\segoeui.ttf",
#     r"C:\Windows\Fonts\arial.ttf",
#     r"C:\Windows\Fonts\calibri.ttf",
#     r"C:\Windows\Fonts\tahoma.ttf",
#     # macOS
#     "/Library/Fonts/Arial.ttf",
#     "/System/Library/Fonts/Supplemental/Arial.ttf",
#     "/System/Library/Fonts/SFNS.ttf",
#     # Linux
#     "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
# ]

# def _pick_font(size: int, preferred: str = "", fallback_cfg: str = "") -> ImageFont.FreeTypeFont:
#     for p in [preferred, fallback_cfg, FONT_PATH, *FONT_CANDIDATES]:
#         if p and os.path.exists(p):
#             try:
#                 return ImageFont.truetype(p, size)
#             except Exception:
#                 pass
#     raise RuntimeError(
#         "No scalable TTF font found. Pass --font path to a .ttf (e.g., C:\\Windows\\Fonts\\segoeui.ttf)."
#     )

# def draw_text_bottom_left_auto(
#     img: Image.Image, text: str, padding: int = 48,
#     preferred_font: str = "", max_lines: int = 2,
#     width_frac: float = 0.86, max_height_frac: float = 0.32
# ) -> Image.Image:
#     if img.mode != "RGBA":
#         img = img.convert("RGBA")
#     W, H = img.size
#     draw = ImageDraw.Draw(img, "RGBA")

#     target_w = W * width_frac
#     target_h = H * max_height_frac
#     size = int(W * 0.14)
#     min_size = max(36, int(W * 0.05))
#     chosen_font, text_block = None, None

#     def wrap_lines(s: str, font: ImageFont.ImageFont) -> str | None:
#         words, lines, line = s.split(), [], ""
#         for w in words:
#             test = (line + " " + w).strip()
#             if draw.textlength(test, font=font) <= target_w:
#                 line = test
#             else:
#                 if line: lines.append(line)
#                 line = w
#                 if len(lines) >= max_lines:
#                     return None
#         if line: lines.append(line)
#         return "\n".join(lines)

#     while size >= min_size:
#         font = _pick_font(size, preferred_font)          # <-- fixed
#         wrapped = wrap_lines(text, font)
#         if wrapped is None:
#             size -= 4
#             continue
#         x0, y0, x1, y1 = draw.multiline_textbbox((0, 0), wrapped, font=font, spacing=int(size * 0.2))
#         block_w, block_h = x1 - x0, y1 - y0
#         if block_w <= target_w and block_h <= target_h:
#             chosen_font, text_block = font, wrapped
#             break
#         size -= 4

#     if chosen_font is None:
#         chosen_font = _pick_font(min_size, preferred_font)   # <-- fixed
#         text_block = wrap_lines(text, chosen_font) or text

#     x = padding
#     _, _, _, block_h = draw.multiline_textbbox((0, 0), text_block, font=chosen_font, spacing=int(chosen_font.size * 0.2))
#     y = H - padding - block_h

#     panel_h = block_h + max(16, padding // 2)
#     overlay = Image.new("RGBA", (W, panel_h), (0, 0, 0, 0))
#     ov = ImageDraw.Draw(overlay)
#     for i in range(panel_h):
#         alpha = int(180 * (i / panel_h))
#         ov.line([(0, i), (W, i)], fill=(0, 0, 0, alpha))
#     img.alpha_composite(overlay, (0, H - panel_h))

#     draw = ImageDraw.Draw(img, "RGBA")
#     draw.multiline_text((x + 2, y + 2), text_block, font=chosen_font, fill=(0, 0, 0, 200), spacing=int(chosen_font.size * 0.2))
#     draw.multiline_text((x, y), text_block, font=chosen_font, fill=(255, 255, 255, 255), spacing=int(chosen_font.size * 0.2))
#     return img.convert("RGB")


# # ----------------------- Short title generator -------------------------
# DROP_SUFFIXES = {"escapes","destinations","getaways","spots","places","trips","adventures"}

# def _clean(text: str) -> str:
#     t = unicodedata.normalize("NFKC", text or "").strip()
#     t = re.sub(r"[\"“”„‟'’ʼ′`´]+", "", t)
#     return t

# def _is_india_token(tok: str) -> bool:
#     return re.fullmatch(r"india(?:s)?", tok.lower()) is not None

# def _filter_tokens(words):
#     stop = {"the","and","of","across","central"}
#     out = []
#     for w in words:
#         wl = w.lower()
#         if _is_india_token(wl) or wl in stop:
#             continue
#         out.append(w)
#     if out and out[-1].lower() in DROP_SUFFIXES:
#         out.pop()
#     return out

# def shorten_title_for_cover(full_title: str) -> str:
#     t = _clean(full_title)
#     t = re.split(r"[:\-–—|]", t, maxsplit=1)[0]  # keep left part
#     if "&" in t:
#         left, right = [s.strip() for s in t.split("&", 1)]
#         lw = _filter_tokens(left.split())
#         rw = _filter_tokens(right.split())
#         left_main  = " ".join(lw[:2]) if lw else ""
#         right_main = " ".join(rw[:2]) if rw else ""
#         short = f"{left_main} & {right_main}".strip(" &")
#         if short:
#             return short
#     words = _filter_tokens(t.split())
#     return (" ".join(words[:3]) if words else t).strip()

# # ----------------------- Prompt builders (India-anchored) --------------
# def extract_region_hint(title: str) -> str:
#     t = (title or "").lower()
#     if "central india" in t: return "Central India"
#     if any(x in t for x in ["rajasthan","jaipur","udaipur"]): return "Rajasthan, India"
#     if "kerala" in t or "backwater" in t: return "Kerala, India"
#     if any(x in t for x in ["ladakh","kashmir","himalaya"]): return "Indian Himalayas"
#     if any(x in t for x in ["goa","konkan"]): return "Western Coast of India"
#     if "sundarbans" in t: return "Sundarbans, India"
#     if any(x in t for x in ["tamil","madurai","tanjore","thanjavur","thanjava"]): return "Tamil Nadu, India"
#     if any(x in t for x in ["karnataka","hampi"]): return "Karnataka, India"
#     if any(x in t for x in ["maharashtra","mumbai","pune"]): return "Maharashtra, India"
#     if "gujarat" in t: return "Gujarat, India"
#     if any(x in t for x in ["assam","meghalaya","northeast"]): return "Northeast India"
#     return "India"

# def guess_subject_hint(title: str) -> str:
#     t = (title or "").lower()
#     if any(k in t for k in ["palace","regal","royal"]):
#         return "a Rajasthani sandstone palace dome (chhatri) with carved arches and jharokha balconies"
#     if any(k in t for k in ["fort","citadel"]):
#         return "a weathered Indian hill fort bastion with crenellations and massive stone walls"
#     if any(k in t for k in ["historic","ruin","ancient","temple","stupa"]):
#         return "an ancient Indian temple entrance with intricate stone carvings, inspired by Khajuraho or Sanchi"
#     if any(k in t for k in ["wildlife","rainforest","jungle"]):
#         return "a Bengal tiger in lush rainforest foliage with shallow depth of field"
#     if any(k in t for k in ["desert","dune","thar"]):
#         return "sweeping golden sand dunes with a lone camel silhouette at sunset"
#     if any(k in t for k in ["beach","coast","island"]):
#         return "a serene tropical Indian beach with palm silhouettes and gentle surf at golden hour"
#     if any(k in t for k in ["backwater","houseboat"]):
#         return "a Kerala backwaters scene with a traditional kettuvallam houseboat and coconut palms"
#     if any(k in t for k in ["tea","plantation","munnar"]):
#         return "a terraced tea estate on rolling hills under soft morning mist in Munnar"
#     if any(k in t for k in ["mountain","himalaya","valley"]):
#         return "snow-capped Himalayan peaks with a clean sky and gentle side-light"
#     return "a striking Indian landmark or landscape that clearly belongs to India"

# def build_image_prompt(theme: str, pid: str) -> str:
#     region  = extract_region_hint(theme)
#     subject = guess_subject_hint(theme)
#     negatives = ("no Eiffel Tower, no Paris, no European or Western landmarks, "
#                  "no text, no logos, no watermarks, no people, no signage, no city skyline")
#     return (f"A minimalistic, cinematic photograph set in {region}. "
#             f"Show {subject} in warm natural light with a softly blurred background; "
#             "balanced composition, modern travel-mag aesthetic, high detail, uncluttered, 1 focal subject. "
#             f"Negative prompts: {negatives}. Unique style token: {pid}.")

# # ----------------------- OpenAI call ------------------------
# def generate_square_image_bytes(prompt: str) -> bytes:
#     from openai import OpenAI
#     if not OPENAI_API_KEY or "PUT_YOUR_OPENAI_API_KEY_HERE" in OPENAI_API_KEY:
#         raise RuntimeError("Please set OPENAI_API_KEY in this file.")
#     client = OpenAI(api_key=OPENAI_API_KEY)
#     resp = client.images.generate(model=OPENAI_IMAGE_MODEL, prompt=prompt, size="1024x1024")
#     return base64.b64decode(resp.data[0].b64_json)

# # ----------------------- Compose outputs --------------------
# def derive_cover_and_hero(base_bytes: bytes, title_for_text: str, user_font: str = "",
#                           debug_print: bool = False, pid: str = "") -> Dict[str, bytes]:
#     base = Image.open(BytesIO(base_bytes)).convert("RGB")

#     out_base = BytesIO(); base.save(out_base, format="JPEG", quality=92)
#     base_jpg = out_base.getvalue()

#     # hero (16:9 crop)
#     tw, th = base.width, base.height
#     target = HERO_W / HERO_H
#     if (tw / th) >= target:
#         new_w = int(th * target); x0 = (tw - new_w) // 2; crop = (x0, 0, x0 + new_w, th)
#     else:
#         new_h = int(tw / target); y0 = (th - new_h) // 2; crop = (0, y0, tw, y0 + new_h)
#     hero = base.crop(crop).resize((HERO_W, HERO_H), Image.LANCZOS)

#     # cover (1:1 + overlay)
#     cover = base.resize((COVER_SIZE, COVER_SIZE), Image.LANCZOS)
#     short = shorten_title_for_cover(title_for_text)
#     if debug_print:
#         print(f"[overlay for {pid}] {short!r}")
#     cover = draw_text_bottom_left_auto(cover, short, padding=TEXT_PADDING, preferred_font=user_font)

#     out_cover = BytesIO(); cover.save(out_cover, format="JPEG", quality=90)
#     out_hero  = BytesIO(); hero.save(out_hero,  format="JPEG", quality=90)
#     return {"base": base_jpg, "cover": out_cover.getvalue(), "hero": out_hero.getvalue()}

# # ----------------------- Firestore (read-only) --------------
# def fetch_titles_from_firestore(ids: List[str]) -> Dict[str, str]:
#     import firebase_admin
#     from firebase_admin import credentials, firestore
#     if not Path(SERVICE_ACCOUNT_JSON).exists():
#         raise RuntimeError(f"Service account JSON not found: {SERVICE_ACCOUNT_JSON}")
#     if not firebase_admin._apps:
#         cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#         firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})
#     db = firestore.client()
#     out: Dict[str, str] = {}
#     for pid in ids:
#         d = db.collection(COLLECTION).document(pid).get()
#         if d.exists:
#             out[pid] = (d.to_dict() or {}).get("title") or "Untitled"
#         else:
#             print(f"⚠️  Doc {pid} not found in '{COLLECTION}'")
#     return out

# # ------------------------------ CLI ------------------------------------
# def main():
#     ap = argparse.ArgumentParser(description="Local-only playlist image generator (no DB writes).")
#     mode = ap.add_mutually_exclusive_group(required=True)
#     mode.add_argument("--ids", nargs="+", help="Firestore doc IDs to fetch titles for (read-only).")
#     mode.add_argument("--id", help="Manual mode: local ID label (requires --title).")
#     ap.add_argument("--title", help="Manual mode: title for the given --id.")
#     ap.add_argument("--outdir", default=str(OUTDIR), help="Output dir (default: ./trial_out)")
#     ap.add_argument("--font", default="", help="Path to a .ttf/.otf font for overlay text")
#     ap.add_argument("--print-prompts", dest="print_prompts", action="store_true", help="Print prompts & overlays")
#     args = ap.parse_args()

#     outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

#     # Build {id: title}
#     id_title: Dict[str, str] = {}
#     if args.ids:
#         id_title = fetch_titles_from_firestore(args.ids)
#         if not id_title:
#             print("No valid documents found; nothing to do.")
#             return
#     else:
#         if not args.title:
#             raise RuntimeError("Manual mode requires --id and --title.")
#         id_title = {args.id: args.title}

#     seen_hashes: set = set()
#     for pid, title in id_title.items():
#         try:
#             # Print the playlist title for this ID (requested behavior)
#             print(f"{pid} • Title: {title}")

#             prompt = build_image_prompt(title, pid=str(pid))
#             if args.print_prompts:
#                 print(f"[prompt for {pid}] {prompt}")

#             base_bytes = generate_square_image_bytes(prompt)

#             # intra-run duplicate guard
#             h = hashlib.sha1(base_bytes).hexdigest()
#             if h in seen_hashes:
#                 prompt2 = prompt.replace(f"Unique style token: {pid}.", f"Unique style token: {pid}-v2.")
#                 if args.print_prompts:
#                     print(f"[duplicate detected for {pid}] retrying with token {pid}-v2")
#                 base_bytes = generate_square_image_bytes(prompt2)
#                 h = hashlib.sha1(base_bytes).hexdigest()
#             seen_hashes.add(h)

#             parts = derive_cover_and_hero(base_bytes, title, user_font=args.font,
#                                           debug_print=args.print_prompts, pid=str(pid))

#             pdir = outdir / str(pid); pdir.mkdir(parents=True, exist_ok=True)
#             (pdir / "base.jpg").write_bytes(parts["base"])
#             (pdir / "cover.jpg").write_bytes(parts["cover"])
#             (pdir / "hero.jpg").write_bytes(parts["hero"])
#             print(f"✓ Saved -> {pdir}\\base.jpg, cover.jpg, hero.jpg\n")
#         except Exception as e:
#             print(f"⚠️  Failed {pid}: {e}\n")

# if __name__ == "__main__":
#     main()









# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# """
# Local trial image generator (NO Firestore writes, NO GCS uploads).

# For each Firestore doc ID in 'playlistsNew':
#   1) Build a region-anchored, title-aware prompt with strong negatives + per-ID style token
#   2) Generate ONE base square (textless) via OpenAI Images (gpt-image-1, 1024x1024)
#   3) Derive:
#        - cover.jpg  (1:1, with short text overlay)
#        - hero.jpg   (16:9, no text)
#   4) Save to ./trial_out/{id}/

# Usage:
#   python generate_playlist_images_local.py --ids 565 571 568
#   python generate_playlist_images_local.py --ids 565 --print-prompts
#   # Manual (no Firestore):
#   python generate_playlist_images_local.py --id 999 --title "India’s Regal Palaces: Timeless Royalty"

# Deps:
#   pip install pillow firebase-admin openai
# """

# import re, base64, argparse, hashlib
# from io import BytesIO
# from pathlib import Path
# from typing import Dict, List, Optional

# from PIL import Image, ImageDraw, ImageFont

# # ---------------------- HARD-CODED CONFIG (edit me) ----------------------
# PROJECT_ID           = "mycasavsc"
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# COLLECTION           = "playlistsNew"

# OPENAI_API_KEY       = "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"   # <--- set your key
# OPENAI_IMAGE_MODEL   = "gpt-image-1"                    # valid sizes: 1024x1024, 1024x1536, 1536x1024, auto

# OUTDIR               = Path("trial_out")
# COVER_SIZE           = 1200
# HERO_W, HERO_H       = 1920, 1080
# FONT_PATH            = ""   # optional .ttf (e.g., r"C:\fonts\Montserrat-SemiBold.ttf")
# TEXT_PADDING         = 48
# # ------------------------------------------------------------------------


# # ----------------------------- Prompting --------------------------------
# def shorten_title_for_cover(full_title: str) -> str:
#     t = re.sub(r"[“”\"'’]", "", (full_title or "").strip())
#     t = re.split(r"[:\-–|]", t, maxsplit=1)[0]
#     keep = []
#     for w in re.split(r"\s+", t):
#         wl = w.lower()
#         if wl in {"central", "across", "india", "indias", "the", "and", "of"}:
#             continue
#         keep.append(w)
#         if len(keep) >= 4:
#             break
#     return (" ".join(keep) or t).strip()

# def extract_region_hint(title: str) -> str:
#     t = (title or "").lower()
#     if "central india" in t:                         return "Central India"
#     if any(x in t for x in ["rajasthan", "jaipur", "udaipur"]): return "Rajasthan, India"
#     if "kerala" in t or "backwater" in t:           return "Kerala, India"
#     if any(x in t for x in ["ladakh", "kashmir", "himalaya"]):  return "Indian Himalayas"
#     if any(x in t for x in ["goa", "konkan"]):      return "Western Coast of India"
#     if "sundarbans" in t:                           return "Sundarbans, India"
#     if any(x in t for x in ["tamil", "madurai", "tanjore", "thanjavur", "thanjava"]): return "Tamil Nadu, India"
#     if any(x in t for x in ["karnataka", "hampi"]): return "Karnataka, India"
#     if any(x in t for x in ["maharashtra", "mumbai", "pune"]):  return "Maharashtra, India"
#     if "gujarat" in t:                              return "Gujarat, India"
#     if any(x in t for x in ["assam", "meghalaya", "northeast"]):return "Northeast India"
#     return "India"

# def guess_subject_hint(title: str) -> str:
#     t = (title or "").lower()
#     # Architecture / history
#     if any(k in t for k in ["palace", "regal", "royal"]):
#         return "a Rajasthani sandstone palace dome (chhatri) with carved arches and jharokha balconies"
#     if any(k in t for k in ["fort", "citadel"]):
#         return "a weathered Indian hill fort bastion with crenellations and massive stone walls"
#     if any(k in t for k in ["historic", "ruin", "ancient", "temple", "stupa"]):
#         return "an ancient Indian temple entrance with intricate stone carvings, inspired by Khajuraho or Sanchi"
#     # Nature / wildlife
#     if any(k in t for k in ["wildlife", "rainforest", "jungle"]):
#         return "a Bengal tiger in lush rainforest foliage with shallow depth of field"
#     if any(k in t for k in ["desert", "dune", "thar"]):
#         return "sweeping golden sand dunes with a lone camel silhouette at sunset"
#     if any(k in t for k in ["beach", "coast", "island"]):
#         return "a serene tropical Indian beach with palm silhouettes and gentle surf at golden hour"
#     if any(k in t for k in ["backwater", "houseboat"]):
#         return "a Kerala backwaters scene with a traditional kettuvallam houseboat and coconut palms"
#     if any(k in t for k in ["tea", "plantation", "munnar"]):
#         return "a terraced tea estate on rolling hills under soft morning mist in Munnar"
#     if any(k in t for k in ["mountain", "himalaya", "valley"]):
#         return "snow-capped Himalayan peaks with a clean sky and gentle side-light"
#     return "a striking Indian landmark or landscape that clearly belongs to India"

# def build_image_prompt(theme: str, pid: str) -> str:
#     region  = extract_region_hint(theme)
#     subject = guess_subject_hint(theme)
#     negatives = (
#         "no Eiffel Tower, no Paris, no European or Western landmarks, "
#         "no text, no logos, no watermarks, no people, no signage, no city skyline"
#     )
#     return (
#         f"A minimalistic, cinematic photograph set in {region}. "
#         f"Show {subject} in warm natural light with a softly blurred background; "
#         "balanced composition, modern travel-mag aesthetic, high detail, uncluttered, 1 focal subject. "
#         f"Negative prompts: {negatives}. "
#         f"Unique style token: {pid}."
#     )

# # ----------------------- OpenAI provider hook -------------------------
# def generate_square_image_bytes(prompt: str) -> bytes:
#     from openai import OpenAI
#     if not OPENAI_API_KEY or "PUT_YOUR_OPENAI_API_KEY_HERE" in OPENAI_API_KEY:
#         raise RuntimeError("Please set OPENAI_API_KEY in this file.")
#     client = OpenAI(api_key=OPENAI_API_KEY)
#     # Force valid size for gpt-image-1
#     resp = client.images.generate(model=OPENAI_IMAGE_MODEL, prompt=prompt, size="1024x1024")
#     return base64.b64decode(resp.data[0].b64_json)

# # ----------------------------- Drawing --------------------------------
# def draw_text_bottom_left(img: Image.Image, text: str, padding: int = TEXT_PADDING) -> Image.Image:
#     draw = ImageDraw.Draw(img)
#     fsize = max(36, img.width // 18)
#     if FONT_PATH:
#         try:
#             font = ImageFont.truetype(FONT_PATH, fsize)
#         except Exception:
#             font = ImageFont.load_default()
#     else:
#         font = ImageFont.load_default()

#     # wrap
#     words = text.split()
#     lines, line = [], ""
#     for w in words:
#         test = (line + " " + w).strip()
#         if draw.textlength(test, font=font) < img.width * 0.85:
#             line = test
#         else:
#             lines.append(line); line = w
#     if line: lines.append(line)
#     txt = "\n".join(lines)

#     _, _, _, h = draw.multiline_textbbox((0,0), txt, font=font, spacing=int(fsize*0.25))
#     x = padding
#     y = img.height - padding - h
#     draw.multiline_text((x+2, y+2), txt, font=font, fill=(0,0,0,160), spacing=int(fsize*0.25))
#     draw.multiline_text((x, y), txt, font=font, fill=(255,255,255,255), spacing=int(fsize*0.25))
#     return img

# def derive_cover_and_hero(base_bytes: bytes, title_for_text: str) -> Dict[str, bytes]:
#     base = Image.open(BytesIO(base_bytes)).convert("RGB")

#     # base.jpg
#     out_base = BytesIO(); base.save(out_base, format="JPEG", quality=92)
#     base_jpg = out_base.getvalue()

#     # hero 16:9
#     tw, th = base.width, base.height
#     target_ratio = HERO_W / HERO_H
#     if (tw / th) >= target_ratio:
#         new_w = int(th * target_ratio); x0 = (tw - new_w) // 2; crop = (x0, 0, x0 + new_w, th)
#     else:
#         new_h = int(tw / target_ratio); y0 = (th - new_h) // 2; crop = (0, y0, tw, y0 + new_h)
#     hero = base.crop(crop).resize((HERO_W, HERO_H), Image.LANCZOS)

#     # cover 1:1 with short overlay
#     cover = base.resize((COVER_SIZE, COVER_SIZE), Image.LANCZOS)
#     cover = draw_text_bottom_left(cover, shorten_title_for_cover(title_for_text))

#     out_cover = BytesIO(); cover.save(out_cover, format="JPEG", quality=90)
#     out_hero  = BytesIO(); hero.save(out_hero,  format="JPEG", quality=90)
#     return {"base": base_jpg, "cover": out_cover.getvalue(), "hero": out_hero.getvalue()}

# # ----------------------------- Firestore (read-only) ------------------
# def fetch_titles_from_firestore(ids: List[str]) -> Dict[str, str]:
#     import firebase_admin
#     from firebase_admin import credentials, firestore
#     if not Path(SERVICE_ACCOUNT_JSON).exists():
#         raise RuntimeError(f"Service account JSON not found: {SERVICE_ACCOUNT_JSON}")
#     if not firebase_admin._apps:
#         cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#         firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})
#     db = firestore.client()

#     out = {}
#     for pid in ids:
#         d = db.collection(COLLECTION).document(pid).get()
#         if d.exists:
#             data = d.to_dict() or {}
#             out[pid] = data.get("title") or "Untitled"
#         else:
#             print(f"⚠️  Doc {pid} not found in '{COLLECTION}'")
#     return out

# # --------------------------------- CLI --------------------------------
# def main():
#     ap = argparse.ArgumentParser(description="Local-only playlist image generator (no DB writes).")
#     mode = ap.add_mutually_exclusive_group(required=True)
#     mode.add_argument("--ids", nargs="+", help="Firestore doc IDs to fetch titles for (read-only).")
#     mode.add_argument("--id", help="Manual mode: local ID label (requires --title).")
#     ap.add_argument("--title", help="Manual mode: title for the given --id.")
#     ap.add_argument("--outdir", default=str(OUTDIR), help="Output dir (default: ./trial_out)")
#     ap.add_argument("--print-prompts", action="store_true", help="Print exact prompts for debugging")
#     args = ap.parse_args()

#     outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

#     # Build {id: title}
#     id_title: Dict[str, str] = {}
#     if args.ids:
#         id_title = fetch_titles_from_firestore(args.ids)
#         if not id_title:
#             print("No valid documents found; nothing to do."); return
#     else:
#         if not args.title:
#             raise RuntimeError("Manual mode requires --id and --title.")
#         id_title = {args.id: args.title}

#     seen_hashes: set = set()
#     for pid, title in id_title.items():
#         try:
#             prompt = build_image_prompt(title, pid=str(pid))
#             if args.print_prompts:
#                 print(f"[prompt for {pid}] {prompt}")

#             base_bytes = generate_square_image_bytes(prompt)

#             # duplicate guard within this run
#             h = hashlib.sha1(base_bytes).hexdigest()
#             if h in seen_hashes:
#                 prompt2 = prompt.replace(f"Unique style token: {pid}.", f"Unique style token: {pid}-v2.")
#                 if args.print_prompts:
#                     print(f"[duplicate detected for {pid}] retrying with token {pid}-v2")
#                 base_bytes = generate_square_image_bytes(prompt2)
#                 h = hashlib.sha1(base_bytes).hexdigest()
#             seen_hashes.add(h)

#             parts = derive_cover_and_hero(base_bytes, title)

#             pdir = outdir / str(pid); pdir.mkdir(parents=True, exist_ok=True)
#             (pdir / "base.jpg").write_bytes(parts["base"])
#             (pdir / "cover.jpg").write_bytes(parts["cover"])
#             (pdir / "hero.jpg").write_bytes(parts["hero"])
#             print(f"✓ {pid} -> {pdir}  (base.jpg, cover.jpg, hero.jpg)")
#         except Exception as e:
#             print(f"⚠️  Failed {pid}: {e}")

# if __name__ == "__main__":
#     main()



# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# """
# Trial generator (local only):
# - Input: Firestore doc IDs (collection 'playlistsNew') OR manual --title
# - Output (local only, no DB writes, no GCS):
#     ./trial_out/{id}/base.jpg   (square, no text)
#     ./trial_out/{id}/cover.jpg  (1:1 with short text)
#     ./trial_out/{id}/hero.jpg   (16:9 no text)

# Usage:
#   # Firestore mode (read-only):
#   python trial_generate_images_local.py --ids 123 456

#   # Manual mode (no Firestore needed):
#   python trial_generate_images_local.py --id 999 --title "Central India's Historic Sites & Ancient Ruins"
# """

# import os, re, base64, argparse
# from io import BytesIO
# from pathlib import Path
# from typing import Any, Dict, List, Optional

# from PIL import Image, ImageDraw, ImageFont

# # ---------------------- HARD-CODED CONFIG (EDIT ME) ----------------------
# OPENAI_API_KEY       = "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"  # <-- EDIT ME
# SERVICE_ACCOUNT_JSON = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"  # <-- EDIT ME if using Firestore mode

# # Firestore collection (read-only)
# PROJECT_ID           = "mycasavsc"
# COLLECTION           = "playlistsNew"

# # Image generation model/size
# OPENAI_IMAGE_MODEL   = "gpt-image-1"
# BASE_GEN_SIZE        = 1024  # square generation

# # Outputs
# OUTDIR               = Path("trial_out")
# COVER_SIZE           = 1200
# HERO_W, HERO_H       = 1920, 1080

# # Text rendering
# FONT_PATH            = ""   # optional .ttf, e.g., r"C:\fonts\Montserrat-SemiBold.ttf"
# TEXT_PADDING         = 48
# # ------------------------------------------------------------------------


# # ----------------------------- Helpers ---------------------------------
# def shorten_title_for_cover(full_title: str) -> str:
#     """Short, punchy overlay (max ~4 strong words)."""
#     t = re.sub(r"[“”\"'’]", "", (full_title or "").strip())
#     t = re.split(r"[:\-–|]", t, maxsplit=1)[0]
#     keep = []
#     for w in re.split(r"\s+", t):
#         if w.lower() in {"central", "across", "india", "indias", "the", "and", "of"}:
#             continue
#         keep.append(w)
#         if len(keep) >= 4:
#             break
#     out = " ".join(keep) or t
#     return out.strip()

# def guess_subject_hint(title: str) -> str:
#     t = (title or "").lower()
#     if "palace" in t or "royal" in t:
#         return "an ornate sandstone palace dome (chhatri) at golden hour"
#     if "ruin" in t or "historic" in t or "temple" in t:
#         return "an ancient stone temple entrance with intricate carvings"
#     if "wildlife" in t or "rainforest" in t or "jungle" in t:
#         return "a Bengal tiger in lush rainforest, shallow depth of field"
#     if "beach" in t or "coast" in t:
#         return "a serene coastal cliff and sea at golden hour"
#     return "a striking landmark scene that fits the theme"

# def build_image_prompt(theme: str) -> str:
#     subject = guess_subject_hint(theme)
#     # Base image is textless; we add overlay locally for the cover
#     return (
#         "A minimalistic, cinematic photograph that captures the essence of the theme. "
#         f"Show {subject} in warm natural light with a softly blurred background, "
#         "balanced composition, modern travel magazine aesthetic, high detail, no people, no watermark, no text."
#     )

# # ----------------------- OpenAI provider hook -------------------------
# def generate_square_image_bytes(prompt: str, size_px: int = BASE_GEN_SIZE) -> bytes:
#     from openai import OpenAI
#     if not OPENAI_API_KEY or "PUT_YOUR_OPENAI_API_KEY_HERE" in OPENAI_API_KEY:
#         raise RuntimeError("Please set OPENAI_API_KEY in this script.")
#     client = OpenAI(api_key=OPENAI_API_KEY)

#     # gpt-image-1 allowed sizes
#     size_str = "1024x1024"  # force a valid, square size
#     resp = client.images.generate(
#         model=OPENAI_IMAGE_MODEL,
#         prompt=prompt,
#         size=size_str
#     )
#     b64 = resp.data[0].b64_json
#     return base64.b64decode(b64)


# # ----------------------------- Drawing --------------------------------
# def draw_text_bottom_left(img: Image.Image, text: str, padding: int = TEXT_PADDING) -> Image.Image:
#     draw = ImageDraw.Draw(img)
#     fsize = max(36, img.width // 18)
#     if FONT_PATH:
#         try:
#             font = ImageFont.truetype(FONT_PATH, fsize)
#         except Exception:
#             font = ImageFont.load_default()
#     else:
#         font = ImageFont.load_default()

#     # simple wrap
#     words = text.split()
#     lines, line = [], ""
#     for w in words:
#         test = (line + " " + w).strip()
#         if draw.textlength(test, font=font) < img.width * 0.85:
#             line = test
#         else:
#             lines.append(line); line = w
#     if line: lines.append(line)
#     txt = "\n".join(lines)

#     # measure height
#     _, _, _, h = draw.multiline_textbbox((0,0), txt, font=font, spacing=int(fsize*0.25))
#     x = padding
#     y = img.height - padding - h

#     # shadow
#     draw.multiline_text((x+2, y+2), txt, font=font, fill=(0,0,0,160), spacing=int(fsize*0.25))
#     # white text
#     draw.multiline_text((x, y), txt, font=font, fill=(255,255,255,255), spacing=int(fsize*0.25))
#     return img

# def derive_cover_and_hero(base_bytes: bytes, title_for_text: str) -> Dict[str, bytes]:
#     base = Image.open(BytesIO(base_bytes)).convert("RGB")

#     # Save base (optional)
#     out_base = BytesIO(); base.save(out_base, format="JPEG", quality=92)
#     base_bytes_jpg = out_base.getvalue()

#     # Hero 16:9 (center crop)
#     tw, th = base.width, base.height
#     target_ratio = HERO_W / HERO_H
#     if (tw / th) >= target_ratio:
#         new_w = int(th * target_ratio)
#         x0 = (tw - new_w) // 2
#         crop = (x0, 0, x0 + new_w, th)
#     else:
#         new_h = int(tw / target_ratio)
#         y0 = (th - new_h) // 2
#         crop = (0, y0, tw, y0 + new_h)
#     hero = base.crop(crop).resize((HERO_W, HERO_H), Image.LANCZOS)

#     # Cover 1:1 with short overlay text
#     cover = base.resize((COVER_SIZE, COVER_SIZE), Image.LANCZOS)
#     cover = draw_text_bottom_left(cover, shorten_title_for_cover(title_for_text))

#     out_cover = BytesIO(); cover.save(out_cover, format="JPEG", quality=90)
#     out_hero  = BytesIO(); hero.save(out_hero,  format="JPEG", quality=90)
#     return {"base": base_bytes_jpg, "cover": out_cover.getvalue(), "hero": out_hero.getvalue()}

# # ----------------------------- Firestore (read-only) ------------------
# def fetch_titles_from_firestore(ids: List[str]) -> Dict[str, str]:
#     """Return {id: title}. Missing docs are skipped."""
#     try:
#         import firebase_admin
#         from firebase_admin import credentials, firestore
#     except Exception:
#         raise RuntimeError("firebase-admin not installed. pip install firebase-admin")

#     if not Path(SERVICE_ACCOUNT_JSON).exists():
#         raise RuntimeError(f"Service account JSON not found: {SERVICE_ACCOUNT_JSON}")

#     if not firebase_admin._apps:
#         cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#         firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})

#     db = firestore.client()
#     out = {}
#     for pid in ids:
#         d = db.collection(COLLECTION).document(pid).get()
#         if d.exists:
#             data = d.to_dict() or {}
#             title = data.get("title") or "Untitled"
#             out[pid] = title
#         else:
#             print(f"⚠️  Doc {pid} not found in '{COLLECTION}'")
#     return out

# # --------------------------------- CLI --------------------------------
# def main():
#     global OPENAI_IMAGE_MODEL
#     ap = argparse.ArgumentParser(description="Trial: generate local cover/hero from playlist title(s). No DB writes.")
#     g = ap.add_mutually_exclusive_group(required=True)
#     g.add_argument("--ids", nargs="+", help="Firestore doc IDs to fetch titles for (read-only).")
#     g.add_argument("--id", help="Single local ID label for manual mode (requires --title).")
#     ap.add_argument("--title", help="Manual mode: title for the given --id (skips Firestore).")
#     ap.add_argument("--outdir", default=str(OUTDIR), help="Output directory (default: ./trial_out)")
#     ap.add_argument("--model", default=OPENAI_IMAGE_MODEL, help="OpenAI image model (default: gpt-image-1)")
#     args = ap.parse_args()
#     OPENAI_IMAGE_MODEL = args.model

#     outdir = Path(args.outdir)
#     outdir.mkdir(parents=True, exist_ok=True)

#     # Resolve {id: title}
#     id_title_map: Dict[str, str] = {}
#     if args.ids:
#         id_title_map = fetch_titles_from_firestore(args.ids)
#         if not id_title_map:
#             print("No valid documents found; nothing to do.")
#             return
#     else:
#         if not args.title:
#             raise RuntimeError("Manual mode requires --id and --title.")
#         id_title_map = {args.id: args.title}

#     # Process each
#     for pid, title in id_title_map.items():
#         try:
#             prompt = build_image_prompt(title)
#             base_bytes = generate_square_image_bytes(prompt, BASE_GEN_SIZE)
#             parts = derive_cover_and_hero(base_bytes, title)

#             # write locally
#             pdir = outdir / str(pid)
#             pdir.mkdir(parents=True, exist_ok=True)
#             (pdir / "base.jpg").write_bytes(parts["base"])
#             (pdir / "cover.jpg").write_bytes(parts["cover"])
#             (pdir / "hero.jpg").write_bytes(parts["hero"])
#             print(f"✓ {pid} -> {pdir}  (base.jpg, cover.jpg, hero.jpg)")
#         except Exception as e:
#             print(f"⚠️  Failed {pid}: {e}")

# if __name__ == "__main__":
#     main()




# #!/usr/bin/env python3
# import os, re, time, json, base64, argparse
# from io import BytesIO
# from typing import Dict, Any, Optional, List

# from PIL import Image, ImageDraw, ImageFont

# import firebase_admin
# from firebase_admin import credentials, firestore, storage

# # ----------------------------- CONFIG --------------------------------
# PROJECT_ID              = os.getenv("FIREBASE_PROJECT_ID", "mycasavsc")
# BUCKET_NAME             = os.getenv("GCS_BUCKET", "mycasavsc.appspot.com")
# SERVICE_ACCOUNT_JSON    = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
# COLLECTION              = os.getenv("PLAYLISTS_COLLECTION", "playlistsNew")

# # Image generation provider (OpenAI example; swap with SD/etc.)
# OPENAI_API_KEY          = os.getenv("OPENAI_API_KEY", "")
# OPENAI_IMAGE_MODEL      = os.getenv("OPENAI_IMAGE_MODEL", "gpt-image-1")

# # Output sizes
# COVER_SIZE              = 1200         # square
# HERO_W, HERO_H          = 1920, 1080   # 16:9
# BASE_GEN_SIZE           = 2048         # generate square this size

# # Rendering
# FONT_PATH               = os.getenv("COVER_FONT_PATH", "")  # optional .ttf like Montserrat-SemiBold.ttf
# TEXT_PADDING            = 48
# SKIP_EXISTING           = True         # don't overwrite unless --force

# # -------------------------- FIREBASE INIT -----------------------------
# def init_firebase():
#     if not firebase_admin._apps:
#         cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#         firebase_admin.initialize_app(cred, {
#             "projectId": PROJECT_ID,
#             "storageBucket": BUCKET_NAME
#         })
#     db = firestore.client()
#     bucket = storage.bucket(BUCKET_NAME)
#     return db, bucket

# # ----------------------------- UTILS ---------------------------------
# def shorten_title_for_cover(full_title: str) -> str:
#     t = re.sub(r"[“”\"'’]", "", (full_title or "").strip())
#     t = re.split(r"[:\-–|]", t, maxsplit=1)[0]  # left of colon/dash
#     keep, out = [], []
#     for w in re.split(r"\s+", t):
#         if w.lower() in {"central", "across", "india", "indias", "the", "and", "of"}:
#             continue
#         keep.append(w)
#         if len(keep) >= 4:
#             break
#     out = " ".join(keep) or t
#     return out.strip()

# def guess_subject_hint(title: str) -> str:
#     t = (title or "").lower()
#     if "palace" in t or "royal" in t:
#         return "an ornate sandstone palace dome (chhatri) at golden hour"
#     if "ruin" in t or "historic" in t or "temple" in t:
#         return "an ancient stone temple entrance with intricate carvings"
#     if "wildlife" in t or "rainforest" in t or "jungle" in t:
#         return "a Bengal tiger in lush rainforest, shallow depth of field"
#     if "beach" in t or "coast" in t:
#         return "a serene coastal cliff and sea at golden hour"
#     return "a striking landmark scene that fits the theme"

# def build_image_prompt(theme: str, subject_hint: Optional[str] = None, want_text: bool = False) -> str:
#     short = shorten_title_for_cover(theme)
#     subject = subject_hint or guess_subject_hint(theme)
#     base = (
#         "A minimalistic, cinematic photograph that captures the essence of the theme. "
#         f"Show {subject} in natural warm light with a softly blurred background, "
#         "balanced composition, modern travel magazine aesthetic, high detail, no people, no watermark."
#     )
#     if want_text:
#         base += f' Add clean white overlay text at the bottom: "{short}".'
#     else:
#         base += " No text, no caption."
#     return base

# # ------------------------ PROVIDER (OPENAI) --------------------------
# def fetch_generated_image_bytes_openai(prompt: str, size_px: int = BASE_GEN_SIZE) -> bytes:
#     from openai import OpenAI
#     client = OpenAI(api_key=OPENAI_API_KEY)
#     size_str = f"{size_px}x{size_px}"
#     resp = client.images.generate(model=OPENAI_IMAGE_MODEL, prompt=prompt, size=size_str)
#     return base64.b64decode(resp.data[0].b64_json)

# # ------------------------------ DRAW ---------------------------------
# def draw_text_bottom_left(img: Image.Image, text: str, padding: int = TEXT_PADDING) -> Image.Image:
#     draw = ImageDraw.Draw(img)
#     fsize = max(36, img.width // 18)
#     if FONT_PATH:
#         try:
#             font = ImageFont.truetype(FONT_PATH, fsize)
#         except Exception:
#             font = ImageFont.load_default()
#     else:
#         font = ImageFont.load_default()

#     words = text.split()
#     lines, line = [], ""
#     for w in words:
#         test = (line + " " + w).strip()
#         if draw.textlength(test, font=font) < img.width * 0.85:
#             line = test
#         else:
#             lines.append(line); line = w
#     if line: lines.append(line)
#     txt = "\n".join(lines)

#     _, _, _, h = draw.multiline_textbbox((0,0), txt, font=font, spacing=int(fsize*0.25))
#     x = padding
#     y = img.height - padding - h

#     draw.multiline_text((x+2, y+2), txt, font=font, fill=(0,0,0,160), spacing=int(fsize*0.25))
#     draw.multiline_text((x, y),       txt, font=font, fill=(255,255,255,255), spacing=int(fsize*0.25))
#     return img

# def derive_images(base_bytes: bytes, title_for_text: str) -> Dict[str, bytes]:
#     base = Image.open(BytesIO(base_bytes)).convert("RGB")

#     # hero 16:9 without text
#     tw, th = base.width, base.height
#     target_ratio = HERO_W / HERO_H
#     if (tw / th) >= target_ratio:
#         new_w = int(th * target_ratio)
#         x0 = (tw - new_w) // 2
#         crop = (x0, 0, x0 + new_w, th)
#     else:
#         new_h = int(tw / target_ratio)
#         y0 = (th - new_h) // 2
#         crop = (0, y0, tw, y0 + new_h)
#     hero = base.crop(crop).resize((HERO_W, HERO_H), Image.LANCZOS)

#     # cover 1:1 with text
#     cover = base.resize((COVER_SIZE, COVER_SIZE), Image.LANCZOS)
#     cover = draw_text_bottom_left(cover, shorten_title_for_cover(title_for_text))

#     out_cover = BytesIO(); cover.save(out_cover, format="JPEG", quality=90)
#     out_hero  = BytesIO(); hero.save(out_hero,  format="JPEG", quality=90)
#     return {"cover": out_cover.getvalue(), "hero": out_hero.getvalue()}

# # ------------------------------- GCS ---------------------------------
# def upload_bytes_to_gcs(bucket, path: str, data: bytes, content_type: str = "image/jpeg", force: bool = False) -> str:
#     blob = bucket.blob(path)
#     if SKIP_EXISTING and not force and blob.exists():
#         return f"https://storage.googleapis.com/{BUCKET_NAME}/{path}"
#     blob.cache_control = "public, max-age=31536000"
#     blob.upload_from_string(data, content_type=content_type)
#     return f"https://storage.googleapis.com/{BUCKET_NAME}/{path}"

# # --------------------------- MAIN PROCESS ----------------------------
# def process_doc(db, bucket, doc: Any, force: bool, provider: str):
#     pid = doc.id
#     data = doc.to_dict() or {}
#     title = data.get("title") or "Untitled"

#     # quick skip
#     if (not force) and data.get("imageUrl") and data.get("heroImageUrl"):
#         print(f"• Skip {pid}: already has cover+hero")
#         return

#     # 1) generate base (no text) from theme
#     prompt = build_image_prompt(title, want_text=False)
#     if provider == "openai":
#         base_bytes = fetch_generated_image_bytes_openai(prompt, BASE_GEN_SIZE)
#     else:
#         raise RuntimeError("Only 'openai' provider stubbed here. Add yours.")

#     # 2) derive artifacts
#     parts = derive_images(base_bytes, title)

#     # 3) upload
#     cover_path = f"playlistsNew_images/{pid}/1.jpg"
#     hero_path  = f"playlistsNew_images/{pid}/hero.jpg"
#     cover_url = upload_bytes_to_gcs(bucket, cover_path, parts["cover"], force=force)
#     hero_url  = upload_bytes_to_gcs(bucket, hero_path,  parts["hero"],  force=force)

#     # 4) update Firestore
#     db.collection(COLLECTION).document(pid).set({
#         "imageUrl": cover_url,
#         "heroImageUrl": hero_url,
#         "imageMeta": {
#             "provider": provider,
#             "updated_ts": int(time.time())
#         }
#     }, merge=True)
#     print(f"✓ {pid} updated  cover={cover_url}  hero={hero_url}")

# def main():
#     ap = argparse.ArgumentParser(description="Generate playlist cover/hero images from titles.")
#     ap.add_argument("--ids", nargs="*", default=[], help="Specific doc IDs to process (space-separated).")
#     ap.add_argument("--slug-contains", default="", help="Process docs whose slug contains this substring.")
#     ap.add_argument("--missing-only", action="store_true", help="Only process docs missing imageUrl or heroImageUrl.")
#     ap.add_argument("--limit", type=int, default=0, help="Max docs to process.")
#     ap.add_argument("--force", action="store_true", help="Force overwrite even if images exist.")
#     ap.add_argument("--provider", default="openai", choices=["openai"], help="Image provider.")
#     args = ap.parse_args()

#     db, bucket = init_firebase()
#     col = db.collection(COLLECTION)

#     docs: List[Any] = []
#     if args.ids:
#         for pid in args.ids:
#             d = col.document(pid).get()
#             if d.exists: docs.append(d)
#     else:
#         q = col
#         if args.slug_contains:
#             # naive filter client-side (Firestore doesn't support contains easily)
#             q = q.select([])
#         stream = list(q.stream())
#         for d in stream:
#             data = d.to_dict() or {}
#             if args.slug_contains and args.slug_contains.lower() not in (data.get("slug","").lower()):
#                 continue
#             if args.missing_only and data.get("imageUrl") and data.get("heroImageUrl"):
#                 continue
#             docs.append(d)

#     if args.limit:
#         docs = docs[:args.limit]

#     print(f"Processing {len(docs)} doc(s) ...")
#     for d in docs:
#         try:
#             process_doc(db, bucket, d, force=args.force, provider=args.provider)
#         except Exception as e:
#             print(f"⚠️ Failed {d.id}: {e}")

# if __name__ == "__main__":
#     main()
