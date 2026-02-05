#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 1 — Extract candidates from multiple 1-day itinerary URLs.

- Input: 2–3 URLs (or more)
- Output: run_dir/01_extracted.json
- Strategy:
  - Fetch HTML (cached)
  - Heuristic candidate extraction
  - Optional LLM extraction for cleaner candidates per source
"""

import argparse
from pathlib import Path
from typing import Any, Dict, List

from itinerary_utils import (
    fetch_html, html_to_text, heuristic_extract_candidates,
    llm_client_from_env, llm_json, clean_txt, slugify, now_ts, save_json
)

def read_urls_file(path: str) -> List[str]:
    urls = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        t = line.strip()
        if t and not t.startswith("#"):
            urls.append(t)
    return urls

LLM_EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "places": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "kind_hint": {"type": "string", "enum": ["sightseeing", "eating"]},
                    "note": {"type": "string"},
                },
                "required": ["name", "kind_hint", "note"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["title", "places"],
    "additionalProperties": False,
}

def llm_extract_places(article_text: str, city: str, country: str, model: str) -> Dict[str, Any]:
    client = llm_client_from_env()
    if not client:
        return {"title": "", "places": []}

    system = (
        "You extract travel places from 1-day itinerary articles.\n"
        "Return a concise list of unique place names.\n"
        "Classify each as sightseeing or eating.\n"
        "Avoid generic headers (FAQ, tips, transport, hotels).\n"
        "Do not invent places not present in the article.\n"
    )
    user = (
        f"City: {city}\nCountry: {country}\n\n"
        "ARTICLE TEXT:\n"
        f"{article_text}\n\n"
        "Return JSON with title and places."
    )

    out = llm_json(
        client=client,
        model=model,
        schema=LLM_EXTRACT_SCHEMA,
        system=system,
        user=user,
        timeout=45,
        cache_key_obj={"step": "extract", "city": city, "hash": article_text[:200]},
    )
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", action="append", default=[], help="Blog URL (repeatable)")
    ap.add_argument("--urls", help="Text file with URLs (one per line)")
    ap.add_argument("--city", required=True)
    ap.add_argument("--state", default="")
    ap.add_argument("--country", default="India")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--out-dir", default="itinerary_output")
    ap.add_argument("--max-per-source", type=int, default=60)
    ap.add_argument("--llm-extract", action="store_true", help="Use LLM to extract per source")
    ap.add_argument("--llm-model", default="gpt-5-mini")
    args = ap.parse_args()

    urls = list(args.url or [])
    if args.urls:
        urls.extend(read_urls_file(args.urls))
    urls = [u.strip() for u in urls if u.strip()]

    if not urls:
        raise SystemExit("No URLs provided. Use --url or --urls.")

    run_id = args.run_id.strip() or f"{slugify(args.city)}_1day_{now_ts()}"
    run_dir = Path(args.out_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    sources = []
    for u in urls:
        html = fetch_html(u, use_cache=True)
        text = html_to_text(html, max_chars=18000)

        heuristic = heuristic_extract_candidates(html, max_items=args.max_per_source)

        llm_out = {"title": "", "places": []}
        if args.llm_extract:
            llm_out = llm_extract_places(text, args.city, args.country, args.llm_model)

        # Merge: LLM places first (clean), then heuristic fill
        merged = []
        seen = set()

        for p in (llm_out.get("places") or []):
            name = clean_txt(p.get("name", ""))
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append({
                "name": name,
                "kind_hint": p.get("kind_hint", "sightseeing"),
                "description": "",
                "raw_context": p.get("note", ""),
            })

        for p in heuristic:
            name = clean_txt(p.get("name", ""))
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(p)

        sources.append({
            "url": u,
            "title": clean_txt(llm_out.get("title", "")) or "",
            "candidates": merged[:args.max_per_source],
        })

    out = {
        "run_id": run_id,
        "city": args.city,
        "state": args.state,
        "country": args.country,
        "days": 1,
        "source_urls": urls,
        "sources": sources,
        "created_ts": now_ts(),
    }

    save_json(run_dir / "01_extracted.json", out)
    print(f"✅ Wrote: {run_dir / '01_extracted.json'}")
    print(f"   Sources: {len(sources)} | URLs: {len(urls)}")

if __name__ == "__main__":
    main()
