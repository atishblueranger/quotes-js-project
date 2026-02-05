#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 2 — Resolve extracted candidates to Google Place IDs.

- Reads: run_dir/01_extracted.json
- Writes: run_dir/02_resolved.json
- Adds: place_id, resolved_name, coords, rating, reviews, types, photo_refs, confidence, resolution_status
- Uses fuzzy scoring + optional LLM judge (grey-zone)
"""

import argparse
from pathlib import Path
from typing import Any, Dict, List

from itinerary_utils import (
    load_json, save_json, PlacesConfig, PlacesResolver, llm_client_from_env
)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--min-confidence", type=float, default=0.80)
    ap.add_argument("--region", default="", help='Google region hint, e.g. "in"')
    ap.add_argument("--language", default="en")
    ap.add_argument("--llm-model", default="gpt-5-mini")
    ap.add_argument("--max-per-source", type=int, default=60)
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    data = load_json(run_dir / "01_extracted.json", default={})
    if not data:
        raise SystemExit("Missing 01_extracted.json")

    city = data.get("city", "")
    state = data.get("state", "")
    country = data.get("country", "")

    cfg = PlacesConfig(
        google_api_key=Path(".").joinpath("").as_posix() and None,  # overwritten below
        language=args.language,
        region=(args.region or None),
        min_confidence=args.min_confidence,
    )
    cfg.google_api_key = (Path(".").joinpath("").as_posix() and None)  # placeholder line
    # Real key from env
    import os
    cfg.google_api_key = os.getenv("GOOGLE_MAPS_API_KEY")

    llm_client = llm_client_from_env()
    resolver = PlacesResolver(cfg, llm_client=llm_client, llm_model=args.llm_model)

    resolved_sources = []
    totals = {"candidates": 0, "publishable": 0}

    for s in data.get("sources", []):
        candidates = (s.get("candidates") or [])[:args.max_per_source]
        new_cands = []
        for c in candidates:
            totals["candidates"] += 1
            name = c.get("name") or ""
            kind_hint = c.get("kind_hint") or "sightseeing"
            context = c.get("raw_context") or c.get("description") or ""

            res = resolver.resolve_one(
                name=name,
                city=city,
                state=state,
                kind_hint=kind_hint,
                context=context,
            )

            merged = dict(c)
            merged.update(res)

            if merged.get("resolution_status") == "publishable":
                totals["publishable"] += 1

            new_cands.append(merged)

        resolved_sources.append({**s, "candidates": new_cands})

    resolver.save()

    out = {**data, "sources": resolved_sources, "resolve_totals": totals}
    save_json(run_dir / "02_resolved.json", out)

    print(f"✅ Wrote: {run_dir / '02_resolved.json'}")
    print(f"   Candidates: {totals['candidates']} | Publishable: {totals['publishable']}")

if __name__ == "__main__":
    main()
