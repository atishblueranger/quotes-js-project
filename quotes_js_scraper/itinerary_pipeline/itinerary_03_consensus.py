#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 3 — Consensus + Curation (LLM picks final 8)

- Reads: run_dir/02_resolved.json
- Writes: run_dir/03_consensus.json

What it does:
- Dedup by place_id (frequency across sources)
- Compute frequency, sources, variants, popularity
- LLM selects exactly:
  - 5 sightseeing
  - 3 eating (breakfast/lunch/dinner)
- Also generates:
  - itinerary title + summary + tags
  - reasoning per selection
- Fallback deterministic selection if LLM unavailable
"""

import argparse
from pathlib import Path
from typing import Any, Dict, List, Tuple

from itinerary_utils import (
    load_json, save_json, llm_client_from_env, llm_json,
    infer_place_type_from_google, popularity_score
)

CONSENSUS_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "summary": {"type": "string"},
        "tags": {"type": "array", "items": {"type": "string"}},
        "sightseeing": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "place_id": {"type": "string"},
                    "role": {"type": "string"},  # iconic/culture/market/viewpoint/sunset/etc
                    "reason": {"type": "string"},
                },
                "required": ["place_id", "role", "reason"],
                "additionalProperties": False,
            },
            "minItems": 5,
            "maxItems": 5,
        },
        "eating": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "place_id": {"type": "string"},
                    "meal": {"type": "string", "enum": ["breakfast", "lunch", "dinner"]},
                    "reason": {"type": "string"},
                },
                "required": ["place_id", "meal", "reason"],
                "additionalProperties": False,
            },
            "minItems": 3,
            "maxItems": 3,
        },
    },
    "required": ["title", "summary", "tags", "sightseeing", "eating"],
    "additionalProperties": False,
}

def build_pool(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Create a unique candidate pool keyed by place_id.
    """
    pool: Dict[str, Dict[str, Any]] = {}

    for src in data.get("sources", []):
        url = src.get("url")
        for c in (src.get("candidates") or []):
            pid = c.get("place_id")
            if not pid:
                continue
            if c.get("resolution_status") != "publishable":
                continue

            rec = pool.get(pid)
            if not rec:
                rec = {
                    "place_id": pid,
                    "name": c.get("resolved_name") or c.get("name"),
                    "types": c.get("types") or [],
                    "rating": float(c.get("rating") or 0),
                    "reviews": int(c.get("reviews") or 0),
                    "address": c.get("address") or "",
                    "lat": c.get("lat"),
                    "lng": c.get("lng"),
                    "kind_hint": c.get("kind_hint") or infer_place_type_from_google(c.get("types") or []),
                    "variants": set(),
                    "sources": set(),
                    "frequency": 0,
                    "pop_score": 0.0,
                }
                pool[pid] = rec

            rec["frequency"] += 1
            rec["sources"].add(url or "")
            rec["variants"].add((c.get("name") or "").strip())

    # finalize
    out = []
    for pid, rec in pool.items():
        rec["variants"] = sorted([v for v in rec["variants"] if v])
        rec["sources"] = sorted([u for u in rec["sources"] if u])
        rec["pop_score"] = popularity_score(rec["rating"], rec["reviews"])
        out.append(rec)

    # sort by frequency then popularity
    out.sort(key=lambda x: (x["frequency"], x["pop_score"]), reverse=True)
    return out

def fallback_pick(pool: List[Dict[str, Any]]) -> Dict[str, Any]:
    sights = [p for p in pool if (p.get("kind_hint") != "eating")]
    eats = [p for p in pool if (p.get("kind_hint") == "eating")]

    sights.sort(key=lambda x: (x["frequency"], x["pop_score"]), reverse=True)
    eats.sort(key=lambda x: (x["frequency"], x["pop_score"]), reverse=True)

    picked_sights = sights[:5]
    picked_eats = eats[:3]

    meals = ["breakfast", "lunch", "dinner"]
    return {
        "title": "One Perfect Day",
        "summary": "A balanced 1-day route with top sights and great local food stops.",
        "tags": ["1 Day", "Highlights"],
        "sightseeing": [{"place_id": p["place_id"], "role": "core", "reason": "High consensus + popularity"} for p in picked_sights],
        "eating": [{"place_id": p["place_id"], "meal": meals[i], "reason": "High consensus + popularity"} for i, p in enumerate(picked_eats)],
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--llm-model", default="gpt-5-mini")
    ap.add_argument("--keep-ratio", type=float, default=1.0, help="Trim candidate pool by quality before LLM")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    data = load_json(run_dir / "02_resolved.json", default={})
    if not data:
        raise SystemExit("Missing 02_resolved.json")

    pool = build_pool(data)

    # Optional keep-ratio trimming
    if 0.0 < args.keep_ratio < 1.0 and pool:
        k = max(10, int(len(pool) * args.keep_ratio))
        pool = pool[:k]

    city = data.get("city", "")
    country = data.get("country", "")
    source_urls = data.get("source_urls", [])

    client = llm_client_from_env()
    if not client:
        out_pick = fallback_pick(pool)
    else:
        # Tell LLM what we want: repeated places, variety, smooth day, max 8 (5+3)
        system = (
            "You are a senior travel editor building a curated 1-day itinerary.\n"
            "Pick places from the provided candidate pool ONLY.\n"
            "Constraints:\n"
            "- Exactly 5 sightseeing stops.\n"
            "- Exactly 3 eating stops: breakfast, lunch, dinner.\n"
            "- Prefer places mentioned across multiple sources (frequency) and high popularity.\n"
            "- Ensure variety: include iconic sight, market/walk, culture/museum OR viewpoint, sunset spot if possible.\n"
            "- Keep the plan realistic for one day.\n"
        )

        # Keep prompt compact
        pool_lines = []
        for p in pool[:80]:
            pool_lines.append(
                f"- {p['place_id']} | {p['name']} | kind={p.get('kind_hint')} | "
                f"freq={p['frequency']} | rating={p['rating']} ({p['reviews']}) | types={p.get('types')[:4]}"
            )
        user = (
            f"City: {city}, {country}\n"
            f"Sources: {len(source_urls)} urls\n\n"
            "CANDIDATE POOL:\n" + "\n".join(pool_lines) + "\n\n"
            "Return JSON selection exactly matching the schema."
        )

        try:
            out_pick = llm_json(
                client=client,
                model=args.llm_model,
                schema=CONSENSUS_SCHEMA,
                system=system,
                user=user,
                timeout=60,
                cache_key_obj={"step": "consensus", "city": city, "pool_hash": [p["place_id"] for p in pool[:40]]},
            )
        except Exception:
            out_pick = fallback_pick(pool)

    out = {
        "run_id": data.get("run_id"),
        "city": city,
        "state": data.get("state", ""),
        "country": country,
        "days": 1,
        "source_urls": source_urls,
        "pool_size": len(pool),
        "selection": out_pick,
        "created_ts": data.get("created_ts"),
        "consensus_ts": __import__("time").time(),
    }

    save_json(run_dir / "03_consensus.json", out)
    print(f"✅ Wrote: {run_dir / '03_consensus.json'}")
    print("   Picked 5 sightseeing + 3 eating")

if __name__ == "__main__":
    main()
