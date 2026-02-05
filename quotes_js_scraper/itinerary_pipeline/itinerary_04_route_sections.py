#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 4 — Route ordering + sections + LLM match %

- Reads:
  - run_dir/02_resolved.json
  - run_dir/03_consensus.json
- Writes:
  - run_dir/04_final_itinerary.json

What it does:
- Builds final list of 8 places with indices.
- Orders sightseeing using nearest-neighbor + 2-opt.
- Places eating stops near start/mid/end of the sightseeing trail (best proximity).
- Creates sections (Morning/Lunch/Afternoon/Evening) + marker colors.
- LLM comparison: asks LLM for a standard 1-day itinerary list and computes match%.
"""

import argparse
from pathlib import Path
from typing import Any, Dict, List, Tuple

from itinerary_utils import (
    load_json, save_json, haversine_km, nearest_neighbor_order, two_opt,
    route_length_km, walkability_percent, pick_anchor_idx, llm_client_from_env, llm_json
)

MATCH_SCHEMA = {
    "type": "object",
    "properties": {
        "suggested": {"type": "array", "items": {"type": "string"}, "minItems": 6, "maxItems": 12},
        "note": {"type": "string"},
    },
    "required": ["suggested", "note"],
    "additionalProperties": False,
}

def normalize(s: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()

def fuzzy_match_percent(ours: List[str], theirs: List[str]) -> int:
    ours_n = [normalize(x) for x in ours]
    theirs_n = [normalize(x) for x in theirs]
    matched = 0
    for o in ours_n:
        if not o:
            continue
        best = 0
        for t in theirs_n:
            if not t:
                continue
            # cheap token overlap
            oset, tset = set(o.split()), set(t.split())
            if not oset or not tset:
                continue
            score = int(100 * len(oset & tset) / len(oset | tset))
            best = max(best, score)
        if best >= 60:
            matched += 1
    return int(round(100 * matched / max(1, len(ours))))

def index_by_place_id(resolved: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    mp = {}
    for src in resolved.get("sources", []):
        for c in (src.get("candidates") or []):
            pid = c.get("place_id")
            if not pid:
                continue
            # Keep best confidence record
            if pid not in mp or float(c.get("confidence") or 0) > float(mp[pid].get("confidence") or 0):
                mp[pid] = c
    return mp

def pick_best_near(target_latlng: Tuple[float, float], eating_items: List[Dict[str, Any]], used: set, max_km: float = 5.0) -> int:
    """
    Returns index in eating_items for best candidate near target.
    """
    best_i = -1
    best_score = 1e9
    for i, it in enumerate(eating_items):
        pid = it["place_id"]
        if pid in used:
            continue
        lat, lng = it.get("lat"), it.get("lng")
        if lat is None or lng is None:
            continue
        d = haversine_km(target_latlng[0], target_latlng[1], lat, lng)
        if d <= max_km and d < best_score:
            best_score = d
            best_i = i
    if best_i >= 0:
        return best_i

    # fallback: nearest even if > max_km
    for i, it in enumerate(eating_items):
        pid = it["place_id"]
        if pid in used:
            continue
        lat, lng = it.get("lat"), it.get("lng")
        if lat is None or lng is None:
            continue
        d = haversine_km(target_latlng[0], target_latlng[1], lat, lng)
        if d < best_score:
            best_score = d
            best_i = i
    return best_i

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--llm-model", default="gpt-5-mini")
    ap.add_argument("--max-food-km", type=float, default=5.0)
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    resolved = load_json(run_dir / "02_resolved.json", default={})
    consensus = load_json(run_dir / "03_consensus.json", default={})
    if not resolved or not consensus:
        raise SystemExit("Missing 02_resolved.json or 03_consensus.json")

    city = resolved.get("city", "")
    country = resolved.get("country", "")
    source_urls = resolved.get("source_urls", [])

    by_pid = index_by_place_id(resolved)

    sel = consensus.get("selection") or {}
    sights_ids = [x["place_id"] for x in (sel.get("sightseeing") or [])]
    eat_objs = (sel.get("eating") or [])

    # Build sight objects with lat/lng
    sights = []
    for pid in sights_ids:
        c = by_pid.get(pid) or {}
        sights.append({
            "place_id": pid,
            "name": c.get("resolved_name") or c.get("name") or pid,
            "lat": c.get("lat"),
            "lng": c.get("lng"),
            "rating": c.get("rating", 0),
            "reviews": c.get("reviews", 0),
            "types": c.get("types", []),
            "confidence": c.get("confidence", 0),
            "frequency": 0,  # filled later if needed
            "role": next((s["role"] for s in sel.get("sightseeing", []) if s["place_id"] == pid), "core"),
            "reason": next((s["reason"] for s in sel.get("sightseeing", []) if s["place_id"] == pid), ""),
        })

    # Order sights by proximity
    pts = [(s["lat"], s["lng"]) for s in sights if s["lat"] is not None and s["lng"] is not None]
    if len(pts) != len(sights):
        # If missing coords, keep original order
        sight_order = list(range(len(sights)))
    else:
        start_idx = pick_anchor_idx(sights)
        nn = nearest_neighbor_order(pts, start_idx=start_idx)
        opt = two_opt(pts, nn, iters=50)
        sight_order = opt

    ordered_sights = [sights[i] for i in sight_order]

    # Eating items resolved
    eating_items = []
    for e in eat_objs:
        pid = e["place_id"]
        c = by_pid.get(pid) or {}
        eating_items.append({
            "place_id": pid,
            "meal": e.get("meal"),
            "name": c.get("resolved_name") or c.get("name") or pid,
            "lat": c.get("lat"),
            "lng": c.get("lng"),
            "rating": c.get("rating", 0),
            "reviews": c.get("reviews", 0),
            "types": c.get("types", []),
            "confidence": c.get("confidence", 0),
            "reason": e.get("reason", ""),
        })

    # Choose breakfast near first sight, lunch near mid, dinner near last (respect meal intent)
    used = set()
    first = (ordered_sights[0]["lat"], ordered_sights[0]["lng"])
    mid = (ordered_sights[len(ordered_sights)//2]["lat"], ordered_sights[len(ordered_sights)//2]["lng"])
    last = (ordered_sights[-1]["lat"], ordered_sights[-1]["lng"])

    # Split by meal
    breakfast_list = [x for x in eating_items if x["meal"] == "breakfast"] or eating_items
    lunch_list = [x for x in eating_items if x["meal"] == "lunch"] or eating_items
    dinner_list = [x for x in eating_items if x["meal"] == "dinner"] or eating_items

    b_i = pick_best_near(first, breakfast_list, used, max_km=args.max_food_km)
    breakfast = breakfast_list[b_i] if b_i >= 0 else breakfast_list[0]
    used.add(breakfast["place_id"])

    l_i = pick_best_near(mid, lunch_list, used, max_km=args.max_food_km)
    lunch = lunch_list[l_i] if l_i >= 0 else lunch_list[0]
    used.add(lunch["place_id"])

    d_i = pick_best_near(last, dinner_list, used, max_km=args.max_food_km)
    dinner = dinner_list[d_i] if d_i >= 0 else dinner_list[0]
    used.add(dinner["place_id"])

    # Final 8 order: breakfast → sight1 → sight2 → lunch → sight3 → sight4 → sight5(sunset) → dinner
    # If 5 sights exist, this produces exactly 8.
    s1, s2, s3, s4, s5 = ordered_sights
    final = [
        {**breakfast, "place_type": "eating", "slot": "breakfast"},
        {**s1, "place_type": "sightseeing", "slot": "morning"},
        {**s2, "place_type": "sightseeing", "slot": "morning"},
        {**lunch, "place_type": "eating", "slot": "lunch"},
        {**s3, "place_type": "sightseeing", "slot": "afternoon"},
        {**s4, "place_type": "sightseeing", "slot": "afternoon"},
        {**s5, "place_type": "sightseeing", "slot": "sunset"},
        {**dinner, "place_type": "eating", "slot": "dinner"},
    ]

    # Assign indices + route stats (sightseeing trail length only + total)
    coords_all = [(x["lat"], x["lng"]) for x in final if x.get("lat") is not None and x.get("lng") is not None]
    total_km = route_length_km(coords_all)
    walk_pct = walkability_percent(total_km)

    # Sections
    COLORS = ["#FF6B6B", "#4ECDC4", "#45B7D1", "#F7DC6F"]
    sections = [
        {
            "id": "morning",
            "heading": "Morning Highlights",
            "short_description": "Start strong with iconic sights and an easy first loop.",
            "placeMarkerColor": COLORS[0],
            "time_range": "9:00 AM - 12:00 PM",
            "sectionPlaces": [],
            "order": 0,
        },
        {
            "id": "lunch",
            "heading": "Lunch Break",
            "short_description": "Refuel near the route before the second half.",
            "placeMarkerColor": COLORS[1],
            "time_range": "12:00 PM - 1:30 PM",
            "sectionPlaces": [],
            "order": 1,
        },
        {
            "id": "afternoon",
            "heading": "Culture & Walks",
            "short_description": "Museums, markets, or a walkable neighborhood stretch.",
            "placeMarkerColor": COLORS[2],
            "time_range": "1:30 PM - 5:00 PM",
            "sectionPlaces": [],
            "order": 2,
        },
        {
            "id": "evening",
            "heading": "Sunset & Dinner",
            "short_description": "Wrap with a sunset spot and a memorable dinner.",
            "placeMarkerColor": COLORS[3],
            "time_range": "5:00 PM - 9:00 PM",
            "sectionPlaces": [],
            "order": 3,
        },
    ]

    for idx, it in enumerate(final, start=1):
        it["index"] = idx

    def add_to_section(sec_id: str, item: Dict[str, Any]):
        sec = next(s for s in sections if s["id"] == sec_id)
        sec["sectionPlaces"].append({
            "place_id": item["place_id"],
            "name": item["name"],
            "index": item["index"],
            "place_type": item["place_type"],
        })

    add_to_section("morning", final[0])
    add_to_section("morning", final[1])
    add_to_section("morning", final[2])
    add_to_section("lunch", final[3])
    add_to_section("afternoon", final[4])
    add_to_section("afternoon", final[5])
    add_to_section("evening", final[6])
    add_to_section("evening", final[7])

    for s in sections:
        s["place_count"] = len(s["sectionPlaces"])

    # LLM comparison: ask for “standard” itinerary list → match %
    llm_match_percentage = None
    llm_suggested = []
    llm_note = ""

    client = llm_client_from_env()
    if client:
        system = "You are a travel planner. Suggest a strong 1-day itinerary for the city."
        user = (
            f"City: {city}, {country}\n"
            "Give a list of places for a 1-day itinerary (6-10 items). Names only.\n"
            "Include a mix of top sights + local food area.\n"
            "Return JSON with {suggested:[...], note:'...'}."
        )
        try:
            out = llm_json(
                client=client,
                model=args.llm_model,
                schema=MATCH_SCHEMA,
                system=system,
                user=user,
                timeout=45,
                cache_key_obj={"step": "match", "city": city},
            )
            llm_suggested = out.get("suggested") or []
            llm_note = out.get("note") or ""
            ours_names = [x["name"] for x in final]
            llm_match_percentage = fuzzy_match_percent(ours_names, llm_suggested)
        except Exception:
            llm_match_percentage = None

    final_doc = {
        "run_id": resolved.get("run_id"),
        "city": city,
        "state": resolved.get("state", ""),
        "country": country,
        "days": 1,
        "source_urls": source_urls,
        "title": sel.get("title") or f"One Perfect Day in {city}",
        "summary": sel.get("summary") or "",
        "tags": sel.get("tags") or [],
        "split": {"sightseeing": 5, "eating": 3},
        "total_places": 8,
        "route_stats": {
            "total_distance_km": round(float(total_km), 2),
            "walkability_percent": int(walk_pct),
        },
        "llm_comparison": {
            "match_percentage": llm_match_percentage,
            "suggested": llm_suggested,
            "note": llm_note,
        },
        "places": final,
        "sections": sections,
    }

    save_json(run_dir / "04_final_itinerary.json", final_doc)
    print(f"✅ Wrote: {run_dir / '04_final_itinerary.json'}")
    print(f"   Total distance: {final_doc['route_stats']['total_distance_km']} km | Walkable: {walk_pct}%")
    if llm_match_percentage is not None:
        print(f"   LLM match: {llm_match_percentage}%")

if __name__ == "__main__":
    main()
