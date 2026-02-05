# PlanUp — 1-Day Itinerary Pipeline (Best-of v3)

This pipeline generates **curated 1-day itineraries** from **2–3 web sources** and uploads them to **Firestore** with subcollections:
- `itineraries/{itineraryId}`
- `itineraries/{itineraryId}/placeMetadata/{placeId}`
- `itineraries/{itineraryId}/sections/{sectionId}`

It merges the best parts of:
- Your playlist workflow (resolve + quality gating + upload structure)
- Friend-1 (multi-source consensus + proximity ordering + section generation + match%)
- Friend-2 (strict 5+3 model + “trail + food” logic, trust signals, skip low quality)

---

## ✅ Core Rules (Your Requirements)

- **Exactly 8 places total**  
  - **5 sightseeing** (core stops)  
  - **3 eating** (breakfast + lunch + dinner)
- **Multi-source consensus**: prioritize places repeated across sources.
- **LLM curation**: pick a varied, realistic day:
  - iconic sight
  - culture/museum or viewpoint
  - walk/market
  - sunset spot
  - plus food stops
- **Proximity-first**: order the route for smooth flow (nearest-neighbor + 2-opt).
- **LLM trust score**: compare with a standard LLM itinerary → match %.
- **Firestore ready**: parent doc + `placeMetadata` + `sections`.

---

## Install

```bash
pip install -r requirements.txt
