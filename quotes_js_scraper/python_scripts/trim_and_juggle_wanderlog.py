import os
import json
import random

# ── CONFIG ──────────────────────────────────────────────────────────────────────

INPUT_DIR  = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\wanderlog_attractions_full_municipality"
OUTPUT_DIR = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\wanderlog_attractions_full_top20_municipality"
MAX_ITEMS  = 20
FIXED_KEEP = 12   # keep first 12 in swapped order
MAX_RANDOM_SOURCE_INDEX = 36  # only pick random items whose swapped index is < 36

# ── SWAP LOGIC ─────────────────────────────────────────────────────────────────

def swap_by_field_index(items):
    # ensure every item has an index
    for i, itm in enumerate(items, start=1):
        itm.setdefault("index", i)

    lookup = {itm["index"]: itm for itm in items}
    max_idx = max(lookup.keys())

    # swap 1↔3, 4↔6, 7↔9, ...
    for start in range(1, max_idx+1, 3):
        end = start + 2
        if start in lookup and end in lookup:
            a, b = lookup[start], lookup[end]
            a["index"], b["index"] = b["index"], a["index"]

    return sorted(items, key=lambda x: x["index"])


# ── MIXED SAMPLING + REINDEX ──────────────────────────────────────────────────

def mixed_sample_and_reindex(items):
    # 1) swap
    swapped = swap_by_field_index(items)

    # 2) take head
    head = swapped[:FIXED_KEEP]

    # 3) eligible tail
    remainder = swapped[FIXED_KEEP:]
    eligible  = [itm for itm in remainder if itm["index"] < MAX_RANDOM_SOURCE_INDEX]

    # 4) pick random tail
    need = MAX_ITEMS - len(head)
    if need <= 0:
        tail = []
    elif len(eligible) <= need:
        tail = eligible
    else:
        tail = random.sample(eligible, need)

    # 5) combine
    final = head + tail

    # 6) re-index sequentially 1..MAX_ITEMS
    for new_idx, itm in enumerate(final, start=1):
        itm["index"] = new_idx

    return final


# ── PROCESSING ────────────────────────────────────────────────────────────────

def process_file(path):
    # load
    with open(path, "r", encoding="utf-8") as f:
        items = json.load(f)

    # sample + reindex
    sampled = mixed_sample_and_reindex(items)

    # write out
    base, _  = os.path.splitext(os.path.basename(path))
    out_name = f"{base}_top{MAX_ITEMS}.json"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, out_name)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(sampled, f, ensure_ascii=False, indent=4)

    print(f"[✓] {base}.json → {len(sampled)} items → {out_name}")


if __name__ == "__main__":
    for fn in os.listdir(INPUT_DIR):
        if fn.lower().endswith(".json"):
            process_file(os.path.join(INPUT_DIR, fn))



# working but issue in index part of random sampling
# import os
# import json
# import random

# # ── CONFIG ──────────────────────────────────────────────────────────────────────

# INPUT_DIR  = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wanderlog_attractions_full"
# OUTPUT_DIR = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wanderlog_attractions_full_top20"
# MAX_ITEMS  = 20
# FIXED_KEEP = 12   # keep first 12 in swapped order
# MAX_RANDOM_SOURCE_INDEX = 36  # only pick random items whose swapped index is < 36

# # ── SWAP LOGIC ─────────────────────────────────────────────────────────────────

# def swap_by_field_index(items):
#     # ensure every item has an index
#     for i, itm in enumerate(items, start=1):
#         itm.setdefault("index", i)

#     lookup = {itm["index"]: itm for itm in items}
#     max_idx = max(lookup.keys())

#     # swap 1↔3, 4↔6, 7↔9, ...
#     for start in range(1, max_idx+1, 3):
#         end = start + 2
#         if start in lookup and end in lookup:
#             a, b = lookup[start], lookup[end]
#             a["index"], b["index"] = b["index"], a["index"]

#     # return sorted by new index
#     return sorted(items, key=lambda x: x["index"])


# # ── MIXED SAMPLING ──────────────────────────────────────────────────────────────

# def mixed_sample(items, total=MAX_ITEMS, keep=FIXED_KEEP, max_source=MAX_RANDOM_SOURCE_INDEX):
#     # 1) apply the 3-swap
#     swapped = swap_by_field_index(items)

#     # 2) take the first `keep` items
#     head = swapped[:keep]

#     # 3) from the remainder, only consider those with index < max_source
#     remainder = swapped[keep:]
#     eligible  = [itm for itm in remainder if itm["index"] < max_source]

#     # 4) randomly pick the rest
#     need = total - len(head)
#     if need <= 0:
#         tail = []
#     elif len(eligible) <= need:
#         tail = eligible
#     else:
#         tail = random.sample(eligible, need)

#     # 5) combine and return
#     return head + tail


# # ── PROCESSING ────────────────────────────────────────────────────────────────

# def process_file(path):
#     with open(path, "r", encoding="utf-8") as f:
#         items = json.load(f)

#     sampled = mixed_sample(items)

#     base, _   = os.path.splitext(os.path.basename(path))
#     out_name  = f"{base}_top{MAX_ITEMS}.json"
#     os.makedirs(OUTPUT_DIR, exist_ok=True)
#     out_path  = os.path.join(OUTPUT_DIR, out_name)

#     with open(out_path, "w", encoding="utf-8") as f:
#         json.dump(sampled, f, ensure_ascii=False, indent=4)

#     print(f"[✓] {base}.json → {len(sampled)} items → {out_name}")


# if __name__ == "__main__":
#     for fn in os.listdir(INPUT_DIR):
#         if fn.lower().endswith(".json"):
#             process_file(os.path.join(INPUT_DIR, fn))



# import os
# import json

# # ── CONFIG ──────────────────────────────────────────────────────────────────────

# INPUT_DIR  = "C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wanderlog_attractions_full"
# OUTPUT_DIR = "C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wanderlog_attractions_full_top20"
# MAX_ITEMS  = 20

# # ── SWAP LOGIC ─────────────────────────────────────────────────────────────────

# def swap_by_field_index(items):
#     lookup = {item["index"]: item for item in items}
#     max_idx = max(lookup.keys())
#     for start in range(1, max_idx + 1, 3):
#         end = start + 2
#         if start in lookup and end in lookup:
#             a = lookup[start]
#             b = lookup[end]
#             a["index"], b["index"] = b["index"], a["index"]
#     return sorted(items, key=lambda x: x["index"])

# # ── PROCESSING ────────────────────────────────────────────────────────────────

# def process_file(path):
#     # load
#     with open(path, "r", encoding="utf-8") as f:
#         items = json.load(f)

#     # swap & truncate
#     items = swap_by_field_index(items)[:MAX_ITEMS]

#     # prepare output filename
#     filename       = os.path.basename(path)                  # e.g. dublin_attractions_full.json
#     name_no_ext, _ = os.path.splitext(filename)
#     out_fname      = f"{name_no_ext}_top{MAX_ITEMS}.json"    # e.g. dublin_attractions_full_top20.json

#     # write to OUTPUT_DIR
#     os.makedirs(OUTPUT_DIR, exist_ok=True)
#     out_path = os.path.join(OUTPUT_DIR, out_fname)
#     with open(out_path, "w", encoding="utf-8") as f:
#         json.dump(items, f, ensure_ascii=False, indent=4)

#     print(f"[✓] {filename} → {len(items)} items → {out_fname}")

# if __name__ == "__main__":
#     # process every JSON in INPUT_DIR
#     for fname in os.listdir(INPUT_DIR):
#         if not fname.lower().endswith(".json"):
#             continue
#         fullpath = os.path.join(INPUT_DIR, fname)
#         process_file(fullpath)
