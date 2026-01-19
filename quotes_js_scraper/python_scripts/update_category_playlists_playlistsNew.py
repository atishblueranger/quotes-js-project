
# """
# This script updates the 'category' field in the 'playlistsNew' collection.
# It reads a JSON file 'category_playlists.json' in the same folder and
# updates the 'category' field of each document in the 'playlistsNew'
# collection with the value from the JSON file.
# """


# # ─── IMPORTS ──────────────────────────────────────────────────────────────────

import json
from pathlib import Path
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore

# ───── CONFIG ────────────────────────────────────────────────────────────────
SERVICE_ACCOUNT = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
COLLECTION_NAME = "playlistsNew"
DRY_RUN = False   # set to False to perform writes
BATCH_SIZE = 500

CATEGORY_MAP = {
    'Food & Dining':                 'Food & Drink',
    'Romantic Spots':                'Things To Do',
    'Unique Stays & Accommodation':  'Travel',
    'Local Attractions':             'Travel',
    'NightLife & Entertainment':     'Music & Nightlife',
    'Outdoor Activities & Nature':   'Things To Do',
    'Shopping & Souvenirs':          'Shopping',
}
DEFAULT_FALLBACK = 'Travel'

# ───── INIT ─────────────────────────────────────────────────────────────────
def init_db():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT)
        firebase_admin.initialize_app(cred)
    return firestore.client()

# ───── UTILS ────────────────────────────────────────────────────────────────
def map_category(old_val: str | None) -> str:
    if not old_val:
        return DEFAULT_FALLBACK
    return CATEGORY_MAP.get(old_val.strip(), DEFAULT_FALLBACK)

def export_backup(docs, out_path: Path):
    payload = [{ "id": d.id, **d.to_dict() } for d in docs]
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"📦 Backup written: {out_path} ({len(payload)} docs)")

# ───── MAIN ────────────────────────────────────────────────────────────────
def main():
    db = init_db()

    # Stream all docs (could be large; streams lazily)
    print("🔎 Fetching documents…")
    docs_iter = db.collection(COLLECTION_NAME).stream()
    docs = list(docs_iter)
    print(f"Found {len(docs)} documents in {COLLECTION_NAME}")

    # Backup
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    backup_path = Path(f"backup_{COLLECTION_NAME}_{ts}.json")
    export_backup(docs, backup_path)

    # Analyze + plan updates
    plan = []
    counts_before = {}
    counts_after  = {}

    for d in docs:
        data = d.to_dict() or {}
        old_cat = data.get("category")
        new_cat = map_category(old_cat)

        counts_before[old_cat or "∅"] = counts_before.get(old_cat or "∅", 0) + 1
        counts_after[new_cat] = counts_after.get(new_cat, 0) + 1

        if old_cat != new_cat:
            plan.append((d.reference, old_cat, new_cat))

    print("\n— Category counts (before) —")
    for k, v in sorted(counts_before.items(), key=lambda x: (-x[1], str(x[0]))):
        print(f"{k:28} : {v}")

    print("\n— Category counts (after) —")
    for k, v in sorted(counts_after.items(), key=lambda x: (-x[1], x[0])):
        print(f"{k:20} : {v}")

    print(f"\nPlanned updates: {len(plan)}")

    if DRY_RUN:
        print("\n🧪 DRY-RUN mode: no writes will be made.")
        for ref, old_cat, new_cat in plan[:10]:
            print(f"  {ref.id}: '{old_cat}' -> '{new_cat}'")
        if len(plan) > 10:
            print(f"  …and {len(plan)-10} more")
        return

    # Apply updates in batches
    print("\n✍️  Applying updates…")
    batch = db.batch()
    counter = 0
    committed = 0

    for ref, old_cat, new_cat in plan:
        batch.update(ref, {
    "category": new_cat
})
        counter += 1
        if counter % BATCH_SIZE == 0:
            batch.commit()
            committed += counter
            print(f"Committed {committed} updates…")
            batch = db.batch()

    if counter % BATCH_SIZE != 0:
        batch.commit()
        committed += counter % BATCH_SIZE

    print(f"✅ Migration complete: {committed} documents updated.")

if __name__ == "__main__":
    main()
