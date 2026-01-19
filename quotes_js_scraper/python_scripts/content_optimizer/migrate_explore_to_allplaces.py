# -*- coding: utf-8 -*-

import json
import csv
import time
from datetime import datetime, timezone
from typing import Dict, Tuple, Set, List

import firebase_admin
from firebase_admin import credentials, firestore

# Graceful handling of transient Firestore errors during point-reads
from google.api_core.exceptions import ServiceUnavailable, DeadlineExceeded

# ───── CONFIG ─────────────────────────────────────────────────────────────────

SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

SOURCE_COLLECTION = "explore"
SOURCE_SUBCOLLECTION = "TouristAttractions"

TARGET_COLLECTION = "allplaces"
TARGET_SUBCOLLECTION = "top_attractions"

FIELD_MAPPING_FILE = "field_mapping_config.json"

# Behavior
DRY_RUN = False                # Flip to False to write
BATCH_SIZE = 450               # keep under Firestore's 500 write limit per batch
MIN_NUM_RATINGS = 100         # quality gate

# ✅ Only migrate these parent IDs (keys). Values are just labels for your logs.
COUNTRY_NAMES = {
    # "1163": "Jalandhar",
    # "581": "Calangute",
    # "1664": "Fatehpur Sikri",
    # "1750": "Somnath",
    # "1215": "Pachmarhi",
    # "146301": "Khandala",
    # "1573": "Tawang",
    # "3369": "Konark",
    # "2711": "Auroville",

    # "131175": "Itacare",
    # "58309": "Hilo",
    # "9827": "Cannes",
    # "79308": "Casablanca",
    # "10134": "Burgos",
    # "131145": "Blumenau",
    # "60831": "Hammond",
    # "59140": "Poipu",
    # "147376": "Saint Augustine Beach",
    # "116": "Nikko",
    # "12573": "Pamukkale",
    # "58370": "Rapid City",
    # "58368": "Carlsbad",
    # "78760": "Manuel Antonio",
    # "188": "Cebu City",
    # "9701": "Nizhny Novgorod",
    # "292": "Vientiane",
    # "10979": "Merida",
    # "10101": "Puerto Del Carmen",
    # "58314": "Hot Springs",
    # "10698": "Oia",
    # "121": "Kamakura",
    # "10058": "Potsdam",
    # "85945": "Beirut",
    # "58949": "Hershey",
    # "10696": "Bonifacio",
    # "584": "Kanchanaburi",
    # "58053": "Winnipeg",
    # "82615": "Taupo",
    # "131332": "Vila Velha",
    # "10229": "Nimes",
    # "12927": "Oswiecim",
    # "13458": "Akrotiri",
    # "9956": "Mykonos Town",
    # "58299": "Fort Myers Beach",
    # "58214": "Sacramento"
    "450": "Paro",


    # Add more IDs as needed
}

# If True, we’ll only migrate IDs that exist in BOTH source and target.
# If False, we'll migrate any parent that exists in SOURCE; the code will create the parent in TARGET if missing.
REQUIRE_BOTH_PARENTS = True

# Logs
CSV_LOG = "migration_log.csv"
SKIP_LOG = "migration_skipped.csv"

# ───── INIT ───────────────────────────────────────────────────────────────────

cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
try:
    firebase_admin.get_app()
except ValueError:
    firebase_admin.initialize_app(cred)
db = firestore.client()

# Load mapping
try:
    with open(FIELD_MAPPING_FILE, "r", encoding="utf-8") as f:
        FIELD_MAPPING = json.load(f)
    print(f"✅ Loaded field mapping from {FIELD_MAPPING_FILE}\n")
except FileNotFoundError:
    print(f"⚠️  {FIELD_MAPPING_FILE} not found. Using empty mapping.\n")
    FIELD_MAPPING = {"direct_copy": [], "rename": {}, "transform": {}, "skip": [], "default": {}}

# ───── CONVERTERS ─────────────────────────────────────────────────────────────

def to_float(v):
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    if isinstance(v, str):
        s = v.strip()
        if s == "": return None
        try: return float(s)
        except ValueError: return None
    return None

def to_bool(v):
    if v is None: return None
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return v != 0
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "yes", "1"): return True
        if s in ("false", "no", "0", ""): return False
    return None

def to_int_or_null(v):
    if v is None: return None
    if isinstance(v, bool): return 1 if v else 0
    if isinstance(v, int): return v
    if isinstance(v, float): return int(v)
    if isinstance(v, str):
        s = v.strip()
        if s == "": return None
        try: return int(float(s))
        except ValueError: return None
    return None

def apply_transform(src_field: str, tgt_field: str, action: str, value):
    action = (action or "").strip().lower()
    if action == "to_float": return to_float(value)
    if action == "to_bool": return to_bool(value)
    if action == "to_int_or_null": return to_int_or_null(value)
    return value

# ───── HELPERS ────────────────────────────────────────────────────────────────

def coerce_int(v):
    if isinstance(v, int): return v
    if isinstance(v, float): return int(v)
    if isinstance(v, str):
        s = v.strip()
        try: return int(float(s))
        except ValueError: return None
    return None

def normalize_types_list(val):
    if val is None: return []
    if isinstance(val, list):
        seen = set()
        out = []
        for x in val:
            sx = str(x)
            if sx not in seen:
                seen.add(sx)
                out.append(sx)
        return out
    return [str(val)]

def extract_candidate_id(sdoc_id: str, sdata: Dict) -> str:
    """Prefer explicit placeId; fall back to source doc id."""
    pid = (sdata.get("placeId") or "").strip()
    return pid if pid else sdoc_id

def ensure_parent_exists(coll: str, parent_id: str):
    """Create a minimal parent document so it’s not 'italic' and exists for rules/queries."""
    pref = db.collection(coll).document(parent_id)
    snap = pref.get()
    if not snap.exists:
        pref.set({"_created": firestore.SERVER_TIMESTAMP}, merge=True)

def commit_with_retry(batch, label: str, attempts=5) -> bool:
    """Retry batch commits with exponential backoff to ride out transient errors."""
    delay = 1.0
    for i in range(attempts):
        try:
            batch.commit()
            return True
        except Exception as e:
            print(f"   ⚠️  commit failed [{label}] attempt {i+1}/{attempts}: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 16.0)
    return False

def point_read_exists(coll: str, doc_id: str, attempts: int = 5, base_delay: float = 0.5) -> bool:
    """Cheap point-read that avoids scanning/streaming entire collections."""
    ref = db.collection(coll).document(doc_id)
    delay = base_delay
    for i in range(attempts):
        try:
            return ref.get().exists
        except (ServiceUnavailable, DeadlineExceeded) as e:
            print(f"   ⚠️  point read retry {i+1}/{attempts} for {coll}/{doc_id}: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 4.0)
        except Exception as e:
            print(f"   ❌ failed to check {coll}/{doc_id}: {e}")
            return False
    # final attempt
    try:
        return ref.get().exists
    except Exception as e:
        print(f"   ❌ failed to check {coll}/{doc_id} (final): {e}")
        return False

# ───── CORE TRANSFORM ─────────────────────────────────────────────────────────

def transform_document(source_data: Dict) -> Dict:
    mapping = FIELD_MAPPING
    direct_copy = set(mapping.get("direct_copy", []))
    renames: Dict[str, str] = mapping.get("rename", {})
    transforms = mapping.get("transform", {})
    defaults = mapping.get("default", {})
    skip = set(mapping.get("skip", []))

    out: Dict = {}

    # 1) direct copy
    for f in direct_copy:
        if f in source_data:
            out[f] = source_data[f]

    # 2) renames (source -> target)
    for src, tgt in renames.items():
        if src in source_data:
            out[tgt] = source_data[src]

    # 3) transforms (write into renamed key if defined)
    for src_field, tconf in transforms.items():
        action = tconf.get("action") if isinstance(tconf, dict) else str(tconf)
        raw_val = source_data.get(src_field, None)
        tgt_field = renames.get(src_field, src_field)  # honor rename destination
        out[tgt_field] = apply_transform(src_field, tgt_field, action, raw_val)

    # 4) ensure skipped fields not present
    for f in skip:
        out.pop(f, None)

    # 5) defaults ONLY if missing/empty
    for k, v in defaults.items():
        if k not in out or out[k] in (None, "", [], {}):
            out[k] = v

    # 6) normalize
    if "types" in out:
        out["types"] = normalize_types_list(out.get("types"))

    # 7) stamps (ISO + server timestamps are both useful)
    iso_now = datetime.now(timezone.utc).isoformat()
    out["migrated_from_explore"] = True
    out["migrated_at"] = iso_now
    out["content_updated_at"] = iso_now
    out["migrated_at_ts"] = firestore.SERVER_TIMESTAMP

    return out

# ───── TARGET STATE (DEDUPE + NEXT INDEX) ─────────────────────────────────────

def get_target_state(parent_id: str) -> Tuple[Set[str], int]:
    """Return (existing IDs in subcollection, next index to assign).
       Uses a light stream of the subcollection under a single parent (bounded)."""
    ref = db.collection(TARGET_COLLECTION).document(parent_id).collection(TARGET_SUBCOLLECTION)
    docs = list(ref.stream())
    existing_ids = {d.id for d in docs}

    max_idx = None
    for d in docs:
        data = d.to_dict() or {}
        idx = coerce_int(data.get("index"))
        if idx is not None:
            max_idx = idx if max_idx is None else max(max_idx, idx)

    next_index = (max_idx + 1) if max_idx is not None else len(docs)
    return existing_ids, next_index

# ───── MIGRATE ONE PARENT ─────────────────────────────────────────────────────

def migrate_parent_document(source_parent_id: str, target_parent_id: str,
                            csv_writer, skip_writer) -> Tuple[int, int, int]:
    label = COUNTRY_NAMES.get(source_parent_id, source_parent_id)
    print(f"\n{'─'*70}")
    print(f"📦 {label}: {SOURCE_COLLECTION}/{source_parent_id} → {TARGET_COLLECTION}/{target_parent_id}")

    # Ensure target parent exists if we're allowed to migrate without pre-existing target parent
    if not REQUIRE_BOTH_PARENTS:
        ensure_parent_exists(TARGET_COLLECTION, target_parent_id)

    # Stream the source subcollection (expected to be bounded per parent)
    src_q = (db.collection(SOURCE_COLLECTION)
               .document(source_parent_id)
               .collection(SOURCE_SUBCOLLECTION))
    source_docs = list(src_q.stream())
    if not source_docs:
        print("   ⚠️  No source docs found.")
        return (0, 0, 0)

    tgt_coll_ref = (db.collection(TARGET_COLLECTION)
                      .document(target_parent_id)
                      .collection(TARGET_SUBCOLLECTION))
    existing_ids, next_index = get_target_state(target_parent_id)

    total = len(source_docs)
    migrated = 0
    skipped = 0

    batch = db.batch()
    ops = 0

    for sdoc in source_docs:
        sdata = sdoc.to_dict() or {}

        # Quality gate
        raw_count = (
            sdata.get("numRatings", None) if "numRatings" in sdata else
            sdata.get("ratingCount", None) if "ratingCount" in sdata else
            sdata.get("user_ratings_total", None)
        )
        rating_count = coerce_int(raw_count) or 0
        if rating_count < MIN_NUM_RATINGS:
            skipped += 1
            print(f"   🚫 [{sdoc.id}] {sdata.get('name','Unknown')} skipped: low_ratings ({rating_count} < {MIN_NUM_RATINGS})")
            if skip_writer:
                skip_writer.writerow([source_parent_id, target_parent_id, sdoc.id,
                                      sdata.get("name", "Unknown"), "low_ratings"])
            continue

        # Dedupe key
        candidate_id = extract_candidate_id(sdoc.id, sdata)
        if candidate_id in existing_ids:
            skipped += 1
            print(f"   ⏭️  [{candidate_id}] exists → skipped")
            if skip_writer:
                skip_writer.writerow([source_parent_id, target_parent_id, candidate_id,
                                      sdata.get("name", "Unknown"), "already_exists"])
            continue

        # Transform
        tdata = transform_document(sdata)
        tdata["source_url"] = "wanderlog"
        tdata["index"] = next_index
        next_index += 1

        tgt_ref = tgt_coll_ref.document(candidate_id)

        if DRY_RUN:
            migrated += 1
            preview = {
                "name": tdata.get("name"),
                "placeId": tdata.get("placeId"),
                "index": tdata.get("index"),
                "rating": tdata.get("rating"),
                "ratingCount": rating_count,
                "permanentlyClosed": tdata.get("permanentlyClosed"),
                "priceLevel": tdata.get("priceLevel"),
                "types": tdata.get("types"),
            }
            print(f"   🔍 DRY RUN → [{candidate_id}] {sdata.get('name','Unknown')} :: "
                  f"{ {k:v for k,v in preview.items() if v is not None} }")
        else:
            batch.set(tgt_ref, tdata, merge=False)
            ops += 1
            migrated += 1
            if ops >= BATCH_SIZE:
                ok = commit_with_retry(batch, f"{target_parent_id}:{ops}")
                print(f"   💾 committed batch of {ops}" if ok else "   ❌ batch commit failed")
                batch = db.batch()
                ops = 0

        if csv_writer:
            csv_writer.writerow([
                source_parent_id, target_parent_id, candidate_id,
                sdata.get("name", "Unknown"),
                sdoc.reference.path, tgt_ref.path,
                "dry_run" if DRY_RUN else "insert"
            ])

    if not DRY_RUN and ops > 0:
        ok = commit_with_retry(batch, f"{target_parent_id}:{ops}")
        print(f"   💾 committed final batch of {ops}" if ok else "   ❌ final batch failed")

    print(f"   Summary: {total} total | {migrated} {'(dry)' if DRY_RUN else ''} | {skipped} skipped")
    return (total, migrated, skipped)

# ───── BUILD ID MAP FROM COUNTRY_NAMES (NO COLLECTION SCANS) ──────────────────

def build_filtered_id_mapping() -> Dict[str, str]:
    """
    Use ONLY the keys in COUNTRY_NAMES and check existence via point reads (doc(id).get()).
    Avoid scanning full collections to prevent timeouts.
    """
    wanted_ids: List[str] = [str(k) for k in COUNTRY_NAMES.keys()]
    if not wanted_ids:
        return {}

    missing_in_source, missing_in_target = [], []
    presence: Dict[str, Tuple[bool, bool]] = {}

    for pid in wanted_ids:
        in_src = point_read_exists(SOURCE_COLLECTION, pid)
        in_tgt = point_read_exists(TARGET_COLLECTION, pid)
        presence[pid] = (in_src, in_tgt)
        if not in_src:
            missing_in_source.append(pid)
        if not in_tgt:
            missing_in_target.append(pid)

    if missing_in_source:
        print(f"⚠️  Missing in SOURCE: {missing_in_source}")
    if missing_in_target:
        print(f"⚠️  Missing in TARGET: {missing_in_target}")

    if REQUIRE_BOTH_PARENTS:
        valid = [pid for pid, (in_src, in_tgt) in presence.items() if in_src and in_tgt]
    else:
        # migrate any that exist in source; we'll create target parent later
        valid = [pid for pid, (in_src, _) in presence.items() if in_src]

    # Map source->target (same id convention here)
    return {pid: pid for pid in valid}

# ───── MAIN ───────────────────────────────────────────────────────────────────

def main():
    print("🚀 Firestore Migration (dedup, next-index, quality-gated, filtered IDs)")
    print(f"   Source: {SOURCE_COLLECTION}/{SOURCE_SUBCOLLECTION}")
    print(f"   Target: {TARGET_COLLECTION}/{TARGET_SUBCOLLECTION}")
    print(f"   Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"   Min ratings: {MIN_NUM_RATINGS}\n")

    if not DRY_RUN:
        confirm = input("⚠️  This will WRITE to Firestore. Type 'yes' to continue: ").strip().lower()
        if confirm != "yes":
            print("Cancelled.")
            return

    id_map = build_filtered_id_mapping()
    if not id_map:
        print("❌ No valid parent IDs to migrate (check presence or REQUIRE_BOTH_PARENTS).")
        return

    # Sorted for predictable order in logs
    id_map = dict(sorted(id_map.items(), key=lambda kv: kv[0]))
    print(f"📋 Parents to migrate ({len(id_map)}): {list(id_map.keys())}\n")

    with open(CSV_LOG, "w", newline="", encoding="utf-8") as cf, \
         open(SKIP_LOG, "w", newline="", encoding="utf-8") as sf:
        csv_writer = csv.writer(cf)
        skip_writer = csv.writer(sf)
        csv_writer.writerow(["source_parent_id","target_parent_id","doc_id","name","source_path","target_path","status"])
        skip_writer.writerow(["source_parent_id","target_parent_id","doc_id","name","reason"])

        total_all = migrated_all = skipped_all = 0
        for spid, tpid in id_map.items():
            total, migrated, skipped = migrate_parent_document(spid, tpid, csv_writer, skip_writer)
            total_all += total
            migrated_all += migrated
            skipped_all += skipped

    print("\n" + "="*70)
    print("🎉 MIGRATION COMPLETE")
    print("="*70)
    print(f"Total docs: {total_all}")
    print(f"Migrated:   {migrated_all} {'(dry run)' if DRY_RUN else ''}")
    print(f"Skipped:    {skipped_all}")
    print("\n📊 Logs:")
    print(f"   {CSV_LOG}")
    print(f"   {SKIP_LOG}")
    print("="*70)
    if DRY_RUN:
        print("\n⚠️  This was a DRY RUN. Set DRY_RUN = False to write.")

if __name__ == "__main__":
    main()


# # -*- coding: utf-8 -*-

# import json
# import csv
# from datetime import datetime, timezone
# from typing import Dict, Tuple, Set, List

# import firebase_admin
# from firebase_admin import credentials, firestore

# # ───── CONFIG ─────────────────────────────────────────────────────────────────

# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# SOURCE_COLLECTION = "explore"
# SOURCE_SUBCOLLECTION = "TouristAttractions"

# TARGET_COLLECTION = "allplaces"
# TARGET_SUBCOLLECTION = "top_attractions"

# FIELD_MAPPING_FILE = "field_mapping_config.json"

# # Behavior
# DRY_RUN = False                # Flip to False to write
# BATCH_SIZE = 450
# MIN_NUM_RATINGS = 500        # quality gate

# # ✅ Only migrate these parent IDs (keys). Values are just labels for your logs.
# COUNTRY_NAMES = {
#     #  "1": "Tokyo",
#     # "10": "Shanghai",
#     # "1000": "Udupi",
#     # "10008": "Lucerne",
#     # "10015": "Ronda",
#     # "1002": "Mathura",
#     # "10021": "Corfu Town",
#     # "10024": "Sitges",
#     # "10025": "Tarragona",
#     # "10030": "Bursa",
#     # "10031": "Lloret de Mar",
#     # "10033": "Valletta",
#     # "10046": "Ravenna",
#     # "1006": "Gulmarg",
#     # "10061": "Versailles",
#     # "10062": "Bonn",
#     # "10064": "Weymouth",
#     # "10074": "Agrigento",
#     # "10086": "Augsburg",
#     # "10095": "Koblenz",
#     # "10106": "Torremolinos",
#     # "10114": "Mannheim",
#     # "10123": "Puerto de la Cruz",
#     # "1013": "Imphal",
#     # "1014": "Trincomalee",
#     # "10143": "Torquay",
#     # "1015": "Kolhapur",
#     # "10171": "Nerja",
#     # "10176": "Salou",
#     # "10184": "Aachen",
#     # "10186": "Groningen",
#     # "10193": "Delft",
#     # "10211": "Vila Nova de Gaia",
#     # "10219": "Windsor",
#     # "10230": "Segovia",
#     # "10235": "Bled",
#     # "10241": "Playa Blanca",
#     # "1025": "Ujjain",
#     # "10260": "Selcuk",
#     # "10262": "Trogir",
#     # "10279": "Nijmegen",
#     # "10291": "Cuenca",
#     # "10366": "Akureyri",
#     # "10382": "Cremona",
#     # "10421": "Bamberg",
#     # "10453": "Konstanz",
#     # "105": "Macau",
#     # "10533": "Ulm",
#     # "1055": "Mahabalipuram",
#     # "10590": "Savona",
#     # "10595": "Peterhof",
#     # "1060": "Ludhiana",
#     # "10792": "Monte-Carlo",
#     # "10804": "Ceuta",
#     # "10876": "Uppsala",
#     # "10884": "Tropea",
#     # "1089": "Bikaner",
#     # "11": "Siem Reap",
#     # "110": "Hakodate",
#     # "11061": "Lund",
#     # "1131": "Sukhothai",
#     # "1135": "Kanpur",
#     # "1138": "Sawai Madhopur",
#     # "11423": "Grindelwald",
#     # "11489": "Mont-Saint-Michel",
#     # "1159": "Bundi",
#     # "1160": "Sandakan",
#     # "1163": "Jalandhar",
#     # "11655": "Berchtesgaden",
#     # "1168": "Hat Yai",
#     # "11706": "Trujillo",
#     # "11769": "Lindos",
#     # "1177": "Ranchi",
#     # "118": "Kandy",
#     # "1192": "Chonburi",
#     # "12": "Phuket",
#     # "121": "Kamakura",
#     # "1212": "Vijayawada",
#     # "1218": "Kota",
#     # "122": "Varanasi",
#     # "1222": "Kalpetta",
#     # "1241": "Alwar",
#     # "12491": "Mdina",
#     # "12525": "Stretford",
#     # "128": "Hua Hin",
#     # "129": "Incheon",
#     # "1291": "Gwalior",
#     # "13": "New Delhi",
#     # "1302": "Kumbakonam",
#     # "131071": "Buenos Aires",
#     # "131072": "Rio de Janeiro",
#     # "131073": "Sao Paulo",
#     # "131074": "Cusco",
#     # "131075": "Santiago",
#     # "131076": "Lima",
#     # "131077": "Bogota",
#     # "131078": "Quito",
#     # "131079": "Medellin",
#     # "131080": "Cartagena",
#     # "131081": "Porto Alegre",
#     # "131083": "Mendoza",
#     # "131084": "Montevideo",
#     # "131085": "Salvador",
#     # "131086": "Florianopolis",
#     # "131087": "Brasilia",
#     # "131088": "Belo Horizonte",
#     # "131089": "Recife",
#     # "131090": "San Carlos de Bariloche",
#     # "131091": "Fortaleza",
#     # "131092": "Manaus",
#     # "131093": "Angra Dos Reis",
#     # "131094": "Paraty",
#     # "131095": "La Paz",
#     # "131098": "Valparaiso",
#     # "131099": "Arequipa",
#     # "131100": "Natal",
#     # "131102": "Guayaquil",
#     # "131103": "Gramado",
#     # "131104": "San Pedro de Atacama",
#     # "131105": "Salta",
#     # "131106": "Campinas",
#     # "131107": "Santa Marta",
#     # "131108": "Ubatuba",
#     # "131109": "Joao Pessoa",
#     # "131110": "Mar del Plata",
#     # "131113": "Rosario",
#     # "131114": "Belem",
#     # "131115": "Maceio",
#     # "131117": "Ouro Preto",
#     # "131118": "Porto Seguro",
#     # "131119": "Ushuaia",
#     # "131122": "Santos",
#     # "131125": "Niteroi",
#     # "131127": "Vitoria",
#     # "131131": "Sao Luis",
#     # "131132": "Petropolis",
#     # "131134": "Jijoca de Jericoacoara",
#     # "131138": "Puerto Varas",
#     # "131139": "El Calafate",
#     # "131140": "Puno",
#     # "131146": "Cabo Frio",
#     # "131147": "Aracaju",
#     # "131150": "Ipojuca",
#     # "131151": "Campos Do Jordao",
#     # "131157": "Canela",
#     # "131162": "Punta del Este",
#     # "131164": "Puerto Iguazu",
#     # "131168": "San Juan",
#     # "131172": "Fernando de Noronha",
#     # "131174": "San Andres Island",
#     # "131177": "Tiradentes",
#     # "131178": "Pocos de Caldas",
#     # "131180": "Puerto Ayora",
#     # "1312": "Kasaragod",
#     # "131315": "Bonito",
#     # "131318": "Machu Picchu",
#     # "131327": "Bombinhas",
#     # "131337": "Guaruja",
#     # "131360": "Maragogi",
#     # "131362": "Cafayate",
#     # "131389": "San Andres",
#     # "131395": "Praia da Pipa",
#     # "131438": "Morro de Sao Paulo",
#     # "131447": "Caldas Novas",
#     # "131457": "Casablanca",
#     # "131478": "Mata de Sao Joao",
#     # "1316": "Shirdi",
#     # "131626": "Penha",
#     # "161": "Ahmedabad",
#     # "1765": "Almora",
#     # "2601": "Amphawa",
#     # "1868": "Baga",
#     # "228": "Beppu",
#     # "19": "Chiang Mai",
#     # "257": "Chiang Rai",
#     # "1887": "Coonoor",
#     # "174": "Da Lat",
#     # "1784": "Dalhousie",
#     # "1808": "Daman",
#     # "2121": "Deoghar",
#     # "283": "Dhaka City",
#     # "1743": "Diu",
#     # "1983": "Dwarka",
#     # "2177": "Gandhinagar",
#     # "1729": "Gaya",
#     # "2371": "Gokarna",
#     # "207": "Gurugram (Gurgaon)",
#     # "1668": "Hassan",
#     # "16": "Ho Chi Minh City",
#     # "21": "Hoi An",
#     # "2083": "Howrah",
#     # "247": "Ise",
#     # "24": "Jaipur",
#     # "183": "Jaisalmer",
#     # "1763": "Jamshedpur",
#     # "288": "Karachi",
#     # "176": "Karon",
#     # "17": "Kathmandu",
#     # "2141": "Kohima",
#     # "1966": "Kovalam",
#     # "223": "Kuching",
#     # "2540": "Kumarakom",
#     # "2": "Kyoto",
#     # "235": "Lahore",
#     # "297": "Leh",
#     # "248": "Lhasa",
#     # "184": "Luang Prabang",
#     # "1886": "Madikeri",
#     # "175": "Manila",
#     # "1699": "Margao",
#     # "187": "Melaka",
#     # "23": "Minato",
#     # "25": "Mumbai",
#     # "2027": "Orchha",
#     # "2253": "Pahalgam",
#     # "287": "Panjim",
#     # "194": "Rishikesh",
#     # "221": "Sapa",
#     # "29": "Sapporo",
#     # "285": "Seogwipo",
#     # "1942": "Shimoga",
#     # "256": "Srinagar",
#     # "18": "Taipei",
#     # "28": "Taito",
#     # "209": "Thimphu",
#     # "200": "Thiruvananthapuram (Trivandrum)",
#     # "1997": "Tiruvannamalai",
#     # "22": "Ubud",
#     # "1659": "Vasco da Gama",
#     # "1874": "Vrindavan",
#     # "20": "Yokohama",
#     # "60": "Agra",
#     # "58191": "Albuquerque",
#     # "58242": "Anaheim",
#     # "58198": "Anchorage",
#     # "58342": "Arlington",
#     # "58185": "Asheville",
#     # "58455": "Athens",
#     # "58169": "Atlanta",
#     # "607": "Aurangabad",
#     # "58163": "Austin",
#     # "58179": "Baltimore",
#     # "58068": "Banff",
#     # "570": "Batu",
#     # "6": "Beijing",
#     # "58162": "Boston",
#     # "58183": "Branson",
#     # "58164": "Brooklyn",
#     # "61": "Busan",
#     # "581": "Calangute",
#     # "58048": "Calgary",
#     # "58170": "Charleston",
#     # "58193": "Charlotte",
#     # "58226": "Chattanooga",
#     # "67": "Chengdu",
#     # "58146": "Chicago",
#     # "58201": "Cincinnati",
#     # "58231": "Clearwater",
#     # "58210": "Cleveland",
#     # "58563": "Columbus",
#     # "58173": "Dallas",
#     # "58248": "Daytona Beach",
#     # "58166": "Denver",
#     # "58218": "Detroit",
#     # "57258": "Dublin",
#     # "58052": "Edmonton",
#     # "660": "Ella",
#     # "58300": "Flagstaff",
#     # "58177": "Fort Lauderdale",
#     # "58211": "Fort Myers",
#     # "58212": "Fort Worth",
#     # "58284": "Fredericksburg",
#     # "58244": "Galveston",
#     # "503": "Gangtok",
#     # "58286": "Gettysburg",
#     # "58182": "Greater Palm Springs",
#     # "524": "Guwahati",
#     # "58059": "Halifax",
#     # "696": "Hampi",
#     # "64": "Hangzhou",
#     # "59": "Hiroshima",
#     # "58153": "Honolulu",
#     # "58161": "Houston",
#     # "58205": "Indianapolis",
#     # "558": "Indore",
#     # "58155": "Island of Hawaii",
#     # "62": "Kanazawa",
#     # "584": "Kanchanaburi",
#     # "58219": "Kansas City",
#     # "53": "Kathu",
#     # "58167": "Kauai",
#     # "58165": "Key West",
#     # "59165": "Keystone",
#     # "58067": "Kingston",
#     # "68": "Kochi (Cochin)",
#     # "69": "Kolkata (Calcutta)",
#     # "527": "Kozhikode",
#     # "58203": "Lahaina",
#     # "58148": "Las Vegas",
#     # "58145": "Los Angeles",
#     # "58196": "Louisville",
#     # "615": "Madurai",
#     # "58151": "Maui",
#     # "58224": "Memphis",
#     # "58157": "Miami",
#     # "58180": "Miami Beach",
#     # "58195": "Milwaukee",
#     # "58184": "Minneapolis",
#     # "58269": "Moab",
#     # "58276": "Monterey",
#     # "58046": "Montreal",
#     # "687": "Mussoorie",
#     # "58202": "Myrtle Beach",
#     # "50": "Naha",
#     # "58189": "Naples",
#     # "58171": "Nashville",
#     # "58156": "New Orleans",
#     # "58144": "New York City",
#     # "58058": "Niagara Falls",
#     # "563": "Noida",
#     # "58079": "North Vancouver",
#     # "58232": "Oklahoma City",
#     # "58213": "Omaha",
#     # "58152": "Orlando",
#     # "58049": "Ottawa",
#     # "58476": "Page",
#     # "673": "Patna",
#     # "58160": "Philadelphia",
#     # "58181": "Phoenix",
#     # "51": "Phuket Town",
#     # "58241": "Pigeon Forge",
#     # "58199": "Pittsburgh",
#     # "589": "Port Blair",
#     # "58158": "Portland",
#     # "57": "Pune",
#     # "58051": "Quebec City",
#     # "58222": "Richmond",
#     # "58175": "Saint Louis",
#     # "58289": "Salem",
#     # "58216": "Salt Lake City",
#     # "58168": "San Antonio",
#     # "58150": "San Diego",
#     # "58147": "San Francisco",
#     # "58206": "Santa Barbara",
#     # "58178": "Santa Fe",
#     # "58228": "Santa Monica",
#     # "58187": "Sarasota",
#     # "58174": "Savannah",
#     # "58192": "Scottsdale",
#     # "58154": "Seattle",
#     # "58188": "Sedona",
#     # "650": "Sentosa Island",
#     # "528": "Singaraja",
#     # "583": "Solo",
#     # "58176": "Tampa",
#     # "541": "Thane",
#     # "536": "Thrissur",
#     # "58748": "Titusville",
#     # "58077": "Tofino",
#     # "58045": "Toronto",
#     # "58172": "Tucson",
#     # "58047": "Vancouver",
#     # "58044": "Vancouver Island",
#     # "662": "Varkala Town",
#     # "58050": "Victoria",
#     # "518": "Visakhapatnam",
#     # "58159": "Washington DC",
#     # "58275": "Williamsburg",
#     # "58345": "Wisconsin Dells",
#     # "52": "Yangon (Rangoon)",
#     # "350": "Alappuzha",
#     # "384": "Amritsar",
#     # "468": "Ayutthaya",
#     # "4": "Bangkok",
#     # "329": "Bardez",
#     # "35": "Bengaluru",
#     # "485": "Bhopal",
#     # "444": "Bhubaneswar",
#     # "304": "Chandigarh",
#     # "42": "Chiyoda",
#     # "371": "Coimbatore",
#     # "43": "Colombo",
#     # "49": "Da Nang",
#     # "349": "Darjeeling",
#     # "405": "Dharamsala",
#     # "3129": "Digha",
#     # "31": "Fukuoka",
#     # "39": "Guangzhou",
#     # "465": "Gyeongju",
#     # "419": "Hikkaduwa",
#     # "376": "Ipoh",
#     # "46": "Jakarta",
#     # "461": "Kannur",
#     # "34": "Kobe",
#     # "33": "Kuala Lumpur",
#     # "37": "Kuta",
#     # "375": "Lucknow",
#     # "313": "Manali Tehsil",
#     # "479": "Mangalore",
#     # "382": "Medan",
#     # "412": "Munnar",
#     # "30": "Nagoya",
#     # "398": "Nagpur",
#     # "449": "Nashik",
#     # "472": "Navi Mumbai",
#     # "41": "New Taipei",
#     # "480": "Ooty (Udhagamandalam)",
#     # "44": "Phnom Penh",
#     # "334": "Pondicherry",
#     # "381": "Semarang",
#     "38": "Shibuya",
#     "428": "Shimla",
#     "32": "Shinjuku",
#     "478": "Surat",
#     "330": "Tashkent",
#     "348": "Vadodara",
#     "79337": "Alexandria",
#     "785": "Allahabad",
#     "79302": "Cairo",
#     "79300": "Cape Town Central",
#     "787": "Chikmagalur",
#     "79333": "Dahab",
#     "79305": "Fes",
#     "79321": "Giza",
#     "783": "Haridwar",
#     "78": "Hyderabad",
#     "730": "Jamnagar",
#     "79306": "Johannesburg",
#     "714": "Kollam",
#     "79": "Krabi Town",
#     "78752": "La Fortuna de San Carlos",
#     "76078": "Lake Louise",
#     "701": "Lonavala",
#     "717": "Male",
#     "79299": "Marrakech",
#     "79304": "Mauritius",
#     "75": "Nagasaki",
#     "79303": "Nairobi",
#     "724": "Ninh Binh",
#     "78744": "Panama City",
#     "722": "Pushkar",
#     "798": "Raipur",
#     "78746": "San Jose",
#     "79309": "Sharm El Sheikh",
#     "728": "Shillong",
#     "738": "Thanjavur",
#     "753": "Thekkady",
#     "74": "Xi'an",
#     "73": "Yerevan",
#     "85942": "Abu Dhabi",
#     "82581": "Adelaide",
#     "81941": "Akumal",
#     "85962": "Al Ain",
#     "88609": "Albania",
#     "87236": "Arunachal Pradesh",
#     "86937": "Assam",
#     "82576": "Auckland",
#     "88384": "Austria",
#     "86729": "Azerbaijan",
#     "82": "Baku",
#     "86851": "Bangladesh",
#     "88406": "Bavaria",
#     "88503": "Belarus",
#     "88408": "Belgium",
#     "86819": "Bhutan",
#     "82577": "Brisbane",
#     "82614": "Broome",
#     "86779": "Brunei Darussalam",
#     "88449": "Bulgaria",
#     "82584": "Cairns",
#     "86659": "Cambodia",
#     "814": "Canacona",
#     "82585": "Canberra",
#     "81904": "Cancun",
#     "82579": "Christchurch",
#     "81908": "Cozumel",
#     "88438": "Croatia",
#     "88527": "Cyprus",
#     "88366": "Czech Republic",
#     "82588": "Darwin",
#     "88402": "Denmark",
#     "85946": "Doha",
#     "85939": "Dubai",
#     "82594": "Dunedin",
#     "88455": "Estonia",
#     "88434": "Finland",
#     "88358": "France",
#     "86743": "Fujian",
#     "88420": "Georgia",
#     "88368": "Germany",
#     "82578": "Gold Coast",
#     "81187": "Grand Cayman",
#     "88375": "Greece",
#     "81912": "Guadalajara",
#     "81924": "Guanajuato",
#     "86689": "Guangdong",
#     "81182": "Havana",
#     "82586": "Hobart",
#     "86676": "Hokkaido",
#     "88380": "Hungary",
#     "88419": "Iceland",
#     "86661": "India",
#     "88386": "Ireland",
#     "88352": "Italy",
#     "86647": "Japan",
#     "85959": "Jeddah",
#     "85941": "Jerusalem",
#     "86722": "Jiangsu",
#     "86686": "Karnataka",
#     "86731": "Kazakhstan",
#     "86714": "Kerala",
#     "850": "Khajuraho",
#     "842": "Kodaikanal",
#     "849": "Kottayam",
#     "86776": "Kyrgyzstan",
#     "86797": "Laos",
#     "88423": "Latvia",
#     "88477": "Lithuania",
#     "88723": "Luxembourg",
#     "86675": "Maharashtra",
#     "88407": "Malta",
#     "85954": "Manama",
#     "85998": "Mecca",
#     "86988": "Meghalaya",
#     "82575": "Melbourne",
#     "81913": "Merida",
#     "81903": "Mexico City",
#     "88731": "Moldova",
#     "86772": "Mongolia",
#     "88654": "Montenegro",
#     "877": "Mount Abu",
#     "87411": "Nagaland",
#     "895": "Nainital",
#     "81194": "Nassau",
#     "86667": "Nepal",
#     "81190": "New Providence Island",
#     "82608": "Newcastle",
#     "87077": "North Korea",
#     "88432": "Norway",
#     "81911": "Oaxaca",
#     "81213": "Ocho Rios",
#     "86904": "Odisha",
#     "86831": "Pakistan",
#     "81981": "Palenque",
#     "80": "Pattaya",
#     "86652": "Philippines",
#     "81905": "Playa del Carmen",
#     "88403": "Poland",
#     "81918": "Puebla",
#     "81219": "Puerto Plata",
#     "81906": "Puerto Vallarta",
#     "86887": "Punjab",
#     "81180": "Punta Cana",
#     "86674": "Rajasthan",
#     "894": "Rajkot",
#     "82593": "Rotorua",
#     "88360": "Russia",
#     "81189": "San Juan",
#     "81250": "Santiago de Cuba",
#     "81197": "Santo Domingo",
#     "88464": "Serbia",
#     "86654": "Singapore",
#     "88478": "Slovakia",
#     "88486": "Slovenia",
#     "86656": "South Korea",
#     "88362": "Spain",
#     "86693": "Sri Lanka",
#     "85": "Suzhou",
#     "88445": "Sweden",
#     "88437": "Switzerland",
#     "82574": "Sydney",
#     "86668": "Taiwan",
#     "86938": "Tajikistan",
#     "86691": "Tamil Nadu",
#     "82615": "Taupo",
#     "85943": "Tehran",
#     "85940": "Tel Aviv",
#     "86651": "Thailand",
#     "88373": "The Netherlands",
#     "805": "Tiruchirappalli",
#     "827": "Tirunelveli",
#     "81909": "Tulum",
#     "88367": "Turkiye",
#     "88359": "United Kingdom",
#     "86706": "Uttar Pradesh",
#     "86805": "Uttarakhand",
#     "81226": "Varadero",
#     "86655": "Vietnam",
#     "82582": "Wellington",
#     "86716": "West Bengal",
#     "81196": "Willemstad",
#  "3": "Osaka",
#  "5": "Luzon",
#   "9": "Seoul",
#   "8": "Hanoi",
#   "7":"Singapore",
#   "91":"Udaipur",  
#   "92":"Hue",
#   "98":"Bophut",
#   "99":"Nara"
# }

# # If True, we’ll only migrate IDs that exist in BOTH source and target.
# REQUIRE_BOTH_PARENTS = True

# # Logs
# CSV_LOG = "migration_log.csv"
# SKIP_LOG = "migration_skipped.csv"

# # ───── INIT ───────────────────────────────────────────────────────────────────

# cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
# try:
#     firebase_admin.get_app()
# except ValueError:
#     firebase_admin.initialize_app(cred)
# db = firestore.client()

# # Load mapping
# try:
#     with open(FIELD_MAPPING_FILE, "r", encoding="utf-8") as f:
#         FIELD_MAPPING = json.load(f)
#     print(f"✅ Loaded field mapping from {FIELD_MAPPING_FILE}\n")
# except FileNotFoundError:
#     print(f"⚠️  {FIELD_MAPPING_FILE} not found. Using empty mapping.\n")
#     FIELD_MAPPING = {"direct_copy": [], "rename": {}, "transform": {}, "skip": [], "default": {}}

# # ───── CONVERTERS ─────────────────────────────────────────────────────────────

# def to_float(v):
#     if v is None: return None
#     if isinstance(v, (int, float)): return float(v)
#     if isinstance(v, str):
#         s = v.strip()
#         if s == "": return None
#         try: return float(s)
#         except ValueError: return None
#     return None

# def to_bool(v):
#     if v is None: return None
#     if isinstance(v, bool): return v
#     if isinstance(v, (int, float)): return v != 0
#     if isinstance(v, str):
#         s = v.strip().lower()
#         if s in ("true", "yes", "1"): return True
#         if s in ("false", "no", "0", ""): return False
#     return None

# def to_int_or_null(v):
#     if v is None: return None
#     if isinstance(v, bool): return 1 if v else 0
#     if isinstance(v, int): return v
#     if isinstance(v, float): return int(v)
#     if isinstance(v, str):
#         s = v.strip()
#         if s == "": return None
#         try: return int(float(s))
#         except ValueError: return None
#     return None

# def apply_transform(src_field: str, tgt_field: str, action: str, value):
#     action = (action or "").strip().lower()
#     if action == "to_float": return to_float(value)
#     if action == "to_bool": return to_bool(value)
#     if action == "to_int_or_null": return to_int_or_null(value)
#     return value

# # ───── HELPERS ────────────────────────────────────────────────────────────────

# def coerce_int(v):
#     if isinstance(v, int): return v
#     if isinstance(v, float): return int(v)
#     if isinstance(v, str):
#         s = v.strip()
#         try: return int(float(s))
#         except ValueError: return None
#     return None

# def normalize_types_list(val):
#     if val is None: return []
#     if isinstance(val, list):
#         seen = set()
#         out = []
#         for x in val:
#             sx = str(x)
#             if sx not in seen:
#                 seen.add(sx)
#                 out.append(sx)
#         return out
#     return [str(val)]

# # ───── CORE TRANSFORM ─────────────────────────────────────────────────────────

# def transform_document(source_data: Dict) -> Dict:
#     mapping = FIELD_MAPPING
#     direct_copy = set(mapping.get("direct_copy", []))
#     renames: Dict[str, str] = mapping.get("rename", {})            # you added renames
#     transforms = mapping.get("transform", {})
#     defaults = mapping.get("default", {})
#     skip = set(mapping.get("skip", []))

#     out: Dict = {}

#     # 1) direct copy
#     for f in direct_copy:
#         if f in source_data:
#             out[f] = source_data[f]

#     # 2) renames (source -> target)
#     for src, tgt in renames.items():
#         if src in source_data:
#             out[tgt] = source_data[src]

#     # 3) transforms (write into renamed key if defined)
#     for src_field, tconf in transforms.items():
#         action = tconf.get("action") if isinstance(tconf, dict) else str(tconf)
#         raw_val = source_data.get(src_field, None)
#         tgt_field = renames.get(src_field, src_field)  # honor rename destination
#         out[tgt_field] = apply_transform(src_field, tgt_field, action, raw_val)

#     # 4) ensure skipped fields not present
#     for f in skip:
#         out.pop(f, None)

#     # 5) defaults ONLY if missing/empty
#     for k, v in defaults.items():
#         if k not in out or out[k] in (None, "", [], {}):
#             out[k] = v

#     # 6) normalize
#     if "types" in out:
#         out["types"] = normalize_types_list(out.get("types"))

#     # 7) stamps
#     iso_now = datetime.now(timezone.utc).isoformat()
#     out["migrated_from_explore"] = True
#     out["migrated_at"] = iso_now
#     out["content_updated_at"] = iso_now

#     return out

# # ───── TARGET STATE (DEDUPE + NEXT INDEX) ─────────────────────────────────────

# def get_target_state(parent_id: str) -> Tuple[Set[str], int]:
#     ref = db.collection(TARGET_COLLECTION).document(parent_id).collection(TARGET_SUBCOLLECTION)
#     docs = list(ref.stream())
#     existing_ids = {d.id for d in docs}

#     max_idx = None
#     for d in docs:
#         data = d.to_dict() or {}
#         idx = coerce_int(data.get("index"))
#     #  if idx is None, ignore
#         if idx is not None:
#             max_idx = idx if max_idx is None else max(max_idx, idx)

#     next_index = (max_idx + 1) if max_idx is not None else len(docs)
#     return existing_ids, next_index

# # ───── MIGRATE ONE PARENT ─────────────────────────────────────────────────────

# def migrate_parent_document(source_parent_id: str, target_parent_id: str,
#                             csv_writer, skip_writer) -> Tuple[int, int, int]:
#     label = COUNTRY_NAMES.get(source_parent_id, source_parent_id)
#     print(f"\n{'─'*70}")
#     print(f"📦 {label}: {SOURCE_COLLECTION}/{source_parent_id} → {TARGET_COLLECTION}/{target_parent_id}")

#     src_q = (db.collection(SOURCE_COLLECTION)
#                .document(source_parent_id)
#                .collection(SOURCE_SUBCOLLECTION))
#     source_docs = list(src_q.stream())
#     if not source_docs:
#         print("   ⚠️  No source docs found.")
#         return (0, 0, 0)

#     tgt_coll_ref = (db.collection(TARGET_COLLECTION)
#                       .document(target_parent_id)
#                       .collection(TARGET_SUBCOLLECTION))
#     existing_ids, next_index = get_target_state(target_parent_id)

#     total = len(source_docs)
#     migrated = 0
#     skipped = 0

#     batch = db.batch()
#     ops = 0

#     for sdoc in source_docs:
#         sdata = sdoc.to_dict() or {}

#         # Quality gate
#         raw_count = (
#             sdata.get("numRatings", None) if "numRatings" in sdata else
#             sdata.get("ratingCount", None) if "ratingCount" in sdata else
#             sdata.get("user_ratings_total", None)
#         )
#         rating_count = coerce_int(raw_count) or 0
#         if rating_count < MIN_NUM_RATINGS:
#             skipped += 1
#             print(f"   🚫 [{sdoc.id}] {sdata.get('name','Unknown')} skipped: low_ratings ({rating_count} < {MIN_NUM_RATINGS})")
#             if skip_writer:
#                 skip_writer.writerow([source_parent_id, target_parent_id, sdoc.id,
#                                       sdata.get("name", "Unknown"), "low_ratings"])
#             continue

#         # Dedupe key
#         candidate_id = (sdata.get("placeId") or "").strip() or sdoc.id
#         if candidate_id in existing_ids:
#             skipped += 1
#             print(f"   ⏭️  [{candidate_id}] exists → skipped")
#             if skip_writer:
#                 skip_writer.writerow([source_parent_id, target_parent_id, candidate_id,
#                                       sdata.get("name", "Unknown"), "already_exists"])
#             continue

#         tdata = transform_document(sdata)
#         tdata["source_url"] = "wanderlog"
#         tdata["index"] = next_index
#         next_index += 1

#         tgt_ref = tgt_coll_ref.document(candidate_id)

#         if DRY_RUN:
#             migrated += 1
#             preview = {
#                 "name": tdata.get("name"),
#                 "placeId": tdata.get("placeId"),
#                 "index": tdata.get("index"),
#                 "rating": tdata.get("rating"),
#                 "ratingCount": rating_count,
#                 "permanentlyClosed": tdata.get("permanentlyClosed"),
#                 "priceLevel": tdata.get("priceLevel"),
#                 "types": tdata.get("types"),
#             }
#             print(f"   🔍 DRY RUN → [{candidate_id}] {sdata.get('name','Unknown')} :: "
#                   f"{ {k:v for k,v in preview.items() if v is not None} }")
#         else:
#             batch.set(tgt_ref, tdata, merge=False)
#             ops += 1
#             migrated += 1
#             if ops >= BATCH_SIZE:
#                 batch.commit()
#                 print(f"   💾 committed batch of {ops}")
#                 batch = db.batch()
#                 ops = 0

#         if csv_writer:
#             csv_writer.writerow([
#                 source_parent_id, target_parent_id, candidate_id,
#                 sdata.get("name", "Unknown"),
#                 sdoc.reference.path, tgt_ref.path,
#                 "dry_run" if DRY_RUN else "insert"
#             ])

#     if not DRY_RUN and ops > 0:
#         batch.commit()
#         print(f"   💾 committed final batch of {ops}")

#     print(f"   Summary: {total} total | {migrated} {'(dry)' if DRY_RUN else ''} | {skipped} skipped")
#     return (total, migrated, skipped)

# # ───── BUILD ID MAP FROM COUNTRY_NAMES ────────────────────────────────────────

# def build_filtered_id_mapping() -> Dict[str, str]:
#     """Use the keys of COUNTRY_NAMES; require presence in source/target if configured."""
#     wanted_ids: List[str] = [str(k) for k in COUNTRY_NAMES.keys()]

#     # Check presence
#     src_ids = {d.id for d in db.collection(SOURCE_COLLECTION).stream()}
#     tgt_ids = {d.id for d in db.collection(TARGET_COLLECTION).stream()}

#     missing_in_source = [i for i in wanted_ids if i not in src_ids]
#     missing_in_target = [i for i in wanted_ids if i not in tgt_ids]

#     if missing_in_source:
#         print(f"⚠️  Missing in SOURCE: {missing_in_source}")
#     if missing_in_target:
#         print(f"⚠️  Missing in TARGET: {missing_in_target}")

#     if REQUIRE_BOTH_PARENTS:
#         valid = [i for i in wanted_ids if i in src_ids and i in tgt_ids]
#     else:
#         # Use whatever exists; map to same id if present in target
#         valid = [i for i in wanted_ids if i in src_ids]
#         valid = [i for i in valid if i in tgt_ids]

#     id_map = {i: i for i in valid}
#     return id_map

# # ───── MAIN ───────────────────────────────────────────────────────────────────

# def main():
#     print("🚀 Firestore Migration (dedup, next-index, quality-gated, filtered IDs)")
#     print(f"   Source: {SOURCE_COLLECTION}/{SOURCE_SUBCOLLECTION}")
#     print(f"   Target: {TARGET_COLLECTION}/{TARGET_SUBCOLLECTION}")
#     print(f"   Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
#     print(f"   Min ratings: {MIN_NUM_RATINGS}\n")

#     if not DRY_RUN:
#         confirm = input("⚠️  This will WRITE to Firestore. Type 'yes' to continue: ").strip().lower()
#         if confirm != "yes":
#             print("Cancelled.")
#             return

#     id_map = build_filtered_id_mapping()
#     if not id_map:
#         print("❌ No valid parent IDs to migrate (check presence or REQUIRE_BOTH_PARENTS).")
#         return

#     # Sorted for predictable order in logs
#     id_map = dict(sorted(id_map.items(), key=lambda kv: kv[0]))
#     print(f"📋 Parents to migrate ({len(id_map)}): {list(id_map.keys())}\n")

#     with open(CSV_LOG, "w", newline="", encoding="utf-8") as cf, \
#          open(SKIP_LOG, "w", newline="", encoding="utf-8") as sf:
#         csv_writer = csv.writer(cf)
#         skip_writer = csv.writer(sf)
#         csv_writer.writerow(["source_parent_id","target_parent_id","doc_id","name","source_path","target_path","status"])
#         skip_writer.writerow(["source_parent_id","target_parent_id","doc_id","name","reason"])

#         total_all = migrated_all = skipped_all = 0
#         for spid, tpid in id_map.items():
#             total, migrated, skipped = migrate_parent_document(spid, tpid, csv_writer, skip_writer)
#             total_all += total
#             migrated_all += migrated
#             skipped_all += skipped

#     print("\n" + "="*70)
#     print("🎉 MIGRATION COMPLETE")
#     print("="*70)
#     print(f"Total docs: {total_all}")
#     print(f"Migrated:   {migrated_all} {'(dry run)' if DRY_RUN else ''}")
#     print(f"Skipped:    {skipped_all}")
#     print("\n📊 Logs:")
#     print(f"   {CSV_LOG}")
#     print(f"   {SKIP_LOG}")
#     print("="*70)
#     if DRY_RUN:
#         print("\n⚠️  This was a DRY RUN. Set DRY_RUN = False to write.")

# if __name__ == "__main__":
#     main()
