import firebase_admin
from firebase_admin import credentials, firestore
from collections import defaultdict
import json

# ───── CONFIG ─────────────────────────────────────────────────────────────────

SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# Collections to compare
SOURCE_COLLECTION = "explore"
SOURCE_SUBCOLLECTION = "TouristAttractions"
TARGET_COLLECTION = "allplaces"
TARGET_SUBCOLLECTION = "top_attractions"

# Sample size for analysis
SAMPLE_SIZE = 5  # Docs to analyze per parent

# ───── INIT ───────────────────────────────────────────────────────────────────

cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
try:
    firebase_admin.get_app()
except ValueError:
    firebase_admin.initialize_app(cred)

db = firestore.client()

# ───── SCHEMA ANALYSIS ────────────────────────────────────────────────────────

def analyze_schema(collection_name: str, subcollection_name: str, sample_size: int = 5):
    """
    Analyze schema of a collection/subcollection.
    Returns field names, types, and sample values.
    """
    print(f"\n📊 Analyzing: {collection_name}/{subcollection_name}")
    
    # Get parent documents
    parent_docs = list(db.collection(collection_name).limit(10).stream())
    
    if not parent_docs:
        print(f"   ⚠️  No parent documents found in {collection_name}")
        return {}
    
    field_info = defaultdict(lambda: {
        "type": set(),
        "samples": [],
        "count": 0,
        "null_count": 0
    })
    
    total_docs = 0
    
    for parent in parent_docs:
        # Get subcollection documents
        subcoll_docs = list(
            parent.reference.collection(subcollection_name).limit(sample_size).stream()
        )
        
        for doc in subcoll_docs:
            total_docs += 1
            data = doc.to_dict() or {}
            
            for field, value in data.items():
                field_info[field]["count"] += 1
                
                # Track type
                if value is None:
                    field_info[field]["null_count"] += 1
                    field_info[field]["type"].add("null")
                else:
                    field_info[field]["type"].add(type(value).__name__)
                
                # Store sample values (first 3 unique)
                if len(field_info[field]["samples"]) < 3:
                    if value not in field_info[field]["samples"]:
                        field_info[field]["samples"].append(value)
    
    print(f"   Total documents analyzed: {total_docs}")
    print(f"   Unique fields found: {len(field_info)}")
    
    return dict(field_info), total_docs

def compare_schemas(source_schema, target_schema, source_total, target_total):
    """
    Compare two schemas and identify differences.
    """
    print(f"\n{'='*80}")
    print("📋 SCHEMA COMPARISON REPORT")
    print(f"{'='*80}")
    
    source_fields = set(source_schema.keys())
    target_fields = set(target_schema.keys())
    
    common_fields = source_fields & target_fields
    source_only = source_fields - target_fields
    target_only = target_fields - source_fields
    
    print(f"\n✅ Common fields ({len(common_fields)}):")
    for field in sorted(common_fields):
        src_types = ", ".join(sorted(source_schema[field]["type"]))
        tgt_types = ", ".join(sorted(target_schema[field]["type"]))
        
        type_match = "✓" if src_types == tgt_types else "⚠️  TYPE MISMATCH"
        
        print(f"   {field}")
        print(f"      Source: {src_types} (in {source_schema[field]['count']}/{source_total} docs)")
        print(f"      Target: {tgt_types} (in {target_schema[field]['count']}/{target_total} docs)")
        print(f"      {type_match}")
    
    print(f"\n⬅️  Fields ONLY in SOURCE ({len(source_only)}):")
    if source_only:
        for field in sorted(source_only):
            types = ", ".join(sorted(source_schema[field]["type"]))
            samples = source_schema[field]["samples"][:2]
            print(f"   • {field} ({types})")
            print(f"      Count: {source_schema[field]['count']}/{source_total} docs")
            print(f"      Samples: {samples}")
    else:
        print("   (None)")
    
    print(f"\n➡️  Fields ONLY in TARGET ({len(target_only)}):")
    if target_only:
        for field in sorted(target_only):
            types = ", ".join(sorted(target_schema[field]["type"]))
            samples = target_schema[field]["samples"][:2]
            print(f"   • {field} ({types})")
            print(f"      Count: {target_schema[field]['count']}/{target_total} docs")
            print(f"      Samples: {samples}")
    else:
        print("   (None)")
    
    return {
        "common": common_fields,
        "source_only": source_only,
        "target_only": target_only
    }

def generate_field_mapping(source_schema, target_schema, comparison):
    """
    Generate a field mapping configuration for migration.
    """
    print(f"\n{'='*80}")
    print("🔄 SUGGESTED FIELD MAPPING")
    print(f"{'='*80}")
    
    mapping = {
        "direct_copy": [],      # Fields that match exactly
        "rename": {},           # Fields to rename
        "transform": {},        # Fields needing transformation
        "skip": [],             # Source fields to skip
        "default": {}           # Target fields needing defaults
    }
    
    # Common fields - direct copy
    for field in sorted(comparison["common"]):
        src_types = source_schema[field]["type"]
        tgt_types = target_schema[field]["type"]
        
        if src_types == tgt_types:
            mapping["direct_copy"].append(field)
        else:
            mapping["transform"][field] = {
                "source_type": list(src_types),
                "target_type": list(tgt_types),
                "action": "needs_conversion"
            }
    
    # Source-only fields
    print("\n⚠️  SOURCE-ONLY FIELDS - Review these:")
    for field in sorted(comparison["source_only"]):
        print(f"   • {field}")
        print(f"      Keep? (will be copied to target)")
        print(f"      Skip? (will be ignored)")
        print(f"      Rename? (specify target field name)")
        mapping["skip"].append(field)  # Default: skip
    
    # Target-only fields
    print("\n⚠️  TARGET-ONLY FIELDS - These need defaults:")
    for field in sorted(comparison["target_only"]):
        types = list(target_schema[field]["type"])
        print(f"   • {field} ({', '.join(types)})")
        
        # Suggest defaults based on type
        if "str" in types:
            default_val = ""
        elif "int" in types or "float" in types:
            default_val = 0
        elif "list" in types:
            default_val = []
        elif "dict" in types:
            default_val = {}
        elif "bool" in types:
            default_val = False
        else:
            default_val = None
        
        mapping["default"][field] = default_val
        print(f"      Suggested default: {default_val}")
    
    # Save mapping to file
    mapping_file = "field_mapping_config.json"
    with open(mapping_file, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Field mapping saved to: {mapping_file}")
    print("   Review and edit this file before running migration!")
    
    return mapping

# ───── MAIN ───────────────────────────────────────────────────────────────────

def main():
    print("🔍 Firestore Schema Comparison Tool")
    print(f"\nSource: {SOURCE_COLLECTION}/{SOURCE_SUBCOLLECTION}")
    print(f"Target: {TARGET_COLLECTION}/{TARGET_SUBCOLLECTION}")
    
    # Analyze source schema
    source_schema, source_total = analyze_schema(
        SOURCE_COLLECTION, 
        SOURCE_SUBCOLLECTION, 
        SAMPLE_SIZE
    )
    
    # Analyze target schema
    target_schema, target_total = analyze_schema(
        TARGET_COLLECTION, 
        TARGET_SUBCOLLECTION, 
        SAMPLE_SIZE
    )
    
    if not source_schema or not target_schema:
        print("\n❌ Could not analyze schemas. Check your collection names.")
        return
    
    # Compare schemas
    comparison = compare_schemas(source_schema, target_schema, source_total, target_total)
    
    # Generate field mapping
    mapping = generate_field_mapping(source_schema, target_schema, comparison)
    
    print(f"\n{'='*80}")
    print("✅ NEXT STEPS:")
    print(f"{'='*80}")
    print("1. Review the schema comparison above")
    print("2. Edit 'field_mapping_config.json' to customize the mapping:")
    print("   - Move fields from 'skip' to 'direct_copy' if you want to keep them")
    print("   - Add rename rules in 'rename' section")
    print("   - Adjust default values for target-only fields")
    print("3. Run the migration script with your customized mapping")

if __name__ == "__main__":
    main()