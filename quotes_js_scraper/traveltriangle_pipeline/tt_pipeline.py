#!/usr/bin/env python3
"""
TravelTriangle Full Pipeline Runner
Orchestrates the complete flow: Extract → Adapt → Resolve → Upload

Usage:
  # Single URL
  python tt_pipeline.py --url "https://..." --city "Karnataka"
  
  # Multiple URLs from file
  python tt_pipeline.py --batch urls.txt --city "Karnataka"
  
  # Dry run with quality filtering (Keep top 80%)
  python tt_pipeline.py --url "https://..." --city "Delhi" --keep-ratio 0.8 --dry-run
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import List, Optional
from datetime import datetime

# ==================== CONFIG ====================
STEP_1_SCRIPT = "traveltriangle_01_extract.py"
STEP_1_5_SCRIPT = "traveltriangle_01_5_adapter.py"
STEP_2_5_SCRIPT = "02_5_resolve_validate.py"
STEP_3_SCRIPT = "03_build_upload.py"

OUTPUT_DIR = Path("tt_pipeline_output")
OUTPUT_DIR.mkdir(exist_ok=True)

# ==================== HELPERS ====================
def run_command(cmd: List[str], step_name: str) -> bool:
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"🚀 {step_name}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd)}\n")
    
    try:
        # Use shell=True on Windows if needed, but list format is safer usually
        # For Python to Python calls, straightforward execution usually works
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=False,  # Show output in real-time
            text=True
        )
        print(f"\n✅ {step_name} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {step_name} failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"\n❌ {step_name} failed: {e}")
        return False

def url_to_basename(url: str) -> str:
    """Convert URL to safe filename."""
    import re
    basename = url.split('/')[-1].replace('.html', '').replace('.htm', '')
    basename = re.sub(r'[^a-z0-9]+', '_', basename.lower())
    return basename[:50] or 'playlist'

def read_urls_from_file(file_path: str) -> List[str]:
    """Read URLs from text file (one per line)."""
    urls = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls

# ==================== PIPELINE STEPS ====================
def step_1_extract(url: str, city: str, country: str, category: str, subtype: str,
                   optimize_ai: bool, title_mode: str, output_file: str) -> bool:
    """Step 1: Extract from TravelTriangle."""
    cmd = [
        "python", STEP_1_SCRIPT,
        "--url", url,
        "--city", city,
        "--country", country,
        "--category", category,
        "--subtype", subtype,
        "--title-mode", title_mode,
        "--out", output_file
    ]
    
    if optimize_ai:
        cmd.append("--optimize-ai")
    
    return run_command(cmd, "Step 1: Extract & AI Optimize")

def step_1_5_adapt(input_file: str, output_file: str) -> bool:
    """Step 1.5: Convert format for resolve_validate."""
    cmd = [
        "python", STEP_1_5_SCRIPT,
        "--in", input_file,
        "--out", output_file
    ]
    
    return run_command(cmd, "Step 1.5: Format Adapter")

def step_2_5_resolve(input_file: str, output_file: str, report_file: str,
                     min_confidence: float, keep_ratio: float, anchor_city: Optional[str],
                     anchor_state: Optional[str], refresh_photos: bool,
                     require_photo: bool) -> bool:
    """Step 2.5: Resolve with Google Places."""
    cmd = [
        "python", STEP_2_5_SCRIPT,
        "--in", input_file,
        "--out", output_file,
        "--report", report_file,
        "--min-confidence", str(min_confidence),
        "--keep-ratio", str(keep_ratio) # <--- NEW PARAM PASSED HERE
    ]
    
    if anchor_city:
        cmd.extend(["--anchor-city", anchor_city])
    if anchor_state:
        cmd.extend(["--anchor-state", anchor_state])
    if refresh_photos:
        cmd.append("--refresh-photos")
    if require_photo:
        cmd.append("--require-photo")
    
    return run_command(cmd, "Step 2.5: Resolve & Validate")

def step_3_upload(input_file: str, dry_run: bool, filter_publishable: bool) -> bool:
    """Step 3: Build & Upload to Firestore."""
    
    # Quick workaround: copy resolved file to expected location for Step 3
    import shutil
    expected_path = Path("playlist_items_resolved.json")
    shutil.copy(input_file, expected_path)
    
    cmd = ["python", STEP_3_SCRIPT]
    
    success = run_command(cmd, "Step 3: Build & Upload to Firestore")
    
    # Clean up
    if expected_path.exists() and expected_path != Path(input_file):
        expected_path.unlink()
    
    return success

# ==================== MAIN ====================
def main():
    parser = argparse.ArgumentParser(
        description="TravelTriangle Full Pipeline: Extract → Resolve → Upload",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    # Input mode
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--url", help="Single TravelTriangle URL")
    input_group.add_argument("--batch", help="Text file with URLs (one per line)")
    
    # Location
    parser.add_argument("--city", required=True, help="City/region name")
    parser.add_argument("--state", help="State name (for better Google Places matching)")
    parser.add_argument("--country", default="India", help="Country name")
    
    # Classification
    parser.add_argument("--category", default="Travel", help="Top-level category")
    parser.add_argument("--subtype", choices=["poi", "destination"], default="poi", 
                       help="Playlist subtype")
    
    # AI optimization
    parser.add_argument("--optimize-ai", action="store_true", 
                       help="Use AI for title/description optimization")
    parser.add_argument("--title-mode", choices=["simple", "catchy"], default="catchy",
                       help="Title mode: simple (remove numbers) or catchy (AI-generated)")
    
    # Resolution
    parser.add_argument("--min-confidence", type=float, default=0.80,
                       help="Minimum confidence for Google Places match (0.0-1.0)")
    
    # --- NEW ARGUMENT ---
    parser.add_argument("--keep-ratio", type=float, default=1.0,
                       help="Fraction of items to keep based on quality score (0.0-1.0). Default 1.0 (keep all).")
    # --------------------

    parser.add_argument("--refresh-photos", action="store_true",
                       help="Re-fetch photos even if cached")
    parser.add_argument("--require-photo", action="store_true",
                       help="Reject places without photos")
    
    # Upload
    parser.add_argument("--dry-run", action="store_true",
                       help="Run pipeline but don't upload to Firestore")
    parser.add_argument("--skip-upload", action="store_true",
                       help="Stop after resolution (don't run Step 3)")
    
    # Output
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR),
                       help=f"Output directory (default: {OUTPUT_DIR})")
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # Determine URLs to process
    urls = []
    if args.url:
        urls = [args.url]
    elif args.batch:
        if not Path(args.batch).exists():
            print(f"❌ Batch file not found: {args.batch}")
            return 1
        urls = read_urls_from_file(args.batch)
        print(f"📋 Loaded {len(urls)} URLs from {args.batch}")
    
    if not urls:
        print("❌ No URLs to process")
        return 1
    
    # Process each URL
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_success = True
    results = []
    
    for idx, url in enumerate(urls, 1):
        print(f"\n{'#'*60}")
        print(f"# Processing URL {idx}/{len(urls)}")
        print(f"# {url}")
        print(f"{'#'*60}\n")
        
        basename = url_to_basename(url)
        prefix = f"{basename}_{timestamp}" if len(urls) > 1 else timestamp
        
        # File paths
        extracted_file = output_dir / f"{prefix}_01_extracted.json"
        adapted_file = output_dir / f"{prefix}_02_adapted.json"
        resolved_file = output_dir / f"{prefix}_03_resolved.json"
        report_file = output_dir / f"{prefix}_03_report.json"
        
        result = {
            "url": url,
            "basename": basename,
            "success": False,
            "steps_completed": []
        }
        
        # Step 1: Extract
        if not step_1_extract(
            url=url,
            city=args.city,
            country=args.country,
            category=args.category,
            subtype=args.subtype,
            optimize_ai=args.optimize_ai,
            title_mode=args.title_mode,
            output_file=str(extracted_file)
        ):
            print(f"\n❌ Pipeline failed at Step 1 for: {url}")
            all_success = False
            results.append(result)
            continue
        
        result["steps_completed"].append("extract")
        
        # Step 1.5: Adapt format
        if not step_1_5_adapt(
            input_file=str(extracted_file),
            output_file=str(adapted_file)
        ):
            print(f"\n❌ Pipeline failed at Step 1.5 for: {url}")
            all_success = False
            results.append(result)
            continue
        
        result["steps_completed"].append("adapt")
        
        # Step 2.5: Resolve (Updated with keep_ratio)
        if not step_2_5_resolve(
            input_file=str(adapted_file),
            output_file=str(resolved_file),
            report_file=str(report_file),
            min_confidence=args.min_confidence,
            keep_ratio=args.keep_ratio,  # <--- PASSED HERE
            anchor_city=args.city,
            anchor_state=args.state,
            refresh_photos=args.refresh_photos,
            require_photo=args.require_photo
        ):
            print(f"\n❌ Pipeline failed at Step 2.5 for: {url}")
            all_success = False
            results.append(result)
            continue
        
        result["steps_completed"].append("resolve")
        
        # Print resolution summary AND CHECK MIN ITEMS
        can_upload = True
        if report_file.exists():
            try:
                report = json.loads(report_file.read_text(encoding="utf-8"))
                summary = report.get("summary", {})
                totals = report.get("totals", {})
                
                pub_count = totals.get("publishable", 0)
                print(f"\n📊 Resolution Summary:")
                print(f"   Publishable Items: {pub_count}")
                
                # --- NEW: MIN ITEM CHECK ---
                if pub_count < 5:
                    print(f"⚠️  SKIPPING UPLOAD: Only {pub_count} items (Minimum required: 5)")
                    can_upload = False
                    result["status_note"] = "Skipped (Low item count)"
                # ---------------------------
            except Exception:
                pass
        
        # Step 3: Upload (unless skipped or dry-run)
        if not args.skip_upload and can_upload:  # <--- Added 'and can_upload'
            if args.dry_run:
                print(f"\n🔍 DRY RUN: Skipping upload to Firestore")
                result["steps_completed"].append("upload (dry-run)")
            else:
                if not step_3_upload(
                    input_file=str(resolved_file),
                    dry_run=args.dry_run,
                    filter_publishable=True
                ):
                    print(f"\n❌ Pipeline failed at Step 3 for: {url}")
                    all_success = False
                    results.append(result)
                    continue
                
                result["steps_completed"].append("upload")
        # # Print resolution summary
        # if report_file.exists():
        #     try:
        #         report = json.loads(report_file.read_text(encoding="utf-8"))
        #         summary = report.get("summary", {})
        #         print(f"\n📊 Resolution Summary:")
        #         print(f"   Total items: {summary.get('total_items', 0)}")
        #         print(f"   Success rate: {summary.get('success_rate', 0):.1f}%")
        #         print(f"   Publishable rate: {summary.get('publishable_rate', 0):.1f}%")
        #     except Exception:
        #         pass
        
        # # Step 3: Upload (unless skipped or dry-run)
        # if not args.skip_upload:
        #     if args.dry_run:
        #         print(f"\n🔍 DRY RUN: Skipping upload to Firestore")
        #         result["steps_completed"].append("upload (dry-run)")
        #     else:
        #         if not step_3_upload(
        #             input_file=str(resolved_file),
        #             dry_run=args.dry_run,
        #             filter_publishable=True
        #         ):
        #             print(f"\n❌ Pipeline failed at Step 3 for: {url}")
        #             all_success = False
        #             results.append(result)
        #             continue
                
        #         result["steps_completed"].append("upload")
        
        result["success"] = True
        result["extracted_file"] = str(extracted_file)
        result["resolved_file"] = str(resolved_file)
        result["report_file"] = str(report_file)
        results.append(result)
        
        print(f"\n✅ Pipeline completed successfully for: {url}")
    
    # Final summary
    print(f"\n{'='*60}")
    print(f"PIPELINE SUMMARY")
    print(f"{'='*60}\n")
    
    successful = sum(1 for r in results if r["success"])
    print(f"URLs processed: {len(urls)}")
    print(f"Successful: {successful}")
    print(f"Failed: {len(urls) - successful}")
    
    if results:
        print(f"\nDetailed results:")
        for r in results:
            status = "✅" if r["success"] else "❌"
            steps = " → ".join(r["steps_completed"]) if r["steps_completed"] else "none"
            print(f"  {status} {r['basename']}: {steps}")
    
    # Save results summary
    summary_file = output_dir / f"pipeline_summary_{timestamp}.json"
    summary_file.write_text(
        json.dumps({
            "timestamp": timestamp,
            "args": vars(args),
            "results": results
        }, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"\n📝 Summary saved to: {summary_file}")
    
    return 0 if all_success else 1

if __name__ == "__main__":
    sys.exit(main())


# #!/usr/bin/env python3
# """
# TravelTriangle Full Pipeline Runner
# Orchestrates the complete flow: Extract → Adapt → Resolve → Upload

# Usage:
#   # Single URL
#   python tt_pipeline.py --url "https://..." --city "Karnataka"
  
#   # Multiple URLs from file
#   python tt_pipeline.py --batch urls.txt --city "Karnataka"
  
#   # Dry run (no Firestore upload)
#   python tt_pipeline.py --url "https://..." --city "Karnataka" --dry-run
# """

# import argparse
# import json
# import subprocess
# import sys
# from pathlib import Path
# from typing import List, Optional
# from datetime import datetime

# # ==================== CONFIG ====================
# STEP_1_SCRIPT = "traveltriangle_01_extract.py"
# STEP_1_5_SCRIPT = "traveltriangle_01_5_adapter.py"
# STEP_2_5_SCRIPT = "02_5_resolve_validate.py"
# STEP_3_SCRIPT = "03_build_upload.py"

# OUTPUT_DIR = Path("tt_pipeline_output")
# OUTPUT_DIR.mkdir(exist_ok=True)

# # ==================== HELPERS ====================
# def run_command(cmd: List[str], step_name: str) -> bool:
#     """Run a command and handle errors."""
#     print(f"\n{'='*60}")
#     print(f"🚀 {step_name}")
#     print(f"{'='*60}")
#     print(f"Command: {' '.join(cmd)}\n")
    
#     try:
#         result = subprocess.run(
#             cmd,
#             check=True,
#             capture_output=False,  # Show output in real-time
#             text=True
#         )
#         print(f"\n✅ {step_name} completed successfully")
#         return True
#     except subprocess.CalledProcessError as e:
#         print(f"\n❌ {step_name} failed with exit code {e.returncode}")
#         return False
#     except Exception as e:
#         print(f"\n❌ {step_name} failed: {e}")
#         return False

# def url_to_basename(url: str) -> str:
#     """Convert URL to safe filename."""
#     import re
#     basename = url.split('/')[-1].replace('.html', '').replace('.htm', '')
#     basename = re.sub(r'[^a-z0-9]+', '_', basename.lower())
#     return basename[:50] or 'playlist'

# def read_urls_from_file(file_path: str) -> List[str]:
#     """Read URLs from text file (one per line)."""
#     urls = []
#     with open(file_path, 'r') as f:
#         for line in f:
#             line = line.strip()
#             if line and not line.startswith('#'):
#                 urls.append(line)
#     return urls

# # ==================== PIPELINE STEPS ====================
# def step_1_extract(url: str, city: str, country: str, category: str, subtype: str,
#                    optimize_ai: bool, title_mode: str, output_file: str) -> bool:
#     """Step 1: Extract from TravelTriangle."""
#     cmd = [
#         "python", STEP_1_SCRIPT,
#         "--url", url,
#         "--city", city,
#         "--country", country,
#         "--category", category,
#         "--subtype", subtype,
#         "--title-mode", title_mode,
#         "--out", output_file
#     ]
    
#     if optimize_ai:
#         cmd.append("--optimize-ai")
    
#     return run_command(cmd, "Step 1: Extract & AI Optimize")

# def step_1_5_adapt(input_file: str, output_file: str) -> bool:
#     """Step 1.5: Convert format for resolve_validate."""
#     cmd = [
#         "python", STEP_1_5_SCRIPT,
#         "--in", input_file,
#         "--out", output_file
#     ]
    
#     return run_command(cmd, "Step 1.5: Format Adapter")

# def step_2_5_resolve(input_file: str, output_file: str, report_file: str,
#                      min_confidence: float, anchor_city: Optional[str],
#                      anchor_state: Optional[str], refresh_photos: bool,
#                      require_photo: bool) -> bool:
#     """Step 2.5: Resolve with Google Places."""
#     cmd = [
#         "python", STEP_2_5_SCRIPT,
#         "--in", input_file,
#         "--out", output_file,
#         "--report", report_file,
#         "--min-confidence", str(min_confidence)
#     ]
    
#     if anchor_city:
#         cmd.extend(["--anchor-city", anchor_city])
#     if anchor_state:
#         cmd.extend(["--anchor-state", anchor_state])
#     if refresh_photos:
#         cmd.append("--refresh-photos")
#     if require_photo:
#         cmd.append("--require-photo")
    
#     return run_command(cmd, "Step 2.5: Resolve & Validate")

# def step_3_upload(input_file: str, dry_run: bool, filter_publishable: bool) -> bool:
#     """Step 3: Build & Upload to Firestore."""
#     # Note: Step 3 script needs to be modified to accept these args
#     # For now, it reads from hardcoded INPUT_JSON
    
#     # We'll need to temporarily modify the script's INPUT_JSON constant
#     # or copy the file to the expected location
    
#     # Quick workaround: copy resolved file to expected location
#     import shutil
#     expected_path = Path("playlist_items_resolved.json")
#     shutil.copy(input_file, expected_path)
    
#     cmd = ["python", STEP_3_SCRIPT]
    
#     success = run_command(cmd, "Step 3: Build & Upload to Firestore")
    
#     # Clean up
#     if expected_path.exists() and expected_path != Path(input_file):
#         expected_path.unlink()
    
#     return success

# # ==================== MAIN ====================
# def main():
#     parser = argparse.ArgumentParser(
#         description="TravelTriangle Full Pipeline: Extract → Resolve → Upload",
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#         epilog="""
# Examples:
#   # Single URL
#   python tt_pipeline.py --url "https://..." --city "Karnataka"
  
#   # Multiple URLs from file
#   python tt_pipeline.py --batch urls.txt --city "Karnataka"
  
#   # With state and AI optimization
#   python tt_pipeline.py --url "https://..." --city "Mysore" --state "Karnataka" --optimize-ai
  
#   # Dry run (no upload)
#   python tt_pipeline.py --url "https://..." --city "Karnataka" --dry-run
#         """
#     )
    
#     # Input mode
#     input_group = parser.add_mutually_exclusive_group(required=True)
#     input_group.add_argument("--url", help="Single TravelTriangle URL")
#     input_group.add_argument("--batch", help="Text file with URLs (one per line)")
    
#     # Location
#     parser.add_argument("--city", required=True, help="City/region name")
#     parser.add_argument("--state", help="State name (for better Google Places matching)")
#     parser.add_argument("--country", default="India", help="Country name")
    
#     # Classification
#     parser.add_argument("--category", default="Travel", help="Top-level category")
#     parser.add_argument("--subtype", choices=["poi", "destination"], default="poi", 
#                        help="Playlist subtype")
    
#     # AI optimization
#     parser.add_argument("--optimize-ai", action="store_true", 
#                        help="Use AI for title/description optimization")
#     parser.add_argument("--title-mode", choices=["simple", "catchy"], default="catchy",
#                        help="Title mode: simple (remove numbers) or catchy (AI-generated)")
    
#     # Resolution
#     parser.add_argument("--min-confidence", type=float, default=0.80,
#                        help="Minimum confidence for Google Places match (0.0-1.0)")
#     parser.add_argument("--refresh-photos", action="store_true",
#                        help="Re-fetch photos even if cached")
#     parser.add_argument("--require-photo", action="store_true",
#                        help="Reject places without photos")
    
#     # Upload
#     parser.add_argument("--dry-run", action="store_true",
#                        help="Run pipeline but don't upload to Firestore")
#     parser.add_argument("--skip-upload", action="store_true",
#                        help="Stop after resolution (don't run Step 3)")
    
#     # Output
#     parser.add_argument("--output-dir", default=str(OUTPUT_DIR),
#                        help=f"Output directory (default: {OUTPUT_DIR})")
    
#     args = parser.parse_args()
    
#     output_dir = Path(args.output_dir)
#     output_dir.mkdir(exist_ok=True, parents=True)
    
#     # Determine URLs to process
#     urls = []
#     if args.url:
#         urls = [args.url]
#     elif args.batch:
#         if not Path(args.batch).exists():
#             print(f"❌ Batch file not found: {args.batch}")
#             return 1
#         urls = read_urls_from_file(args.batch)
#         print(f"📋 Loaded {len(urls)} URLs from {args.batch}")
    
#     if not urls:
#         print("❌ No URLs to process")
#         return 1
    
#     # Process each URL
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     all_success = True
#     results = []
    
#     for idx, url in enumerate(urls, 1):
#         print(f"\n{'#'*60}")
#         print(f"# Processing URL {idx}/{len(urls)}")
#         print(f"# {url}")
#         print(f"{'#'*60}\n")
        
#         basename = url_to_basename(url)
#         prefix = f"{basename}_{timestamp}" if len(urls) > 1 else timestamp
        
#         # File paths
#         extracted_file = output_dir / f"{prefix}_01_extracted.json"
#         adapted_file = output_dir / f"{prefix}_02_adapted.json"
#         resolved_file = output_dir / f"{prefix}_03_resolved.json"
#         report_file = output_dir / f"{prefix}_03_report.json"
        
#         result = {
#             "url": url,
#             "basename": basename,
#             "success": False,
#             "steps_completed": []
#         }
        
#         # Step 1: Extract
#         if not step_1_extract(
#             url=url,
#             city=args.city,
#             country=args.country,
#             category=args.category,
#             subtype=args.subtype,
#             optimize_ai=args.optimize_ai,
#             title_mode=args.title_mode,
#             output_file=str(extracted_file)
#         ):
#             print(f"\n❌ Pipeline failed at Step 1 for: {url}")
#             all_success = False
#             results.append(result)
#             continue
        
#         result["steps_completed"].append("extract")
        
#         # Step 1.5: Adapt format
#         if not step_1_5_adapt(
#             input_file=str(extracted_file),
#             output_file=str(adapted_file)
#         ):
#             print(f"\n❌ Pipeline failed at Step 1.5 for: {url}")
#             all_success = False
#             results.append(result)
#             continue
        
#         result["steps_completed"].append("adapt")
        
#         # Step 2.5: Resolve
#         if not step_2_5_resolve(
#             input_file=str(adapted_file),
#             output_file=str(resolved_file),
#             report_file=str(report_file),
#             min_confidence=args.min_confidence,
#             anchor_city=args.city,
#             anchor_state=args.state,
#             refresh_photos=args.refresh_photos,
#             require_photo=args.require_photo
#         ):
#             print(f"\n❌ Pipeline failed at Step 2.5 for: {url}")
#             all_success = False
#             results.append(result)
#             continue
        
#         result["steps_completed"].append("resolve")
        
#         # Print resolution summary
#         if report_file.exists():
#             try:
#                 report = json.loads(report_file.read_text(encoding="utf-8"))
#                 summary = report.get("summary", {})
#                 print(f"\n📊 Resolution Summary:")
#                 print(f"   Total items: {summary.get('total_items', 0)}")
#                 print(f"   Success rate: {summary.get('success_rate', 0):.1f}%")
#                 print(f"   Publishable rate: {summary.get('publishable_rate', 0):.1f}%")
#             except Exception:
#                 pass
        
#         # Step 3: Upload (unless skipped or dry-run)
#         if not args.skip_upload:
#             if args.dry_run:
#                 print(f"\n🔍 DRY RUN: Skipping upload to Firestore")
#                 result["steps_completed"].append("upload (dry-run)")
#             else:
#                 if not step_3_upload(
#                     input_file=str(resolved_file),
#                     dry_run=args.dry_run,
#                     filter_publishable=True
#                 ):
#                     print(f"\n❌ Pipeline failed at Step 3 for: {url}")
#                     all_success = False
#                     results.append(result)
#                     continue
                
#                 result["steps_completed"].append("upload")
        
#         result["success"] = True
#         result["extracted_file"] = str(extracted_file)
#         result["resolved_file"] = str(resolved_file)
#         result["report_file"] = str(report_file)
#         results.append(result)
        
#         print(f"\n✅ Pipeline completed successfully for: {url}")
    
#     # Final summary
#     print(f"\n{'='*60}")
#     print(f"PIPELINE SUMMARY")
#     print(f"{'='*60}\n")
    
#     successful = sum(1 for r in results if r["success"])
#     print(f"URLs processed: {len(urls)}")
#     print(f"Successful: {successful}")
#     print(f"Failed: {len(urls) - successful}")
    
#     if results:
#         print(f"\nDetailed results:")
#         for r in results:
#             status = "✅" if r["success"] else "❌"
#             steps = " → ".join(r["steps_completed"]) if r["steps_completed"] else "none"
#             print(f"  {status} {r['basename']}: {steps}")
    
#     # Save results summary
#     summary_file = output_dir / f"pipeline_summary_{timestamp}.json"
#     summary_file.write_text(
#         json.dumps({
#             "timestamp": timestamp,
#             "args": vars(args),
#             "results": results
#         }, ensure_ascii=False, indent=2),
#         encoding="utf-8"
#     )
#     print(f"\n📝 Summary saved to: {summary_file}")
    
#     return 0 if all_success else 1

# if __name__ == "__main__":
#     sys.exit(main())