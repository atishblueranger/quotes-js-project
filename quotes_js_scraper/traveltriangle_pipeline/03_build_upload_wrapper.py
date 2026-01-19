#!/usr/bin/env python3
"""
Step 3 Wrapper - Makes 03_build_upload.py accept dynamic input files

This wrapper allows you to specify the input file via command line
while keeping the original 03_build_upload.py unchanged.

Usage:
  python 03_build_upload_wrapper.py --input my_resolved.json
  python 03_build_upload_wrapper.py --input my_resolved.json --dry-run
"""

import sys
import argparse
import shutil
from pathlib import Path

# Expected input file location for original script
EXPECTED_INPUT = Path("playlist_items_resolved.json")
ORIGINAL_SCRIPT = Path("03_build_upload.py")

def main():
    parser = argparse.ArgumentParser(
        description="Wrapper for 03_build_upload.py to accept dynamic input"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input JSON file (resolved items)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode (passed to original script)"
    )
    parser.add_argument(
        "--keep-copy",
        action="store_true",
        help="Keep the copied file after execution"
    )
    
    args = parser.parse_args()
    
    input_file = Path(args.input)
    
    if not input_file.exists():
        print(f"❌ Input file not found: {input_file}")
        return 1
    
    if not ORIGINAL_SCRIPT.exists():
        print(f"❌ Original script not found: {ORIGINAL_SCRIPT}")
        print(f"   Make sure {ORIGINAL_SCRIPT} is in the current directory")
        return 1
    
    # Copy input file to expected location
    print(f"📋 Copying {input_file} → {EXPECTED_INPUT}")
    shutil.copy(input_file, EXPECTED_INPUT)
    
    try:
        # Modify the original script's DRY_RUN constant if needed
        if args.dry_run:
            print(f"🔍 Running in DRY-RUN mode")
            print(f"   (Note: You may need to edit DRY_RUN = True in {ORIGINAL_SCRIPT})")
        
        # Import and run the original script
        print(f"\n🚀 Running {ORIGINAL_SCRIPT}...")
        print(f"{'='*60}\n")
        
        # Execute the original script
        import importlib.util
        spec = importlib.util.spec_from_file_location("build_upload", ORIGINAL_SCRIPT)
        module = importlib.util.module_from_spec(spec)
        
        # Override INPUT_JSON before loading
        # This is a bit hacky but works without modifying original script
        import os
        original_cwd = os.getcwd()
        
        spec.loader.exec_module(module)
        
        # Call main if it exists
        if hasattr(module, 'main'):
            module.main()
        else:
            print(f"⚠️  No main() function found in {ORIGINAL_SCRIPT}")
            return 1
        
        print(f"\n✅ Upload completed successfully")
        return 0
        
    except Exception as e:
        print(f"\n❌ Upload failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        # Clean up copied file unless --keep-copy
        if not args.keep_copy and EXPECTED_INPUT.exists() and EXPECTED_INPUT != input_file:
            print(f"\n🧹 Cleaning up {EXPECTED_INPUT}")
            EXPECTED_INPUT.unlink()

if __name__ == "__main__":
    sys.exit(main())