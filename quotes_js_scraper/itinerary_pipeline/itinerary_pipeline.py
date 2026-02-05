#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Master runner: Extract → Resolve → Consensus → Route/Sections → Upload

Examples:
  python itinerary_pipeline.py --urls sources.txt --city "Jaipur" --state "Rajasthan" --llm-extract
  python itinerary_pipeline.py --url "..." --url "..." --city "New Delhi" --start-from resolve --stop-after route
"""

import argparse
import subprocess
from pathlib import Path
from typing import List, Optional
from datetime import datetime

def run(cmd: List[str], name: str) -> None:
    print("\n" + "="*70)
    print(name)
    print("="*70)
    print(" ".join(cmd) + "\n")
    subprocess.run(cmd, check=True)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", action="append", default=[])
    p.add_argument("--urls", help="file with URLs")
    p.add_argument("--city", required=True)
    p.add_argument("--state", default="")
    p.add_argument("--country", default="India")

    p.add_argument("--out-dir", default="itinerary_output")
    p.add_argument("--run-id", default="")
    p.add_argument("--start-from", choices=["extract","resolve","consensus","route","upload"], default="extract")
    p.add_argument("--stop-after", choices=["extract","resolve","consensus","route","upload"], default="upload")

    p.add_argument("--llm-extract", action="store_true")
    p.add_argument("--llm-model", default="gpt-5-mini")

    p.add_argument("--min-confidence", type=float, default=0.80)
    p.add_argument("--keep-ratio", type=float, default=1.0)

    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    # Decide run_id
    run_id = args.run_id.strip()
    if not run_id:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_city = "".join([c.lower() if c.isalnum() else "_" for c in args.city]).strip("_")
        run_id = f"{safe_city}_1day_{ts}"

    run_dir = Path(args.out_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    order = ["extract","resolve","consensus","route","upload"]
    start_i = order.index(args.start_from)
    stop_i = order.index(args.stop_after)

    # Step 1
    if start_i <= 0 <= stop_i:
        cmd = [
            "python", "itinerary_01_extract.py",
            "--city", args.city,
            "--state", args.state,
            "--country", args.country,
            "--out-dir", args.out_dir,
            "--run-id", run_id,
            "--llm-model", args.llm_model,
        ]
        for u in args.url:
            cmd += ["--url", u]
        if args.urls:
            cmd += ["--urls", args.urls]
        if args.llm_extract:
            cmd += ["--llm-extract"]
        run(cmd, "Step 1: Extract")

    # Step 2
    if start_i <= 1 <= stop_i:
        cmd = [
            "python", "itinerary_02_resolve.py",
            "--run-dir", str(run_dir),
            "--min-confidence", str(args.min_confidence),
            "--llm-model", args.llm_model,
        ]
        run(cmd, "Step 2: Resolve")

    # Step 3
    if start_i <= 2 <= stop_i:
        cmd = [
            "python", "itinerary_03_consensus.py",
            "--run-dir", str(run_dir),
            "--llm-model", args.llm_model,
            "--keep-ratio", str(args.keep_ratio),
        ]
        run(cmd, "Step 3: Consensus")

    # Step 4
    if start_i <= 3 <= stop_i:
        cmd = [
            "python", "itinerary_04_route_sections.py",
            "--run-dir", str(run_dir),
            "--llm-model", args.llm_model,
        ]
        run(cmd, "Step 4: Route + Sections")

    # Step 5
    if start_i <= 4 <= stop_i and (not args.skip_upload):
        cmd = [
            "python", "itinerary_05_upload.py",
            "--run-dir", str(run_dir),
        ]
        if args.dry_run:
            cmd += ["--dry-run"]
        run(cmd, "Step 5: Upload")

    print("\n✅ Pipeline done.")
    print(f"Run dir: {run_dir}")

if __name__ == "__main__":
    main()
