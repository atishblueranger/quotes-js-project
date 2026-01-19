import argparse
import json
import sys
from pathlib import Path
from typing import List, Dict, Any, Iterable
import csv

# =========================
# CONFIG (edit these)
# =========================
# Put your default input file path here. Example:
# CONFIG_INPUT_PATH = r"C:\dev\data\traveltriangle_blogs_1_24.json"
CONFIG_INPUT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\traveltriangle_blog.json"  # leave empty to require CLI

CONFIG_DEFAULT_COUNTRY = "India"   # default country if --country not passed
CONFIG_DEFAULT_DEDUPE = True       # default dedupe behavior if flag not passed


def load_items(path: Path) -> List[Dict[str, Any]]:
    """
    Loads either a JSON array file or a JSONL (one object per line) file.
    """
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    # Try JSON array first
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return data
        raise ValueError("JSON root is not a list")
    except Exception:
        # Fallback: treat it as JSONL
        items = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                items.append(json.loads(line))
        return items


def normalize_country(s: str) -> str:
    return (s or "").strip().lower()


def filter_items(items: Iterable[Dict[str, Any]], target_country: str) -> List[Dict[str, Any]]:
    t = normalize_country(target_country)
    out = []
    for it in items:
        ctry = normalize_country(it.get("country", ""))
        if ctry == t:
            out.append(it)
    return out


def dedupe_by_url(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    unique = []
    for it in items:
        u = (it.get("source_url") or "").rstrip("/").lower()
        if not u or u in seen:
            continue
        seen.add(u)
        unique.append(it)
    return unique


def save_json(path: Path, items: List[Dict[str, Any]]) -> None:
    path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def save_jsonl(path: Path, items: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


def save_csv(path: Path, items: List[Dict[str, Any]]) -> None:
    fields = ["title", "category", "source_url", "city", "country"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for it in items:
            w.writerow({k: it.get(k, "") for k in fields})


def resolve_input_path(cli_input: str | None) -> Path:
    """
    Precedence: CLI --input > CONFIG_INPUT_PATH.
    """
    if cli_input:
        return Path(cli_input)
    if CONFIG_INPUT_PATH:
        return Path(CONFIG_INPUT_PATH)
    print(
        "ERROR: No input file provided. Either set CONFIG_INPUT_PATH in the script "
        "or pass --input path\\to\\file.json",
        file=sys.stderr,
    )
    sys.exit(2)


def main():
    p = argparse.ArgumentParser(description="Filter scraped entries by country.")
    p.add_argument("--input", "-i", help="Path to input JSON (array) or JSONL file")
    p.add_argument("--output", "-o", required=True,
                   help="Path to output (uses extension: .json | .jsonl | .csv)")
    p.add_argument("--country", "-c", default=CONFIG_DEFAULT_COUNTRY,
                   help=f"Country to keep (default: {CONFIG_DEFAULT_COUNTRY})")
    p.add_argument("--dedupe", action="store_true",
                   help=f"Enable dedupe (default: {CONFIG_DEFAULT_DEDUPE})")
    p.add_argument("--no-dedupe", action="store_true",
                   help=f"Disable dedupe (overrides default {CONFIG_DEFAULT_DEDUPE})")
    args = p.parse_args()

    in_path = resolve_input_path(args.input)
    out_path = Path(args.output)

    # Resolve dedupe behavior (CLI flags override config default)
    if args.no_dedupe:
        do_dedupe = False
    elif args.dedupe:
        do_dedupe = True
    else:
        do_dedupe = CONFIG_DEFAULT_DEDUPE

    try:
        items = load_items(in_path)
    except Exception as e:
        print(f"Failed to read {in_path}: {e}", file=sys.stderr)
        sys.exit(1)

    filtered = filter_items(items, args.country)
    if do_dedupe:
        filtered = dedupe_by_url(filtered)

    ext = out_path.suffix.lower()
    if ext == ".jsonl":
        save_jsonl(out_path, filtered)
    elif ext == ".csv":
        save_csv(out_path, filtered)
    else:
        save_json(out_path, filtered)

    print(f"Kept {len(filtered)} entries for country='{args.country}'. Wrote: {out_path}")


if __name__ == "__main__":
    main()
