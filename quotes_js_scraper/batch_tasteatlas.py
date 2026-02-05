#!/usr/bin/env python
"""
Batch scraper for TasteAtlas URLs.
Place this file in your scrapy_selenium project root.

Usage:
    python batch_tasteatlas.py
    python batch_tasteatlas.py --max-dishes 30 --delay 5
    python batch_tasteatlas.py --start-from 10  # Resume from index 10
"""

import subprocess
import json
import time
import os
import sys
from datetime import datetime


# Configuration
URLS_FILE = "urls_tasteatlas.json"
OUTPUT_DIR = "tasteatlas_output"
DEFAULT_MAX_DISHES = 50
DEFAULT_DELAY = 3


def load_urls(file_path):
    """Load URLs from JSON file"""
    with open(file_path, 'r') as f:
        return json.load(f)


def extract_place_name(url):
    """Extract place name from URL"""
    clean_url = url.split('?')[0].rstrip('/')
    place = clean_url.split('/')[-1]
    return place.lower().replace('-', '_')


def run_spider(url, max_dishes, output_dir):
    """Run scrapy spider for a single URL"""
    place_name = extract_place_name(url)
    
    cmd = [
        'scrapy', 'crawl', 'tasteatlas_dishes',
        '-a', f'url={url}',
        '-a', f'output_dir={output_dir}',
    ]
    
    if max_dishes:
        cmd.extend(['-a', f'max_dishes={max_dishes}'])
    
    print(f"\n{'─'*50}")
    print(f"🍽️  Scraping: {place_name}")
    print(f"   URL: {url}")
    print(f"{'─'*50}")
    
    try:
        result = subprocess.run(cmd, timeout=300)
        
        # Check if output file exists
        output_file = os.path.join(output_dir, f"tasteatlas_{place_name}_dishes.json")
        
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                dishes = json.load(f)
            return {'success': True, 'count': len(dishes), 'file': output_file, 'place': place_name}
        else:
            return {'success': False, 'error': 'No output file', 'place': place_name}
            
    except subprocess.TimeoutExpired:
        return {'success': False, 'error': 'Timeout', 'place': place_name}
    except Exception as e:
        return {'success': False, 'error': str(e), 'place': place_name}


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Batch scrape TasteAtlas URLs')
    parser.add_argument('--urls', type=str, default=URLS_FILE, help='JSON file with URLs')
    parser.add_argument('--max-dishes', type=int, default=DEFAULT_MAX_DISHES, help='Max dishes per place')
    parser.add_argument('--delay', type=int, default=DEFAULT_DELAY, help='Delay between requests (seconds)')
    parser.add_argument('--output-dir', type=str, default=OUTPUT_DIR, help='Output directory')
    parser.add_argument('--start-from', type=int, default=0, help='Start from index (for resuming)')
    
    args = parser.parse_args()
    
    # Load URLs
    if not os.path.exists(args.urls):
        print(f"❌ Error: File not found: {args.urls}")
        return 1
    
    urls = load_urls(args.urls)
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Print config
    print(f"\n{'='*60}")
    print(f"🍽️  TasteAtlas Batch Scraper")
    print(f"{'='*60}")
    print(f"URLs: {len(urls)}")
    print(f"Max dishes: {args.max_dishes}")
    print(f"Delay: {args.delay}s")
    print(f"Output: {args.output_dir}/")
    print(f"Starting from: {args.start_from}")
    print(f"{'='*60}")
    
    # Track results
    results = []
    successful = 0
    failed = 0
    total_dishes = 0
    
    # Scrape each URL
    for idx, url in enumerate(urls[args.start_from:], start=args.start_from):
        place = extract_place_name(url)
        print(f"\n[{idx + 1}/{len(urls)}] {place}")
        
        result = run_spider(url, args.max_dishes, args.output_dir)
        result['url'] = url
        result['index'] = idx
        results.append(result)
        
        if result['success']:
            successful += 1
            total_dishes += result['count']
            print(f"   ✅ {result['count']} dishes")
        else:
            failed += 1
            print(f"   ❌ {result.get('error', 'Unknown error')}")
        
        # Delay (except last URL)
        if idx < len(urls) - 1:
            print(f"   ⏳ Waiting {args.delay}s...")
            time.sleep(args.delay)
    
    # Save summary
    summary = {
        'timestamp': datetime.now().isoformat(),
        'config': vars(args),
        'summary': {
            'total': len(urls),
            'successful': successful,
            'failed': failed,
            'total_dishes': total_dishes,
        },
        'results': results
    }
    
    summary_file = os.path.join(args.output_dir, 'scraping_summary.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"📊 COMPLETE")
    print(f"{'='*60}")
    print(f"✅ Successful: {successful}/{len(urls)}")
    print(f"❌ Failed: {failed}")
    print(f"🍽️  Total dishes: {total_dishes}")
    print(f"📁 Output: {args.output_dir}/")
    print(f"{'='*60}")
    
    # Show failed URLs
    if failed > 0:
        print(f"\n⚠️  Failed URLs (re-run with --start-from):")
        for r in results:
            if not r['success']:
                print(f"   [{r['index']}] {r['place']}: {r.get('error')}")
    
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())