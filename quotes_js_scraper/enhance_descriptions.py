#!/usr/bin/env python
"""
Enhance Dish Descriptions using OpenAI

This script uses OpenAI to:
1. Complete truncated descriptions (ending with "...")
2. Rephrase them to be more engaging
3. Keep them concise (2-3 sentences)

Usage:
    # Process all files
    python enhance_descriptions.py --api-key YOUR_OPENAI_API_KEY
    
    # Process single country (for testing)
    python enhance_descriptions.py --api-key YOUR_KEY --country india
    
    # Dry run (preview without API calls)
    python enhance_descriptions.py --api-key YOUR_KEY --dry-run --country india

Prerequisites:
    pip install openai
"""

import json
import os
import glob
import argparse
import time
import re
from datetime import datetime

try:
    from openai import OpenAI
except ImportError:
    print("❌ openai not installed!")
    print("   Run: pip install openai")
    exit(1)


# Configuration
INPUT_DIR = "tasteatlas_output"
OUTPUT_DIR = "tasteatlas_enhanced"
OPENAI_MODEL = "gpt-4o-mini"  # Cost-effective model
MAX_RETRIES = 3
DELAY_BETWEEN_CALLS = 0.5  # seconds


def load_json(file_path):
    """Load JSON file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(data, file_path):
    """Save JSON file"""
    os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def needs_enhancement(description):
    """Check if description needs enhancement"""
    if not description:
        return True
    # Truncated descriptions end with "..."
    if description.strip().endswith('...'):
        return True
    # Very short descriptions
    if len(description) < 50:
        return True
    return False


def enhance_description(client, dish_name, category, original_description, country, ingredients=None):
    """Use OpenAI to enhance a dish description"""
    
    # Build context
    context_parts = [
        f"Dish: {dish_name}",
        f"Category: {category}",
        f"Country: {country}",
    ]
    
    if ingredients:
        context_parts.append(f"Key ingredients: {', '.join(ingredients[:5])}")
    
    if original_description:
        # Remove trailing "..." for cleaner context
        clean_desc = original_description.rstrip('.')
        context_parts.append(f"Partial description: {clean_desc}")
    
    context = "\n".join(context_parts)
    
    prompt = f"""Write a concise, engaging description for this dish. 

{context}

Requirements:
- 2-3 sentences maximum (under 200 characters ideal)
- Mention origin/cultural significance briefly
- Highlight key flavors or preparation method
- Do NOT start with "This" or the dish name
- Write in present tense
- Be factual and appetizing

Output only the description, nothing else."""

    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are a food writer creating concise, appetizing dish descriptions for a travel app. Keep descriptions short and engaging."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            max_tokens=150,
            temperature=0.7,
        )
        
        enhanced = response.choices[0].message.content.strip()
        
        # Clean up any quotes or extra formatting
        enhanced = enhanced.strip('"\'')
        
        # Ensure it doesn't start with the dish name
        if enhanced.lower().startswith(dish_name.lower()):
            enhanced = enhanced[len(dish_name):].lstrip(' ,:-')
            enhanced = enhanced[0].upper() + enhanced[1:] if enhanced else enhanced
        
        return enhanced
        
    except Exception as e:
        print(f"      ⚠️  API error: {e}")
        return None


def process_country_file(client, input_file, output_file, dry_run=False):
    """Process all dishes in a country file"""
    
    dishes = load_json(input_file)
    
    # Extract country from filename
    basename = os.path.basename(input_file)
    country = basename.replace('tasteatlas_', '').replace('_dishes.json', '').replace('_', ' ').title()
    
    enhanced_count = 0
    skipped_count = 0
    error_count = 0
    
    for i, dish in enumerate(dishes):
        name = dish.get('name', 'Unknown')
        category = dish.get('category', 'DISH')
        original = dish.get('short_description', '')
        ingredients = dish.get('ingredients', [])
        
        # Check if needs enhancement
        if not needs_enhancement(original):
            skipped_count += 1
            continue
        
        if dry_run:
            print(f"      [{i+1}] {name}: Would enhance")
            print(f"          Original: {original[:80]}...")
            enhanced_count += 1
            continue
        
        # Call OpenAI
        print(f"      [{i+1}/{len(dishes)}] Enhancing: {name}...", end=' ', flush=True)
        
        for attempt in range(MAX_RETRIES):
            enhanced = enhance_description(
                client=client,
                dish_name=name,
                category=category,
                original_description=original,
                country=country,
                ingredients=ingredients
            )
            
            if enhanced:
                dish['short_description'] = enhanced
                print(f"✓")
                enhanced_count += 1
                break
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(1)  # Wait before retry
                else:
                    print(f"✗ (keeping original)")
                    error_count += 1
        
        # Rate limiting
        time.sleep(DELAY_BETWEEN_CALLS)
    
    # Save enhanced data
    if not dry_run:
        save_json(dishes, output_file)
    
    return {
        'total': len(dishes),
        'enhanced': enhanced_count,
        'skipped': skipped_count,
        'errors': error_count,
    }


def main():
    parser = argparse.ArgumentParser(description='Enhance dish descriptions with OpenAI')
    parser.add_argument('--api-key', '-k', type=str, required=True,
                       help='OpenAI API key')
    parser.add_argument('--input-dir', '-i', type=str, default=INPUT_DIR,
                       help='Input directory with scraped JSON files')
    parser.add_argument('--output-dir', '-o', type=str, default=OUTPUT_DIR,
                       help='Output directory for enhanced files')
    parser.add_argument('--country', '-c', type=str, default=None,
                       help='Process only this country (for testing)')
    parser.add_argument('--model', '-m', type=str, default=OPENAI_MODEL,
                       help='OpenAI model to use')
    parser.add_argument('--dry-run', '-d', action='store_true',
                       help='Preview without making API calls')
    
    args = parser.parse_args()
    
    # Find input files
    pattern = os.path.join(args.input_dir, 'tasteatlas_*_dishes.json')
    input_files = sorted(glob.glob(pattern))
    
    if not input_files:
        print(f"❌ No files found in {args.input_dir}")
        return 1
    
    # Filter to single country if specified
    if args.country:
        country_key = args.country.lower().replace(' ', '_').replace('-', '_')
        input_files = [f for f in input_files if country_key in f.lower()]
        if not input_files:
            print(f"❌ No file found for country: {args.country}")
            return 1
    
    print(f"\n{'='*60}")
    print(f"🍽️  Enhance Dish Descriptions with OpenAI")
    print(f"{'='*60}")
    print(f"📂 Input: {args.input_dir}")
    print(f"📂 Output: {args.output_dir}")
    print(f"🤖 Model: {args.model}")
    print(f"📋 Files to process: {len(input_files)}")
    
    if args.dry_run:
        print(f"🔍 DRY RUN - no API calls")
    
    # Initialize OpenAI client
    client = OpenAI(api_key=args.api_key)
    
    # Test connection (unless dry run)
    if not args.dry_run:
        try:
            # Quick test
            test = client.chat.completions.create(
                model=args.model,
                messages=[{"role": "user", "content": "Say 'OK'"}],
                max_tokens=5
            )
            print(f"✅ OpenAI connection verified")
        except Exception as e:
            print(f"❌ OpenAI connection failed: {e}")
            return 1
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Process each file
    print(f"\n{'─'*60}")
    
    total_stats = {
        'files': 0,
        'total_dishes': 0,
        'enhanced': 0,
        'skipped': 0,
        'errors': 0,
    }
    
    for input_file in input_files:
        basename = os.path.basename(input_file)
        country = basename.replace('tasteatlas_', '').replace('_dishes.json', '').replace('_', ' ').title()
        output_file = os.path.join(args.output_dir, basename)
        
        print(f"\n📍 {country}")
        print(f"   Input: {input_file}")
        
        stats = process_country_file(
            client=client,
            input_file=input_file,
            output_file=output_file,
            dry_run=args.dry_run
        )
        
        print(f"   Enhanced: {stats['enhanced']}/{stats['total']} dishes")
        if stats['skipped']:
            print(f"   Skipped (already good): {stats['skipped']}")
        if stats['errors']:
            print(f"   Errors: {stats['errors']}")
        
        if not args.dry_run:
            print(f"   Output: {output_file}")
        
        total_stats['files'] += 1
        total_stats['total_dishes'] += stats['total']
        total_stats['enhanced'] += stats['enhanced']
        total_stats['skipped'] += stats['skipped']
        total_stats['errors'] += stats['errors']
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📊 ENHANCEMENT COMPLETE")
    print(f"{'='*60}")
    print(f"📁 Files processed: {total_stats['files']}")
    print(f"🍽️  Total dishes: {total_stats['total_dishes']}")
    print(f"✨ Enhanced: {total_stats['enhanced']}")
    print(f"⏭️  Skipped: {total_stats['skipped']}")
    print(f"❌ Errors: {total_stats['errors']}")
    
    if not args.dry_run:
        print(f"\n📂 Enhanced files saved to: {args.output_dir}/")
        print(f"\n💡 Next step: Update upload script to use enhanced files:")
        print(f"   python upload_to_firebase.py --credentials service_account.json --output-dir {args.output_dir}")
    else:
        print(f"\n🔍 This was a DRY RUN")
        print(f"   Remove --dry-run to process with OpenAI")
    
    # Estimate cost
    if total_stats['enhanced'] > 0:
        # Rough estimate: ~200 tokens per request at $0.15/1M input + $0.60/1M output
        est_cost = (total_stats['enhanced'] * 200 * 0.00000015) + (total_stats['enhanced'] * 100 * 0.0000006)
        print(f"\n💰 Estimated API cost: ~${est_cost:.2f}")
    
    print(f"{'='*60}")
    
    return 0


if __name__ == '__main__':
    exit(main())