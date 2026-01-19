import scrapy
from scrapy_selenium import SeleniumRequest
import json
import re
import os
import random
import time
import hashlib
from pathlib import Path
from typing import Dict, List, Any, Optional
import asyncio
from dataclasses import dataclass

# LangChain for AI validation
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate

# Google Places API for enrichment
import googlemaps
import requests

from dotenv import load_dotenv
load_dotenv()

@dataclass
class TrialConfig:
    # AI Settings
    model: str = os.getenv("LC_MODEL", "gpt-4o-mini")
    
    # Processing Settings
    trim_percentage: float = 0.30  # Remove 30% of places
    min_places_per_playlist: int = 8
    max_places_per_playlist: int = 20
    
    # API Keys
    google_maps_api_key: str = os.getenv("GOOGLE_MAPS_API_KEY", "")
    
    # Output Settings (Trial Mode)
    base_dir: Path = Path(__file__).resolve().parent
    output_dir: Path = base_dir / "trial_output"
    playlists_file: Path = output_dir / "wanderlog_playlists_trial.json"
    detailed_report_file: Path = output_dir / "processing_report.json"
    cache_dir: Path = base_dir / "cache"
    
    # Trial Settings
    bucket_name: str = "mycasavsc.appspot.com"  # For URL generation only
    dry_run: bool = True  # Always true for trial
    
    def __post_init__(self):
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.cache_dir.mkdir(exist_ok=True, parents=True)

# AI Prompts (same as before)
CATEGORY_VALIDATOR_PROMPT = PromptTemplate.from_template("""
You are validating whether places actually belong to a specific category.

Category: {category}
City: {city}

Here are the places to validate:
{places_list}

For each place, determine if it ACTUUALLY belongs to the "{category}" category based on the place name and description.

Return a JSON array of place names that ACTUALLY belong to this category:
""")

PLAYLIST_TITLE_GENERATOR = PromptTemplate.from_template("""
Create an engaging playlist title for this collection:

City: {city}
Category: {category}
Sample places: {sample_places}

Create a 4-8 word title that's engaging and travel-focused.
Return only the title:
""")

class WanderlogTrialSpider(scrapy.Spider):
    name = "smart_wanderlog_to_playlist"
    
    # Fixed Spider settings
    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 2,  # Lower for trial
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_selenium.SeleniumMiddleware': 800
        },
        'SELENIUM_DRIVER_NAME': 'chrome',
        'SELENIUM_DRIVER_ARGUMENTS': [
            '--headless',
            '--no-sandbox', 
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--window-size=1920,1080',
            '--disable-blink-features=AutomationControlled'
        ]
    }

    def __init__(self):
        self.config = TrialConfig()
        self.llm = ChatOpenAI(model=self.config.model, temperature=0.0)
        
        # Google Maps client
        self.gmaps = googlemaps.Client(key=self.config.google_maps_api_key) if self.config.google_maps_api_key else None
        
        # Caches
        self.google_places_cache = self.load_cache("google_places_cache.json")
        
        # Storage for scraped data and processing stats
        self.scraped_data = {}
        self.processing_stats = {
            'total_urls_scraped': 0,
            'total_places_found': 0,
            'places_after_quality_filter': 0,
            'places_after_ai_validation': 0,
            'places_after_trimming': 0,
            'total_playlists_created': 0,
            'total_photos_simulated': 0,
            'api_calls': {
                'google_places_details': 0,
                'openai_validation': 0,
                'openai_titles': 0
            }
        }
    
    def load_cache(self, filename: str) -> Dict:
        cache_file = self.config.cache_dir / filename
        if cache_file.exists():
            try:
                return json.loads(cache_file.read_text(encoding='utf-8'))
            except:
                return {}
        return {}
    
    def save_cache(self, cache: Dict, filename: str):
        cache_file = self.config.cache_dir / filename
        try:
            cache_file.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding='utf-8')
        except Exception as e:
            print(f"Failed to save cache {filename}: {e}")
    
    async def start(self):
        """Generate requests for trial dataset"""
        # Small trial dataset (just Mumbai for testing)
        trial_dataset = {
            "Mumbai": {
                "beaches": [{"title": "The 30 best beaches in and around Mumbai", "url": "https://wanderlog.com/list/geoCategory/109807"}],
                "waterfalls": [{"title": "The 3 best waterfalls near Mumbai", "url": "https://wanderlog.com/list/geoCategory/178169"}],
                "national parks": [{"title": "The 9 best national parks around Mumbai", "url": "https://wanderlog.com/list/geoCategory/144218"}],
            }
        }
        
        for city, categories in trial_dataset.items():
            for category, entries in categories.items():
                for entry in entries:
                    yield SeleniumRequest(
                        url=entry['url'],
                        callback=self.parse_places_list,
                        wait_time=5,  # Increased wait time for stability
                        meta={
                            'city': city,
                            'category': category,
                            'title': entry['title']
                        }
                    )
    
    def parse_places_list(self, response):
        """Extract places from both place_metadata and boardSections"""
        city = response.meta['city']
        category = response.meta['category']
        page_title = response.meta['title']
        
        self.processing_stats['total_urls_scraped'] += 1
        
        # Extract MOBX state
        script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
        
        if script_text:
            mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
            if mobx_data:
                try:
                    mobx_json = json.loads(mobx_data.group(1))
                    places_list_page = mobx_json.get('placesListPage', {})
                    data = places_list_page.get('data', {})
                    
                    # Extract from place_metadata
                    place_metadata = data.get('placeMetadata', [])
                    
                    # Extract from boardSections
                    board_sections = data.get('boardSections', [])
                    board_places = []
                    
                    for section in board_sections:
                        blocks = section.get('blocks', [])
                        for block in blocks:
                            if block.get('type') == 'place':
                                board_places.append(block)
                    
                    # Merge data from both sources
                    merged_places = self.merge_place_data(place_metadata, board_places)
                    
                    if merged_places:
                        self.processing_stats['total_places_found'] += len(merged_places)
                        
                        key = f"{city}_{category}_{hash(response.url) % 10000}"
                        self.scraped_data[key] = {
                            'city': city,
                            'category': category,
                            'page_title': page_title,
                            'url': response.url,
                            'places': merged_places
                        }
                        
                        print(f"✅ Scraped {len(merged_places)} places for {city} - {category}")
                        print(f"  - Board places: {len(board_places)}, Metadata places: {len(place_metadata)}")
                    
                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error for {response.url}: {e}")
        else:
            print(f"❌ No MOBX state found for {response.url}")
    
    def merge_place_data(self, place_metadata: List[Dict], board_places: List[Dict]) -> List[Dict]:
        """Merge data from place_metadata and boardSections"""
        merged = []
        
        # Create lookup for board places by placeId
        board_lookup = {}
        for board_place in board_places:
            place_info = board_place.get('place', {})
            place_id = place_info.get('placeId')
            if place_id:
                board_lookup[place_id] = board_place
        
        # Process place_metadata and enrich with board data
        for metadata_place in place_metadata:
            place_id = metadata_place.get('placeId')
            
            # Start with metadata
            merged_place = self.extract_place_from_metadata(metadata_place)
            
            # Enrich with board data if available
            if place_id and place_id in board_lookup:
                board_data = self.extract_place_from_board(board_lookup[place_id])
                merged_place.update(board_data)
            
            if self.basic_quality_filter(merged_place):
                merged.append(merged_place)
        
        # Add any board places that weren't in metadata
        for board_place in board_places:
            place_info = board_place.get('place', {})
            place_id = place_info.get('placeId')
            
            # Check if this place wasn't already processed
            already_processed = any(p.get('placeId') == place_id for p in merged)
            if not already_processed and place_id:
                board_data = self.extract_place_from_board(board_place)
                if self.basic_quality_filter(board_data):
                    merged.append(board_data)
        
        self.processing_stats['places_after_quality_filter'] += len(merged)
        return merged
    
    def extract_place_from_metadata(self, place: Dict[str, Any]) -> Dict[str, Any]:
        """Extract place data from place_metadata section"""
        return {
            'id': str(place.get('id', '')),
            'name': self.clean_text(place.get('name', '')),
            'placeId': place.get('placeId', ''),
            'description': self.clean_text(place.get('description', '')),
            'generalDescription': self.clean_text(place.get('generatedDescription', '')),
            'categories': place.get('categories', []),
            'address': self.clean_text(place.get('address', '')),
            'rating': float(place.get('rating', 0)) if place.get('rating') else 0,
            'numRatings': int(place.get('numRatings', 0)) if place.get('numRatings') else 0,
            'tripadvisorRating': float(place.get('tripadvisorRating', 0)) if place.get('tripadvisorRating') else 0,
            'tripadvisorNumRatings': int(place.get('tripadvisorNumRatings', 0)) if place.get('tripadvisorNumRatings') else 0,
            'website': place.get('website', ''),
            'internationalPhoneNumber': place.get('internationalPhoneNumber', ''),
            'priceLevel': place.get('priceLevel'),
            'permanentlyClosed': bool(place.get('permanentlyClosed', False)),
            'latitude': place.get('latitude'),
            'longitude': place.get('longitude'),
            'minMinutesSpent': place.get('minMinutesSpent'),
            'maxMinutesSpent': place.get('maxMinutesSpent'),
            'imageKeys': place.get('imageKeys', []),
            'utcOffset': place.get('utcOffset', 330),
            'openingPeriods': place.get('openingPeriods', []),
            'wanderlog_reviews': place.get('reviews', []),
        }
    
    def extract_place_from_board(self, board_block: Dict[str, Any]) -> Dict[str, Any]:
        """Extract place data from boardSections blocks"""
        place_info = board_block.get('place', {})
        text_ops = board_block.get('text', {}).get('ops', [])
        
        # Extract description from ops format
        description = ""
        for op in text_ops:
            if isinstance(op, dict) and 'insert' in op:
                description += op['insert']
        description = description.strip()
        
        return {
            'name': place_info.get('name', ''),
            'placeId': place_info.get('placeId', ''),
            'latitude': place_info.get('latitude'),
            'longitude': place_info.get('longitude'),
            'board_description': self.clean_text(description),
            'selectedImageKey': board_block.get('selectedImageKey', ''),
            'imageKeys': board_block.get('imageKeys', []),
            'board_id': board_block.get('id', ''),
        }
    
    def basic_quality_filter(self, place: Dict[str, Any]) -> bool:
        """Basic quality filtering"""
        if not place.get('name'):
            return False
        if place.get('permanentlyClosed'):
            return False
        if not (place.get('placeId') or (place.get('latitude') and place.get('longitude'))):
            return False
        return True
    
    def clean_text(self, text: Optional[str]) -> str:
        """Clean text"""
        if not text:
            return ""
        
        cleaned_text = (text
            .replace(u'\u2019', "'")   
            .replace(u'\u2014', "-")   
            .replace(u'\u201c', '"')   
            .replace(u'\u201d', '"')
            .replace(u'\u00a0', ' ')
            .replace('\n', ' ')
        )
        
        return re.sub(r'\s+', ' ', cleaned_text.strip())
    
    def simulate_google_places_enrichment(self, place: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate Google Places enrichment (for trial mode)"""
        if not place.get('placeId'):
            return place
        
        place_id = place['placeId']
        
        # Check if we would make an API call (but don't actually make it in trial mode)
        if place_id not in self.google_places_cache:
            self.processing_stats['api_calls']['google_places_details'] += 1
            
            # Simulate what the API would return
            simulated_data = {
                'photo_refs': ['simulated_photo_ref_1', 'simulated_photo_ref_2', 'simulated_photo_ref_3'],
                'international_phone_number': '+91 12345 67890',
                'website': 'https://example.com',
                'opening_hours': {'periods': []},
                'types': ['tourist_attraction', 'point_of_interest'],
                'formatted_address': f"{place.get('name', 'Unknown Place')}, {place.get('address', 'India')}"
            }
            
            # Cache the simulated data
            self.google_places_cache[place_id] = simulated_data
        
        google_data = self.google_places_cache.get(place_id, {})
        
        if google_data:
            place.update({
                'internationalPhoneNumber': google_data.get('international_phone_number', place.get('internationalPhoneNumber', '')),
                'website': google_data.get('website', place.get('website', '')),
                'openingPeriods': google_data.get('opening_hours', {}).get('periods', place.get('openingPeriods', [])),
                'categories': google_data.get('types', place.get('categories', [])),
                'address': google_data.get('formatted_address', place.get('address', '')),
                'photo_refs': google_data.get('photo_refs', []),
            })
        
        return place
    
    def validate_category_relevance(self, city: str, category: str, places: List[Dict]) -> List[Dict]:
        """AI category validation (synchronous API call)"""
        if len(places) <= 10:
            return places
        
        places_text = []
        for place in places:
            name = place.get('name', 'Unknown')
            desc = (place.get('description', '') or 
                    place.get('board_description', '') or 
                    place.get('generalDescription', ''))[:100]
            if desc:
                places_text.append(f"- {name}: {desc}")
            else:
                places_text.append(f"- {name}")
        
        try:
            self.processing_stats['api_calls']['openai_validation'] += 1
            response = self.llm.invoke(
                CATEGORY_VALIDATOR_PROMPT.format(
                    category=category,
                    city=city,
                    places_list="\n".join(places_text)
                )
            )
            
            validated_names = json.loads(response.content.strip())
            
            validated_places = []
            for name in validated_names:
                for place in places:
                    if place.get('name', '').lower() == name.lower():
                        validated_places.append(place)
                        break
            
            self.processing_stats['places_after_ai_validation'] += len(validated_places)
            print(f"🤖 AI validated {len(validated_places)}/{len(places)} places for {category}")
            return validated_places
            
        except Exception as e:
            print(f"⚠️ AI validation failed for {city}-{category}: {e}")
            self.processing_stats['places_after_ai_validation'] += len(places)
            return places
    
    def generate_playlist_title(self, city: str, category: str, sample_names: List[str]) -> str:
        """Generate playlist title (synchronous)"""
        try:
            self.processing_stats['api_calls']['openai_titles'] += 1
            response = self.llm.invoke(
                PLAYLIST_TITLE_GENERATOR.format(
                    city=city,
                    category=category,
                    sample_places=", ".join(sample_names[:3])
                )
            )
            return response.content.strip().strip('"')
        except:
            return f"{category.title()} in {city}"
    
    def simulate_photo_processing(self, place: Dict[str, Any], list_id: int) -> List[str]:
        """Simulate photo processing and return simulated g_image_urls"""
        place_id = place.get('placeId')
        photo_refs = place.get('photo_refs', [])
        
        if not place_id:
            return []
        
        # Simulate uploading 3 photos
        simulated_photos = min(3, len(photo_refs)) if photo_refs else 0
        
        # If no photos, simulate static map fallback
        if simulated_photos == 0 and place.get('latitude') and place.get('longitude'):
            simulated_photos = 1
        
        self.processing_stats['total_photos_simulated'] += simulated_photos
        
        # Generate simulated URLs
        base_url = f"https://storage.googleapis.com/{self.config.bucket_name}/playlistsPlaces/{list_id}/{place_id}"
        return [f"{base_url}/{i}.jpg" for i in range(1, simulated_photos + 1)]
    
    def format_reviews(self, wanderlog_reviews: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert Wanderlog reviews to playlist review structure"""
        if not wanderlog_reviews:
            return []
        
        formatted_reviews = []
        selected_indices = [i for i in range(0, len(wanderlog_reviews), 2)][:3]
        
        for idx in selected_indices:
            if idx >= len(wanderlog_reviews):
                break
                
            review = wanderlog_reviews[idx]
            review_time = self.convert_iso_to_timestamp(review.get('time', ''))
            relative_time = self.calculate_relative_time(review_time)
            
            formatted_review = {
                'rating': review.get('rating', 0),
                'text': review.get('reviewText', ''),
                'author_name': review.get('reviewerName', 'Anonymous'),
                'relative_time_description': relative_time,
                'time': review_time,
                'profile_photo_url': ''
            }
            
            formatted_reviews.append(formatted_review)
        
        return formatted_reviews
    
    def convert_iso_to_timestamp(self, iso_string: str) -> int:
        """Convert ISO timestamp to Unix timestamp"""
        if not iso_string:
            return int(time.time())
        
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
            return int(dt.timestamp())
        except Exception:
            return int(time.time())
    
    def calculate_relative_time(self, timestamp: int) -> str:
        """Calculate relative time description"""
        if not timestamp:
            return "Unknown time"
        
        try:
            now = time.time()
            diff_seconds = now - timestamp
            diff_days = diff_seconds / (24 * 3600)
            diff_months = diff_days / 30.44
            
            if diff_days < 1:
                return "Today"
            elif diff_days < 7:
                return f"{int(diff_days)} day{'s' if int(diff_days) != 1 else ''} ago"
            elif diff_days < 30:
                weeks = int(diff_days / 7)
                return f"{weeks} week{'s' if weeks != 1 else ''} ago"
            elif diff_months < 12:
                months = int(diff_months)
                return f"{months} month{'s' if months != 1 else ''} ago"
            else:
                years = int(diff_months / 12)
                return f"{years} year{'s' if years != 1 else ''} ago"
        except:
            return "Unknown time"
    
    def process_scraped_data(self):
        """Process scraped data and create trial playlists"""
        playlists = []
        
        print(f"\n🔄 Processing {len(self.scraped_data)} scraped datasets...")
        
        # Group by city-category
        grouped_data = {}
        for key, data in self.scraped_data.items():
            group_key = f"{data['city']}_{data['category']}"
            if group_key not in grouped_data:
                grouped_data[group_key] = {
                    'city': data['city'],
                    'category': data['category'],
                    'places': []
                }
            grouped_data[group_key]['places'].extend(data['places'])
        
        for group_key, group_data in grouped_data.items():
            try:
                playlist = self.create_trial_playlist(group_data)
                if playlist:
                    playlists.append(playlist)
                    self.processing_stats['total_playlists_created'] += 1
                    print(f"✅ Created playlist: {playlist['title']} ({len(playlist['items'])} places)")
            except Exception as e:
                print(f"❌ Failed to process {group_key}: {e}")
        
        return playlists
    
    def create_trial_playlist(self, group_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a trial playlist without actual uploads"""
        city = group_data['city']
        category = group_data['category']
        places = group_data['places']
        
        if len(places) < 5:
            return None
        
        # Step 1: Simulate Google Places enrichment
        enriched_places = []
        for place in places:
            enriched_place = self.simulate_google_places_enrichment(place)
            enriched_places.append(enriched_place)
        
        # Step 2: AI category validation
        validated_places = self.validate_category_relevance(city, category, enriched_places)
        
        if len(validated_places) < self.config.min_places_per_playlist:
            return None
        
        # Step 3: Trim and shuffle
        target_count = int(len(validated_places) * (1 - self.config.trim_percentage))
        target_count = min(target_count, self.config.max_places_per_playlist)
        target_count = max(target_count, self.config.min_places_per_playlist)
        
        sorted_places = sorted(validated_places, key=lambda x: (x.get('rating', 0), x.get('numRatings', 0)), reverse=True)
        selected_places = sorted_places[:target_count + 5]
        random.shuffle(selected_places)
        final_places = selected_places[:target_count]
        
        self.processing_stats['places_after_trimming'] += len(final_places)
        
        # Step 4: Generate playlist title and ID
        sample_names = [p.get('name', '') for p in final_places[:3]]
        playlist_title = self.generate_playlist_title(city, category, sample_names)
        
        list_id_hash = hashlib.md5(f"{playlist_title}_{city}_{category}".encode()).hexdigest()[:10]
        list_id = int(str(int(time.time()))[-8:] + str(abs(hash(list_id_hash)))[:2])
        
        # Step 5: Process places with simulated photos
        processed_places = []
        for idx, place in enumerate(final_places, 1):
            g_image_urls = self.simulate_photo_processing(place, list_id)
            processed_place = self.format_place_for_trial(place, idx, g_image_urls)
            processed_places.append(processed_place)
        
        # Step 6: Build playlist structure
        playlist = {
            'list_id': str(list_id),
            'title': playlist_title,
            'description': f'Discover amazing {category} in {city} with this curated collection of must-visit places.',
            'city': city,
            'city_id': city,
            'category': 'Travel',
            'source': 'wanderlog',
            'subtype': self.determine_subtype(category),
            'slug': self.slugify(playlist_title),
            'source_urls': [],
            'created_ts': int(time.time()),
            'imageUrl': f"https://storage.googleapis.com/{self.config.bucket_name}/playlistsNew_images/{list_id}/1.jpg",
            'items': processed_places
        }
        
        return playlist
    
    def determine_subtype(self, category: str) -> str:
        """Determine playlist subtype"""
        category_lower = category.lower()
        if category_lower in ['beaches', 'waterfalls', 'national parks']:
            return 'natural'
        elif category_lower in ['castles', 'architecture', 'photo spots']:
            return 'poi'
        else:
            return 'destination'
    
    def slugify(self, text: str) -> str:
        """Create URL-friendly slug"""
        text = text.lower().strip()
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[\s_-]+', '-', text)
        return text.strip('-')
    
    def format_place_for_trial(self, place: Dict[str, Any], index: int, g_image_urls: List[str]) -> Dict[str, Any]:
        """Format place for trial output"""
        description = (place.get('description') or 
                       place.get('board_description') or 
                       place.get('generalDescription') or None)
        
        formatted_reviews = self.format_reviews(place.get('wanderlog_reviews', []))
        
        return {
            '_id': place.get('placeId', f"wanderlog_{place.get('id', index)}"),
            'index': index,
            'id': place.get('placeId', ''),
            'name': place.get('name', ''),
            'placeId': place.get('placeId', ''),
            'description': description,
            'generalDescription': place.get('generalDescription', ''),
            'categories': place.get('categories', []),
            'address': place.get('address', ''),
            'latitude': place.get('latitude'),
            'longitude': place.get('longitude'),
            'rating': place.get('rating', 0),
            'numRatings': place.get('numRatings', 0),
            'tripadvisorRating': place.get('tripadvisorRating', 0),
            'tripadvisorNumRatings': place.get('tripadvisorNumRatings', 0),
            'website': place.get('website', ''),
            'internationalPhoneNumber': place.get('internationalPhoneNumber', ''),
            'priceLevel': place.get('priceLevel'),
            'permanentlyClosed': place.get('permanentlyClosed', False),
            'minMinutesSpent': place.get('minMinutesSpent'),
            'maxMinutesSpent': place.get('maxMinutesSpent'),
            'imageKeys': place.get('imageKeys', []),
            'utcOffset': place.get('utcOffset', 330),
            'openingPeriods': place.get('openingPeriods', []),
            'reviews': formatted_reviews,
            'sources': [],
            'ratingDistribution': {},
            'g_image_urls': g_image_urls  # Simulated photo URLs
        }
    
    def closed(self, reason):
        """Generate trial output files"""
        if self.scraped_data:
            print(f"\n🔄 Processing scraped data for trial...")
            
            # Run processing synchronously
            playlists = self.process_scraped_data()
            
            if playlists:
                # Save playlists
                self.config.playlists_file.write_text(
                    json.dumps(playlists, ensure_ascii=False, indent=2),
                    encoding='utf-8'
                )
                
                # Save detailed report
                report = {
                    'processing_stats': self.processing_stats,
                    'estimated_costs': {
                        'google_places_details_calls': self.processing_stats['api_calls']['google_places_details'],
                        'estimated_cost_places_api': self.processing_stats['api_calls']['google_places_details'] * 0.017,
                        'estimated_photos_to_download': self.processing_stats['total_photos_simulated'],
                        'estimated_cost_photos_api': self.processing_stats['total_photos_simulated'] * 0.007,
                        'openai_calls': self.processing_stats['api_calls']['openai_validation'] + self.processing_stats['api_calls']['openai_titles'],
                        'total_estimated_cost_usd': (
                            self.processing_stats['api_calls']['google_places_details'] * 0.017 +
                            self.processing_stats['total_photos_simulated'] * 0.007 +
                            (self.processing_stats['api_calls']['openai_validation'] + self.processing_stats['api_calls']['openai_titles']) * 0.002
                        )
                    },
                    'sample_playlist_structure': playlists[0] if playlists else None
                }
                
                self.config.detailed_report_file.write_text(
                    json.dumps(report, ensure_ascii=False, indent=2),
                    encoding='utf-8'
                )
                
                print(f"\n✅ Trial Complete!")
                print(f"📂 Playlists saved: {self.config.playlists_file}")
                print(f"📊 Report saved: {self.config.detailed_report_file}")
                print(f"🎯 Created {len(playlists)} playlists with {sum(len(p['items']) for p in playlists)} places")
                print(f"💰 Estimated cost for full run: ${report['estimated_costs']['total_estimated_cost_usd']:.2f}")
            else:
                print("❌ No playlists created - check if data was scraped successfully")
            
            # Save caches
            self.save_cache(self.google_places_cache, "google_places_cache.json")
        else:
            print("❌ No data scraped - check if URLs are accessible and selectors are correct")

if __name__ == "__main__":
    # Allow running this file directly with: `python wanderlog_to_playlist_migration/wanderlog_trial_scraper.py`
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings
    import os

    # Ensure Scrapy loads the project settings module
    os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "quotes_js_scraper.settings")

    settings = get_project_settings()
    # Respect project settings; spider's class-level custom_settings will merge in
    process = CrawlerProcess(settings)
    process.crawl(WanderlogTrialSpider)
    process.start()

