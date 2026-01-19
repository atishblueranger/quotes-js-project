# File: atlasobscura_enrichment_spider.py
# Save this in your Scrapy project's spiders directory

import scrapy
from scrapy_selenium import SeleniumRequest
from scrapy.http import Request
import json
import os
import re
import requests
import time


class AtlasObscuraEnrichmentSpider(scrapy.Spider):
    name = "atlasobscura_enriched"

    # Google Places API configuration - set via command line arg
    GOOGLE_API_KEY = None

    # Fields to request from Places API Details endpoint
    PLACE_DETAILS_FIELDS = (
        "formatted_address,name,international_phone_number,geometry,"
        "opening_hours,website,price_level,rating,user_ratings_total,photos,"
        "types,permanently_closed,utc_offset,editorial_summary"
    )

    # Custom settings for the spider
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_DELAY': 1,  # Be nice to the server
    }

    def __init__(self, city="mumbai", country="india", max_pages=None, api_key=None, output_file=None, *args, **kwargs):
        super(AtlasObscuraEnrichmentSpider, self).__init__(*args, **kwargs)
        # Set location parameters
        self.city = city
        self.country = country

        # Set API key from command line argument
        if api_key:
            self.GOOGLE_API_KEY = api_key

        # Set output file
        self.output_file = output_file or f"atlasobscura_{self.city}_{self.country}_enriched.json"

        # Set the maximum number of pages to scrape
        self.max_pages = int(max_pages) if max_pages is not None else None
        self.current_page = 1

        # List to store all the enriched places data
        self.enriched_places = []

        # Track pages already processed
        self.processed_pages = set()

        # Counter for indexing places
        self.place_index = 1

        # Rate limiting for API calls
        self.api_call_interval = 0.5  # seconds between API calls

        # Log initialization
        self.logger.info(f"Spider initialized for {self.city}, {self.country}")
        self.logger.info(f"Output will be saved to {self.output_file}")

        # Check API key
        if not self.GOOGLE_API_KEY:
            self.logger.error("Google API Key is required. Set it with -a api_key=YOUR_KEY")
            raise ValueError("Missing Google API Key")

    def start_requests(self):
        # Construct the start URL based on city and country
        start_url = f"https://www.atlasobscura.com/things-to-do/{self.city}-{self.country}/places"
        self.logger.info(f"Starting scrape at {start_url}")

        yield SeleniumRequest(
            url=start_url,
            callback=self.parse_places,
            wait_time=3,
            # Scroll to the bottom to ensure all dynamic content is loaded
            script="window.scrollTo(0, document.body.scrollHeight);"
        )

    def parse_places(self, response):
        # Add current page to processed pages
        self.processed_pages.add(response.url)

        # Extract all place cards on the page
        cards = response.css("a.Card--flat[data-type='Place']")
        places_found = 0

        for card in cards:
            places_found += 1
            # Attempt to extract the place name from the <h3> element
            place_name = card.css("h3.Card__heading span::text").get()
            if not place_name:
                # If not found, derive the name from the href attribute
                href = card.attrib.get("href", "")
                if href.startswith("/places/"):
                    place_name = href.replace("/places/", "").replace("-", " ").title()

            # Extract other attributes from the data-* attributes
            latitude = card.attrib.get("data-lat")
            longitude = card.attrib.get("data-lng")
            city = card.attrib.get("data-city")
            state = card.attrib.get("data-state")
            country = card.attrib.get("data-country")
            place_url = response.urljoin(card.attrib.get("href", ""))

            # Extract the description from the card (the text inside the subtitle div)
            description = card.css("div.Card__content.js-subtitle-content::text").get()
            if description:
                description = re.sub(r'\s+', ' ', description).strip()

            # Prepare the initial item dictionary, including the scraped description
            place = {
                "index": self.place_index,
                "place_name": place_name,
                "description": description,  # Scraped and cleaned description
                "latitude": latitude,
                "longitude": longitude,
                "city": city,
                "state": state,
                "country": country,
                "url": place_url,
            }

            # Increment the index for the next place
            self.place_index += 1

            # Process the place immediately through our enrichment pipeline
            yield Request(
                url="dummy://placeholder",  # Using dummy:// scheme to avoid actual HTTP requests
                callback=self.enrich_place,
                dont_filter=True,
                meta={"place": place}
            )

        self.logger.info(f"Found {places_found} places on page {self.current_page}")

        # Check if we should continue to the next page
        if self.max_pages is None or self.current_page < self.max_pages:
            # Find the next page using either navigation links or by constructing the URL
            next_page_url = None

            # Method 1: Look for the "next" link
            next_link = response.css("a[rel='next']::attr(href)").get()
            if next_link:
                next_page_url = response.urljoin(next_link)
            else:
                # Method 2: Look for pagination information
                pagination = response.css("nav.pagination")
                if pagination:
                    # Find the current page
                    current_page_span = pagination.css("span.page.current::text").get()
                    if current_page_span:
                        current_page_number = int(current_page_span.strip())
                        next_page_number = current_page_number + 1

                        # Check if there's a link to the next page in the pagination
                        next_page_exists = pagination.css(f"a[href*='page={next_page_number}']").get()
                        if next_page_exists:
                            # Construct the next page URL
                            base_url = re.sub(r'\?page=\d+', '', response.url)
                            next_page_url = f"{base_url}?page={next_page_number}"
                # Method 3: If we're on the first page without a page parameter
                elif "page=" not in response.url:
                    next_page_url = f"{response.url}?page=2"

            # If we found a next page URL and haven't processed it yet
            if next_page_url and next_page_url not in self.processed_pages:
                self.current_page += 1
                self.logger.info(f"Moving to page {self.current_page}: {next_page_url}")
                yield SeleniumRequest(
                    url=next_page_url,
                    callback=self.parse_places,
                    wait_time=3,
                    script="window.scrollTo(0, document.body.scrollHeight);"
                )
            else:
                self.logger.info(f"No more pages to process or reached max pages limit ({self.max_pages})")

    def enrich_place(self, response):
        """Pipeline step 1: Find Google Place ID"""
        place = response.meta["place"]
        self.logger.info(f"Enriching place: {place.get('place_name')}")

        # Get the Google Place ID
        place_id = self.get_google_place_id(
            place.get("place_name"),
            place.get("latitude"),
            place.get("longitude")
        )
        place["placeId"] = place_id

        if place_id:
            # Get detailed place information
            place = self.get_place_details(place)

        # Store the enriched place data
        self.enriched_places.append(place)
        self.logger.info(f"Enriched place {place.get('place_name')} (Place ID: {place.get('placeId')})")

    def get_google_place_id(self, place_name, latitude, longitude):
        """
        Use the Google Places API 'Find Place from Text' endpoint
        to search for the place and return its google place_id.
        """
        if not place_name or not latitude or not longitude:
            self.logger.warning(f"Missing data for place: {place_name}")
            return None

        base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"

        # Construct the request parameters
        params = {
            "input": place_name,
            "inputtype": "textquery",
            "fields": "place_id",
            # Use location biasing to help Google narrow down the results:
            "locationbias": f"point:{latitude},{longitude}",
            "key": self.GOOGLE_API_KEY
        }

        # Apply rate limiting
        time.sleep(self.api_call_interval)

        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            self.logger.error(f"Error fetching place ID for {place_name}: HTTP {response.status_code}")
            return None

        data = response.json()
        candidates = data.get("candidates", [])
        if candidates:
            # Return the first candidate's place_id
            return candidates[0].get("place_id")
        else:
            self.logger.warning(f"No place ID found for {place_name}")
            return None

    def get_place_details(self, place):
        """
        Calls the Google Place Details API using the given place_id
        and enriches the place data with detailed information.
        """
        google_place_id = place.get("placeId")
        if not google_place_id:
            self.logger.warning(f"Missing Google Place ID for {place.get('place_name')}")
            return place  # Return unchanged if no google place id

        base_url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            "place_id": google_place_id,
            "fields": self.PLACE_DETAILS_FIELDS,
            "key": self.GOOGLE_API_KEY
        }

        # Apply rate limiting
        time.sleep(self.api_call_interval)

        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            self.logger.error(f"HTTP error {response.status_code} for place_id: {google_place_id}")
            return place

        data = response.json()
        if data.get("status") != "OK":
            self.logger.error(f"Error for place_id {google_place_id}: {data.get('status')}")
            return place

        details = data.get("result")
        if not details:
            return place

        # Map fields from details to our record
        place["address"] = details.get("formatted_address")
        place["name"] = details.get("name")  # May override the scraped name
        place["internationalPhoneNumber"] = details.get("international_phone_number")
        place["categories"] = details.get("types")
        # Only update description if it wasn't already set from the page
        if not place.get("description"):
            editorial_summary = details.get("editorial_summary")
            if editorial_summary:
                place["description"] = editorial_summary.get("overview")
            else:
                place["description"] = None

        # Update latitude and longitude using details (if available)
        if details.get("geometry") and details["geometry"].get("location"):
            location = details["geometry"]["location"]
            place["latitude"] = location.get("lat", place.get("latitude"))
            place["longitude"] = location.get("lng", place.get("longitude"))

        # Ratings and pricing
        place["rating"] = details.get("rating")
        place["numRatings"] = details.get("user_ratings_total")
        place["priceLevel"] = details.get("price_level")

        # Opening periods from opening_hours (if available)
        if details.get("opening_hours"):
            place["openingPeriods"] = details["opening_hours"].get("periods")
        else:
            place["openingPeriods"] = None

        # Permanently closed flag
        place["permanentlyClosed"] = details.get("permanently_closed")
        # utcOffset (if available)
        place["utcOffset"] = details.get("utc_offset")
        # Website
        place["website"] = details.get("website")

        # Build image URLs and collect photo references
        photos = details.get("photos")
        image_urls, image_keys = self.build_image_urls(photos)
        place["g_image_urls"] = image_urls
        place["imageKeys"] = image_keys

        # Use the Google Place ID as the ID in our record
        place["id"] = details.get("place_id")
        # ratingDistribution is not provided by Google; set to None
        place["ratingDistribution"] = None

        return place

    def build_image_urls(self, photos):
        """
        From the photos array in the details response, build a list of
        Google Place Photo URLs (using maxwidth=400) and return both the URLs
        and the underlying photo references (imageKeys).
        """
        image_urls = []
        image_keys = []
        if not photos:
            return image_urls, image_keys

        for photo in photos:
            photo_reference = photo.get("photo_reference")
            if photo_reference:
                image_keys.append(photo_reference)
                # Build URL for the photo using the Place Photo API endpoint
                url = (
                    f"https://maps.googleapis.com/maps/api/place/photo?"
                    f"maxwidth=400&photo_reference={photo_reference}&key={self.GOOGLE_API_KEY}"
                )
                image_urls.append(url)
        return image_urls, image_keys

    def closed(self, reason):
        """Writes all collected enriched data to a JSON file when the spider finishes."""
        self.logger.info(f"Spider closing. Reason: {reason}")
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(self.enriched_places, f, ensure_ascii=False, indent=4)

        self.logger.info(f"Saved {len(self.enriched_places)} enriched places to {self.output_file}")




# # File: atlasobscura_enrichment_spider.py
# # Save this in your Scrapy project's spiders directory

# import scrapy
# from scrapy_selenium import SeleniumRequest
# from scrapy.http import Request
# import json
# import os
# import re
# import requests
# import time


# class AtlasObscuraEnrichmentSpider(scrapy.Spider):
#     name = "atlasobscura_enriched"

#     # Google Places API configuration - set via command line arg
#     GOOGLE_API_KEY = None
    
#     # Fields to request from Places API Details endpoint
#     PLACE_DETAILS_FIELDS = (
#     "formatted_address,name,international_phone_number,geometry,"
#     "opening_hours,website,price_level,rating,user_ratings_total,photos,"
#     "types,permanently_closed,utc_offset,editorial_summary"
#     )
    
#     # Custom settings for the spider
#     custom_settings = {
#         'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#         'DOWNLOAD_DELAY': 1,  # Be nice to the server
#     }

#     def __init__(self, city="mumbai", country="india", max_pages=None, api_key=None, output_file=None, *args, **kwargs):
#         super(AtlasObscuraEnrichmentSpider, self).__init__(*args, **kwargs)
#         # Set location parameters
#         self.city = city
#         self.country = country
        
#         # Set API key from command line argument
#         if api_key:
#             self.GOOGLE_API_KEY = api_key
        
#         # Set output file
#         self.output_file = output_file or f"atlasobscura_{self.city}_{self.country}_enriched.json"
        
#         # Set the maximum number of pages to scrape
#         self.max_pages = int(max_pages) if max_pages is not None else None
#         self.current_page = 1
        
#         # List to store all the enriched places data
#         self.enriched_places = []
        
#         # Track pages already processed
#         self.processed_pages = set()
        
#         # Counter for indexing places
#         self.place_index = 1
        
#         # Rate limiting for API calls
#         self.api_call_interval = 0.5  # seconds between API calls
        
#         # Log initialization
#         self.logger.info(f"Spider initialized for {self.city}, {self.country}")
#         self.logger.info(f"Output will be saved to {self.output_file}")
        
#         # Check API key
#         if not self.GOOGLE_API_KEY:
#             self.logger.error("Google API Key is required. Set it with -a api_key=YOUR_KEY")
#             raise ValueError("Missing Google API Key")

#     def start_requests(self):
#         # Construct the start URL based on city and country
#         start_url = f"https://www.atlasobscura.com/things-to-do/{self.city}-{self.country}/places"
#         self.logger.info(f"Starting scrape at {start_url}")
        
#         yield SeleniumRequest(
#             url=start_url,
#             callback=self.parse_places,
#             wait_time=3,
#             # Scroll to the bottom to ensure all dynamic content is loaded
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse_places(self, response):
#         # Add current page to processed pages
#         self.processed_pages.add(response.url)
        
#         # Extract all place cards on the page
#         cards = response.css("a.Card--flat[data-type='Place']")
#         places_found = 0
        
#         for card in cards:
#             places_found += 1
#             # Attempt to extract the place name from the <h3> element
#             place_name = card.css("h3.Card__heading span::text").get()
#             if not place_name:
#                 # If not found, derive the name from the href attribute
#                 href = card.attrib.get("href", "")
#                 if href.startswith("/places/"):
#                     place_name = href.replace("/places/", "").replace("-", " ").title()

#             # Extract other attributes from the data-* attributes
#             latitude = card.attrib.get("data-lat")
#             longitude = card.attrib.get("data-lng")
#             city = card.attrib.get("data-city")
#             state = card.attrib.get("data-state")
#             country = card.attrib.get("data-country")
#             place_url = response.urljoin(card.attrib.get("href", ""))

#             # Prepare the initial item dictionary
#             place = {
#                 "index": self.place_index,
#                 "place_name": place_name,
#                 "latitude": latitude,
#                 "longitude": longitude,
#                 "city": city,
#                 "state": state,
#                 "country": country,
#                 "url": place_url,
#             }

#             # Increment the index for the next place
#             self.place_index += 1

#             # Process the place immediately through our enrichment pipeline
#             yield Request(
#                 url="dummy://placeholder",  # Using dummy:// scheme to avoid actual HTTP requests
#                 callback=self.enrich_place,
#                 dont_filter=True,
#                 meta={"place": place}
# )
        
#         self.logger.info(f"Found {places_found} places on page {self.current_page}")
        
#         # Check if we should continue to the next page
#         if self.max_pages is None or self.current_page < self.max_pages:
#             # Find the next page using either navigation links or by constructing the URL
#             next_page_url = None
            
#             # Method 1: Look for the "next" link
#             next_link = response.css("a[rel='next']::attr(href)").get()
#             if next_link:
#                 next_page_url = response.urljoin(next_link)
#             else:
#                 # Method 2: Look for pagination information
#                 pagination = response.css("nav.pagination")
#                 if pagination:
#                     # Find the current page
#                     current_page_span = pagination.css("span.page.current::text").get()
#                     if current_page_span:
#                         current_page_number = int(current_page_span.strip())
#                         next_page_number = current_page_number + 1
                        
#                         # Check if there's a link to the next page in the pagination
#                         next_page_exists = pagination.css(f"a[href*='page={next_page_number}']").get()
#                         if next_page_exists:
#                             # Construct the next page URL
#                             base_url = re.sub(r'\?page=\d+', '', response.url)
#                             next_page_url = f"{base_url}?page={next_page_number}"
#                 # Method 3: If we're on the first page without a page parameter
#                 elif "page=" not in response.url:
#                     next_page_url = f"{response.url}?page=2"
            
#             # If we found a next page URL and haven't processed it yet
#             if next_page_url and next_page_url not in self.processed_pages:
#                 self.current_page += 1
#                 self.logger.info(f"Moving to page {self.current_page}: {next_page_url}")
#                 yield SeleniumRequest(
#                     url=next_page_url,
#                     callback=self.parse_places,
#                     wait_time=3,
#                     script="window.scrollTo(0, document.body.scrollHeight);"
#                 )
#             else:
#                 self.logger.info(f"No more pages to process or reached max pages limit ({self.max_pages})")

#     def enrich_place(self, response):
#         """Pipeline step 1: Find Google Place ID"""
#         place = response.meta["place"]
#         self.logger.info(f"Enriching place: {place.get('place_name')}")
        
#         # Get the Google Place ID
#         place_id = self.get_google_place_id(
#             place.get("place_name"), 
#             place.get("latitude"), 
#             place.get("longitude")
#         )
#         place["placeId"] = place_id
        
#         if place_id:
#             # Get detailed place information
#             place = self.get_place_details(place)
        
#         # Store the enriched place data
#         self.enriched_places.append(place)
#         self.logger.info(f"Enriched place {place.get('place_name')} (Place ID: {place.get('placeId')})")

#     def get_google_place_id(self, place_name, latitude, longitude):
#         """
#         Use the Google Places API 'Find Place from Text' endpoint
#         to search for the place and return its google place_id.
#         """
#         if not place_name or not latitude or not longitude:
#             self.logger.warning(f"Missing data for place: {place_name}")
#             return None
            
#         base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
        
#         # Construct the request parameters
#         params = {
#             "input": place_name,
#             "inputtype": "textquery",
#             "fields": "place_id",
#             # Use location biasing to help Google narrow down the results:
#             "locationbias": f"point:{latitude},{longitude}",
#             "key": self.GOOGLE_API_KEY
#         }
        
#         # Apply rate limiting
#         time.sleep(self.api_call_interval)
        
#         response = requests.get(base_url, params=params)
#         if response.status_code != 200:
#             self.logger.error(f"Error fetching place ID for {place_name}: HTTP {response.status_code}")
#             return None

#         data = response.json()
#         candidates = data.get("candidates", [])
#         if candidates:
#             # Return the first candidate's place_id
#             return candidates[0].get("place_id")
#         else:
#             self.logger.warning(f"No place ID found for {place_name}")
#             return None

# # def get_place_details(self, place):
# #     """
# #         Calls the Google Place Details API using the given place_id
# #         and enriches the place data with detailed information.
# #          """
# #     google_place_id = place.get("placeId")
# #     if not google_place_id:
# #         self.logger.warning(f"Missing Google Place ID for {place.get('place_name')}")
# #         return place  # Return unchanged if no google place id

# #     base_url = "https://maps.googleapis.com/maps/api/place/details/json"
# #     params = {
# #         "place_id": google_place_id,
# #         "fields": self.PLACE_DETAILS_FIELDS,
# #         "key": self.GOOGLE_API_KEY
# #     }
    
# #     # Apply rate limiting
# #     time.sleep(self.api_call_interval)
    
# #     response = requests.get(base_url, params=params)
# #     if response.status_code != 200:
# #         self.logger.error(f"HTTP error {response.status_code} for place_id: {google_place_id}")
# #         return place

# #     data = response.json()
# #     if data.get("status") != "OK":
# #         self.logger.error(f"Error for place_id {google_place_id}: {data.get('status')}")
# #         return place

# #     details = data.get("result")
# #     if not details:
# #         return place

# #     # Map fields from details to our record
# #     place["address"] = details.get("formatted_address")
# #     place["name"] = details.get("name")  # May override the scraped name
# #     place["internationalPhoneNumber"] = details.get("international_phone_number")
# #     place["categories"] = details.get("types")
    
# #     # Use editorial_summary for description if available
# #     editorial_summary = details.get("editorial_summary")
# #     if editorial_summary:
# #         # Typically, the summary overview is in the 'overview' key
# #         place["description"] = editorial_summary.get("overview")
# #     else:
# #         place["description"] = None

# #     # Update latitude and longitude using details (if available)
# #     if details.get("geometry") and details["geometry"].get("location"):
# #         location = details["geometry"]["location"]
# #         place["latitude"] = location.get("lat", place.get("latitude"))
# #         place["longitude"] = location.get("lng", place.get("longitude"))

# #     # Ratings and pricing
# #     place["rating"] = details.get("rating")
# #     place["numRatings"] = details.get("user_ratings_total")
# #     place["priceLevel"] = details.get("price_level")

# #     # Opening periods from opening_hours (if available)
# #     if details.get("opening_hours"):
# #         place["openingPeriods"] = details["opening_hours"].get("periods")
# #     else:
# #         place["openingPeriods"] = None

# #     # Permanently closed flag
# #     place["permanentlyClosed"] = details.get("permanently_closed")
# #     # utcOffset (if available)
# #     place["utcOffset"] = details.get("utc_offset")
# #     # Website
# #     place["website"] = details.get("website")

# #     # Build image URLs and collect photo references
# #     photos = details.get("photos")
# #     image_urls, image_keys = self.build_image_urls(photos)
# #     place["g_image_urls"] = image_urls
# #     place["imageKeys"] = image_keys

# #     # Use the Google Place ID as the ID in our record
# #     place["id"] = details.get("place_id")
# #     # ratingDistribution is not provided by Google; set to None
# #     place["ratingDistribution"] = None

# #     return place
#     def get_place_details(self, place):
#         """
#         Calls the Google Place Details API using the given place_id
#         and enriches the place data with detailed information.
#         """
#         google_place_id = place.get("placeId")
#         if not google_place_id:
#             self.logger.warning(f"Missing Google Place ID for {place.get('place_name')}")
#             return place  # Return unchanged if no google place id

#         base_url = "https://maps.googleapis.com/maps/api/place/details/json"
#         params = {
#             "place_id": google_place_id,
#             "fields": self.PLACE_DETAILS_FIELDS,
#             "key": self.GOOGLE_API_KEY
#         }
        
#         # Apply rate limiting
#         time.sleep(self.api_call_interval)
        
#         response = requests.get(base_url, params=params)
#         if response.status_code != 200:
#             self.logger.error(f"HTTP error {response.status_code} for place_id: {google_place_id}")
#             return place

#         data = response.json()
#         if data.get("status") != "OK":
#             self.logger.error(f"Error for place_id {google_place_id}: {data.get('status')}")
#             return place

#         details = data.get("result")
#         if not details:
#             return place

#         # Map fields from details to our record
#         place["address"] = details.get("formatted_address")
#         place["name"] = details.get("name")  # May override the scraped name
#         place["internationalPhoneNumber"] = details.get("international_phone_number")
#         place["categories"] = details.get("types")
#         # Google does not provide a description; set it to None
#         # Use editorial_summary for description if available
#         editorial_summary = details.get("editorial_summary")
#         if editorial_summary:
#             # Typically, the summary overview is in the 'overview' key
#             place["description"] = editorial_summary.get("overview")
#         else:
#             place["description"] = None

#         # Update latitude and longitude using details (if available)
#         if details.get("geometry") and details["geometry"].get("location"):
#             location = details["geometry"]["location"]
#             place["latitude"] = location.get("lat", place.get("latitude"))
#             place["longitude"] = location.get("lng", place.get("longitude"))

#         # Ratings and pricing
#         place["rating"] = details.get("rating")
#         place["numRatings"] = details.get("user_ratings_total")
#         place["priceLevel"] = details.get("price_level")

#         # Opening periods from opening_hours (if available)
#         if details.get("opening_hours"):
#             place["openingPeriods"] = details["opening_hours"].get("periods")
#         else:
#             place["openingPeriods"] = None

#         # Permanently closed flag
#         place["permanentlyClosed"] = details.get("permanently_closed")
#         # utcOffset (if available)
#         place["utcOffset"] = details.get("utc_offset")
#         # Website
#         place["website"] = details.get("website")

#         # Build image URLs and collect photo references
#         photos = details.get("photos")
#         image_urls, image_keys = self.build_image_urls(photos)
#         place["g_image_urls"] = image_urls
#         place["imageKeys"] = image_keys

#         # Use the Google Place ID as the ID in our record
#         place["id"] = details.get("place_id")
#         # ratingDistribution is not provided by Google; set to None
#         place["ratingDistribution"] = None

#         return place

#     def build_image_urls(self, photos):
#         """
#         From the photos array in the details response, build a list of
#         Google Place Photo URLs (using maxwidth=400) and return both the URLs
#         and the underlying photo references (imageKeys).
#         """
#         image_urls = []
#         image_keys = []
#         if not photos:
#             return image_urls, image_keys

#         for photo in photos:
#             photo_reference = photo.get("photo_reference")
#             if photo_reference:
#                 image_keys.append(photo_reference)
#                 # Build URL for the photo using the Place Photo API endpoint
#                 url = (
#                     f"https://maps.googleapis.com/maps/api/place/photo?"
#                     f"maxwidth=400&photo_reference={photo_reference}&key={self.GOOGLE_API_KEY}"
#                 )
#                 image_urls.append(url)
#         return image_urls, image_keys

#     def closed(self, reason):
#         """Writes all collected enriched data to a JSON file when the spider finishes."""
#         self.logger.info(f"Spider closing. Reason: {reason}")
#         with open(self.output_file, "w", encoding="utf-8") as f:
#             json.dump(self.enriched_places, f, ensure_ascii=False, indent=4)
        
#         self.logger.info(f"Saved {len(self.enriched_places)} enriched places to {self.output_file}")

