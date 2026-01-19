
import scrapy
from scrapy_selenium import SeleniumRequest
import json
import os
import re

class AtlasObscuraPlacesSpider(scrapy.Spider):
    name = "atlasobscura_places"

    def __init__(self, city=None, country=None, max_pages=None, *args, **kwargs):
        super(AtlasObscuraPlacesSpider, self).__init__(*args, **kwargs)
        self.places_data = []  # List to store all the places data
        self.processed_pages = set()  # Track pages already processed
        
        # Set default location if not provided
        self.city = city or "mumbai"
        self.country = country or "india"
        
        # Set the maximum number of pages to scrape (None means all available pages)
        self.max_pages = int(max_pages) if max_pages is not None else None
        self.current_page = 1
        
        # Counter for indexing places
        self.place_index = 1

    def start_requests(self):
        # Construct the start URL based on city and country
        start_url = f"https://www.atlasobscura.com/things-to-do/{self.city}-{self.country}/places"
        self.logger.info(f"Starting scrape for {self.city}, {self.country} at {start_url}")
        
        yield SeleniumRequest(
            url=start_url,
            callback=self.parse_places,
            wait_time=3,
            # Scroll to the bottom to ensure all dynamic content is loaded.
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

            # Prepare the item dictionary with index field
            item = {
                "index": self.place_index,
                "place_name": place_name,
                "latitude": latitude,
                "longitude": longitude,
                "city": city,
                "state": state,
                "country": country,
                "url": place_url,
                # "source_page": response.url,
                # "page_number": self.current_page
            }

            # Increment the index for the next place
            self.place_index += 1

            # Store the item and yield it
            self.places_data.append(item)
            yield item
        
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
    
    def close(self, reason):
        """Writes all collected data to a JSON file when the spider finishes."""
        # Generate output filename based on city and country
        filename = f"atlasobscura_{self.city}_{self.country}_places.json"
        output_file = os.path.join(os.getcwd(), filename)
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(self.places_data, f, ensure_ascii=False, indent=4)
        
        self.logger.info(f"Saved {len(self.places_data)} places to {filename}")



# import scrapy
# from scrapy_selenium import SeleniumRequest
# import json
# import os
# import re

# class AtlasObscuraPlacesSpider(scrapy.Spider):
#     name = "atlasobscura_places"

#     def __init__(self, city=None, country=None, max_pages=None, *args, **kwargs):
#         super(AtlasObscuraPlacesSpider, self).__init__(*args, **kwargs)
#         self.places_data = []  # List to store all the places data
#         self.processed_pages = set()  # Track pages already processed
        
#         # Set default location if not provided
#         self.city = city or "mumbai"
#         self.country = country or "india"
        
#         # Set the maximum number of pages to scrape (None means all available pages)
#         self.max_pages = int(max_pages) if max_pages is not None else None
#         self.current_page = 1

#     def start_requests(self):
#         # Construct the start URL based on city and country
#         start_url = f"https://www.atlasobscura.com/things-to-do/{self.city}-{self.country}/places"
#         self.logger.info(f"Starting scrape for {self.city}, {self.country} at {start_url}")
        
#         yield SeleniumRequest(
#             url=start_url,
#             callback=self.parse_places,
#             wait_time=3,
#             # Scroll to the bottom to ensure all dynamic content is loaded.
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

#             # Prepare the item dictionary
#             item = {
#                 "place_name": place_name,
#                 "latitude": latitude,
#                 "longitude": longitude,
#                 "city": city,
#                 "state": state,
#                 "country": country,
#                 "url": place_url,
#                 "source_page": response.url
#             }

#             # Store the item and yield it
#             self.places_data.append(item)
#             yield item
        
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
    
#     def close(self, reason):
#         """Writes all collected data to a JSON file when the spider finishes."""
#         # Generate output filename based on city and country
#         filename = f"atlasobscura_{self.city}_{self.country}_places2.json"
#         output_file = os.path.join(os.getcwd(), filename)
        
#         with open(output_file, "w", encoding="utf-8") as f:
#             json.dump(self.places_data, f, ensure_ascii=False, indent=4)
        
#         self.logger.info(f"Saved {len(self.places_data)} places to {filename}")











