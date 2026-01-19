# import scrapy
# from scrapy_selenium import SeleniumRequest
# import json
# import re
# import firebase_admin
# from firebase_admin import credentials, firestore
# import os
# import time
# from selenium.webdriver.common.by import By

# class WanderlogSpider(scrapy.Spider):
#     name = "wanderlog"

#     def __init__(self):
#         # Initialize Firebase Admin SDK
#         firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         self.db = firestore.client()  # Firestore database instance

#     def start_requests(self):
#         start_url = "https://wanderlog.com/explore"
#         yield SeleniumRequest(
#             url=start_url,
#             callback=self.parse_places,
#             wait_time=5,
#             script='window.scrollTo(0, document.body.scrollHeight);'
#         )

#     def parse_places(self, response):
#         driver = response.meta['driver']

#         # Scroll until no more new places are loaded
#         SCROLL_PAUSE_TIME = 2
#         last_height = driver.execute_script("return document.body.scrollHeight")
#         while True:
#             # Scroll down to bottom
#             driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

#             # Wait to load page
#             time.sleep(SCROLL_PAUSE_TIME)

#             # Calculate new scroll height and compare with last scroll height
#             new_height = driver.execute_script("return document.body.scrollHeight")
#             if new_height == last_height:
#                 break
#             last_height = new_height

#         # Now parse the places
#         places = driver.find_elements(By.CSS_SELECTOR, 'div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3')

#         self.log(f"Found {len(places)} places.")

#         for place in places:
#             try:
#                 place_link_element = place.find_element(By.CSS_SELECTOR, 'a.color-gray-900')
#                 place_link = place_link_element.get_attribute('href')
#                 city_name = place_link_element.text

#                 if place_link:
#                     yield SeleniumRequest(
#                         url=place_link,
#                         callback=self.parse_place_details,
#                         meta={'city_name': city_name}
#                     )
#             except Exception as e:
#                 self.log(f"Error parsing place: {e}")

#     def parse_place_details(self, response):
#         city_name = response.meta['city_name']
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
        
#         if script_text:
#             mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
#             if mobx_data:
#                 mobx_json = json.loads(mobx_data.group(1))
#                 explore_data = mobx_json.get('explorePage', {}).get('data', {})
#                 geo_data = explore_data.get('geo', {})

#                 if geo_data:
#                     # Clean the descriptions
#                     manual_description = self.clean_text(geo_data.get('manualDescription'))
#                     place_description = self.clean_text(geo_data.get('placeDescription'))

#                     place_data = {
#                         'place_id': geo_data.get('id'),
#                         'city_name': geo_data.get('name', city_name),
#                         'state_name': geo_data.get('stateName'),
#                         'country_name': geo_data.get('countryName'),
#                         'depth': geo_data.get('depth'),
#                         'latitude': geo_data.get('latitude'),
#                         'longitude': geo_data.get('longitude'),
#                         'parent_id': geo_data.get('parentId'),
#                         'popularity': geo_data.get('popularity'),
#                         'subcategory': geo_data.get('subcategory'),
#                         'image_key': geo_data.get('imageKey'),
#                         'manual_description': manual_description,
#                         'place_description': place_description,
#                         'bounds': geo_data.get('bounds'),
#                         'city_name_search': (geo_data.get('name', city_name) or '').lower()
#                     }

#                     # Store data in Firebase Firestore
#                     self.store_in_firestore(place_data)

#     def clean_text(self, text):
#         """Cleans up special unicode characters in text."""
#         if text:
#             cleaned_text = (text
#             .replace(u'\u2019', "'")   # Replace right single quote
#             .replace(u'\u2014', "-")   # Replace em dash
#             .replace(u'\u00f3', 'ó')   # Replace o with acute
#             .replace(u'\u201c', '"')   # Replace left double quote
#             .replace(u'\u201d', '"')   # Replace right double quote
#             .replace(u'\u00f9', 'ù')   # Replace u with grave
#             .replace(u'\u1ea1', 'ạ')   # Replace a with dot below
#             .replace(u'\u0111', 'đ')   # Replace d with stroke
#             .replace(u'\u1ee7', 'ủ')   # Replace u with hook above
#             .replace(u'\u00ed', 'í')   # Replace i with acute
#             .replace(u'\u00e9', 'é')   # Replace e with acute
#             .replace(u'\u00e8', 'è')   # Replace e with grave
#             .replace(u'\u00e0', 'à')   # Replace a with grave
#             .replace(u'\u00b2', '²')   # Replace superscript two
#             .replace(u'\u00f8', 'ø')   # Replace o with stroke
#             .replace(u'\u00f4', 'ô')   # Replace o with circumflex
#             .replace(u'\u00e7', 'ç')   # Replace c with cedilla
#             .replace(u'\u00eb', 'ë')   # Replace e with diaeresis
#             .replace(u'\u00fc', 'ü')   # Replace u with diaeresis
#             .replace(u'\u00f1', 'ñ')   # Replace n with tilde
#             .replace(u'\u00e2', 'â')   # Replace a with circumflex
#             .replace(u'\u00e4', 'ä')   # Replace a with diaeresis
#             .replace(u'\u00c4', 'Ä')   # Replace A with diaeresis
#             .replace(u'\u00f6', 'ö')   # Replace o with diaeresis
#             .replace(u'\u00df', 'ß')   # Replace sharp s
#             .replace(u'\u00c0', 'À')   # Replace A with grave
#             .replace(u'\u00c9', 'É')   # Replace E with acute
#             .replace(u'\u00d3', 'Ó')   # Replace O with acute
#             .replace(u'\u00d1', 'Ñ')   # Replace N with tilde
#             .replace(u'\u00b0', '°')   # Replace degree symbol
#             .replace(u'\u00ba', 'º')   # Replace masculine ordinal indicator
#             .replace(u'\u00e5', 'å')   # Replace a with ring above
#             .replace(u'\u00c5', 'Å')   # Replace A with ring above
#             .replace(u'\u00f2', 'ò')   # Replace o with grave
#             .replace(u'\u00fa', 'ú')   # Replace u with acute
#             .replace(u'\u00e1', 'á')   # Replace a with acute
#             .replace(u'\u00ea', 'ê')   # Replace e with circumflex
#             .replace(u'\u00f5', 'õ')   # Replace o with tilde
#             )
#             return cleaned_text
#         return text

#     def store_in_firestore(self, place_data):
#         """Stores place data in Firestore."""
#         place_id = place_data.get('place_id')
#         doc_ref = self.db.collection('explore').document(str(place_id))
#         doc_ref.set(place_data)
#         self.log(f"Stored place_id {place_id} in Firestore")



import scrapy
from scrapy_selenium import SeleniumRequest
import json
import re
import os

class WanderlogSpider(scrapy.Spider):
    name = "wanderlog"

    def __init__(self):
        self.places_data = []  # List to store all the places data

    def start_requests(self):
        start_url = "https://wanderlog.com/explore"
        yield SeleniumRequest(
            url=start_url,
            callback=self.parse_places,
            wait_time=3,
            script='window.scrollTo(0, document.body.scrollHeight);'
        )

    def parse_places(self, response):
        places = response.css('div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3')
        for place in places:
            place_link = place.css('a.color-gray-900::attr(href)').get()
            city_name = place.css('a.color-gray-900::text').get()

            if place_link:
                place_url = response.urljoin(place_link)
                yield SeleniumRequest(
                    url=place_url,
                    callback=self.parse_place_details,
                    meta={'city_name': city_name}
                )

    def parse_place_details(self, response):
        city_name = response.meta['city_name']
        script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()

        if script_text:
            mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
            if mobx_data:
                mobx_json = json.loads(mobx_data.group(1))
                explore_data = mobx_json.get('explorePage', {}).get('data', {})
                geo_data = explore_data.get('geo', {})

                if geo_data:
                    # Clean the descriptions
                    # manual_description = self.clean_text(geo_data.get('manualDescription'))
                    # place_description = self.clean_text(geo_data.get('placeDescription'))

                    place_data = {
                        'place_id': geo_data.get('id'),
                        'city_name': geo_data.get('name', city_name),
                        # 'state_name': geo_data.get('stateName'),
                        # 'country_name': geo_data.get('countryName'),
                        # 'depth': geo_data.get('depth'),
                        'latitude': geo_data.get('latitude'),
                        'longitude': geo_data.get('longitude'),
                        # 'parent_id': geo_data.get('parentId'),
                        # 'popularity': geo_data.get('popularity'),
                        # 'subcategory': geo_data.get('subcategory'),
                        # 'image_key': geo_data.get('imageKey'),
                        # 'manual_description': manual_description,
                        # 'place_description': place_description,
                        # 'bounds': geo_data.get('bounds'),
                    }

                    # Add place data to list
                    self.places_data.append(place_data)

    def clean_text(self, text):
        """Cleans up special unicode characters in text."""
        if text:
            cleaned_text = (text
            .replace(u'\u2019', "'")   # Replace right single quote
            .replace(u'\u2014', "-")   # Replace em dash
            # Other replacements...
            )
            return cleaned_text
        return text

    def close(self, reason):
        """Writes all collected data to a JSON file when the spider finishes."""
        output_file = os.path.join(os.getcwd(), 'explore_places_data.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.places_data, f, ensure_ascii=False, indent=4)
        self.log(f"Saved {len(self.places_data)} places to places_data.json")



# Last working script
# import scrapy
# from scrapy_selenium import SeleniumRequest
# import json
# import re
# import firebase_admin
# from firebase_admin import credentials, firestore
# import os
# class WanderlogSpider(scrapy.Spider):
#     name = "wanderlog"

#     def __init__(self):
#         # Initialize Firebase Admin SDK
#         firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         self.db = firestore.client()  # Firestore database instance

#     def start_requests(self):
#         start_url = "https://wanderlog.com/explore"
#         yield SeleniumRequest(
#             url=start_url,
#             callback=self.parse_places,
#             wait_time=3,
#             script='window.scrollTo(0, document.body.scrollHeight);'
#         )

#     def parse_places(self, response):
#         places = response.css('div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3')
#         for place in places:
#             place_link = place.css('a.color-gray-900::attr(href)').get()
#             city_name = place.css('a.color-gray-900::text').get()

#             if place_link:
#                 place_url = response.urljoin(place_link)
#                 yield SeleniumRequest(
#                     url=place_url,
#                     callback=self.parse_place_details,
#                     meta={'city_name': city_name}
#                 )

#     def parse_place_details(self, response):
#         city_name = response.meta['city_name']
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
        
#         if script_text:
#             mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
#             if mobx_data:
#                 mobx_json = json.loads(mobx_data.group(1))
#                 explore_data = mobx_json.get('explorePage', {}).get('data', {})
#                 geo_data = explore_data.get('geo', {})

#                 if geo_data:
#                     # Clean the descriptions
#                     manual_description = self.clean_text(geo_data.get('manualDescription'))
#                     place_description = self.clean_text(geo_data.get('placeDescription'))

#                     place_data = {
#                         'place_id': geo_data.get('id'),
#                         'city_name': geo_data.get('name', city_name),
#                         'state_name': geo_data.get('stateName'),
#                         'country_name': geo_data.get('countryName'),
#                         'depth': geo_data.get('depth'),
#                         'latitude': geo_data.get('latitude'),
#                         'longitude': geo_data.get('longitude'),
#                         'parent_id': geo_data.get('parentId'),
#                         'popularity': geo_data.get('popularity'),
#                         'subcategory': geo_data.get('subcategory'),
#                         'image_key': geo_data.get('imageKey'),
#                         'manual_description': manual_description,
#                         'place_description': place_description,
#                         'bounds': geo_data.get('bounds'),
#                     }

#                     # Store data in Firebase Firestore
#                     self.store_in_firestore(place_data)

#     def clean_text(self, text):
#         """Cleans up special unicode characters in text."""
#         if text:
#             cleaned_text = (text
#             .replace(u'\u2019', "'")   # Replace right single quote
#             .replace(u'\u2014', "-")   # Replace em dash
#             .replace(u'\u00f3', 'ó')   # Replace o with acute
#             .replace(u'\u201c', '"')   # Replace left double quote
#             .replace(u'\u201d', '"')   # Replace right double quote
#             .replace(u'\u00f9', 'ù')   # Replace u with grave
#             .replace(u'\u1ea1', 'ạ')   # Replace a with dot below
#             .replace(u'\u0111', 'đ')   # Replace d with stroke
#             .replace(u'\u1ee7', 'ủ')   # Replace u with hook above
#             .replace(u'\u00ed', 'í')   # Replace i with acute
#             .replace(u'\u00e9', 'é')   # Replace e with acute
#             .replace(u'\u00e8', 'è')   # Replace e with grave
#             .replace(u'\u00e0', 'à')   # Replace a with grave
#             .replace(u'\u00b2', '²')   # Replace superscript two
#             .replace(u'\u00f8', 'ø')   # Replace o with stroke
#             .replace(u'\u00f4', 'ô')   # Replace o with circumflex
#             .replace(u'\u00e7', 'ç')   # Replace c with cedilla
#             .replace(u'\u00eb', 'ë')   # Replace e with diaeresis
#             .replace(u'\u00fc', 'ü')   # Replace u with diaeresis
#             .replace(u'\u00f1', 'ñ')   # Replace n with tilde
#             .replace(u'\u00e2', 'â')   # Replace a with circumflex
#             .replace(u'\u00e4', 'ä')   # Replace a with diaeresis
#             .replace(u'\u00c4', 'Ä')   # Replace A with diaeresis
#             .replace(u'\u00f6', 'ö')   # Replace o with diaeresis
#             .replace(u'\u00df', 'ß')   # Replace sharp s
#             .replace(u'\u00c0', 'À')   # Replace A with grave
#             .replace(u'\u00c9', 'É')   # Replace E with acute
#             .replace(u'\u00d3', 'Ó')   # Replace O with acute
#             .replace(u'\u00d1', 'Ñ')   # Replace N with tilde
#             .replace(u'\u00b0', '°')   # Replace degree symbol
#             .replace(u'\u00ba', 'º')   # Replace masculine ordinal indicator
#             .replace(u'\u00e5', 'å')   # Replace a with ring above
#             .replace(u'\u00c5', 'Å')   # Replace A with ring above
#             .replace(u'\u00f2', 'ò')   # Replace o with grave
#             .replace(u'\u00fa', 'ú')   # Replace u with acute
#             .replace(u'\u00e1', 'á')   # Replace a with acute
#             .replace(u'\u00ea', 'ê')   # Replace e with circumflex
#             .replace(u'\u00f5', 'õ')   # Replace o with tilde
#             )
#             return cleaned_text
#         return text

#     def store_in_firestore(self, place_data):
#         """Stores place data in Firestore."""
#         place_id = place_data.get('place_id')
#         doc_ref = self.db.collection('explore').document(str(place_id))
#         doc_ref.set(place_data)
#         self.log(f"Stored place_id {place_id} in Firestore")






# import scrapy
# from scrapy_selenium import SeleniumRequest
# import json
# import re

# class WanderlogSpider(scrapy.Spider):
#     name = "wanderlog"

#     def start_requests(self):
#         start_url = "https://wanderlog.com/explore"
#         yield SeleniumRequest(
#             url=start_url,
#             callback=self.parse_places,
#             wait_time=3,  # Wait for the content to load
#             script='window.scrollTo(0, document.body.scrollHeight);'  # Scroll to the bottom to load all places
#         )

#     def parse_places(self, response):
#         # Extract the place elements from the fully loaded page
#         places = response.css('div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3')
#         for place in places:
#             place_link = place.css('a.color-gray-900::attr(href)').get()
#             city_name = place.css('a.color-gray-900::text').get()

#             if place_link:
#                 # Construct the full URL
#                 place_url = response.urljoin(place_link)
#                 # Use SeleniumRequest to navigate to the place's explore page and handle the dynamic content
#                 yield SeleniumRequest(
#                     url=place_url,
#                     callback=self.parse_place_details,
#                     meta={'city_name': city_name}  # Pass the city name as metadata
#                 )

#     def parse_place_details(self, response):
#         city_name = response.meta['city_name']

#         # Find the JavaScript block that contains the window.__MOBX_STATE__ data
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
        
#         if script_text:
#             # Use regex to extract the JSON object from the JavaScript
#             mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
#             if mobx_data:
#                 # Load the JSON data
#                 mobx_json = json.loads(mobx_data.group(1))
                
#                 # Navigate to the explorePage and then to the data section
#                 explore_data = mobx_json.get('explorePage', {}).get('data', {})
                
#                 # Extract the relevant fields from the geo section and other fields
#                 geo_data = explore_data.get('geo', {})
#                 if geo_data:
#                     yield {
#                         'place_id': geo_data.get('id'),
#                         'city_name': geo_data.get('name', city_name),
#                         'state_name': geo_data.get('stateName'),
#                         'country_name': geo_data.get('countryName'),
#                         'depth': geo_data.get('depth'),
#                         'latitude': geo_data.get('latitude'),
#                         'longitude': geo_data.get('longitude'),
#                         'parent_id': geo_data.get('parentId'),
#                         'popularity': geo_data.get('popularity'),
#                         'subcategory': geo_data.get('subcategory'),
#                         'image_key': geo_data.get('imageKey'),
#                         'manual_description': geo_data.get('manualDescription'),
#                         'place_description': geo_data.get('placeDescription'),
#                         'bounds': geo_data.get('bounds'),
#                         # 'ancestors': geo_data.get('ancestors'),
#                         # 'multi_geos_cards': explore_data.get('multiGeosCards'),
#                         # 'nearby': explore_data.get('nearby'),
#                         # 'places_lists': explore_data.get('placesLists'),
#                         # 'geo_pairs': explore_data.get('geoPairs'),
#                         # 'categories': explore_data.get('categories'),
#                         # 'searched_categories': explore_data.get('searchedCategories'),
#                     }

#         # Optionally, handle additional scrolls or pagination if more places are available



# import scrapy
# from scrapy_selenium import SeleniumRequest
# from scrapy.http import HtmlResponse

# class WanderlogSpider(scrapy.Spider):
#     name = "wanderlog"


#     def start_requests(self):
#         start_url = "https://wanderlog.com/explore"
#         # Use SeleniumRequest to handle the dynamic content and scrolling
#         yield SeleniumRequest(
#             url=start_url,
#             callback=self.parse_places,
#             wait_time=3,  # Wait for content to load
#             script='window.scrollTo(0, document.body.scrollHeight);'  # Scroll to the bottom to load all places
#         )

#     def parse_places(self, response):
#         # Extract the place elements from the fully loaded page
#         places = response.css('div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3')
#         for place in places:
#             place_link = place.css('a.color-gray-900::attr(href)').get()
#             city_name = place.css('a.color-gray-900::text').get()

#             if place_link:
#                 place_id = place_link.split('/')[2]
#                 city_name_page = place_link.split('/')[-1]

#                 # Yield the extracted data
#                 yield {
#                     'place_id': place_id,
#                     'city_name': city_name,
#                     'city_name_page': city_name_page,
#                 }

#         # Optionally, if you want to scroll again and extract more, you can:
#         # Check if further scrolling is needed, and repeat the process by yielding another SeleniumRequest.
#         # Example:
#         # if some_condition_to_scroll_further:
#         #     yield SeleniumRequest(
#         #         url=response.url,
#         #         callback=self.parse_places,
#         #         script='window.scrollTo(0, document.body.scrollHeight);'
#         #     )








