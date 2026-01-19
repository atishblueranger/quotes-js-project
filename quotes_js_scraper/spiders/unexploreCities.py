import scrapy
from scrapy_selenium import SeleniumRequest
import json
import re
import firebase_admin
from firebase_admin import credentials, firestore
import os
import logging

class WanderlogSpider(scrapy.Spider):
    name = "allPlaces"

    # Define a custom setting to hold the place IDs to scrape
    custom_settings = {
        'JOBDIR': 'crawls/allplaces_subset', # Optional: Separate job directory for subset runs
    }

    def __init__(self, place_ids_to_scrape=None, *args, **kwargs):
        super(WanderlogSpider, self).__init__(*args, **kwargs)
        # Initialize Firebase Admin SDK
        firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
        try:
            # Check if app is already initialized to avoid error on re-runs in same process
            firebase_admin.get_app()
        except ValueError:
            cred = credentials.Certificate(firebase_credentials_path)
            firebase_admin.initialize_app(cred)

        self.db = firestore.client()  # Firestore database instance

        # Load the list of places from 'cleanedExploreTripPlannercities.json'
        all_places = self.load_places_list()

        # --- New Filtering Logic ---
        self.places_list = []
        if place_ids_to_scrape:
            # Convert the comma-separated string of IDs into a set for efficient lookup
            desired_place_ids = set(place_ids_to_scrape.split(','))
            self.log(f"Filtering for place IDs: {desired_place_ids}", level=logging.INFO)

            # Filter the loaded places list
            for place in all_places:
                if str(place.get('place_id')) in desired_place_ids: # Convert to string for comparison
                    self.places_list.append(place)
            
            if not self.places_list:
                 self.log(f"No matching place_id found in the JSON for the provided list: {place_ids_to_scrape}", level=logging.WARNING)
        else:
            # If no place_ids_to_scrape are provided, scrape all places
            self.places_list = all_places
            self.log("No specific place_ids_to_scrape provided. Scraping all places.", level=logging.INFO)
        # --- End New Filtering Logic ---


    def load_places_list(self):
        # Load the list of places from 'cleanedExploreTripPlannercities.json'
        try:
            with open('cleanedExploreTripPlannercities.json', 'r', encoding='utf-8') as f:
                places = json.load(f)
                self.log(f"Loaded {len(places)} places from JSON file.", level=logging.INFO)
            return places
        except FileNotFoundError:
            self.log("Error: cleanedExploreTripPlannercities copy.json not found.", level=logging.ERROR)
            return []
        except json.JSONDecodeError:
            self.log("Error: Could not decode JSON from cleanedExploreTripPlannercities copy.json.", level=logging.ERROR)
            return []


    def start_requests(self):
        base_url = "https://wanderlog.com/explore"
        if not self.places_list:
            self.log("Places list is empty after filtering. No requests to schedule.", level=logging.WARNING)
            return # Stop if there are no places to scrape

        for place in self.places_list:
            place_id = place.get('place_id')
            city_name_page = place.get('city_name_page')
            if place_id and city_name_page:
                # Construct the URL using the new JSON data
                place_url = f"{base_url}/{place_id}/{city_name_page}"
                self.log(f"Scheduling request for place_id: {place_id} at URL: {place_url}", level=logging.DEBUG)
                yield SeleniumRequest(
                    url=place_url,
                    callback=self.parse_place_details,
                    meta={
                        'city_name': place.get('city_name'),
                        'place_id': place_id,
                        'city_name_page': city_name_page
                    },
                    wait_time=3,
                    errback=self.handle_error
                )
            else:
                self.log(f"Missing place_id or city_name_page for place: {place}. Skipping.", level=logging.WARNING)

    def parse_place_details(self, response):
        city_name = response.meta.get('city_name')
        place_id = response.meta.get('place_id')
        city_name_page = response.meta.get('city_name_page')

        self.log(f"Parsing details for {city_name} (place_id: {place_id})", level=logging.INFO)

        script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()

        if script_text:
            mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text, re.DOTALL) # Use re.DOTALL
            if mobx_data:
                try:
                    mobx_json = json.loads(mobx_data.group(1))
                    explore_data = mobx_json.get('explorePage', {}).get('data', {})
                    geo_data = explore_data.get('geo', {})

                    if geo_data:
                        # Clean the descriptions
                        manual_description = self.clean_text(geo_data.get('manualDescription'))
                        place_description = self.clean_text(geo_data.get('placeDescription'))

                        place_data = {
                            'place_id': geo_data.get('id', place_id),
                            'city_name': geo_data.get('name', city_name),
                            'state_name': geo_data.get('stateName'),
                            'country_name': geo_data.get('countryName'),
                            'depth': geo_data.get('depth'),
                            'latitude': geo_data.get('latitude'),
                            'longitude': geo_data.get('longitude'),
                            'parent_id': geo_data.get('parentId'),
                            'popularity': geo_data.get('popularity'),
                            'subcategory': geo_data.get('subcategory'),
                            'image_key': geo_data.get('imageKey'),
                            'manual_description': manual_description,
                            'place_description': place_description,
                            'bounds': geo_data.get('bounds'),
                        }

                        # Extract 'nearby' and 'ancestors' data
                        nearby_places = explore_data.get('nearby', [])
                        ancestors = explore_data.get('ancestors', [])

                        # Store data in Firebase Firestore
                        self.store_in_firestore(place_data, nearby_places, ancestors)
                    else:
                        self.log(f"No geo_data found in MOBX state for {city_name} (place_id: {place_id})", level=logging.WARNING)
                except json.JSONDecodeError:
                     self.log(f"Failed to decode MOBX JSON data for {city_name} (place_id: {place_id})", level=logging.WARNING)
                except Exception as e:
                     self.log(f"An error occurred while processing MOBX data for {city_name} (place_id: {place_id}): {e}", level=logging.ERROR)
            else:
                self.log(f"Could not find window.__MOBX_STATE__ data in script tag for {city_name} (place_id: {place_id})", level=logging.WARNING)
        else:
            self.log(f"No script text containing window.__MOBX_STATE__ found for {city_name} (place_id: {place_id})", level=logging.WARNING)


    def handle_error(self, failure):
        request = failure.request
        self.log(f"Request failed: {failure.value} on {request.url}", level=logging.ERROR)

    def clean_text(self, text):
        """Cleans up special unicode characters in text."""
        if text:
            # Ensure text is a string before replacing
            if not isinstance(text, str):
                 self.log(f"Warning: clean_text received non-string input: {text}", level=logging.WARNING)
                 return text # Return as is if not a string

            cleaned_text = (text
                .replace(u'\u2019', "'")   # Replace right single quote
                .replace(u'\u2014', "-")   # Replace em dash
                .replace(u'\u00f3', 'ó')   # Replace o with acute
                .replace(u'\u201c', '"')   # Replace left double quote
                .replace(u'\u201d', '"')   # Replace right double quote
                .replace(u'\u00f9', 'ù')   # Replace u with grave
                .replace(u'\u1ea1', 'ạ')   # Replace a with dot below
                .replace(u'\u0111', 'đ')   # Replace d with stroke
                .replace(u'\u1ee7', 'ủ')   # Replace u with hook above
                .replace(u'\u00ed', 'í')   # Replace i with acute
                .replace(u'\u00e9', 'é')   # Replace e with acute
                .replace(u'\u00e8', 'è')   # Replace e with grave
                .replace(u'\u00e0', 'à')   # Replace a with grave
                .replace(u'\u00b2', '²')   # Replace superscript two
                .replace(u'\u00f8', 'ø')   # Replace o with stroke
                .replace(u'\u00f4', 'ô')   # Replace o with circumflex
                .replace(u'\u00e7', 'ç')   # Replace c with cedilla
                .replace(u'\u00eb', 'ë')   # Replace e with diaeresis
                .replace(u'\u00fc', 'ü')   # Replace u with diaeresis
                .replace(u'\u00f1', 'ñ')   # Replace n with tilde
                .replace(u'\u00e2', 'â')   # Replace a with circumflex
                .replace(u'\u00e4', 'ä')   # Replace a with diaeresis
                .replace(u'\u00c4', 'Ä')   # Replace A with diaeresis
                .replace(u'\u00f6', 'ö')   # Replace o with diaeresis
                .replace(u'\u00df', 'ß')   # Replace sharp s
                .replace(u'\u00c0', 'À')   # Replace A with grave
                .replace(u'\u00c9', 'É')   # Replace E with acute
                .replace(u'\u00d3', 'Ó')   # Replace O with acute
                .replace(u'\u00d1', 'Ñ')   # Replace N with tilde
                .replace(u'\u00b0', '°')   # Replace degree symbol
                .replace(u'\u00ba', 'º')   # Replace masculine ordinal indicator
                .replace(u'\u00e5', 'å')   # Replace a with ring above
                .replace(u'\u00c5', 'Å')   # Replace A with ring above
                .replace(u'\u00f2', 'ò')   # Replace o with grave
                .replace(u'\u00fa', 'ú')   # Replace u with acute
                .replace(u'\u00e1', 'á')   # Replace a with acute
                .replace(u'\u00ea', 'ê')   # Replace e with circumflex
                .replace(u'\u00f5', 'õ')   # Replace o with tilde
            )
            return cleaned_text
        return text

    def store_in_firestore(self, place_data, nearby_places, ancestors):
        """Stores place data and subcollections in Firestore."""
        place_id = place_data.get('place_id')
        if place_id is None:
            self.log("Cannot store data in Firestore: place_id is missing.", level=logging.ERROR)
            return

        doc_ref = self.db.collection('allplaces').document(str(place_id))
        try:
            doc_ref.set(place_data)
            self.log(f"Stored main place data for place_id {place_id} in Firestore", level=logging.INFO)

            # Store 'nearby' places as a subcollection
            if nearby_places:
                nearby_batch = self.db.batch()
                for nearby_place in nearby_places:
                    nearby_place_id = nearby_place.get('id')
                    if nearby_place_id:
                        nearby_doc_ref = doc_ref.collection('nearby').document(str(nearby_place_id))
                        nearby_batch.set(nearby_doc_ref, nearby_place)
                nearby_batch.commit()
                self.log(f"Stored {len(nearby_places)} nearby places for place_id {place_id}", level=logging.INFO)

            # Store 'ancestors' as a subcollection
            if ancestors:
                ancestors_batch = self.db.batch()
                for ancestor in ancestors:
                    ancestor_id = ancestor.get('id')
                    if ancestor_id:
                        ancestor_doc_ref = doc_ref.collection('ancestors').document(str(ancestor_id))
                        ancestors_batch.set(ancestor_doc_ref, ancestor)
                ancestors_batch.commit()
                self.log(f"Stored {len(ancestors)} ancestors for place_id {place_id}", level=logging.INFO)

        except Exception as e:
            self.log(f"Error storing data for place_id {place_id} in Firestore: {e}", level=logging.ERROR)
# Working
# import scrapy
# from scrapy_selenium import SeleniumRequest
# import json
# import re
# import firebase_admin
# from firebase_admin import credentials, firestore
# import os
# import logging

# class WanderlogSpider(scrapy.Spider):
#     name = "allPlaces"

#     def __init__(self):
#         # Initialize Firebase Admin SDK
#         firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         self.db = firestore.client()  # Firestore database instance

#         # Load the list of places from 'cleanedExploreTripPlannercities.json'
#         self.places_list = self.load_places_list()

#     def load_places_list(self):
#         # Load the list of places from 'cleanedExploreTripPlannercities.json'
#         with open('cleanedExploreTripPlannercities copy.json', 'r', encoding='utf-8') as f:
#             places = json.load(f)
#         return places

#     def start_requests(self):
#         base_url = "https://wanderlog.com/explore"
#         for place in self.places_list:
#             place_id = place.get('place_id')
#             city_name_page = place.get('city_name_page')
#             if place_id and city_name_page:
#                 # Construct the URL using the new JSON data
#                 place_url = f"{base_url}/{place_id}/{city_name_page}"
#                 yield SeleniumRequest(
#                     url=place_url,
#                     callback=self.parse_place_details,
#                     meta={
#                         'city_name': place.get('city_name'),
#                         'place_id': place_id,
#                         'city_name_page': city_name_page
#                     },
#                     wait_time=3,
#                     errback=self.handle_error
#                 )
#             else:
#                 self.log(f"Missing place_id or city_name_page for place: {place}", level=logging.WARNING)

#     def parse_place_details(self, response):
#         city_name = response.meta.get('city_name')
#         place_id = response.meta.get('place_id')
#         city_name_page = response.meta.get('city_name_page')

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
#                         'place_id': geo_data.get('id', place_id),
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

#                     # Extract 'nearby' and 'ancestors' data
#                     nearby_places = explore_data.get('nearby', [])
#                     ancestors = explore_data.get('ancestors', [])

#                     # Store data in Firebase Firestore
#                     self.store_in_firestore(place_data, nearby_places, ancestors)
#                 else:
#                     self.log(f"No geo_data found for {city_name}", level=logging.WARNING)
#             else:
#                 self.log(f"Failed to parse MOBX data for {city_name}", level=logging.WARNING)
#         else:
#             self.log(f"No script text found for {city_name}", level=logging.WARNING)

#     def handle_error(self, failure):
#         request = failure.request
#         self.log(f"Request failed: {failure.value} on {request.url}", level=logging.ERROR)

#     def clean_text(self, text):
#         """Cleans up special unicode characters in text."""
#         if text:
#             cleaned_text = (text
#                 .replace(u'\u2019', "'")   # Replace right single quote
#                 .replace(u'\u2014', "-")   # Replace em dash
#                 .replace(u'\u00f3', 'ó')   # Replace o with acute
#                 .replace(u'\u201c', '"')   # Replace left double quote
#                 .replace(u'\u201d', '"')   # Replace right double quote
#                 .replace(u'\u00f9', 'ù')   # Replace u with grave
#                 .replace(u'\u1ea1', 'ạ')   # Replace a with dot below
#                 .replace(u'\u0111', 'đ')   # Replace d with stroke
#                 .replace(u'\u1ee7', 'ủ')   # Replace u with hook above
#                 .replace(u'\u00ed', 'í')   # Replace i with acute
#                 .replace(u'\u00e9', 'é')   # Replace e with acute
#                 .replace(u'\u00e8', 'è')   # Replace e with grave
#                 .replace(u'\u00e0', 'à')   # Replace a with grave
#                 .replace(u'\u00b2', '²')   # Replace superscript two
#                 .replace(u'\u00f8', 'ø')   # Replace o with stroke
#                 .replace(u'\u00f4', 'ô')   # Replace o with circumflex
#                 .replace(u'\u00e7', 'ç')   # Replace c with cedilla
#                 .replace(u'\u00eb', 'ë')   # Replace e with diaeresis
#                 .replace(u'\u00fc', 'ü')   # Replace u with diaeresis
#                 .replace(u'\u00f1', 'ñ')   # Replace n with tilde
#                 .replace(u'\u00e2', 'â')   # Replace a with circumflex
#                 .replace(u'\u00e4', 'ä')   # Replace a with diaeresis
#                 .replace(u'\u00c4', 'Ä')   # Replace A with diaeresis
#                 .replace(u'\u00f6', 'ö')   # Replace o with diaeresis
#                 .replace(u'\u00df', 'ß')   # Replace sharp s
#                 .replace(u'\u00c0', 'À')   # Replace A with grave
#                 .replace(u'\u00c9', 'É')   # Replace E with acute
#                 .replace(u'\u00d3', 'Ó')   # Replace O with acute
#                 .replace(u'\u00d1', 'Ñ')   # Replace N with tilde
#                 .replace(u'\u00b0', '°')   # Replace degree symbol
#                 .replace(u'\u00ba', 'º')   # Replace masculine ordinal indicator
#                 .replace(u'\u00e5', 'å')   # Replace a with ring above
#                 .replace(u'\u00c5', 'Å')   # Replace A with ring above
#                 .replace(u'\u00f2', 'ò')   # Replace o with grave
#                 .replace(u'\u00fa', 'ú')   # Replace u with acute
#                 .replace(u'\u00e1', 'á')   # Replace a with acute
#                 .replace(u'\u00ea', 'ê')   # Replace e with circumflex
#                 .replace(u'\u00f5', 'õ')   # Replace o with tilde
#             )
#             return cleaned_text
#         return text

#     def store_in_firestore(self, place_data, nearby_places, ancestors):
#         """Stores place data and subcollections in Firestore."""
#         place_id = place_data.get('place_id')
#         doc_ref = self.db.collection('allplaces').document(str(place_id))
#         doc_ref.set(place_data)
#         self.log(f"Stored place_id {place_id} in Firestore", level=logging.INFO)

#         # Store 'nearby' places as a subcollection
#         if nearby_places:
#             for nearby_place in nearby_places:
#                 nearby_place_id = nearby_place.get('id')
#                 if nearby_place_id:
#                     nearby_doc_ref = doc_ref.collection('nearby').document(str(nearby_place_id))
#                     nearby_doc_ref.set(nearby_place)
#             self.log(f"Stored {len(nearby_places)} nearby places for place_id {place_id}", level=logging.INFO)

#         # Store 'ancestors' as a subcollection
#         if ancestors:
#             for ancestor in ancestors:
#                 ancestor_id = ancestor.get('id')
#                 if ancestor_id:
#                     ancestor_doc_ref = doc_ref.collection('ancestors').document(str(ancestor_id))
#                     ancestor_doc_ref.set(ancestor)
#             self.log(f"Stored {len(ancestors)} ancestors for place_id {place_id}", level=logging.INFO)



# import scrapy
# from scrapy_selenium import SeleniumRequest
# import json
# import re
# import firebase_admin
# from firebase_admin import credentials, firestore
# import os
# import logging

# class WanderlogSpider(scrapy.Spider):
#     name = "allPlaces"

#     def __init__(self):
#         # Initialize Firebase Admin SDK
#         firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         self.db = firestore.client()  # Firestore database instance

#         # Load the list of places from 'cleanedTripPlannercities.json'
#         self.places_list = self.load_places_list()

#     def load_places_list(self):
#         # Load the list of places from 'cleanedTripPlannercities.json'
#         with open('pendingTripPlannercities.json', 'r', encoding='utf-8') as f:
#             places = json.load(f)
#         return places

#     def start_requests(self):
#         base_url = "https://wanderlog.com/explore"
#         for place in self.places_list:
#             place_id = place.get('place_id')
#             city_name_page = place.get('city_name_page')
#             if place_id and city_name_page:
#                 # Construct the URL
#                 place_url = f"{base_url}/{place_id}/{city_name_page}"
#                 yield SeleniumRequest(
#                     url=place_url,
#                     callback=self.parse_place_details,
#                     meta={
#                         'city_name': place.get('city_name'),
#                         'place_id': place_id,
#                         'city_name_page': city_name_page
#                     },
#                     wait_time=3,
#                     errback=self.handle_error
#                 )
#             else:
#                 self.log(f"Missing place_id or city_name_page for place: {place}", level=logging.WARNING)

#     def parse_place_details(self, response):
#         city_name = response.meta.get('city_name')
#         place_id = response.meta.get('place_id')
#         city_name_page = response.meta.get('city_name_page')

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
#                         'place_id': geo_data.get('id', place_id),
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

#                     # Extract 'nearby' and 'ancestors' data
#                     nearby_places = explore_data.get('nearby', [])
#                     ancestors = explore_data.get('ancestors', [])

#                     # Store data in Firebase Firestore
#                     self.store_in_firestore(place_data, nearby_places, ancestors)
#                 else:
#                     self.log(f"No geo_data found for {city_name}", level=logging.WARNING)
#             else:
#                 self.log(f"Failed to parse MOBX data for {city_name}", level=logging.WARNING)
#         else:
#             self.log(f"No script text found for {city_name}", level=logging.WARNING)

#     def handle_error(self, failure):
#         request = failure.request
#         self.log(f"Request failed: {failure.value} on {request.url}", level=logging.ERROR)

#     def clean_text(self, text):
#         """Cleans up special unicode characters in text."""
#         if text:
#             cleaned_text = (text
#  .replace(u'\u2019', "'")   # Replace right single quote
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
#             .replace(u'\u00f5', 'õ')  # Replace o with tilde
#             )
#             return cleaned_text
#         return text

#     def store_in_firestore(self, place_data, nearby_places, ancestors):
#         """Stores place data and subcollections in Firestore."""
#         place_id = place_data.get('place_id')
#         doc_ref = self.db.collection('allplaces').document(str(place_id))
#         doc_ref.set(place_data)
#         self.log(f"Stored place_id {place_id} in Firestore", level=logging.INFO)

#         # Store 'nearby' places as a subcollection
#         if nearby_places:
#             for nearby_place in nearby_places:
#                 nearby_place_id = nearby_place.get('id')
#                 if nearby_place_id:
#                     nearby_doc_ref = doc_ref.collection('nearby').document(str(nearby_place_id))
#                     nearby_doc_ref.set(nearby_place)
#             self.log(f"Stored {len(nearby_places)} nearby places for place_id {place_id}", level=logging.INFO)

#         # Store 'ancestors' as a subcollection
#         if ancestors:
#             for ancestor in ancestors:
#                 ancestor_id = ancestor.get('id')
#                 if ancestor_id:
#                     ancestor_doc_ref = doc_ref.collection('ancestors').document(str(ancestor_id))
#                     ancestor_doc_ref.set(ancestor)
#             self.log(f"Stored {len(ancestors)} ancestors for place_id {place_id}", level=logging.INFO)
