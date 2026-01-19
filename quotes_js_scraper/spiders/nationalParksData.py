import scrapy
import json
import os
import re
import firebase_admin
from firebase_admin import credentials, firestore
from scrapy_selenium import SeleniumRequest

class NationalParksSpider(scrapy.Spider):
    name = "national_parks_spider"

    def __init__(self, *args, **kwargs):
        # Option 1: Define a manual list of national park URLs
        # This list contains URLs as strings.
        self.manual_parks = [
            "https://wanderlog.com/explore/3444/sundarbans-national-park",
            "https://wanderlog.com/explore/8135/panna-tiger-reserve",
            "https://wanderlog.com/explore/3795/bandhavgarh-national-park",
            # Add more URLs as needed.
        ]
        
        # Option 2: Load national park data from a JSON file if needed
        national_parks_file = os.path.join(os.getcwd(), 'cleaned_matched_national_parks.json')
        if os.path.exists(national_parks_file):
            with open(national_parks_file, 'r', encoding='utf-8') as f:
                self.national_parks_data = json.load(f)
        else:
            self.national_parks_data = []

        # Initialize Firebase Admin SDK
        firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
        cred = credentials.Certificate(firebase_credentials_path)
        firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    def start_requests(self):
        # If the manual_parks list (of URLs) is non-empty, use it.
        if self.manual_parks:
            for url in self.manual_parks:
                # Extract place_id and national_park using a regular expression
                # Expected URL format: https://wanderlog.com/explore/{place_id}/{national-park-name}
                pattern = r"https?://wanderlog\.com/explore/([^/]+)/([^/]+)"
                match = re.search(pattern, url)
                if match:
                    place_id = match.group(1)
                    # Convert national park slug to a title-case name (replace dashes with spaces)
                    national_park_slug = match.group(2)
                    national_park = national_park_slug.replace('-', ' ').title()
                else:
                    self.logger.warning(f"URL format not recognized: {url}")
                    continue

                yield SeleniumRequest(
                    url=url,
                    callback=self.parse_park_details,
                    wait_time=3,
                    script='window.scrollTo(0, document.body.scrollHeight);',
                    meta={
                        'place_id': place_id,
                        'national_park': national_park
                    }
                )
        else:
            # Otherwise, fall back on loading JSON file data to build URLs
            base_url = "https://wanderlog.com/explore/{}/{}"
            for park in self.national_parks_data:
                place_id = park['place_id']
                city_name_page = park['city_name_page']
                url = base_url.format(place_id, city_name_page)
                yield SeleniumRequest(
                    url=url,
                    callback=self.parse_park_details,
                    wait_time=3,
                    script='window.scrollTo(0, document.body.scrollHeight);',
                    meta={
                        'place_id': place_id,
                        'national_park': park['national_park']
                    }
                )

    def parse_park_details(self, response):
        place_id = response.meta['place_id']
        national_park = response.meta['national_park']
        script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()

        if script_text:
            mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
            if mobx_data:
                mobx_json = json.loads(mobx_data.group(1))
                explore_data = mobx_json.get('explorePage', {}).get('data', {})
                geo_data = explore_data.get('geo', {})
                sections = explore_data.get('sections', [])

                if geo_data:
                    park_data = {
                        'place_id': place_id,
                        'national_park': national_park,
                        'latitude': geo_data.get('latitude'),
                        'longitude': geo_data.get('longitude'),
                    }

                    # Store main park data in Firestore
                    self.store_park_data(park_data)

                    # Extract and store attractions and restaurants
                    for section in sections:
                        section_type = section.get('type')
                        if section_type in ['attractions', 'restaurants']:
                            subcollection_name = 'TouristAttractions' if section_type == 'attractions' else 'TouristRestaurants'
                            self.extract_and_store_subplaces(section, place_id, subcollection_name)

    def extract_and_store_subplaces(self, section, parent_place_id, subcollection_name):
        metadata = section.get('metadata', {})
        for place_id, place in metadata.items():
            subplace_data = {
                'id': str(place.get('id')),
                'name': place.get('name'),
                'description': place.get('description') or place.get('generatedDescription'),
                'categories': place.get('categories', []),
                'address': place.get('address'),
                'rating': place.get('rating'),
                'numRatings': place.get('numRatings'),
                'website': place.get('website'),
                'internationalPhoneNumber': place.get('internationalPhoneNumber'),
                'priceLevel': place.get('priceLevel'),
                'imageKeys': place.get('imageKeys', []),
                'placeId': place.get('placeId'),
                'permanentlyClosed': place.get('permanentlyClosed'),
                'ratingDistribution': place.get('ratingDistribution', {}),
                'utcOffset': place.get('utcOffset'),
                'openingPeriods': place.get('openingPeriods', []),
            }

            self.store_subplace_data(parent_place_id, subcollection_name, subplace_data)

    def store_park_data(self, park_data):
        place_id = park_data['place_id']
        doc_ref = self.db.collection('nationalParks').document(place_id)
        doc_ref.set(park_data)
        self.logger.info(f"Stored park data for {place_id}")

    def store_subplace_data(self, parent_place_id, subcollection_name, subplace_data):
        subplace_id = subplace_data.get('placeId') or subplace_data.get('id')
        if subplace_id:
            subplace_id = str(subplace_id)
            doc_ref = self.db.collection('nationalParks').document(parent_place_id).collection(subcollection_name).document(subplace_id)
            doc_ref.set(subplace_data)
            self.logger.info(f"Stored {subcollection_name[:-1]} data for {subplace_id} under {parent_place_id}")
        else:
            self.logger.warning(f"Subplace missing 'placeId' or 'id', data: {subplace_data}")

    def clean_text(self, text):
        """Cleans up special unicode characters in text."""
        if text:
            cleaned_text = (
                text.replace(u'\u2019', "'")
                    .replace(u'\u2014', "-")
                    # Add more replacements as needed
            )
            return cleaned_text
        return text





# import scrapy
# import json
# import os
# import re
# import firebase_admin
# from firebase_admin import credentials, firestore
# from scrapy_selenium import SeleniumRequest

# class NationalParksSpider(scrapy.Spider):
#     name = "national_parks_spider"

#     def __init__(self):
#         # Load national parks data from the JSON file
#         national_parks_file = os.path.join(os.getcwd(), 'cleaned_matched_national_parks.json')
#         with open(national_parks_file, 'r', encoding='utf-8') as f:
#             self.national_parks_data = json.load(f)

#         # Initialize Firebase Admin SDK
#         firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         self.db = firestore.client()

#     def start_requests(self):
#         # Create URL for each national park and start scraping
#         base_url = "https://wanderlog.com/explore/{}/{}"
#         for park in self.national_parks_data:
#             place_id = park['place_id']
#             city_name_page = park['city_name_page']
#             url = base_url.format(place_id, city_name_page)
#             yield SeleniumRequest(
#                 url=url,
#                 callback=self.parse_park_details,
#                 wait_time=3,
#                 script='window.scrollTo(0, document.body.scrollHeight);',
#                 meta={'place_id': place_id, 'national_park': park['national_park']}
#             )

#     def parse_park_details(self, response):
#         place_id = response.meta['place_id']
#         national_park = response.meta['national_park']
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()

#         if script_text:
#             mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
#             if mobx_data:
#                 mobx_json = json.loads(mobx_data.group(1))
#                 explore_data = mobx_json.get('explorePage', {}).get('data', {})
#                 geo_data = explore_data.get('geo', {})
#                 sections = explore_data.get('sections', [])

#                 if geo_data:
#                     park_data = {
#                         'place_id': place_id,
#                         'national_park': national_park,
#                         'latitude': geo_data.get('latitude'),
#                         'longitude': geo_data.get('longitude'),
#                     }

#                     # Store main park data in Firestore
#                     self.store_park_data(park_data)

#                     # Extract and store attractions and restaurants
#                     for section in sections:
#                         section_type = section.get('type')
#                         if section_type in ['attractions', 'restaurants']:
#                             self.extract_and_store_subplaces(
#                                 section, place_id, 'TouristAttractions' if section_type == 'attractions' else 'TouristRestaurants'
#                             )

#     def extract_and_store_subplaces(self, section, parent_place_id, subcollection_name):
#         metadata = section.get('metadata', {})
#         for place_id, place in metadata.items():
#             subplace_data = {
#                 'id': str(place.get('id')),
#                 'name': place.get('name'),
#                 'description': place.get('description') or place.get('generatedDescription'),
#                 'categories': place.get('categories', []),
#                 'address': place.get('address'),
#                 'rating': place.get('rating'),
#                 'numRatings': place.get('numRatings'),
#                 'website': place.get('website'),
#                 'internationalPhoneNumber': place.get('internationalPhoneNumber'),
#                 'priceLevel': place.get('priceLevel'),
#                 'imageKeys': place.get('imageKeys', []),
#                 'placeId': place.get('placeId'),
#                 'permanentlyClosed': place.get('permanentlyClosed'),
#                 'ratingDistribution': place.get('ratingDistribution', {}),
#                 'utcOffset': place.get('utcOffset'),
#                 'openingPeriods': place.get('openingPeriods', []),
#             }

#             # Store subplace data in Firestore under the appropriate subcollection
#             self.store_subplace_data(
#                 parent_place_id, subcollection_name, subplace_data)

#     def store_park_data(self, park_data):
#         place_id = park_data['place_id']
#         doc_ref = self.db.collection('nationalParks').document(place_id)
#         doc_ref.set(park_data)
#         self.logger.info(f"Stored park data for {place_id}")

#     def store_subplace_data(self, parent_place_id, subcollection_name, subplace_data):
#         subplace_id = subplace_data.get('placeId') or subplace_data.get('id')
#         if subplace_id:
#             subplace_id = str(subplace_id)
#             doc_ref = self.db.collection('nationalParks').document(parent_place_id).collection(
#                 subcollection_name).document(subplace_id)
#             doc_ref.set(subplace_data)
#             self.logger.info(
#                 f"Stored {subcollection_name[:-1]} data for {subplace_id} under {parent_place_id}")
#         else:
#             self.logger.warning(
#                 f"Subplace missing 'placeId' or 'id', data: {subplace_data}")

#     def clean_text(self, text):
#         """Cleans up special unicode characters in text."""
#         if text:
#             cleaned_text = (text
#                             .replace(u'\u2019', "'")   # Replace right single quote
#                             .replace(u'\u2014', "-")   # Replace em dash
#                             # Other replacements...
#                             )
#             return cleaned_text
#         return text
