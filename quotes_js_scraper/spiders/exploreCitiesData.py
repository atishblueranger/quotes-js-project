import scrapy
from scrapy_selenium import SeleniumRequest
import json
import re
import os
import firebase_admin
from firebase_admin import credentials, firestore

class WanderlogSpider(scrapy.Spider):
    name = "exploreCitiesData"

    def __init__(self):
        self.places_data = []  # List to store all the places data

        # Initialize Firebase Admin SDK
        firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
        cred = credentials.Certificate(firebase_credentials_path)
        firebase_admin.initialize_app(cred)
        self.db = firestore.client()

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
                sections = explore_data.get('sections', [])

                if geo_data:
                    # Clean the descriptions
                    place_data = {
                        'place_id': str(geo_data.get('id')),
                        'city_name': geo_data.get('name', city_name),
                        'latitude': geo_data.get('latitude'),
                        'longitude': geo_data.get('longitude'),
                    }

                    # Store main place data in Firestore
                    self.store_place_data(place_data)

                    # Extract attractions and restaurants from sections
                    for section in sections:
                        section_type = section.get('type')
                        if section_type == 'attractions' or section_type == 'restaurants':
                            self.extract_and_store_subplaces(
                                section, place_data['place_id'], 'TouristAttractions' if section_type == 'attractions' else 'TouristRestaurants')

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

            # Store subplace data in Firestore under the appropriate subcollection
            self.store_subplace_data(
                parent_place_id, subcollection_name, subplace_data)

    def store_place_data(self, place_data):
        place_id = place_data['place_id']
        doc_ref = self.db.collection('exploreData').document(place_id)
        doc_ref.set(place_data)
        self.logger.info(f"Stored main place data for {place_id}")

    def store_subplace_data(self, parent_place_id, subcollection_name, subplace_data):
        subplace_id = subplace_data.get('placeId') or subplace_data.get('id')
        if subplace_id:
            subplace_id = str(subplace_id)
            doc_ref = self.db.collection('exploreData').document(parent_place_id).collection(
                subcollection_name).document(subplace_id)
            doc_ref.set(subplace_data)
            self.logger.info(
                f"Stored {subcollection_name[:-1]} data for {subplace_id} under {parent_place_id}")
        else:
            self.logger.warning(
                f"Subplace missing 'placeId' or 'id', data: {subplace_data}")

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

    # def close(self, reason):
    #     """Optional: Writes all collected data to a JSON file when the spider finishes."""
    #     if self.places_data:
    #         output_file = os.path.join(os.getcwd(), 'explore_places_data.json')
    #         with open(output_file, 'w', encoding='utf-8') as f:
    #             json.dump(self.places_data, f, ensure_ascii=False, indent=4)
    #         self.logger.info(f"Saved {len(self.places_data)} places to explore_places_data.json")
