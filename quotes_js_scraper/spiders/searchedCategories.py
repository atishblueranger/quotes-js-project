import scrapy
from scrapy_selenium import SeleniumRequest
import json
import re
import os
import firebase_admin
from firebase_admin import credentials, firestore


class SearchedCategoriesSpider(scrapy.Spider):
    name = "searchedCategoriesData"

    def __init__(self):
        # Initialize Firebase Admin SDK
        firebase_credentials_path = os.path.join(
            os.getcwd(),
            r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
        )
        cred = credentials.Certificate(firebase_credentials_path)
        firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    def start_requests(self):
        # Starting URL for the searched categories
        start_url = "https://wanderlog.com/searchedCategories"
        yield SeleniumRequest(
            url=start_url,
            callback=self.parse_cities,
            wait_time=3,
            script='window.scrollTo(0, document.body.scrollHeight);'
        )

    def parse_cities(self, response):
        # Extracting city links from the searchedCategories page
        city_links = response.css(
            'div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 a.color-gray-900::attr(href)'
        ).getall()
        
        if city_links:
            for city_link in city_links:
                city_url = response.urljoin(city_link)
                yield SeleniumRequest(
                    url=city_url,
                    callback=self.parse_searched_category,
                )
        else:
            self.logger.warning("No city links found on the searchedCategories page.")

    def parse_searched_category(self, response):
        # Extract the search categories for each city
        search_categories = response.css(
            'div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 a.color-gray-900::attr(href)'
        ).getall()
        
        if search_categories:
            for category_link in search_categories:
                category_url = response.urljoin(category_link)
                yield SeleniumRequest(
                    url=category_url,
                    callback=self.parse_places_list,
                )
        else:
            self.logger.warning(f"No categories found for city: {response.url}")

    def parse_places_list(self, response):
        script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()

        if script_text:
            mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
            if mobx_data:
                mobx_json = json.loads(mobx_data.group(1))
                places_list_page = mobx_json.get('placesListPage', {})
                data = places_list_page.get('data', {})
                geo_data = data.get('geo', {})
                place_metadata = data.get('placeMetadata', [])

                if geo_data and place_metadata:
                    city_id = str(geo_data.get('id'))
                    city_name = geo_data.get('name')

                    # Extract the title of the category
                    category_title = data.get('title', 'Unknown Category')

                    # Clean the category title for use as Firestore collection name
                    category_title = self.clean_category_title(category_title)

                    # Store places data in Firestore
                    for index, place in enumerate(place_metadata):
                        place_data = self.extract_place_data(place, index)
                        self.store_in_firestore(
                            city_id=city_id,
                            city_name=city_name,
                            category_title=category_title,
                            place_data=place_data
                        )
                else:
                    self.logger.warning("Missing geo_data or place_metadata in places_list_page data.")

    def extract_place_data(self, place, index):
        """Extract relevant place details to store in Firestore"""
        return {
            'index': index,
            'id': str(place.get('id')),
            'name': place.get('name'),
            'placeId': str(place.get('placeId')),
            'description': place.get('description'),
            'generalDescription': place.get('generatedDescription'),
            'categories': place.get('categories', []),
            'minMinutesSpent': place.get('minMinutesSpent'),
            'maxMinutesSpent': place.get('maxMinutesSpent'),
            'address': place.get('address'),
            'rating': place.get('rating'),
            'numRatings': place.get('numRatings'),
            'tripadvisorRating': place.get('tripadvisorRating'),
            'tripadvisorNumRatings': place.get('tripadvisorNumRatings'),
            'website': place.get('website'),
            'internationalPhoneNumber': place.get('internationalPhoneNumber'),
            'priceLevel': place.get('priceLevel'),
            'imageKeys': place.get('imageKeys', []),
            'permanentlyClosed': place.get('permanentlyClosed'),
            'ratingDistribution': place.get('ratingDistribution', {}),
            'utcOffset': place.get('utcOffset'),
            'openingPeriods': place.get('openingPeriods', []),
            'sources': [],  # Keeping it empty as per your request
            'reviews': []   # Keeping it empty as per your request
        }

    def store_in_firestore(self, city_id, city_name, category_title, place_data):
        """Stores place data into Firestore under the correct city and category"""
        # Store city data if not already stored
        city_doc_ref = self.db.collection('searchedCategories2').document(city_id)
        city_doc_ref.set({'city_name': city_name}, merge=True)

        # Store place data under the category subcollection
        place_id = place_data.get('placeId')
        if place_id:
            doc_ref = city_doc_ref.collection(category_title).document(place_id)
            doc_ref.set(place_data)
            self.logger.info(f"Stored place data for {place_id} in city: {city_name}, category: {category_title}")
        else:
            self.logger.warning(f"Place missing 'id', data: {place_data}")

    def clean_category_title(self, title):
        """Cleans the category title to be used as a Firestore collection name"""
        # Replace or remove characters that are not allowed in Firestore collection names
        invalid_chars = ['/', '\\', '.', '[', ']', '#', '$']
        for char in invalid_chars:
            title = title.replace(char, '-')
        return title

    # def clean_text(self, text):
    #     """Cleans up special unicode characters in text."""
    #     if text:
    #         cleaned_text = (text
    #                         .replace(u'\u2019', "'")   # Replace right single quote
    #                         .replace(u'\u2014', "-")   # Replace em dash
    #                         # ... other replacements ...
    #                         )
    #         return cleaned_text
    #     return text




# For one city and one category
# import scrapy
# from scrapy_selenium import SeleniumRequest
# import json
# import re
# import os
# import firebase_admin
# from firebase_admin import credentials, firestore


# class SearchedCategoriesSpider(scrapy.Spider):
#     name = "searchedCategoriesData"

#     def __init__(self):
#         # Initialize Firebase Admin SDK
#         firebase_credentials_path = os.path.join(
#             os.getcwd(),
#             r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#         )
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         self.db = firestore.client()

#     def start_requests(self):
#         # You can pass a 'city_url' argument when running the spider
#         city_url = getattr(self, 'city_url', None)
#         if city_url:
#             yield SeleniumRequest(
#                 url=city_url,
#                 callback=self.parse_searched_category,
#                 wait_time=3,
#                 script='window.scrollTo(0, document.body.scrollHeight);'
#             )
#         else:
#             # Starting URL for the searched categories
#             start_url = "https://wanderlog.com/searchedCategories"
#             yield SeleniumRequest(
#                 url=start_url,
#                 callback=self.parse_cities,
#                 wait_time=3,
#                 script='window.scrollTo(0, document.body.scrollHeight);'
#             )

#     def parse_cities(self, response):
#         # Extracting city links from the searchedCategories page
#         city_links = response.css(
#             'div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 a.color-gray-900::attr(href)'
#         ).getall()
        
#         if city_links:
#             # Process only the first city
#             city_link = city_links[0]
#             city_url = response.urljoin(city_link)
#             yield SeleniumRequest(
#                 url=city_url,
#                 callback=self.parse_searched_category,
#             )
#         else:
#             self.logger.warning("No city links found on the searchedCategories page.")

#     def parse_searched_category(self, response):
#         # Extract the search categories for each city
#         search_categories = response.css(
#             'div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 a.color-gray-900::attr(href)'
#         ).getall()
        
#         if search_categories:
#             # Process only the first category
#             category_link = search_categories[0]
#             category_url = response.urljoin(category_link)
#             yield SeleniumRequest(
#                 url=category_url,
#                 callback=self.parse_places_list,
#             )
#         else:
#             self.logger.warning("No categories found for this city.")

#     def parse_places_list(self, response):
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()

#         if script_text:
#             mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
#             if mobx_data:
#                 mobx_json = json.loads(mobx_data.group(1))
#                 places_list_page = mobx_json.get('placesListPage', {})
#                 data = places_list_page.get('data', {})
#                 geo_data = data.get('geo', {})
#                 place_metadata = data.get('placeMetadata', [])

#                 if geo_data and place_metadata:
#                     city_id = str(geo_data.get('id'))
#                     city_name = geo_data.get('name')

#                     # Extract the title of the category
#                     category_title = data.get('title', 'Unknown Category')

#                     # Clean the category title for use as Firestore collection name
#                     category_title = self.clean_category_title(category_title)

#                     # Store places data in Firestore
#                     for index, place in enumerate(place_metadata):
#                         place_data = self.extract_place_data(place, index)
#                         self.store_in_firestore(
#                             city_id=city_id,
#                             city_name=city_name,
#                             category_title=category_title,
#                             place_data=place_data
#                         )
#                 else:
#                     self.logger.warning("Missing geo_data or place_metadata in places_list_page data.")

#     def extract_place_data(self, place, index):
#         """Extract relevant place details to store in Firestore"""
#         return {
#             'index': index,
#             'id': str(place.get('id')),
#             'name': place.get('name'),
#             'placeId': str(place.get('placeId')),
#             'description': place.get('description'),
#             'generalDescription': place.get('generatedDescription'),
#             'categories': place.get('categories', []),
#             'minMinutesSpent': place.get('minMinutesSpent'),
#             'maxMinutesSpent': place.get('maxMinutesSpent'),
#             'address': place.get('address'),
#             'rating': place.get('rating'),
#             'numRatings': place.get('numRatings'),
#             'tripadvisorRating': place.get('tripadvisorRating'),
#             'tripadvisorNumRatings': place.get('tripadvisorNumRatings'),
#             'website': place.get('website'),
#             'internationalPhoneNumber': place.get('internationalPhoneNumber'),
#             'priceLevel': place.get('priceLevel'),
#             'imageKeys': place.get('imageKeys', []),
#             'permanentlyClosed': place.get('permanentlyClosed'),
#             'ratingDistribution': place.get('ratingDistribution', {}),
#             'utcOffset': place.get('utcOffset'),
#             'openingPeriods': place.get('openingPeriods', []),
#             'sources': [],  # Keeping it empty as per your request
#             'reviews': []   # Keeping it empty as per your request
#         }

#     def store_in_firestore(self, city_id, city_name, category_title, place_data):
#         """Stores place data into Firestore under the correct city and category"""
#         # Store city data if not already stored
#         city_doc_ref = self.db.collection('searchedCategoriesNew').document(city_id)
#         city_doc_ref.set({'city_name': city_name}, merge=True)

#         # Store place data under the category subcollection
#         place_id = place_data.get('placeId')
#         if place_id:
#             doc_ref = city_doc_ref.collection(category_title).document(place_id)
#             doc_ref.set(place_data)
#             self.logger.info(f"Stored place data for {place_id} in city: {city_name}, category: {category_title}")
#         else:
#             self.logger.warning(f"Place missing 'id', data: {place_data}")

#     def clean_category_title(self, title):
#         """Cleans the category title to be used as a Firestore collection name"""
#         # Replace or remove characters that are not allowed in Firestore collection names
#         invalid_chars = ['/', '\\', '.', '[', ']', '#', '$']
#         for char in invalid_chars:
#             title = title.replace(char, '-')
#         return title

    def clean_text(self, text):
        """Cleans up special unicode characters in text."""
        if text:
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



# import scrapy
# from scrapy_selenium import SeleniumRequest
# import json
# import re
# import os
# import firebase_admin
# from firebase_admin import credentials, firestore


# class SearchedCategoriesSpider(scrapy.Spider):
#     name = "searchedCategoriesData"

#     def __init__(self):
#         # Initialize Firebase Admin SDK
#         firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         self.db = firestore.client()

#     def start_requests(self):
#         # Starting URL for the searched categories
#         start_url = "https://wanderlog.com/searchedCategories"
#         yield SeleniumRequest(
#             url=start_url,
#             callback=self.parse_cities,
#             wait_time=3,
#             script='window.scrollTo(0, document.body.scrollHeight);'
#         )

#     def parse_cities(self, response):
#         # Extracting city links from the searchedCategories page
#         city_links = response.css('div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 a.color-gray-900::attr(href)').getall()
#         for city_link in city_links:
#             city_url = response.urljoin(city_link)
#             yield SeleniumRequest(
#                 url=city_url,
#                 callback=self.parse_searched_category,
#             )

#     def parse_searched_category(self, response):
#         # Extract the search categories for each city (e.g., "New Delhi", etc.)
#         search_categories = response.css('div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 a.color-gray-900::attr(href)').getall()
#         for category_link in search_categories:
#             category_url = response.urljoin(category_link)
#             yield SeleniumRequest(
#                 url=category_url,
#                 callback=self.parse_places_list,
#             )

#     def parse_places_list(self, response):
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()

#         if script_text:
#             mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
#             if mobx_data:
#                 mobx_json = json.loads(mobx_data.group(1))
#                 places_list_page = mobx_json.get('placesListPage', {})
#                 data = places_list_page.get('data', {})
#                 geo_data = data.get('geo', {})
#                 place_metadata = data.get('placeMetadata', [])

#                 if geo_data and place_metadata:
#                     city_id = str(geo_data.get('id'))
#                     city_name = geo_data.get('name')

#                     # Extract the title of the category (e.g., "The 50 best breakfast spots in New Delhi")
#                     category_title = data.get('title', 'Unknown Category')

#                     # Clean the category title for use as Firestore collection name
#                     category_title = self.clean_category_title(category_title)

#                     # Store places data in Firestore
#                     for place in place_metadata:
#                         place_data = self.extract_place_data(place)
#                         self.store_in_firestore(
#                             city_id=city_id,
#                             city_name=city_name,
#                             category_title=category_title,
#                             place_data=place_data
#                         )
#                 else:
#                     self.logger.warning("Missing geo_data or place_metadata in places_list_page data.")

#     def extract_place_data(self, place):
#         """Extract relevant place details to store in Firestore"""
#         return {
#             'id': str(place.get('id')),
#             'name': place.get('name'),
#             'placeId':str(place.get('placeId')),
#             'description': place.get('description'),
#             'generalDescription':place.get('generatedDescription'),
#             'categories': place.get('categories', []),
#             'minMinutesSpent':place.get('minMinutesSpent'),
#             'maxMinutesSpent':place.get('maxMinutesSpent'),
#             'address': place.get('address'),
#             'rating': place.get('rating'),
#             'numRatings': place.get('numRatings'),
#             'tripadvisorRating': place.get('tripadvisorRating'),
#             'tripadvisorNumRatings': place.get('tripadvisorNumRatings'),
#             'website': place.get('website'),
#             'internationalPhoneNumber': place.get('internationalPhoneNumber'),
#             'priceLevel': place.get('priceLevel'),
#             'imageKeys': place.get('imageKeys', []),
#             'permanentlyClosed': place.get('permanentlyClosed'),
#             'ratingDistribution': place.get('ratingDistribution', {}),
#             'utcOffset': place.get('utcOffset'),
#             'openingPeriods': place.get('openingPeriods', []),
#             'sources': [],  # Keeping it empty as per your request
#             'reviews': []   # Keeping it empty as per your request
#         }

#     def store_in_firestore(self, city_id, city_name, category_title, place_data):
#         """Stores place data into Firestore under the correct city and category"""
#         # Store city data if not already stored
#         city_doc_ref = self.db.collection('searchedCategories').document(city_id)
#         city_doc_ref.set({'city_name': city_name}, merge=True)

#         # Store place data under the category subcollection
#         place_id = place_data.get('placeId')
#         if place_id:
#             doc_ref = city_doc_ref.collection(category_title).document(place_id)
#             doc_ref.set(place_data)
#             self.logger.info(f"Stored place data for {place_id} in city: {city_name}, category: {category_title}")
#         else:
#             self.logger.warning(f"Place missing 'id', data: {place_data}")

#     def clean_category_title(self, title):
#         """Cleans the category title to be used as a Firestore collection name"""
#         # Replace or remove characters that are not allowed in Firestore collection names
#         invalid_chars = ['/', '\\', '.', '[', ']', '#', '$']
#         for char in invalid_chars:
#             title = title.replace(char, '-')
#         return title

#     # def clean_text(self, text):
#     #     """Cleans up special unicode characters in text."""
#     #     if text:
#     #         cleaned_text = (text
#     #                         .replace(u'\u2019', "'")   # Replace right single quote
#     #                         .replace(u'\u2014', "-")   # Replace em dash
#     #                         # Other replacements...
#     #                         )
#     #         return cleaned_text
#     #     return text
    
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
