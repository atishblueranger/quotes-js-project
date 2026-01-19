import scrapy
from scrapy_selenium import SeleniumRequest
import json
import re
import os
import firebase_admin
from firebase_admin import credentials, firestore

class SearchedCategoriesSpider(scrapy.Spider):
    name = "searchedCategoriesDataIndia100cities"
    # Pre-compile the regex for efficiency
    MOBX_REGEX = re.compile(r'window\.__MOBX_STATE__\s*=\s*({.*?});')
    
    # Manual list of cities (add or adjust as needed)
    # Each dictionary contains: name, id, and url.
    # For cities where no URL is provided or marked as "No searches found", we use that value.
    cities_manual = [
        {"name": "Srinagar", "id": "256", "url": f"https://wanderlog.com/searchedCategories/256"},
        {"name": "Jaipur", "id": "24", "url": f"https://wanderlog.com/searchedCategories/24"},
        {"name": "Varanasi", "id": "122", "url": f"https://wanderlog.com/searchedCategories/122"},
        {"name": "lakshadweep", "id": "88144", "url": f"https://wanderlog.com/searchedCategories/88144"},
        {"name": "Kodagu (Coorg)", "id": "87325", "url": f"https://wanderlog.com/searchedCategories/87325"},
        {"name": "Havelock Island", "id": "1483", "url": "https://wanderlog.com/searchedCategories/1483"},
        # For entries with only id provided, we generate the URL using typical structure.
        {"name": "Rishikesh", "id": "194", "url": f"https://wanderlog.com/searchedCategories/194"},
        {"name": "kodaikanal", "id": "842", "url": f"https://wanderlog.com/searchedCategories/842"},
        {"name": "Mcleodganj", "id": "146282", "url": f"https://wanderlog.com/searchedCategories/146282"},
        {"name": "Nanital", "id": "895", "url": f"https://wanderlog.com/searchedCategories/895"},
        {"name": "Shimla", "id": "428", "url": f"https://wanderlog.com/searchedCategories/428"},
        {"name": "New Delhi", "id": "13", "url": f"https://wanderlog.com/searchedCategories/13"},
        {"name": "Kolkata", "id": "69", "url": f"https://wanderlog.com/searchedCategories/69"},
        {"name": "Mussoorie", "id": "687", "url": f"https://wanderlog.com/searchedCategories/687"},
        {"name": "Pondicherry", "id": "334", "url": f"https://wanderlog.com/searchedCategories/334"},
        {"name": "Mumbai", "id": "25", "url": f"https://wanderlog.com/searchedCategories/25"},
        {"name": "Lonavala", "id": "701", "url": f"https://wanderlog.com/searchedCategories/701"},
        {"name": "Gokarna", "id": "2371", "url": f"https://wanderlog.com/searchedCategories/2371"},
        {"name": "Bir Billing (Bir)", "id": "1622", "url": f"https://wanderlog.com/searchedCategories/1622"},
        {"name": "Varkala", "id": "662", "url": f"https://wanderlog.com/searchedCategories/662"},
        {"name": "Dalhousie", "id": "1784", "url": f"https://wanderlog.com/searchedCategories/1784"},
        {"name": "Mount Abu", "id": "877", "url": f"https://wanderlog.com/searchedCategories/877"},
        {"name": "Wayanad", "id": "87128", "url": f"https://wanderlog.com/searchedCategories/87128"},
        {"name": "Pachmarhi", "id": "1215", "url": f"https://wanderlog.com/searchedCategories/1215"},
        {"name": "Bangalore (Bengaluru)", "id": "35", "url": f"https://wanderlog.com/searchedCategories/35"},
        {"name": "Jodhpur", "id": "143", "url": f"https://wanderlog.com/searchedCategories/143"},
        {"name": "Jaisalmer", "id": "183", "url": f"https://wanderlog.com/searchedCategories/183"},
        {"name": "Ujjain", "id": "1025", "url": f"https://wanderlog.com/searchedCategories/1025"},
        {"name": "Nubra Valley", "id": "88231", "url": f"https://wanderlog.com/searchedCategories/88231"},
        {"name": "Hyderabad", "id": "78", "url": f"https://wanderlog.com/searchedCategories/78"},
        {"name": "Khajuraho", "id": "850", "url": f"https://wanderlog.com/searchedCategories/850"},
        {"name": "Chennai (Madras)", "id": "40", "url": f"https://wanderlog.com/searchedCategories/40"},
        # For cities with missing id/URL, we mark them as "No searches found"
        {"name": "katra", "id": "2976", "url": f"https://wanderlog.com/searchedCategories/2976"},
        {"name": "Haridwar", "id": "783", "url": f"https://wanderlog.com/searchedCategories/783"},
        {"name": "Kanyakumari", "id": "926", "url": f"https://wanderlog.com/searchedCategories/926"},
        {"name": "Hampi", "id": "696", "url": f"https://wanderlog.com/searchedCategories/696"},
        {"name": "Kochi", "id": "157", "url": f"https://wanderlog.com/searchedCategories/157"},
        {"name": "Tirupati", "id": "914", "url": f"https://wanderlog.com/searchedCategories/914"},
        {"name": "Alibaug", "id": "1480", "url": f"https://wanderlog.com/searchedCategories/1480"},
        {"name": "Ahmedabad", "id": "161", "url": f"https://wanderlog.com/searchedCategories/161"},
        {"name": "Kanha National Park", "id": "7807", "url": f"https://wanderlog.com/searchedCategories/7807"},
        {"name": "Kasol (Kullu)", "id": "86862", "url": f"https://wanderlog.com/searchedCategories/86862"},
        {"name": "Mysore", "id": "280", "url": f"https://wanderlog.com/searchedCategories/280"},
        {"name": "Almora", "id": "1765", "url": f"https://wanderlog.com/searchedCategories/1765"},
        {"name": "Shirdi", "id": "1316", "url": f"https://wanderlog.com/searchedCategories/1316"},
        {"name": "Madurai", "id": "615", "url": f"https://wanderlog.com/searchedCategories/615"},
        {"name": "Bodh Gaya", "id": "1365", "url": f"https://wanderlog.com/searchedCategories/1365"},
        {"name": "Auli", "id": "", "url": f"https://wanderlog.com/searchedCategories/2477"},
        {"name": "Mahabaleshwar", "id": "978", "url": f"https://wanderlog.com/searchedCategories/978"},
        {"name": "Kaziranga National Park", "id": "", "url": f"https://wanderlog.com/searchedCategories/8175"},
        {"name": "Jim Corbett NationalPark", "id": "1635", "url": f"https://wanderlog.com/searchedCategories/1635"},
        {"name": "Nashik", "id": "449", "url": f"https://wanderlog.com/searchedCategories/449"},
        {"name": "Chandigarh", "id": "403", "url": f"https://wanderlog.com/searchedCategories/403"},
        {"name": "Mathura", "id": "1002", "url": f"https://wanderlog.com/searchedCategories/1002"},
        {"name": "Shimoga (Shivamogga)", "id": "1942", "url": f"https://wanderlog.com/searchedCategories/1942"},
        {"name": "Rameshwaram", "id": "1325", "url": f"https://wanderlog.com/searchedCategories/1325"},
        {"name": "Visakhapatnam", "id": "518", "url": f"https://wanderlog.com/searchedCategories/518"},
        {"name": "Pune", "id": "57", "url": f"https://wanderlog.com/searchedCategories/57"},
        {"name": "Vrindavan", "id": "1874", "url": f"https://wanderlog.com/searchedCategories/1874"},
        {"name": "Ranthambore National Park", "id": "3825", "url": f"https://wanderlog.com/searchedCategories/3825"},
        {"name": "Coimbatore", "id": "371", "url": f"https://wanderlog.com/searchedCategories/371"},
        {"name": "Lucknow", "id": "375", "url": f"https://wanderlog.com/searchedCategories/375"},
        {"name": "Dharamshala", "id": "405", "url": f"https://wanderlog.com/searchedCategories/405"},
        {"name": "Pahalgam", "id": "2253", "url": f"https://wanderlog.com/searchedCategories/2253"},
        {"name": "Gwailor", "id": "1291", "url": f"https://wanderlog.com/searchedCategories/1291"},
        {"name": "Khandala", "id": "146301", "url": f"https://wanderlog.com/searchedCategories/146301"},
        {"name": "Kovalam", "id": "1966", "url": f"https://wanderlog.com/searchedCategories/1966"},
        {"name": "Madikeri", "id": "1886", "url": f"https://wanderlog.com/searchedCategories/1886"},
        {"name": "Matheran", "id": "1667", "url": f"https://wanderlog.com/searchedCategories/1667"},
        {"name": "Kamlimpong", "id": "1407", "url": f"https://wanderlog.com/searchedCategories/1407"},
        {"name": "Thanjavur", "id": "738", "url": f"https://wanderlog.com/searchedCategories/738"},
        {"name": "Bhubaneswar", "id": "444", "url": f"https://wanderlog.com/searchedCategories/444"},
        {"name": "kasauli", "id": "2397", "url": f"https://wanderlog.com/searchedCategories/2397"},
        {"name": "Ajmer", "id": "956", "url": f"https://wanderlog.com/searchedCategories/956"},
        {"name": "Aurangabad", "id": "607", "url": f"https://wanderlog.com/searchedCategories/607"},
        {"name": "Jammu", "id": "981", "url": f"https://wanderlog.com/searchedCategories/981"}
    ]
    
    def __init__(self):
        # Initialize Firebase Admin SDK (only once)
        firebase_credentials_path = os.path.join(
            os.getcwd(),
            r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
        )
        cred = credentials.Certificate(firebase_credentials_path)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    def start_requests(self):
        """
        Instead of scraping the main page, we iterate through our manual list of cities.
        We skip any entry where the URL is missing or marked as "No searches found."
        """
        for city in self.cities_manual:
            url = city.get("url", "").strip()
            if not url or "No searches found" in url:
                self.logger.info(f"Skipping city {city.get('name')} due to missing URL or 'No searches found'.")
                continue
            yield SeleniumRequest(
                url=url,
                callback=self.parse_searched_category,
                meta={"city_manual_data": city},  # Pass the manual data downstream
                wait_time=3,
                script='window.scrollTo(0, document.body.scrollHeight);'
            )

    def parse_searched_category(self, response):
        """
        Extracts category links for the city.
        """
        city_manual_data = response.meta.get("city_manual_data", {})
        search_categories = response.css(
            'div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 a.color-gray-900::attr(href)'
        ).getall()
        
        if search_categories:
            for category_link in search_categories:
                category_url = response.urljoin(category_link)
                yield SeleniumRequest(
                    url=category_url,
                    callback=self.parse_places_list,
                    meta={"city_manual_data": city_manual_data}
                )
        else:
            self.logger.warning(f"No categories found for city: {response.url}")

    def parse_places_list(self, response):
        """
        Parses the places list page and stores data in Firestore.
        """
        city_manual_data = response.meta.get("city_manual_data", {})
        script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
        if not script_text:
            self.logger.warning("No script text found.")
            return

        mobx_data = self.MOBX_REGEX.search(script_text)
        if not mobx_data:
            self.logger.warning("No mobx state found in script text.")
            return

        try:
            mobx_json = json.loads(mobx_data.group(1))
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error: {e}")
            return

        places_list_page = mobx_json.get('placesListPage', {})
        data = places_list_page.get('data', {})
        geo_data = data.get('geo', {})
        place_metadata = data.get('placeMetadata', [])

        if not (geo_data and place_metadata):
            self.logger.warning("Missing geo_data or place_metadata in places_list_page data.")
            return

        # Use the manual data if provided; fallback to geo_data from the page.
        city_id = str(city_manual_data.get("id", geo_data.get('id')))
        city_name = city_manual_data.get("name", geo_data.get('name'))
        category_title = self.clean_category_title(data.get('title', 'Unknown Category'))

        for index, place in enumerate(place_metadata):
            place_data = self.extract_place_data(place, index)
            self.store_in_firestore(city_id, city_name, category_title, place_data)

    def extract_place_data(self, place, index):
        """Extracts and returns the relevant details for a place."""
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
            'sources': [],
            'reviews': []
        }

    def store_in_firestore(self, city_id, city_name, category_title, place_data):
        """
        Stores the place data in Firestore under the document for the city
        and within a subcollection for the category.
        """
        city_doc_ref = self.db.collection('searchedCategories2').document(city_id)
        city_doc_ref.set({'city_name': city_name}, merge=True)

        place_id = place_data.get('placeId')
        if place_id:
            doc_ref = city_doc_ref.collection(category_title).document(place_id)
            doc_ref.set(place_data)
            self.logger.info(f"Stored place data for {place_id} in city: {city_name}, category: {category_title}")
        else:
            self.logger.warning(f"Place missing 'id', data: {place_data}")

    def clean_category_title(self, title):
        """Cleans the category title for use as a Firestore collection name."""
        return re.sub(r'[\/\\.\[\]#$]', '-', title)
