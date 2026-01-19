import scrapy
import json



class AllTrailsSpider(scrapy.Spider):
    name = 'alltrails_spider'

    def start_requests(self):
        # The base URL with ScrapeOps proxy
        base_url = "https://proxy.scrapeops.io/v1/?api_key=0dd51678-5065-4d91-87a4-51013e50a107&url={}&render_js=true"

        # Loop through pages 1 to 26 for trails starting with 'B'
        for page_num in range(1, 2):
            target_url = f'https://www.alltrails.com/directory/parks/Z/{page_num}'
            scrapeops_url = base_url.format(target_url)
            
            # Sending request through ScrapeOps Proxy for each page
            yield scrapy.Request(url=scrapeops_url, callback=self.parse)

    def parse(self, response):
        # Extract trail names and their URLs
        trails = response.css('li a.xlate-none')
        trail_data = []
        
        # Creating a list of trail names and URLs
        for trail in trails:
            trail_name = trail.attrib.get('title')
            trail_url = response.urljoin(trail.attrib.get('href'))
            trail_data.append({'trail_name': trail_name, 'trail_url': trail_url})

        # Load existing data if the file already exists
        output_file = 'national_park_names_urls_Z.json'
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_trails = json.load(f)
        except FileNotFoundError:
            existing_trails = []

        # Append new trail data to the existing list
        existing_trails.extend(trail_data)

        # Save all trail names and URLs back to the JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(existing_trails, f, ensure_ascii=False, indent=4)

        self.log(f"Saved {len(trail_data)} new trails to {output_file}")


# import scrapy
# import json

# class AllTrailsSpider(scrapy.Spider):
#     name = 'alltrails_spider'

#     def start_requests(self):
#         # The base URL with ScrapeOps proxy
#         base_url = "https://proxy.scrapeops.io/v1/?api_key=39aec2e2-9f7b-4f01-b71f-e025cbfea6cc&url={}&render_js=true"

#         # Loop through pages 1 to 26 for trails starting with 'A'
#         for page_num in range(1, 26):
#             target_url = f'https://www.alltrails.com/directory/trails/A/{page_num}'
#             scrapeops_url = base_url.format(target_url)
            
#             # Sending request through ScrapeOps Proxy for each page
#             yield scrapy.Request(url=scrapeops_url, callback=self.parse)

#     def parse(self, response):
#         # Extract trail names from the page using the title attribute in <a> tags
#         trails = response.css('li a.xlate-none::attr(title)').getall()

#         # Creating a list of trail names
#         trail_names = []
#         for trail in trails:
#             trail_names.append({'trail_name': trail})

#         # Load existing data if the file already exists
#         output_file = 'trail_names2.json'
#         try:
#             with open(output_file, 'r', encoding='utf-8') as f:
#                 existing_trails = json.load(f)
#         except FileNotFoundError:
#             existing_trails = []

#         # Append new trail names to the existing list
#         existing_trails.extend(trail_names)

#         # Save all trail names back to the JSON file
#         with open(output_file, 'w', encoding='utf-8') as f:
#             json.dump(existing_trails, f, ensure_ascii=False, indent=4)

#         self.log(f"Saved {len(trail_names)} new trails to {output_file}")






# import scrapy
# import json

# class AllTrailsSpider(scrapy.Spider):
#     name = 'alltrails_spider'

#     def start_requests(self):
#         # The base URL with ScrapeOps proxy
#         base_url = "https://proxy.scrapeops.io/v1/?api_key=39aec2e2-9f7b-4f01-b71f-e025cbfea6cc&url={}&render_js=true"

#         # The URL to scrape (page 2 of trails starting with 'A')
#         target_url = 'https://www.alltrails.com/directory/trails/A/2'

#         # Construct the full URL by passing target URL through ScrapeOps proxy
#         scrapeops_url = base_url.format(target_url)
        
#         # Sending request through ScrapeOps Proxy
#         yield scrapy.Request(url=scrapeops_url, callback=self.parse)

#     def parse(self, response):
#         # Extract trail names from the page using the title attribute in <a> tags
#         trails = response.css('li a.xlate-none::attr(title)').getall()

#         # Creating a list of trail names
#         trail_names = []
#         for trail in trails:
#             trail_names.append({'trail_name': trail})

#         # Save the trail names to a JSON file
#         output_file = 'trail_names.json'
#         with open(output_file, 'w', encoding='utf-8') as f:
#             json.dump(trail_names, f, ensure_ascii=False, indent=4)

#         self.log(f"Saved {len(trail_names)} trails to {output_file}")



# import scrapy
# import json
# import os

# class AllTrailsSpider(scrapy.Spider):
#     name = 'alltrails_spider'

#     def __init__(self, *args, **kwargs):
#         super(AllTrailsSpider, self).__init__(*args, **kwargs)
#         self.trail_data_list = []
#         self.output_file = os.path.join(os.getcwd(), 'alltrails_trails_data.json')

#     def start_requests(self):
#         base_url = 'https://www.alltrails.com/directory/trails/A/{}'
#         total_pages = 3  # Limit to 2 pages for testing
#         for page_num in range(2, total_pages + 1):
#             url = base_url.format(page_num)
#             yield scrapy.Request(
#                 url=url,
#                 callback=self.parse_directory,
#                 meta={
#                     # ScrapeOps parameters
#                     'sops_render_js': True,  # Enable JavaScript rendering
#                     # 'sops_country': 'us',    # Optional: specify country
#                 }
#             )

#     def parse_directory(self, response):
#         if "captcha" in response.text.lower():
#             self.logger.warning("CAPTCHA detected on directory page.")
#             return

#         # Extract trail links from the directory page
#         trail_links = response.css('li a.xlate-none::attr(href)').getall()
#         for link in trail_links:
#             trail_url = response.urljoin(link)
#             yield scrapy.Request(
#                 url=trail_url,
#                 callback=self.parse_trail,
#                 meta={
#                     # ScrapeOps parameters
#                     'sops_render_js': True,
#                     # 'sops_country': 'us',
#                 }
#             )

#     def parse_trail(self, response):
#         if "captcha" in response.text.lower():
#             self.logger.warning("CAPTCHA detected on trail page.")
#             return

#         # Extract the JSON data from the script tag with id '__NEXT_DATA__'
#         script_text = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
#         if script_text:
#             try:
#                 data = json.loads(script_text)
#                 trail_details = data.get('props', {}).get('pageProps', {}).get('trailDetails', {})
#                 if trail_details:
#                     trail_data = self.extract_trail_data(trail_details)
#                     self.trail_data_list.append(trail_data)
#                     self.logger.info(f"Extracted trail data for trailId: {trail_data.get('id')}")
#                 else:
#                     self.logger.warning("Trail details not found in the JSON data.")
#             except json.JSONDecodeError as e:
#                 self.logger.error(f"JSON decoding error: {e}")
#         else:
#             self.logger.warning("No JSON script tag found on the trail page.")

#     def extract_trail_data(self, trail_details):
#         """Extract relevant trail details"""
#         return {
#             'id': str(trail_details.get('id')),
#             'name': trail_details.get('name'),
#             'overview': trail_details.get('overview'),
#             'routeType': trail_details.get('routeType', {}).get('name'),
#             'popularity': trail_details.get('popularity'),
#             'location': trail_details.get('location', {}),
#             # Add other fields as needed
#         }

#     def close(self, reason):
#         """Writes all collected data to a JSON file when the spider finishes."""
#         with open(self.output_file, 'w', encoding='utf-8') as f:
#             json.dump(self.trail_data_list, f, ensure_ascii=False, indent=4)
#         self.logger.info(f"Saved {len(self.trail_data_list)} trails to {self.output_file}")



# import scrapy
# from scrapy_selenium import SeleniumRequest
# import json
# import os


# class AllTrailsSpider(scrapy.Spider):
#     name = 'alltrails_spider'

#     def __init__(self):
#         self.trail_data_list = []  # List to store all the trails data
#         self.output_file = os.path.join(os.getcwd(), 'alltrails_trails_data.json')  # Output file path

#     def start_requests(self):
#         # For testing, limit to pages 1 and 2 for trails starting with 'A'
#         base_url = 'https://www.alltrails.com/directory/trails/A/{}'
#         total_pages = 3  # Limit to 2 pages
#         for page_num in range(2, total_pages + 1):
#             url = base_url.format(page_num)
#             yield SeleniumRequest(
#                 url=url,
#                 callback=self.parse_directory,
#                 wait_time=3,
#                 script='window.scrollTo(0, document.body.scrollHeight);'
#             )

#     def parse_directory(self, response):
#         # Extract trail links from the directory page
#         trail_links = response.css('li a.xlate-none::attr(href)').getall()
#         for link in trail_links:
#             trail_url = response.urljoin(link)
#             yield SeleniumRequest(
#                 url=trail_url,
#                 callback=self.parse_trail,
#                 wait_time=3,
#                 script='window.scrollTo(0, document.body.scrollHeight);'
#             )

#     def parse_trail(self, response):
#         # Extract the JSON data from the script tag with id '__NEXT_DATA__'
#         script_text = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
#         if script_text:
#             try:
#                 data = json.loads(script_text)
#                 trail_details = data.get('props', {}).get('pageProps', {}).get('trailDetails', {})
#                 if trail_details:
#                     # Extract trail data and append to the list
#                     trail_data = self.extract_trail_data(trail_details)
#                     self.trail_data_list.append(trail_data)
#                     self.logger.info(f"Extracted trail data for trailId: {trail_data.get('id')}")
#                 else:
#                     self.logger.warning("Trail details not found in the JSON data.")
#             except json.JSONDecodeError as e:
#                 self.logger.error(f"JSON decoding error: {e}")
#         else:
#             self.logger.warning("No JSON script tag found on the trail page.")

#     def extract_trail_data(self, trail_details):
#         """Extract relevant trail details"""
#         return {
#             'id': str(trail_details.get('id')),
#             'name': trail_details.get('name'),
#             'overview': trail_details.get('overview'),
#             'routeType': trail_details.get('routeType', {}).get('name'),
#             'popularity': trail_details.get('popularity'),
#             'location': trail_details.get('location', {}),
#             'attributes': trail_details.get('attributes', {}),
#             'defaultActivityStats': trail_details.get('defaultActivityStats', {}),
#             'groupedTags': trail_details.get('groupedTags', []),
#             'trailGeoStats': trail_details.get('trailGeoStats', {}),
#             'defaultPhoto': trail_details.get('defaultPhoto', {}),
#             'avgRating': trail_details.get('avgRating'),
#             'ratingsBreakdown': trail_details.get('ratingsBreakdown', {}),
#             'source': trail_details.get('source'),
#             'trailCounts': trail_details.get('trailCounts', {}),
#             'description': trail_details.get('description'),
#             'description_html': trail_details.get('description_html'),
#             'metadata': trail_details.get('metadata', {}),
#             'alerts': trail_details.get('alerts', []),
#         }

#     def close(self, reason):
#         """Writes all collected data to a JSON file when the spider finishes."""
#         with open(self.output_file, 'w', encoding='utf-8') as f:
#             json.dump(self.trail_data_list, f, ensure_ascii=False, indent=4)
#         self.logger.info(f"Saved {len(self.trail_data_list)} trails to {self.output_file}")






# import scrapy
# from scrapy_selenium import SeleniumRequest
# import json
# import re
# import firebase_admin
# from firebase_admin import credentials, firestore
# import os


# class AllTrailsSpider(scrapy.Spider):
#     name = 'alltrails_spider'

#     def __init__(self):
#         # Initialize Firebase Admin SDK
#         firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         self.db = firestore.client()

#     def start_requests(self):
#         # There are 26 pages for trails starting with 'A'
#         base_url = 'https://www.alltrails.com/directory/trails/A/{}'
#         total_pages = 26
#         for page_num in range(1, total_pages + 1):
#             url = base_url.format(page_num)
#             yield SeleniumRequest(
#                 url=url,
#                 callback=self.parse_directory,
#                 wait_time=3,
#                 script='window.scrollTo(0, document.body.scrollHeight);'
#             )

#     def parse_directory(self, response):
#         # Extract trail links from the directory page
#         trail_links = response.css('li a.xlate-none::attr(href)').getall()
#         for link in trail_links:
#             trail_url = response.urljoin(link)
#             yield SeleniumRequest(
#                 url=trail_url,
#                 callback=self.parse_trail,
#                 wait_time=3,
#                 script='window.scrollTo(0, document.body.scrollHeight);'
#             )

#     def parse_trail(self, response):
#         # Extract the JSON data from the script tag with id '__NEXT_DATA__'
#         script_text = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
#         if script_text:
#             try:
#                 data = json.loads(script_text)
#                 trail_details = data.get('props', {}).get('pageProps', {}).get('trailDetails', {})
#                 if trail_details:
#                     # Use 'id' from 'trailDetails' as 'trailId'
#                     trail_id = str(trail_details.get('id'))
#                     # Extract trail data
#                     trail_data = self.extract_trail_data(trail_details)
#                     # Store in Firestore
#                     self.store_in_firestore(trail_id, trail_data)
#                     self.logger.info(f"Stored trail data for trailId: {trail_id}")
#                 else:
#                     self.logger.warning("Trail details not found in the JSON data.")
#             except json.JSONDecodeError as e:
#                 self.logger.error(f"JSON decoding error: {e}")
#         else:
#             self.logger.warning("No JSON script tag found on the trail page.")

#     def extract_trail_data(self, trail_details):
#         """Extract relevant trail details to store in Firestore"""
#         # You can adjust the fields as needed
#         return {
#             'id': str(trail_details.get('id')),
#             'name': trail_details.get('name'),
#             'overview': trail_details.get('overview'),
#             'routeType': trail_details.get('routeType', {}).get('name'),
#             'popularity': trail_details.get('popularity'),
#             'location': trail_details.get('location', {}),
#             'attributes': trail_details.get('attributes', {}),
#             'defaultActivityStats': trail_details.get('defaultActivityStats', {}),
#             'groupedTags': trail_details.get('groupedTags', []),
#             'trailGeoStats': trail_details.get('trailGeoStats', {}),
#             'defaultPhoto': trail_details.get('defaultPhoto', {}),
#             'avgRating': trail_details.get('avgRating'),
#             'ratingsBreakdown': trail_details.get('ratingsBreakdown', {}),
#             'source': trail_details.get('source'),
#             'trailCounts': trail_details.get('trailCounts', {}),
#             'description': trail_details.get('description'),
#             'description_html': trail_details.get('description_html'),
#             'metadata': trail_details.get('metadata', {}),
#             'alerts': trail_details.get('alerts', []),
#             # Include any other fields you need
#         }

#     def store_in_firestore(self, trail_id, trail_data):
#         """Stores trail data into Firestore under the 'trails' collection"""
#         # Reference to the 'trails' collection and document with 'trail_id'
#         doc_ref = self.db.collection('trails').document(trail_id)
#         doc_ref.set(trail_data)





# import scrapy
# import json
# import re
# import firebase_admin
# from firebase_admin import credentials, firestore
# import os


# class AllTrailsSpider(scrapy.Spider):
#     name = 'alltrails_spider'

#     def __init__(self):
#         # Initialize Firebase Admin SDK
#         firebase_credentials_path = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json")
#         cred = credentials.Certificate(firebase_credentials_path)
#         firebase_admin.initialize_app(cred)
#         self.db = firestore.client()

#     def start_requests(self):
#         # There are 26 pages for trails starting with 'A'
#         base_url = 'https://www.alltrails.com/directory/trails/A/{}'
#         total_pages = 26
#         for page_num in range(1, total_pages + 1):
#             url = base_url.format(page_num)
#             yield scrapy.Request(url=url, callback=self.parse_directory)

#     def parse_directory(self, response):
#         # Extract trail links from the directory page
#         trail_links = response.css('li a.xlate-none::attr(href)').getall()
#         for link in trail_links:
#             trail_url = response.urljoin(link)
#             yield scrapy.Request(url=trail_url, callback=self.parse_trail)

#     def parse_trail(self, response):
#         # Extract the JSON data from the script tag with id '__NEXT_DATA__'
#         script_text = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
#         if script_text:
#             try:
#                 data = json.loads(script_text)
#                 trail_details = data.get('props', {}).get('pageProps', {}).get('trailDetails', {})
#                 if trail_details:
#                     # Use 'id' from 'trailDetails' as 'trailId'
#                     trail_id = str(trail_details.get('id'))
#                     # Clean up the data if necessary
#                     trail_data = self.clean_trail_data(trail_details)
#                     # Store in Firestore
#                     self.store_in_firestore(trail_id, trail_data)
#                     self.logger.info(f"Stored trail data for trailId: {trail_id}")
#                 else:
#                     self.logger.warning("Trail details not found in the JSON data.")
#             except json.JSONDecodeError as e:
#                 self.logger.error(f"JSON decoding error: {e}")
#         else:
#             self.logger.warning("No JSON script tag found on the trail page.")

#     def clean_trail_data(self, trail_details):
#         # Here you can process or clean up the trail_details if needed
#         # For now, we'll just return it as is
#         return trail_details

#     def store_in_firestore(self, trail_id, trail_data):
#         # Reference to the 'trails' collection and document with 'trail_id'
#         doc_ref = self.db.collection('trails').document(trail_id)
#         doc_ref.set(trail_data)

