# spider name: wanderlog_manual_explore_details.py

import scrapy
import re
import json

class WanderlogManualExploreDetailsSpider(scrapy.Spider):
    name = "wanderlog_manual_explore_details"
    custom_settings = {
        # optional: be polite to the server
        'DOWNLOAD_DELAY': 1,
        'USER_AGENT': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/114.0.0.0 Safari/537.36'
        ),
    }

    def start_requests(self):
        # ← your manual list of IDs
        manual_ids = [
            #Municipilaties
            "85939", "9630", "9636", "82574", "15", "9618", "2", "9637", "58150", "9643", "11", "9635", "9649", "81905", "9707", "131100", "3", "131086", "18", "131118", "7865", "82576", "9745", "58186", "9798", "58183", "22", "78744", "24", "9656", "82578", "58170", "53", "9711", "10413", "58175", "9683", "9666", "9670", "9735", "80", "9730", "81904", "81180", "9654", "9679", "131150", "131090", "81909", "21", "19", "131083", "9767", "131078", "131132", "37", "131115", "79", "131134", "28", "131094", "9712", "9792", "82593", "9826", "9800", "9751", "9663", "9763", "82585", "9802", "9696", "23", "10219", "29", "9803", "9668", "85940", "10061", "38", "58187", "9801", "131079", "184", "79309", "9850", "131318", "20", "58049", "81226", "9724", "58182", "10008", "82580", "9690", "9889", "82581", "9723", "9836", "9677", "58191", "51", "32", "9765", "42", "59", "131093", "9840", "9689", "81194", "10046", "9687", "131117", "9725", "9691", "9930", "9694", "9891", "9935", "9998", "79306", "11769", "58198", "9709", "9676", "9838", "9945", "9756", "30", "9799", "41", "10074", "176", "58062", "9903", "68", "131478", "78752", "9713", "34", "79303", "9919", "131360", "82594", "31", "10064", "10235", "10211", "118", "9937", "9861", "9917", "9727", "9734", "98", "7865", "24", "68", "412", "297", "313", "428", "842", "405", "304", "753", "701", "329", "783", "1868", "787", "814", "2253", "956", "461", "541", "2761", "2397", "3684", "1673",



            # Islands
            # "9612", "58149", "9632", "9628", "9619", "9627", "12",   "9648",
            # "58165", "9671", "81177", "58044", "58151", "9661", "9639",
            # "58155", "5",     "9678", "9697", "58167", "9644", "9749",  "9686",
            # "650",  "9718", "9699", "9685", "146",  "81908","131172","81190",
            # "81187","9732","131174","9664","9844","81186","9788","9786",
            # "101","9755","9828","9906","473","9874","36","211",
            # "9933","10187","10131","293","10163","150","189","96",
            # "10473","10900","202","169","360","560","1483","3065",
            # "87925","3427","5756","7708","6514","7076","8812","5816",
            # "6790","8096"
        ]

        for mid in manual_ids:
            url = f"https://wanderlog.com/explore/{mid}"
            self.logger.info(f"→ Requesting explore page for ID {mid}")
            yield scrapy.Request(
                url,
                callback=self.parse_details,
                meta={'manual_id': mid}
            )

    def parse_details(self, response):
        mid = response.meta['manual_id']
        script = response.xpath(
            '//script[contains(text(),"window.__MOBX_STATE__")]/text()'
        ).get()
        if not script:
            self.logger.error(f"[{mid}] No MobX state script found.")
            return

        m = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script)
        if not m:
            self.logger.error(f"[{mid}] Failed to extract JSON blob.")
            return

        mobx = json.loads(m.group(1))
        data = mobx.get('explorePage', {}).get('data', {})
        geo  = data.get('geo', {})
        secs = data.get('sections', [])

        # find the attractions link
        geo_cat_url = None
        for sec in secs:
            if sec.get("listId", {}).get("type") == "geoCategory":
                geo_cat_url = sec.get("linkInfo", {}).get("geoCategoryUrl")
                break

        yield {
            "manual_id":     mid,
            "place_id":      str(geo.get("id")),
            "city_name":     geo.get("name"),
            "geoCategoryUrl": geo_cat_url
        }

# # spider name: wanderlog_explore_attractions_full_urls.py
# # Working for explore places
# import scrapy
# import re
# import json
# import time
# from scrapy.http import HtmlResponses
# from selenium.webdriver.common.by import By

# # Selenium imports for manual control
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager

# class WanderlogExploreAttractionsFullUrlsSpider(scrapy.Spider):
#     name = "wanderlog_explore_attractions_full_urls"
#     start_urls = ["https://wanderlog.com/explore"]

#     def __init__(self, *args, **kwargs):
#         """
#         1. SETUP: Initialize Selenium driver once when the spider starts.
#         """
#         super().__init__(*args, **kwargs)
#         chrome_opts = Options()
#         chrome_opts.add_argument("--headless")  # Runs Chrome without a visible window
#         driver_service = Service(ChromeDriverManager().install())
#         self.driver = webdriver.Chrome(service=driver_service, options=chrome_opts)
#         self.logger.info("Selenium driver initialized.")

#     def parse(self, response):
#         """
#         2. PARSE INDEX: Get all city links from the main explore page.
#         """
#         self.logger.info(f"Fetching city links from {response.url}")
#         self.driver.get(response.url)
#         time.sleep(5) # Wait for initial page load

#         # Scroll down to load all cities
#         self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#         time.sleep(3)

#         # Find all the city link elements
#         city_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.col-12.col-sm-6 a.color-gray-900')
        
#         # Store URLs and names to avoid issues while iterating
#         city_links = [(elem.get_attribute('href'), elem.text) for elem in city_elements]
#         self.logger.info(f"Found {len(city_links)} city links to process.")

#         for url, name in city_links:
#             # For each city, yield a request to its detail page
#             yield scrapy.Request(
#                 url=url,
#                 callback=self.parse_place_details,
#                 meta={'city_name': name} # Pass the city name along
#             )
            
#     def parse_place_details(self, response):
#         """
#         3. PARSE DETAILS: For each city page, extract the required three fields.
#         """
#         self.logger.info(f"Parsing details for: {response.meta.get('city_name')}")
        
#         # The response body already contains the dynamic content from Scrapy's request
#         script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()

#         if not script_text:
#             self.logger.error(f"MobX state script not found on {response.url}")
#             return

#         mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
#         if not mobx_data:
#             self.logger.error(f"Could not parse MobX JSON on {response.url}")
#             return

#         mobx_json = json.loads(mobx_data.group(1))
#         explore_data = mobx_json.get('explorePage', {}).get('data', {})
#         geo_data = explore_data.get('geo', {})
#         sections = explore_data.get('sections', [])

#         # Find the specific "geoCategoryUrl" for attractions
#         geo_category_url = None
#         for section in sections:
#             if section.get("listId", {}).get("type") == "geoCategory":
#                 geo_category_url = section.get("linkInfo", {}).get("geoCategoryUrl")
#                 break
        
#         # Yield the final, clean data item
#         yield {
#             'place_id': str(geo_data.get('id')),
#             'city_name': geo_data.get('name'),
#             'geoCategoryUrl': geo_category_url
#         }

#     def close(self, reason):
#         """
#         4. CLEANUP: Close the browser when the spider finishes.
#         """
#         self.logger.info("Spider finished. Closing Selenium driver.")
#         self.driver.quit()























# import scrapy
# import re
# import json
# import time
# from scrapy.http import HtmlResponse

# # Selenium imports
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager


# class DublinGeoCategorySpider(scrapy.Spider):
#     name = "dublin_geo_category"
#     # 1. Use start_urls to begin
#     start_urls = ["https://wanderlog.com/explore/9633/dublin"]

#     # No custom_settings needed here anymore

#     def __init__(self, *args, **kwargs):
#         # 2. Set up the Selenium driver configuration
#         super().__init__(*args, **kwargs)
#         chrome_opts = Options()
#         chrome_opts.add_argument("--headless")
#         self.driver_service = Service(ChromeDriverManager().install())
#         self.chrome_options = chrome_opts

#     def parse(self, response):
#         # 3. Launch Selenium, get the page, and execute the scroll script
#         self.logger.info(f"Starting Selenium driver for URL: {response.url}")
#         driver = webdriver.Chrome(service=self.driver_service, options=self.chrome_options)
#         driver.get(response.url)
        
#         # Wait for the page to load
#         time.sleep(10)
        
#         # Execute the scroll script
#         driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
#         # Wait a moment for any new content to load after scrolling
#         time.sleep(2)

#         html = driver.page_source
#         driver.quit()

#         # 4. Create a new response with the dynamic content
#         selenium_response = HtmlResponse(url=response.url, body=html, encoding="utf-8")
        
#         # 5. Hand off to the original parsing method
#         yield from self.parse_details(selenium_response)

#     def parse_details(self, response):
#         # This method remains exactly the same as before
#         raw = response.xpath(
#             '//script[contains(text(),"window.__MOBX_STATE__")]/text()'
#         ).get()
#         if not raw:
#             self.logger.error("MobX state script not found")
#             return

#         m = re.search(r"window\.__MOBX_STATE__\s*=\s*({.*?});", raw)
#         if not m:
#             self.logger.error("Failed to regex MobX JSON")
#             return

#         data = json.loads(m.group(1))
#         explore  = data.get("explorePage", {}).get("data", {})
#         geo      = explore.get("geo", {})
#         sections = explore.get("sections", [])

#         place_id  = str(geo.get("id", ""))
#         city_name = geo.get("name", "")

#         geo_url = None
#         for sec in sections:
#             if sec.get("listId", {}).get("type") == "geoCategory":
#                 geo_url = sec.get("linkInfo", {}).get("geoCategoryUrl")
#                 break

#         yield {
#             "place_id":       place_id,
#             "city_name":      city_name,
#             "geoCategoryUrl": geo_url
#         }


# import scrapy
# import re
# import json
# from scrapy_selenium import SeleniumRequest

# class DublinGeoCategorySpider(scrapy.Spider):
#     name = "dublin_geo_category"

#     custom_settings = {
#         # 1. Enable the Scrapy-Selenium downloader middleware.
#         "DOWNLOADER_MIDDLEWARES": {
#             "scrapy_selenium.SeleniumMiddleware": 800,
#         },
#         # 2. Configure Selenium to use Chrome in headless mode.
#         # By not setting an executable_path, Selenium will automatically
#         # download and use the correct driver version.
#         "SELENIUM_DRIVER_NAME": "chrome",
#         "SELENIUM_DRIVER_ARGUMENTS": ["--headless"],
#         "LOG_LEVEL": "INFO",
#     }

#     def start_requests(self):
#         # This part is already correct. It yields a SeleniumRequest
#         # which will be processed by the middleware.
#         yield SeleniumRequest(
#             url="https://wanderlog.com/explore/9633/dublin",
#             callback=self.parse_details,
#             wait_time=10,  # give React plenty of time to hydrate
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse_details(self, response):
#         # This parsing logic is also correct.
        
#         # 1) Grab the MobX JSON blob
#         raw = response.xpath(
#             '//script[contains(text(),"window.__MOBX_STATE__")]/text()'
#         ).get()
#         if not raw:
#             self.logger.error("MobX state script not found")
#             return

#         m = re.search(r"window\.__MOBX_STATE__\s*=\s*({.*?});", raw)
#         if not m:
#             self.logger.error("Failed to regex MobX JSON")
#             return

#         data = json.loads(m.group(1))
#         explore  = data.get("explorePage", {}).get("data", {})
#         geo      = explore.get("geo", {})
#         sections = explore.get("sections", [])

#         # 2) Extract place_id and city_name
#         place_id  = str(geo.get("id", ""))
#         city_name = geo.get("name", "")

#         # 3) Find the geoCategoryUrl in the sections array
#         geo_url = None
#         for sec in sections:
#             if sec.get("listId", {}).get("type") == "geoCategory":
#                 geo_url = sec.get("linkInfo", {}).get("geoCategoryUrl")
#                 break

#         # 4) Yield your three-field JSON
#         yield {
#             "place_id":       place_id,
#             "city_name":      city_name,
#             "geoCategoryUrl": geo_url
#         }









#Working
# # quotes_js_scraper/spiders/wanderlog_attractions_full.py

# import scrapy
# import re
# import json
# import time
# from scrapy.http import HtmlResponse

# # Selenium imports
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager


# class OrlandoGeoSpider(scrapy.Spider):
#     name = "orlando_geo"
#     # 1. Let Scrapy start with a standard request to this URL
#     start_urls = ["https://wanderlog.com/explore/58152/orlando"]

#     custom_settings = {
#         "FEEDS": {
#             "orlando_geo.json": {
#                 "format": "json",
#                 "encoding": "utf8",
#                 "indent": 4,
#                 "overwrite": True,
#             }
#         },
#         "LOG_LEVEL": "INFO",
#     }

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         chrome_opts = Options()
#         chrome_opts.add_argument("--headless")
#         chrome_opts.add_argument("--no-sandbox")
#         chrome_opts.add_argument("--disable-gpu")
#         self.driver_service = Service(ChromeDriverManager().install())
#         self.chrome_options = chrome_opts

#     # 2. The start_urls request will be handled by this parse method
#     def parse(self, response):
#         """
#         This method launches the Selenium driver to get the JS-rendered page source.
#         """
#         self.logger.info(f"Starting Selenium driver for URL: {response.url}")

#         # All your Selenium logic now lives here
#         driver = webdriver.Chrome(service=self.driver_service, options=self.chrome_options)
#         driver.get(response.url)
#         time.sleep(5)  # Allow time for the page to load
#         html = driver.page_source
#         driver.quit()

#         self.logger.info("Selenium driver finished. Parsing response.")

#         # Create a new Scrapy Response object with the JS-rendered HTML
#         selenium_response = HtmlResponse(url=response.url, body=html, encoding="utf-8")

#         # 3. Yield the results from your original parsing method
#         #    This is allowed inside a 'parse' method.
#         yield from self.parse_place_details(selenium_response)

#     def parse_place_details(self, response):
#         """
#         This method extracts the data from the MobX script. It remains unchanged.
#         """
#         script = response.xpath(
#             '//script[contains(text(),"window.__MOBX_STATE__")]/text()'
#         ).get()
#         if not script:
#             self.logger.error("Could not find MobX bootstrap script.")
#             return

#         m = re.search(r"window\.__MOBX_STATE__\s*=\s*({.*?});", script)
#         if not m:
#             self.logger.error("Regex didn’t match the JSON blob.")
#             return

#         data = json.loads(m.group(1))
#         geo = data.get("explorePage", {}).get("data", {}).get("geo", {})
#         if not geo:
#             self.logger.error("Geo block not present in MobX data.")
#             return

#         # This final 'yield' is now valid because it originates from the parse callback
#         yield {
#             "place_id": str(geo.get("id")),
#             "city_name": geo.get("name"),
#             "latitude": geo.get("latitude"),
#             "longitude": geo.get("longitude"),
#         }

#     def close(self, reason):
#         # A good practice to ensure the driver is closed if the spider stops
#         # This is more of a safeguard, as it's already quit in the parse method
#         self.logger.info("Spider closed.")


