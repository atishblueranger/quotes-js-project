# import scrapy
# from scrapy_selenium import SeleniumRequest

# class WanderlogListUrlsSpider(scrapy.Spider):
#     name = "wanderlog_list_urls"
#     custom_settings = {
#         "DOWNLOAD_DELAY": 1,
#     }

#     def start_requests(self):
#         yield SeleniumRequest(
#             url="https://wanderlog.com/explore",
#             callback=self.parse_explore,
#             wait_time=3,
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse_explore(self, response):
#         cards = response.css("div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3")
#         for card in cards:
#             rel_link = card.css("a.color-gray-900::attr(href)").get()
#             city_name = card.css("a.color-gray-900::text").get()
#             if not rel_link:
#                 continue

#             # extract place_id from URL: /explore/<place_id>/<slug>
#             parts = rel_link.strip("/").split("/")
#             place_id = parts[1] if len(parts) >= 2 and parts[0] == "explore" else None

#             detail_url = response.urljoin(rel_link)
#             yield SeleniumRequest(
#                 url=detail_url,
#                 callback=self.parse_detail,
#                 meta={
#                     "city_name": city_name.strip() if city_name else None,
#                     "place_id":  place_id
#                 },
#                 wait_time=2
#             )

#     def parse_detail(self, response):
#         city     = response.meta.get("city_name")
#         place_id = response.meta.get("place_id")
#         list_rel = response.css('a[href*="/list/geoCategory/"]::attr(href)').get()

#         if list_rel and place_id:
#             yield {
#                 "city_name": city,
#                 "place_id":  place_id,
#                 "list_url":  response.urljoin(list_rel)
#             }
#         else:
#             self.logger.warning(
#                 f"Missing data for {city or place_id} at {response.url}"

# import scrapy
# from scrapy_selenium import SeleniumRequest

# class WanderlogListUrlsSpider(scrapy.Spider):
#     name = "wanderlog_list_urls"
#     custom_settings = {
#         "DOWNLOAD_DELAY": 1,
#     }

#     def __init__(self, limit=None, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         # if you pass -a limit=5, self.limit becomes 5 (int)
#         self.limit    = int(limit) if limit is not None else None
#         self.processed = 0

#     def start_requests(self):
#         yield SeleniumRequest(
#             url="https://wanderlog.com/explore",
#             callback=self.parse_explore,
#             wait_time=3,
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse_explore(self, response):
#         cards = response.css("div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3")

#         for card in cards:
#             # stop if we’ve hit our trial limit
#             if self.limit and self.processed >= self.limit:
#                 return

#             rel_link  = card.css("a.color-gray-900::attr(href)").get()
#             city_name = card.css("a.color-gray-900::text").get() or ""
#             parts     = rel_link.strip("/").split("/")
#             place_id  = parts[1] if len(parts) >= 2 and parts[0] == "explore" else None

#             if not rel_link or not place_id:
#                 continue

#             self.processed += 1
#             detail_url = response.urljoin(rel_link)

#             yield SeleniumRequest(
#                 url=detail_url,
#                 callback=self.parse_detail,
#                 meta={"city_name": city_name.strip(), "place_id": place_id},
#                 wait_time=2
#             )

#     def parse_detail(self, response):
#         city     = response.meta["city_name"]
#         place_id = response.meta["place_id"]
#         list_rel = response.css('a[href*="/list/geoCategory/"]::attr(href)').get()

#         if list_rel:
#             yield {
#                 "city_name": city,
#                 "place_id":  place_id,
#                 "list_url":  response.urljoin(list_rel)
#             }
#         else:
#             self.logger.warning(f"No list URL for {city} ({place_id})")


# import scrapy
# from scrapy_selenium import SeleniumRequest

# class WanderlogListUrlsSpider(scrapy.Spider):
#     name = "wanderlog_list_urls"
#     custom_settings = {
#         "DOWNLOAD_DELAY": 1,
#         # make sure SeleniumMiddleware is enabled in your project settings!
#         "DOWNLOADER_MIDDLEWARES": {
#             'scrapy_selenium.SeleniumMiddleware': 800
#         }
#     }

#     def __init__(self, limit=None, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.limit     = int(limit) if limit is not None else None
#         self.processed = 0

#     def start_requests(self):
#         yield SeleniumRequest(
#             url="https://wanderlog.com/explore",
#             callback=self.parse_explore,
#             wait_time=5,  # give it more time
#             script="""
#                 window.scrollTo(0, 0);
#                 window.scrollTo(0, document.body.scrollHeight);
#                 window.scrollTo(0, document.body.scrollHeight/2);
#             """
#         )

#     def parse_explore(self, response):
#         cards = response.css("div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3")
#         self.logger.info(f"🕵️ Found {len(cards)} place cards on Explore page")
#         for card in cards:
#             if self.limit and self.processed >= self.limit:
#                 self.logger.info("🔒 Trial limit reached, stopping early")
#                 return

#             rel_link  = card.css("a.color-gray-900::attr(href)").get()
#             city_name = card.css("a.color-gray-900::text").get() or ""
#             if not rel_link:
#                 self.logger.warning("⚠️  Card without a link, skipping")
#                 continue

#             parts    = rel_link.strip("/").split("/")
#             place_id = parts[1] if len(parts) >= 2 and parts[0] == "explore" else None
#             if not place_id:
#                 self.logger.warning(f"⚠️  Couldn’t parse place_id from {rel_link}")
#                 continue

#             self.processed += 1
#             detail_url = response.urljoin(rel_link)
#             yield SeleniumRequest(
#                 url=detail_url,
#                 callback=self.parse_detail,
#                 meta={"city_name": city_name.strip(), "place_id": place_id},
#                 wait_time=3
#             )

#     def parse_detail(self, response):
#         city     = response.meta["city_name"]
#         place_id = response.meta["place_id"]
#         # debug log so we know this ran
#         self.logger.info(f"🏷  parse_detail for {city} ({place_id})")

#         list_rel = response.css('a[href*="/list/geoCategory/"]::attr(href)').get()
#         if list_rel:
#             full_url = response.urljoin(list_rel)
#             self.logger.info(f"✅  Found list URL: {full_url}")
#             yield {
#                 "city_name": city,
#                 "place_id":  place_id,
#                 "list_url":  full_url
#             }
#         else:
#             self.logger.warning(f"❌  No list URL for {city} ({place_id}) at {response.url}")



# import scrapy

# class OrlandoListTestSpider(scrapy.Spider):
#     name = "orlando_list_test"
#     start_urls = ["https://wanderlog.com/explore/58152/orlando"]

#     def parse(self, response):
#         # Grab the first link whose text contains “See full list”
#         rel = response.xpath('//a[contains(normalize-space(.),"See full list")]/@href').get()
#         full = response.urljoin(rel) if rel else None

#         yield {
#             "page": response.url,
#             "list_url": full
#         }


# import scrapy
# from scrapy_selenium import SeleniumRequest

# class OrlandoXpathTestSpider(scrapy.Spider):
#     name = "orlando_xpath_test"

#     def start_requests(self):
#         yield SeleniumRequest(
#             url="https://wanderlog.com/explore/58152/orlando",
#             callback=self.parse,
#             wait_time=5,
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse(self, response):
#         href = response.xpath(
#             "/html/body/div[1]/div[5]/div[2]/div/div/div[8]/div[2]/div[6]/div[2]/a"
#         ).get()

#         yield {
#             "list_url_via_xpath": response.urljoin(href) if href else None
#         }



# import scrapy
# from scrapy_selenium import SeleniumRequest

# class OrlandoCssTestSpider(scrapy.Spider):
#     name = "orlando_css_test"

#     def start_requests(self):
#         yield SeleniumRequest(
#             url="https://wanderlog.com/explore/58152/orlando",
#             callback=self.parse,
#             wait_time=5,
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse(self, response):
#         href = response.css(
#             "#react-main > div:nth-child(6) > div.EditorContainer > div > div > div.pb-3 > div:nth-child(4) > div.mt-3.d-flex.align-items-center > div.ButtonUnpad__md_top.ButtonUnpad__md_bottom.ButtonUnpad__md_left.ButtonUnpad__md_right.pr-3.pt-2.pb-2.pl-2 > a::attr(href)"
#         ).get()

#         yield {
#             "list_url_via_css": response.urljoin(href) if href else None
#         }




# import scrapy
# from scrapy_selenium import SeleniumRequest

# class OrlandoListTestSpider(scrapy.Spider):
#     name = "orlando_list_test"

#     custom_settings = {
#         # 1) enable the Selenium middleware
#         "DOWNLOADER_MIDDLEWARES": {
#             "scrapy_selenium.SeleniumMiddleware": 800,
#         },
#         # 2) (if you haven’t already) point to your chromedriver or geckodriver:
#         # "SELENIUM_DRIVER_NAME": "chrome",
#         # "SELENIUM_DRIVER_EXECUTABLE_PATH": "/path/to/chromedriver",
#         # "SELENIUM_DRIVER_ARGUMENTS": ["--headless"],
#         "LOG_LEVEL": "INFO",
#     }

#     def start_requests(self):
#         yield SeleniumRequest(
#             url="https://wanderlog.com/explore/58152/orlando",
#             callback=self.parse,
#             wait_time=7,  # give the page time to hydrate
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse(self, response):
#         # Find any <a> that has a nested <span> whose text is “See full list”
#         href = response.xpath(
#             "//a[.//span[normalize-space(text())='See full list']]/@href"
#         ).get()

#         # Debug: log a bit of the wrapper if it exists
#         wrapper = response.xpath(
#             "//span[normalize-space(text())='See full list']/ancestor::div[1]"
#         ).get()
#         self.logger.info("Wrapper around button: %s", wrapper or "(not found)")

#         yield {
#             "page":     response.url,
#             "list_url": response.urljoin(href) if href else None
#         }


# import scrapy
# from scrapy_selenium import SeleniumRequest
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support import expected_conditions as EC

# class WanderlogSpider(scrapy.Spider):
#     """
#     A Scrapy spider that uses scrapy-selenium to handle dynamic, JavaScript-rendered
#     content on Wanderlog explore pages.
#     """
#     name = 'wanderlog_attractions_dublin'

#     def start_requests(self):
#         """
#         This method initiates the requests using SeleniumRequest.
#         """
#         urls = [
#             'https://wanderlog.com/explore/9633/dublin'
#         ]
#         for url in urls:
#             # We yield a SeleniumRequest which will render the page in a browser
#             yield SeleniumRequest(
#                 url=url,
#                 callback=self.parse,
#                 # Wait for 15 seconds for the element to be present
#                 wait_time=15,
#                 # The condition to wait for. Here, we wait for the section header
#                 # with the specific ID to be present in the DOM.
#                 wait_until=EC.presence_of_element_located(
#                     (By.ID, 'ExplorePageGeoCategorySection__attractions')
#                 )
#             )

#     def parse(self, response):
#         """
#         This method is called after the page is fully loaded by Selenium.
#         """
#         self.log(f"Parsing fully rendered page: {response.url}")

#         # --- Verification: Let's check for the "Chester Beatty" element ---
#         # This confirms that the dynamic content has loaded successfully.
#         chester_beatty_selector = "//div[contains(., 'Tips and more reviews for Chester Beatty')]"
#         chester_beatty_element = response.xpath(chester_beatty_selector).get()
#         if chester_beatty_element:
#             self.log("✅ Successfully located the 'Chester Beatty' dynamic content.")
#         else:
#             self.log("❌ Failed to locate the 'Chester Beatty' dynamic content.")

#         # --- Main Goal: Extract the "See full list" URL ---
#         # This XPath is the same as before and will work now that the page is loaded.
#         xpath_selector = "//h2[@id='ExplorePageGeoCategorySection__attractions']/parent::div//a[.//span[text()='See full list']]/@href"
        
#         # .get() retrieves the first result found.
#         relative_url = response.xpath(xpath_selector).get()

#         if relative_url:
#             # Use response.urljoin() to create a full, absolute URL from a relative one.
#             attractions_list_url = response.urljoin(relative_url)
#             self.log(f"Found 'See full list' URL: {attractions_list_url}")
            
#             yield {
#                 'source_url': response.url,
#                 'attractions_list_url': attractions_list_url
#             }
#         else:
#             self.log("Could not find the 'See full list' URL. The page structure may have changed.")

# import scrapy
# import json
# import re
# from scrapy_selenium import SeleniumRequest

# class DublinGeoCategorySpider(scrapy.Spider):
#     name = "wanderlog_attractions_dublin"
#     custom_settings = {
#         "DOWNLOADER_MIDDLEWARES": {
#             "scrapy_selenium.SeleniumMiddleware": 800,
#         },
#         "LOG_LEVEL": "INFO",
#     }

#     def start_requests(self):
#         # ① Hit only the Dublin page
#         yield SeleniumRequest(
#             url="https://wanderlog.com/explore/9633/dublin",
#             callback=self.parse_place_details,
#             wait_time=5,
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse_place_details(self, response):
#         # ② Grab the MobX JSON blob
#         raw = response.xpath(
#             '//script[contains(text(),"window.__MOBX_STATE__")]/text()'
#         ).get()
#         if not raw:
#             self.logger.error("No MobX script block found")
#             return

#         m = re.search(r"window\.__MOBX_STATE__\s*=\s*({.*?});", raw)
#         if not m:
#             self.logger.error("MobX JSON regex failed")
#             return

#         data = json.loads(m.group(1))
#         explore = data.get("explorePage", {}).get("data", {})
#         geo    = explore.get("geo", {})
#         sections = explore.get("sections", [])

#         # ③ Build your place context
#         place_id   = str(geo.get("id"))
#         city_name  = geo.get("name", "dublin")

#         # ④ Find the geoCategory section and pull its URL
#         geo_url = None
#         for sec in sections:
#             if sec.get("type") == "geoCategory":
#                 geo_url = sec.get("geoCategoryUrl")
#                 break

#         # ⑤ Yield what you need
#         yield {
#             "place_id":        place_id,
#             "city_name":       city_name,
#             "geoCategoryUrl":  geo_url
#         }




# import scrapy
# import json
# import re
# from scrapy_selenium import SeleniumRequest

# class DublinGeoTestSpider(scrapy.Spider):
#     name = "dublin_geo_test"
#     custom_settings = {
#         "DOWNLOADER_MIDDLEWARES": {
#             "scrapy_selenium.SeleniumMiddleware": 800,
#         },
#         "LOG_LEVEL": "INFO",
#     }

#     def start_requests(self):
#         yield SeleniumRequest(
#             url="https://wanderlog.com/explore/9633/dublin",
#             callback=self.parse_place_details,
#             wait_time=5,
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse_place_details(self, response):
#         # 1) grab the MobX JSON blob
#         raw = response.xpath(
#             '//script[contains(text(),"window.__MOBX_STATE__")]/text()'
#         ).get()
#         if not raw:
#             self.logger.error("❌ No MobX script block found")
#             return

#         m = re.search(r"window\.__MOBX_STATE__\s*=\s*({.*?});", raw)
#         if not m:
#             self.logger.error("❌ MobX JSON regex failed")
#             return

#         data = json.loads(m.group(1))
#         geo  = data.get("explorePage", {}).get("data", {}).get("geo", {})

#         # 2) extract the two fields
#         place_id  = str(geo.get("id", ""))
#         city_name = geo.get("name", "")

#         # 3) log and yield
#         self.logger.info(f"✅ Extracted place_id={place_id}, city_name={city_name}")
#         yield {
#             "place_id":  place_id,
#             "city_name": city_name
#         }



# import scrapy, re, json
# from scrapy_selenium import SeleniumRequest


# class OrlandoGeoSpider(scrapy.Spider):
#     name = "orlando_geo"

#     # ── Feed export: “scrapy crawl orlando_geo” will overwrite orlando_geo.json
#     custom_settings = {
#         "FEEDS": {
#             "orlando_geo.json": {
#                 "format": "json",
#                 "encoding": "utf8",
#                 "indent": 4,
#                 "overwrite": True,
#             }
#         },
#         "DOWNLOADER_MIDDLEWARES": {
#             "scrapy_selenium.SeleniumMiddleware": 800,
#         },
#         "LOG_LEVEL": "INFO",
#     }

#     start_urls = ["https://wanderlog.com/explore/58152/orlando"]

#     def start_requests(self):
#         # one and only page we need
#         yield SeleniumRequest(
#             url=self.start_urls[0],
#             callback=self.parse_place_details,
#             wait_time=3,
#             script="window.scrollTo(0, document.body.scrollHeight);",
#         )

#     def parse_place_details(self, response):
#         """Extract the city’s geo block from the MobX bootstrap script
#            and send it to Scrapy’s feed exporter."""
#         script = response.xpath(
#             '//script[contains(text(),"window.__MOBX_STATE__")]/text()'
#         ).get()

#         if not script:
#             self.logger.error("Could not locate MOBX bootstrap script.")
#             return

#         match = re.search(r"window\.__MOBX_STATE__\s*=\s*({.*?});", script)
#         if not match:
#             self.logger.error("Regex failed to isolate JSON blob.")
#             return

#         data = json.loads(match.group(1))
#         geo = (
#             data.get("explorePage", {})
#             .get("data", {})
#             .get("geo", {})
#         )

#         if not geo:
#             self.logger.error("Geo block missing in page data.")
#             return

#         yield {
#             "place_id": str(geo.get("id")),
#             "city_name": geo.get("name"),
#             "latitude": geo.get("latitude"),
#             "longitude": geo.get("longitude"),
#         }




import scrapy, re, json
from scrapy_selenium import SeleniumRequest


class OrlandoGeoSpider(scrapy.Spider):
    name = "orlando_geo"

    custom_settings = {
        "FEEDS": {
            "orlando_geo.json": {
                "format": "json",
                "encoding": "utf8",
                "indent": 4,
                "overwrite": True,
            }
        },
        # --- THIS IS THE FIX ---
        # Temporarily disable all pipelines for this spider
        "ITEM_PIPELINES": {},
        # --- END OF FIX ---
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy_selenium.SeleniumMiddleware": 800,
        },
        "LOG_LEVEL": "INFO",
    }

    start_urls = ["https://wanderlog.com/explore/58152/orlando"]

    def start_requests(self):
        # one and only page we need
        yield SeleniumRequest(
            url=self.start_urls[0],
            callback=self.parse_place_details,
            wait_time=10,  # Increased wait_time slightly for reliability
        )

    def parse_place_details(self, response):
    
        script = response.xpath(
            '//script[contains(text(),"window.__MOBX_STATE__")]/text()'
        ).get()

        if not script:
            self.logger.error("Could not locate MOBX bootstrap script.")
            return

        match = re.search(r"window\.__MOBX_STATE__\s*=\s*({.*?});", script)
        if not match:
            self.logger.error("Regex failed to isolate JSON blob.")
            return

        data = json.loads(match.group(1))
        geo = (
            data.get("explorePage", {})
            .get("data", {})
            .get("geo", {})
        )

        if not geo:
            self.logger.error("Geo block missing in page data.")
            return

        yield {
            "place_id": str(geo.get("id")),
            "city_name": geo.get("name"),
            "latitude": geo.get("latitude"),
            "longitude": geo.get("longitude"),
        }