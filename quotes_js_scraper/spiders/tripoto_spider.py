import scrapy
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

class TripotoTravelGuideSpider(scrapy.Spider):
    name = 'tripoto_travel_guide'
    # default to 5 pages if not passed via -a pages=N
    def __init__(self, *args, pages=5, **kwargs):
        super().__init__(*args, **kwargs)
        self.pages = int(pages)

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920,1080")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.logger.info("🛠 Selenium driver initialized")

    def start_requests(self):
        base = 'https://www.tripoto.com/travel-guide/index/page:'
        for i in range(1, self.pages + 1):
            url = f"{base}{i}"
            yield scrapy.Request(url, callback=self.parse_index)

    def parse_index(self, response):
        """
        Parse a travel-guide index page, pull out each place link + name,
        then schedule a Selenium-backed Request to the detail page.
        """
        sel = scrapy.Selector(text=response.text)
        for a in sel.css('li.col-md-4 a'):
            href = a.attrib.get('href')
            name = a.xpath('normalize-space(text())').get()
            if href and name:
                # strip the "/travel-guide" prefix so we land on e.g. "/northern-ireland"
                detail_path = href.replace('/travel-guide/', '/')
                detail_url = response.urljoin(detail_path)
                yield scrapy.Request(
                    detail_url,
                    callback=self.parse_place,
                    meta={'place_name': name, 'place_url': detail_url}
                )

    def parse_place(self, response):
        """
        Load the place detail page in Selenium, wait for the collection cards to render,
        then extract each card’s title + url, along with the parent place context.
        """
        place_name = response.meta['place_name']
        place_url = response.meta['place_url']

        self.driver.get(response.url)
        time.sleep(5)  # adjust if needed

        sel = scrapy.Selector(text=self.driver.page_source)
        cards = sel.css('a.card-img.alternate-background-card')
        self.logger.info(f"🔍 Found {len(cards)} cards on {place_name}")

        for card in cards:
            href = card.attrib.get('href')
            title = card.attrib.get('title') or card.css('div.break-word::text').get()
            yield {
                # 'place_name': place_name,
                # 'place_url':  place_url,
                'card_title': title.strip() if title else '',
                'card_url':   response.urljoin(href) if href else '',
            }

    def closed(self, reason):
        self.driver.quit()
        self.logger.info("✅ Selenium driver closed")



# import scrapy
# import time
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager

# class TripotoBeachesSpider(scrapy.Spider):
#     name = 'tripoto_beaches'
#     start_urls = ['https://www.tripoto.com/meghalaya']

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         chrome_options = Options()
#         chrome_options.add_argument("--headless")
#         chrome_options.add_argument("--window-size=1920,1080")
#         service = Service(ChromeDriverManager().install())
#         self.driver = webdriver.Chrome(service=service, options=chrome_options)
#         self.logger.info("🛠 Selenium driver initialized")

#     def parse(self, response):
#         # Load the page in Selenium
#         self.driver.get(response.url)
#         self.logger.info(f"🌐 Loaded {response.url} in Selenium")
        
#         # Give the JS time to render all cards
#         time.sleep(5)

#         # Grab the full rendered HTML
#         page_source = self.driver.page_source
#         sel = scrapy.Selector(text=page_source)

#         # Select every collection card
#         cards = sel.css('a.card-img.alternate-background-card')
#         self.logger.info(f"🔍 Found {len(cards)} collection cards on the page")

#         for card in cards:
#             href = card.attrib.get('href')
#             # Try the title attribute first, fallback to inner text
#             title = card.attrib.get('title') or card.css('div.break-word::text').get()
#             yield {
#                 'title': title.strip() if title else '',
#                 'url': response.urljoin(href) if href else '',
#             }

#     def closed(self, reason):
#         self.driver.quit()
#         self.logger.info("✅ Selenium driver closed")




# import scrapy
# import time
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager

# class TripotoWeekendGetawaysSpider(scrapy.Spider):
#     name = 'tripoto_weekend_getaways'
#     start_urls = ['https://www.tripoto.com/weekend-getaways']

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         chrome_options = Options()
#         chrome_options.add_argument("--headless")
#         chrome_options.add_argument("--window-size=1920,1080")
#         service = Service(ChromeDriverManager().install())
#         self.driver = webdriver.Chrome(service=service, options=chrome_options)
#         self.logger.info("🛠 Selenium driver initialized")

#     def parse(self, response):
#         self.driver.get(response.url)
#         self.logger.info(f"🌐 Loaded {response.url} in Selenium")
        
#         # Wait a moment for the slick slider to render
#         time.sleep(5)

#         page_source = self.driver.page_source
#         sel = scrapy.Selector(text=page_source)

#         # Each slide (even off-screen) is a 'div.slick-slide'
#         slides = sel.css('div.slick-slide')
#         self.logger.info(f"🔍 Found {len(slides)} slides on the page")

#         for slide in slides:
#             a = slide.css('a.card-img.alternate-background-card')
#             if not a:
#                 continue  # skip any non-card slides
#             href = a.attrib.get('href')
#             title = a.css('div.break-word::text').get()
#             yield {
#                 'title': title.strip() if title else '',
#                 'url': response.urljoin(href) if href else '',
#             }

#     def closed(self, reason):
#         self.driver.quit()
#         self.logger.info("✅ Selenium driver closed")
