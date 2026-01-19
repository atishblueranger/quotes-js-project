import scrapy
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

class HolidifyCollectionsSpider(scrapy.Spider):
    name = 'holidify_collections'
    start_urls = ['https://www.holidify.com/collections/category/wildlife-and-nature/']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920,1080")
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.logger.info("Selenium driver initialized")

    def parse(self, response):
        self.driver.get(response.url)
        self.logger.info(f"Page {response.url} loaded in Selenium.")
        
        # --- Scrolling logic to load all dynamic content ---
        scroll_pause_time = 3
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        
        while True:
            # Scroll down to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Wait to load page
            time.sleep(scroll_pause_time)
            
            # Calculate new scroll height and compare with last scroll height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                self.logger.info("Reached the end of the page.")
                break
            last_height = new_height
            self.logger.info("Scrolled down to load more content.")

        # Get the page source after all content is loaded
        page_source = self.driver.page_source
        selector = scrapy.Selector(text=page_source)

        # --- Updated CSS selector for Holidify ---
        # Each collection is in a div with class 'ptv-item'
        collections = selector.css('div.ptv-item')
        self.logger.info(f"Found {len(collections)} collections after scrolling.")

        for collection in collections:
            # The 'a' tag contains both the URL and the title's parent <p> tag
            link_selector = collection.css('a')
            
            relative_url = link_selector.css('::attr(href)').get()
            title = link_selector.css('p::text').get()

            # Ensure both title and URL were found before yielding
            if title and relative_url:
                yield {
                    'title': title.strip(),
                    'url': response.urljoin(relative_url)
                }

    def closed(self, reason):
        self.driver.quit()
        self.logger.info("Selenium driver closed.")