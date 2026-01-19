import scrapy

class CntravelerSpider(scrapy.Spider):
    """
    A flexible Scrapy spider to scrape collection titles and URLs from
    multiple Condé Nast Traveler destination pages.
    """
    name = 'cntraveler_spider'

    # This list now acts as a default set of URLs if none are provided
    # via the command line. Feel free to add more URLs here.
    start_urls = [
        # 'https://www.cntraveler.com/destinations/paris',
        # 'https://www.cntraveler.com/destinations/tokyo',
        # 'https://www.cntraveler.com/destinations/edinburgh',
        # 'https://www.cntraveler.com/destinations/florence',
        # 'https://www.cntraveler.com/destinations/lisbon',
        # 'https://www.cntraveler.com/destinations/madrid',
        # 'https://www.cntraveler.com/destinations/rome',
        # 'https://www.cntraveler.com/destinations/dubai',
        # 'https://www.cntraveler.com/destinations/africa',
        # 'https://www.cntraveler.com/destinations/middle-east',
        # 'https://www.cntraveler.com/destinations/australia',
        # 'https://www.cntraveler.com/destinations/italy',
        # 'https://www.cntraveler.com/destinations/venice',
        # 'https://www.cntraveler.com/destinations/france',
        # 'https://www.cntraveler.com/destinations/turkey',
        # 'https://www.cntraveler.com/destinations/greenland',
        # 'https://www.cntraveler.com/destinations/ho-chi-minh-city',
        # 'https://www.cntraveler.com/destinations/cuba',
        # 'https://www.cntraveler.com/destinations/djerba',
        # 'https://www.cntraveler.com/destinations/antarctica',
        # 'https://www.cntraveler.com/destinations/queensland',
        # 'https://www.cntraveler.com/destinations/palau',
        # 'https://www.cntraveler.com/destinations/marseille',
        # 'https://www.cntraveler.com/destinations/uganda',
        # 'https://www.cntraveler.com/destinations/faroe-islands',
        # 'https://www.cntraveler.com/destinations/la-paz',
        # 'https://www.cntraveler.com/destinations/karakol',
        # 'https://www.cntraveler.com/destinations/sussex',
        # 'https://www.cntraveler.com/destinations/subantarctic-islands',
        # 'https://www.cntraveler.com/destinations/ngorongoro-crater',
        # 'https://www.cntraveler.com/destinations/africa',
        # 'https://www.cntraveler.com/destinations/asia',
        # 'https://www.cntraveler.com/destinations/europe',
        # 'https://www.cntraveler.com/destinations/north-america',
        # 'https://www.cntraveler.com/destinations/puerto-rico',
        # 'https://www.cntraveler.com/destinations/antigua',
        # 'https://www.cntraveler.com/destinations/abu-dhabi',
        # 'https://www.cntraveler.com/destinations/amsterdam',
        # 'https://www.cntraveler.com/destinations/austin',
        # 'https://www.cntraveler.com/destinations/atlanta',
        # 'https://www.cntraveler.com/destinations/bali',
        # 'https://www.cntraveler.com/destinations/bangkok',
        # 'https://www.cntraveler.com/destinations/barcelona',
        # 'https://www.cntraveler.com/destinations/berlin',
        # 'https://www.cntraveler.com/destinations/bermuda',
        # 'https://www.cntraveler.com/destinations/boston',
        # 'https://www.cntraveler.com/destinations/new-york-city',
        # 'https://www.cntraveler.com/destinations/san-francisco',
        # 'https://www.cntraveler.com/destinations/seattle',
        # 'https://www.cntraveler.com/destinations/new-orleans',
        'https://www.cntraveler.com/destinations/cape-town',
        'https://www.cntraveler.com/destinations/charleston',
        'https://www.cntraveler.com/destinations/chicago',
        'https://www.cntraveler.com/destinations/copenhagen',
        'https://www.cntraveler.com/destinations/dallas',
        'https://www.cntraveler.com/destinations/denver',
        'https://www.cntraveler.com/destinations/dublin',
        'https://www.cntraveler.com/destinations/hong-kong',
        'https://www.cntraveler.com/destinations/key-west',
        'https://www.cntraveler.com/destinations/los-angeles',
        'https://www.cntraveler.com/destinations/melbourne',
        'https://www.cntraveler.com/destinations/mexico-city',
        'https://www.cntraveler.com/destinations/miami',
        'https://www.cntraveler.com/destinations/montreal',
        'https://www.cntraveler.com/destinations/nashville',
        'https://www.cntraveler.com/destinations/new-york',
        'https://www.cntraveler.com/destinations/orlando',
        'https://www.cntraveler.com/destinations/philadelphia',
        'https://www.cntraveler.com/destinations/phoenix',
        'https://www.cntraveler.com/destinations/portland-maine',
        'https://www.cntraveler.com/destinations/portland-oregon',
        'https://www.cntraveler.com/destinations/savannah',
        'https://www.cntraveler.com/destinations/singapore',
        'https://www.cntraveler.com/destinations/south-america',
        'https://www.cntraveler.com/destinations/sydney',
        'https://www.cntraveler.com/destinations/toronto',
        'https://www.cntraveler.com/destinations/vancouver',
        'https://www.cntraveler.com/destinations/washington-dc'
    ]

    # It's good practice to set a common browser User-Agent to avoid being blocked.
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
    }

    def start_requests(self):
        """
        This method generates the initial requests. It can read URLs from the command
        line arguments or fall back to the default `start_urls` list.
        """
        # Get URLs from command line arguments, if provided
        # -a urls="url1,url2,..."
        urls_arg = getattr(self, 'urls', None)
        # -a file="path/to/your/urls.txt"
        urls_file = getattr(self, 'file', None)

        if urls_arg:
            # Use URLs from the 'urls' argument (comma-separated)
            urls = urls_arg.split(',')
            self.logger.info(f"Scraping URLs from command line argument: {urls}")
        elif urls_file:
            # Use URLs from the specified file
            self.logger.info(f"Scraping URLs from file: {urls_file}")
            try:
                with open(urls_file, 'r') as f:
                    urls = [line.strip() for line in f if line.strip()]
            except FileNotFoundError:
                self.logger.error(f"URL file not found: {urls_file}")
                return # Stop the spider if the file is not found
        else:
            # Fallback to the default start_urls list
            urls = self.start_urls
            self.logger.info(f"Scraping default URLs defined in the spider: {urls}")

        for url in urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        """
        This method parses each destination page, finds each collection item,
        and extracts its title and URL. The source page is added to the output
        for better context.
        """
        self.logger.info(f"Parsing page: {response.url}")

        source_page_url = response.url
        collections = response.css('li[data-testid="carousel-list-item"]')
        self.logger.info(f"Found {len(collections)} collection items on {source_page_url}")

        for collection in collections:
            title = collection.css('div[data-testid="SummaryItemHed"]::text').get()
            relative_url = collection.css('a.summary-item__hed-link::attr(href)').get()

            if title and relative_url:
                yield {
                    # Added 'source_page' to know where the data came from
                    # 'source_page': source_page_url,
                    'title': title.strip(),
                    'url': response.urljoin(relative_url)
                }
# import scrapy
# import time
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager

# class CnTravelerCarouselSpider(scrapy.Spider):
#     name = 'cntraveler_spider'
#     start_urls = ['https://www.cntraveler.com/destinations/bali']

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         chrome_options = Options()
#         chrome_options.add_argument("--headless")
#         chrome_options.add_argument("--window-size=1920,1080")
#         self.driver = webdriver.Chrome(
#             service=Service(ChromeDriverManager().install()),
#             options=chrome_options
#         )
#         self.logger.info("🛠 Selenium driver initialized")

#     def parse(self, response):
#         # load page and let JS render
#         self.driver.get(response.url)
#         self.logger.info(f"🌐 Loaded {response.url} in Selenium")
#         time.sleep(5)

#         # grab rendered HTML
#         selector = scrapy.Selector(text=self.driver.page_source)

#         # select every carousel item
#         items = selector.css('li[data-testid="carousel-list-item"]')
#         self.logger.info(f"Found {len(items)} carousel items")

#         for item in items:
#             # title under the data-testid="SummaryItemHed"
#             title = item.css('[data-testid="SummaryItemHed"]::text').get()
#             if title:
#                 title = title.strip()

#             # the link to the gallery/detail page always starts with /gallery
#             rel = item.css('a[href^="/gallery"]::attr(href)').get()
#             url = response.urljoin(rel) if rel else None

#             yield {
#                 'title': title,
#                 'url': url
#             }

#     def closed(self, reason):
#         self.driver.quit()
#         self.logger.info("🛠 Selenium driver closed")
