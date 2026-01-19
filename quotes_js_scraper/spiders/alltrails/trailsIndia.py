import scrapy
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class AllTrailsSpider(scrapy.Spider):
    name = "allTrails"
    # custom_settings = {
    #     'FEEDS': {'data/%(name)s_%(time)s.csv': {'format': 'csv'}},
    #     'DOWNLOADER_MIDDLEWARES': {
    #         'scrapy_selenium.SeleniumMiddleware': 800  # Ensure Selenium middleware is enabled
    #     }
    # }

    def start_requests(self):
        start_url = "https://www.alltrails.com/india"
        yield SeleniumRequest(
            url=start_url,
            callback=self.parse_trails,
            wait_time=3,
            script='window.scrollTo(0, document.body.scrollHeight);',
        )

    def parse_trails(self, response):
        # Extract trail elements from the loaded page using `data-testid` attributes
        trails = response.css('div[data-testid*="TrailCard"]')
        for trail in trails:
            # Extract fields with fallbacks for missing values
            name = trail.css('div[data-testid*="_Title"]::text').get() or "Not Available"
            difficulty = trail.css('span[data-testid*="_Difficulty"]::text').get() or "Not Available"
            rating = trail.css('span[data-testid*="_Rating"]::text').get() or "Not Rated"
            location = trail.css('a[data-testid*="_Location_Link"]::text').get() or "Not Available"
            length = trail.css('div[data-testid*="_Stats"]::text').get() or "Not Available"

            yield {
                'name': name,
                'difficulty': difficulty,
                'rating': rating,
                'location': location,
                'length': length
            }

        # Find "Show more trails" button by using Selenium to click it
        button = response.meta['driver'].find_element(By.CSS_SELECTOR, 'button[data-testid="show-more-button"]')
        if button:
            self.logger.info("Clicking 'Show more trails' button to load more results.")
            button.click()

            # Wait for the page to load more trails
            WebDriverWait(response.meta['driver'], 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-testid*="TrailCard"]'))
            )

            # Reissue the request to continue scraping the newly loaded trails
            yield SeleniumRequest(
                url=response.url,
                callback=self.parse_trails,
                wait_time=3,
                script='window.scrollTo(0, document.body.scrollHeight);',
                dont_filter=True
            )
