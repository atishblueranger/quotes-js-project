import scrapy
from scrapy_selenium import SeleniumRequest

class SearchedCategoriesSpider(scrapy.Spider):
    name = "searchedCategories"
    # custom_settings = {
    #     'FEEDS': {'data/%(name)s_%(time)s.csv': {'format': 'csv'}},
    #     'DOWNLOADER_MIDDLEWARES': {
    #         'scrapy_selenium.SeleniumMiddleware': 800  # Ensure Selenium middleware is enabled
    #     }
    # }

    def start_requests(self):
        start_url = "https://wanderlog.com/searchedCategories"
        # Use SeleniumRequest to handle the dynamic content and scrolling
        yield SeleniumRequest(
            url=start_url,
            callback=self.parse_places,
            wait_time=3,  # Wait for content to load
            script='window.scrollTo(0, document.body.scrollHeight);'  # Scroll to load more places
        )

    def parse_places(self, response):
        # Extract the place elements from the loaded page
        places = response.css('div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3')
        for place in places:
            place_link = place.css('a.color-gray-900::attr(href)').get()
            city_name = place.css('a.color-gray-900::text').get()

            if place_link:
                place_id = place_link.split('/')[2]
                city_name_page = place_link.split('/')[-1]

                # Yield the extracted data
                yield {
                    'place_id': place_id,
                    'city_name': city_name,
                    'city_name_page': city_name_page,
                }

        # Optionally, add more scrolling logic if there are more results to load.
