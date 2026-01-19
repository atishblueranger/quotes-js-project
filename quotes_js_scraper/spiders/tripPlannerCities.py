import scrapy
from scrapy_selenium import SeleniumRequest
from scrapy.http import HtmlResponse

class WanderlogSpider(scrapy.Spider):
    name = "tripPlanner"
    # custom_settings = {
    #     'FEEDS': {'data/%(name)s_%(time)s.csv': {'format': 'csv'}},
    #     'DOWNLOADER_MIDDLEWARES': {
    #         'scrapy_selenium.SeleniumMiddleware': 800  # Ensure this middleware is enabled
    #     }
    # }

    def start_requests(self):
        start_url = "https://wanderlog.com/tp"
        # Use SeleniumRequest to handle the dynamic content and scrolling
        yield SeleniumRequest(
            url=start_url,
            callback=self.parse_places,
            wait_time=3,  # Wait for content to load
            script='window.scrollTo(0, document.body.scrollHeight);'  # Scroll to the bottom to load all places
        )

    def parse_places(self, response):
        # Extract the place elements from the loaded page
        places = response.css('div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3')
        for place in places:
            place_link = place.css('a.TripPlannerGeosListPage__geo::attr(href)').get()
            city_name = place.css('a.TripPlannerGeosListPage__geo::text').get()

            if place_link:
                place_id = place_link.split('/')[2]
                city_name_page = place_link.split('/')[-1]

                # Yield the extracted data
                yield {
                    'place_id': place_id,
                    'city_name': city_name,
                    'city_name_page': city_name_page,
                }



