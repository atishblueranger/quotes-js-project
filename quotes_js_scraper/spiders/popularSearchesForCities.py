import scrapy
from scrapy_selenium import SeleniumRequest

class SearchedCategoriesSpider(scrapy.Spider):
    name = "popularSearchedCategoriesCities"

    def start_requests(self):
        start_url = "https://wanderlog.com/searchedCategories"
        # Use SeleniumRequest to handle dynamic content and scrolling
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

                # Create the full URL for the place
                place_url = f"https://wanderlog.com{place_link}"

                # Follow the link to extract popular searches for this place
                yield SeleniumRequest(
                    url=place_url,
                    callback=self.parse_popular_searches,
                    meta={
                        'place_id': place_id,
                        'city_name': city_name,
                        'city_name_page': city_name_page
                    },
                    wait_time=3,
                    script='window.scrollTo(0, document.body.scrollHeight);'
                )

    def parse_popular_searches(self, response):
        # Extract popular searches for this place
        popular_searches = []
        popular_search_elements = response.css('div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 a.color-gray-900')

        for search in popular_search_elements:
            search_term = search.css('::text').get()
            if search_term:
                popular_searches.append(search_term)

        # Retrieve the place metadata from the meta dictionary
        place_id = response.meta['place_id']
        city_name = response.meta['city_name']
        city_name_page = response.meta['city_name_page']

        # Yield the nested structure with place data and popular searches
        yield {
            'place_id': place_id,
            'city_name': city_name,
            'city_name_page': city_name_page,
            'popular_searches': popular_searches
        }
