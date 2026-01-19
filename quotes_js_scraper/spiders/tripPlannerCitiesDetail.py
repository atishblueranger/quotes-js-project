import scrapy
from scrapy_selenium import SeleniumRequest
from scrapy.http import HtmlResponse

import scrapy
from scrapy_selenium import SeleniumRequest
import json
import re

class TripPlannerSpider(scrapy.Spider):
    name = "tripPlannerDetail"
    
    def start_requests(self):
        start_url = "https://wanderlog.com/tp"
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
            place_link = place.css('a.TripPlannerGeosListPage__geo::attr(href)').get()
            city_name = place.css('a.TripPlannerGeosListPage__geo::text').get()

            if place_link:
                # Construct the full URL
                place_url = response.urljoin(place_link)
                # Use SeleniumRequest to navigate to the place page and handle the dynamic content
                yield SeleniumRequest(
                    url=place_url,
                    callback=self.parse_place_details,
                    meta={'city_name': city_name}  # Pass city name as metadata
                )

    def parse_place_details(self, response):
        city_name = response.meta['city_name']
        
        # Find the JavaScript block that contains the window.__MOBX_STATE__ data
        script_text = response.xpath('//script[contains(text(),"window.__MOBX_STATE__")]/text()').get()
        
        if script_text:
            # Use regex to extract the JSON object from the JavaScript
            mobx_data = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', script_text)
            if mobx_data:
                # Load the JSON data
                mobx_json = json.loads(mobx_data.group(1))
                
                # Extract the relevant place information
                geo_data = mobx_json.get('geoTripPlannerPage', {}).get('data', {}).get('geo', {})
                
                if geo_data:
                    yield {
                        'place_id': geo_data.get('id'),
                        'city_name': city_name,
                        'state_name': geo_data.get('stateName'),
                        'country_name': geo_data.get('countryName'),
                        'depth': geo_data.get('depth'),
                        'latitude': geo_data.get('latitude'),
                        'longitude': geo_data.get('longitude'),
                        'parent_id': geo_data.get('parentId'),
                        'popularity': geo_data.get('popularity'),
                        'subcategory': geo_data.get('subcategory'),
                        'image_key': geo_data.get('imageKey'),
                        'manual_description': geo_data.get('manualDescription'),
                        'place_description': geo_data.get('placeDescription'),
                        'bounds': geo_data.get('bounds'),
                    }

        # Optionally, handle other elements from the page such as nearby attractions or restaurants




