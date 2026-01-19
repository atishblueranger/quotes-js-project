import scrapy
from scrapy_selenium import SeleniumRequest
import re
import json

class ZomatoSpider(scrapy.Spider):
    name = 'zomato_spider'

    def __init__(self):
        self.data = {}  # Initialize an empty dictionary to store the data

    def start_requests(self):
        start_url = 'https://www.zomato.com/india'
        yield SeleniumRequest(url=start_url, callback=self.parse_cities)

    def parse_cities(self, response):
        # Extract city URLs
        city_links = response.css('div.sc-bke1zw-1 a::attr(href)').re(r'https://www.zomato.com/[^/]+$')
        for city_link in city_links:
            # Extract the city name from the URL
            city_name = city_link.split('/')[-1].replace('-', ' ').title()
            # Construct the collections URL for each city
            collections_url = city_link + '/collections'
            yield SeleniumRequest(
                url=collections_url,
                callback=self.parse_collections,
                meta={'city_name': city_name}
            )

    def parse_collections(self, response):
        city_name = response.meta['city_name']
        # Initialize the city in the data dictionary if not already present
        if city_name not in self.data:
            self.data[city_name] = {}

        # Extract collection names and URLs
        collections = response.css('div.sc-bke1zw-1 h5::text').getall()
        collection_links = response.css('div.sc-bke1zw-1 a::attr(href)').getall()

        for collection_name, collection_link in zip(collections, collection_links):
            # Clean the collection name
            collection_name = collection_name.strip()
            # Some links may be relative; ensure they are absolute
            if not collection_link.startswith('http'):
                collection_link = response.urljoin(collection_link)

            # Initialize the collection in the city data
            if collection_name not in self.data[city_name]:
                self.data[city_name][collection_name] = []

            yield SeleniumRequest(
                url=collection_link,
                callback=self.parse_collection,
                meta={'city_name': city_name, 'collection_name': collection_name}
            )

    def parse_collection(self, response):
        city_name = response.meta['city_name']
        collection_name = response.meta['collection_name']

        # Extract restaurant cards
        restaurant_cards = response.css('div.search-card')
        for card in restaurant_cards:
            restaurant_name = card.css('a.result-title::text').get()
            restaurant_link = card.css('a.result-title::attr(href)').get()

            if restaurant_link:
                # Some links may contain query parameters; remove them
                restaurant_link = restaurant_link.split('?')[0]
                yield SeleniumRequest(
                    url=restaurant_link,
                    callback=self.parse_restaurant,
                    meta={
                        'city_name': city_name,
                        'collection_name': collection_name,
                        'restaurant_name': restaurant_name
                    }
                )

        # Pagination
        next_page = response.css('a.paginator_item.next.item::attr(href)').get()
        if next_page:
            yield SeleniumRequest(
                url=response.urljoin(next_page),
                callback=self.parse_collection,
                meta={'city_name': city_name, 'collection_name': collection_name}
            )

    def parse_restaurant(self, response):
        city_name = response.meta['city_name']
        collection_name = response.meta['collection_name']
        restaurant_name = response.meta['restaurant_name']

        # Extract latitude and longitude from the map image URL
        map_image_url = response.css('img[src*="staticmap"]::attr(src)').get()
        if map_image_url:
            lat_lng_match = re.search(r'markers=([\d.]+),([\d.]+)', map_image_url)
            if lat_lng_match:
                latitude = lat_lng_match.group(1)
                longitude = lat_lng_match.group(2)
            else:
                latitude = longitude = None
        else:
            latitude = longitude = None

        # Compile the restaurant data
        restaurant_data = {
            'restaurant_name': restaurant_name.strip() if restaurant_name else None,
            'latitude': latitude,
            'longitude': longitude,
            'detail_page': response.url,
        }

        # Append the restaurant data to the city's collection
        self.data[city_name][collection_name].append(restaurant_data)

    def close(self, reason):
        # Write the data to a JSON file when the spider closes
        with open('zomato_data.json', 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=4)
        self.logger.info('Data saved to zomato_data.json')
