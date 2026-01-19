# import scrapy
# import json

# class LonelyPlanetCitiesSpider(scrapy.Spider):
#     name = "lonelyplanet_cities"

#     # Custom settings for politeness
#     custom_settings = {
#         'USER_AGENT': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
#                        'AppleWebKit/537.36 (KHTML, like Gecko) '
#                        'Chrome/91.0.4472.124 Safari/537.36'),
#         'DOWNLOAD_DELAY': 1,
#     }

#     def __init__(self, *args, **kwargs):
#         super(LonelyPlanetCitiesSpider, self).__init__(*args, **kwargs)
#         # List to hold all the cities items
#         self.cities = []
#         # Running index for numbering the cities
#         self.city_index = 1

#     def start_requests(self):
#         """
#         Generate requests for 100 pages of cities.
#         The URL structure includes a page query parameter.
#         """
#         base_url = "https://www.lonelyplanet.com/places?type=City&sort=DESC&page={}"
#         # Loop over page numbers 1 through 100
#         for page in range(1, 101):
#             url = base_url.format(page)
#             self.logger.info(f"Requesting page {page}: {url}")
#             yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

#     def parse(self, response):
#         """
#         Parse the JSON embedded in the __NEXT_DATA__ script tag.
#         The JSON has the city data inside the "props" -> "pageProps" -> "places" -> "items".
#         """
#         # Locate the JSON data in the script tag
#         json_text = response.css('script#__NEXT_DATA__::text').get()
#         if not json_text:
#             self.logger.error("No JSON found in the page")
#             return

#         try:
#             data = json.loads(json_text)
#         except json.JSONDecodeError as e:
#             self.logger.error(f"Error decoding JSON: {e}")
#             return

#         # The structure may vary; usually the items are located here:
#         # data["props"]["pageProps"]["places"]["items"]
#         try:
#             items = data["props"]["pageProps"]["places"]["items"]
#         except (KeyError, TypeError) as e:
#             self.logger.error(f"Error accessing items in JSON: {e}")
#             return

#         self.logger.info(f"Found {len(items)} cities on this page")

#         for item in items:
#             # Build the city record with a running index ("Sno")
#             city_record = {
#                 "index": self.city_index,
#                 "esid": item.get("esid"),
#                 "eyebrow": item.get("eyebrow"),  # usually country or region name
#                 "title": item.get("title"),      # city name
#                 "slug": item.get("slug"),
#                 "featuredImage": item.get("featuredImage")  # image details, if available
#             }
#             self.cities.append(city_record)
#             yield city_record
#             self.city_index += 1

#     def closed(self, reason):
#         """
#         When the spider finishes, write all collected city data into a JSON file.
#         """
#         output_file = "lonelyplanet_cities.json"
#         with open(output_file, "w", encoding="utf-8") as f:
#             json.dump(self.cities, f, ensure_ascii=False, indent=4)

#         self.logger.info(f"Saved {len(self.cities)} cities to {output_file}")



import scrapy
import json

class LonelyPlanetAttractionsSpider(scrapy.Spider):
    name = "lonelyplanet_attractions"
    custom_settings = {
        'USER_AGENT': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/91.0.4472.124 Safari/537.36'),
        'DOWNLOAD_DELAY': 1,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.results = []

    def start_requests(self):
        base_url = "https://www.lonelyplanet.com/places?type=Country&sort=DESC&page={}"
        for page in range(1, 20):
            url = base_url.format(page)
            self.logger.info(f"Requesting list page {page}")
            yield scrapy.Request(url, callback=self.parse_list, dont_filter=True)

    def parse_list(self, response):
        # pull the JSON payload
        raw = response.css('script#__NEXT_DATA__::text').get()
        data = json.loads(raw or "{}")
        items = data.get("props", {})\
                    .get("pageProps", {})\
                    .get("places", {})\
                    .get("items", [])

        for city in items:
            title = city.get("title")
            slug  = city.get("slug")  # e.g. "/hungary/budapest"
            if not slug or not title:
                continue

            detail_url = response.urljoin(slug)
            yield scrapy.Request(
                detail_url,
                callback=self.parse_attractions,
                meta={'city_title': title},
                dont_filter=True
            )

    def parse_attractions(self, response):
        title = response.meta['city_title']

        # select the "View more attractions" link by its aria-label
        href = response.css('a[aria-label="View more attractions"]::attr(href)').get()
        full_url = response.urljoin(href) if href else None

        record = {
            "title": title,
            "attractions_url": full_url
        }
        self.results.append(record)
        yield record

    def closed(self, reason):
        output = "lonely_planet_countries_attractions.json"
        with open(output, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=4)
        self.logger.info(f"Saved {len(self.results)} records to {output}")

