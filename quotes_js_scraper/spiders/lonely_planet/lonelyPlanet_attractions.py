import scrapy
from scrapy_selenium import SeleniumRequest
import json

class LonelyPlanetAttractionsSpider(scrapy.Spider):
    name = "lonelyplanet_attractions"

    # Custom settings (adjust user agent or delay as needed)
    custom_settings = {
        'USER_AGENT': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/91.0.4472.124 Safari/537.36'),
        'DOWNLOAD_DELAY': 1,  # Be respectful to the server
    }

    def __init__(self, *args, **kwargs):
        super(LonelyPlanetAttractionsSpider, self).__init__(*args, **kwargs)
        # List to store all scraped attractions
        self.places = []

    def start_requests(self):
        """
        Starts the crawl by navigating to the Lonely Planet Budapest attractions page.
        """
        start_url = "https://www.lonelyplanet.com/hungary/budapest/attractions"
        self.logger.info(f"Starting scrape at: {start_url}")

        # Use SeleniumRequest to allow rendering of dynamic content if needed.
        yield SeleniumRequest(
            url=start_url,
            callback=self.parse_attractions,
            wait_time=3,
            # Scroll to the bottom to trigger lazy-loading, if any
            script="window.scrollTo(0, document.body.scrollHeight);"
        )

    def parse_attractions(self, response):
        """
        Parse the rendered response to extract details of each attraction.
        Each attraction is assumed to be contained within a <li> element with a class including 'col-span-1'.
        """
        # Adjust the CSS selector as needed based on the actual page structure.
        cards = response.css("li.col-span-1")
        self.logger.info(f"Found {len(cards)} attraction cards")

        for card in cards:
            # Extract the attraction name from the nested <span> element inside the <a> tag.
            place_name = card.css("div.space-y-4.mt-4 p a span.heading-05::text").get()
            # Extract the relative URL from the same <a> tag and build the absolute URL.
            relative_url = card.css("div.space-y-4.mt-4 p a::attr(href)").get()
            place_url = response.urljoin(relative_url) if relative_url else None
            # Extract the description text.
            description = card.css("div.space-y-4.mt-4 p.relative.line-clamp-3::text").get()

            # Build the attraction item as a dictionary.
            item = {
                "place_name": place_name,
                "url": place_url,
                "description": description
            }

            # Append the item to our list.
            self.places.append(item)

            # Yield the item (can be useful if you also use Scrapy pipelines or feed exports).
            yield item

    def closed(self, reason):
        """
        Once the spider completes, dump all collected attractions to a JSON file.
        """
        output_file = "lonelyplanet_places.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(self.places, f, ensure_ascii=False, indent=4)

        self.logger.info(f"Saved {len(self.places)} attractions to {output_file}")
