import scrapy
import json
from w3lib.html import remove_tags

class TimeoutDelhiParksSpider(scrapy.Spider):
    name = "timeout_delhi_parks"
    allowed_domains = ["timeout.com"]
    start_urls = ["https://www.timeout.com/delhi/things-to-do/best-parks-in-delhi"]

    custom_settings = {
        'USER_AGENT': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/91.0.4472.124 Safari/537.36'
        ),
        'DOWNLOAD_DELAY': 1,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parks = []
        self.page_title = ""
        self.page_subtitle = ""

    def parse(self, response):
        # 1. Page-level info
        self.page_title = response.css(
            'h1[data-testid="article-title_testID"]::text'
        ).get(default="").strip()
        self.page_subtitle = response.css(
            'p[data-testid="article-subtitle_testID"]::text'
        ).get(default="").strip()

        # 2. Loop over each park tile
        for tile in response.css('article[data-testid="tile-zone-large-list_testID"]'):
            # extract the running index (e.g. "2.")
            idx_text = tile.css('h3 span::text').get(default="").strip()
            try:
                index = int(idx_text.rstrip('.'))
            except ValueError:
                index = None

            # name from <img title="…">
            name = tile.css('img::attr(title)').get(default="").strip()

            # raw HTML of the summary block
            summary_html = tile.css('div[data-testid="summary_testID"]').get()
            description = remove_tags(summary_html).strip() if summary_html else ""

            self.parks.append({
                "index": index,
                "name": name,
                "description": description
            })

    def closed(self, reason):
        # assemble final payload
        output = {
            "title": self.page_title,
            "subtitle": self.page_subtitle,
            "parks": self.parks
        }
        # write to JSON
        with open("timeout_delhi_parks.json", "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=4)
        self.logger.info(f"Saved {len(self.parks)} parks to timeout_delhi_parks.json")
