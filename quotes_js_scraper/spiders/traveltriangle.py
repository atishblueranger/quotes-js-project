import re
import scrapy
from scrapy.http import Request


POST_ALLOW = re.compile(r"^https?://traveltriangle\.com/blog/[^/]+/?$", re.I)
POST_BLOCK = re.compile(r"/(page/|category/|tag/|author/|feed/)", re.I)


class TravelTriangleSitemapSpider(scrapy.Spider):
    name = "traveltriangle_sitemap"

    allowed_domains = ["traveltriangle.com"]

    handle_httpstatus_list = [400, 401, 403, 404, 410, 429, 500, 502, 503, 504]

    custom_settings = {
        # Polite + resilient
        "ROBOTSTXT_OBEY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "CONCURRENT_REQUESTS": 6,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
        "RETRY_BACKOFF_BASE": 2,
        "RETRY_BACKOFF_MAX": 60,
        "DOWNLOAD_TIMEOUT": 25,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "DOWNLOAD_DELAY": 0.25,

        # Resume + cache
        "JOBDIR": "crawls/traveltriangle_sitemap",
        "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_DIR": ".httpcache/traveltriangle_sitemap",

        # Export field order
        "FEED_EXPORT_FIELDS": ["title", "url"],
        "LOG_LEVEL": "INFO",
    }

    def __init__(
        self,
        start_page=1,
        end_page=24,
        output_format="json",
        out_prefix="traveltriangle_blogs",
        **kwargs,
    ):
        """
        CLI examples:
          -a start_page=1 -a end_page=24 -a output_format=json|jl|csv
        """
        super().__init__(**kwargs)
        self.start_page = int(start_page)
        self.end_page = int(end_page)
        self.output_format = output_format.lower()
        self.out_prefix = out_prefix

        file_base = f"{self.out_prefix}_{self.start_page}_{self.end_page}"
        if self.output_format == "jl":
            feeds = {f"{self.out_prefix}.jl": {"format": "jsonlines", "encoding": "utf8", "overwrite": False}}
        elif self.output_format == "csv":
            feeds = {f"{file_base}.csv": {"format": "csv", "encoding": "utf8", "overwrite": True}}
        else:  # json array
            feeds = {f"{file_base}.json": {"format": "json", "encoding": "utf8", "overwrite": True}}
        self.custom_settings["FEEDS"] = feeds

        self.seen = set()

    def start_requests(self):
        base = "https://traveltriangle.com/blog/html-sitemap/"
        for p in range(self.start_page, self.end_page + 1):
            url = base if p == 1 else f"{base}page/{p}/"
            yield Request(url, callback=self.parse_sitemap, dont_filter=True)

    def parse_sitemap(self, response):
        # Grab all <li><a href="...">Title</a></li> links that look like posts
        for a in response.css("li > a"):
            href = a.attrib.get("href", "").strip()
            title = a.css("::text").get(default="").strip()

            if not href or not title:
                continue
            if POST_BLOCK.search(href):
                continue
            if not POST_ALLOW.match(href):
                continue

            key = (title, href)
            if key in self.seen:
                continue
            self.seen.add(key)

            # Normalize whitespace in title
            title = re.sub(r"\s+", " ", title).strip()

            yield {"title": title, "url": href}
