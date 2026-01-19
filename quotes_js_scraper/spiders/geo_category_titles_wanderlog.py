import re
import scrapy
from scrapy.http import Request

class GeoCategoryTitlesSpider(scrapy.Spider):
    name = "geo_category_titles"

    # Handle common error statuses without crashing
    handle_httpstatus_list = [400, 401, 403, 404, 410, 429, 500, 502, 503, 504]

    custom_settings = {
        # Be polite & resilient
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "CONCURRENT_REQUESTS": 8,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
        "DOWNLOAD_TIMEOUT": 25,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        # Respect robots by default; flip to False only if you have permission
        "ROBOTSTXT_OBEY": True,
        # Safer for long runs: append lines; can resume
        "FEEDS": {
            "geoCategory_titles.jl": {
                "format": "jsonlines",
                "encoding": "utf8",
                "overwrite": False,
            }
        },
        # Resume (creates state under this folder)
        "JOBDIR": "crawls/geoCategoryTitles",
        # A realistic UA helps reduce friction
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0.0.0 Safari/537.36"
        ),
    }

    def __init__(self, start_id=1, end_id=115294, output_format="jl", **kwargs):
        """
        Args (CLI):
          -a start_id=1 -a end_id=115294
          -a output_format=jl|json   (jl = jsonlines (default), json = array)
        """
        super().__init__(**kwargs)
        self.start_id = int(start_id)
        self.end_id = int(end_id)
        self.output_format = output_format

        # If user wants a JSON array file instead of .jl, switch feeds here.
        if self.output_format == "json":
            # Overwrite to keep the JSON valid as an array for a single run
            self.custom_settings["FEEDS"] = {
                f"geoCategory_titles_{self.start_id}_{self.end_id}.json": {
                    "format": "json",
                    "encoding": "utf8",
                    "overwrite": True,
                }
            }

        # Precompile cleanup regex
        self._suffix_pat = re.compile(r"\s*[\|\-–—]\s*Wanderlog\s*$", re.IGNORECASE)

    def start_requests(self):
        for i in range(self.start_id, self.end_id + 1):
            url = f"https://wanderlog.com/list/geoCategory/{i}"
            yield Request(url, callback=self.parse_page, meta={"page_id": i}, dont_filter=True)

    def parse_page(self, response):
        page_id = response.meta["page_id"]

        # Skip missing or blocked pages
        if response.status in (404, 410):
            self.logger.debug(f"ID {page_id}: not found ({response.status})")
            return
        if response.status >= 400 and response.status not in (404, 410):
            # We'll rely on retry middleware for transient errors
            self.logger.debug(f"ID {page_id}: HTTP {response.status} (skipped if not retried)")
            # Still try to parse in case content exists
            # but commonly 4xx/5xx won’t have useful title
            # fall through to extraction

        title = self._extract_title(response)
        if not title:
            # Nothing meaningful → skip
            self.logger.debug(f"ID {page_id}: no title found; URL={response.url}")
            return

        # Normalize whitespace
        title = re.sub(r"\s+", " ", title).strip()

        yield {
            "title": title,
            "url": response.url,  # final URL after any redirects
        }

    # ------------------ helpers ------------------

    def _extract_title(self, response):
        """
        Try multiple signals for robustness, in order:
          1) Specific H1 with classes from the provided snippet
          2) Any first H1 on the page (normalized)
          3) og:title meta
          4) <title> (suffix ‘| Wanderlog’ / ‘- Wanderlog’ removed)
        """
        # 1) The exact-ish H1 you showed (allow class order/flexibility via contains)
        sel1 = response.xpath(
            "//h1[contains(@class,'font-weight-bold') and "
            "contains(@class,'line-height-1') and "
            "(contains(@class,'font-size-36') or contains(@class,'font-size-lg-48'))]"
            "/text()"
        ).get()
        if sel1:
            return sel1.strip()

        # 2) Fallback: first H1 with text
        sel2 = response.xpath("normalize-space((//h1[normalize-space()])[1]/text())").get()
        if sel2:
            return sel2.strip()

        # 3) og:title
        og = response.css('meta[property="og:title"]::attr(content)').get()
        if og:
            return og.strip()

        # 4) <title> (strip Wanderlog suffix if present)
        t = response.xpath("normalize-space(//title/text())").get()
        if t:
            t = self._suffix_pat.sub("", t.strip())
            return t or None

        return None
