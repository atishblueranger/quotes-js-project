# import re
# import scrapy
# from scrapy.http import Request


# class WebPlacesListTitlesSpider(scrapy.Spider):
#     name = "web_places_list_titles"

#     # Handle common error statuses without crashing
#     handle_httpstatus_list = [400, 401, 403, 404, 410, 429, 500, 502, 503, 504]

#     custom_settings = {
#         # Be polite & resilient
#         "AUTOTHROTTLE_ENABLED": True,
#         "AUTOTHROTTLE_START_DELAY": 0.5,
#         "AUTOTHROTTLE_MAX_DELAY": 10,
#         "CONCURRENT_REQUESTS": 8,
#         "RETRY_ENABLED": True,
#         "RETRY_TIMES": 3,
#         "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
#         "DOWNLOAD_TIMEOUT": 25,
#         "RANDOMIZE_DOWNLOAD_DELAY": True,
#         # Respect robots by default; flip to False only if you have permission
#         "ROBOTSTXT_OBEY": True,
#         # Safer for long runs: append lines; can resume
#         "FEEDS": {
#             "webPlacesList_titles_itinerary.jl": {
#                 "format": "jsonlines",
#                 "encoding": "utf8",
#                 "overwrite": False,
#             }
#         },
#         # Resume (creates state under this folder)
#         "JOBDIR": "crawls/webPlacesListTitles",
#         # A realistic UA helps reduce friction
#         "DEFAULT_REQUEST_HEADERS": {
#             "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#             "Accept-Language": "en-US,en;q=0.9",
#         },
#         "USER_AGENT": (
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#             "AppleWebKit/537.36 (KHTML, like Gecko) "
#             "Chrome/119.0.0.0 Safari/537.36"
#         ),
#     }

#     def __init__(self, start_id=2148, end_id=4400, output_format="jl", **kwargs):
#         """
#         Args (CLI):
#           -a start_id=2148 -a end_id=4400
#           -a output_format=jl|json   (jl = jsonlines (default), json = array)
#         """
#         super().__init__(**kwargs)
#         self.start_id = int(start_id)
#         self.end_id = int(end_id)
#         self.output_format = output_format

#         # If user wants a JSON array file instead of .jl, switch feeds here.
#         if self.output_format == "json":
#             # Overwrite to keep the JSON valid as an array for a single run
#             self.custom_settings["FEEDS"] = {
#                 f"webPlacesList_titles__itinerary{self.start_id}_{self.end_id}.json": {
#                     "format": "json",
#                     "encoding": "utf8",
#                     "overwrite": True,
#                 }
#             }

#         # For last-resort <title> cleanup (if we ever fall back to it)
#         self._suffix_pat = re.compile(r"\s*[\|\-–—]\s*Wanderlog\s*$", re.IGNORECASE)

#     def start_requests(self):
#         for i in range(self.start_id, self.end_id + 1):
#             url = f"https://wanderlog.com/list/webPlacesList/{i}"
#             yield Request(
#                 url,
#                 callback=self.parse_page,
#                 meta={"list_id": i},
#                 dont_filter=True,
#             )

#     def parse_page(self, response):
#         list_id = response.meta["list_id"]

#         # Skip missing or blocked pages
#         if response.status in (404, 410):
#             self.logger.debug(f"webPlacesList ID {list_id}: not found ({response.status})")
#             return
#         if response.status >= 400 and response.status not in (404, 410):
#             # We'll rely on retry middleware for transient errors
#             self.logger.debug(
#                 f"webPlacesList ID {list_id}: HTTP {response.status} (skipped if not retried)"
#             )
#             # Still fall through to extraction in case content exists

#         title = self._extract_title(response)
#         if not title:
#             self.logger.debug(
#                 f"webPlacesList ID {list_id}: no title found; URL={response.url}"
#             )
#             return

#         # Normalize whitespace
#         title = re.sub(r"\s+", " ", title).strip()

#         yield {
#             "webPlacesListId": list_id,
#             "title": title,
#             "url": response.url,  # final URL after any redirects (/top-things-to-do-in-...)
#         }

#     # ------------------ helpers ------------------

#     def _extract_title(self, response):
#         """
#         Try multiple signals for robustness, in order:
#           1) H1 with class PlacesListPageLoaded__webPlacesListTitle
#           2) Any first H1 on the page (normalized)
#           3) og:title meta
#           4) <title> (suffix ‘| Wanderlog’ / ‘- Wanderlog’ removed)
#         """

#         # 1) Exact-ish H1 for webPlacesList pages
#         sel0 = response.xpath(
#             "//h1[contains(@class,'PlacesListPageLoaded__webPlacesListTitle')]/text()"
#         ).get()
#         if sel0:
#             return sel0.strip()

#         # 2) Fallback: first H1 with text
#         sel2 = response.xpath("normalize-space((//h1[normalize-space()])[1]/text())").get()
#         if sel2:
#             return sel2.strip()

#         # 3) og:title
#         og = response.css('meta[property="og:title"]::attr(content)').get()
#         if og:
#             return og.strip()

#         # 4) <title> (strip Wanderlog suffix if present)
#         t = response.xpath("normalize-space(//title/text())").get()
#         if t:
#             t = self._suffix_pat.sub("", t.strip())
#             return t or None

#         return None



# Script for itineraries blog
import json
import re
import scrapy
from scrapy.http import Request


class WebPlacesListReferenceBlogSpider(scrapy.Spider):
    name = "web_places_list_reference_blog"

    handle_httpstatus_list = [400, 401, 403, 404, 410, 429, 500, 502, 503, 504]

    custom_settings = {
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "CONCURRENT_REQUESTS": 8,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
        "DOWNLOAD_TIMEOUT": 25,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "ROBOTSTXT_OBEY": True,

        # OUTPUT (appendable)
        "FEEDS": {
            "webPlacesList_with_reference_blog.jl": {
                "format": "jsonlines",
                "encoding": "utf8",
                "overwrite": False,
            }
        },

        "JOBDIR": "crawls/webPlacesListReferenceBlog",

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

    def __init__(self, input_file="webPlacesList_titles_itinerary.jl", output_format="jl", **kwargs):
        """
        Args (CLI):
          -a input_file=webPlacesList_titles_itinerary.jl
          -a output_format=jl|json
        """
        super().__init__(**kwargs)
        self.input_file = input_file
        self.output_format = output_format

        # If user wants a JSON array output
        if self.output_format == "json":
            self.custom_settings["FEEDS"] = {
                "webPlacesList_with_reference_blog.json": {
                    "format": "json",
                    "encoding": "utf8",
                    "overwrite": True,
                }
            }

        # Some pages may show internal Wanderlog links too; we’ll prefer true external ones.
        self._wanderlog_host_pat = re.compile(r"^https?://([a-z0-9-]+\.)*wanderlog\.com/", re.IGNORECASE)

    def start_requests(self):
        """
        Read JSONL input file line-by-line.
        Each line should be a JSON object with at least: url
        """
        with open(self.input_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    self.logger.warning(f"Skipping invalid JSON at line {line_num} in {self.input_file}")
                    continue

                url = item.get("url")
                if not url:
                    self.logger.warning(f"Skipping line {line_num}: missing 'url'")
                    continue

                yield Request(
                    url,
                    callback=self.parse_page,
                    meta={"item": item, "line_num": line_num},
                    dont_filter=True,
                )

    def parse_page(self, response):
        item = response.meta["item"]
        line_num = response.meta["line_num"]

        if response.status in (404, 410):
            item["reference_blog_url"] = None
            item["error"] = f"not_found_{response.status}"
            yield item
            return

        if response.status >= 400 and response.status not in (404, 410):
            # Still attempt extraction; then mark status for debugging
            item["http_status"] = response.status

        ref_url = self._extract_reference_blog_url(response)

        item["reference_blog_url"] = ref_url
        if not ref_url:
            # useful debug info for later
            item["reference_blog_url_missing"] = True
            item["source_line_num"] = line_num

        # Always keep final redirected url too (helpful)
        item["url"] = response.url

        yield item

    def _extract_reference_blog_url(self, response):
        """
        Goal: find the external attribution link like:
        <a href="https://www.nytimes.com/...." class="Button ... Button__withIcon" target="_blank" ...>

        Strategy:
          1) Grab all outbound links that open in a new tab (target=_blank) and are not wanderlog.com
          2) If multiple, prefer the first one within the button-like structure
          3) Fallback: any external <a href> that is not wanderlog.com
        """

        # 1) Strong signal: outbound links that open in new tab
        candidates = response.xpath(
            "//a[@target='_blank' and @href]/@href"
        ).getall()

        # Filter out wanderlog internal links
        candidates = [u.strip() for u in candidates if u and not self._wanderlog_host_pat.search(u.strip())]

        if candidates:
            return candidates[0]

        # 2) Slightly more specific: “Button__withIcon” anchor
        candidates2 = response.xpath(
            "//a[contains(@class,'Button__withIcon') and @href]/@href"
        ).getall()
        candidates2 = [u.strip() for u in candidates2 if u and not self._wanderlog_host_pat.search(u.strip())]
        if candidates2:
            return candidates2[0]

        # 3) Last resort: first external link anywhere on page
        all_links = response.xpath("//a[@href]/@href").getall()
        all_links = [u.strip() for u in all_links if u and u.strip().startswith("http")]
        all_links = [u for u in all_links if not self._wanderlog_host_pat.search(u)]
        return all_links[0] if all_links else None
