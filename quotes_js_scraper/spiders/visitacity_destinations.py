import re
import time
import scrapy
from urllib.parse import urljoin
from scrapy_selenium import SeleniumRequest
from scrapy.http import HtmlResponse

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException


DEST_ALLOW = re.compile(r"^https?://(www\.)?visitacity\.com/en/[^/?#]+/?$", re.I)


class VisitACityDestinationsSeleniumSpider(scrapy.Spider):
    name = "visitacity_destinations_selenium"
    allowed_domains = ["visitacity.com", "www.visitacity.com"]

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 0.25,
        "LOG_LEVEL": "INFO",
        # Example export (optional)
        # "FEEDS": {"visitacity_destinations.json": {"format": "json", "overwrite": True, "encoding": "utf8"}},
    }

    def __init__(self, url="https://www.visitacity.com/", max_total_clicks=80, max_clicks_per_button=20, **kwargs):
        super().__init__(**kwargs)
        self.start_url = url
        self.seen = set()

        # Safeties against infinite loops
        self.max_total_clicks = int(max_total_clicks)
        self.max_clicks_per_button = int(max_clicks_per_button)

    def start_requests(self):
        yield SeleniumRequest(
            url=self.start_url,
            callback=self.parse,
            wait_time=10,
            dont_filter=True,
        )

    def parse(self, response):
        driver = response.request.meta.get("driver")
        if not driver:
            self.logger.error("No Selenium driver found.")
            return

        wait = WebDriverWait(driver, 10)

        # Ensure initial cards present
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/en/']")))
        except TimeoutException:
            self.logger.warning("No /en/ links found initially. Page may not have rendered.")
            # still attempt to parse whatever is present

        # Expand all continents by clicking all "More ... Destinations" buttons
        self._expand_all_destinations(driver, wait)

        # Parse final DOM with Scrapy selectors
        html = driver.page_source
        response = HtmlResponse(
            url=driver.current_url,
            body=html,
            encoding="utf-8",
            request=response.request,
        )

        anchors = response.css("a[href*='/en/']")
        self.logger.info("Candidate anchors after expansion: %d", len(anchors))

        for a in anchors:
            href = (a.attrib.get("href") or "").strip()
            if not href:
                continue

            url = urljoin(response.url, href)
            if not DEST_ALLOW.match(url):
                continue

            url_norm = url.rstrip("/")
            if url_norm in self.seen:
                continue
            self.seen.add(url_norm)

            city_name = (
                a.css(".home-cities-name::text").get()
                or a.attrib.get("title")
                or a.css("img::attr(alt)").get()
                or ""
            )
            city_name = " ".join(city_name.split()).strip() or None

            continent_id = a.xpath("preceding::div[starts-with(@id,'continent_')][1]/@id").get()
            continent_key = continent_id.replace("continent_", "") if continent_id else None

            continent_title = a.xpath(
                "preceding::div[starts-with(@id,'continent_')][1]"
                "//div[contains(@class,'section_title')]/text()"
            ).get()
            continent_title = " ".join(continent_title.split()).strip() if continent_title else None

            yield {
                "continent": continent_key,
                "continent_title": continent_title,
                "city": city_name,
                "url": url_norm,
            }

    # -------------------------
    # Expansion helpers
    # -------------------------

    def _expand_all_destinations(self, driver, wait):
        """
        Click any "More ... Destinations" buttons until they stop adding new destination cards.
        Works even if the same button remains but stops responding.
        """
        total_clicks = 0

        def card_count():
            return len(driver.find_elements(By.CSS_SELECTOR, "a[href*='/en/']"))

        prev_cards = card_count()
        self.logger.info("Initial /en/ link count: %d", prev_cards)

        # We'll loop finding buttons again because the DOM changes after clicks
        while total_clicks < self.max_total_clicks:
            buttons = self._find_more_destination_buttons(driver)
            if not buttons:
                self.logger.info("No 'More ... Destinations' buttons found. Expansion complete.")
                break

            clicked_any = False

            # Iterate through currently visible buttons
            for btn in buttons:
                if total_clicks >= self.max_total_clicks:
                    break

                # Try clicking this button a few times (some continents need multiple loads)
                per_btn_clicks = 0
                while per_btn_clicks < self.max_clicks_per_button and total_clicks < self.max_total_clicks:
                    try:
                        # Re-acquire element if it went stale
                        # (Angular re-renders a lot)
                        if not btn.is_displayed():
                            break

                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                        time.sleep(0.2)

                        before = card_count()

                        driver.execute_script("arguments[0].click();", btn)
                        total_clicks += 1
                        per_btn_clicks += 1
                        clicked_any = True

                        # Wait until count increases OR timeout
                        if self._wait_for_count_increase(driver, wait, before):
                            after = card_count()
                            self.logger.info(
                                "Clicked 'More Destinations' (total=%d, thisBtn=%d): %d -> %d",
                                total_clicks, per_btn_clicks, before, after
                            )
                        else:
                            # No growth after click => this button likely exhausted
                            self.logger.info(
                                "Button click produced no new items (total=%d). Moving on.",
                                total_clicks
                            )
                            break

                        # If you're being capped by lazy-load, a small scroll helps
                        driver.execute_script("window.scrollBy(0, 400);")
                        time.sleep(0.2)

                    except StaleElementReferenceException:
                        # Re-find buttons fresh and break this inner loop
                        break
                    except Exception as e:
                        self.logger.warning("Error clicking 'More Destinations' button: %s", e)
                        break

            new_cards = card_count()

            # If we clicked but nothing increased overall, stop.
            if clicked_any and new_cards <= prev_cards:
                self.logger.info(
                    "No overall growth after clicking buttons (%d -> %d). Stopping expansion.",
                    prev_cards, new_cards
                )
                break

            prev_cards = new_cards

            # If we didn't click anything (buttons not clickable/visible), stop
            if not clicked_any:
                self.logger.info("Buttons found but none could be clicked. Stopping expansion.")
                break

        self.logger.info("Expansion finished. Total clicks=%d, final /en/ link count=%d", total_clicks, prev_cards)

    def _find_more_destination_buttons(self, driver):
        """
        Buttons are divs like:
          <div class="city_all_something_button ...">More Africa & Middle East Destinations</div>
        We'll locate by class + text containing 'More' and 'Destinations'.
        """
        candidates = driver.find_elements(By.CSS_SELECTOR, "div.city_all_something_button")
        buttons = []
        for el in candidates:
            try:
                txt = (el.text or "").strip().lower()
                if "more" in txt and "destination" in txt:
                    buttons.append(el)
            except Exception:
                continue
        return buttons

    def _wait_for_count_increase(self, driver, wait, before_count, timeout=8):
        """
        Wait until number of destination links increases beyond before_count.
        """
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                now = len(driver.find_elements(By.CSS_SELECTOR, "a[href*='/en/']"))
                if now > before_count:
                    return True
            except Exception:
                pass
            time.sleep(0.25)
        return False


# import re
# import scrapy
# from urllib.parse import urljoin
# from scrapy_selenium import SeleniumRequest
# from scrapy.http import HtmlResponse

# DEST_ALLOW = re.compile(r"^https?://(www\.)?visitacity\.com/en/[^/?#]+/?$", re.I)

# class VisitACityDestinationsSeleniumSpider(scrapy.Spider):
#     name = "visitacity_destinations_selenium"
#     allowed_domains = ["visitacity.com", "www.visitacity.com"]

#     custom_settings = {
#         "ROBOTSTXT_OBEY": True,
#         "AUTOTHROTTLE_ENABLED": True,
#         "CONCURRENT_REQUESTS": 2,
#         "DOWNLOAD_DELAY": 0.25,
#         "LOG_LEVEL": "INFO",
#     }

#     def __init__(self, url="https://www.visitacity.com/", **kwargs):
#         super().__init__(**kwargs)
#         self.start_url = url
#         self.seen = set()

#     def start_requests(self):
#         yield SeleniumRequest(
#             url=self.start_url,
#             callback=self.parse,
#             wait_time=8,
#             dont_filter=True,
#         )

#     def parse(self, response):
#         driver = response.request.meta.get("driver")
#         if not driver:
#             self.logger.error("No Selenium driver found.")
#             return

#         # A couple scrolls to trigger lazy-load if any
#         driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
#         driver.implicitly_wait(2)
#         driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#         driver.implicitly_wait(2)

#         response = HtmlResponse(
#             url=driver.current_url,
#             body=driver.page_source,
#             encoding="utf-8",
#             request=response.request,
#         )

#         for a in response.css("a[href*='/en/']"):
#             href = a.attrib.get("href", "").strip()
#             if not href:
#                 continue

#             url = urljoin(response.url, href)

#             if not DEST_ALLOW.match(url):
#                 continue

#             url_norm = url.rstrip("/")
#             if url_norm in self.seen:
#                 continue
#             self.seen.add(url_norm)

#             city_name = (
#                 a.css(".home-cities-name::text").get()
#                 or a.attrib.get("title")
#                 or a.css("img::attr(alt)").get()
#                 or ""
#             )
#             city_name = " ".join(city_name.split()).strip() or None

#             continent_id = a.xpath("preceding::div[starts-with(@id,'continent_')][1]/@id").get()
#             continent_key = continent_id.replace("continent_", "") if continent_id else None

#             continent_title = a.xpath(
#                 "preceding::div[starts-with(@id,'continent_')][1]"
#                 "//div[contains(@class,'section_title')]/text()"
#             ).get()
#             continent_title = " ".join(continent_title.split()).strip() if continent_title else None

#             yield {"continent": continent_key, "continent_title": continent_title, "city": city_name, "url": url_norm}
