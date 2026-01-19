import scrapy
import json
import time
import requests
from w3lib.html import remove_tags

class TimeoutDelhiParksEnrichmentSpider(scrapy.Spider):
    name = "timeout_delhi_parks_enrich"
    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        ),
    }

    # fields to pull from the Details endpoint
    PLACE_DETAILS_FIELDS = (
        "formatted_address,name,international_phone_number,geometry,"
        "opening_hours,website,price_level,rating,user_ratings_total,photos,"
        "types,permanently_closed,utc_offset,editorial_summary"
    )

    def __init__(self, api_key=None, input_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not api_key:
            raise ValueError("You must pass -a api_key=YOUR_GOOGLE_API_KEY")
        self.api_key = api_key
        self.input_file = input_file or "timeout_delhi_parks.json"
        self.enriched = []

    def start_requests(self):
        # load the JSON array you produced earlier
        with open(self.input_file, encoding="utf-8") as f:
            data = json.load(f)

        for park in data.get("parks", []):
            # carry forward page‑level info only once
            meta = {
                "page_title": data.get("title"),
                "page_subtitle": data.get("subtitle"),
                "record": park
            }
            # dummy request to kick off our enrichment
            yield scrapy.Request(
                url="about:blank",
                callback=self.parse_enrich,
                dont_filter=True,
                meta=meta
            )

    def parse_enrich(self, response):
        park = response.meta["record"]
        name = park.get("name")
        # 1) Find the place_id
        place_id = self._find_place_id(name)
        park["placeId"] = place_id

        # 2) Pull the details if we got a place_id
        if place_id:
            details = self._get_place_details(place_id)
            # merge in fields
            park.update({
                "address": details.get("formatted_address"),
                "internationalPhoneNumber": details.get("international_phone_number"),
                "website": details.get("website"),
                "priceLevel": details.get("price_level"),
                "rating": details.get("rating"),
                "numRatings": details.get("user_ratings_total"),
                "permanentlyClosed": details.get("permanently_closed"),
                "utcOffset": details.get("utc_offset"),
                # geometry/location
                "latitude": details.get("geometry", {}).get("location", {}).get("lat"),
                "longitude": details.get("geometry", {}).get("location", {}).get("lng"),
                # opening hours periods
                "openingPeriods": details.get("opening_hours", {}).get("periods"),
                # types/categories
                "categories": details.get("types"),
                # editorial summary override if missing
                "description": park.get("description"),
                "g_editorial_summary":details.get("editorial_summary", {}).get("overview"),
                # photos → g_image_urls & imageKeys
                **self._build_image_fields(details.get("photos"))
            })

        self.enriched.append(park)

    def closed(self, reason):
        output = {
            "title": self.enriched and self.enriched[0].get("title") or "",
            "subtitle": self.enriched and self.enriched[0].get("subtitle") or "",
            "parks": self.enriched
        }
        with open("timeout_delhi_parks_enriched.json", "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=4)
        self.logger.info(f"Saved {len(self.enriched)} enriched records.")

    def _find_place_id(self, text):
        """Find place_id via FindPlaceFromText."""
        url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
        params = {
            "input": text,
            "inputtype": "textquery",
            "fields": "place_id",
            "key": self.api_key
        }
        time.sleep(0.5)
        resp = requests.get(url, params=params).json()
        cands = resp.get("candidates", [])
        return cands[0].get("place_id") if cands else None

    def _get_place_details(self, place_id):
        """Fetch the full details for a place_id."""
        url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            "place_id": place_id,
            "fields": self.PLACE_DETAILS_FIELDS,
            "key": self.api_key
        }
        time.sleep(0.5)
        resp = requests.get(url, params=params).json()
        return resp.get("result", {})

    def _build_image_fields(self, photos):
        """Turn `photos` array into g_image_urls & imageKeys."""
        urls, keys = [], []
        if not photos:
            return {"g_image_urls": [], "imageKeys": []}

        for p in photos:
            ref = p.get("photo_reference")
            keys.append(ref)
            urls.append(
                f"https://maps.googleapis.com/maps/api/place/photo?"
                f"maxwidth=400&photo_reference={ref}&key={self.api_key}"
            )
        return {"g_image_urls": urls, "imageKeys": keys}
