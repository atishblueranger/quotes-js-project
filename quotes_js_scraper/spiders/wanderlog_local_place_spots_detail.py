# spiders/wanderlog_spots.py

# wanderlog_spots.py

# wanderlog_spots.py

import os
import re
import json
import requests
import scrapy
from scrapy_selenium import SeleniumRequest
import firebase_admin
from firebase_admin import credentials, firestore, storage

class WanderlogSpotsSpider(scrapy.Spider):
    name = "wanderlog_spots"
    allowed_domains = ["wanderlog.com"]

    # Path to your commented JSON list of cities
    CITIES_FILE = os.path.join(os.getcwd(), r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\local_spot_cities.json")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 1) FIREBASE INIT (only once)
        if not firebase_admin._apps:
            cred_path = os.path.join(
                os.getcwd(),
                r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
            )
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred, {
                "storageBucket": "mycasavsc.appspot.com"
            })

        # Firestore + Storage clients
        self.db     = firestore.client()
        self.bucket = storage.bucket("mycasavsc.appspot.com")

        # 2) LOAD & PARSE cities JSON (ignore commented lines)
        raw_lines = open(self.CITIES_FILE, encoding="utf-8").read().splitlines()
        filtered  = [
            line for line in raw_lines
            if line.strip() and not line.strip().startswith(("#", "//"))
        ]
        # join and parse
        self.cities = json.loads("\n".join(filtered))

    def start_requests(self):
        # For each city entry in your JSON file, queue its page
        for city in self.cities:
            yield scrapy.Request(
                city["url"],
                callback=self.parse_city,
                cb_kwargs={
                    "city_id":   city["city_id"],
                    "city_name": city["slug"]
                }
            )

    def parse_city(self, response, city_id, city_name):
        # Find each spot link on the city page
        for href in response.css(
            "div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 "
            "a.color-gray-900::attr(href)"
        ).getall():
            if href.startswith("/place/details/"):
                yield scrapy.Request(
                    response.urljoin(href),
                    callback=self.parse_spot,
                    cb_kwargs={
                        "city_id":   city_id,
                        "city_name": city_name
                    }
                )

    def parse_spot(self, response, city_id, city_name):
        html = response.text
        m = re.search(
            r"window\.__MOBX_STATE__\s*=\s*(\{[\s\S]*?\})\s*;",
            html,
            re.DOTALL
        )
        if not m:
            self.logger.warning("MOBX_STATE JSON not found at %s", response.url)
            return

        state = json.loads(m.group(1))
        data  = state["placePage"]["data"]
        metaM = data["placeMetadata"]

        spot_id = metaM.get("placeId") or f"wl_{metaM.get('id')}"

        # Download + upload images
        public_urls = []
        for key in metaM.get("imageKeys", []):
            img_url  = f"https://itin-dev.wanderlogstatic.com/freeImage/{key}"
            img_resp = requests.get(img_url, timeout=10)
            if img_resp.status_code == 200:
                blob = self.bucket.blob(f"localspots/{spot_id}/images/{key}.jpg")
                blob.upload_from_string(img_resp.content, content_type="image/jpeg")
                blob.make_public()
                public_urls.append(blob.public_url)

        # Build core & meta
        maps_geo = data.get("mapsPlace", {})
        ta_geo   = data.get("tripadvisorGeo", {})
        core = {
            # "city_id":                 city_id,
            # "city_name":               city_name,
            "name":                    metaM.get("name"),
            "latitude":                maps_geo.get("latitude"),
            "longitude":               maps_geo.get("longitude"),
            "categories":              metaM.get("categories", []),
            "address":                 metaM.get("address"),
            "rating":                  metaM.get("rating"),
            "numRatings":              metaM.get("numRatings"),
            "ratingDistribution":      metaM.get("ratingDistribution", {}),
            "website":                 metaM.get("website"),
            "internationalPhoneNumber":metaM.get("internationalPhoneNumber"),
            "minMinutesSpent":         metaM.get("minMinutesSpent"),
            "maxMinutesSpent":         metaM.get("maxMinutesSpent"),
            "generatedDescription":    metaM.get("generatedDescription"),
            "images":                  public_urls,
            "openingPeriods":          metaM.get("openingPeriods", []),
            "ancestorGeo": {
                "id":            ta_geo.get("id"),
                "name":          ta_geo.get("name"),
                "countryName":   ta_geo.get("countryName"),
                "stateName":     ta_geo.get("stateName"),
                "tripadvisorId": ta_geo.get("tripadvisorId"),
            },
        }
        meta_doc = {
            "wanderlog_id":    str(metaM.get("id")),
            "google_place_id": metaM.get("placeId"),
            "scraped_at":      firestore.SERVER_TIMESTAMP,
        }

        # Upsert to Firestore
        doc_ref = self.db.collection("localspots").document(spot_id)
        doc_ref.set({"core": core, "meta": meta_doc}, merge=True)

        # bookingInformation
        if bi := data.get("bookingInformation"):
            plat = bi.get("reservationPlatform", "booking")
            doc_ref.collection("bookingInformation").document(plat).set({
                "type":             bi.get("type"),
                "idOnPlatform":     bi.get("idOnPlatform"),
                "daysInAdvance":    bi.get("daysInAdvance"),
                "highAvailability": bi.get("highAvailability"),
                "lowAvailability":  bi.get("lowAvailability"),
            }, merge=True)

        # reasonsToVisit, tips, menuItems, reviews
        for i, txt in enumerate(data.get("reasonsToVisit", [])):
            doc_ref.collection("reasonsToVisit").document(str(i)).set({"text": txt})
        for i, tip in enumerate(data.get("tips", [])):
            doc_ref.collection("tips").document(str(i)).set({"text": tip})
        for i, itm in enumerate(data.get("menuItems", [])):
            doc_ref.collection("menuItems").document(str(i)).set({
                "name":       itm.get("name"),
                "imageKey":   itm.get("imageKey"),
                "captionURL": itm.get("captionURL"),
            })
        for rev in metaM.get("reviews", []):
            rid = rev.get("reviewId") or str(rev.get("rank"))
            doc_ref.collection("reviews").document(rid).set({
                "reviewerName": rev.get("reviewerName"),
                "time":         rev.get("time"),
                "rating":       rev.get("rating"),
                "rank":         rev.get("rank"),
                "reviewText":   rev.get("reviewText"),
            })

        self.logger.info(
            "🗸 Stored %s (reviews: %d, openingPeriods: %d)",
            spot_id,
            len(metaM.get("reviews", [])),
            len(metaM.get("openingPeriods", []))
        )



# import os
# import re
# import json
# import requests
# import scrapy
# from scrapy_selenium import SeleniumRequest
# import firebase_admin
# from firebase_admin import credentials, firestore, storage

# class WanderlogSpotsSpider(scrapy.Spider):
#     name = "wanderlog_spots"
#     allowed_domains = ["wanderlog.com"]
#     start_urls = ["https://wanderlog.com/placePageGeos"]

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         # Initialize Firebase app only once
#         if not firebase_admin._apps:
#             cred_path = os.path.join(
#                 os.getcwd(),
#                 r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#             )
#             cred = credentials.Certificate(cred_path)
#             firebase_admin.initialize_app(cred, {
#                 "storageBucket": "mycasavsc.appspot.com"
#             })

#         # Bind Firestore client and Storage bucket to the spider
#         self.db     = firestore.client()
#         self.bucket = storage.bucket()

#     def start_requests(self):
#         # Use SeleniumRequest only for the JS‐rendered main index page
#         yield SeleniumRequest(
#             url=self.start_urls[0],
#             callback=self.parse_cities,
#             wait_time=3,
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse_cities(self, response):
#         # Extract links like /placePageGeos/{id}/{city}
#         for href in response.css(
#             "div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 "
#             "a.color-gray-900::attr(href)"
#         ).getall():
#             if href.startswith("/placePageGeos/"):
#                 yield scrapy.Request(
#                     response.urljoin(href),
#                     callback=self.parse_city
#                 )

#     def parse_city(self, response):
#         # Pull city_id and city_name from URL
#         parts = response.url.rstrip("/").split("/")
#         city_id, city_name = parts[-2], parts[-1]

#         # For each spot link on the city page, queue a detail parse
#         for href in response.css(
#             "div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 "
#             "a.color-gray-900::attr(href)"
#         ).getall():
#             if href.startswith("/place/details/"):
#                 yield scrapy.Request(
#                     response.urljoin(href),
#                     callback=self.parse_spot,
#                     cb_kwargs={"city_id": city_id, "city_name": city_name}
#                 )

#     def parse_spot(self, response, city_id, city_name):
#         html = response.text
#         # Greedy DOTALL regex to capture the full JSON blob
#         m = re.search(
#             r"window\.__MOBX_STATE__\s*=\s*(\{[\s\S]*?\})\s*;",
#             html,
#             re.DOTALL
#         )
#         if not m:
#             self.logger.warning("MOBX_STATE JSON not found at %s", response.url)
#             return

#         state = json.loads(m.group(1))
#         data  = state["placePage"]["data"]
#         metaM = data["placeMetadata"]

#         # Determine Firestore doc ID (prefer Google Place ID)
#         spot_id = metaM.get("placeId") or f"wl_{metaM.get('id')}"

#         # Download & upload images
#         public_urls = []
#         for key in metaM.get("imageKeys", []):
#             img_url  = f"https://itin-dev.wanderlogstatic.com/freeImage/{key}"
#             img_resp = requests.get(img_url, timeout=10)
#             if img_resp.status_code == 200:
#                 blob = self.bucket.blob(f"localspots/{spot_id}/images/{key}.jpg")
#                 blob.upload_from_string(img_resp.content, content_type="image/jpeg")
#                 blob.make_public()
#                 public_urls.append(blob.public_url)

#         # Build core & meta maps
#         maps_geo = data.get("mapsPlace", {})
#         ta_geo   = data.get("tripadvisorGeo", {})
#         core = {
#             # "city_id":               city_id,
#             # "city_name":             city_name,
#             "name":                  metaM.get("name"),
#             "latitude":              maps_geo.get("latitude"),
#             "longitude":             maps_geo.get("longitude"),
#             "categories":            metaM.get("categories", []),
#             "address":               metaM.get("address"),
#             "rating":                metaM.get("rating"),
#             "numRatings":            metaM.get("numRatings"),
#             "ratingDistribution":    metaM.get("ratingDistribution", {}),
#             "website":               metaM.get("website"),
#             "internationalPhoneNumber": metaM.get("internationalPhoneNumber"),
#             "minMinutesSpent":       metaM.get("minMinutesSpent"),
#             "maxMinutesSpent":       metaM.get("maxMinutesSpent"),
#             "generatedDescription":  metaM.get("generatedDescription"),
#             "images":                public_urls,
#             "openingPeriods":        metaM.get("openingPeriods", []),
#             "ancestorGeo": {
#                 "id":            ta_geo.get("id"),
#                 "name":          ta_geo.get("name"),
#                 "countryName":   ta_geo.get("countryName"),
#                 "stateName":     ta_geo.get("stateName"),
#                 "tripadvisorId": ta_geo.get("tripadvisorId"),
#             }
#         }
#         meta_doc = {
#             "wanderlog_id":    str(metaM.get("id")),
#             "google_place_id": metaM.get("placeId"),
#             "scraped_at":      firestore.SERVER_TIMESTAMP
#         }

#         # Upsert main document
#         doc_ref = self.db.collection("localspots").document(spot_id)
#         doc_ref.set({"core": core, "meta": meta_doc}, merge=True)

#         # bookingInformation subcollection
#         if bi := data.get("bookingInformation"):
#             plat = bi.get("reservationPlatform", "booking")
#             doc_ref.collection("bookingInformation").document(plat).set({
#                 "type":             bi.get("type"),
#                 "idOnPlatform":     bi.get("idOnPlatform"),
#                 "daysInAdvance":    bi.get("daysInAdvance"),
#                 "highAvailability": bi.get("highAvailability"),
#                 "lowAvailability":  bi.get("lowAvailability"),
#             }, merge=True)

#         # reasonsToVisit, tips, menuItems, reviews subcollections
#         for i, txt in enumerate(data.get("reasonsToVisit", [])):
#             doc_ref.collection("reasonsToVisit").document(str(i)).set({"text": txt})
#         for i, tip in enumerate(data.get("tips", [])):
#             doc_ref.collection("tips").document(str(i)).set({"text": tip})
#         for i, itm in enumerate(data.get("menuItems", [])):
#             doc_ref.collection("menuItems").document(str(i)).set({
#                 "name":       itm.get("name"),
#                 "imageKey":   itm.get("imageKey"),
#                 "captionURL": itm.get("captionURL"),
#             })
#         for rev in metaM.get("reviews", []):
#             rid = rev.get("reviewId") or str(rev.get("rank"))
#             doc_ref.collection("reviews").document(rid).set({
#                 "reviewerName": rev.get("reviewerName"),
#                 "time":         rev.get("time"),
#                 "rating":       rev.get("rating"),
#                 "rank":         rev.get("rank"),
#                 "reviewText":   rev.get("reviewText"),
#             })

#         self.logger.info(
#             "🗸 Stored %s (reviews: %d, openingPeriods: %d)",
#             spot_id,
#             len(metaM.get("reviews", [])),
#             len(metaM.get("openingPeriods", []))
#         )



# import os
# import re
# import json
# import scrapy
# from scrapy_selenium import SeleniumRequest
# import firebase_admin
# from firebase_admin import credentials, firestore, storage

# class WanderlogSpotsSpider(scrapy.Spider):
#     name = "wanderlog_spots"
#     allowed_domains = ["wanderlog.com"]
#     start_urls = ["https://wanderlog.com/placePageGeos"]

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         # FIREBASE INIT
#         if not firebase_admin._apps:
#             cred_path = os.path.join(
#                 os.getcwd(),
#                 r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
#             )
#             cred = credentials.Certificate(cred_path)
#             firebase_admin.initialize_app(cred, {
#                 "storageBucket": "mycasavsc.appspot.com"
#             })

#     def start_requests(self):
#         # only this first page needs Selenium
#         yield SeleniumRequest(
#             url=self.start_urls[0],
#             callback=self.parse_cities,
#             wait_time=3,
#             script="window.scrollTo(0, document.body.scrollHeight);"
#         )

#     def parse_cities(self, response):
#         for href in response.css(
#             "div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 "
#             "a.color-gray-900::attr(href)"
#         ).getall():
#             if href.startswith("/placePageGeos/"):
#                 yield scrapy.Request(response.urljoin(href), callback=self.parse_city)

#     def parse_city(self, response):
#         parts = response.url.rstrip("/").split("/")
#         city_id, city_name = parts[-2], parts[-1]
#         for href in response.css(
#             "div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 "
#             "a.color-gray-900::attr(href)"
#         ).getall():
#             if href.startswith("/place/details/"):
#                 yield scrapy.Request(
#                     response.urljoin(href),
#                     callback=self.parse_spot,
#                     cb_kwargs={"city_id": city_id, "city_name": city_name}
#                 )

#     def parse_spot(self, response, city_id, city_name):
#         html = response.text
#         m = re.search(
#             r"window\.__MOBX_STATE__\s*=\s*(\{[\s\S]*?\})\s*;",
#             html, re.DOTALL
#         )
#         if not m:
#             self.logger.warning("MOBX_STATE JSON not found at %s", response.url)
#             return

#         state = json.loads(m.group(1))
#         data  = state["placePage"]["data"]
#         metaM = data["placeMetadata"]  # placeMetadata holds reviews & openingPeriods

#         # doc ID
#         spot_id = metaM.get("placeId") or f"wl_{metaM.get('id')}"

#         # upload images
#         public_urls = []
#         for key in metaM.get("imageKeys", []):
#             img_url  = f"https://itin-dev.wanderlogstatic.com/freeImage/{key}"
#             img_resp = scrapy.Request(img_url)  # you could also use requests here
#             img_resp = response.follow(img_url).body if False else __import__('requests').get(img_url)
#             if img_resp.status_code == 200:
#                 blob = self.bucket.blob(f"localspots/{spot_id}/images/{key}.jpg")
#                 blob.upload_from_string(img_resp.content, content_type="image/jpeg")
#                 blob.make_public()
#                 public_urls.append(blob.public_url)

#         # build core + meta
#         maps_geo = data.get("mapsPlace", {})
#         ta_geo   = data.get("tripadvisorGeo", {})
#         core = {
#             # "city_id":               city_id,
#             # "city_name":             city_name,
#             "name":                  metaM.get("name"),
#             "latitude":              maps_geo.get("latitude"),
#             "longitude":             maps_geo.get("longitude"),
#             "categories":            metaM.get("categories", []),
#             "address":               metaM.get("address"),
#             "rating":                metaM.get("rating"),
#             "numRatings":            metaM.get("numRatings"),
#             "ratingDistribution":    metaM.get("ratingDistribution", {}),
#             "website":               metaM.get("website"),
#             "internationalPhoneNumber": metaM.get("internationalPhoneNumber"),
#             "minMinutesSpent":       metaM.get("minMinutesSpent"),
#             "maxMinutesSpent":       metaM.get("maxMinutesSpent"),
#             "generatedDescription":  metaM.get("generatedDescription"),
#             "images":                public_urls,
#             "openingPeriods":        metaM.get("openingPeriods", []),
#             "ancestorGeo": {
#                 "id":            ta_geo.get("id"),
#                 "name":          ta_geo.get("name"),
#                 "countryName":   ta_geo.get("countryName"),
#                 "stateName":     ta_geo.get("stateName"),
#                 "tripadvisorId": ta_geo.get("tripadvisorId"),
#             }
#         }
#         meta_doc = {
#             "wanderlog_id":   str(metaM.get("id")),
#             "google_place_id": metaM.get("placeId"),
#             "scraped_at":      firestore.SERVER_TIMESTAMP
#         }

#         # write main doc
#         doc_ref = self.db.collection("localspots").document(spot_id)
#         doc_ref.set({"core": core, "meta": meta_doc}, merge=True)

#         # bookingInformation
#         bi = data.get("bookingInformation") or {}
#         if bi:
#             plat = bi.get("reservationPlatform", "booking")
#             doc_ref.collection("bookingInformation").document(plat).set({
#                 "type":             bi.get("type"),
#                 "idOnPlatform":     bi.get("idOnPlatform"),
#                 "daysInAdvance":    bi.get("daysInAdvance"),
#                 "highAvailability": bi.get("highAvailability"),
#                 "lowAvailability":  bi.get("lowAvailability"),
#             }, merge=True)

#         # reasonsToVisit
#         for i, txt in enumerate(data.get("reasonsToVisit", [])):
#             doc_ref.collection("reasonsToVisit").document(str(i)).set({"text": txt})

#         # tips
#         for i, txt in enumerate(data.get("tips", [])):
#             doc_ref.collection("tips").document(str(i)).set({"text": txt})

#         # menuItems
#         for i, itm in enumerate(data.get("menuItems", [])):
#             doc_ref.collection("menuItems").document(str(i)).set({
#                 "name":       itm.get("name"),
#                 "imageKey":   itm.get("imageKey"),
#                 "captionURL": itm.get("captionURL"),
#             })

#         # reviews (from metaM)
#         for rev in metaM.get("reviews", []):
#             rid = rev.get("reviewId") or str(rev.get("rank"))
#             doc_ref.collection("reviews").document(rid).set({
#                 "reviewerName": rev.get("reviewerName"),
#                 "time":         rev.get("time"),
#                 "rating":       rev.get("rating"),
#                 "rank":         rev.get("rank"),
#                 "reviewText":   rev.get("reviewText"),
#             })

#         self.logger.info("🗸 Stored %s (with %d reviews, %d openingPeriods)",
#                          spot_id,
#                          len(metaM.get("reviews", [])),
#                          len(metaM.get("openingPeriods", [])))
