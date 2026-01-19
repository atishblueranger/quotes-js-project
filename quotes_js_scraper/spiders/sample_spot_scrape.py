# spiders/wanderlog_cities.py
import re
import scrapy

class WanderlogCitiesSpider(scrapy.Spider):
    name = "wanderlog_cities"
    # disable all ITEM_PIPELINES for this spider
    custom_settings = {
        "ITEM_PIPELINES": {}
    }
    start_urls = ["https://wanderlog.com/placePageGeos"]

    def parse(self, response):
        for href in response.css(
            "div.col-12.col-sm-6.col-md-6.col-lg-4.col-xl-3.mt-3 "
            "a.color-gray-900::attr(href)"
        ).getall():
            if href.startswith("/placePageGeos/"):
                match = re.match(r"/placePageGeos/(\d+)/([^/]+)", href)
                if match:
                    city_id, slug = match.groups()
                    yield {
                        "city_id": city_id,
                        "slug":     slug,
                        "url":      response.urljoin(href)
                    }






# #!/usr/bin/env python3
# import os, re, json, requests
# import firebase_admin
# from firebase_admin import credentials, firestore, storage

# # ─── 1) FIREBASE & BUCKET ──────────────────────────────────────────────────────
# FIREBASE_CRED = os.path.join(
#     os.getcwd(),
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )
# cred = credentials.Certificate(FIREBASE_CRED)
# firebase_admin.initialize_app(cred, {"storageBucket": "mycasavsc.appspot.com"})
# db, bucket = firestore.client(), storage.bucket()

# # ─── 2) TARGET URL ─────────────────────────────────────────────────────────────
# DETAIL_URL = "https://wanderlog.com/place/details/3543766/miyashita"

# # ─── 3) FETCH & PULL MOBX JSON ────────────────────────────────────────────────
# html = requests.get(DETAIL_URL).text
# m = re.search(r"window\.__MOBX_STATE__\s*=\s*(\{[\s\S]*?})\s*;", html, re.DOTALL)
# if not m:
#     raise RuntimeError("MOBX_STATE JSON not found")

# state   = json.loads(m.group(1))
# data    = state["placePage"]["data"]
# metaM   = data["placeMetadata"]               # <— holds reviews & openingPeriods

# # ─── 4) DOC ID ─────────────────────────────────────────────────────────────────
# spot_id = metaM["placeId"] or f"wl_{metaM['id']}"

# # ─── 5) IMAGE UPLOADS ─────────────────────────────────────────────────────────-
# urls = []
# for key in metaM.get("imageKeys", []):
#     img = requests.get(f"https://itin-dev.wanderlogstatic.com/freeImage/{key}")
#     if img.ok:
#         blob = bucket.blob(f"localspots/{spot_id}/images/{key}.jpg")
#         blob.upload_from_string(img.content, content_type="image/jpeg")
#         blob.make_public()
#         urls.append(blob.public_url)

# # ─── 6) CORE + META MAPS ───────────────────────────────────────────────────────
# maps_geo, ta_geo = data.get("mapsPlace", {}), data.get("tripadvisorGeo", {})
# core = {
#     "name":                 metaM["name"],
#     "latitude":             maps_geo.get("latitude"),
#     "longitude":            maps_geo.get("longitude"),
#     "categories":           metaM.get("categories", []),
#     "address":              metaM.get("address"),
#     "rating":               metaM.get("rating"),
#     "numRatings":           metaM.get("numRatings"),
#     "ratingDistribution":   metaM.get("ratingDistribution", {}),   # FIX
#     "website":              metaM.get("website"),
#     "internationalPhoneNumber": metaM.get("internationalPhoneNumber"),
#     "generatedDescription": metaM.get("generatedDescription"),
#     "images":               urls,
#     "openingPeriods":       metaM.get("openingPeriods", []),       # FIX
#     "ancestorGeo": {
#         "id": ta_geo.get("id"),
#         "name": ta_geo.get("name"),
#         "countryName": ta_geo.get("countryName"),
#         "stateName": ta_geo.get("stateName"),
#         "tripadvisorId": ta_geo.get("tripadvisorId"),
#     },
# }

# meta_doc = {
#     "wanderlog_id":  str(metaM["id"]),
#     "google_place_id": metaM.get("placeId"),
#     "scraped_at": firestore.SERVER_TIMESTAMP,
# }

# # ─── 7) WRITE MAIN DOC ────────────────────────────────────────────────────────
# ref = db.collection("localspots").document(spot_id)
# ref.set({"core": core, "meta": meta_doc}, merge=True)

# # ─── 8) SUB-COLLECTIONS ───────────────────────────────────────────────────────
# # bookingInformation
# if data.get("bookingInformation"):
#     bi = data["bookingInformation"]
#     ref.collection("bookingInformation").document(
#         bi.get("reservationPlatform", "booking")
#     ).set({
#         "type": bi.get("type"),
#         "idOnPlatform": bi.get("idOnPlatform"),
#         "daysInAdvance": bi.get("daysInAdvance"),
#         "highAvailability": bi.get("highAvailability"),
#         "lowAvailability": bi.get("lowAvailability"),
#     }, merge=True)

# # reasonsToVisit
# for i, txt in enumerate(data.get("reasonsToVisit", [])):
#     ref.collection("reasonsToVisit").document(str(i)).set({"text": txt})

# # tips
# for i, txt in enumerate(data.get("tips", [])):
#     ref.collection("tips").document(str(i)).set({"text": txt})

# # menuItems
# for i, m in enumerate(data.get("menuItems", [])):
#     ref.collection("menuItems").document(str(i)).set({
#         "name": m.get("name"),
#         "imageKey": m.get("imageKey"),
#         "captionURL": m.get("captionURL"),
#     })

# # reviews  (now from metaM)
# for r in metaM.get("reviews", []):            # FIX
#     ref.collection("reviews").document(r.get("reviewId") or str(r.get("rank"))
#     ).set({
#         "reviewerName": r.get("reviewerName"),
#         "time":         r.get("time"),
#         "rating":       r.get("rating"),
#         "rank":         r.get("rank"),
#         "reviewText":   r.get("reviewText"),
#     })

# print("✅ Finished. openingPeriods & reviews now stored inside core and sub-collections.")





