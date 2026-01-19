# spider name: wanderlog_attractions_details_full.py

import scrapy
import re
import json
import time
import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

class WanderlogAttractionsDetailsFullSpider(scrapy.Spider):
    name = "wanderlog_attractions_details_full"

    def start_requests(self):
        # ─── 1) load full JSON of all geoCategoryUrls ───
        input_file = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\manual_places_attractions_municipality.json"
        if not os.path.exists(input_file):
            self.logger.error(f"Input file not found: {input_file}")
            return

        with open(input_file, "r", encoding="utf-8") as f:
            all_cities = json.load(f)

        # ─── 2) specify only the place_ids you want to process ───
        selected_ids = {
            "25",
            # "9633", "131121","174526","58207","147600","40081","81976","131141","131160","131165", # "9783",
            # "9720","131166", "146246", "85958", "81907", "58251", "58391", "9892", "58208", "147372", "81231", "58204", "9759", "58341", "82652", "82652", "147505", "82621", "82614", "82746", "82608", "82606", "82614", "82608", "82973", "85954", "283", "450", "11668", "15794", "11971", "146605", "15785", "14083", "58110", "58053", "58061", "58077", "58067", "58082", "58087", "10058", "9837", "9839", "10184", "10461", "11655", "10095", "10421", "10062", "10453", "10086", "10037", "10533", "10114", "11084", "9954", "174526", "9783", "9720",
            # "146246",
            # "9759","10134","10698","121","584","82615","117","921","9944","9946","9832", "10025", "146408","660", "828","9885",
            # "10136","285","10021","638","724","9914","9856","10109",
            # "10884","9867","1160","79346","570","95",
            # "247","85956","376","146241","15400","546",
            # "10291","147","10193","79337","10024","9997",
            # "85959","221","381","10262","1131",
            # "228","1431","85962","10073","9920","85998",
            # "13290","10382","419","9985","30290","11423",
            # "147011","382","528","583","129","10186",
            # "10366","11706","288","10876","11061","14113",
            # "717","79333","1014","10279","905","456",
            # "10590","147473","561",
            # "1192","1168","920","2601","1401",
            # "1613","1358","1590","10804",
            # "1983","993","1025","1365",
            # "2791","3543","1529","2178","2149","830",
            # "1863","1312","827","1251","1479","1163",
            # "1218","1331","1699","1424","2083","1581",	
            # "2141","3466","146277",	
            # "1812","2040",	
            # "2121",	
            # "1659",	
            # "1060",	
            # "1531",	
            # "1729",	
            # "1754",
            # "5095",	
            # "1997",	
            # "4239",	
            # "1006",	
            # "581"

            # "9612",	
            # "58149",	
            # "9632",	
            # "9628",	
            # "9619",	
            # "9627",	
            # "12",	
            # "9648",	
            # "58165",	
            # "9671",	
            # "81177",	
            # "58044",	
            # "58151",
            # "9661",	
            # "9639",	
            # "58155",	
            # "5",	
            # "9678",	
            # "9697",	
            # "58167",	
            # "9644",	
            # "9749",	
            # "9686",	
            # "650",	
            # "9718",	
            # "9699",	
            # "9685",	
            # "146",	
            # "81908",	
            # "131172",	
            # "81190",	
            # "81187",	
            # "9732",	
            # "131174",	
            # "9664",	
            # "9844",	
            # "81186",
            # "9788",	
            # "9786",	
            # "101",	
            # "9755",	
            # "9828",
            # "9749",	
            # "146",	
            # "9844",	
            # "9906",	
            # "473",	
            # "9874",
            # "36",	
            # "211",	
            # "9933",	
            # "10187",	
            # "10131",	
            # "293",	
            # "10163",	
            # "150",	
            # "189",	
            # "96",	
            # "10473",	
            # "10900",
            # "202",	
            # "169",	
            # "360",
            # "560",	
            # "1483",	
            # "3065",	
            # "87925",	
            # "3427",	
            # "5756",	
            # "7708",	
            # "6514",	
            # "7076",	
            # "8812",	
            # "5816",	
            # "6790",
            # "8096"

            #Municipilaties
            # "85939", "9630", "9636", "82574", "15", "9618", "2", "9637", "58150", "9643", "11", "9635", "9649", "81905", "9707", "131100", "3", "131086", "18", "131118", 
            # "82576", "9745", "58186", "9798", "58183", "22", "78744", "24", "9656", "82578", "58170", "53", "9711", "10413", "58175", "9683", "9666", "9670", "9735", "80", "9730", "81904", "81180", "9654", "9679", "131150", "131090", "81909", "21", "19", "131083", "9767", "131078", "131132", "37", "131115", "79", "131134", "28", "131094", "9712", "9792", "82593", "9826", "9800", "9751", "9663", "9763", "82585", "9802", "9696", "23", "10219", "29", "9803", "9668", "85940", "10061", "38", "58187", "9801", "131079", "184", "79309", "9850", "131318", "20", "58049", "81226", "9724", "58182", "10008", "82580", "9690", "9889", "82581", "9723", "9836", "9677", "58191", "51", "32", "9765", "42", "59", "131093", "9840", "9689", "81194", "10046", "9687", "131117", "9725", "9691", "9930", "9694", "9891", "9935", "9998", "79306", "11769", "58198", "9709", "9676", "9838", "9945", "9756", "30", "9799", "41", "10074", "176", "58062", "9903", "68", "131478", "78752", "9713", "34", "79303", "9919", "131360", "82594", "31", "10064", "10235", "10211", "118", "9937", "9861", "9917", "9727", "9734", "98", "24", "68", "412", "297", "313", "428", "842", "405", "304", "753", "701", "329", "783", "1868", "787", "814", "2253", "956", "461", "541", "2761", "2397", "3684", "1673",

            
            # "58152","7","57258","9633","9634","131103","58153","33","9673","9640","131076","9693","131087","131085","58058","131084","58169","131088","131091","85942","58180","85941","9642","58157","131074","9660","9665","131121","58174","9651","131077","58241","131080","9812","9674","131151","58224","131089","9681","9667","58179","9688","131157","82577","131081","44","58051","9750","9675","58242","9672","58176","9682","58173","58166","58188","146244","9662","60","58161","10033","9915","9761","10123","131119","58202","174526","9680","35","81189","82582","58210","9702","105","58275","131092","58207","74","58199","147600","58163","131164","58172","9659","131104","58050","58196","58158","9655","9692","17","9757","40081","9852","81906","58181","131162","81976","9714","58178","131114","46","9646","78","131315","81213","58228","82579","131105","58276","131141","131147","131109","10176","81196",
            # "131160","58226","131165","131139","9771","58219","91","52","58476","9717","58048","9783","85946","58164","58203","131108","49","9950","9720","9721","58177","58068","9722","131395","58231","9638","82586","93","9703","131166","58201","9875","58286","146246","9772","85958","82584","147352","9753","9737","131110","81907","58251","131122","9738","58206","131178","131098","82","215","9684","9708","11489","10171","10792","58289","131116","58222","79305","10595","58052","131626","131095","131113","58197","81913","12525","9896","9830","73","131175","131327","58309","9827","131140","131107","110","58211","131131","43","81918","79308","9887","131447","58345","10134","81219","9769","131145","10241","12491","58213","58728","10051","81924","954","10106","9736","146406","9843","145","133","9705","131177","9984","468","10031","9817","58284","174","78746","9715","131138","131337","9886","58300","9804","59165","58248","9928","131106"
            # "9703", "131166","58201",
            # "9875",
            # "58286",
            # "146246",
            # "9772","85958","82584","147352","9753","9737","131110","81907","58251","131122","9739","58269","82588","79321","10230","58391","128","58059","61","40","146238","62","9892",
            # "9841","58205","58208","9849","147352","81231","58185","58216","58079","58204","131099","131125",
            # "9759","58342","10260","58341","58218","58184","131127",
            # "146468","131438","58748","10143","58192","99","50",
            # "131146","81941","58244","9942","10015","257",
            # "175","9857","9785","76078","58110","131101",
            # "131389","146242","58212","58234","67",
            # "58193","81911","9899","9738","76078",
            # "58206","131178","131098","82","215","9684","9708",
            # "11489","10171","10792","58289","131116",
            # "58222","79305","10595","58052","131626",
            # "131095","131113","58197","81913","12525","9896",
            # "9830",
            # "73",
            # "131175","131327","58309","9827","131140","131107",
            # "110","58211","131131","43","81918","79308","9887","131447","58345","10134","81219","9769",
            # "131145","10241","12491","58213","58728","10051","81924","954","10106","9736","146406","9843","145","133","9705",
            # "131177","9984","468","10031","9817","58284",
            # "174","78746","9715","131138","131337","9886","58300","9804","59165","58248","9928","131106"

            # "57","122","161","143","256","480","183","503","384","146271","349","334","589",
            # "287","200","207","518","1055","975","696","687","895","978","1222","926","146282",
            # "615","194","877","607","375","449","728","1325","1886","485","444","1138","348",
            # "558","371","914","722","524","850","350","1966","479","1316","472","1435","662",
            # "563","1089","1743","1874","981","805","1887","738","1291","673","478","1784","1015",
            # "2371","1241","1365","536","398","1000","1808","2791","2027","527","1406","785",
            # "1942","3543","994","1529","2178","2149","1480","1002","1302","1407","976","1212",
            # "894","1159","1177","798","714","1540","4646","1463","1444","979","830","982","730",
            # "2540","1473","1863","2177","1765","1763","1135","3129","849","948","1013","1668",
            # "1442"
        }

        # ─── 3) filter down to your chosen cities ───
        cities_to_scrape = [
            city for city in all_cities
            if str(city.get("place_id")) in selected_ids
        ]
        if not cities_to_scrape:
            self.logger.warning("No matching place_id in input JSON—nothing to do.")
            return

        # ─── 4) initialize Selenium driver ONCE ───
        chrome_opts = Options()
        chrome_opts.add_argument("--headless")
        driver_svc = Service(ChromeDriverManager().install())
        driver     = webdriver.Chrome(service=driver_svc, options=chrome_opts)
        self.logger.info(f"Selenium driver initialized. Processing {len(cities_to_scrape)} cities.")

        # ─── 5) loop through each filtered city ───
        for city_data in cities_to_scrape:
            city_name        = city_data["city_name"]
            geo_category_url = city_data["geoCategoryUrl"]
            city_attractions = []

            self.logger.info(f"→ Scraping attractions for {city_name}")
            driver.get(geo_category_url)
            time.sleep(5)  # allow React to hydrate

            html  = driver.page_source
            match = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', html)
            if not match:
                self.logger.error(f"  💥 MOBX data missing for {city_name}")
                continue

            mobx_json = json.loads(match.group(1))
            data_root = mobx_json.get("placesListPage", {}).get("data", {})

            # primary source of place data
            metadata = data_root.get("placeMetadata", {})
            if isinstance(metadata, dict):
                attractions_list = list(metadata.values())
            else:
                attractions_list = metadata or []

            if not attractions_list:
                self.logger.warning(f"  ⚠️  No attractions in metadata for {city_name}")
                continue

            # build a fallback coordinate map from boardSections
            coord_map = {}
            for section in data_root.get("boardSections", []):
                for block in section.get("blocks", []):
                    if block.get("type") == "place":
                        p   = block.get("place", {})
                        pid = p.get("placeId")
                        if pid:
                            coord_map[pid] = {
                                "latitude":  p.get("latitude"),
                                "longitude": p.get("longitude"),
                            }

            self.logger.info(f"  ✔ Found {len(attractions_list)} attractions—collecting fields")

            # collect desired fields with geo fallback
            for idx, details in enumerate(attractions_list, start=1):
                geo = details.get("geo") or {}
                lat = geo.get("latitude")
                lon = geo.get("longitude")
                pid = details.get("placeId")
                if (lat is None or lon is None) and pid in coord_map:
                    lat = coord_map[pid]["latitude"]
                    lon = coord_map[pid]["longitude"]

                city_attractions.append({
                    "placeId":            pid,
                    "website":            details.get("website"),
                    "index":              idx,
                    "utcOffset":          details.get("utcOffset"),
                    "longitude":          lon,
                    "latitude":           lat,
                    "rating":             details.get("rating"),
                    "excerpt":            details.get("description"),
                    "detail_description": details.get("generatedDescription"),
                    "imageKeys":          details.get("imageKeys", []),
                    "ratingCount":        details.get("numRatings"),
                    "openingPeriods":     details.get("openingPeriods", []),
                    "name":               details.get("name"),
                    "permanentlyClosed":  details.get("permanentlyClosed"),
                    "priceLevel":         details.get("priceLevel"),
                    "phone":              details.get("internationalPhoneNumber"),
                    "address":            details.get("address"),
                    "types":              details.get("categories", []),
                    "ratingDistribution": details.get("ratingDistribution", {}),
                })

            # write out JSON for this city
            out_dir = "wanderlog_attractions_full_municipality"
            os.makedirs(out_dir, exist_ok=True)
            slug     = city_name.lower().replace(" ", "_")
            out_path = os.path.join(out_dir, f"{slug}_attractions_full.json")
            with open(out_path, "w", encoding="utf-8") as w:
                json.dump(city_attractions, w, ensure_ascii=False, indent=4)
            self.logger.info(f"  📝 Saved {len(city_attractions)} items to {out_path}")

        # ─── cleanup & final yield ───
        driver.quit()
        self.logger.info("All done — driver closed.")
        yield {"status": "finished", "cities_processed": len(cities_to_scrape)}

# import scrapy
# import re
# import json
# import time
# import os

# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager

# class WanderlogAttractionsDetailsFullSpider(scrapy.Spider):
#     name = "wanderlog_attractions_details_full"

#     def start_requests(self):
#         # ─── 1) load full JSON of all geoCategoryUrls ───
#         input_file = "C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wanderlog_explore_attractions_full_urls_data2.json"
#         if not os.path.exists(input_file):
#             self.logger.error(f"Input file not found: {input_file}")
#             return

#         with open(input_file, "r", encoding="utf-8") as f:
#             all_cities = json.load(f)

#         # ─── 2) specify only the place_ids you want to process ───
#         selected_ids = {"57","122","161","143","256","480","183","503","384","146271","349","334","589","287","200","207","518","1055","975","696","687","895","978","1222","926","146282","615","194","877","607","375","449","728","1325","1886","485","444","1138","348","558","371","914","722","524","850","350","1966","479","1316","472","1435","662","563","1089","1743","1874","981","805","1887","738","1291","673","478","1784","1015","2371","1241","1365","536","398","1000","1808","2791","2027","527","1406","785","1942","3543","994","1529","2178","2149","1480","1002","1302","1407","976","1212","894","1159","1177","798","714","1540","4646","1463","1444","979","830","982","730","2540","1473","1863","2177","1765","1763","1135","3129","849","948","1013","1668","1442"}

#         # ─── 3) filter down to your chosen cities ───
#         cities_to_scrape = [
#             city for city in all_cities
#             if str(city.get("place_id")) in selected_ids
#         ]
#         if not cities_to_scrape:
#             self.logger.warning("No matching place_id in input JSON—nothing to do.")
#             return

#         # ─── 4) init Selenium driver ONCE ───
#         chrome_opts   = Options()
#         chrome_opts.add_argument("--headless")
#         driver_svc    = Service(ChromeDriverManager().install())
#         driver        = webdriver.Chrome(service=driver_svc, options=chrome_opts)
#         self.logger.info(f"Selenium driver initialized. Processing {len(cities_to_scrape)} cities.")

#         # ─── 5) now loop only the filtered list ───
#         for city_data in cities_to_scrape:
#             city_name        = city_data["city_name"]
#             geo_category_url = city_data["geoCategoryUrl"]
#             city_attractions = []

#             self.logger.info(f"→ Scraping attractions for {city_name}")
#             driver.get(geo_category_url)
#             time.sleep(5)  # allow React to hydrate

#             html  = driver.page_source
#             match = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', html)
#             if not match:
#                 self.logger.error(f"  💥 MOBX data missing for {city_name}")
#                 continue

#             mobx_json = json.loads(match.group(1))
#             metadata  = (
#                 mobx_json
#                 .get("placesListPage", {})
#                 .get("data", {})
#                 .get("placeMetadata", {})
#             )
#             # unify dict vs list
#             if isinstance(metadata, dict):
#                 attractions_list = list(metadata.values())
#             else:
#                 attractions_list = metadata or []

#             if not attractions_list:
#                 self.logger.warning(f"  ⚠️  No attractions in metadata for {city_name}")
#                 continue

#             self.logger.info(f"  ✔ Found {len(attractions_list)} attractions—building output")

#             # Collect your fields
#             for idx, details in enumerate(attractions_list, start=1):
#                 geo = details.get("geo") or {}
#                 city_attractions.append({
#                     "placeId":            details.get("placeId"),
#                     "website":            details.get("website"),
#                     "index":              idx,
#                     "utcOffset":          details.get("utcOffset"),
#                     "longitude":          geo.get("longitude"),
#                     "latitude":           geo.get("latitude"),
#                     "rating":             details.get("rating"),
#                     "excerpt":            details.get("description"),
#                     "detail_description": details.get("generatedDescription"),
#                     "imageKeys":          details.get("imageKeys", []),
#                     "ratingCount":        details.get("numRatings"),
#                     "openingPeriods":     details.get("openingPeriods", []),
#                     "name":               details.get("name"),
#                     "permanentlyClosed":  details.get("permanentlyClosed"),
#                     "priceLevel":         details.get("priceLevel"),
#                     "phone":              details.get("internationalPhoneNumber"),
#                     "address":            details.get("address"),
#                     "types":              details.get("categories", []),
#                     "ratingDistribution": details.get("ratingDistribution", {}),
#                 })

#             # Write out JSON
#             out_dir = "wanderlog_attractions_full_2"
#             os.makedirs(out_dir, exist_ok=True)
#             slug     = city_name.lower().replace(" ", "_")
#             out_path = os.path.join(out_dir, f"{slug}_attractions_full.json")
#             with open(out_path, "w", encoding="utf-8") as w:
#                 json.dump(city_attractions, w, ensure_ascii=False, indent=4)
#             self.logger.info(f"  📝 Saved {len(city_attractions)} items to {out_path}")

#         # ─── cleanup and final yield ───
#         driver.quit()
#         self.logger.info("All done — driver closed.")
#         yield {"status": "finished", "cities_processed": len(cities_to_scrape)}





# import scrapy
# import re
# import json
# import time
# import os

# # Selenium imports
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager

# class WanderlogAttractionsDetailsFullSpider(scrapy.Spider):
#     name = "wanderlog_attractions_details_full"

#     def start_requests(self):
#         # ───── YOUR INPUT ─────
#         cities_to_scrape = [
#             # {"place_id": "9634", "city_name": "Venice", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104659/top-things-to-do-and-attractions-in-venice"},
#             # {"place_id": "131103", "city_name": "Gramado", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105986/top-things-to-do-and-attractions-in-gramado"},
#             # {"place_id": "58153", "city_name": "Honolulu", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105423/top-things-to-do-and-attractions-in-honolulu"},
#             # {"place_id": "33", "city_name": "Kuala Lumpur", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104417/top-things-to-do-and-attractions-in-kuala-lumpur"}
#             # {"place_id": "9673", "city_name": "Stockholm", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104692/top-things-to-do-and-attractions-in-stockholm"},
#             # {"place_id": "9640", "city_name": "Krakow", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104664/top-things-to-do-and-attractions-in-krakow"},
#             # {"place_id": "131076", "city_name": "Lima", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105959/top-things-to-do-and-attractions-in-lima"},
#             # {"place_id": "78757", "city_name": "Granada", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105753/top-things-to-do-and-attractions-in-granada"},
#             # {"place_id": "131087", "city_name": "Brasilia", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105970/top-things-to-do-and-attractions-in-brasilia"},
#             # {"place_id": "131085", "city_name": "Salvador", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105968/top-things-to-do-and-attractions-in-salvador"},
#             # {"place_id": "58058", "city_name": "Niagara Falls", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105403/top-things-to-do-and-attractions-in-niagara-falls"},
#             # {"place_id": "131084", "city_name": "Montevideo", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105967/top-things-to-do-and-attractions-in-montevideo"},
#             # {"place_id": "58169", "city_name": "Atlanta", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105436/top-things-to-do-and-attractions-in-atlanta"},
#             # {"place_id": "131088", "city_name": "Belo Horizonte", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105971/top-things-to-do-and-attractions-in-belo-horizonte"},
#         #     {"place_id": "85942", "city_name": "Abu Dhabi", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105934/top-things-to-do-and-attractions-in-abu-dhabi"},
#         #    {"place_id": "131091", "city_name": "Fortaleza", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105974/top-things-to-do-and-attractions-in-fortaleza"},
#         #    {"place_id": "58180", "city_name": "Miami Beach", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105447/top-things-to-do-and-attractions-in-miami-beach"},
#         #    {"place_id": "85941", "city_name": "Jerusalem", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105933/top-things-to-do-and-attractions-in-jerusalem"},
#         #    {"place_id": "9642", "city_name": "Warsaw", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104666/top-things-to-do-and-attractions-in-warsaw"},
#         #    {"place_id": "58157", "city_name": "Miami", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105426/top-things-to-do-and-attractions-in-miami"},
#         #    {"place_id": "131074", "city_name": "Cusco", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105957/top-things-to-do-and-attractions-in-cusco"},
#         #    {"place_id": "9660", "city_name": "Liverpool", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104682/top-things-to-do-and-attractions-in-liverpool"},
#         #    {"place_id": "9665", "city_name": "Oslo", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104685/top-things-to-do-and-attractions-in-oslo"},
#         #    {"place_id": "131121", "city_name": "Armacao dos Buzios", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/106004/top-things-to-do-and-attractions-in-armacao-dos-buzios"}
#             # {"place_id": "58174", "city_name": "Savannah", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105441/top-things-to-do-and-attractions-in-savannah"},
#             # # {"place_id": "9651", "city_name": "Glasgow", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104673/top-things-to-do-and-attractions-in-glasgow"},
#             # {"place_id": "131077", "city_name": "Bogota", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105960/top-things-to-do-and-attractions-in-bogota"},
#             # {"place_id": "58241", "city_name": "Pigeon Forge", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105505/top-things-to-do-and-attractions-in-pigeon-forge"},
#             # {"place_id": "131080", "city_name": "Cartagena", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105963/top-things-to-do-and-attractions-in-cartagena"},
#             # {"place_id": "9812", "city_name": "Cordoba", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104808/top-things-to-do-and-attractions-in-cordoba"}
#             {"place_id": "9674", "city_name": "Nice", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104693/top-things-to-do-and-attractions-in-nice"},
#             {"place_id": "131151", "city_name": "Campos Do Jordao", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/106028/top-things-to-do-and-attractions-in-campos-do-jordao"},
#             {"place_id": "58224", "city_name": "Memphis", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105490/top-things-to-do-and-attractions-in-memphis"},
#             {"place_id": "131089", "city_name": "Recife", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105972/top-things-to-do-and-attractions-in-recife"},
#             {"place_id": "9681", "city_name": "Malaga", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104699/top-things-to-do-and-attractions-in-malaga"},
#             {"place_id": "9667", "city_name": "Helsinki", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104687/top-things-to-do-and-attractions-in-helsinki"},
#             {"place_id": "58179", "city_name": "Baltimore", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105446/top-things-to-do-and-attractions-in-baltimore"},
#         ]

#         # ─── Initialize Selenium driver once ───
#         chrome_opts = Options()
#         chrome_opts.add_argument("--headless")
#         driver_service = Service(ChromeDriverManager().install())
#         driver = webdriver.Chrome(service=driver_service, options=chrome_opts)
#         self.logger.info(f"Selenium driver initialized. Processing {len(cities_to_scrape)} cities.")

#         # ─── Loop through each city ───
#         for city_data in cities_to_scrape:
#             city_name       = city_data["city_name"]
#             place_id        = city_data["place_id"]
#             geo_category_url= city_data["geoCategoryUrl"]
#             city_attractions = []

#             self.logger.info(f"Navigating to attractions page for: {city_name}")
#             driver.get(geo_category_url)
#             time.sleep(5)  # wait for React to hydrate

#             html = driver.page_source
#             match = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', html)
#             if not match:
#                 self.logger.error(f"Could not find MOBX data for {city_name}.")
#                 continue

#             mobx_json = json.loads(match.group(1))
#             place_metadata = (
#                 mobx_json
#                 .get("placesListPage", {})
#                 .get("data", {})
#                 .get("placeMetadata", {})
#             )

#             # unify dict vs list
#             if isinstance(place_metadata, dict):
#                 attractions_list = list(place_metadata.values())
#             else:
#                 attractions_list = place_metadata or []

#             if not attractions_list:
#                 self.logger.warning(f"No place metadata found for: {city_name}")
#                 continue

#             self.logger.info(f"Found {len(attractions_list)} attractions for {city_name} — collecting fields.")

#             # ─── Build your output list ───
#             for idx, details in enumerate(attractions_list, start=1):
#                 geo = details.get("geo") or {}
#                 city_attractions.append({
#                     "placeId":            details.get("placeId"),
#                     "website":            details.get("website"),
#                     "index":              idx,
#                     "utcOffset":          details.get("utcOffset"),
#                     "longitude":          geo.get("longitude"),
#                     "latitude":           geo.get("latitude"),
#                     "rating":             details.get("rating"),
#                     "excerpt":            details.get("description"),
#                     "detail_description": details.get("generatedDescription"),
#                     "imageKeys":          details.get("imageKeys", []),
#                     "ratingCount":        details.get("numRatings"),
#                     "openingPeriods":     details.get("openingPeriods", []),
#                     "name":               details.get("name"),
#                     "permanentlyClosed":  details.get("permanentlyClosed"),
#                     "priceLevel":         details.get("priceLevel"),
#                     "phone":              details.get("internationalPhoneNumber"),
#                     "address":            details.get("address"),
#                     "types":              details.get("categories", []),
#                     "ratingDistribution": details.get("ratingDistribution", {}),
#                 })

#             # ─── Write JSON file ───
#             folder = "wanderlog_attractions_full"
#             os.makedirs(folder, exist_ok=True)
#             slug = city_name.lower().replace(" ", "_")
#             path = os.path.join(folder, f"{slug}_attractions_full.json")
#             with open(path, "w", encoding="utf-8") as f:
#                 json.dump(city_attractions, f, ensure_ascii=False, indent=4)

#             self.logger.info(f"SUCCESS: Saved {len(city_attractions)} items to {path}")

#         # ─── Clean up & final yield ───
#         driver.quit()
#         self.logger.info("All cities processed. Driver closed.")
#         # one final yield so Scrapy sees an item and shuts down cleanly
#         yield {
#             "status":            "finished",
#             "cities_processed":  len(cities_to_scrape)
#         }









































# partially working
# import scrapy
# import re
# import json
# import time
# import os

# # Selenium imports
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager

# class WanderlogAttractionsDetailsFullSpider(scrapy.Spider):
#     name = "wanderlog_attractions_details_full"

#     def start_requests(self):
#         cities_to_scrape = [
#             {
#               "place_id": "58144",
#               "city_name": "New York City",
#               "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105416/top-things-to-do-and-attractions-in-new-york-city"
#             }
#         ]

#         # Initialize Selenium driver ONCE
#         chrome_opts = Options()
#         chrome_opts.add_argument("--headless")
#         driver_service = Service(ChromeDriverManager().install())
#         driver = webdriver.Chrome(service=driver_service, options=chrome_opts)
#         self.logger.info(f"Selenium driver initialized. Processing {len(cities_to_scrape)} cities.")

#         # Loop through each city in the list
#         for city_data in cities_to_scrape:
#             city_name = city_data['city_name']
#             place_id = city_data['place_id']
#             geo_category_url = city_data['geoCategoryUrl']
#             city_attractions = []

#             self.logger.info(f"Navigating to attractions page for: {city_name}")
#             driver.get(geo_category_url)
#             time.sleep(5)

#             html = driver.page_source
            
#             mobx_data_match = re.search(r'window\.__MOBX_STATE__\s*=\s*({.*?});', html)
#             if not mobx_data_match:
#                 self.logger.error(f"Could not find MOBX data for {city_name}.")
#                 continue

#             mobx_json = json.loads(mobx_data_match.group(1))
#             place_metadata = mobx_json.get('placesListPage', {}).get('data', {}).get('placeMetadata', {})

#             # --- THE FIX IS HERE ---
#             # Prepare a unified list of attractions, whether the source is a dict or a list.
#             attractions_list = []
#             if isinstance(place_metadata, dict):
#                 attractions_list = place_metadata.values()
#             elif isinstance(place_metadata, list):
#                 attractions_list = place_metadata
            
#             if not attractions_list:
#                 self.logger.warning(f"No place metadata found for: {city_name}")
#                 continue
            
#             self.logger.info(f"Found {len(attractions_list)} attractions for {city_name}. Collecting items.")
            
#             # Now, safely loop through the unified attractions_list
#             for item_details in attractions_list:
#                 geo_info = item_details.get('geo', {}) or {}
                
#                 city_attractions.append({
#                     "name": item_details.get('name'),
#                     "placeId": item_details.get('placeId'),
#                     "address": item_details.get('address'),
#                     "phone": item_details.get('internationalPhoneNumber'),
#                     "website": item_details.get('website'),
#                     "types": item_details.get('categories', []),
#                     "excerpt": item_details.get('description'),
#                     "detail_description": item_details.get('generatedDescription'),
#                     "rating": item_details.get('rating'),
#                     "ratingCount": item_details.get('numRatings'),
#                     "ratingDistribution": item_details.get('ratingDistribution', {}),
#                     "priceLevel": item_details.get('priceLevel'),
#                     "permanentlyClosed": item_details.get('permanentlyClosed'),
#                     "openingPeriods": item_details.get('openingPeriods', []),
#                     "latitude": geo_info.get('latitude'),
#                     "longitude": geo_info.get('longitude'),
#                     "utcOffset": item_details.get('utcOffset'),
#                     "imageKeys": item_details.get('imageKeys', []),
#                     'parent_city_name': city_name,
#                     'parent_city_id': place_id,
#                 })
            
#             if city_attractions:
#                 folder_name = "wanderlog_attractions_full"
#                 os.makedirs(folder_name, exist_ok=True)
#                 file_name = f"{city_name.lower().replace(' ', '_')}_attractions_full.json"
#                 output_path = os.path.join(folder_name, file_name)

#                 with open(output_path, 'w', encoding='utf-8') as f:
#                     json.dump(city_attractions, f, ensure_ascii=False, indent=4)
                
#                 self.logger.info(f"SUCCESS: Saved {len(city_attractions)} attractions to {output_path}")
        
#         driver.quit()
#         self.logger.info("All cities processed. Driver closed.")
# import os
# import re
# import json
# import scrapy
# from scrapy_selenium import SeleniumRequest
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager

# class WanderlogAttractionsDetailsFullSpider(scrapy.Spider):
#     name = "wanderlog_attractions_details_full"

#     # ─── HARD-CODED INPUTS ───────────────
#     INPUTS = [
#         {"place_id": "58152", "city_name": "Orlando", "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105422/top-things-to-do-and-attractions-in-orlando"},
#         # {
#         #     "place_id":       "58144",
#         #     "city_name":      "New York City",
#         #     "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105416/top-things-to-do-and-attractions-in-new-york-city"
#         # },
#         # {
#         #     "place_id":       "131072",
#         #     "city_name":      "Rio de Janeiro",
#         #     "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/105955/top-things-to-do-and-attractions-in-rio-de-janeiro"
#         # },
#         # {
#         #     "place_id":       "9621",
#         #     "city_name":      "Madrid",
#         #     "geoCategoryUrl": "https://wanderlog.com/list/geoCategory/104649/top-things-to-do-and-attractions-in-madrid"
#         # },
#     ]

#     OUTPUT_DIR = "wanderlog_attractions_full"

#     custom_settings = {
#         "DOWNLOADER_MIDDLEWARES": {
#             "scrapy_selenium.SeleniumMiddleware": 800,
#         },
#         "LOG_LEVEL": "INFO",
#     }

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         # ensure output folder exists
#         os.makedirs(self.OUTPUT_DIR, exist_ok=True)

#         # init headless Chrome once
#         opts = Options()
#         opts.add_argument("--headless")
#         service = Service(ChromeDriverManager().install())
#         self.driver = webdriver.Chrome(service=service, options=opts)
#         self.logger.info("✅ Selenium driver ready")

#     def start_requests(self):
#         for inp in self.INPUTS:
#             yield SeleniumRequest(
#                 url=inp["geoCategoryUrl"],
#                 callback=self.parse,
#                 meta={"input": inp},
#                 wait_time=7,
#                 script="window.scrollTo(0, document.body.scrollHeight);"
#             )

#     def parse(self, response):
#         inp = response.meta["input"]
#         city = inp["city_name"]
#         self.logger.info(f"🗺  Scraping {city}")

#         # grab MobX JSON blob
#         raw = response.xpath(
#             '//script[contains(text(),"window.__MOBX_STATE__")]/text()'
#         ).get()
#         if not raw:
#             self.logger.error(f"No MobX state for {city}")
#             return

#         m = re.search(r"window\.__MOBX_STATE__\s*=\s*({.*?});", raw)
#         if not m:
#             self.logger.error(f"Could not parse MobX JSON for {city}")
#             return

#         data     = json.loads(m.group(1))
#         page     = data.get("placesListPage", {}).get("data", {})
#         metadata = page.get("placeMetadata", {})

#         # build list of items
#         items = []
#         for idx, (pid, place) in enumerate(metadata.items(), start=1):
#             items.append({
#                 "placeId":            place.get("placeId"),
#                 "website":            place.get("website"),
#                 "index":              idx,
#                 "utcOffset":          place.get("utcOffset"),
#                 "longitude":          place.get("geo", {}).get("longitude"),
#                 "latitude":           place.get("geo", {}).get("latitude"),
#                 "rating":             place.get("rating"),
#                 "excerpt":            place.get("description"),
#                 "detail_description": place.get("generatedDescription"),
#                 "imageKeys":          place.get("imageKeys"),
#                 "ratingCount":        place.get("numRatings"),
#                 "openingPeriods":     place.get("openingPeriods"),
#                 "name":               place.get("name"),
#                 "permanentlyClosed":  place.get("permanentlyClosed"),
#                 "priceLevel":         place.get("priceLevel"),
#                 "phone":              place.get("internationalPhoneNumber"),
#                 "address":            place.get("address"),
#                 "types":              place.get("categories"),
#                 "ratingDistribution": place.get("ratingDistribution"),
#             })

#         # slugify city name for filename
#         slug = re.sub(r'\W+', '_', city.lower()).strip('_')
#         path = f"{self.OUTPUT_DIR}/{slug}_attractions_full.json"

#         # dump JSON array
#         with open(path, 'w', encoding='utf-8') as f:
#             json.dump(items, f, ensure_ascii=False, indent=2)

#         self.logger.info(f"✅ Wrote {len(items)} items to {path}")

#     def closed(self, reason):
#         self.driver.quit()
#         self.logger.info("🔒 Selenium driver closed")
