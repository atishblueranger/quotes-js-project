import scrapy
import json

class MultiAdventurousRoadTripsSpider(scrapy.Spider):
    name = 'multi_adventurous_road_trips'
    allowed_domains = ['lbb.in', 'www.lbb.in']

    # ← put as many collection pages here as you like
    start_urls = [
        'https://lbb.in/delhi/weekend-getaways-gurgaon/',
        'https://lbb.in/delhi/budget-road-trips-from-delhi/',
        'https://lbb.in/delhi/monsoon-getaways-near-delhi/',
        'https://lbb.in/delhi/holi-weekend-getaway/',
        # 'https://www.lbb.in/delhi/adventurous-road-trips-autumn-october/',
        # 'https://www.lbb.in/delhi/monsoon-getaways-near-delhi/',
        # 'https://www.lbb.in/delhi/holi-weekend-getaway/',
        # …etc…
    ]

    def parse(self, response):
        # 1) Grab the collection title (unique per page)
        title = (
            response
            .css('h1.styled-components__StyledMainTitle-sc-w4s7c5-12.eBLcIr span::text')
            .get(default='')
            .strip()
        )

        # 2) Pull down the JSON blob
        raw = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        data = json.loads(raw)

        # 3) Dive into discoverySSR.children
        children = (
            data.get('props', {})
                .get('pageProps', {})
                .get('discoverySSR', {})
                .get('children', [])
        )

        # 4) Normalize every place under that collection
        places = []
        for node in children:
            if node.get('type') == 'Place':
                places.append(self._normalize_place(node, desc_override=None))
            elif node.get('type') == 'Article':
                # Article → has its own 'sections'[0] as the description override
                sec = node.get('sections') or []
                override = sec[0].get('content') if sec else None
                for p in node.get('places', []):
                    places.append(self._normalize_place(p, desc_override=override))

        # 5) Yield one item per page
        yield {
            "url": response.url,
            "collection_title": title,
            "places": places
        }

    def _normalize_place(self, item, desc_override=None):
        name = item.get('title') or item.get('name') or ''

        # budget as a nested dict
        budget = item.get('budget', {})

        # coordinates
        loc    = item.get('location') or {}
        coords = loc.get('coordinates') or {}
        lat    = coords.get('lat')
        lng    = coords.get('lng') or coords.get('lon')

        # pick up the first non-empty place‐level section
        place_desc = None
        for s in item.get('sections', []):
            if s.get('content'):
                place_desc = s['content']
                break

        return {
            "name":               name,
            "budget":             budget,
            "coordinates":        {"lat": lat, "lng": lng},
            "generalDescription": desc_override or place_desc
        }



# import scrapy
# import json

# class AdventurousRoadTripsSpider(scrapy.Spider):
#     name = 'adventurous_road_trips'
#     allowed_domains = ['lbb.in', 'www.lbb.in']
#     start_urls = ['https://www.lbb.in/delhi/adventurous-road-trips-autumn-october/']
#     def parse(self, response):
#         # 1) Collection title
#         collection_title = (
#             response
#             .css('h1.styled-components__StyledMainTitle-sc-w4s7c5-12.eBLcIr span::text')
#             .get(default='')
#             .strip()
#         )

#         # 2) Next.js JSON blob
#         raw = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
#         data = json.loads(raw)

#         # 3) discoverySSR.children
#         children = (
#             data.get('props', {})
#                 .get('pageProps', {})
#                 .get('discoverySSR', {})
#                 .get('children', [])
#         )

#         # 4) Collect places
#         places = []
#         for node in children:
#             if node.get('type') == 'Place':
#                 # Place nodes sometimes carry their own sections
#                 places.append(self._normalize_place(node, desc_override=None))
#             elif node.get('type') == 'Article':
#                 # Article nodes wrap places but hold the true 'sections' for description
#                 # get article-level description
#                 art_sec = node.get('sections') or []
#                 desc_override = art_sec[0].get('content') if art_sec else None

#                 for p in node.get('places', []):
#                     places.append(self._normalize_place(p, desc_override=desc_override))

#         yield {
#             "collection_title": collection_title,
#             "places": places
#         }

#     def _normalize_place(self, item, desc_override=None):
#         # 1) name
#         name = item.get('title') or item.get('name') or ''

#         # 2) budget dict
#         budget = item.get('budget', {})

#         # 3) coordinates nested
#         loc    = item.get('location', {}) or {}
#         coords = loc.get('coordinates', {}) or {}
#         lat    = coords.get('lat')
#         lng    = coords.get('lng') or coords.get('lon')

#         # 4) place-level description
#         place_desc = None
#         for sec in item.get('sections', []):
#             if sec.get('content'):
#                 place_desc = sec['content']
#                 break

#         # 5) choose override first, then place_desc
#         description = desc_override or place_desc

#         return {
#             "name":               name,
#             "budget":             budget,
#             "coordinates":        {"lat": lat, "lng": lng},
#             "generalDescription": description
#         }



