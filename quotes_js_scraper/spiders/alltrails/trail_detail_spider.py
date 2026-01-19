import scrapy
import json
import os

class AllTrailsSpider(scrapy.Spider):
    name = 'alltrails_detail_spider'

    def __init__(self):
        # Store trail data
        self.trail_data_list = []
        self.output_file = os.path.join(os.getcwd(), 'alltrails_trail_data.json')

    def start_requests(self):
        # Using ScrapeOps proxy to fetch the trail data
        target_url = 'https://www.alltrails.com/trail/belgium/west-flanders/avelgem'
        scrapeops_url = f'https://proxy.scrapeops.io/v1/?api_key=076c4b91-a0a2-4dd6-904a-51f59647e4c7&url={target_url}&render_js=true'

        yield scrapy.Request(url=scrapeops_url, callback=self.parse_trail)

    def parse_trail(self, response):
        # Extract the JSON data from the script tag with id '__NEXT_DATA__'
        script_text = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()

        if script_text:
            try:
                data = json.loads(script_text)
                trail_details = data.get('props', {}).get('pageProps', {}).get('trailDetails', {})
                
                if trail_details:
                    # Extract trail data and append to the list
                    trail_data = self.extract_trail_data(trail_details)
                    self.trail_data_list.append(trail_data)
                    self.logger.info(f"Extracted trail data for trailId: {trail_data.get('id')}")
                else:
                    self.logger.warning("Trail details not found in the JSON data.")
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decoding error: {e}")
        else:
            self.logger.warning("No JSON script tag found on the trail page.")

    def extract_trail_data(self, trail_details):
        """Extract relevant trail details"""
        return {
            'id': str(trail_details.get('id')),
            'name': trail_details.get('name'),
            'overview': trail_details.get('overview'),
            'routeType': trail_details.get('routeType', {}).get('name'),
            'popularity': trail_details.get('popularity'),
            'location': trail_details.get('location', {}),
            'attributes': trail_details.get('attributes', {}),
            'defaultActivityStats': trail_details.get('defaultActivityStats', {}),
            'groupedTags': trail_details.get('groupedTags', []),
            'trailGeoStats': trail_details.get('trailGeoStats', {}),
            'defaultPhoto': trail_details.get('defaultPhoto', {}),
            'avgRating': trail_details.get('avgRating'),
            'ratingsBreakdown': trail_details.get('ratingsBreakdown', {}),
            'source': trail_details.get('source'),
            'trailCounts': trail_details.get('trailCounts', {}),
            'description': trail_details.get('description'),
            'description_html': trail_details.get('description_html'),
            'metadata': trail_details.get('metadata', {}),
            'alerts': trail_details.get('alerts', []),
        }

    def close(self, reason):
        """Writes all collected data to a JSON file when the spider finishes."""
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(self.trail_data_list, f, ensure_ascii=False, indent=4)
        self.logger.info(f"Saved {len(self.trail_data_list)} trails to {self.output_file}")
