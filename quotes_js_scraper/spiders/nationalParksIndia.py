import scrapy
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import json
import time

class MakeMyTripNationalParksSpider(scrapy.Spider):
    name = 'makemytrip_national_parks'
    
    def start_requests(self):
        url = "https://www.makemytrip.com/tripideas/national-parks-in-india"
        
        # Using SeleniumRequest properly with wait_until
        yield SeleniumRequest(
            url=url,
            callback=self.parse,
            wait_time=10,
            wait_until=EC.presence_of_element_located((By.CSS_SELECTOR, "a.DestinationCard__Container-sc-11r6g4i-0"))
        )
    
    def parse(self, response):
        # The driver is automatically passed in response.meta when using SeleniumRequest
        driver = response.meta.get('driver')
        
        if not driver:
            self.log("Error: Selenium driver not found in response.meta")
            return
        
        # Wait to ensure elements are loaded
        time.sleep(2)
        
        # Scroll down to load all content
        self.scroll_down(driver)
        
        # Extract park cards using Selenium since we have JavaScript rendered content
        try:
            park_cards = driver.find_elements(By.CSS_SELECTOR, "a.DestinationCard__Container-sc-11r6g4i-0")
            self.log(f"Found {len(park_cards)} national park cards")
            
            park_data = []
            for card in park_cards:
                try:
                    # Extract the park name
                    park_name_element = card.find_element(By.CSS_SELECTOR, "h3.DestinationCard__Title-sc-11r6g4i-2")
                    full_text = park_name_element.text
                    
                    # Handle park name with rank number
                    try:
                        rank_elements = card.find_elements(By.CSS_SELECTOR, "span.DestinationCard__TitleRank-sc-11r6g4i-1")
                        if rank_elements:
                            # Remove the rank number from the park name
                            name_parts = full_text.split()
                            park_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else full_text
                        else:
                            park_name = full_text
                    except:
                        park_name = full_text
                    
                    # Get park URL
                    park_url = card.get_attribute('href')
                    
                    # Get description if available
                    try:
                        description = card.find_element(By.CSS_SELECTOR, "p.DestinationCard__Description-sc-11r6g4i-4").text
                    except:
                        description = ""
                    
                    # Get image URL if available
                    try:
                        image_url = card.find_element(By.CSS_SELECTOR, "img").get_attribute('src')
                    except:
                        image_url = ""
                    
                    park_data.append({
                        'park_name': park_name,
                        'park_url': park_url,
                        'description': description,
                        'image_url': image_url
                    })
                    
                except Exception as e:
                    self.log(f"Error extracting data from card: {str(e)}")
            
            # Save data to JSON file
            output_file = 'indian_national_parks2.json'
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(park_data, f, ensure_ascii=False, indent=4)
            
            self.log(f"Saved {len(park_data)} national parks to {output_file}")
            
            # Return park data
            for park in park_data:
                yield park
                
        except Exception as e:
            self.log(f"Error during parsing: {str(e)}")
    
    def scroll_down(self, driver):
        """Helper method to scroll down the page to load all elements"""
        try:
            # Initial scroll height
            last_height = driver.execute_script("return document.body.scrollHeight")
            
            for _ in range(3):  # Limit to 3 scroll attempts
                # Scroll down to bottom
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # Wait for content to load
                time.sleep(2)
                
                # Calculate new scroll height and compare with last scroll height
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        except Exception as e:
            self.log(f"Error during scrolling: {str(e)}")
# import scrapy
# import json
# import re

# class NationalParksSpider(scrapy.Spider):
#     name = "national_parks_india"
#     # Update allowed_domains to include both the target and the proxy domain.
#     allowed_domains = ["makemytrip.com", "proxy.scrapeops.io"]

#     def start_requests(self):
#         # Base URL for ScrapeOps proxy. Replace YOUR_API_KEY with your actual ScrapeOps API key.
#         base_url = (
#             "https://proxy.scrapeops.io/v1/?api_key=0dd51678-5065-4d91-87a4-51013e50a107&url={}&render_js=true"
#         )
#         target_url = "https://www.makemytrip.com/tripideas/national-parks-in-india"
#         scrapeops_url = base_url.format(target_url)
#         self.logger.info("Fetching URL via ScrapeOps proxy: %s", scrapeops_url)
#         yield scrapy.Request(url=scrapeops_url, callback=self.parse)

#     def parse(self, response):
#         # For debugging, save the fetched HTML into a file to inspect what is returned.
#         with open("debug_national_parks.html", "w", encoding="utf-8") as f:
#             f.write(response.text)
#         self.logger.info("Saved debug HTML file (debug_national_parks.html)")

#         parks = []

#         # Log the number of park card containers found.
#         park_cards = response.css("a.DestinationCard__Container-sc-11r6g4i-0.hwZpfY")
#         self.logger.info("Found %d park card elements", len(park_cards))
        
#         for park in park_cards:
#             # Extract the park name. Remove any ranking numbers if present.
#             name = park.xpath(
#                 ".//h3[contains(@class,'DestinationCard__Title-sc-11r6g4i-2')]/text()[normalize-space()]"
#             ).get()
#             if not name:
#                 raw_name = park.css("h3.DestinationCard__Title-sc-11r6g4i-2 ::text").getall()
#                 name = " ".join(raw_name).strip()
#                 name = re.sub(r'^\d+\s*', '', name)
            
#             # Extract the park description.
#             description = park.css("p.DestinationCard__Description-sc-11r6g4i-4.ddzHtq::text").get()
#             if description:
#                 description = re.sub(r'\s+', ' ', description).strip()

#             parks.append({
#                 "name": name,
#                 "description": description
#             })

#         # Save the extracted park data to a JSON file.
#         output_file = "national_parks_india.json"
#         with open(output_file, "w", encoding="utf-8") as f:
#             json.dump(parks, f, ensure_ascii=False, indent=4)

#         self.logger.info("Saved %d national parks to %s", len(parks), output_file)


# import scrapy
# from scrapy_selenium import SeleniumRequest
# import json
# import re
# from selenium.webdriver.support import expected_conditions as EC

# class NationalParksSpider(scrapy.Spider):
#     name = "national_parks_india"
#     allowed_domains = ["makemytrip.com"]

#     custom_settings = {
#         'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#         'DOWNLOAD_DELAY': 1,  # be kind to the server
#     }

#     def start_requests(self):
#         url = "https://www.makemytrip.com/tripideas/national-parks-in-india"
#         yield SeleniumRequest(
#         url=url,
#         callback=self.parse,
#         wait_time=10,  # Increase wait time if necessary
#         # wait_until=EC.presence_of_element_located(
#         #     (By.CSS_SELECTOR, "a.DestinationCard__Container-sc-11r6g4i-0.hwZpfY") # type: ignore
#         # ),
#         script="window.scrollTo(0, document.body.scrollHeight);"
#     )

#     def parse(self, response):
#             # For debugging: save the response body to verify its content
#         with open("debug_page.html", "w", encoding="utf-8") as f:
#             f.write(response.text)
#         parks = []
#         # Each national park card is contained in an <a> tag with these classes.
#         for park in response.css("a.DestinationCard__Container-sc-11r6g4i-0.hwZpfY"):
#             # --- Extract the park name ---
#             # The <h3> element contains a ranking <span> followed by the park name.
#             # First try with XPath to extract the text node after the span.
#             name = park.xpath(
#                 ".//h3[contains(@class,'DestinationCard__Title-sc-11r6g4i-2')]/text()[normalize-space()]"
#             ).get()
#             if not name:
#                 # Fallback: Join all text nodes inside the h3 and then remove leading numbers.
#                 raw_name = park.css("h3.DestinationCard__Title-sc-11r6g4i-2 ::text").getall()
#                 name = " ".join(raw_name).strip()
#                 name = re.sub(r'^\d+\s*', '', name)

#             # --- Extract the park description ---
#             description = park.css("p.DestinationCard__Description-sc-11r6g4i-4.ddzHtq::text").get()
#             if description:
#                 description = re.sub(r'\s+', ' ', description).strip()

#             parks.append({
#                 "name": name,
#                 "description": description
#             })

#         # Save the extracted data to a JSON file
#         output_file = "national_parks_india.json"
#         with open(output_file, "w", encoding="utf-8") as f:
#             json.dump(parks, f, ensure_ascii=False, indent=4)

#         self.log(f"Saved {len(parks)} national parks to {output_file}")


# import scrapy
# import json
# import re
# import os

# class NationalParksSpider(scrapy.Spider):
#     name = "national_parks_india"
#     allowed_domains = ["makemytrip.com"]
#     start_urls = ["https://www.makemytrip.com/tripideas/national-parks-in-india"]

#     # custom_settings = {
#     #     'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#     #     'DOWNLOAD_DELAY': 1,  # be kind to the server
#     # }

#     def parse(self, response):
#         parks = []
#         # Each national park is in an <a> element with the following classes.
#         for park in response.css("a.DestinationCard__Container-sc-11r6g4i-0.hwZpfY"):
#             # --- Extract the park name ---
#             # The <h3> contains a ranking <span> and then the name.
#             # Option 1: Use XPath to obtain the text node following the ranking span.
#             name = park.xpath(".//h3[contains(@class,'DestinationCard__Title-sc-11r6g4i-2')]/text()[normalize-space()]").get()
#             if not name:
#                 # Option 2 (fallback): Join all text nodes inside the h3 and remove any leading rank digits.
#                 raw_name = park.css("h3.DestinationCard__Title-sc-11r6g4i-2 ::text").getall()
#                 name = " ".join(raw_name).strip()
#                 name = re.sub(r'^\d+\s*', '', name)

#             # --- Extract the park description ---
#             description = park.css("p.DestinationCard__Description-sc-11r6g4i-4.ddzHtq::text").get()
#             if description:
#                 description = re.sub(r'\s+', ' ', description).strip()

#             # Build a dictionary for this park
#             parks.append({
#                 "name": name,
#                 "description": description
#             })

#         # Save the extracted data into a JSON file
#         output_file = "national_parks_india.json"
#         with open(output_file, "w", encoding="utf-8") as f:
#             json.dump(parks, f, ensure_ascii=False, indent=4)

#         self.log(f"Saved {len(parks)} national parks to {output_file}")
