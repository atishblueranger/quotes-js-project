"""
TasteAtlas Dishes Spider
Scrapes popular dishes from TasteAtlas country/region pages.

Usage:
    scrapy crawl tasteatlas_dishes -a url="https://www.tasteatlas.com/india?ref=main-menu"
    scrapy crawl tasteatlas_dishes -a url="https://www.tasteatlas.com/japan?ref=main-menu" -a max_dishes=30
"""

import scrapy
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import json
import os
import re
import time


class TasteAtlasDishesSpider(scrapy.Spider):
    name = "tasteatlas_dishes"
    
    def __init__(self, url=None, max_dishes=None, output_dir="tasteatlas_output", *args, **kwargs):
        super(TasteAtlasDishesSpider, self).__init__(*args, **kwargs)
        
        self.dishes_data = []
        self.start_url = url or "https://www.tasteatlas.com/india?ref=main-menu"
        self.place_name = self._extract_place_name(self.start_url)
        self.max_dishes = int(max_dishes) if max_dishes else None
        self.output_dir = output_dir
        
        self.logger.info(f"Initialized scraper for: {self.place_name}")
        self.logger.info(f"Max dishes: {self.max_dishes or 'All'}")

    def _extract_place_name(self, url):
        """Extract place name from URL"""
        clean_url = url.split('?')[0].rstrip('/')
        place = clean_url.split('/')[-1]
        return place.lower().replace('-', '_')

    def start_requests(self):
        self.logger.info(f"Starting scrape: {self.start_url}")
        yield SeleniumRequest(
            url=self.start_url,
            callback=self.parse_dishes,
            wait_time=5,
            # script="window.scrollTo(0, document.body.scrollHeight / 2);"
        )

    def parse_dishes(self, response):
        driver = response.request.meta.get('driver')
        
        if driver:
            self._load_all_dishes(driver)
            from scrapy.http import HtmlResponse
            response = HtmlResponse(
                url=driver.current_url,
                body=driver.page_source,
                encoding='utf-8',
                request=response.request
            )
        
        dish_items = response.css('div.similar-list__item')
        self.logger.info(f"Found {len(dish_items)} dish items")
        
        for item in dish_items:
            dish = self._parse_dish_item(item, response)
            if dish:
                if self.max_dishes and len(self.dishes_data) >= self.max_dishes:
                    break
                self.dishes_data.append(dish)
                yield dish
        
        self.logger.info(f"Total dishes scraped: {len(self.dishes_data)}")

    def _load_all_dishes(self, driver):
        """Click 'View More' button until all dishes loaded"""
        click_count = 0
        max_clicks = 50
        
        while click_count < max_clicks:
            try:
                view_more_btn = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((
                        By.CSS_SELECTOR, 
                        'div.search-results__view-more button.btn--underscore'
                    ))
                )
                
                if not view_more_btn.is_displayed():
                    break
                
                driver.execute_script("arguments[0].scrollIntoView(true);", view_more_btn)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", view_more_btn)
                click_count += 1
                self.logger.info(f"Clicked 'View More' ({click_count})")
                time.sleep(1.5)
                
                current_dishes = driver.find_elements(By.CSS_SELECTOR, 'div.similar-list__item')
                self.logger.info(f"Current dish count: {len(current_dishes)}")
                
                if self.max_dishes and len(current_dishes) >= self.max_dishes:
                    break
                    
            except (TimeoutException, NoSuchElementException):
                self.logger.info("No more 'View More' button - all dishes loaded")
                break
            except Exception as e:
                self.logger.warning(f"Error clicking View More: {e}")
                break
        
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)

    def _parse_dish_item(self, item, response):
        """Parse single dish item"""
        try:
            # Rank
            rank_text = item.css('div.similar-list__item-position span::text').get()
            rank = int(rank_text.strip()) if rank_text else len(self.dishes_data) + 1
            
            # Name
            name = item.css('h2.h2 a::text').get()
            if not name:
                name = item.css('h2 a::text').get()
            name = name.strip() if name else None
            
            if not name:
                return None
            
            # ID
            dish_id = self._generate_id(name)
            
            # Category
            category = item.css('div.group a::text').get()
            category = category.strip().upper() if category else "DISH"
            
            # Image URL
            image_url = (
                item.css('div.search-results__item-image img::attr(lazy-source)').get() or
                item.css('div.search-results__item-image img::attr(src)').get() or
                item.css('img::attr(lazy-source)').get() or
                item.css('img::attr(src)').get() or
                ""
            )
            if image_url and not image_url.startswith('http'):
                image_url = 'https://cdn.tasteatlas.com' + image_url
            
            # Region
            region_name = item.css('a.item-location div::text').get()
            region_name = region_name.strip() if region_name else None
            
            # Description
            description = item.css('div.search-results__item-description p::text').get()
            if not description:
                description = item.css('div.search-results__item-description p').xpath('string()').get()
            description = self._clean_description(description) if description else ""
            
            # Most iconic place
            iconic_elem = item.css('div.similar-list__item-description')
            most_iconic_place = None
            most_iconic_location = None
            
            if iconic_elem:
                iconic_text = iconic_elem.css('a::text').get()
                if iconic_text:
                    most_iconic_place = iconic_text.strip()
                full_text = iconic_elem.xpath('string()').get()
                if full_text:
                    location_match = re.search(r'\(([^)]+)\)', full_text)
                    if location_match:
                        most_iconic_location = location_match.group(1).strip()
            
            # Ingredients
            ingredients = []
            for ing_item in item.css('li.mini-ingredients__item a')[:6]:
                ing_name = ing_item.css('::attr(title)').get()
                if ing_name:
                    ingredients.append(ing_name.strip())
            
            return {
                'id': dish_id,
                'name': name,
                'local_name': None,
                'category': category,
                'rank': rank,
                'short_description': description[:200] + '...' if len(description) > 200 else description,
                'image_url': image_url,
                'most_iconic_place': most_iconic_place,
                'most_iconic_location': most_iconic_location or region_name,
                'ingredients': ingredients,
                'is_active': True,
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing dish: {e}")
            return None

    def _generate_id(self, name):
        dish_id = name.lower().strip()
        dish_id = re.sub(r'[^\w\s-]', '', dish_id)
        dish_id = re.sub(r'[\s-]+', '_', dish_id)
        return dish_id.strip('_')

    def _clean_description(self, text):
        if not text:
            return ""
        text = re.sub(r'\s*Read more\s*$', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def close(self, reason):
        """Save to JSON on spider close"""
        os.makedirs(self.output_dir, exist_ok=True)
        filename = f"tasteatlas_{self.place_name}_dishes.json"
        output_file = os.path.join(self.output_dir, filename)
        
        final_data = [{k: v for k, v in d.items()} for d in self.dishes_data]
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"{'='*50}")
        self.logger.info(f"SCRAPING COMPLETE: {self.place_name}")
        self.logger.info(f"Total dishes: {len(final_data)}")
        self.logger.info(f"Output: {output_file}")
        self.logger.info(f"{'='*50}")