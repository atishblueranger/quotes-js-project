import scrapy
from quotes_js_scraper.items import QuoteItem
from scrapy_selenium import SeleniumRequest


class QuotesSpider(scrapy.Spider):
    name = 'quotes'
    # allowed_domains = ['quotes.toscrape.com']
    # start_urls = ['http://quotes.toscrape.com/']

    def start_requests(self):
        url='https://quotes.toscrape.com/js/'
        yield SeleniumRequest(url=url,callback=self.parse )

    def parse(self, response):
        quote_item=QuoteItem()
        for quote in response.css('div.quote'):
            quote_item['text']=quote.css('span.text::text').get()
            quote_item['author']=quote.css('small.author::text').get()
            quote_item['tags']=quote.css('div.tags a.tag::text').getall()
            yield quote_item
    # def parse(self, response):
    #     if 'screenshot' in response.meta:
    #         self.logger.info("Screenshot captured.")
    #         with open("screenshot.png", "wb") as f:
    #             f.write(response.meta['screenshot'])
    #     else:
    #         self.logger.error("No screenshot captured.")
