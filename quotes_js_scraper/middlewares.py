# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import undetected_chromedriver as uc
from scrapy_selenium import SeleniumMiddleware  # <--- ADD THIS LINE
from itemadapter import is_item, ItemAdapter

class QuotesJsScraperSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class QuotesJsScraperDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class CustomSeleniumMiddleware:
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        return s

    def process_request(self, request, spider):
        if request.meta.get('screenshot', False):
            spider.logger.info("Screenshot requested.")
            self.driver.get(request.url)
            screenshot = self.driver.get_screenshot_as_png()
            request.meta['screenshot'] = screenshot
            return HtmlResponse(url=request.url, body=self.driver.page_source, encoding='utf-8', request=request)
        else:
            self.driver.get(request.url)
            return HtmlResponse(url=request.url, body=self.driver.page_source, encoding='utf-8', request=request)


    def spider_closed(self, spider):
        self.driver.quit()


class Selenium4Middleware(SeleniumMiddleware):
    """
    Patched middleware using undetected-chromedriver to bypass Cloudflare
    """
    def __init__(self, driver_executable_path, driver_arguments):
        # 1. Configure Chrome Options
        options = uc.ChromeOptions()
        
        # Add your arguments from settings.py
        for argument in driver_arguments:
            options.add_argument(argument)

        # 2. Initialize the Undetected Driver
        # UC automatically handles the driver versioning and anti-bot patching.
        # We generally do NOT pass a service or executable_path to UC; it handles it best alone.
        # self.driver = uc.Chrome(
        #     options=options, 
        #     version_main=143  # <--- ADD THIS PARAMETER
        # )
        # Define the path to your manually downloaded driver
        driver_path = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\chromedriver.exe"

        # Initialize the driver with the local path
        self.driver = uc.Chrome(
            driver_executable_path=driver_path,
            version_main=143,  # This tells it to stop checking for updates
            options=options
        )

    @classmethod
    def from_crawler(cls, crawler):
        # We only need the arguments. UC handles the path logic itself.
        return cls(
            driver_executable_path=None, 
            driver_arguments=crawler.settings.get('SELENIUM_DRIVER_ARGUMENTS')
        )

    def spider_closed(self, spider):
        # Gracefully close the driver when spider finishes
        self.driver.quit()

# class Selenium4Middleware(SeleniumMiddleware):
#     """
#     Patched middleware: Auto-manages ChromeDriver version
#     """
#     def __init__(self, driver_executable_path, driver_arguments):
#         # 1. Setup Chrome Options
#         chrome_options = Options()
#         for argument in driver_arguments:
#             chrome_options.add_argument(argument)
        
#         # 2. Setup Service
#         # If a specific path is NOT provided in settings, use the Manager to auto-install
#         if not driver_executable_path:
#             print("Installing matching ChromeDriver...")
#             driver_path = ChromeDriverManager().install()
#             service = Service(executable_path=driver_path)
#         else:
#             service = Service(executable_path=driver_executable_path)

#         self.driver = webdriver.Chrome(service=service, options=chrome_options)

#     @classmethod
#     def from_crawler(cls, crawler):
#         return cls(
#             driver_executable_path=crawler.settings.get('SELENIUM_DRIVER_EXECUTABLE_PATH'),
#             driver_arguments=crawler.settings.get('SELENIUM_DRIVER_ARGUMENTS')
#         )