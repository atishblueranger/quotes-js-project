# # settings.py

# import random


# BOT_NAME = 'quotes_js_scraper'

# SPIDER_MODULES = ['quotes_js_scraper.spiders']
# NEWSPIDER_MODULE = 'quotes_js_scraper.spiders'

# # Do not obey robots.txt
# ROBOTSTXT_OBEY = False

# # Set concurrent requests to 1 to reduce load and mimic human browsing
# CONCURRENT_REQUESTS = 1

# # ScrapeOps API key and proxy settings
# SCRAPEOPS_API_KEY = '39aec2e2-9f7b-4f01-b71f-e025cbfea6cc'  # Replace with your actual API key
# SCRAPEOPS_PROXY_ENABLED = True

# # Downloader middlewares
# DOWNLOADER_MIDDLEWARES = {
#     'scrapeops_scrapy_proxy_sdk.scrapeops_scrapy_proxy_sdk.ScrapeOpsScrapyProxySdk': 725,
#     # Remove or comment out CustomSeleniumMiddleware if not needed
#     # 'quotes_js_scraper.middlewares.CustomSeleniumMiddleware': 400,
# }

# # Optional: Set download delay to mimic human behavior
# # DOWNLOAD_DELAY = 1  # Delay in seconds
# DOWNLOAD_DELAY = random.uniform(1, 3)










# Scrapy settings for quotes_js_scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# 

# for Chrome driver 
# from shutil import which
  
# SELENIUM_DRIVER_NAME = 'chrome'
# SELENIUM_DRIVER_EXECUTABLE_PATH = 'C:\dev\python_runs\scrapy_selenium\quotes-js-project\chromedriver.exe'
# SELENIUM_DRIVER_ARGUMENTS=['--headless']  
  
# DOWNLOADER_MIDDLEWARES = {
#      'scrapy_selenium.SeleniumMiddleware': 800
#      }

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
    'scrapeops_scrapy_proxy_sdk.scrapeops_scrapy_proxy_sdk.ScrapeOpsScrapyProxySdk': 543,
    'quotes_js_scraper.middlewares.CustomSeleniumMiddleware': 400,
}






BOT_NAME = 'quotes_js_scraper'

SPIDER_MODULES = ['quotes_js_scraper.spiders']
NEWSPIDER_MODULE = 'quotes_js_scraper.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'quotes_js_scraper (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False


SCRAPEOPS_API_KEY = '0dd51678-5065-4d91-87a4-51013e50a107'
SCRAPEOPS_PROXY_ENABLED = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 1

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 1
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'quotes_js_scraper.middlewares.QuotesJsScraperSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'quotes_js_scraper.middlewares.QuotesJsScraperDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    'quotes_js_scraper.pipelines.QuotesJsScraperPipeline': 300,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
