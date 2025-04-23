import scrapy
import json
import urllib.parse
from scrapy.exceptions import CloseSpider
import logging
from pathlib import Path
from fake_useragent import UserAgent
ua=UserAgent()

class NominatimSpider(scrapy.Spider):
    name = 'nominatim'
    allowed_domains = ['nominatim.openstreetmap.org']
    
   
    # Custom settings for this spider
    custom_settings = {
        'LOG_FILE': 'nominatim_logger.log',  # Log file location
        'LOG_LEVEL': 'INFO',                 # Log level
        'DOWNLOAD_DELAY': 1,                 # Respect Nominatim usage policy (1 req/sec)
        'USER_AGENT':ua.edge ,
        # Explicitly enable the pipeline in the spider's custom settings
        'ITEM_PIPELINES': {
            'nominatim_scraper.pipelines.NominatimScraperPipeline': 300,
        },
    }
    
    
    def __init__(self, address_file=None, proxy=None, *args, **kwargs):
        super(NominatimSpider, self).__init__(*args, **kwargs)
        self.proxy = proxy
        
        # If a proxy is specified, configure it
        if proxy:
            self.logger.info(f"Using proxy")
            self.custom_settings['DOWNLOADER_MIDDLEWARES'] = {
                'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
            }
            self.custom_settings['HTTPPROXY_ENABLED'] = True
            self.custom_settings['HTTP_PROXY'] = proxy
            self.custom_settings['HTTPS_PROXY'] = proxy
        
        # Check if address file exists and load addresses
        self.addresses = []
        if address_file:
            try:
                with open(address_file, 'r') as f:
                    self.addresses = [line.strip() for line in f if line.strip()]
                self.logger.info(f"Loaded {len(self.addresses)} addresses from {address_file}")
            except Exception as e:
                self.logger.error(f"Error loading address file: {e}")
                raise CloseSpider(f"Could not load addresses from {address_file}")
        else:
            self.logger.error("No address file specified")
            raise CloseSpider("No address file specified")
    
    def start_requests(self):
        if not self.addresses:
            self.logger.error("No addresses to process")
            return
            
        for address in self.addresses:
            # URL encode the address
            encoded_address = urllib.parse.quote(address)
            url = f"https://nominatim.openstreetmap.org/search?q={encoded_address}&format=json&addressdetails=1&limit=1"
            

            # Create request meta with the address
            meta = {'address': address}
            
            # Add proxy to meta if specified (this is the key fix)
            if self.proxy:
                meta['proxy'] = self.proxy
            
            
            # Add our address as metadata so we can reference it in the callback
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={'address': address}
            )
    
    def parse(self, response):
        original_address = response.meta.get('address', 'Unknown')
        
        try:
            # Parse JSON response
            data = json.loads(response.text)
            
            # Log info about the response
            if data:
                self.logger.info(f"Found data for address: {original_address}")
            else:
                self.logger.warning(f"No data found for address: {original_address}")
            
            # Return the item with both original address and response data
            yield {
                'input_address': original_address,
                'geocode_data': data
            }
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing JSON for {original_address}: {e}")
            yield {
                'input_address': original_address,
                'error': f"JSON parsing error: {str(e)}",
                'raw_response': response.text[:200]  # First 200 chars for debugging
            }


