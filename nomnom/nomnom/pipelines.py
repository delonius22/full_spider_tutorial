# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json,os
from pathlib import Path
import logging


class NomnomPipeline:
    def __init__(self):
        self.file = None
        self.first_item = True
        self.logger = logging.getLogger(__name__)
    
    def open_spider(self, spider):
        # Create results directory if it doesn't exist
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        
        # Full path to output file
        self.file_path = results_dir / "geocode_results.json"
        
        try:
            # Open the file for writing
            self.file = open(self.file_path, 'w', encoding='utf-8')
            self.file.write('[\n')  # Start JSON array with newline for readability
            self.logger.info(f"Pipeline opened output file: {self.file_path}")
        except Exception as e:
            self.logger.error(f"Failed to open output file: {e}")
            # If we can't open the file, create a null file object
            # This prevents errors in close_spider and process_item
            self.file = type('NullFile', (), {'write': lambda *args: None, 'close': lambda: None})()
    
    def close_spider(self, spider):
        if self.file:
            try:
                self.file.write('\n]')  # End JSON array
                self.file.close()
                self.logger.info(f"Pipeline closed output file: {self.file_path}")
                
                # Log the file size to verify it was written
                file_size = os.path.getsize(self.file_path)
                self.logger.info(f"Output file size: {file_size} bytes")
                
                # Log the absolute path for clarity
                abs_path = os.path.abspath(self.file_path)
                self.logger.info(f"Output file absolute path: {abs_path}")
            except Exception as e:
                self.logger.error(f"Error closing output file: {e}")
    
    def process_item(self, item, spider):
        if self.file:
            try:
                # Convert item to JSON
                line = json.dumps(dict(item), ensure_ascii=False, indent=2)
                
                # Add comma if not the first item
                if not self.first_item:
                    self.file.write(',\n')
                else:
                    self.first_item = False
                
                # Write the item
                self.file.write(line)
                self.file.flush()  # Flush to make sure data is written
                
                # Log for debugging
                addr = item.get('input_address', 'unknown')
                self.logger.debug(f"Processed item for address: {addr}")
            except Exception as e:
                self.logger.error(f"Error processing item: {e}")
        
        return item
