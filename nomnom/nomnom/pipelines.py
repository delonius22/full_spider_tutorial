# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json
from pathlib import Path


class NomnomPipeline:
    def open_spider(self, spider):
        Path("results").mkdir(exist_ok=True)
        self.file = open('results/geocode_results.json', 'w')
        self.file.write('[')
        self.first_item = True
        
    def close_spider(self, spider):
        self.file.write(']')
        self.file.close()
        
    def process_item(self, item, spider):
        line = json.dumps(item, ensure_ascii=False)
        if not self.first_item:
            self.file.write(',\n')
        else:
            self.first_item = False
        self.file.write(line)
        return item
