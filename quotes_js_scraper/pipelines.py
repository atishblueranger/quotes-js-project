import json

class CountryJsonWriterPipeline:
    def open_spider(self, spider):
        # country → file handle
        self.files       = {}
        # country → bool (first item?)
        self.first_item  = {}

    def process_item(self, item, spider):
        country = item["country"]
        # on first item for this country, open & write “[”
        if country not in self.files:
            f = open(f"{country}_attractions.json", "w", encoding="utf-8")
            f.write("[\n")
            self.files[country]      = f
            self.first_item[country] = True

        f = self.files[country]
        # comma‐separate items
        if not self.first_item[country]:
            f.write(",\n")
        # write the pretty JSON for this item
        f.write(json.dumps(item, ensure_ascii=False, indent=4))
        self.first_item[country] = False

        return item

    def close_spider(self, spider):
        # close each file by writing “]”
        for f in self.files.values():
            f.write("\n]")
            f.close()



# # Define your item pipelines here
# #
# # Don't forget to add your pipeline to the ITEM_PIPELINES setting
# # See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# # useful for handling different item types with a single interface
# from itemadapter import ItemAdapter


# class QuotesJsScraperPipeline:
#     def process_item(self, item, spider):
#         return item
