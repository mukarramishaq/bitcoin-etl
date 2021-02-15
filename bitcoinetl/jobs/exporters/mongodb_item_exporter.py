import json
from pymongo import MongoClient
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
class MongoDBItemExporter:
    def __init__(self, mongoURI="mongodb://localhost:27017/bitcoinetl"):
        self.client = MongoClient(mongoURI)
        self.db = self.client.get_database()
        self.item_types = ["block", "transaction"]
        self.item_2_collection = {
            "block": "blocks",
            "transaction": "transactions"
        }
    def open(self):
        pass

    def export_items(self, items):
        item_exporter = InMemoryItemExporter(item_types=['block', 'transaction'])
        item_exporter.open()
        for item in items:
            item_exporter.export_item(item)
        for item_type in self.item_types:
            self.db[self.item_2_collection[item_type]].insert_many(item_exporter.get_items(item_type))

    def export_item(self, item):
        item_type = item.get("type", None)
        if item_type is None:
            raise ValueError('type key is not found in item {}'.format(repr(item)))
        self.db[item_type].insert_one(item)


    def close(self):
        pass