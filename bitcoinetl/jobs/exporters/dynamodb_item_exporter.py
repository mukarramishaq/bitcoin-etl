import boto3
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
class DynamoDBItemExporter:
    def __init__(self, mongoURI="mongodb://localhost:27017/bitcoinetl"):
        self.db = boto3.resource('dynamodb')
        self.item_types = ["block", "transaction"]
        self.item_2_collection = {
            "block": "blocks",
            "transaction": "transactions"
        }
        self.item_2_collection_keys = {
            "block": ['hash', 'number'],
            "transaction": ['hash', 'block_number']
        }
    def open(self):
        pass

    def export_items(self, items):
        item_exporter = InMemoryItemExporter(item_types=['block', 'transaction'])
        item_exporter.open()
        for item in items:
            item_exporter.export_item(item)
        for item_type in self.item_types:
            with self.db.Table(self.item_2_collection[item_type]).batch_writer(overwrite_by_pkeys=self.item_2_collection_keys[item_type]) as batch:
                for item in item_exporter.get_items(item_type):
                    batch.put_item(item)

    def export_item(self, item):
        item_type = item.get("type", None)
        if item_type is None:
            raise ValueError('type key is not found in item {}'.format(repr(item)))
        self.db.Table(self.item_2_collection[item_type]).put_item(item)


    def close(self):
        pass