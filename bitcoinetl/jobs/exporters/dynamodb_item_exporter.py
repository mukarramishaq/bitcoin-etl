import boto3
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
import urllib.parse as urlparse
from urllib.parse import parse_qs
class DynamoDBItemExporter:
    def __init__(self, uri="dynamodb://example.com/?"):
        self.db = boto3.resource('dynamodb')
        parsed = urlparse.urlparse(uri)
        self.chunkSize = parse_qs(parsed.query)['chunkSize']
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
            chunks = list(self.chunks(item_exporter.get_items(item_type), self.chunkSize or 10))
            for chunk in chunks:
                with self.db.Table(self.item_2_collection[item_type]).batch_writer(overwrite_by_pkeys=self.item_2_collection_keys[item_type]) as batch:
                    for item in chunk:
                        batch.put_item(item)

    def export_item(self, item):
        item_type = item.get("type", None)
        if item_type is None:
            raise ValueError('type key is not found in item {}'.format(repr(item)))
        self.db.Table(self.item_2_collection[item_type]).put_item(item)


    def close(self):
        pass

    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]