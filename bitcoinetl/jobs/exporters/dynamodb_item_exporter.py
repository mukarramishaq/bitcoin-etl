import boto3
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
import urllib.parse as urlparse
from urllib.parse import parse_qs
import sys
class DynamoDBItemExporter:
    def __init__(self, uri="dynamodb://example.com/?chunkSize=10"):
        self.db = boto3.resource('dynamodb')
        parsed = urlparse.urlparse(uri)
        self.chunkSize = int(float(parse_qs(parsed.query)['chunkSize'].pop() or "10"))
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
            chunks = list(self.chunks(item_exporter.get_items(item_type), self.chunkSize))
            for chunk in chunks:
                with self.db.Table(self.item_2_collection[item_type]).batch_writer(overwrite_by_pkeys=self.item_2_collection_keys[item_type]) as batch:
                    for item in chunk:
                        print(self.get_size(item)/1024)
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
    
    def get_size(self, obj, seen=None):
        """Recursively finds size of objects"""
        size = sys.getsizeof(obj)
        if seen is None:
            seen = set()
        obj_id = id(obj)
        if obj_id in seen:
            return 0
        # Important mark as seen *before* entering recursion to gracefully handle
        # self-referential objects
        seen.add(obj_id)
        if isinstance(obj, dict):
            size += sum([self.get_size(v, seen) for v in obj.values()])
            size += sum([self.get_size(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += self.get_size(obj.__dict__, seen)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self.get_size(i, seen) for i in obj])
        return size