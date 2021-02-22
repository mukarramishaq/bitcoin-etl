import boto3
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter

class DynamoDBItemExporter:
    def __init__(self, uri="dynamodb"):
        self.db = boto3.resource('dynamodb')
        self.item_types = ["block", "transaction", "transaction_input", "transaction_output"]
        self.item_2_collection = {
            "block": "blocks",
            "transaction": "transactions",
            "transaction_input": "transaction_inputs",
            "transaction_output": "transaction_outputs"
        }
        self.item_2_collection_keys = {
            "block": ['hash', 'number'],
            "transaction": ['hash', 'block_number'],
            "transaction_output": ['key', 'index'],
            "transaction_input": ['key', 'index']
        }

    def open(self):
        pass

    def export_items(self, items):
        item_exporter = InMemoryItemExporter(
            item_types=['block', 'transaction', 'transaction_input', 'transaction_output'])
        item_exporter.open()
        for item in items:
            if item.get('type', None) == 'transaction':
                inputs = item.pop('inputs')
                inputs = self.enrich_inputs(inputs, item)
                for i in inputs:
                    item_exporter.export_item(i)

                outputs = item.pop('outputs')
                outputs = self.enrich_outputs(outputs, item)
                for o in outputs:
                    item_exporter.export_item(o)
                
            item_exporter.export_item(item)
        for item_type in self.item_types:
            with self.db.Table(self.item_2_collection[item_type]).batch_writer(overwrite_by_pkeys=self.item_2_collection_keys[item_type]) as batch:
                for item in item_exporter.get_items(item_type):
                    batch.put_item(item)
    
    def enrich_input(self, item, txItem, index):
        item["index"] = index
        item["transaction_hash"] = txItem.get("hash")
        item["block_number"] = txItem.get("block_number")
        item["key"] = str(item["block_number"])+"::"+item["transaction_hash"]+"::"+str(item["index"])
        item["type"] = "transaction_input"
        return item
    
    def enrich_inputs(self, items, txItem):
        enriched_inputs = []
        for idx, item in enumerate(items):
           enriched_inputs.append(self.enrich_input(item, txItem, idx))
        return enriched_inputs
    
    def enrich_output(self, item, txItem, index):
        item["index"] = index
        item["transaction_hash"] = txItem.get("hash")
        item["block_number"] = txItem.get("block_number")
        item["key"] = str(item["block_number"])+"::"+item["transaction_hash"]+"::"+str(item["index"])
        item["type"] = "transaction_output"
        return item
    
    def enrich_outputs(self, items, txItem):
        enriched_outputs = []
        for idx, item in enumerate(items):
           enriched_outputs.append(self.enrich_input(item, txItem, idx))
        return enriched_outputs

    def export_item(self, item):
        item_type = item.get("type", None)
        if item_type is None:
            raise ValueError(
                'type key is not found in item {}'.format(repr(item)))
        self.db.Table(self.item_2_collection[item_type]).put_item(Item=item)

    def close(self):
        pass

