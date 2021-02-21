from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from bitcoinetl.jobs.exporters.mongodb_item_exporter import MongoDBItemExporter
from bitcoinetl.jobs.exporters.dynamodb_item_exporter import DynamoDBItemExporter

def get_item_exporter(output):
    if output.startswith("dynamodb"):
        item_exporter = DynamoDBItemExporter(output)
    elif output.startswith("mongodb://"):
        item_exporter = MongoDBItemExporter(output)
    elif output is not None:
        from blockchainetl.jobs.exporters.google_pubsub_item_exporter import GooglePubSubItemExporter
        item_exporter = GooglePubSubItemExporter(
            item_type_to_topic_mapping={
                'block': output + '.blocks',
                'transaction': output + '.transactions'
            },
            message_attributes=('item_id',))
    else:
        item_exporter = ConsoleItemExporter()

    return item_exporter
