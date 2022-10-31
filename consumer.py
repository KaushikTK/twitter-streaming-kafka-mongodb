from kafka import KafkaConsumer
import json
from bson import json_util
from pymongo import MongoClient

def create_mongodb_connection():
    client = MongoClient('mongodb://localhost:27017/')
    db = client.twitter_streaming
    collection = db.tweets
    return collection

def insert_into_mongodb(data):
    collection = create_mongodb_connection()
    collection.insert_one(data)

def main():
    topic = 'twitter-streaming'
    consumer_group = 'twitter_streaming_consumer_group'

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers = ["localhost:9092"],
        auto_offset_reset = 'earliest',
        group_id = consumer_group
    )

    for msg in consumer:
        doc = json.loads(msg.value, object_hook=json_util.object_hook)
        print('Inserting into database..')
        insert_into_mongodb(doc)

main()