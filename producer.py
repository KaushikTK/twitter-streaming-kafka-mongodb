from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import json
from bson import json_util
from tweepy import StreamingClient


class Twitter_Streamer(StreamingClient):
    def on_connect(self): print('Connected to twitter API..')

    def initialise_kafka_producer(self, topic_name):
        self.producer = KafkaProducer(bootstrap_servers = ["localhost:9092"], value_serializer = json_serialiser)
        self.topic = topic_name

    def on_response(self,data):
        print('Pushing data into kafka server..')
        self.producer.send(self.topic, data)


# function to read twitter credentials
def read_twitter_credentials(file_path='./twitter-credentials.json'):
    credentials_file = open(file_path, 'r')
    return json.loads(credentials_file.read())


def create_topic(topic_name):
    # create a new topic if not present
    admin_client = KafkaAdminClient(bootstrap_servers = ["localhost:9092"])
    if topic_name in admin_client.list_topics(): return
    admin_client.create_topics(new_topics=[NewTopic(name="twitter-streaming", num_partitions=2, replication_factor=1)], validate_only=False)


def json_serialiser(obj):
    data = obj.data
    users = obj.includes
    doc = {
        'tweet_info':{'tweet': data.text},
        'users': [{
            'id': i.id,
            'username': i.username,
            'name': i.name
        } for i in users['users']]
    }

    return json.dumps(doc, default=json_util.default).encode('utf-8')


def start_producer(topic_name, bearer_token):
    streamer = Twitter_Streamer(bearer_token)
    streamer.initialise_kafka_producer(topic_name)
    streamer.sample(expansions=['author_id'],tweet_fields=["created_at"])



def main():
    topic = 'twitter-streaming'
    print('Creating topic..')
    create_topic(topic)
    twitter_cred = read_twitter_credentials()
    print('Starting producer..')
    start_producer(topic,twitter_cred['bearer token'])


main()