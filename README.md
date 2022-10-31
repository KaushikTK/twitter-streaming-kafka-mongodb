# twitter-streaming-kafka-mongodb
 
This streams tweets from the twitter API v2 and pushes it into the kafka server. The kafka consumer pushes the messages from the kafka server to mongodb.


Create a virtual python enviroment by running `pip -m venv env` in cmd. Activate the environment and run `pip install -r requirements.txt` to install all the necessary python modules.


To start zookeeper, kafka and mongodb, open cmd and run `docker-compose up`.


Sign in to twitter developer website and create a new project and copy paste the credentials into the twitter-credentials.json file.


To start the producer, open cmd and run `py producer.py`


To start the consumer, open cmd and run `py consumer.py`