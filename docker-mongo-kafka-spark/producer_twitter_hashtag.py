"""API ACCESS KEYS"""

access_token = "1012622045186871296-g1cxl2KjYsofTNnzf9cAMx1SxHVyQA"
access_token_secret = "29GDmqe5SWT1mWwOGMGfqo1AtpAgRtz5ArrKzdLDGabfG"
consumer_key = "fDLolIWAySzQBp6JxQh9z484e"
consumer_secret = "ajGDn5PMfEjrMXB8UkROHnVCCCeoxIYcHn4ZiD5NnL0mszRagE"


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092') #Same port as your Kafka server


topic_name = "topic_twitter"


class twitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth



class TwitterStreamer():

    """SET UP STREAMER"""
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS()
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter(track=["Bitcoin"], stall_warnings=True, languages= ["en"])


class ListenerTS(StreamListener):

    def on_data(self, raw_data):
            producer.send(topic_name, str.encode(raw_data))
            return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()