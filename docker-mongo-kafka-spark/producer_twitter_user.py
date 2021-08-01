from kafka import KafkaProducer
import json
import numpy as np
import time
import requests
import tweepy

p = KafkaProducer(bootstrap_servers=['localhost:9092'])


consumer_key = 'XXXXXXX'
consumer_secret = 'XXXXXX'
access_token = 'XXXXXX'
access_token_secret = 'XXXXXX'

authentification = tweepy.OAuthHandler(consumer_key, consumer_secret)
authentification.set_access_token(access_token, access_token_secret)
api = tweepy.API(authentification)
# user=tweepy.API.me(api)
#user = api.get_user('elonmusk')
#for tweet in tweepy.Cursor(api.search,q='#Bitcoin',tpp=100).(MAX_TWEETS):
# print ("User id: " + user.id_str)
# print (user.name)
# print ("Description: " + user.description)
# print ("Language: " + user.lang)
# print ("Account created at: " + str(user.created_at))
# print ("Location: " + user.location)
# print ("Time zone: " + user.time_zone)
# print ("Number of tweets: " + str(user.statuses_count))
# print ("Number of followers: " + str(user.followers_count))
# print ("Following: " + str(user.friends_count))
# print ("A member of " + str(user.listed_count) + " lists.")


#statuses = api.user_timeline(id=user.id, count=200)

for status in tweepy.Cursor(api.search,q='#Bitcoin',rpp=100).items():
    # if statuses.coordinates:

    # print ("***")
    print("Tweet id: " + status.id_str)
    print("user: " + status.user.screen_name)
    print(status.text)
    data={"tweet_id":status.id_str,"user_name":status.user.screen_name,"text":status.text}
    time.sleep(1)
    # print ("Retweet count: " + str(status.retweet_count))
    # print ("Favorite count: " + str(status.favorite_count))
    # print (status.created_at)
    # print ("Status place: " + str(status.place))
    # print ("Source: " + status.source)
    # print ("Coordinates: " + str(status.coordinates))
    # time.sleep(1)
    p.send('topic_twitter2', json.dumps(data).encode('utf-8'))
    p.flush()


