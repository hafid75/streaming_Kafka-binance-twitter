from kafka import KafkaConsumer
import pymongo
from pymongo import *
import json


x=MongoClient('mongo',27017,username='root',password='example')
x.start_session()
c=KafkaConsumer('topic1',bootstrap_servers=['localhost:9092'],api_version=(2,6,0))

def process_msg(msg):
    print(msg.offset)
    print(json.loads(msg.value))

for msg in c :
    x =MongoClient('mongo',27017,username='root',password='example')
    print(msg)
    msg=json.loads(msg.value)

    x["db_projet_kafka"]["tbl_btc"].insert_one(msg)
process_msg(msg)