from kafka import KafkaProducer
import json
import numpy as np
import time
import requests

p = KafkaProducer(bootstrap_servers=['localhost:9092'])

url="https://api.binance.com/api/v3/ticker/bookTicker"
payload = {'symbol':'BTCUSDT'}

def make_request(url,payload):
        r=requests.get(url,payload)
        print(r.url)
        if r.ok:
            print(f'statuts ok {r.status_code}')
            return r.json()
        else:
            print(f'erreur status {r.status_code}')
i = 0
while True:
    data=make_request(url,payload)
    p.send('topic1', json.dumps(data).encode('utf-8'))
    p.flush()
    i += 1
    time.sleep(2)
