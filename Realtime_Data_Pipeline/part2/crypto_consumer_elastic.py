from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from elasticsearch import Elasticsearch
from datetime import date, datetime
from khayyam import *


consumer = KafkaConsumer(
    'crypto__presist_channel',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset= 'earliest', # 'earliest', # Start from last consumed, #'latest' start from last produce
     enable_auto_commit=True,
     auto_commit_interval_ms = 1000,
     group_id='crypto_ch_elastic',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     api_version=(0,10))


# Elastic search configuation

es = Elasticsearch([{"host": "localhost", "port": 9200}])

counter = 1
for message in consumer:
    
    msg = message.value
    #print(list(msg.keys()))
    crypto_data = {k: msg[k] for k in list(msg.keys())}
    
    price = crypto_data['p']
    crypto_data['p'] = float(price)
    
    date_per = crypto_data['datetime']
    time_ =date_per.split(" ")[1].split(":")
    hour, minute = int(time_[0]), int(time_[1])
    date_ = date_per.split(" ")[0].split("/")
    year,month,day = int(date_[0]), int(date_[1]), int(date_[2])
    date_ = JalaliDatetime(year, month, day, hour,minute).todatetime()
    
    crypto_data['datetime'] = date_
    crypto_data['date']= date_.strftime('%Y/%m/%d %H:%M')

    index=es.index(index="crypto", body=crypto_data)
    
    print(counter)
    print("*************************")
    counter +=1
ئ
   
