from __future__ import unicode_literals
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from elasticsearch import Elasticsearch
import re
import string
from datetime import date, datetime
from khayyam import *
from hazm import *


consumer = KafkaConsumer(
    'news__presist_channel',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset= 'earliest', # 'earliest', # Start from last consumed, #'latest' start from last produce
     enable_auto_commit=True,
     auto_commit_interval_ms = 1000,
     group_id='news_ch_elastic',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     api_version=(0,10))



# Elastic search configuation

es = Elasticsearch([{"host": "localhost", "port": 9200}])

counter = 1

for message in consumer:
    
    msg = message.value
    #print(list(msg.keys()))
    news_data = {k: msg[k] for k in list(msg.keys())}
    tags =re.findall(r"'(.+?)'",news_data['tags'])

    for i in range(len(tags)):
    	tags[i] = tags[i].replace("\\n", "").strip()
   
    news_data['tags'] = tags
    text = news_data['text']
    
    
    tokenizer = WordTokenizer(replace_links=True, replace_IDs=True, separate_emoji=True, replace_hashtags=False, replace_emails=True)
    tokens = word_tokenize(text) # Split words
    tokens = [t  for t in tokens if t not in stop_words and not re.search(r'[a-zA-Z\u06F0-\u06F90-9]', t)]
    news_data['tokens'] = tokens
    
    hour =news_data['date'][-5:].split(":")[0]
    minute =news_data['date'][-5:].split(":")[1]
    day = re.search(r'\d+', news_data['date']).group()
    date_ = JalaliDatetime(1401, 4, int(day), int(hour), int(minute)).todatetime()

    news_data['date']= date_.strftime('%Y/%m/%d %H:%M')
    news_data['datetime'] = date_
    
    index=es.index(index="news", body=news_data)
    
    print(counter)
    print("*************************")
    counter +=1

