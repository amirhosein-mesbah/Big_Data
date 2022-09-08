from __future__ import unicode_literals
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from elasticsearch import Elasticsearch
import re
import string
from datetime import date, datetime
from khayyam import *
from hazm import *


def json_serializer(data):
    return dumps(data).encode('utf-8')


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         api_version=(0,11,5),
                       value_serializer=json_serializer)

producer_stats = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         api_version=(0,11,5),
                       value_serializer=json_serializer)

consumer = KafkaConsumer(
    'news_channel',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset= 'earliest',
     enable_auto_commit=True,
     auto_commit_interval_ms = 1000,
     group_id='news_ch',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     api_version=(0,10))

counter = 1

f = open('stopwords.txt', 'r', encoding="utf8")
stop_words = f.read()
stop_words = stop_words.splitlines()
f.close()

for message in consumer:
    
    msg = message.value
    news_data = {k: msg[k] for k in list(msg.keys())}
    text = news_data['text']
    text = text.translate(str.maketrans('', '', string.punctuation + "؟!.،,?:؛»«")) # Remove punctuation
    normalizer = Normalizer() # use halfspaces
    text = normalizer.normalize(text)
    stemmer = Stemmer()
    text = stemmer.stem(text) # Make plurals singular
    lemmatizer = Lemmatizer()
    text = lemmatizer.lemmatize(text) # Verbs
    
    tokenizer = WordTokenizer(replace_links=True, replace_IDs=True, separate_emoji=True, replace_hashtags=False, replace_emails=True)
    tokens = word_tokenize(text) # Split words
    tokens = [t  for t in tokens if t not in stop_words and not re.search(r'[a-zA-Z\u06F0-\u06F90-9]', t)]
    text = " ".join(tokens)
    news_data['text'] = text
    
    producer.send('news__presist_channel', news_data)
    producer_stats.send('news_stats', news_data)
    print(counter)
    print("*************************")
    counter +=1

