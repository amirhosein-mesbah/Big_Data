from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from datetime import date, datetime
from khayyam import *


def json_serializer(data):
    return dumps(data).encode('utf-8')


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         api_version=(0,11,5),
                         value_serializer=json_serializer)

producer_stats = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         api_version=(0,11,5),
                         value_serializer=json_serializer)


consumer = KafkaConsumer(
    'crypto_channel',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset= 'earliest', # 'earliest', # Start from last consumed, #'latest' start from last produce
     enable_auto_commit=True,
     auto_commit_interval_ms = 1000,
     group_id='crypto_ch',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     api_version=(0,10))


counter = 1
for message in consumer:
    
    msg = message.value
    crypto_data = {k: msg[k] for k in list(msg.keys())}
    
    
    producer.send('crypto__presist_channel', crypto_data)
    producer_stats.send('crypto_stats', crypto_data)
    print(counter)
    print("*************************")
    counter +=1
   
