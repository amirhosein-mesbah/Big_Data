import pandas as pd
from kafka import KafkaProducer
import json
import time


def json_serializer(data):
    return json.dumps(data).encode('utf-8')
    


def write():
    producer = KafkaProducer(bootstrap_servers=['kafka:29092'],
			  api_version=(0,11,5),
                       value_serializer=json_serializer)
    df = pd.read_csv("./dags/data/final_news_df.csv")
    for i in range(df.shape[0]):
        news_data = df.loc[i,:].to_dict()
        producer.send('news_channel', news_data)
        time.sleep(0.5)
