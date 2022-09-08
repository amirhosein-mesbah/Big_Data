from faker import Faker
from kafka import KafkaProducer
import json
fake = Faker()
import time

def get_registered_data():
    return {
        'first name': fake.first_name(),
        'last name': fake.last_name(),
        'age': fake.random_int(0, 60),
        'address': fake.address(),
        'register year': fake.year(),
        'register month': fake.month(),
        'register day': fake.day_of_month(),
        'monthly income ($NT)': fake.random_int(28000, 100000)
    }

def json_serializer(data):
    return json.dumps(data).encode('utf-8')
    
producer = KafkaProducer(bootstrap_servers=['kafka:29092'],
			  api_version=(0,11,5),
                         value_serializer=json_serializer)

if __name__ == '__main__':
    while True:
        registered_data = get_registered_data()
        print(registered_data)
        producer.send('registered_user', registered_data)
        time.sleep(3)
