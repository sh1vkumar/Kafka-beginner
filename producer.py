from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

users = ['u1', 'u2', 'u3']
products = ['apple', 'banana', 'chair', 'headphones']

while True:
    data = {
        'user_id': random.choice(users),
        'product': random.choice(products),
        'price': round(random.uniform(5.0, 500.0), 2),
        'timestamp': time.time()
    }
    producer.send('test-topic', value=data)
    print(f"Produced: {data}")
    time.sleep(5)