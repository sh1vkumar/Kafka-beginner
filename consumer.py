from kafka import KafkaConsumer
import json

def safe_deserializer(x):
    try:
        return json.loads(x.decode('utf-8'))
    except Exception as e:
        print(f"❌ Deserialization error: {e}, raw data: {x}")
        return None

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='localhost:9092',
    group_id='test-group-1',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=safe_deserializer
)

print("Listening for messages...")

for message in consumer:
    if message.value is not None:
        print(f"✔ Received: {message.value}")