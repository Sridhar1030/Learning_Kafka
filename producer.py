import json
import uuid
from confluent_kafka import Producer
producer_config ={
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        print(f"Message value: {msg.value().decode('utf-8')}") 
        



order = {
    'order_id': str(uuid.uuid4()),
    'user':"sri",
    'product': 'Laptop',
    'quantity': 2
}

json_str=json.dumps(order).encode('utf-8')

producer.produce(
    topic='orders',
    value=json_str,
    callback=delivery_report
)

producer.flush()