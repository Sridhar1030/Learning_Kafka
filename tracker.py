from confluent_kafka import Consumer
import json

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

consumer.subscribe(['orders'])

print("consumer is running and waiting for messages in 'orders'...")


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        value = msg.value().decode('utf-8')
        order = json.loads(value)
        print(f"Received order: {order['quantity']} unit(s) of {order['product']} by user {order['user']}")

except KeyboardInterrupt:
    print("Consumer interrupted")
    
finally:
    consumer.close()