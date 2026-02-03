import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", port=5672))

channel = connection.channel()

channel.exchange_declare(exchange='order_events_fanout', exchange_type='fanout', durable=True)

for i in range(1, 20):
    message = f"Order number {i} created."
    channel.basic_publish(
        exchange='order_events_fanout',
        routing_key='',
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)
    )
    print("Event sent:", message)
    time.sleep(0.5)
    

connection.close()