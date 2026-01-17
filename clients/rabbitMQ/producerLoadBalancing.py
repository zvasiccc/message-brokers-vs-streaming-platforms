import pika
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost", port=5672)
)
channel = connection.channel()

# Jedan queue za sve zadatke
channel.queue_declare(queue='orders_queue', durable=True)

for i in range(1, 20):
    message = f"process_order_{i}"
    channel.basic_publish(
        exchange='',
        routing_key='orders_queue',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2
        )
    )
    print("Sent:", message)
    time.sleep(0.5)

connection.close()
