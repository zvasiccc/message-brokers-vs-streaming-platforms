import pika
import time
import random

def process_order(ch, method, properties, body):
    order = body.decode()
    print(f"Consumer2 processing {order}")

    time.sleep(random.randint(5,7))

    print(f"Consumer2 finished {order}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost", port=5672)
)
channel = connection.channel()

channel.queue_declare(queue='orders_queue', durable=True)

channel.basic_qos(prefetch_count=1)

channel.basic_consume(
    queue='orders_queue',
    on_message_callback=process_order
)

channel.start_consuming()
