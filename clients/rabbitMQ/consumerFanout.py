import pika
import time
import random

def process_order(ch, method, properties, body):
    order = body.decode()
    print(f"Consumer1 processing {order}")
    time.sleep(random.randint(1, 4))
    print(f"Consumer1 finished {order}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost", port=5672)
)
channel = connection.channel()

channel.exchange_declare(exchange='logs_fanout', exchange_type='fanout')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='logs_fanout', queue=queue_name)

channel.basic_qos(prefetch_count=1)

channel.basic_consume(
    queue=queue_name,
    on_message_callback=process_order
)

channel.start_consuming()