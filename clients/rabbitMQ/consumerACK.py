import pika
import time

def process_email(ch,method, properties, body):
    print(f"Consumer received task {body.decode()}")
    
    time.sleep(2)
    print(f"Consumer has sent email")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost", port=5672)
)
channel = connection.channel()

channel.queue_declare( queue='email_tasks', durable=True)

channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue='email_tasks', on_message_callback=process_email)

channel.start_consuming()