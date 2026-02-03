import pika

def process_event(ch, method, properties, body):
    event = body.decode()
    print(f" Consumer3 is processing event: {event}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", port=5672))
channel = connection.channel()

channel.exchange_declare(exchange='order_events_fanout', exchange_type='fanout', durable=True)

channel.queue_declare(queue='email_notifications', durable=True)

channel.queue_bind(exchange='order_events_fanout', queue='email_notifications')

channel.basic_consume(
    queue='email_notifications',
    on_message_callback=process_event
)

channel.start_consuming()