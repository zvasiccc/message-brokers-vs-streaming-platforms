import pika

def process_event(ch, method, properties, body):
    event = body.decode()
    print(f" Consumer3 is processing event: {event}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", port=5672))
channel = connection.channel()


channel.queue_declare(queue='inApp_notifications', durable=True)

channel.queue_bind(exchange='order_events_fanout', queue='inApp_notifications')

channel.basic_consume(
    queue='inApp_notifications',
    on_message_callback=process_event
)

channel.start_consuming()