import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", port=5672))

channel = connection.channel()

channel.exchange_declare(exchange='email_exchange', exchange_type='direct')

channel.queue_declare(queue='email_tasks', durable=True)

channel.queue_bind(exchange='email_exchange', queue='email_tasks', routing_key='welcome_email')
message = "send_welcome_email"

channel.basic_publish(
    exchange='email_exchange',
    routing_key='welcome_email',
    body=message,
    properties=pika.BasicProperties(delivery_mode=2)
)

print("Sent task:", message)

connection.close()