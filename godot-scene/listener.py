import pika

def callback(ch, method, properties, body):
    print(f"🏢 Building Clicked: {body.decode()}")

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(exchange="DTProject", exchange_type="direct")
queue = channel.queue_declare(queue="", exclusive=True).method.queue
channel.queue_bind(exchange="DTProject", queue=queue, routing_key="building.clicked")

channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
print("✅ Listening for building clicks...")
channel.start_consuming()
