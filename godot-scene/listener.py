import pika
import random
import json

# RabbitMQ connection details
RABBITMQ_HOST = "localhost"  # Change if RabbitMQ is on a different server
RABBITMQ_USER = "incubator"  # Username from your .NET script
RABBITMQ_PASSWORD = "incubator"  # Password from your .NET script
EXCHANGE_NAME = "PropagationProject"
ROUTING_KEY_CLICKED = "propagation.clicked.building"  # Must match the routing key from Godot
ROUTING_KEY_AFFECTED = "propagation.affected.buildings"  # The new routing key for sending affected buildings
QUEUE_NAME = "python_consumer_queue"

def callback(ch, method, properties, body):
    print(f"📩 Received message: {body.decode()}")
    
    # Generate a list of 10 random integers between 1 and 100
    random_values = [random.randint(1, 100) for _ in range(10)]
    print(f"🔢 Generated list of random values: {random_values}")
    
    # Send this list to the 'propagation.affected.buildings' routing key
    send_affected_buildings(random_values + [int(body.decode())])

def send_affected_buildings(data):
    # Convert the list of integers to JSON format
    message = json.dumps(data)
    
    # Publish the message to 'propagation.affected.buildings'
    channel.basic_publish(exchange=EXCHANGE_NAME,
                          routing_key=ROUTING_KEY_AFFECTED,
                          body=message)
    print(f"📤 Sent list of affected buildings: {data}")

# Setup connection with authentication
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Declare exchange and queue for receiving messages
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=False)
channel.queue_declare(queue=QUEUE_NAME, durable=False)  # Keep messages if RabbitMQ restarts
channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY_CLICKED)

print("🔄 Waiting for 'propagation.clicked.building' messages... Press CTRL+C to exit.")

# Start consuming
channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
channel.start_consuming()
