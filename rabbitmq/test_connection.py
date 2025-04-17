import pika

# RabbitMQ connection details
RABBITMQ_HOST = "localhost"  # Use "rabbitmq" if testing from inside Docker
RABBITMQ_USER = "incubator"
RABBITMQ_PASSWORD = "incubator"

# Setup connection with authentication
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)

try:
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    print("✅ Connection to RabbitMQ successful!")
    connection.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")

