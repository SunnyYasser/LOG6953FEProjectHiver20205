import pika
import json
import os
import subprocess
import time
from threading import Thread

# RabbitMQ configuration
RABBITMQ_HOST = "localhost"
RABBITMQ_USER = "incubator"
RABBITMQ_PASSWORD = "incubator"
EXCHANGE_NAME = "PropagationProject"
ROUTING_KEY_AFFECTED = "propagation.affected.buildings"  # Queue to listen to
ROUTING_KEY_RESULTS = "propagation.neo4j.results"  # Queue to send results to
QUEUE_NAME = "neo4j_processor_queue"

# File paths
INPUT_QUERY_PATH = "../input_query.txt"
RESULTS_PATH = "Neo4J_results.txt"
STATS_PATH = "Neo4J_stats.txt"

# Set up connection parameters
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    credentials=credentials,
    heartbeat=600,
    blocked_connection_timeout=300
)

# Global flag to track processing state
is_processing = False

def send_to_rabbitmq(channel, routing_key, message_type, content):
    """Send message to RabbitMQ"""
    try:
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=routing_key,
            body=json.dumps({
                "type": message_type,
                "content": content
            })
        )
        print(f"✅ Sent {message_type} to RabbitMQ")
        return True
    except Exception as e:
        print(f"❌ Error sending {message_type}: {e}")
        return False

def process_results(channel):
    """Read and send results and stats files"""
    global is_processing

    # Wait for files to be completely written
    time.sleep(1)

    # Process results file
    try:
        with open(RESULTS_PATH, 'r') as f:
            results_content = f.read()
        send_to_rabbitmq(channel, ROUTING_KEY_RESULTS, "results", results_content)
    except Exception as e:
        print(f"❌ Error reading results file: {e}")

    # Process stats file
    try:
        with open(STATS_PATH, 'r') as f:
            stats_content = f.read()
        send_to_rabbitmq(channel, ROUTING_KEY_RESULTS, "stats", stats_content)
    except Exception as e:
        print(f"❌ Error reading stats file: {e}")

    # Reset processing flag
    is_processing = False
    print("🔄 Ready for next query")

def run_neo4j_script(channel):
    """Run the Neo4j script and process its output"""
    global is_processing

    try:
        print("🚀 Running Neo4j script...")
        process = subprocess.Popen(
            ["python", "../neo4j/execute_prop_query.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()

        print(f"📋 Neo4j script output: {stdout.decode()}")

        if stderr:
            print(f"⚠️ Neo4j script errors: {stderr.decode()}")
            send_to_rabbitmq(channel, ROUTING_KEY_RESULTS, "error", stderr.decode())
            is_processing = False
            return

        # Process results
        process_results(channel)

    except Exception as e:
        print(f"❌ Error running Neo4j script: {e}")
        is_processing = False

def process_message(channel, body):
    """Process incoming messages and trigger Neo4j script"""
    global is_processing

    try:
        print(f"📩 Received message: {body.decode()}")
        data = json.loads(body.decode())

        # Only process if not already running a query
        if not is_processing:
            is_processing = True

            # Write the data to the input file
            with open(INPUT_QUERY_PATH, 'w') as f:
                f.write(str(data))
            print(f"📝 Wrote data to {INPUT_QUERY_PATH}")

            # Run the Neo4j script
            run_neo4j_script(channel)
        else:
            print("⏳ Already processing, skipping this message")

    except json.JSONDecodeError:
        print(f"❌ Invalid JSON in message: {body.decode()}")
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        is_processing = False

def callback(ch, method, properties, body):
    """RabbitMQ message callback"""
    process_message(ch, body)

def main():
    try:
        # Create connection
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Set up exchange and queue
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=False)
        channel.queue_declare(queue=QUEUE_NAME, durable=False)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY_AFFECTED)

        print(f"🔄 Waiting for '{ROUTING_KEY_AFFECTED}' messages... Press CTRL+C to exit.")

        # Start consuming
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

        connection.close()

    except pika.exceptions.AMQPConnectionError as e:
        print(f"❌ Failed to connect to RabbitMQ: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    main()
