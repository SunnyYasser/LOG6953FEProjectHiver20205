import pika
import json
import os
import subprocess
import time
import re
from threading import Thread

# RabbitMQ configuration
RABBITMQ_HOST = "localhost"
RABBITMQ_USER = "incubator"
RABBITMQ_PASSWORD = "incubator"
EXCHANGE_NAME = "PropagationProject"
ROUTING_KEY_AFFECTED = "propagation.affected.buildings"  # Queue to listen to
ROUTING_KEY_NEO4J_RESULTS = "propagation.neo4j.results"  # Queue to send Neo4j results to
ROUTING_KEY_TACHOSDB_RESULTS = "propagation.tachosdb.results"  # Queue to send TachosDB results to
ROUTING_KEY_PERFORMANCE_RATIO = "propagation.performance.ratio"  # New routing key for performance ratio
QUEUE_NAME = "failure_propagation_queue"

# File paths
INPUT_QUERY_PATH = "../input_query.txt"
NEO4J_RESULTS_PATH = "Neo4J_results.txt"
NEO4J_STATS_PATH = "Neo4J_stats.txt"
TACHOSDB_RESULTS_PATH = "TachosDB_results.txt"
TACHOSDB_STATS_PATH = "TachosDB_stats.txt"

# Executable paths
NEO4J_SCRIPT_PATH = "../neo4j/execute_prop_query.py"
TACHOSDB_EXEC_PATH = "../cmake-build-release/mydb2"

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
        print(f"✅ Sent {message_type} to RabbitMQ with routing key {routing_key}")
        return True
    except Exception as e:
        print(f"❌ Error sending {message_type}: {e}")
        return False

def extract_total_execution_time(file_path):
    """Extract the total execution time from a stats file"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()

        # Look for the total execution time line
        match = re.search(r'Total Execution Time \(all queries\): (\d+) us', content)
        if match:
            return int(match.group(1))

        # If not found with the new format, try the old format (sum of individual queries)
        # This is a fallback in case the format differs
        total_time = 0
        for match in re.finditer(r'(?:Query \d+ )?Execution Time: (\d+) us', content):
            total_time += int(match.group(1))

        return total_time
    except Exception as e:
        print(f"❌ Error extracting execution time from {file_path}: {e}")
        return None

def calculate_performance_ratio(channel):
    """Calculate the ratio of Neo4j to TachosDB execution times and send to RabbitMQ"""
    try:
        # Wait for files to be completely written
        time.sleep(2)

        # Extract execution times
        neo4j_time = extract_total_execution_time(NEO4J_STATS_PATH)
        tachosdb_time = extract_total_execution_time(TACHOSDB_STATS_PATH)

        if neo4j_time is None or tachosdb_time is None:
            print("❌ Failed to extract execution times")
            return False

        if tachosdb_time == 0:
            print("❌ TachosDB execution time is zero, cannot calculate ratio")
            return False

        # Calculate ratio (Neo4j time / TachosDB time)
        ratio = neo4j_time / tachosdb_time

        # Prepare detailed result
        performance_data = {
            "neo4j_time_us": neo4j_time,
            "tachosdb_time_us": tachosdb_time,
            "ratio_neo4j_to_tachosdb": ratio,
            "timestamp": time.time()
        }

        # Send to RabbitMQ
        print(f"📊 Performance ratio: Neo4j/TachosDB = {ratio:.2f} ({neo4j_time}/{tachosdb_time})")
        return send_to_rabbitmq(channel, ROUTING_KEY_PERFORMANCE_RATIO, "performance_ratio", performance_data)

    except Exception as e:
        print(f"❌ Error calculating performance ratio: {e}")
        return False

def process_neo4j_results(channel):
    """Read and send Neo4j results and stats files"""
    # Wait for files to be completely written
    time.sleep(1)

    # Process results file
    try:
        with open(NEO4J_RESULTS_PATH, 'r') as f:
            results_content = f.read()
        send_to_rabbitmq(channel, ROUTING_KEY_NEO4J_RESULTS, "results", results_content)
    except Exception as e:
        print(f"❌ Error reading Neo4j results file: {e}")

    # Process stats file
    try:
        with open(NEO4J_STATS_PATH, 'r') as f:
            stats_content = f.read()
        send_to_rabbitmq(channel, ROUTING_KEY_NEO4J_RESULTS, "stats", stats_content)
    except Exception as e:
        print(f"❌ Error reading Neo4j stats file: {e}")

def process_tachosdb_results(channel):
    """Read and send TachosDB results and stats files"""
    # Wait for files to be completely written
    time.sleep(1)

    # Process results file
    try:
        with open(TACHOSDB_RESULTS_PATH, 'r') as f:
            results_content = f.read()
        send_to_rabbitmq(channel, ROUTING_KEY_TACHOSDB_RESULTS, "results", results_content)
    except Exception as e:
        print(f"❌ Error reading TachosDB results file: {e}")

    # Process stats file
    try:
        with open(TACHOSDB_STATS_PATH, 'r') as f:
            stats_content = f.read()
        send_to_rabbitmq(channel, ROUTING_KEY_TACHOSDB_RESULTS, "stats", stats_content)
    except Exception as e:
        print(f"❌ Error reading TachosDB stats file: {e}")

def run_neo4j_script(channel):
    """Run the Neo4j script and process its output"""
    try:
        print("🚀 Running Neo4j script...")
        process = subprocess.Popen(
            ["python", NEO4J_SCRIPT_PATH],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()

        print(f"📋 Neo4j script output: {stdout.decode()}")

        if stderr:
            print(f"⚠️ Neo4j script errors: {stderr.decode()}")
            send_to_rabbitmq(channel, ROUTING_KEY_NEO4J_RESULTS, "error", stderr.decode())
            return False

        # Process results
        process_neo4j_results(channel)
        return True

    except Exception as e:
        print(f"❌ Error running Neo4j script: {e}")
        return False

def run_tachosdb_executable(channel):
    """Run the TachosDB executable and process its output"""
    try:
        print("🚀 Running TachosDB executable...")
        process = subprocess.Popen(
            [TACHOSDB_EXEC_PATH],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()

        print(f"📋 TachosDB executable output: {stdout.decode()}")

        if stderr:
            print(f"⚠️ TachosDB executable errors: {stderr.decode()}")
            send_to_rabbitmq(channel, ROUTING_KEY_TACHOSDB_RESULTS, "error", stderr.decode())
            return False

        # Process results
        process_tachosdb_results(channel)
        return True

    except Exception as e:
        print(f"❌ Error running TachosDB executable: {e}")
        return False

def process_message(channel, body):
    """Process incoming messages and trigger scripts"""
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
            neo4j_success = run_neo4j_script(channel)

            # Run the TachosDB executable
            tachosdb_success = run_tachosdb_executable(channel)

            # Calculate and send performance ratio
            if neo4j_success and tachosdb_success:
                calculate_performance_ratio(channel)

            # Reset processing flag
            is_processing = False
            print("🔄 Ready for next query")

        else:
            print("⏳ Already processing, skipping this message")

    except json.JSONDecodeError:
        print(f"❌ Invalid JSON in message: {body.decode()}")
        is_processing = False
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        is_processing = False

def callback(ch, method, properties, body):
    """RabbitMQ message callback"""
    process_message(ch, body)

def setup_rabbitmq(channel):
    """Set up RabbitMQ exchange, queues, and bindings"""
    try:
        # Declare exchange
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
        print(f"✅ Exchange '{EXCHANGE_NAME}' declared")

        # Declare main queue
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        print(f"✅ Queue '{QUEUE_NAME}' declared")

        # Bind queue to exchange
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY_AFFECTED)
        print(f"✅ Queue '{QUEUE_NAME}' bound to exchange '{EXCHANGE_NAME}' with routing key '{ROUTING_KEY_AFFECTED}'")

        # Declare result queues
        neo4j_queue = "neo4j_results_queue"
        tachosdb_queue = "tachosdb_results_queue"
        performance_queue = "performance_ratio_queue"

        channel.queue_declare(queue=neo4j_queue, durable=True)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=neo4j_queue, routing_key=ROUTING_KEY_NEO4J_RESULTS)
        print(f"✅ Queue '{neo4j_queue}' declared and bound")

        channel.queue_declare(queue=tachosdb_queue, durable=True)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=tachosdb_queue, routing_key=ROUTING_KEY_TACHOSDB_RESULTS)
        print(f"✅ Queue '{tachosdb_queue}' declared and bound")

        channel.queue_declare(queue=performance_queue, durable=True)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=performance_queue, routing_key=ROUTING_KEY_PERFORMANCE_RATIO)
        print(f"✅ Queue '{performance_queue}' declared and bound")

        return True
    except Exception as e:
        print(f"❌ Error setting up RabbitMQ: {e}")
        return False

def main():
    try:
        # Create connection
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Set up RabbitMQ with proper exchange and queues
        setup_rabbitmq(channel)

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