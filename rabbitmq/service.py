import pika
import json
import os
import subprocess
import time
from threading import Thread
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# RabbitMQ connection details
RABBITMQ_HOST = "localhost"
RABBITMQ_USER = "incubator"
RABBITMQ_PASSWORD = "incubator"
EXCHANGE_NAME = "PropagationProject"
ROUTING_KEY_AFFECTED = "propagation.affected.buildings"  # Queue to listen to
ROUTING_KEY_RESULTS = "propagation.neo4j.results"  # Queue to send results to
QUEUE_NAME = "neo4j_processor_queue"

# File paths
INPUT_QUERY_PATH = "../input_query.txt"
RESULTS_PATH = "../neo4j/Neo4J_results.txt"
STATS_PATH = "../neo4j/Neo4J_stats.txt"

# Set up connection with authentication
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST, 
    credentials=credentials,
    heartbeat=600,  # Keep connection alive longer
    blocked_connection_timeout=300
)

# Flag to track if the Neo4j script is currently running
neo4j_script_running = False

class ResultsFileHandler(FileSystemEventHandler):
    """Watch for changes to the results files and send them to RabbitMQ when they're modified."""
    
    def __init__(self, channel):
        self.channel = channel
        self.results_sent = False
        self.stats_sent = False
    
    def on_modified(self, event):
        global neo4j_script_running
        
        if not event.is_directory:
            filepath = event.src_path
            
            # Allow a small delay for the file to be completely written
            time.sleep(0.5)
            
            if filepath.endswith(RESULTS_PATH) and not self.results_sent:
                try:
                    with open(RESULTS_PATH, 'r') as f:
                        results_content = f.read()
                    
                    print(f"📤 Sending Neo4j results...")
                    self.channel.basic_publish(
                        exchange=EXCHANGE_NAME,
                        routing_key=ROUTING_KEY_RESULTS,
                        body=json.dumps({
                            "type": "results",
                            "content": results_content
                        })
                    )
                    self.results_sent = True
                    print(f"✅ Neo4j results sent")
                    
                except Exception as e:
                    print(f"❌ Error sending results: {e}")
            
            elif filepath.endswith(STATS_PATH) and not self.stats_sent:
                try:
                    with open(STATS_PATH, 'r') as f:
                        stats_content = f.read()
                    
                    print(f"📤 Sending Neo4j stats...")
                    self.channel.basic_publish(
                        exchange=EXCHANGE_NAME,
                        routing_key=ROUTING_KEY_RESULTS,
                        body=json.dumps({
                            "type": "stats",
                            "content": stats_content
                        })
                    )
                    self.stats_sent = True
                    print(f"✅ Neo4j stats sent")
                    
                except Exception as e:
                    print(f"❌ Error sending stats: {e}")
            
            # If both files have been sent, reset flags for next run
            if self.results_sent and self.stats_sent:
                self.results_sent = False
                self.stats_sent = False
                neo4j_script_running = False
                print("🔄 Ready for next query")

def run_neo4j_script():
    """Run the Neo4j script and handle any errors."""
    global neo4j_script_running
    
    try:
        print("🚀 Running Neo4j script...")
        process = subprocess.Popen(["python", "../neo4j/execute_prop_query.py"], 
                                  stdout=subprocess.PIPE, 
                                  stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        print(f"📋 Neo4j script output: {stdout.decode()}")
        
        if stderr:
            print(f"⚠️ Neo4j script errors: {stderr.decode()}")
            # Send error message to RabbitMQ
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=ROUTING_KEY_RESULTS,
                body=json.dumps({
                    "type": "error",
                    "content": stderr.decode()
                })
            )
            connection.close()
            neo4j_script_running = False
            
    except Exception as e:
        print(f"❌ Error running Neo4j script: {e}")
        neo4j_script_running = False

def callback(ch, method, properties, body):
    """Process messages from the RabbitMQ queue."""
    global neo4j_script_running
    
    try:
        print(f"📩 Received message: {body.decode()}")
        data = json.loads(body.decode())
        
        # Only process if not already running a query
        if not neo4j_script_running:
            neo4j_script_running = True
            
            # Write the data to the input file
            with open(INPUT_QUERY_PATH, 'w') as f:
                f.write(str(data))
            print(f"📝 Wrote data to {INPUT_QUERY_PATH}")
            
            # Run the Neo4j script in a separate thread
            thread = Thread(target=run_neo4j_script)
            thread.daemon = True
            thread.start()
        else:
            print("⏳ Neo4j script already running, skipping this message")
        
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON in message: {body.decode()}")
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        neo4j_script_running = False

def main():
    try:
        # Create connection
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Declare exchange and queue
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=False)
        channel.queue_declare(queue=QUEUE_NAME, durable=False)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY_AFFECTED)
        
        print(f"🔄 Waiting for '{ROUTING_KEY_AFFECTED}' messages... Press CTRL+C to exit.")
        
        # Set up file watching
        handler = ResultsFileHandler(channel)
        observer = Observer()
        observer.schedule(handler, path=os.path.dirname(os.path.abspath(RESULTS_PATH)) or '.', recursive=False)
        observer.start()
        
        # Start consuming
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
        
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            observer.stop()
            channel.stop_consuming()
        
        observer.join()
        connection.close()
        
    except pika.exceptions.AMQPConnectionError as e:
        print(f"❌ Failed to connect to RabbitMQ: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    main()
