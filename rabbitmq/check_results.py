import pika
import json
import time

# RabbitMQ connection details
RABBITMQ_HOST = "localhost"
RABBITMQ_USER = "incubator"
RABBITMQ_PASSWORD = "incubator"
EXCHANGE_NAME = "PropagationProject"
ROUTING_KEY_NEO4J = "propagation.neo4j.results"     # Routing key for Neo4j results
ROUTING_KEY_TACHOSDB = "propagation.tachosdb.results"  # Routing key for TachosDB results

# Set up connection with authentication
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    credentials=credentials,
    heartbeat=600,
    blocked_connection_timeout=300
)

def process_results(result_data, source):
    """
    Process the received results.
    You can customize this function to handle the results as needed.

    Args:
        result_data: The parsed JSON data
        source: String identifying the source ('neo4j' or 'tachosdb')
    """
    msg_type = result_data.get("type")
    content = result_data.get("content")

    print(f"\n{'='*50}")
    print(f"📊 Received {source.upper()} {msg_type}:")
    print(f"{'='*50}")
    print(content)
    print(f"{'='*50}\n")

    # Add your custom processing logic here
    # For example, you might want to:
    # - Store results in a database
    # - Trigger additional analysis
    # - Update UI components
    # - Compare results between Neo4j and TachosDB

    return True

def callback(ch, method, properties, body):
    """Process messages from the RabbitMQ queue."""
    try:
        # Determine the source based on routing key
        if method.routing_key == ROUTING_KEY_NEO4J:
            source = "neo4j"
        elif method.routing_key == ROUTING_KEY_TACHOSDB:
            source = "tachosdb"
        else:
            source = "unknown"

        data = json.loads(body.decode())
        print(f"📩 Received {source} message of type: {data.get('type', 'unknown')}")

        # Process the results with source information
        process_results(data, source)

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        print(f"❌ Invalid JSON in message: {body.decode()}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def main():
    connection = None
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        try:
            # Create connection
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Declare exchange (should match the producer setup)
            channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=False)

            # Create a temporary queue that auto-deletes when consumer disconnects
            result = channel.queue_declare(queue='', exclusive=True, auto_delete=True)
            temp_queue_name = result.method.queue

            # Bind our temporary queue to both routing keys
            channel.queue_bind(
                exchange=EXCHANGE_NAME,
                queue=temp_queue_name,
                routing_key=ROUTING_KEY_NEO4J
            )

            channel.queue_bind(
                exchange=EXCHANGE_NAME,
                queue=temp_queue_name,
                routing_key=ROUTING_KEY_TACHOSDB
            )

            print(f"🔄 Listening for results on multiple routing keys:")
            print(f"   - Neo4j: '{ROUTING_KEY_NEO4J}'")
            print(f"   - TachosDB: '{ROUTING_KEY_TACHOSDB}'")
            print(f"   Using temporary queue: {temp_queue_name}")
            print(f"   Press CTRL+C to exit.")

            # Start consuming - explicit ack mode
            channel.basic_consume(
                queue=temp_queue_name,
                on_message_callback=callback,
                auto_ack=False
            )

            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            retry_count += 1
            wait_time = min(30, 2 ** retry_count)
            print(f"❌ Connection error (attempt {retry_count}/{max_retries}): {e}")
            print(f"⏱️ Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
            break
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            retry_count += 1
            time.sleep(5)
        finally:
            if connection and not connection.is_closed:
                try:
                    if channel.is_open:
                        channel.stop_consuming()
                    connection.close()
                    print("🔌 Connection closed")
                except Exception:
                    pass

if __name__ == "__main__":
    main()