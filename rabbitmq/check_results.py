import pika
import json
import time

# RabbitMQ connection details
RABBITMQ_HOST = "localhost"
RABBITMQ_USER = "incubator"
RABBITMQ_PASSWORD = "incubator"
EXCHANGE_NAME = "PropagationProject"
ROUTING_KEY_RESULTS = "propagation.neo4j.results"  # The routing key used for results

# Set up connection with authentication
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    credentials=credentials,
    heartbeat=600,
    blocked_connection_timeout=300
)

def process_results(result_data):
    """
    Process the received results.
    You can customize this function to handle the results as needed.
    """
    msg_type = result_data.get("type")
    content = result_data.get("content")
    
    print(f"\n{'='*50}")
    print(f"📊 Received Neo4j {msg_type}:")
    print(f"{'='*50}")
    print(content)
    print(f"{'='*50}\n")
    
    # Add your custom processing logic here
    
    return True

def callback(ch, method, properties, body):
    """Process messages from the RabbitMQ queue."""
    try:
        data = json.loads(body.decode())
        print(f"📩 Received message of type: {data.get('type', 'unknown')}")
        
        # Process the results
        process_results(data)
        
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
            # This allows reading messages without interfering with other consumers
            result = channel.queue_declare(queue='', exclusive=True, auto_delete=True)
            temp_queue_name = result.method.queue
            
            # Bind our temporary queue to the exchange with the routing key
            channel.queue_bind(
                exchange=EXCHANGE_NAME, 
                queue=temp_queue_name, 
                routing_key=ROUTING_KEY_RESULTS
            )
            
            print(f"🔄 Listening for Neo4j results on routing key '{ROUTING_KEY_RESULTS}'...")
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
