#!/bin/bash
# Set script to exit on error
set -e

echo "=============================================="
echo "Installing dependencies from requirements.txt"
echo "=============================================="
pip install -r requirements.txt
echo "✅ Dependencies installed successfully"

echo "=============================================="
echo "Starting RabbitMQ and Neo4j containers..."
echo "=============================================="
sudo docker-compose up -d

echo "Waiting for services to be ready..."
# Wait for RabbitMQ to be fully ready
echo "Waiting for RabbitMQ..."
until docker exec rabbitmq rabbitmq-diagnostics -q ping; do
  echo "RabbitMQ is not ready yet, waiting..."
  sleep 5
done
echo "✅ RabbitMQ is up and running!"

# Wait for Neo4j to be fully ready
echo "Waiting for Neo4j..."
until docker exec neo4j cypher-shell -u neo4j -p 12345678 "RETURN 1;" 2>/dev/null; do
  echo "Neo4j is not ready yet, waiting..."
  sleep 5
done
echo "✅ Neo4j is up and running!"

echo "=============================================="
echo "Loading data into Neo4j..."
echo "=============================================="
python neo4j/push_data_to_db.py
echo "✅ Data loaded into Neo4j successfully"

echo "=============================================="
echo "Starting Graph Database Service..."
echo "=============================================="
cd rabbitmq 
python graph_db_service.py &
GRAPH_SERVICE_PID=$!
echo "✅ Graph Database Service started with PID: $GRAPH_SERVICE_PID"

echo "=============================================="
echo "All services are running!"
echo "=============================================="
echo "Access RabbitMQ management at: http://localhost:15672 (user: incubator, pass: incubator)"
echo "Access Neo4j browser at: http://localhost:7474 (user: neo4j, pass: 12345678)"
echo ""
echo "To stop all services, press Ctrl+C"

# Add a trap to handle script termination and clean up
trap "echo 'Stopping services...'; kill $GRAPH_SERVICE_PID; docker-compose down; echo 'All services stopped.'" SIGINT SIGTERM EXIT

# Keep the script running to maintain the background process
wait $GRAPH_SERVICE_PID
