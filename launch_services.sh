#!/bin/bash
# Set script to exit on error
set -e

# Function to detect OS type
detect_os() {
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    export OS_TYPE="linux"
    echo "Detected Linux OS"
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    export OS_TYPE="mac"
    echo "Detected macOS"
  else
    echo "This script only supports Linux and macOS."
    exit 1
  fi
}

# Function to install Neo4j on Linux
install_neo4j_linux() {
  echo "Installing Neo4j on Linux..."
  
  # Add Neo4j repository
  wget -O - https://debian.neo4j.com/neotechnology.gpg.key | sudo apt-key add -
  echo 'deb https://debian.neo4j.com stable latest' | sudo tee -a /etc/apt/sources.list.d/neo4j.list
  
  # Update package list and install Neo4j
  sudo apt-get update
  sudo apt-get install -y neo4j
  
  # Configure Neo4j password
  sudo neo4j-admin set-initial-password 12345678
  
  echo "Neo4j installed successfully on Linux"
}

# Function to install Neo4j on macOS
install_neo4j_mac() {
  echo "Installing Neo4j on macOS..."
  
  # Check if Homebrew is installed
  if ! command -v brew &> /dev/null; then
    echo "Homebrew is required to install Neo4j on macOS."
    echo "Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  fi
  
  # Install Neo4j using Homebrew
  brew install neo4j
  
  # Configure Neo4j password
  neo4j-admin set-initial-password 12345678
  
  echo "Neo4j installed successfully on macOS"
}

# Function to check if Neo4j is installed and install if needed
check_and_install_neo4j() {
  echo "Checking if Neo4j is installed..."
  
  if ! command -v neo4j &> /dev/null; then
    echo "Neo4j is not installed."
    
    if [[ "$OS_TYPE" == "linux" ]]; then
      install_neo4j_linux
    elif [[ "$OS_TYPE" == "mac" ]]; then
      install_neo4j_mac
    fi
  else
    echo "Neo4j is already installed."
  fi
}

# Function to check if Neo4j is running and start if needed
check_and_start_neo4j() {
  echo "Checking if Neo4j service is running..."
  
  # Check if Neo4j is running using systemctl on Linux or brew services on Mac
  if [[ "$OS_TYPE" == "linux" ]]; then
    if ! systemctl is-active --quiet neo4j; then
      echo "Neo4j is not running. Starting Neo4j service..."
      sudo systemctl start neo4j
    else
      echo "Neo4j service is already running."
    fi
  elif [[ "$OS_TYPE" == "mac" ]]; then
    if ! brew services list | grep neo4j | grep -q "started"; then
      echo "Neo4j is not running. Starting Neo4j service..."
      brew services start neo4j
    else
      echo "Neo4j service is already running."
    fi
  fi
}

# Main script execution starts here

echo "=============================================="
echo "Detecting operating system..."
echo "=============================================="
detect_os

echo "=============================================="
echo "Installing dependencies from requirements.txt"
echo "=============================================="
pip install -r requirements.txt
echo "✅ Dependencies installed successfully"

echo "=============================================="
echo "Checking Neo4j installation..."
echo "=============================================="
check_and_install_neo4j

echo "=============================================="
echo "Starting RabbitMQ container and Neo4j service..."
echo "=============================================="

# Start RabbitMQ using Docker
sudo docker-compose up -d rabbitmq
echo "Waiting for RabbitMQ to be ready..."

# Wait for RabbitMQ to be fully ready
until docker exec rabbitmq rabbitmq-diagnostics -q ping; do
  echo "RabbitMQ is not ready yet, waiting..."
  sleep 5
done
echo "✅ RabbitMQ is up and running!"

# Start Neo4j
echo "Starting Neo4j service..."
check_and_start_neo4j

# Wait for Neo4j to be fully ready
echo "Waiting for Neo4j..."
MAX_RETRY=30
RETRY_COUNT=0

while ! cypher-shell -u neo4j -p 12345678 "RETURN 1;" &>/dev/null; do
  echo "Neo4j is not ready yet, waiting... (Attempt $RETRY_COUNT of $MAX_RETRY)"
  RETRY_COUNT=$((RETRY_COUNT + 1))
  
  if [ $RETRY_COUNT -ge $MAX_RETRY ]; then
    echo "Error: Neo4j did not become ready in time. Please check Neo4j logs."
    if [[ "$OS_TYPE" == "linux" ]]; then
      sudo journalctl -u neo4j -n 50
    elif [[ "$OS_TYPE" == "mac" ]]; then
      tail -n 50 $(brew --prefix)/var/log/neo4j/neo4j.log
    fi
    exit 1
  fi
  
  sleep 5
done
echo "✅ Neo4j is up and running!"

echo "=============================================="
echo "Loading data into Neo4j..."
echo "=============================================="
cd neo4j
python push_data_to_db.py
echo "✅ Data loaded into Neo4j successfully"
cd ..

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
trap cleanup SIGINT SIGTERM EXIT

# Cleanup function
cleanup() {
  echo "Stopping services..."
  kill $GRAPH_SERVICE_PID 2>/dev/null || true
  docker-compose down
  
  if [[ "$OS_TYPE" == "linux" ]]; then
    sudo systemctl stop neo4j
  elif [[ "$OS_TYPE" == "mac" ]]; then
    brew services stop neo4j
  fi
  
  echo "All services stopped."
}

# Keep the script running to maintain the background process
wait $GRAPH_SERVICE_PID
