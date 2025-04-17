# 🚀 Launcher

This is the course project for LOG6953FE - Digital Twins Engineering at Polytechnique Montreal, Winter 2025 term.
The project has two seperate parts - a frontend service and a backend service. The frontend is a Godot project which connects
with the backend service via a RabbitMQ message broker.

---
## 📦 Frontend Services launcher
Simply run the Godot design AFTER the backend services are launched. 


## 📦 Backend Services launcher - what It Does

The `launch_services.sh` script performs the following:

- Detects your operating system.
- Installs Neo4j (if not already installed).
- Starts RabbitMQ using Docker Compose.
- Waits for both services to be fully operational.
- Loads initial data into Neo4j from a Python script.
- Launches a background Python-based graph database service.

---

## ✅ Prerequisites

Make sure you have the following installed:

### Common (Linux/macOS)
- Python 3.7+
- `pip`
- Docker
- Docker Compose

### macOS only
- Homebrew (the script will install it if missing)

---

## 📁 Project Structure

```
.
├── launch_services.sh
├── requirements.txt
├── docker-compose.yml
├── neo4j/
│   └── push_data_to_db.py
├── rabbitmq/
│   └── graph_db_service.py
```

---

## 🚀 How to Use

```bash
chmod +x launch_services.sh
./launch_services.sh
```

The script will:
- Install and start Neo4j.
- Start RabbitMQ via Docker.
- Load data into Neo4j.
- Launch the graph service.

---

## 🔐 Default Access Credentials

| Service   | URL                      | Username  | Password   |
|-----------|--------------------------|-----------|------------|
| RabbitMQ  | http://localhost:15672   | incubator | incubator  |
| Neo4j     | http://localhost:7474    | neo4j     | 12345678   |

---

## 🛑 Cleanup

To stop all services:

- Press `Ctrl+C` (the script will handle shutdown automatically)
- Or manually run:

```bash
docker-compose down
sudo systemctl stop neo4j        # Linux
brew services stop neo4j         # macOS
```

---

## 🧠 Troubleshooting

**Neo4j not ready?**  
View logs:
```bash
sudo journalctl -u neo4j -n 50         # Linux
tail -n 50 $(brew --prefix)/var/log/neo4j/neo4j.log  # macOS
```

**RabbitMQ not responding?**  
Check container logs:
```bash
docker logs rabbitmq
```

---

## ✨ Notes

- Default Neo4j password is `12345678` — change it in the script if needed.
- The script uses `cypher-shell` to check Neo4j readiness — ensure it's in your PATH.
- Make sure Docker has enough memory/CPU allocated.

---

