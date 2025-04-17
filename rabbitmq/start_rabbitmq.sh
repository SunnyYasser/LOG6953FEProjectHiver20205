#!/bin/bash
sudo docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=incubator \
  -e RABBITMQ_DEFAULT_PASS=incubator \
  rabbitmq:3-management
