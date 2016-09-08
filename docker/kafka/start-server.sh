#!/bin/sh
echo "Starting zookeeper"
bin/zookeeper-server-start.sh config/zookeeper.properties &

echo "Staring Kafka"
bin/kafka-server-start.sh config/server.properties
