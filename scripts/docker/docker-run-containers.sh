#!/usr/bin/env bash
HOSTNAME=`hostname`

docker run \
    --name=test_cassandra \
    -d \
    -p 9042:9042 \
    -e CASSANDRA_BROADCAST_ADDRESS=127.0.0.1 \
    cassandra:latest

docker run -d \
	--name test_elasticsearch \
	-p 9200:9200 -p 9300:9300 \
	-e "discovery.type=single-node" \
	docker.elastic.co/elasticsearch/elasticsearch:6.4.0

docker run -d \
    -p 5432:5432 \
    --name test_postgre \
    -e POSTGRES_PASSWORD=spark \
    postgres

docker run -d \
    -p 2181:2181 \
    --name=test_zoo \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    confluentinc/cp-zookeeper:5.0.0

docker run -d \
    -p 9092:9092 \
    --name=test_kafka \
    -e KAFKA_ZOOKEEPER_CONNECT=$HOSTNAME:2181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    confluentinc/cp-kafka:5.0.0
