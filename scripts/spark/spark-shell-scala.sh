#!/bin/bash

SPARK_VERSION=2.4.0

spark-shell \
    --master local[*] \
    --driver-memory 4G \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
	--conf spark.driver.extraJavaOptions="-Dscala.color" \
    --conf spark.cassandra.auth.username=cassandra \
    --conf spark.cassandra.auth.password=cassandra \
    --conf spark.cassandra.connection.host=127.0.0.1 \
    --conf spark.es.nodes=127.0.0.1 \
    --conf spark.es.port=9200 \
    --conf spark.es.nodes.wan.only=true \
    --conf spark.sql.crossJoin.enabled=true \
    --packages com.datastax.spark:spark-cassandra-connector_2.11:$SPARK_VERSION,org.elasticsearch:elasticsearch-spark-20_2.11:6.6.0 \

