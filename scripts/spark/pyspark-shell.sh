#!/bin/bash

SPARK_VERSION=2.4.0

PYSPARK_PYTHON=python3.7 pyspark \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
    --conf spark.sql.execution.arrow.enabled=true \
    --conf spark.sql.crossJoin.enabled=true \
    --master local[*] \
    --packages com.datastax.spark:spark-cassandra-connector_2.11:$SPARK_VERSION,org.apache.spark:spark-sql-kafka-0-10_2.11:$SPARK_VERSION




