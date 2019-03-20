#!/bin/bash
SPARK_VERSION=2.4.0

PYSPARK_PYTHON=python3.7 spark-submit \
    --master local[*] \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
    --conf spark.cassandra.auth.username=cassandra \
    --conf spark.cassandra.auth.password=cassandra \
    --conf spark.cassandra.connection.host=127.0.0.1 \
    --conf spark.sql.crossJoin.enabled=true \
    --conf spark.es.nodes=127.0.0.1 \
    --conf spark.es.port=9200 \
    --conf spark.es.nodes.wan.only=true \
    --jars lib/udf-funcs_2.11-0.1.jar \
    --packages com.datastax.spark:spark-cassandra-connector_2.11:$SPARK_VERSION,org.elasticsearch:elasticsearch-spark-20_2.11:6.4.2 \
    python/task1.py


