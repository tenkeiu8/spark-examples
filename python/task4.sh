#!/bin/bash

SPARK_VERSION=2.4.0

PYSPARK_PYTHON=python3.7 spark-submit \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:conf/conf/log4j.properties" \
    --conf spark.sql.streaming.schemaInference=true \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:$SPARK_VERSION \
    python/task4.py


