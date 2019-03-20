#!/bin/bash

SPARK_VERSION=2.4.0

PYSPARK_PYTHON=python3.7 spark-submit \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
    --master local[*] \
    --conf spark.sql.execution.arrow.enabled=true \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:$SPARK_VERSION \
    python/task3.py


