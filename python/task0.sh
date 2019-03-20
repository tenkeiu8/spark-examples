#!/bin/bash

PYSPARK_PYTHON=python3.7 spark-submit \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
    --master local[*] \
    python/task0.py


