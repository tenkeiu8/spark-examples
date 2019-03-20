#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SimpleStreamingApp").getOrCreate()

stream = spark.readStream.json('streaming')

out = stream \
  .groupBy().count() \
  .writeStream \
  .trigger(processingTime='5 seconds') \
  .outputMode('complete') \
  .format('console') \
  .option('truncate','false') \
  .option('numRows','5') \
  .option("checkpointLocation", "checkpoint/task4") \
  .start()


out.awaitTermination()