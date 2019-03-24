#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SimpleStreamingApp").getOrCreate()

to_kafka = spark.read.json('datasets/data1.json').drop('_corrupt_record')

columns = to_kafka.columns
schema = to_kafka.schema

to_kafka \
  .select(F.to_json(F.struct(*columns)).alias('value')) \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "test_topic") \
  .save()

batch_from_kafka = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "test_topic") \
  .load()


batch_from_kafka \
  .select(batch_from_kafka['value'].cast("String")) \
  .show(20, False)

stream_from_kafka = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "test_topic") \
  .load()

out = stream_from_kafka \
  .select(stream_from_kafka['value'].cast("String").alias("value")) \
  .writeStream \
  .trigger(processingTime='5 seconds') \
  .outputMode('append') \
  .format('console') \
  .option('truncate','false') \
  .option('numRows','5') \
  .start()


"""
# Structured streaming usage examples

from pyspark.sql.types import *
import uuid


def gen_uuid(something):
    return str(uuid.uuid4())

uuid_gen = F.pandas_udf(gen_uuid, returnType=StringType())


file_stream = spark.readStream.schema(schema).json('streaming')

tmp_df = file_stream \
  .withColumn("unique_id", uuid_gen(F.col('continent')))

out = tmp_df \
  .writeStream \
  .trigger(processingTime='5 seconds') \
  .outputMode('append') \
  .format('console') \
  .option('truncate','false') \
  .option('numRows','50') \
  .start()

out = tmp_df \
  .writeStream \
  .trigger(processingTime='5 seconds') \
  .outputMode('append') \
  .format('console') \
  .option('truncate','false') \
  .option('numRows','50') \
  .start()
 
tmp_df \
  .select(tmp_df['value.continent'].alias('continent'), tmp_df['value.population'].alias('population')) \
  .na.drop() \
  .groupBy('continent').sum('population') \
  .writeStream \
  .trigger(processingTime='5 seconds') \
  .outputMode('complete') \
  .format('console') \
  .option('truncate','false') \
  .option('numRows','50') \
  .start()

tmp_df \
  .select(tmp_df['value.continent'].alias('continent'), tmp_df['value.population'].alias('population')) \
  .na.drop() \
  .groupBy().count() \
  .writeStream \
  .trigger(processingTime='5 seconds') \
  .outputMode('complete') \
  .format('console') \
  .option('truncate','false') \
  .option('numRows','50') \
  .start()

out = stream_from_kafka \
  .select(stream_from_kafka['value'].cast("String")) \
  .writeStream \
  .trigger(processingTime='5 seconds') \
  .outputMode('append') \
  .format('kafka') \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "test_topic") \
  .option("checkpointLocation", "checkpoint/task3") \
  .start()
"""

out.awaitTermination()
