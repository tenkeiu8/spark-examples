#!/usr/bin/env python3

# This is legacy RDD based streaming

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


spark = SparkSession.builder.appName("SimpleStreamingApp").getOrCreate()

sc = spark.sparkContext

spark_conf = sc.getConf()

duration = int(spark_conf.get('spark.streaming.batch.duration'))

ssc = StreamingContext(sc, duration)

streaming_directory = 'datasets/streaming'

file_stream = ssc.textFileStream(streaming_directory)
file_stream.pprint()

wc_stream = file_stream.map(lambda line: len(line))
wc_stream.pprint()

count_stream = file_stream.count()
count_stream.pprint()

another_count_stream = wc_stream.reduce(lambda x,y: x + y)
another_count_stream.pprint()

count_stream_again = file_stream.transform(lambda rdd: sc.parallelize([rdd.count()]))
count_stream_again.pprint()

count_stream_one_more = file_stream.flatMap(lambda line: list(line)).countByValue()
count_stream_one_more.pprint()

count_stream_really = file_stream.flatMap(lambda line: list(line)).map(lambda char: (char, 1)).reduceByKey( lambda x,y: x + y)
count_stream_really.pprint()

def json_to_df(rdd):
    lower_rdd = rdd.map(lambda line: line.lower())
    df = spark.read.json(lower_rdd)
    df.show(10, False)

file_stream.foreachRDD(json_to_df)

def save_to_hive(rdd):
    if not rdd.isEmpty():
        lower_rdd = rdd.map(lambda line: line.lower())
        df = spark.read.json(lower_rdd).na.fill({'continent' : 'earth'}).drop('_corrupt_record')
        df.write.format('orc').mode('append').partitionBy('continent').saveAsTable('test_table')

file_stream.foreachRDD(save_to_hive)

ssc.start()
ssc.awaitTermination()
