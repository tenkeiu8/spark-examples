val streamingDir = "datasets"

val readKafkaParams = Map("kafka.bootstrap.servers" -> "localhost:9092", "subscribe" -> "test_topic", "startingOffsets" -> "earliest")
val writeKafkaParams = Map("kafka.bootstrap.servers" -> "localhost:9092", "topic" -> "test_topic", "checkpointLocation" -> "tmp/kafka-stream-2")

import org.apache.spark.sql.types._

// create schema
val columns = List("name", "country", "continent", "population")
val schema = StructType(columns.map { column => StructField(column, StringType, true) })


// create input stream
val inputStream = spark.readStream.format("kafka").options(readKafkaParams).load

import org.apache.spark.sql.streaming.Trigger

//parse stream
val parsed = inputStream.select(json_tuple('value.cast("String"), columns:_*).as(columns))

// clean stream
val clean = parsed.na.drop("all").withColumn("population", 'population.cast("Long")).na.fill(Map("continent" -> "Earth", "population" -> 0))

// print agg to console
val query1 = clean.groupBy('name).agg(count("*").as("cnt"), sum("population").as("sum")).writeStream.trigger(Trigger.ProcessingTime("5 seconds")).outputMode("complete").format("console").start

// write clean to kafka
val query2 = clean.toJSON.writeStream.format("kafka").outputMode("append").trigger(Trigger.ProcessingTime("10 seconds")).options(writeKafkaParams).start

