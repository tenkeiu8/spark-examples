val streamingDir = "datasets"

val readKafkaParams = Map("kafka.bootstrap.servers" -> "localhost:9092", "subscribe" -> "test_topic")
val writeKafkaParams = Map("kafka.bootstrap.servers" -> "localhost:9092", "topic" -> "test_topic", "checkpointLocation" -> "tmp/kafka-stream-1")

import org.apache.spark.sql.types._

// create schema
val columns = List("name", "country", "continent", "population")
val schema = StructType(columns.map { column => StructField(column, StringType, true) })


// create input stream
val inputStream = spark.readStream.schema(schema).json(streamingDir)

import org.apache.spark.sql.streaming.Trigger

// print stream
inputStream.writeStream.trigger(Trigger.Once).outputMode("append").format("console").start

// clean stream
val clean = inputStream.na.drop("all").withColumn("population", 'population.cast("Long")).na.fill(Map("continent" -> "Earth", "population" -> 0))

// print clean to console
clean.writeStream.trigger(Trigger.Once).outputMode("append").format("console").start

// write clean to kafka
clean.toJSON.writeStream.format("kafka").outputMode("append").trigger(Trigger.Once).options(writeKafkaParams).start


