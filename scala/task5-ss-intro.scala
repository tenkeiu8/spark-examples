// create DF from JSON file
val inputDir = "datasets/data1.json"
val df = spark.read.json(inputDir).drop("_corrupt_record").na.drop("all").na.fill(Map("continent"->"Earth", "population" -> 0))

val readKafkaParams = Map("kafka.bootstrap.servers" -> "localhost:9092", "subscribe" -> "test_topic")
val writeKafkaParams = Map("kafka.bootstrap.servers" -> "localhost:9092", "topic" -> "test_topic")

// write to kafka
val jsonDF = df.toJSON
jsonDF.printSchema
jsonDF.show(5, false)
jsonDF.write.format("kafka").mode("append").options(writeKafkaParams).save()

// read batch from kafka
val kafkaDF = spark.read.format("kafka").options(readKafkaParams).load()
val value = kafkaDF.select('value.cast("String").alias("value"))
value.show(20, false)
