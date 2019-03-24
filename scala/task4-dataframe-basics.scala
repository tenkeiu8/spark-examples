
// read JSON file to DF
val inputData = "datasets/data1.json"
val raw = spark.read.json(inputData)

// print first 20 rows of DF
raw.show

// print DF schema
raw.printSchema

// select specific columns
val selected = raw.select('name, 'country, 'continent, 'population)
selected.show

// get distinct rows
val distinct = selected.distinct
distinct.show

// drop null rows
val dropped = distinct.na.drop("all")

//fill null values
val filled = dropped.na.fill(Map("continent" -> "Earth", "population" -> 0))
filled.show

val df = filled

// count group by 'continent'
val groupedA = filled.groupBy('continent).count
groupedA.show


// count, sum group by 'continent'
val groupedB = filled.groupBy('continent).agg(count("*").alias("population_count"), sum("population").alias("population_sum"))
groupedB.show

// write to cassandra
groupedB.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> "agg0", "keyspace" -> "test")).save

// read from cassandra
val cassDF = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "agg0", "keyspace" -> "test")).load
cassDF.show(20, false)

// write to ES
cassDF.write.format("org.elasticsearch.spark.sql").mode("overwrite").save("agg0/agg0")

// join df and cassandra table
val joined = df.join(cassDF, Seq("continent"))
joined.show(20, false)

//write to parquet
joined.write.partitionBy("continent").format("parquet").mode("overwrite").save("tmp/data1.parquet")

//window sum, row number
import org.apache.spark.sql.expressions.Window
val window = Window.partitionBy("continent").orderBy("country")
val windowed = df.select(col("*"), row_number().over(window).alias("rn"), sum("population").over(window).alias("population_sum"))
windowed.show


val city_udf = udf { (name: String) => "I love " + name }

windowed.withColumn("attitude", city_udf('name)).show
