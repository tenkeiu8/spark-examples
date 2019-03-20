#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

input_data = 'datasets/data1.json'

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

df = spark.read.json(input_data)

print("Dataframe")
print(df)

print("First lines of DF")
df.show()

print("DF schema")
df.printSchema()

print("Select")
df.select('name', 'country', 'continent', 'population').distinct().show()


print("drop not available data")
df.select('name', 'country', 'continent', 'population').distinct().na.drop("all").show()


print("fill not available data")
df.select('name', 'country', 'continent', 'population').distinct().na.drop("all").na.fill( {'continent': 'Earth', 'population': 0 } ).show()

print("basic aggregation")
df.select('name', 'country', 'continent', 'population') \
.distinct().na.drop("all") \
.na.fill( {'continent': 'Earth', 'population': 0 } ) \
.groupBy('continent').count() \
.show()

print("advanced aggregation")
df.select('name', 'country', 'continent', 'population') \
.distinct().na.drop("all") \
.na.fill( {'continent': 'Earth', 'population': 0 } ) \
.groupBy('continent') \
.agg(F.count('continent'), F.sum('population')) \
.show()

print("write to cassandra")
df.select('name', 'country', 'continent', 'population') \
.distinct().na.drop("all") \
.na.fill( {'continent': 'Earth', 'population': 0 } ) \
.groupBy('continent') \
.agg(F.count('continent').alias('population_count'), F.sum('population').alias('population_sum')) \
.write.format('org.apache.spark.sql.cassandra').mode('append').options(table='agg0',keyspace='test').save()

print("read from cassandra")
cas_agg0 = spark.read.format('org.apache.spark.sql.cassandra').options(table='agg0', keyspace='test').load()
cas_agg0.show()

print("write to orc file")
cas_agg0.filter(cas_agg0.continent != 'Africa') \
.write.format('orc').mode('overwrite').save('agg0.orc')

print("read orc")
orc_agg0 = spark.read.format('orc').load('agg0.orc')
orc_agg0.show()

print("write to hive table")
orc_agg0 \
.withColumn('year', F.lit(2018)) \
.withColumn('month', F.lit(10)) \
.withColumn('day', F.lit(10)) \
.write.format('orc').mode('append').partitionBy('year', 'month', 'day').saveAsTable('agg0')

print("List Hive tables")
hive_tables = spark.catalog.listTables()
print(hive_tables)

print("show create table")
spark.sql("SHOW CREATE TABLE agg0").show(20, False)

print('read hive table')
hive_agg0 = spark.table('agg0')
part_agg0 = hive_agg0.where((hive_agg0.year == 2018) & (hive_agg0.month == 10) & (hive_agg0.day == 10))
part_agg0.show()

print("explain sql")
part_agg0.explain(False)

print("write to elasticsearch")
part_agg0.write.format("org.elasticsearch.spark.sql").mode('overwrite').save("agg0/agg0")

print("read from elasticsearch")
es_agg0 = spark.read.format("org.elasticsearch.spark.sql").load("agg0")
es_agg0.show()

print("Join example")
df.select('name', 'country', 'continent', 'population').distinct().na.drop("all").na.fill( {'continent': 'Earth', 'population': 0 } ) \
.join(cas_agg0, 'continent').show()

print("UDF")
spark.udf.registerJavaFunction("starts_with_e", "local.spark.udf.TestUDF", BooleanType())

df.select('name', 'country', 'continent', 'population', ).distinct().na.drop("all").na.fill( {'continent': 'Earth', 'population': 0 } ) \
.registerTempTable('temp')


spark.sql("""SELECT continent, starts_with_e(continent) AS e FROM temp""").show()

print("stop spark session")
spark.stop()







