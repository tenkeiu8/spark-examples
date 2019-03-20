#!/usr/bin/env python3

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

dataset = ['Moscow', 'Madrid', 'Paris', 'Berlin', 'Barselona', 'Cairo', 'Perm']

rdd = spark.sparkContext.parallelize(dataset)

res1 = rdd.first()
print(res1)


res2 = rdd.take(2)
print(res2)

res3 = rdd.map(lambda x: x.lower()).first()
print(res3)

res4 = rdd.map(lambda x: x.lower()).collect()
print(res4)

res5 = rdd.map(lambda x: x.lower()).filter(lambda x: x.startswith('m')).collect()
print(res5)

res6 = rdd.map(lambda x: x.lower()).filter(lambda x: x.startswith('m')).map(lambda x: 'I love ' + x).collect()
print(res6)

res7 = rdd.map(lambda x: x.lower()).filter(lambda x: x.startswith('m')).map(lambda x: 'I love ' + x).map(lambda x: len(x)).reduce(lambda x,y: x + y)
print(res7)


def first_letter(input):
	return input[0]

res8 = rdd.map(lambda x: first_letter(x)).collect()
print(res8)

spark.stop()