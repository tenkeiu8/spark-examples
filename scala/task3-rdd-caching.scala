val rdd = spark.sparkContext.parallelize(1 to 100000000)
  .filter( i => i > 100000)
  .map(x => x*3)
  .map( x => x + 1)

rdd.cache

val checks = 1 to 9
checks.map { check => (check, rdd.filter(i => i % check == 0).count) } 
