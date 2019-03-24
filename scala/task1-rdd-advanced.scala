// create local List[String]
val dataset = List("Moscow", "Madrid", "Paris", "Berlin", "Barselona", "Cairo", "Perm")

// create RDD[String] from List[String]
val rdd = spark.sparkContext.parallelize(dataset)

// create key-value pair RDD
case class City(name: String)
val kv = rdd.map { name => (name.head, City(name)) }


// perform various operations on paired RDD
kv.countByKey // Map[Char, Long]

kv.groupByKey // RDD[(Char, Iterable[City])]

val reducedKV = kv.map { case (letter: Char, city: City) => (letter, List(city)) }.reduceByKey { (x,y) => x ++ y } // RDD[(Char List[City])]
reducedKV.collect.foreach(println)

val reducedValues = reducedKV.map { case (letter: Char, list: List[City]) => list }
reducedValues.flatMap { _.map(_.name) } // RDD[String]



