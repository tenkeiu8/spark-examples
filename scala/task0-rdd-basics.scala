// create local List[String]
val dataset = List("Moscow", "Madrid", "Paris", "Berlin", "Barselona", "Cairo", "Perm")

// create RDD[String] from List[String]
val rdd = spark.sparkContext.parallelize(dataset)

// take first item of RDD
rdd.first()

// take 2 items from RDD
rdd.take(2)

// create new RDD with all values in lower case
val lowerCities = rdd.map { city => city.toLowerCase }

// create local Array[String] from lower case cities RDD
rdd.map { city => city.toLowerCase }.collect

// filter only cities starting with 'm'
val mCities = lowerCities.filter { city => city.startsWith("m") }

// add few words to each value
val favCities = mCities.map { city => "I love " + city }

// count letters in RDD
val count = favCities.map { _.length }.reduce { (x,y) => x + y }
