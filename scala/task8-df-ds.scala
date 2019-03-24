// read JSON file to DF
val inputData = "datasets/data1.json"
val raw = spark.read.json(inputData)

// select specific columns
val selected = raw.select('name, 'country, 'continent, 'population)

// get distinct rows
val distinct = selected.distinct

// drop null rows
val dropped = distinct.na.drop("all")

//fill null values
val filled = dropped.na.fill(Map("continent" -> "Earth", "population" -> 0))

val df = filled

case class City(name: String, country: String, continent: String, population: Long)
val ds = df.as[City]

// DS
ds.printSchema
ds.take(4)
ds.filter(city => city.name == "Moscow").show
ds.map(city => "I love " + city.name).show


// DF
df.map(city => "I love " + city(0)).show
df.map(city => "I love " + city.getAs[String]("name")).show
