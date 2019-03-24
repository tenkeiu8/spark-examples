// create local List[String]
val dataset = List("Moscow", "Madrid", "Paris", "Berlin", "Barselona", "Cairo", "Perm")

// create RDD[String] from List[String]
val rdd = spark.sparkContext.parallelize(dataset)

// create key-value pair RDD
case class City(name: String)
val kv = rdd.map { name => (name.head, City(name)) }

kv.partitioner
kv.getNumPartitions

import org.apache.spark.Partitioner

class CustomPartitioner extends Partitioner {
  override def numPartitions(): Int = Runtime.getRuntime.availableProcessors
  override def getPartition(key: Any): Int = {
    key match {
      case k: Int => k % this.numPartitions
      case k: Char => k.toLower.toInt % this.numPartitions
      case k => throw new IllegalArgumentException(s"Key of type ${k.getClass} is not supported")
    }
  }
}

val customPartitioner = new CustomPartitioner()

val part = kv.partitionBy(customPartitioner)
part.getNumPartitions
part.partitioner
part.glom.take(2)
