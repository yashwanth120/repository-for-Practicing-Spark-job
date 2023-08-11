import org.apache.spark.{SparkConf, SparkContext}

object RDDExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDDCreationExample").setMaster("local[*]")


    val sc = new SparkContext(conf)


    val data = Array(1, 2, 3, 4, 5)
    val rdd = sc.parallelize(data)

    val sum = rdd.reduce(_ + _)
    println("Sum of elements" + sum)



    sc.stop()
  }
}