import org.apache.spark.sql.{DataFrame, SparkSession}

object df3 extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()

  import spark.implicits._

  val columns = Seq("language", "users_count")




  val numbers = List(1, 2, 3, 4)
  val doubledNumbers = numbers.map(_ * 2)
  val df2= doubledNumbers.toDF()
  df2.show()


  val flatMap = columns.flatMap(_.toUpperCase())
   val map= columns.map(_.toUpperCase)
  println("map"+map)
  println("flatmap"+flatMap)


  }



