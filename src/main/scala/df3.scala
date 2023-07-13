import org.apache.spark.sql.{DataFrame, SparkSession}

object df3 extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()

  import spark.implicits._

  val columns = Seq("language", "users_count")
  val data = Seq(("Java", "200"), ("Python", "100"), ("Scala", "300"))

val df1= data.toDF()
  df1.show()

  val numbers = List(1, 2, 3, 4)
  val doubledNumbers = numbers.map(_ * 2)
  val df2= doubledNumbers.toDF()
  df2.show()


  val flatdf = columns.flatMap(_.toUpperCase())

  println("flatmap"+flatdf)
  }



