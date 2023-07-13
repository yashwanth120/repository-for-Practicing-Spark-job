import org.apache.spark.sql.{DataFrame, SparkSession}

object df extends App {
  def flatMap(function: Nothing => Any) = ???

  val spark = SparkSession.
    builder().master("local").appName("dataframe").getOrCreate()

  val data = Seq(
    ("manoj", 20),
    ("kumar", 21),
    ("reddy", 22)
  )

  val df: DataFrame = spark.createDataFrame(data).toDF("name", "age")


  df.show()

  }
