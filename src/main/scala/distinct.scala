import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object distinct extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()

  val data = Seq(
    ("manoj", 24),
    ("manoj", 24),
    ("manoj", 25),
    ("yashwanth", 28),
    ("yashwanth", 28),
    ("yashwanth", 29),
    ("pradeep", 35),
    ("pradeep", 35), ("pradeep", 45)
  )

  val df = spark.createDataFrame(data).toDF("Name", "age")

  val distinct = df.distinct()
  distinct.show()

}
