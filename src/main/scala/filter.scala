import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object filter extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()

  val data= Seq(
    ("manoj", 24),
    ("yashwanth", 28),
    ("pradeep", 35)
       )

  val df= spark.createDataFrame(data).toDF("Name", "age")
  val filtered = {
    df.filter(col("age")> 25)
  }

  filtered.show()
}
