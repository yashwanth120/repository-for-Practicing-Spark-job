
import org.apache.spark.sql.{DataFrame, SparkSession}

object df2 extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()


  val data = Seq("manoj",
    "kumar",
    "reddy",
  )

  import spark.sqlContext.implicits._
  val df = data.toDF("data")
  df.show()






}