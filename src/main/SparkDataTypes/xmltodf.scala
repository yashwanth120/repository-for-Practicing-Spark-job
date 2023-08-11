import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object xmltodf extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()


  val df: DataFrame = spark.read
    .format("xml")
    .option("rowTag", "book")
    .load("/Users/manojreddy/Desktop/sample/books.xml")
     df.show()
}
