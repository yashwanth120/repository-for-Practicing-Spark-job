import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, explode, expr}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object xmltodf extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()


  val df: DataFrame = spark.read
    .format("xml")
    .option("rowTag", "item")
    .load("/Users/manojreddy/Desktop/sample/sample.xml")
     df.show()

  val explodedDf = df
    .withColumn("batter", explode(col("item.batters.batter")))
    .withColumn("topping", explode(col("item.topping")))
    .withColumn("filling", explode(col("item.fillings.filling")))
  // Add more explode columns as needed

  explodedDf.show(false)

}
