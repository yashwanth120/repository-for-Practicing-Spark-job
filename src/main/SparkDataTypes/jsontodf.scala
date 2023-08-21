
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.types.{StructType,DataType}

object jsontodf extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()



  val df = spark.read.option("multiLine", "true").json("/Users/manojreddy/Desktop/sample/sample.json")

  // Flatten the DataFrame
  val flattenedDF = df.select(
    col("coffee.region").alias("coffee_region"),
    col("coffee.country").alias("coffee_country"),
    col("brewing.region").alias("brewing_region"),
    col("brewing.country").alias("brewing_country")
  )
    .selectExpr(
      "coffee_region[0].id as coffee_region_id",
      "coffee_region[0].name as coffee_region_name",
      "coffee_country.id as coffee_country_id",
      "coffee_country.company as coffee_company",
      "brewing_region[0].id as brewing_region_id",
      "brewing_region[0].name as brewing_region_name",
      "brewing_country.id as brewing_country_id",
      "brewing_country.company as brewing_company"
    )

  flattenedDF.show()

}