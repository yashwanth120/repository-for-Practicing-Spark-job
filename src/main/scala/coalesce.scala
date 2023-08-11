import org.apache.spark.sql.{DataFrame, SparkSession}


object coalesce extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()

  val df: DataFrame = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/industries.csv")

  val coalescedf = df.coalesce(1)


  coalescedf.write
    .option("header", "true")
    .csv("src/main/resources/industry_single.csv")
}

