import org.apache.spark.sql.{DataFrame, SparkSession}


object repart extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()

  val df: DataFrame = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/industry.csv")

  val repartitionedDF = df.repartition(5)


  repartitionedDF.write
    .option("header", "true")
    .csv("src/main/resources/industries.csv")


}