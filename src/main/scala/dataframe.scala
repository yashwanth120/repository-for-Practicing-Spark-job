import org.apache.spark.sql.SparkSession

object dataframe extends App{
  val spark = SparkSession.
    builder().master("local").appName("dataframe").getOrCreate()
  val df = spark.read.format(source = "csv").option("header", "true").option("inferSchema", "true").load(path = "src/main/resources/industry.csv")
  df.show()

}
