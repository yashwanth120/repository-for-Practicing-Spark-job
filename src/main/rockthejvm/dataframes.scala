package rockthejvm
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
object dataframes extends App {
  val spark = SparkSession.
    builder().master("local").appName("spark").getOrCreate()

  val carsDF= spark.read.option("inferSchema","true").json("src/main/resources/cars.json")
    carsDF.show()

  val firstColumn=carsDF.col("Name")
  val carNames= carsDF.select(firstColumn)
  carNames.show()

  carsDF.select("Name", "Year")
  val weightinkg=carsDF.col("Weight_in_lbs") / 2.2

  val carswithweight= carsDF.select (col("name"),col("Weight_in_lbs"),weightinkg as("Weight in KG"))
  carswithweight.show()


  val carsWithKg3DF = carsDF.withColumn ("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  val carsWithColumnRenamed = carsDF.withColumnRenamed ("Weight_in_lbs", "Weight in pounds")
  carsWithColumnRenamed.selectExpr("Weight in pounds")






}
