import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import scala.language.postfixOps


object temporarytable extends App {
  val spark = SparkSession.
    builder().master("local").appName("spark").getOrCreate()


  val temptableDF = spark.read.option("inferSchema", "true").json("src/main/resources/movies.json")
  temptableDF.show()
  temptableDF.createTempView("temporary_table")

  val output= spark.sql("select Major_Genre from temporary_table")
    output.show()

}