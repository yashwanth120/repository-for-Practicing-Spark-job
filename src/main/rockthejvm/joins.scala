
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps


object joins extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()

  val guitarDF = spark.read.option("inferSchema", "true").json("src/main/resources/guitars.json")

  val guitarplayersDF=spark.read.option("inferSchema", "true").json("src/main/resources/guitarPlayers.json")
  val bandsDF=spark.read.option("inferSchema", "true").json("src/main/resources/bands.json")

  val guitarbandsDF= guitarplayersDF.join(bandsDF, guitarplayersDF.col("band")=== bandsDF.col("id"),"inner")
  guitarbandsDF.show()

  guitarplayersDF.join(bandsDF, guitarplayersDF.col("band")=== bandsDF.col("id"),"left_outer").show()



  guitarplayersDF.join(bandsDF, guitarplayersDF.col("band")=== bandsDF.col("id"),"right_outer").show()

  guitarplayersDF.join(bandsDF, guitarplayersDF.col("band")=== bandsDF.col("id"),"outer").show()


  guitarplayersDF.join(bandsDF, guitarplayersDF.col("band")=== bandsDF.col("id"),"left_semi").show()

  guitarplayersDF.join(bandsDF, guitarplayersDF.col("band")=== bandsDF.col("id"),"left_anti").show()

}