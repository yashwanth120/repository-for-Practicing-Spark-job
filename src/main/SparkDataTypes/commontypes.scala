
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import scala.language.postfixOps


object commontypes extends App {
  val spark = SparkSession.
    builder().master("local").appName("spark").getOrCreate()

  val moviesDF=spark.read.option("inferSchema", "true").json("src/main/resources/movies.json")
   moviesDF.select(col("Title"), lit(25)as "Plain value").show()

 val dramafilter=col("Movie_Genre")=== "Drama"
  val goodratingfilter= col("IMDB_Rating")>7.0
  val goodmovie= dramafilter and goodratingfilter

  moviesDF.select("Title").where(goodratingfilter).show()

  


}


