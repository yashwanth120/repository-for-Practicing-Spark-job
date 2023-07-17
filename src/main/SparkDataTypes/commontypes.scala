
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object commontypes extends App {
  val spark = SparkSession.
    builder().master("local").appName("spark").getOrCreate()

  val moviesDF=spark.read.option("inferSchema", "true").json("src/main/resources/movies.json")
   moviesDF.select(col("Title"), lit(25)as "Plain value").show()

 val dramafilter=moviesDF.select(col("Movie_Genre")=== "Drama")
  val goodratingfilter= moviesDF.select(col("IMDB_Rating")>7.0)



}


