package rockthejvm
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession, expressions}
object moviesDF extends App {
  val spark = SparkSession.
    builder().master("local").appName("spark").getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/movies.json")

  moviesDF.show()
  val newdf= moviesDF.select("Title", "Release_Date")
  newdf.show()

  val gross_collections= moviesDF.withColumn("Gross_income",col("US_Gross")+ col("Worldwide_Gross"))
   gross_collections.show()
  val comedy_movies= moviesDF.select("Title","IMDB_Rating")
    .where(col("Major_Genre")=== "Comedy" )
    .where(col("IMDB_Rating") > 7.0)
  comedy_movies.show()
}