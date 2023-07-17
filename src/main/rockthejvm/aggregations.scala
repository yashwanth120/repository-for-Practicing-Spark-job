package rockthejvm
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, max, mean, min, stddev, sum}
import org.apache.spark.sql.{DataFrame, SparkSession, expressions}
import rockthejvm.aggregations.director_stats

import scala.language.postfixOps


object aggregations extends App {
  val spark = SparkSession.
    builder().master("local").appName("spark").getOrCreate()

  val moviesDF1 = spark.read.option("inferSchema", "true").json("src/main/resources/movies.json")

  val genres_count = moviesDF1.select(count(col("Major_Genre")))
      genres_count.show()
  val distinct_genres= moviesDF1.select((countDistinct("Major_Genre")))
      distinct_genres.show()
  val min_rating= moviesDF1.select(min(col("IMDB_Rating")))
  val max_rating= moviesDF1.select(max(col("IMDB_Rating")))
  min_rating.show()
  max_rating.show()

  val sum_votes=moviesDF1.select(sum(col("IMDB_Votes")))
  val avg_votes=moviesDF1.select(avg(col("IMDB_Votes")))
    sum_votes.show()
  avg_votes.show()

  val countby_genre= moviesDF1.groupBy(col("Major_Genre")).count()
  countby_genre.show()
  //Exercise


  //1 sum of profits of all movies

  val sum_profits= moviesDF1.select(sum(col("US_Gross")+col("Worldwide_Gross")+col("US_DVD_Sales"))).as("profits")
    sum_profits.show()

  //count of distinct directors

  val dist_directors= moviesDF1.select(countDistinct("Director"))
  dist_directors.show()

  val us_gross_mean=moviesDF1.select(mean(col("US_Gross")))
  val us_gross_sd=moviesDF1.select(stddev(col("US_Gross")))
  us_gross_sd.show()
  us_gross_mean.show()

  val director_stats = moviesDF1.groupBy("Director")
    .agg(
      avg(col("IMDB_Rating").as("Average_rating") ),
      sum(col("US_Gross").as ("gross"))
    ).show()

}