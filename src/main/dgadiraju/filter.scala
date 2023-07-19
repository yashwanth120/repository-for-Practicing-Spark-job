package dgadiraju
import org.apache.commons.lang3.BooleanUtils.and
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, max, mean, min, stddev, sum}
import org.apache.spark.sql.{DataFrame, SparkSession, expressions}
import rockthejvm.aggregations.director_stats

import scala.language.postfixOps


object filter extends App {
  val spark = SparkSession.
    builder().master("local").appName("spark").getOrCreate()

  val airline_path = "src/main/resources/airline_data.parquet"

  val airlines_all = spark.read.parquet(airline_path)

  val airlines= airlines_all.filter(col("IsArrDelayed")==="NO" and col("IsDepDelayed")==="YES" ).count()

airlines_all.filter("DepDelay > 60").show()


}