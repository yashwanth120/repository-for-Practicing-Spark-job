package scala

import org.apache.commons.lang3.BooleanUtils.and
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, max, mean, min, sqrt, stddev, sum}
import org.apache.spark.sql.{DataFrame, SparkSession, expressions}
import rockthejvm.aggregations.director_stats

import scala.language.postfixOps


object actions extends App {
  val spark = SparkSession.
    builder().master("local").appName("spark").getOrCreate()


  val mydf = spark.read.option("inferSchema", "true").json("src/main/resources/cars.json")
//reduce
  val mylist = List(1, 2, 3, 4, 5, 6, 7)
  val listnew=mylist.reduce((a, b) => a * b)
   println(listnew)


  // collect

  val sample =mydf.collect()
  sample.foreach(println)

//count
 val count = mydf.count()
  println(count)

val distcount= mydf.select("Origin").distinct()
 val origincount = distcount.count()
  println(origincount)

  //first

 val firstrow=  mydf.first()
  println(firstrow)
// take only works with lists and arrays
  val takeelements= mylist.take(3)
  println(takeelements)

//save as textfile
  val filterdf= {
    mydf.filter(col("Origin" ) === " Japan")

  }

filterdf.write.format("src/main/resources/japancars.txt")

}
