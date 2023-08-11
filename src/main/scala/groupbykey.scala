import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object group extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()


  val data = Seq(("manoj", 85),
    ("manoj", 90),
    ("manoj", 75),
    ("yashwanth", 65),
    ("yashwanth", 88),
    ("yashwanth", 95),
    ("pradeep", 99),
    ("pradeep", 79),
    ("pradeep", 29)
  )


  val rdd = spark.sparkContext.parallelize(data)


  val groupedData = rdd.groupByKey()


  val aggregatedData = groupedData.mapValues(scores => (scores.sum, scores.size, scores.max, scores.min))
  aggregatedData.foreach { case (name, (sum, count, maxScore, minScore)) =>
    println(s"Name: $name, Average_Score: ${sum.toDouble / count}, Max_Score: $maxScore, Min_Score: $minScore, Subject_Count: $count")
  }
  val totalScores = rdd.reduceByKey(_ + _)
  totalScores.foreach { case (name, totalScore) =>
    println(s"Name: $name, Total Score: $totalScore")


}}