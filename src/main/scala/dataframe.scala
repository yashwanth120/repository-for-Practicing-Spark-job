import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
object dataframe extends App{
  val spark = SparkSession.
    builder().master("local").appName("dataframe").getOrCreate()
  //val df = spark.read.format(source = "csv").option("header", "true").option("inferSchema", "true").load(path = "src/main/resources/industry.csv")
  //df.show()
  val list1= Seq((1,22),(2,23),(3,25))
  val rdd1=spark.sparkContext.parallelize(list1)
  val columns1 = Seq("ID", "Age")
  val df1 = spark.createDataFrame(rdd1).toDF(columns1: _*)
  val list2= Seq((1,"A"),(2,"B"),(3,"C"),(4,"D"))
  val rdd2=spark.sparkContext.parallelize(list2)
  val columns2 = Seq("ID", "Name")
  val df2 = spark.createDataFrame(rdd2).toDF(columns2: _*)
  val result1=df1.join(df2,Seq("ID"),"inner")
  result1.show()

}
