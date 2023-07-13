import org.apache.spark.sql.{DataFrame, SparkSession}


object loops extends App {
  val spark = SparkSession.
    builder().master("local").appName("map").getOrCreate()

  val a = Array("yellow", "black", "red")
  for {
    z<- a
  }
    print(z)

   for(x<-a) {
     var y=x.toUpperCase()
     print(y)
   }
}