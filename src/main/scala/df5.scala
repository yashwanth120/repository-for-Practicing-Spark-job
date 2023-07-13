
import org.apache.spark.sql.{DataFrame, SparkSession}


object df5 extends App {
    val spark = SparkSession.
      builder().master("local").appName("map").getOrCreate()



    val info = new Array[String](5)
     info(0)="a"
      info(1)="b"
  info(2)="c"
  info(3)="d"
  info(4)="e"

  println(info(3))

  var list= List(1,2,3,4,5)
  list=list.map(x=>x+5)
  println(list)

  var list2=List("hello how are you bro")
  var list3=list2.flatMap(_.toUpperCase())
    println(list3)
}

