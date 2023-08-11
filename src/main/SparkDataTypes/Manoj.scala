import java.sql.DriverManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
object Manoj extends App {

  val spark=SparkSession.builder()
    .master("local")
    .appName("Read Data from csv")
    .getOrCreate()

   val properties = new java.util.Properties()
   properties.put("user", "MANOJ8799")
   properties.put("password", "Manoj@321")
   properties.put("account", "su22025")
   properties.put("warehouse", "manoj")
   properties.put("db", "manoj")
   properties.put("schema", "public")
   properties.put("role", "ACCOUNTADMIN")


   val jdbcUrl = "jdbc:snowflake://su22025.south-central-us.azure.snowflakecomputing.com/"
   println("Created JDBC connection")

   val connection = DriverManager.getConnection(jdbcUrl, properties)
   println("Done creating JDBC connection")
   println("Created JDBC statement")
   val statement = connection.createStatement

  statement.executeUpdate("create or replace table world_cups(year number,host_country varchar,winner varchar,runners_up varchar,third varchar,fourth varchar, goals_scored number, qualified_teams number, matches_played number)")


  println("Done loading data")

  println("End connection")

  statement.close()
  connection.close()


}