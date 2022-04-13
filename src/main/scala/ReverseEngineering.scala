import org.apache.spark.sql.types.IntegerType

object ReverseEngineering extends App {

  import org.apache.spark.sql.SparkSession

  val path = if (args.length > 0) args(0)
  else "/home/adam/IdeaProjects/reverse-engineering/input.csv"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val input = spark
    .read
    .option("truncate", "true")
    .option("comment", "+")
    .option("delimiter", "|")
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/adam/IdeaProjects/reverse-engineering/input.csv")
    .drop($"_c0")
    .drop($"_c4")
}
