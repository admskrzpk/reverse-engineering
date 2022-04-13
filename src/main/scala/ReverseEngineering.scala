
object ReverseEngineering extends App {

  import org.apache.spark.sql.SparkSession

  val path = if (args.length > 0) args(0)
  else "/home/adam/IdeaProjects/reverse-engineering/input.csv"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

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
    .toDF("id", "Text1", "Text2")
    .withColumn("id",'id.cast("Int"))
    .show()
}