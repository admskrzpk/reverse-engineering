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
    .show(truncate = false)


  //val newFile = input.map(_.mkString("")).map(_.replaceAll("[+ -]", ""))

  //.toDF("id", "text1", "text2").show()

  //(_.mkString("")).map(_.replaceAll("[|,+]", "")).toDF("id, Text1, Text2")


  //val inputFormatted = input.map(_.mkString("|"))

}
