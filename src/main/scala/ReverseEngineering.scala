object ReverseEngineering extends App {

  import org.apache.spark.sql.SparkSession

  val path = if (args.length > 0) args(0)
  else "target.csv"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val input = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "|")
    .csv(path).toDF
    .show()

  //val inputFormatted = input.map(_.mkString("|"))

}
