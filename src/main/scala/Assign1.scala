import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, when,max}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Assign1 {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    val salesData = Seq(
      ("Product1", "Category1", 100),
      ("Product2", "Category2", 200),
      ("Product3", "Category1", 150),
      ("Product4", "Category3", 300),
      ("Product5", "Category2", 250),
      ("Product6", "Category3", 180),
      ("Product1", "Category2", 200),
      ("Product2", "Category3", 300),
      ("Product3", "Category1", 450),
      ("Product4", "Category4", 600),
      ("Product5", "Category2", 750),
      ("Product6", "Category1", 880)
    ).toDF("Product", "Category", "Revenue")


    val windowspec = Window.partitionBy(col("Product")).orderBy(col("Revenue"))

    val df = salesData.select(max(col("Revenue")).over(windowspec).as("details"))

    df.show()

  }
}