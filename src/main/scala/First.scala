
import org.apache.spark.{SparkConf, SparkContext}

object A {
  def main(args: Array[String]): Unit = {
    // Create a SparkConf and SparkContext
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Specify the input file path (update the path as needed)
    val inputFile = "path/to/input/file.txt"

    // Read the input file
    val inputRDD = sc.textFile(inputFile)

    // Perform the word count
    val wordCounts = inputRDD
      .flatMap(line => line.split("\\s+")) // Split each line into words
      .map(word => (word, 1)) // Create a pair (word, 1)
      .reduceByKey(_ + _) // Aggregate counts by word

    // Collect and print the results
    wordCounts.collect().foreach { case (word, count) =>
      println(s"$word: $count")
    }

    // Optionally save the result to an output file
    // val outputPath = "path/to/output/directory"
    // wordCounts.saveAsTextFile(outputPath)

    // Stop the SparkContext
    sc.stop()
  }
}