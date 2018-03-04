package wordCount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)

    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }

    // Save the word
    counts.saveAsTextFile(outputFile)
  }
}