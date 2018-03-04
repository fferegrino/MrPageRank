package wordCount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount-v0"));
		sc.textFile(args[0])
		.flatMap(s -> Arrays.asList(s.split(" ")))
		.mapToPair(s -> new Tuple2<String, Integer>(s, 1))
		.reduceByKey((x, y) -> x + y)
		.saveAsTextFile(args[1]);
		sc.close();
	}
}
