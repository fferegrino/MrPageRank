package pageRank;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import pageRank.dataTypes.WikipediaRevision;
import scala.Tuple2;

public class Original {
	public static void main(String[] args) {
		String inputFile = args[0];
		String outputFile = args[1];
		int iterations = Integer.parseInt(args[2]);
		
		SparkConf conf = new SparkConf().setAppName("Original Page Rank");
		Configuration hadoopConf = new Configuration();
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.hadoopConfiguration().set("textinputformat.record.delimiter","\n\n");
		
		JavaRDD<WikipediaRevision> revisions = sc.newAPIHadoopFile(inputFile, TextInputFormat.class, 
				LongWritable.class, Text.class,hadoopConf)
		.map(v -> v._2.toString())
		.map(record -> {
			String[] lines = record.split("\n");
			String[] revision = lines[0].split(" ");
			Long revisionId =  Long.parseLong(revision[2]);
			String articleId = revision[3].trim();
			String[] mainArray = lines [3].split(" ", 2);
			String outlinks = null;
			if(mainArray.length == 2) {
				outlinks = mainArray[1];
			}
			return new WikipediaRevision(articleId, revisionId, outlinks);
		});
		
		JavaRDD<WikipediaRevision> singleRevisions = revisions.groupBy(revision -> revision.getArticleId())
		.mapValues(revs -> {
			long latestRevisionId  = 0;
	        String articleId = null;
	        String outlinks = null;
	        for(WikipediaRevision revision : revs)
	        {
		          if(revision.getRevisionId() > latestRevisionId)
		          {
		            articleId = revision.getArticleId();
		            latestRevisionId = revision.getRevisionId();
		            outlinks = revision.getOutlinks();
		          }
	        }
	        return new WikipediaRevision(articleId, latestRevisionId, outlinks);
		}).map(t -> t._2);
		/*
		singleRevisions.mapToPair(r -> {
			
		});
		
		JavaPairRDD<String, Iterable<String>> links = lines
				.mapToPair( s ->
				String[] parts = s.split('\\s+');
				return new Tuple2<String, String>(parts[0], parts[1]); )
				.distinct().groupByKey().cache();
				*/
	}
}
