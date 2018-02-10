package mapreduce;

import java.util.ArrayList;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import mapreduce.datatypes.*;
import mapreduce.input.*;
import mapreduce.mapping.*;
import mapreduce.output.PageRankOutputFormat;
import mapreduce.reducing.*;

public class WikiPageRank extends Configured implements Tool{

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new WikiPageRank(), args));
	}
	
	@Override
	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		
		conf.set("mapreduce.map.java.opts","-Xmx1843M");
		FileSystem fsys = FileSystem.get(conf);
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Path intermediate = new Path("inter0");

		fsys.delete(output, true);
		
		int numLoops = 5;
		if( args.length > 2) {
			numLoops = Integer.parseInt(args[2]);
		}

		Job cleaningJob = Job.getInstance(conf);		
		
		FileInputFormat.setInputPaths(cleaningJob, input);
		FileOutputFormat.setOutputPath(cleaningJob, intermediate);
		
		cleaningJob.setJobName("Mighty-WikiPageRank_1(" + args[0] + ")");
		cleaningJob.setJarByClass(getClass());
		
		cleaningJob.setInputFormatClass(WikiInputFormat.class);
		cleaningJob.setOutputFormatClass(TextOutputFormat.class);

		// Mapping configuration
		cleaningJob.setMapperClass(ArticleMapper.class);
		cleaningJob.setMapOutputKeyClass(Text.class);
		cleaningJob.setMapOutputValueClass(WikiInputValue.class);
		
		// Reducer configuration
		cleaningJob.setReducerClass(ArticleReducer.class);
		// wait for completion
		cleaningJob.waitForCompletion(true);

		Path previousPath = intermediate;
		
		boolean succeeded = false;
		for(int currentLoop = 1; currentLoop < numLoops +1; currentLoop++) {
			Path nextPath = null;
			
			Job pageRankJob = Job.getInstance(conf);

			pageRankJob.setJarByClass(getClass());
			pageRankJob.setJobName("Mighty-WikiPageRank_2( Loop: "+ currentLoop +" )");
			
			// Mapping configuration
			pageRankJob.setMapperClass(PageRankMapper.class);
			pageRankJob.setMapOutputKeyClass(Text.class);
			pageRankJob.setMapOutputValueClass(WikiIntermediatePageRankValue.class);
			
			// Reducer configuration
			pageRankJob.setReducerClass(PageRankReducer.class);
			
			if (currentLoop == numLoops) { // Es la última corrida
				pageRankJob.setOutputFormatClass(PageRankOutputFormat.class);
				nextPath = output;
			}
			else {
				pageRankJob.setOutputFormatClass(TextOutputFormat.class);
				nextPath = new Path("inter" + currentLoop );
			}

			FileInputFormat.setInputPaths(pageRankJob, previousPath);
			FileOutputFormat.setOutputPath(pageRankJob, nextPath);
			
			pageRankJob.setInputFormatClass(TextInputFormat.class);
			
			succeeded = pageRankJob.waitForCompletion(true);

			fsys.delete(previousPath, true);
			
			previousPath = nextPath;
			if (!succeeded) {
				break;
			}
		}

		return (succeeded ? 0 : 1);
	}

}
