package mapreduce;

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
import mapreduce.reducing.*;

public class WikiPageRank extends Configured implements Tool{

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new WikiPageRank(), args));
	}
	
	public int run(String[] args) throws Exception{
		DistributedFileSystem dfs = new DistributedFileSystem();
		Configuration conf = getConf();
		conf.set("mapreduce.map.java.opts","-Xmx1843M");
		Job job1 = Job.getInstance(conf);
		
		Path interPath = new Path("interCleansing");
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		if (dfs.exists(interPath)) {
			dfs.delete(interPath, true);
		}
		FileOutputFormat.setOutputPath(job1, interPath);
		
		job1.setJobName("Mighty-WikiPageRank_1(" + args[0] + ")");
		job1.setJarByClass(getClass());
		
		job1.setInputFormatClass(WikiInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		// Mapping configuration
		job1.setMapperClass(ArticleMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(WikiInputValue.class);
		
		// Reducer configuration
		job1.setReducerClass(ArticleReducer.class);
		// wait for completion
		job1.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job2, interPath);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		job2.setJobName("Mighty-WikiPageRank_2(" + args[1] + ")");
		job2.setJarByClass(getClass());
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		// Mapping configuration
		job2.setMapperClass(PageRankMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(WikiIntermediatePageRankValue.class);
		
		// Reducer configuration
		job2.setReducerClass(PageRankReducer.class);
		
		dfs.close();
		// wait for completion
		return (job2.waitForCompletion(true) ? 0 : 1);
	}

}
