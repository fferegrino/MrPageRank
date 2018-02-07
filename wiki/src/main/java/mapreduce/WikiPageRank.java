package mapreduce;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import mapreduce.datatypes.WikiInputValue;
import mapreduce.input.WikiInputFormat;
import mapreduce.mapping.ArticleMapper;
import mapreduce.reducing.ArticleReducer;

public class WikiPageRank extends Configured implements Tool{

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new WikiPageRank(), args));
	}
	
	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		conf.set("mapreduce.map.java.opts","-Xmx1843M -Dfile.encoding=UTF-8");
		Job job = Job.getInstance(conf);
		
		
		job.setJobName("Mighty-WikiPageRank(" + args[0] + ")");
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(WikiInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Mapping configuration
		job.setMapperClass(ArticleMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WikiInputValue.class);
		
		// Reducer configuration
		job.setReducerClass(ArticleReducer.class);
		

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
