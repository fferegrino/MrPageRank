package mapreduce;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import mapreduce.input.WikiInputFormat;

public class WikiPageRank extends Configured implements Tool{

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new WikiPageRank(), args));
	}
	
	public int run(String[] args) throws Exception{
		Job job = Job.getInstance(getConf());
		job.setJobName("Mighty-WikiPageRank(" + args[0] + ")");
		job.setJarByClass(getClass());
		job.setInputFormatClass(WikiInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//more components
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
