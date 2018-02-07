package mapreduce.mapping;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mapreduce.datatypes.WikiInputValue;

public class ArticleMapper extends Mapper<Text, WikiInputValue, Text, WikiInputValue>  {
	
	static enum MapperCounters {
		TOTAL_WIKI_ARTICLES
	}
	
	@Override
	protected void map(Text key, WikiInputValue value,
			Mapper<Text, WikiInputValue, Text, WikiInputValue>.Context context)
			throws IOException, InterruptedException {

		context.write(key, value);

		context.getCounter(MapperCounters.TOTAL_WIKI_ARTICLES).increment(1);
	}

}
