package mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class WikiInputFormat extends FileInputFormat<LongWritable, Text>{
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context){
		return new WikiMultiRecordReader();
	}
}