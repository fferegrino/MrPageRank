package mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class WikiInputFormat extends FileInputFormat<Text, Text>{
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context){
		return new WikiMultiRecordReader();
	}
}