package mapreduce.input;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import mapreduce.datatypes.WikiInputValue;

public class WikiInputFormat extends FileInputFormat<Text, WikiInputValue>{
	public RecordReader<Text, WikiInputValue> createRecordReader(InputSplit split, TaskAttemptContext context){
		return new WikiMultiRecordReader();
	}
}