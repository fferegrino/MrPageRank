package testing;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import mapreduce.WikiInputFormat;
import mapreduce.WikiMultiRecordReader;

public class InputFormatTest {
	
	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration conf = new Configuration(false);
		conf.set("fs.default.name", "file:///");

		File testFile = new File("/Users/fferegrino/Downloads/enwiki-20080103-sample.txt");
		Path path = new Path(testFile.getAbsoluteFile().toURI());
		FileSplit split = new FileSplit(path, 0, testFile.length(), null);

		InputFormat inputFormat = ReflectionUtils.newInstance(WikiInputFormat.class, conf);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		WikiMultiRecordReader reader = (WikiMultiRecordReader) inputFormat.createRecordReader(split, context);

		reader.initialize(split, context);
		
		reader.nextKeyValue();
		printCurrent(reader);
		reader.nextKeyValue();
		printCurrent(reader);
		reader.nextKeyValue();
		printCurrent(reader);
		reader.nextKeyValue();
		printCurrent(reader);
		reader.nextKeyValue();
		printCurrent(reader);
		reader.nextKeyValue();
		printCurrent(reader);
		reader.nextKeyValue();
		printCurrent(reader);
		
	}
	
	public static void printCurrent(WikiMultiRecordReader wmrr) {
		String key = wmrr.getCurrentKey().toString();
		String val = wmrr.getCurrentValue().toString();
		
		System.out.println(key + ": " + val);
	}

}
