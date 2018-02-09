package mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import mapreduce.datatypes.WikiInOutPageRankValue;

public class PageRankOutputFormat extends TextOutputFormat<Text, WikiInOutPageRankValue> {
	
	@Override
	public RecordWriter<Text, WikiInOutPageRankValue> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {

		Path file = getDefaultWorkFile(job, ".txt");
		 
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		 
		FSDataOutputStream fileOut = fs.create(file, false);
		 
		return new PageRankRecordWriter(fileOut);
		
	}


}
