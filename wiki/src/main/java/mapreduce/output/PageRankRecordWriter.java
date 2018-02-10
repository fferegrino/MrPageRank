package mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import mapreduce.datatypes.WikiInOutPageRankValue;

public class PageRankRecordWriter extends RecordWriter<Text, WikiInOutPageRankValue> {
	
	private DataOutputStream out;
	static final String separator = " ";
	
	public PageRankRecordWriter(DataOutputStream out) {
		this.out = out;
	}

	@Override
	public void write(Text key, WikiInOutPageRankValue value) throws IOException, InterruptedException {
		String outKey = key.toString();
		float pageRank = value.getPageRank();
		out.writeBytes(outKey + separator + pageRank + System.getProperty("line.separator"));
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();
	}

}
