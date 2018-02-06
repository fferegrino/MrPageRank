package mapreduce;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class WikiMultiRecordReader extends RecordReader<LongWritable, Text>{
	private static final byte[] recordSeparator = "\n\n".getBytes();
	private FSDataInputStream fsin;
	private long start, end;
	private boolean stillInChunk = true;
	private DataOutputBuffer buffer = new DataOutputBuffer();
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	
	
	/** 
	 * Initialization method that configures node's context for reading an input 
	 * split text file (parsed version of the complete Wikipedia edit history).
	 * @param inputSplit is a logical chunk of data that points to start and end 
	 * locations within physical blocks.
	 * @param context object contains configuration data to interact with Hadoop's
	 * environment (system).
	 */
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException{
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		
		fsin = fs.open(path);
		start = split.getStart();
		end = split.getStart() + split.getLength();
		fsin.seek(start);
		
		if(start != 0) {
			readRecord(false);
		}
	}
	
	private boolean readRecord(boolean withinBlock) throws IOException{
		int i = 0, b;
		while(true) {
			if((b = fsin.read()) == -1) {
				return false;
			}
			if(withinBlock) {
				buffer.write(b);
			}
			if(b == recordSeparator[i]) {
				if(++i == recordSeparator.length) {
					return fsin.getPos() < end;
				}
			} else {
				i=0;
			}
		}
	}
	
	public boolean nextKeyValue() throws IOException {
		if(!stillInChunk) {
			return false;
		}
		boolean status = readRecord(true);
		value = new Text();
		value.set(buffer.getData(), 0, buffer.getLength());
		key.set(fsin.getPos());
		buffer.reset();
		if(!status) {
			stillInChunk = false;
		}
		return true;		
	}
	
	public LongWritable getCurrentKey() {
		return key;
	}
	
	public Text getCurrentValue() {
		return value;
	}
	
	public float getProgress() throws IOException{
		return (float) (fsin.getPos() - start) / (end - start);
	}
	
	public void close() throws IOException{
		fsin.close();
	}
}