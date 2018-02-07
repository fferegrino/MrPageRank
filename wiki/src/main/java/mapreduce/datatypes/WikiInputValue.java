package mapreduce.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WikiInputValue implements Writable {
	
	private Text outlinks;
	private LongWritable revisionId;
	
	public WikiInputValue()
	{
		outlinks = new Text();
		revisionId = new LongWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		outlinks.readFields(in);
		revisionId.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		outlinks.write(out);
		revisionId.write(out);
	}
	
	@Override
	public String toString() {
		return revisionId + " " + outlinks;
	}

	public long getRevisionId() {
		return revisionId.get();
	}

	public LongWritable getUnderlyingRevisionId() {
		return revisionId;
	}

	public void setRevisionId(long revisionId) {
		this.revisionId = new LongWritable(revisionId);
	}

	public String getOutlinks() {
		return outlinks.toString();
	}

	public Text getUnderlyingOutlinks() {
		return outlinks;
	}

	public void setOutlinks(String outlinks) {
		this.outlinks = new Text(outlinks);
	}

}
