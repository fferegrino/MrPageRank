package mapreduce.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WikiIntermediatePageRankValue implements Writable  {
	
	private FloatWritable pageRank = new FloatWritable();
	private IntWritable parentOutlinksNumber = new IntWritable();
	private Text parent = new Text();
	private Text outlinks = new Text();

	@Override
	public void readFields(DataInput in) throws IOException {
		pageRank.readFields(in);
		parentOutlinksNumber.readFields(in);
		parent.readFields(in);
		outlinks.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		pageRank.write(out);
		parentOutlinksNumber.write(out);
		parent.write(out);
		outlinks.write(out);
	}

	public float getPageRank() {
		return pageRank.get();
	}

	public void setPageRank(float pageRank) {
		this.pageRank = new FloatWritable(pageRank);
	}

	public int getParentOutlinksNumber() {
		return parentOutlinksNumber.get();
	}

	public void setParentOutlinksNumber(int parentOutlinksNumber) {
		this.parentOutlinksNumber = new IntWritable(parentOutlinksNumber);
	}

	public String getParent() {
		return parent.toString();
	}

	public void setParent(String parent) {
		this.parent = new Text(parent);
	}

	public String getOutlinks() {
		return outlinks.toString();
	}

	public void setOutlinks(String outlinks) {
		this.outlinks = new Text(outlinks);
	}

}
