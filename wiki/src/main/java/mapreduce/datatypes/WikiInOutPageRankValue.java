package mapreduce.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WikiInOutPageRankValue implements Writable  {
	
	private FloatWritable pageRank;
	private IntWritable outlinksNumber;
	private Text outlinks;

	@Override
	public void readFields(DataInput in) throws IOException {
		pageRank.readFields(in);
		outlinksNumber.readFields(in);
		outlinks.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		pageRank.write(out);
		outlinksNumber.write(out);
		outlinks.write(out);
	}
	
	@Override
	public String toString() {
		return getPageRank() +  "|" + getOutlinksNumber() + "|" + getOutlinks();
	}

	public float getPageRank() {
		return pageRank.get();
	}

	public void setPageRank(float pageRank) {
		this.pageRank = new FloatWritable(pageRank);
	}

	public int getOutlinksNumber() {
		return outlinksNumber.get();
	}

	public void setOutlinksNumber(int outlinksNumber) {
		this.outlinksNumber = new IntWritable(outlinksNumber);
	}

	public String getOutlinks() {
		return outlinks.toString();
	}

	public void setOutlinks(String outlinks) {
		this.outlinks = new Text(outlinks);
	}

}
