package mapreduce.datatypes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents the input of the cleaning stage for the PageRank job
 *
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class WikiInputValue implements Writable {

    private Text outlinks;
    private LongWritable revisionId;
    private IntWritable outlinksNumber;

    public WikiInputValue() {
        outlinks = new Text();
        outlinksNumber = new IntWritable();
        revisionId = new LongWritable();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        outlinks.readFields(in);
        outlinksNumber.readFields(in);
        revisionId.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        outlinks.write(out);
        outlinksNumber.write(out);
        revisionId.write(out);
    }

    @Override
    public String toString() {
        return revisionId + " " + outlinksNumber + " " + outlinks;
    }

    public long getRevisionId() {
        return revisionId.get();
    }

    public void setRevisionId(long revisionId) {
        this.revisionId = new LongWritable(revisionId);
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
