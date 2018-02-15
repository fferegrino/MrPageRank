package mapreduce.datatypes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable class that defines the input values for the data gathering and cleansing Mapping process.
 *
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class WikiInputValue implements Writable {

    //Out-links list (by article name)
    private Text outlinks;
    //Article revision id
    private LongWritable revisionId;
    //Out-links number (list count)
    private IntWritable outlinksNumber;

    /**
     * WikiInputValue default constructor method.
     */
    public WikiInputValue() {
        outlinks = new Text();
        outlinksNumber = new IntWritable();
        revisionId = new LongWritable();
    }

    /**
     * ReadFields method for deserializing DataInput fields.
     *
     * @param in contains selected Wikipedia article's attributes.
     * @throws IOException if deserializable input data can not be read.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        outlinks.readFields(in);
        outlinksNumber.readFields(in);
        revisionId.readFields(in);
    }

    /**
     * Write method for serializing DataOutput fields.
     *
     * @param out stores selected Wikipedia article's attributes.
     * @throws IOException if serializable output data can not be written.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        outlinks.write(out);
        outlinksNumber.write(out);
        revisionId.write(out);
    }

    /**
     * String representation for WikiInputValue class: "Revision id" + space delimiter + "out-links
     * number of elements" + space delimiter + "out-links elements list".
     *
     * @return the string description.
     */
    @Override
    public String toString() {
        return revisionId + " " + outlinksNumber + " " + outlinks;
    }

    /**
     * GetRevisionId method returns the article's revision id value according to the associated key.
     *
     * @return the revision id.
     */
    public long getRevisionId() {
        return revisionId.get();
    }

    /**
     * SetRevisionId method sets the article's revision id value according to the associated key.
     *
     * @param revisionId long value
     */
    public void setRevisionId(long revisionId) {
        this.revisionId = new LongWritable(revisionId);
    }

    /**
     * GetOutlinksNumber method returns the number of elements in the out-links list according to the
     * associated key.
     *
     * @return the number of out-links
     */
    public int getOutlinksNumber() {
        return outlinksNumber.get();
    }

    /**
     * SetOutlinksNumber method sets the number of elements in the out-links list according to the
     * associated key.
     *
     * @param outlinksNumber integer value.
     */
    public void setOutlinksNumber(int outlinksNumber) {
        this.outlinksNumber = new IntWritable(outlinksNumber);
    }

    /**
     * GetOutlinks method returns the elements in the out-links list according to the associated key.
     *
     * @return the string representation of the elements in the out-links list.
     */
    public String getOutlinks() {
        return outlinks.toString();
    }

    /**
     * SetOutlinks method sets the elements in the out-links list according to the associated key.
     *
     * @param out-links string list.
     */
    public void setOutlinks(String outlinks) {
        this.outlinks = new Text(outlinks);
    }
}
