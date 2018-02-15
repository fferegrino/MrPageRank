package mapreduce.datatypes;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable class that defines the value for linking the Mapping and Reducer processes
 * for the PageRank's calculation.
 *
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class WikiIntermediatePageRankValue implements Writable {
    //Page rank
    private FloatWritable pageRank = new FloatWritable();
    //Out-links number (list count) from referencing page
    private IntWritable parentOutlinksNumber = new IntWritable();
    //Article name for referencing page (parent)
    private Text parent = new Text();
    //Out-links list (by article name)
    private Text outlinks = new Text();

    /**
     * ReadFields method for deserializing DataInput fields.
     *
     * @param in contains PageRank's attributes required for calculation.
     * @throws IOException if deserializable input data can not be read.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        pageRank.readFields(in);
        parentOutlinksNumber.readFields(in);
        parent.readFields(in);
        outlinks.readFields(in);
    }

    /**
     * Write method for serializing DataOutput fields.
     *
     * @param out stores processed PageRank's attributes required for calculation.
     * @throws IOException if serializable output data can not be written.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        pageRank.write(out);
        parentOutlinksNumber.write(out);
        parent.write(out);
        outlinks.write(out);
    }

    /**
     * GetPageRank method returns the page rank value according to the associated key.
     *
     * @return the page rank.
     */
    public float getPageRank() {
        return pageRank.get();
    }

    /**
     * SetPageRank method sets the page rank value according to the associated key.
     *
     * @param pageRank float value.
     */
    public void setPageRank(float pageRank) {
        this.pageRank = new FloatWritable(pageRank);
    }

    /**
     * GetParentOutlinksNumber method returns the number of elements in the out-links list for the
     * referencing page (parent) according to the associated key.
     *
     * @return the referencing page's number of out-links
     */
    public int getParentOutlinksNumber() {
        return parentOutlinksNumber.get();
    }

    /**
     * SetParentOutlinksNumber method sets the number of elements in the out-links list for the
     * referencing page (parent) according to the associated key.
     *
     * @param parentOutlinksNumber integer value.
     */
    public void setParentOutlinksNumber(int parentOutlinksNumber) {
        this.parentOutlinksNumber = new IntWritable(parentOutlinksNumber);
    }

    /**
     * GetParent method returns the article's name for the referencing page (parent) according
     * to the associated key.
     *
     * @return the string article's name
     */
    public String getParent() {
        return parent.toString();
    }

    /**
     * SetParent method sets the article's name for the referencing page (parent) according
     * to the associated key.
     *
     * @param parent string name
     */
    public void setParent(String parent) {
        this.parent = new Text(parent);
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
