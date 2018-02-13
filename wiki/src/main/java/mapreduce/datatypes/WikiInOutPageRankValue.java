package mapreduce.datatypes;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable class that defines the output values for the PageRank's Reducer process, and the input values
 * for the Mapping process (PageRank's iterative processing) 
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class WikiInOutPageRankValue implements Writable {
	//Page rank
    private FloatWritable pageRank;
    //Out-links number (list count)
    private IntWritable outlinksNumber;
    //Out-links list (by article name)
    private Text outlinks;
    
    /**
     * ReadFields method for deserializing DataInput fields.
     * @param in contains PageRank's attributes required for calculation.
     * @throws IOException if deserializable input data can not be read. 
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        pageRank.readFields(in);
        outlinksNumber.readFields(in);
        outlinks.readFields(in);
    }
    
    /**
     * Write method for serializing DataOutput fields.
     * @param out stores processed PageRank's attributes required for calculation.
     * @throws IOException if serializable output data can not be written.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        pageRank.write(out);
        outlinksNumber.write(out);
        outlinks.write(out);
    }
    
    /**
     * String representation for WikiInOutPageRankValue class: "Page rank" + | delimiter + "out-links 
     * number of elements" + | delimiter + "out-links elements list".
     * @return the string description.
     */
    @Override
    public String toString() {
        return getPageRank() + "|" + getOutlinksNumber() + "|" + getOutlinks();
    }

    /**
     * GetPageRank method returns the page rank value according to the associated key.
     * @return the page rank.
     */
    public float getPageRank() {
        return pageRank.get();
    }

    /**
     * SetPageRank method sets the page rank value according to the associated key.
     * @param pageRank float value.
     */
    public void setPageRank(float pageRank) {
        this.pageRank = new FloatWritable(pageRank);
    }

    /**
     * GetOutlinksNumber method returns the number of elements in the out-links list according to the 
     * associated key.
     * @return the number of out-links
     */
    public int getOutlinksNumber() {
        return outlinksNumber.get();
    }

    /**
     * SetOutlinksNumber method sets the number of elements in the out-links list according to the 
     * associated key.
     * @param outlinksNumber integer value.
     */
    public void setOutlinksNumber(int outlinksNumber) {
        this.outlinksNumber = new IntWritable(outlinksNumber);
    }

    /**
     * GetOutlinks method returns the elements in the out-links list according to the associated key.
     * @return the string representation of the elements in the out-links list.
     */
    public String getOutlinks() {
        return outlinks.toString();
    }

    /**
     * SetOutlinks method sets the elements in the out-links list according to the associated key.
     * @param out-links string list.
     */
    public void setOutlinks(String outlinks) {
        this.outlinks = new Text(outlinks);
    }
}
