package mapreduce.datatypes;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents the output value of the PageRank reducer module, and, at the same time
 * it is used as the input for the Mapper of the same PageRank job
 *
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class WikiInOutPageRankValue implements Writable {

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
        return getPageRank() + "|" + getOutlinksNumber() + "|" + getOutlinks();
    }

    /**
     * @return the page rank for the page specified by the key associated to this value
     */
    public float getPageRank() {
        return pageRank.get();
    }

    /**
     * Set the page rank for the page specified by the key associated to this value
     *
     * @param pageRank
     */
    public void setPageRank(float pageRank) {
        this.pageRank = new FloatWritable(pageRank);
    }

    /**
     * @return get the number of outlinks that the key associated to this value has
     */
    public int getOutlinksNumber() {
        return outlinksNumber.get();
    }

    /**
     * Set the number of outlinks that the page specified by the key associated to this value has
     *
     * @param outlinksNumber
     */
    public void setOutlinksNumber(int outlinksNumber) {
        this.outlinksNumber = new IntWritable(outlinksNumber);
    }

    /**
     * @return all the outlinks that the page specified by the key associated to this value has,
     * the outlinks are presented as strings separated by a blank space
     */
    public String getOutlinks() {
        return outlinks.toString();
    }

    /**
     * Set the outlinks that the page specified by the key associated to this value has
     *
     * @param outlinks
     */
    public void setOutlinks(String outlinks) {
        this.outlinks = new Text(outlinks);
    }

}
