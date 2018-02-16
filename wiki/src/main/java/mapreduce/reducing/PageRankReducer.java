package mapreduce.reducing;

import mapreduce.datatypes.WikiInOutPageRankValue;
import mapreduce.datatypes.WikiIntermediatePageRankValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
/**
 * PageRankReducer class performs the PageRank calculation, by taking a list of 'WikiIntermediatePageRankValue'
 * objects, allowing the calculation of a given page over a single loop where the processed records must meet 
 * the following conditions:
 * - Parent field's value must be different to the key value
 * - Not having out-links specified in the "out-links" field 
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class PageRankReducer extends Reducer<Text, WikiIntermediatePageRankValue, Text, WikiInOutPageRankValue> {
	// Constant for calculation
    final float d = 0.85f;
    
    /**
     * Reduce method that performs the PageRank calculation.
     * @param inKey   referenced article's name.
     * @param value   as defined by the WikiIntermediatePageRankValue class.
     * @param context object contains configuration data to interact with Hadoop's
     *                environment and defines OutputFormat types for key-value.
     */
    @Override
    protected void reduce(Text inKey, Iterable<WikiIntermediatePageRankValue> inValues,
                          Reducer<Text, WikiIntermediatePageRankValue, Text, WikiInOutPageRankValue>.Context context)
            throws IOException, InterruptedException {
    	// Variable that stores new calculated value
        float newPageRank;
        float vote = 0;
        // List of out-links
        String outlinks = null;
        // Number of out-links elements (pre-computed)
        int outlinksNumber = 0;

        String key = inKey.toString();
        // Variable for storing input object
        WikiIntermediatePageRankValue value;
        // PageRank calculation
        for (Iterator<WikiIntermediatePageRankValue> it = inValues.iterator(); it.hasNext(); ) {
            value = it.next();
            // validate if calculation should be made, or is the Parent record
            if (value.getParent().equalsIgnoreCase(key) && value.getOutlinks() != null && !value.getOutlinks().equals("")) {
                outlinks = value.getOutlinks();
                outlinksNumber = value.getParentOutlinksNumber();
                continue;
            }
            // No parent out-links, means the voting result is zero
            if (value.getParentOutlinksNumber() == 0) {
                continue;
            }
            // Voting section by each page
            vote += value.getPageRank() / value.getParentOutlinksNumber();
        }
        // Final PageRank calculation
        newPageRank = (1 - d) + (d * vote);
        // Replace null out-links list for empty space to avoid errors
        if (outlinks == null) {
            outlinks = "";
        }
        // Create output object with new results
        Text outKey = new Text(key);
        WikiInOutPageRankValue outValue = new WikiInOutPageRankValue();
        outValue.setOutlinks(outlinks);
        outValue.setPageRank(newPageRank);
        outValue.setOutlinksNumber(outlinksNumber);
        // Write processed data to the context data flow
        context.write(outKey, outValue);
    }

}
