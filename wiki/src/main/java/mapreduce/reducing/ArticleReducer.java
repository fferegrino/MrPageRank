package mapreduce.reducing;

import mapreduce.datatypes.WikiInOutPageRankValue;
import mapreduce.datatypes.WikiInputValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
/**
 * ArticleReducer class that selects the latest revision of each Article that will be used
 * for the PageRank's calculation. Our Assumption is that only the latest pages are available
 * to submit a "vote" for the PageRank.
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class ArticleReducer extends Reducer<Text, WikiInputValue, Text, WikiInOutPageRankValue> {
    private WikiInOutPageRankValue outputValue = new WikiInOutPageRankValue();

	/**
	 * Reduce method that selects the latest revision of each Article that will be used
	 * for the PageRank's calculation.
	 * @param inKey article name.
	 * @param value as defined by the WikiInputValue class.
	 * @param context object contains configuration data to interact with Hadoop's
	 * environment and defines OutputFormat types for key-value.
	 */
    @Override
    protected void reduce(Text inKey, Iterable<WikiInputValue> inValues,
                          Reducer<Text, WikiInputValue, Text, WikiInOutPageRankValue>.Context context) throws IOException, InterruptedException {
        // Initial latest revision id
    	long latestRevisionId = 0;
    	// Initial out-links counter
        int outlinksNumber = 0;
        // Initial out-links list
        String currentOutlinks = null;
        // Initial WikiInputValue object
        WikiInputValue value = null;
        // for all the records with the same article name look for the latest revision id
        for (Iterator<WikiInputValue> it = inValues.iterator(); it.hasNext(); ) {
            value = it.next();
            if (value == null)
                continue;

            if (latestRevisionId < value.getRevisionId()) {
                outlinksNumber = value.getOutlinksNumber();
                latestRevisionId = value.getRevisionId();
                currentOutlinks = value.getOutlinks();
            }
        }

        // Write Articles counter to determine the number of articles processed
        context.getCounter(ReducerCounters.REDUCED_WIKI_ARTICLES).increment(1);
        // If the Article does not have Out-links set list to empty instead of NULL
        if (currentOutlinks == null)
            currentOutlinks = "";
        // Set OutPut objects values
        outputValue.setOutlinks(currentOutlinks);
        outputValue.setPageRank(1);
        outputValue.setOutlinksNumber(outlinksNumber);
        // Write processed data to the context data flow
        context.write(inKey, outputValue);
    }
    // Counter to determine the number of articles processed
    static enum ReducerCounters {
        REDUCED_WIKI_ARTICLES
    }

}
