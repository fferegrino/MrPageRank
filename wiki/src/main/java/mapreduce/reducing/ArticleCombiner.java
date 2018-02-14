package mapreduce.reducing;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import mapreduce.datatypes.WikiInputValue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ArticleCombiner class that pre-processes preliminary local records and
 * selects the latest revision of each Article that will be used for the
 * PageRank's calculation. Our Assumption is that only the latest pages are
 * available to submit a "vote" for the PageRank.
 * 
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class ArticleCombiner extends Reducer<Text, WikiInputValue, Text, WikiInputValue> {

	private static final Log LOG = LogFactory.getLog(ArticleCombiner.class);

	/**
	 * Reduce method that selects from preliminary local records, the latest
	 * revision of each Article that will be used for the PageRank's calculation.
	 * 
	 * @param inKey
	 *            article name.
	 * @param value
	 *            as defined by the WikiInputValue class.
	 * @param context
	 *            object contains configuration data to interact with Hadoop's
	 *            environment and defines OutputFormat types for key-value.
	 */
	@Override
	protected void reduce(Text inKey, Iterable<WikiInputValue> inValues,
			Reducer<Text, WikiInputValue, Text, WikiInputValue>.Context context)
			throws IOException, InterruptedException {

		// Initial WikiInputValue object
		WikiInputValue value = null;
		// Initial latest revision id
		long latestRevisionId = 0;
		// Initial out-links counter
		int outlinksNumber = 0;
		// Initial out-links list
		String currentOutlinks = null;

		// for all the records with the same article name look for the latest revision id
		for (Iterator<WikiInputValue> it = inValues.iterator(); it.hasNext();) {
			value = it.next();
			if (value == null)
				continue;

			if (latestRevisionId < value.getRevisionId()) {
				LOG.info("Changing " + inKey.toString() + " revision from " + latestRevisionId + " to "
						+ value.getRevisionId());
				latestRevisionId = value.getRevisionId();
				outlinksNumber = value.getOutlinksNumber();
				currentOutlinks = value.getOutlinks();
				value = null;
			}
		}

		WikiInputValue latest = new WikiInputValue();
		latest.setOutlinks(currentOutlinks);
		latest.setOutlinksNumber(outlinksNumber);
		latest.setRevisionId(latestRevisionId);
		// Write processed data to the context data flow
		context.write(inKey, latest);
	}
}
