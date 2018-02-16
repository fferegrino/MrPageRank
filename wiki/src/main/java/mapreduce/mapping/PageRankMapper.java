package mapreduce.mapping;

import mapreduce.datatypes.WikiIntermediatePageRankValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * PageRankMapper class that creates the in-links list for page Y based on the out-links 
 * list for page X by replacing the Key (article's name) of the Parent article with
 * the Referenced or "Child" article, and setting the Parent's current PageRank.
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, WikiIntermediatePageRankValue> {
    /**
     * Map method that generates the in-links list for each out-link element referenced by an Article.
     * @param key     value for Parent article.
     * @param value   with Parent article detail.
     * @param context object contains configuration data to interact with Hadoop's
     *                environment.
     */
    @Override
    protected void map(LongWritable key, Text line,
                       Mapper<LongWritable, Text, Text, WikiIntermediatePageRankValue>.Context context)
            throws IOException, InterruptedException {

        // In parent value the hole line has this format: parent\tPR|#outlinks|out1 out2 out3
        String[] split = line.toString().split("\t");

        String parent = split[0].trim();

        String[] values = split[1].split("\\|");

        float parentPageRank = Float.parseFloat(values[0]);
        int parentNumberOfOutlinks = Integer.parseInt(values[1]);
        // Create intermediate value for looping process
        WikiIntermediatePageRankValue intermediateValue;
        Text intermediateKey;
        // Iterate over Article's out-links only if it has out-links
        if (values.length > 2) {
            String[] parentOutlinks = values[2].split(" ");
            // Loop that performs in-links generation
            for (int i = 0; i < parentNumberOfOutlinks; i++) {
                String currentOutlink = parentOutlinks[i];

                intermediateKey = new Text(currentOutlink);

                intermediateValue = new WikiIntermediatePageRankValue();
                intermediateValue.setPageRank(parentPageRank);
                intermediateValue.setParent(parent);
                intermediateValue.setParentOutlinksNumber(parentNumberOfOutlinks);
                // Write in-link Referenced Key with Parent (voter) details
                context.write(intermediateKey, intermediateValue);
            }

        }
        // Create new Parent object with original information (recovery of state in Reducer phase)
        intermediateKey = new Text(parent);

        intermediateValue = new WikiIntermediatePageRankValue();
        intermediateValue.setPageRank(parentPageRank);
        intermediateValue.setParent(parent);
        if (values.length > 2) {
            intermediateValue.setOutlinks(values[2]);
        }
        intermediateValue.setParentOutlinksNumber(parentNumberOfOutlinks);
        // Write the parent with the original out-links details
        context.write(intermediateKey, intermediateValue);
    }

}
