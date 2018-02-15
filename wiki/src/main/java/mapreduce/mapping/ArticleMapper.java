package mapreduce.mapping;

import mapreduce.datatypes.WikiInputValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * ArticleMapper class that performs the data "gathering and cleansing" processes for data sources.
 *
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class ArticleMapper extends Mapper<Text, Text, Text, WikiInputValue> {

    private final static boolean FilterDuplicates = true;

    /**
     * Map method that performs data "gathering and cleansing" processes.
     *
     * @param key     value as defined by the WikiInputFormat class.
     * @param value   as defined by the WikiInputFormat class.
     * @param context object contains configuration data to interact with Hadoop's
     *                environment.
     */
    @Override
    protected void map(Text key, Text value,
                       Mapper<Text, Text, Text, WikiInputValue>.Context context)
            throws IOException, InterruptedException {
        //split record contents with spaces
        String[] contents = value.toString().split(" ", 2);

        WikiInputValue outValue = new WikiInputValue();
        outValue.setRevisionId(Long.parseLong(contents[0]));

        String newOutlinksJoint = "";
        int numberOfOutlinks = 0;
        // Has out-links? If not do not process the contents of the record
        if (contents.length == 2) {
            // Clean the MAIN out-links; removing spaces and not printable characters.
            String[] originalOutlinks = contents[1].split("\\s");
            // Check filtering configuration
            if (FilterDuplicates) {
                // Create array list from array of cleaned out-links
                ArrayList<String> newOutlinks = new ArrayList<>(originalOutlinks.length);
                // Get hash set with unique values
                HashSet<String> unique = new HashSet<String>(originalOutlinks.length);
                // Remove duplicated elements
                for (String outlink : originalOutlinks) {
                    if (!unique.contains(outlink)) {
                        unique.add(outlink);
                        newOutlinks.add(outlink);
                    }
                }
                numberOfOutlinks = newOutlinks.size();
                // Create a new list for deduplicated elements
                newOutlinksJoint = String.join(" ", newOutlinks);
            }
            // If filtering configuration not required
            else {
                // Assign all out-links to a new list
                numberOfOutlinks = originalOutlinks.length;
                newOutlinksJoint = String.join(" ", originalOutlinks);
            }
        }

        outValue.setOutlinks(newOutlinksJoint);
        outValue.setOutlinksNumber(numberOfOutlinks);
        // Write processed data to the context data flow
        context.write(key, outValue);
    }

}
