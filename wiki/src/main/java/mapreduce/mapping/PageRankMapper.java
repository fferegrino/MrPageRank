package mapreduce.mapping;

import mapreduce.datatypes.WikiIntermediatePageRankValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, WikiIntermediatePageRankValue> {

    @Override
    protected void map(LongWritable key, Text line,
                       Mapper<LongWritable, Text, Text, WikiIntermediatePageRankValue>.Context context)
            throws IOException, InterruptedException {

        // En parent value viene toda la línea en este formato: parent\tPR|#outlinks|out1 out2 out3
        String[] split = line.toString().split("\t");

        String parent = split[0].trim();

        String[] values = split[1].split("\\|");

        float parentPageRank = Float.parseFloat(values[0]);
        int parentNumberOfOutlinks = Integer.parseInt(values[1]);

        WikiIntermediatePageRankValue intermediateValue;
        Text intermediateKey;

        if (values.length > 2) {
            String[] parentOutlinks = values[2].split(" ");
            for (int i = 0; i < parentNumberOfOutlinks; i++) {
                String currentOutlink = parentOutlinks[i];

                intermediateKey = new Text(currentOutlink);

                intermediateValue = new WikiIntermediatePageRankValue();
                intermediateValue.setPageRank(parentPageRank);
                intermediateValue.setParent(parent);
                intermediateValue.setParentOutlinksNumber(parentNumberOfOutlinks);
                // No se necesitan, otra vez

                context.write(intermediateKey, intermediateValue);
            }

        }
        // Send again the parent with the list of outlinks:
        intermediateKey = new Text(parent);

        intermediateValue = new WikiIntermediatePageRankValue();
        intermediateValue.setPageRank(parentPageRank);
        intermediateValue.setParent(parent);
        if (values.length > 2) {
            intermediateValue.setOutlinks(values[2]);
        }
        intermediateValue.setParentOutlinksNumber(parentNumberOfOutlinks);

        context.write(intermediateKey, intermediateValue);
    }

}
