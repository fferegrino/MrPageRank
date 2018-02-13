package mapreduce.reducing;

import mapreduce.datatypes.WikiInOutPageRankValue;
import mapreduce.datatypes.WikiInputValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class ArticleReducer extends Reducer<Text, WikiInputValue, Text, WikiInOutPageRankValue> {
    private WikiInOutPageRankValue outputValue = new WikiInOutPageRankValue();

    @Override
    protected void reduce(Text inKey, Iterable<WikiInputValue> inValues,
                          Reducer<Text, WikiInputValue, Text, WikiInOutPageRankValue>.Context context) throws IOException, InterruptedException {

        long latestRevisionId = 0;
        int outlinksNumber = 0;
        String currentOutlinks = null;

        WikiInputValue value = null;
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


        context.getCounter(ReducerCounters.REDUCED_WIKI_ARTICLES).increment(1);

        if (currentOutlinks == null)
            currentOutlinks = "";

        outputValue.setOutlinks(currentOutlinks);
        outputValue.setPageRank(1);
        outputValue.setOutlinksNumber(outlinksNumber);

        context.write(inKey, outputValue);

    }

    static enum ReducerCounters {
        REDUCED_WIKI_ARTICLES
    }

}
