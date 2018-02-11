package mapreduce.mapping;

import mapreduce.datatypes.WikiInputValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ArticleMapper extends Mapper<Text, WikiInputValue, Text, WikiInputValue> {

    @Override
    protected void map(Text key, WikiInputValue value,
                       Mapper<Text, WikiInputValue, Text, WikiInputValue>.Context context)
            throws IOException, InterruptedException {

        context.write(key, value);

        context.getCounter(MapperCounters.TOTAL_WIKI_ARTICLES).increment(1);
    }

    static enum MapperCounters {
        TOTAL_WIKI_ARTICLES
    }

}
