package mapreduce.output;

import mapreduce.datatypes.WikiInOutPageRankValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

public class PageRankRecordWriter extends RecordWriter<Text, WikiInOutPageRankValue> {

    static final String separator = " ";
    private DataOutputStream out;

    public PageRankRecordWriter(DataOutputStream out) {
        this.out = out;
    }

    @Override
    public void write(Text key, WikiInOutPageRankValue value) throws IOException, InterruptedException {
        String outKey = key.toString();
        float pageRank = value.getPageRank();
        out.writeBytes(outKey + separator + pageRank + System.getProperty("line.separator"));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        out.close();
    }

}
