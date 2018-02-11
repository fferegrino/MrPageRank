package mapreduce.input;

import mapreduce.datatypes.WikiInputValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class WikiInputFormat extends FileInputFormat<Text, WikiInputValue> {
    public RecordReader<Text, WikiInputValue> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new WikiMultiRecordReader();
    }
}