package mapreduce.input;

import mapreduce.datatypes.WikiInputValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * FileInputFormat class that defines a specific record reader for Wikipedia edit history
 * tagged multi-line's format.
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class WikiInputFormat extends FileInputFormat<Text, Text> {
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new WikiMultiRecordReader();
    }
}