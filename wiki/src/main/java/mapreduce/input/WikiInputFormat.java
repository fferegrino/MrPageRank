package mapreduce.input;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
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
	// Setting a lower split size to use more containers in the "data gathering and cleansing" phase
    final static long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new WikiMultiRecordReader();
    }
    
    /**
     * Set the maximum split size
     * @param to modify
     * @param size the maximum split size
     */
    public static void setMaxInputSplitSize(Job job, long size) {
      job.getConfiguration().setLong(SPLIT_MAXSIZE, job.getConfiguration().getLong(FileInputFormat.SPLIT_MAXSIZE, DEFAULT_SPLIT_SIZE) / 8);
    }
}
