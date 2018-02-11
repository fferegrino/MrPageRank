package mapreduce.input;

import mapreduce.datatypes.WikiInputValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WikiMultiRecordReader extends RecordReader<Text, WikiInputValue> {
    private static final byte[] recordSeparator = "\n\n".getBytes();
    private FSDataInputStream fsin;
    private long start, end;
    private boolean stillInChunk = true;
    private StringBuffer sb;
    private Text key = new Text();
    private WikiInputValue value;


    /**
     * Initialization method that configures node's context for reading an input
     * split text file (parsed version of the complete Wikipedia edit history).
     *
     * @param inputSplit is a logical chunk of data that points to start and end
     *                   locations within physical blocks.
     * @param context    object contains configuration data to interact with Hadoop's
     *                   environment (system).
     */
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        sb = new StringBuffer();
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = context.getConfiguration();
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);

        fsin = fs.open(path);
        start = split.getStart();
        end = split.getStart() + split.getLength();
        fsin.seek(start);

        if (start != 0) {
            readRecord(false);
        }
    }

    private boolean readRecord(boolean withinBlock) throws IOException {
        int i = 0, b;
        while (true) {
            if ((b = fsin.read()) == -1) { // End of file
                return false;
            }

            if (withinBlock) {
                sb.append((char) b);
            }

            if (b == recordSeparator[i]) {
                if (++i == recordSeparator.length) {
                    return fsin.getPos() < end;
                }
            } else {
                i = 0;
            }
        }
    }

    public boolean nextKeyValue() throws IOException {
        if (!stillInChunk) {
            return false;
        }

        boolean status = readRecord(true);

        String[] lines = sb.toString().split("\n");

        String[] revisionValues = lines[0].split(" ");
        key.set(revisionValues[3]);

        value = new WikiInputValue();
        String mainLine = lines[3];
        if (mainLine.length() > 5) {
            String[] outlinks = mainLine.substring(5).split("\\s");
            value.setOutlinksNumber(outlinks.length);
            value.setOutlinks(String.join(" ", outlinks));
        }
        value.setRevisionId(Long.parseLong(revisionValues[2]));

        // Clear the buffer
        sb.setLength(0);

        if (!status) {
            stillInChunk = false;
        }
        return true;
    }

    public Text getCurrentKey() {
        return key;
    }

    public WikiInputValue getCurrentValue() {
        return value;
    }

    public float getProgress() throws IOException {
        return (float) (fsin.getPos() - start) / (end - start);
    }

    public void close() throws IOException {
        fsin.close();
    }
}