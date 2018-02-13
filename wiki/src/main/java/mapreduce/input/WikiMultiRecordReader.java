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

/**
 * RecordReader class that specifies the required format for Wikipedia edit
 * history tagged multi-line's file.
 * 
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class WikiMultiRecordReader extends RecordReader<Text, Text> {
	// Record separator
	private static final byte[] recordSeparator = "\n\n".getBytes();
	private FSDataInputStream fsin;
	private long start, end;
	private boolean stillInChunk = true;
	private StringBuffer sb;
	private Text key = new Text();
	private Text value;

	/**
	 * Initialize method that configures node's context for reading an input split
	 * text file (parsed version of the complete Wikipedia edit history).
	 * 
	 * @param inputSplit
	 *            is a logical chunk of data that points to start and end locations
	 *            within physical blocks.
	 * @param context
	 *            object contains configuration data to interact with Hadoop's
	 *            environment.
	 * @throws IOException
	 *             when the file or the filesystem can not be reached.
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

	/**
	 * ReadRecord method for determining the type of the processed block.
	 * 
	 * @param withinBlock
	 *            is a boolean value that determines the type of the processed block
	 *            (end of block or inner block).
	 * @return a boolean value with the current type of the processed block.
	 * @throws IOException
	 *             when the block can not be read.
	 */
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

	/**
	 * NextKeyValueRead method reads the next key, value pair.
	 * 
	 * @return true if a key/value pair is read
	 * @throws IOException
	 *             if the record reader can not be reached.
	 */
	public boolean nextKeyValue() throws IOException {
		if (!stillInChunk) {
			return false;
		}

		boolean status = readRecord(true);

		String[] lines = sb.toString().split("\n");

		String[] revisionValues = lines[0].split(" ");
		key.set(revisionValues[3]);

		long revisionId = Long.parseLong(revisionValues[2]);
		String outlinks = null;
		String mainLine = lines[3];
		if (mainLine.length() > 5) {
			outlinks = mainLine.substring(5).trim();
		}

		if (outlinks != null) {
			value = new Text(revisionId + " " + outlinks);
		} else {
			value = new Text(String.valueOf(revisionId));
		}

		// Clear the buffer
		sb.setLength(0);

		if (!status) {
			stillInChunk = false;
		}
		return true;
	}

	/**
	 * GetCurrentKey method retrieves the current key of the processed record.
	 * 
	 * @return the current key or null if there is no current key.
	 * @throws IOException
	 *             if the record reader can not be reached.
	 */
	public Text getCurrentKey() {
		return key;
	}

	/**
	 * GetCurrentValue method retrieves the current value of the processed record.
	 * 
	 * @return the read record.
	 * @throws IOException
	 *             if the record reader can not be reached.
	 */
	public Text getCurrentValue() {
		return value;
	}

	/**
	 * GetProgress method retrieves the current progress of the record reader
	 * through the data.
	 * 
	 * @return a number between 0.0 and 1.0 that is the portion of read data.
	 * @throws IOException
	 *             if the record reader can not be reached.
	 */
	public float getProgress() throws IOException {
		return (float) (fsin.getPos() - start) / (end - start);
	}

	/**
	 * Close method terminates the record reader object.
	 * 
	 * @throws IOException
	 *             if the record reader can not be closed.
	 */
	public void close() throws IOException {
		fsin.close();
	}
}