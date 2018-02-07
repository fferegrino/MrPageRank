package mapreduce.input;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import mapreduce.datatypes.WikiInputValue;
/**
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class WikiInputFormat extends FileInputFormat<Text, WikiInputValue>{
	public RecordReader<Text, WikiInputValue> createRecordReader(InputSplit split, TaskAttemptContext context){
		return new WikiMultiRecordReader();
	}
}