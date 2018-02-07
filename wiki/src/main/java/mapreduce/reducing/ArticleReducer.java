package mapreduce.reducing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.datatypes.WikiInputValue;


public class ArticleReducer  extends Reducer<Text, WikiInputValue, Text, WikiInputValue> {
	private WikiInputValue _value = new WikiInputValue();
	

	static enum ReducerCounters {
		REDUCED_WIKI_ARTICLES
	}
	
	
	@Override
	protected void reduce(Text inKey, Iterable<WikiInputValue> inValues,
			Reducer<Text, WikiInputValue, Text, WikiInputValue>.Context context) throws IOException, InterruptedException {

		long latestRevisionId = 0;
		String currentOutlinks = null;
		
		WikiInputValue value = null;
		for (Iterator<WikiInputValue> it = inValues.iterator(); it.hasNext();)
		{
			value = it.next();
			if (value == null)
				continue;
			
			if(latestRevisionId < value.getRevisionId())
			{
				latestRevisionId = value.getRevisionId();
				currentOutlinks = value.getOutlinks();
			}
		}
		

		context.getCounter(ReducerCounters.REDUCED_WIKI_ARTICLES).increment(1);
		
		if(currentOutlinks == null)
			currentOutlinks = "";
		_value.setOutlinks(currentOutlinks);
		_value.setRevisionId(latestRevisionId);
		context.write(inKey, _value);
		
	}

}
