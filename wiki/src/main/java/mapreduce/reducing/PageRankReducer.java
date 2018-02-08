package mapreduce.reducing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.datatypes.WikiInOutPageRankValue;
import mapreduce.datatypes.WikiInputValue;
import mapreduce.datatypes.WikiIntermediatePageRankValue;

public class PageRankReducer extends Reducer<Text, WikiIntermediatePageRankValue, Text, WikiInOutPageRankValue>{
	
	final float d = 0.85f; 
	
	@Override
	protected void reduce(Text inKey, Iterable<WikiIntermediatePageRankValue> inValues,
			Reducer<Text, WikiIntermediatePageRankValue, Text, WikiInOutPageRankValue>.Context context)
			throws IOException, InterruptedException {
		
		float newPageRank ;
		float vote = 0;
		String outlinks = null;
		int outlinksNumber = 0;
		
		String key = inKey.toString();
		
		WikiIntermediatePageRankValue value;
		for (Iterator<WikiIntermediatePageRankValue> it = inValues.iterator(); it.hasNext();)
		{
			value = it.next();
			if(value.getParent() == key) {
				outlinks = value.getOutlinks();
				outlinksNumber = value.getParentOutlinksNumber();
				continue;
			}
			
			vote += value.getPageRank() / value.getParentOutlinksNumber();
		}
		newPageRank = 1 - d + (d * vote);
		
		Text outKey = new Text(key);
		WikiInOutPageRankValue outValue = new WikiInOutPageRankValue();
		outValue.setOutlinks(outlinks);
		outValue.setPageRank(newPageRank);
		outValue.setOutlinksNumber(outlinksNumber);
		

		context.write(outKey, outValue);
	}

}
