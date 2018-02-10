package mapreduce.reducing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.datatypes.*;
import mapreduce.reducing.ArticleReducer.*;

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
			if(value.getParent().equalsIgnoreCase(key) && value.getOutlinks() != null && !value.getOutlinks().equals("")) {
				outlinks = value.getOutlinks();
				outlinksNumber = value.getParentOutlinksNumber();
				continue;
			}
			
			
			if(value.getParentOutlinksNumber() == 0){
				continue;
			}
			vote += value.getPageRank() / value.getParentOutlinksNumber();
		}
		newPageRank = (1 - d) + (d * vote);
		
		if(outlinks == null) {
			outlinks = "";
		}
		
		Text outKey = new Text(key);
		WikiInOutPageRankValue outValue = new WikiInOutPageRankValue();
		outValue.setOutlinks(outlinks);
		outValue.setPageRank(newPageRank);
		outValue.setOutlinksNumber(outlinksNumber);
		

		context.write(outKey, outValue);
	}

}
