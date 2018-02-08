package mapreduce.mapping;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mapreduce.datatypes.WikiInOutPageRankValue;
import mapreduce.datatypes.WikiInputValue;
import mapreduce.datatypes.WikiIntermediatePageRankValue;

public class PageRankMapper extends Mapper<Text, WikiInOutPageRankValue, Text, WikiIntermediatePageRankValue> {
	
	@Override
	protected void map(Text key, WikiInOutPageRankValue parentValue,
			Mapper<Text, WikiInOutPageRankValue, Text, WikiIntermediatePageRankValue>.Context context)
			throws IOException, InterruptedException {
		
		String parent = key.toString();
		
		float parentPageRank = parentValue.getPageRank();
		String [] parentOutlinks = parentValue.getOutlinks().split(" ");
		int parentNumberOfOutlinks = parentValue.getOutlinksNumber();
		
		WikiIntermediatePageRankValue intermediateValue;
		Text intermediateKey;
		for(int i = 0; i < parentNumberOfOutlinks; i++)
		{
			String currentOutlink = parentOutlinks[i];
			
			intermediateKey = new Text(currentOutlink);
			
			intermediateValue = new WikiIntermediatePageRankValue();
			intermediateValue.setPageRank(parentPageRank);
			intermediateValue.setParent(parent);
			intermediateValue.setParentOutlinksNumber(parentNumberOfOutlinks);
			// Omitimos Outlinks porque estos no necesitan
			
			context.write(intermediateKey, intermediateValue);
		}
		
		
		// Send again the parent with the list of outlinks:
		intermediateKey = new Text(parent);
		
		intermediateValue = new WikiIntermediatePageRankValue();
		intermediateValue.setPageRank(parentPageRank);
		intermediateValue.setParent(parent);
		intermediateValue.setOutlinks(parentValue.getOutlinks());
		intermediateValue.setParentOutlinksNumber(parentNumberOfOutlinks);
		
		context.write(intermediateKey, intermediateValue);
	}

}
