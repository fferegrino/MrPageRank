package mapreduce.mapping;

import mapreduce.datatypes.WikiInputValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class ArticleMapper extends Mapper<Text, Text, Text, WikiInputValue> {

	private final static boolean FilterDuplicates = true;

	@Override
    protected void map(Text key, Text value,
                       Mapper<Text, Text, Text, WikiInputValue>.Context context)
            throws IOException, InterruptedException {
 
        String[] contents = value.toString().split(" ", 2);
        
        WikiInputValue outValue = new WikiInputValue();
    		outValue.setRevisionId(Long.parseLong(contents[0]));

    		String newOutlinksJoint = "";
    		int numberOfOutlinks = 0;
    		
        if(contents.length == 2) { // Has outlinks
        		String [] originalOutlinks = contents[1].split("\\s");
        		
        		if(FilterDuplicates)
        		{
	        		ArrayList<String> newOutlinks = new ArrayList<>(originalOutlinks.length);
	        		HashSet<String> unique = new HashSet<String>(originalOutlinks.length);
	        		
	        		for(String outlink : originalOutlinks ) {
	        			if(!unique.contains(outlink)) {
	        				unique.add(outlink);
	        				newOutlinks.add(outlink);
	        			}
	        		}
	        		numberOfOutlinks = newOutlinks.size();
	        		newOutlinksJoint = String.join(" ", newOutlinks);
        		}
        		else {
	        		numberOfOutlinks = originalOutlinks.length;
        			newOutlinksJoint = String.join(" ", originalOutlinks);
        		}
        }
        
	    	outValue.setOutlinks(newOutlinksJoint);
	    	outValue.setOutlinksNumber(numberOfOutlinks);
        
        context.write(key, outValue);

    }


}
