package mapreduce;

import mapreduce.datatypes.WikiInputValue;
import mapreduce.datatypes.WikiIntermediatePageRankValue;
import mapreduce.input.WikiInputFormat;
import mapreduce.mapping.ArticleMapper;
import mapreduce.mapping.PageRankMapper;
import mapreduce.output.PageRankOutputFormat;
import mapreduce.reducing.ArticleReducer;
import mapreduce.reducing.PageRankReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * Main class for the PageRank's calculation of a given set using Apache Hadoop's ecosystem.
 * The PageRank is an algorithm used to establish web-pages relevance or importance within 
 * a web-site or the Internet.
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */
public class WikiPageRank extends Configured implements Tool {

    /**
     * Main method responsible for orchestrating and managing the PageRank's calculation 
     * scheduling Hadoop's jobs in N iterations as requested by the client.
     * @param args is a string array that contains the full set of command-line parameters.
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WikiPageRank(), args));
    }
    
    /**
     * Run method responsible for scheduling jobs in Hadoop's cluster.
     * @param args is a string array that contains the full set of command-line parameters.
     * @param[0] input file path, e.g. Wikipedia edit history file.
     * @param[1] output directory path.
     * @param[2] number of iterations for PageRank's calculation.
     */
    @Override
    public int run(String[] args) throws Exception {
    	// Obtain configuration details from Hadoop's cluster
        Configuration conf = getConf();
        conf.set("mapreduce.map.java.opts", "-Xmx620M");
        // Setting a lower split size to use more containers in the "data gathering and cleansing" phase
        final long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;
        // Lower split size by factor of 8, considering cluster's block size is 128M
        conf.setLong(FileInputFormat.SPLIT_MAXSIZE, conf.getLong(FileInputFormat.SPLIT_MAXSIZE, DEFAULT_SPLIT_SIZE) / 4);
        // Enable Mapping output compression
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        
        // Obtain filesystem configuration details from Hadoop's cluster
        FileSystem fsys = FileSystem.get(conf);
        // Define input file path
        Path input = new Path(args[0]);
        // Define output directory path
        Path output = new Path(args[1]);
        // Define staging files path
        Path intermediate = new Path("inter0");
        // Delete recursively the output directory if exists  
        fsys.delete(output, true);
        // Default number of iterations for PageRank's calculation if not defined by command-line.
        int numLoops = 10;
        if (args.length > 2) {
            numLoops = Integer.parseInt(args[2]);
        }
        // Define and configure "data gathering and cleansing" job
        Job cleaningJob = Job.getInstance(conf);
        FileInputFormat.setInputPaths(cleaningJob, input);
        FileOutputFormat.setOutputPath(cleaningJob, intermediate);
        cleaningJob.setJobName("Mighty-WikiPageRank(init)");
        cleaningJob.setJarByClass(getClass());
        cleaningJob.setPriority(JobPriority.HIGH);
        cleaningJob.setNumReduceTasks(80);
        cleaningJob.setInputFormatClass(WikiInputFormat.class);
        cleaningJob.setOutputFormatClass(TextOutputFormat.class);
        // Mapping configuration for "data gathering and cleansing" job
        cleaningJob.setMapperClass(ArticleMapper.class);
        cleaningJob.setMapOutputKeyClass(Text.class);
        cleaningJob.setMapOutputValueClass(WikiInputValue.class);

        // Reducer configuration for "data gathering and cleansing" job
        cleaningJob.setReducerClass(ArticleReducer.class);
        
        // Wait for completion
        cleaningJob.waitForCompletion(true);

        // PageRank's iterative calculation
        Path previousPath = intermediate;
        boolean succeeded = false;
        for (int currentLoop = 1; currentLoop < numLoops + 1; currentLoop++) {
            Path nextPath = null;
            // Define and configure current "PageRank's calculation" job
            Job pageRankJob = Job.getInstance(conf);
            pageRankJob.setJarByClass(getClass());
            pageRankJob.setJobName("Mighty-WikiPageRank(Loop: " + currentLoop + ")");
            pageRankJob.setPriority(JobPriority.HIGH);
            pageRankJob.setNumReduceTasks(80);

            // Mapping configuration for current "PageRank's calculation" job
            pageRankJob.setMapperClass(PageRankMapper.class);
            pageRankJob.setMapOutputKeyClass(Text.class);
            pageRankJob.setMapOutputValueClass(WikiIntermediatePageRankValue.class);

            // Reducer configuration for current "PageRank's calculation" job
            pageRankJob.setReducerClass(PageRankReducer.class);
            
            // Conditional section that defines the current job output's format based on the loop's number
            if (currentLoop == numLoops) { // Last iteration, defines the output format as "article_name page-rank"
                pageRankJob.setOutputFormatClass(PageRankOutputFormat.class);
                nextPath = output;
            } else { // Intermediate iteration, defines the output format as "article_name page-rank outlinks_number outlinks_list
                pageRankJob.setOutputFormatClass(TextOutputFormat.class);
                nextPath = new Path("inter" + currentLoop);
                //FileOutputFormat.setOutputCompressorClass(pageRankJob, org.apache.hadoop.io.compress.SnappyCodec.class);
            }

            FileInputFormat.setInputPaths(pageRankJob, previousPath);
            FileOutputFormat.setOutputPath(pageRankJob, nextPath);

            pageRankJob.setInputFormatClass(TextInputFormat.class);
            // Wait for completion
            succeeded = pageRankJob.waitForCompletion(true);
            // After current succeeded execution, clean the previous filesystem staging files
            fsys.delete(previousPath, true);

            previousPath = nextPath;
            if (!succeeded) {
                break;
            }
        }

        return (succeeded ? 0 : 1);
    }

}
