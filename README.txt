This file contains a simple explanation of how to execute and the input/output types of this implementation of the PageRank algorithm, for a deeper explanation please see the README.md file.

After having succesfully compiled the project and generated the file "wiki/target/uog-bigdata-0.0.1-SNAPSHOT.jar" it is necessary to point the HADOOP_CLASSPATH to it. After that, it is just a matter of running the job using "mapreduce.WikiPageRank" as the classnamme for the job.  

Input-Ouptut key-value pairs  

  First MapReduce job:
    Input for mapper:   Text as key, Text as value
    Input for reducer:  Text as key, WikiInputValue (custom type)
    Output of reducer:  Text as key, WikiInOutPageRankValue (custom type)

  Second MapReduce job:
    Input for mapper:   LongWritable as key, Text as value
    Input for reducer:  Text as key, WikiIntermediatePageRankValue (custom type)
    Output of reducer:  Text as key, WikiInOutPageRankValue (custom type)
  
Assumptions we are making:
 - We are only considering the latest revision found on the revisions dataset
 - We are considering just one link between two pages and discarding the rest
 
 Please, look for more details on the README.md file!
