tl;dr version  

After succesfully compiling the project and generating the JAR file "wiki/target/uog-bigdata-0.0.1-SNAPSHOT.jar" it is required to assign it to the HADOOP_CLASSPATH variable. After that, run the job with "mapreduce.WikiPageRank" as the classnamme for the job.  

Input-Ouptut key-value pairs  

  First MapReduce job ("data gathering and cleansing"):
    Input for mapper:   Text as key, Text as value
    Input for reducer:  Text as key, WikiInputValue (custom type)
    Output of reducer:  Text as key, WikiInOutPageRankValue (custom type)

  Second MapReduce job ("PageRank's calculation"):
    Input for mapper:   LongWritable as key, Text as value
    Input for reducer:  Text as key, WikiIntermediatePageRankValue (custom type)
    Output of reducer:  Text as key, WikiInOutPageRankValue (custom type)
  
Assumptions:
 - Only the latest revision records are considered. This means that the bigger revision number for a specific Article is processed. The rest of the historical records are discarded.
 - The out-links list contains only unique keys (Articles). This means that a "Referenced" article page can not be voted twice or more times by a single "Parent" Article page.
 - An article can reference itself. The same rule about "multiple" voting applies for these cases. 
 - There can be Articles with no references to them. These articles receive a default PageRank of 0.15
 
For more details, check the following section!

Long version (uses markdown)

# Mister Mighty PageRank 

Our PageRank implementation is comprised by two MapReduce jobs. The first one pre-processes the data retrieved from the input files (data source), and the second performs the PageRank calculation.

## Gathering and cleansing stage

The first stage/job is called "data gathering and cleansing".

The purpose of this job is to gather the data from the source file and remove not required/important information, formatting the output for the PageRank calculation. The input type's key for the first mapper is `Text`, and contains the `article_title`. Its value type is `Text` and contains the contents read from the `MAIN` tag.  

### Custom InputFormat. 

For the data gathering process we created specific classes (`WikiInputFormat` and `WikiMultiRecordReader`) for reading the file in chunks of 14 lines(record terminator included), rather than reading one by one as we need the data for a single revision to stay together.  

### Mapper

The mapper parses the list of values obtained from the value variable, getting the corresponding `rev_id` and splitting the list of outlinks (using the regular expression "\s") to count them, and joining them by a single blank space. The output of this mapper is a custom type named `WikiInputValue`, defined as:

 - a `Text` field with the out-links of the article,  
 - a `LongWritable` with the `rev_id`,
 - an `IntWritable` with the number of out-links

This record value is identified by a `Text` key (`article_title`).

### Reducer

Determines which revision should be processed. According to our assumptions, only the latest revision for each article should be processed. For selection, the reducer iterates over a list of values picking the revision with the greatest `rev_id`. The outputs are: a `Text` key and a `WikiInOutPageRankValue` value, defined as:

 - a `FloatWritable` with the current PageRank's score,
 - an `IntWritable` with the number of out-links,
 - a `Text` that contains the actual out-links for the given key

The resulting key-value pair output is:  

```
article_title1    page_rank1|number_of_outlinks1|outlinks1
article_title2    page_rank2|number_of_outlinks2|outlinks2
```

The init `page_rank` value is `1.0` for all articles.

### Combiner

We created a combiner process for performing the reducer's logic, in order to reduce the number of records transferred through the network to the reducer's servers.

## Page Rank calculation stage 
The PageRank is a `vote`, by all the other pages on a Web-site or the Web, and establishes how important a page is. A `link` from a `Parent` page to a `Referenced` page counts as a vote of support. If there is no link from a `Parent` there is no support, but it should be considered more like a voting abstention rather than a vote against.

The implemented `PageRank` calculation is defined as: `PR(R)` = (1-d) + d (`PR(P1)`/`C(P1)` + ... + `PR(Pn)`/`C(Pn)`)
Where: `d` = 0.85 and the `PageRank - PR` of the `Referenced` page `PR(R)` depends on the voting from all of its `Parent` pages `(Pn)`, based on their current PageRank `PR(Pn)` and their number of  outgoing links `C(Pn)`.

### Mapper

As explained before, our logic states that the output for the previous loop job is the input for the next iteration. For this stage no custom input classes were needed as it relies on the default line-by-line reading interface provided by the MapReduce API. The Mapper's key is a `LongWritable` type, and a `Text` value. The mapper reads the `Text` value and splits it to obtain:
 - The `article_title`,
 - The `page_rank` score for the `article_title`,
 - The `number_of_outlinks` for the `article_title`,
 - The `out-links` list separated by `|`

For each `out-link` the mapper produces a key-value pair with a `Text` type key that contains the "Referenced" article (`out-link`), and a custom value type `WikiIntermediatePageRankValue`, defined as:

 - a `FloatWritable` with the PageRank for the Parent's `article_title`
 - an `IntWritable` with the number of out-links,
 - a `Text` for the Parent's `article_title`. The "Parent" article is defined as the article that has reference to other articles. 

Finally, and this is a trick we came up with, once all the values for the out-links have been written, a final key-value pair is created with the current state of the "Parent" article. This record defines the `article_title` as the key, and a `WikiIntermediatePageRankValue` with its "parent" set to itself, as well as an additional `Text` property named `outlinks` that contains the list of original outlinks (current state). This value is used to rebuild the original input of the mapper and execute a new iteration of the PageRank's job.

### Reducer

As explained in the Mapper job, the reducer takes a list of `WikiIntermediatePageRankValue` values, for the PageRank calculation of a given page over a single `for` loop where only the records that meet the following criteria are processed:

 - The field `parent` value is different to the key and,
 - the `out-links` field have no out-links 

When a record's value does not meet these conditions it means that it contains the out-links list of the article and it is a "Parent" article and should not be considered for the calculation process.

After the PageRank calculation, a single `WikiInOutPageRankValue` output is generated with all the information related to the given key, the output is printed to the file in the following manner:  

```
article_title1    page_rank1|number_of_outlinks1|outlinks1
```

This is the input expected for the PageRank Mapper.

### Producing the required output

If the PageRank MapReduce job is always feeding itself, how can it produce a final output as expected by the user?

The answer can be set on the `main` method, where the jobs are scheduled. Within the loop section we create one job after another for each iteration, sincronizing the creation and deletion of input and output files. It can be identified the execution of the last iteration, and change the default `TextOutputFormat` to a custom `PageRankOutputFormat` used to write the required output file:

```
article_title1 score1
article_title2 score2
```

## Assumptions made by this implementation

 - Only the latest revision records are considered. This means that the bigger revision number for a specific Article is processed. The rest of the historical records are discarded.
 - The out-links list contains only unique keys (Articles). This means that a "Referenced" article page can not be voted twice or more times by a single "Parent" Article page.
 - An article can reference itself. The same rule about "multiple" voting applies for these cases. 
 - There can be Articles with no references to them. These articles receive a default PageRank of 0.15 as specified by the PageRank calculation formula.

## Interesting aspects of the solution
 - One way we found to optimize the usage of disk storage, network and data processing resources was through generating the least amount of required data for each phase.
 - The Mapping output compression was enabled for all the Mapping processes using the `SnappyCodec`. The overhead for `compressing/decompressing` data is almost always minimal compared to the large gains from sending over the network significantly lower data volumes from compressed data.
 - Automatic deletion of the staging (working) files and final results, through the usage of the `org.apache.hadoop.fs.FileSystem` class. Our solution only uses the required amount of disk space for each iteration.
 - Setting the default number of reducers (`setNumReduceTasks`) to 80 for the `PageRank` calculation. In this way, based on the knowledge we have about the cluster, we can maximize its usage.
 - Testing and comparing the results using Python's `Jupyter Notebooks` (included in the delivery package - `MrPageRank/notebooks` directory). With these unit-test components we can guarantee that the `PageRank` results generated by the solution are the same as those proposed by Dr. Ntarmos `Variant 3` scenario.
