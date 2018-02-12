# Mister Mighty PageRank 

Our PageRank implementation is comprised by two MapReduce jobs. One is used to preprocess the data coming from the input files and the other is where the PageRank calculation takes place.

## Cleaning stage

We will begin by describing the workings of the first job, which we will call "cleaning job/stage" from now on.

The purpose of the cleaning job is to take the data from the file and removing all the non important information and leave the data ready for the PageRank calculation. The input key for the first mapper is of type `Text` and it only contains the `article_title`, its associated value is a custom type named `WikiInputValue`, this custom type contains:

 - a `Text` field where the links going out of the article represented byu the key are stored,  
 - a `LongWritable` where the `rev_id` property is stored,
 - an `IntWritable` where the number of outlinks is stored  

In order to read the file and convert it to the key-value pairs mentioned before we created a couple of extra classes (`WikiInputFormat` and `WikiMultiRecordReader`) that allowed us to read the file in chunks of 14 lines, rather than going one by one as we need the information for a single revision to stay together.  

The output of this mapper is the same set of key-value pair of its input, as the mapper only passes whatever was read to the reducer.

The job of the reducer is to determine which revision should we take into account, in this case, we decided to use only the latest revision of each article, to select it, in the reducer we iterate over the list of values selecting the revision with the greatest `rev_id`. The output of this reducer is a key of type `Text` and a value of type `WikiInOutPageRankValue`, such type is composed of:

 - a `FloatWritable` that contains the specified PageRank score for the associated key
 - an `IntWritable` that specifies the number of outlinks the key has, and,
 - a `Text` that contains the actual outlinks for the given key

The goal of using this set of key-values is to have an output like the following:  

```
article_title1    page_rank1|number_of_outlinks1|outlinks1
article_title2    page_rank2|number_of_outlinks2|outlinks2
```

Where the  `page_rank` value is set to `1.0` for all pages.

## Page Rank calculation stage 


As we mentioned before, the output for the previous job is also the input for the calculation stage, for this job no custom input classes were needed as we can simply rely on the default  line-by-line reading interface that is provided by the MapReduce API. The key for the mapper is a `LongWritable` type that we are not using, and a `Text` that contains a single line from the file obtained form cleaning job execution. This mapper takes this `Text` value and splits it to obtain 4 different values:

 - `article_title`,
 - `page_rank` the calculated PageRank score for the page denoted by `article_title`,
 - `number_of_outlinks` this one is pretty self-explanatory,
 - `outlinks` a list of outlines separated by a `|`

Then, for each `outlink` in the list of `outlinks` the mapper emits a key-value pair where the key is a `Text` type that contains the value of `outlink`, with a value of type `WikiIntermediatePageRankValue`. This custom value contains:

 - a `FloatWritable` that specifies the page rank assigned to the page specified by `article_title`
 - an `IntWritable` that specifies the number of outlines that the `article_title` has
 - a `Text` that contains the "parent" of `outlink`, here we define parent as the article that points to it, in other words `article_title`. 

Finally, once all the values for the outlines have been emitted,  a final key-value pair is emitted this one has `article_title` as the key, and a `WikiIntermediatePageRankValue` with his "parent" set to itself, as well as an additional property of type `Text` that will contain the list of `outlinks`. This last value will help us to reconstruct the original input of the mapper in order to re-use the output of this job as its input.

As specified by the mapper, the reducer takes a list of...
