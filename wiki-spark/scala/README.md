## Solution outline

## Why Scala?
We choose Scala because we wanted to experiment and learn a language that is being highly demanded by companies hiring Big Data professionals. 

### Data gathering
Following the lecturer's recommendation, the file was read using the MapReduce's `TextInputFormat` class to read the data and specifying the custom delimiter `"\n\n"'`, each record is mapped to a user-defined class  (`WikipediaRevision`) that contains all the required information. 

### Filtering process
Filtering is a two-step process; the first step is applied right after finishing mapping the records to our custom class, filtering out all the revisions with a date greater than the specified by the user. Something to highlight is that we are converting all dates to their long representation in an attempt to increase performance and decrease resources usage.

We apply the second step to the already filtered data; this filter helps to get the latest revision for each article. This filtering is performed by comparing the difference between the specified date and each revision's date. 

After the latest revision is selected, its list of outlinks is cleaned by removing duplicates and self-references. However, here is where we came up with the idea of adding a link to itself at the end of the outlink list, to avoid losing unreferenced (without links pointing to them) articles. Otherwise, we would need to perform an outer join when calculating the PageRank with the algorithm provided.

### PageRank Calculation
The PageRank calculation was preserved as explained in the Lecture. Some modifications were made due to previous adjustments discussed above.
The first modification to avoid using the self-reference outlink (introduced by our process) was made to prevent a wrong PageRank calculation.
The second modification was an attempt to improve the distribution of the workload by multiplying the default number of partitions (assigned by the system automatically) by 3. This means that whatever number of workers assigned to our application, for that step we increase that number three times. 

### Writing an appropriate output
After the iterative calculation ends, a writing step is in charge of saving the results to disk. A sorting process is applied to order them by the PageRank score. Once again, the workload is redistributed with a repartition, this time, multiplying by two the assigned number of partitions (instead of three like in the calculation step).

Finally, to get the required output, each key-value is mapped to a string containing the article and its PageRank separated by a blank space. Then, the Spark context is explicitly stopped to release resources.

### Helper methods
A set of auxiliary methods were created to avoid cluttering the code, this includes: 

 - `iterToInt`: this handles the iterations integer conversion from string to int. If a string representation cannot be converted the returned value es 10.
 - `dateTimeToISO8601`: this handles the date conversion from string to long. If a string representation cannot be converted to a date, the returned value is the current date. 
 - `textToRevision`: this handles the conversion from a `Text` data type to our custom `WikipediaRevision`.


## Build & package

As standard with Scala/Spark projects, package this solution, it is needed to have installed [sbt](https://www.scala-sbt.org/), if you alread have it, it is just necessary to execute

```
sbt package
```

This will create the file `target/scala-2.10/spark-pagerank_2.10-0.1.jar`. The class name to execute the job is `pageRank.Improved`.
