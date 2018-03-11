package pageRank

import pageRank.dataTypes.WikipediaRevision
import pageRank.dataTypes.WikipediaRevision

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

import scala.collection.immutable.Set

import java.util.ArrayList
import java.util.Date
import utils.ISO8601


/**
 * Main object for PageRank's calculation of a given set using Apache Hadoop's ecosystem.
 * The PageRank is an algorithm used to establish web-pages relevance or importance within
 * a web-site or the Internet.
 *
 * @author 2338066f ANTONIO FEREGRINO BOLANOS
 * @author 2338067g HOMERO GARCIA MERINO
 */

object Improved {
 
  /**
   * Main method responsible for orchestrating and managing the PageRank's calculation
   * scheduling Spark jobs in N iterations as requested by the client.
   *
   * @param args is a string array that contains the full set of command-line parameters.
   *   @param[0] input file path, e.g. Wikipedia edit history file.
   *   @param[1] output directory path (it can exist or not).
   *   @param[2] number of iterations for PageRank's calculation.
   *   @param[3] ISO8601-formatted date for which the PageRank scores will be computed.
   */
  def main(args: Array[String]) {
    // arguments validation and usage display in case of incorrect input of parameters
    // Iterations (args(2)) and ISO8601-formatted date (args(3)) arguments are optional
    if (args.length <  3) {
      System.err.println("MrPageRankII usage: PageRank <String input_filepath> <String output_filepath> <Int iterations> <Datetime YYYY-MM-DD'T'HH:mm:ss'Z' format>")
      System.exit(1)
    }
    
    // arguments handling
    val inputFile = args(0)
    val outputFile = new Path(args(1))
    val iterations = iterToInt(args(2)) // A default value is assigned if no value is present or the format is incorrect
    val timeLimit = if (args.length == 4) datetimeToISO8601(args(3)) else new Date().getTime // A default value is assigned if no value is present or the format is incorrect
    
    // create configuration object and register application
    val conf = new SparkConf().setAppName("Mighty-WikiPageRank_II")
    // obtain Spark context from configuration object
    val sc = new SparkContext(conf)
    sc.setJobDescription("Mighty-WikiPageRank_II")
    
		// Obtain filesystem configuration details from Hadoop's cluster
		val fsys = FileSystem.get(sc.hadoopConfiguration);
    // Delete recursively results - output directory if exists  
    fsys.delete(outputFile, true)
    
    // Set input format record delimiter for Wikipedia's HDFS files
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "\n\n")  

    // Data gathering from HDFS filesystem
    val revisions: RDD[WikipediaRevision] = sc.newAPIHadoopFile(inputFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      .values
      .map(textToRevision) // selection and cleansing processes
      .filter(r => r.revisionTime <= timeLimit) // All revisions, filtered initially by date

    // Second, and final filtering step (group by to get the last date of the filtered selection)
    val singleRevisions: RDD[WikipediaRevision] = revisions.groupBy(revision => revision.articleId)
      .mapValues(revisions => {
        var articleId = ""
        var outlinks = ""
        var timeDifference = Long.MaxValue

        for (revision <- revisions) {
          val difference = timeLimit - revision.revisionTime
          if (difference < timeDifference) {
            timeDifference = difference
            articleId = revision.articleId
            outlinks = if (revision.outlinks == null) "" else revision.outlinks
          }
        }
        // outlinks cleansing and Parent article Id assigned at the end of the string
        val cleanLinks = outlinks.split("\\s+").toSet - "" + articleId
        // wrapping data into WikipediaRevision class
        new WikipediaRevision(articleId, 0, cleanLinks.mkString(" "))
      }).values

    // Pairs in the form: (Parent article Id, outlinks)
    val links: RDD[(String, Array[String])] = singleRevisions
      .map(rev => (rev.articleId, rev.outlinks.split("\\s"))).cache()

    // Pairs in the form: (Parent article Id, PageRank score)
    var ranks: RDD[(String, Double)] = links.mapValues(rev => 1.0)
    // Number of partitions asigned to ranks
    var ranks_par = ranks.partitions.size
    
    // PageRank's iterative calculation
    for (i <- 1 to iterations) {
      // Pairs in the form: (Children, PageRank contribution from Parent article Id)
      val contribs: RDD[(String, Double)] = links.join(ranks)
        .flatMap((values) => {
          val(parentArticle, info) = values
          val(outlinks, pr) = info

          var res: List[Tuple2[String, Double]] = List[Tuple2[String, Double]]()
          var urlCount = outlinks.length - 1
          for (child <- outlinks) {
            val contribution = if (child == parentArticle) 0 else pr / urlCount
            val tuple = new Tuple2[String, Double](child, contribution)
            res = res.+:(tuple)
          }
          res
        })
      // PageRank calculation - repartition is performed to provide more resources for calculation stage
      ranks = contribs.reduceByKey((a, b) => a + b).mapValues(v => 0.15 + v * 0.85).repartition(ranks_par*3).setName("Rankings_RDD")
    }
    // Sorting in descending score order and delivery - write results to HDFS output path
    ranks.sortBy(f=>f._2, false,ranks_par*2).map(prs => prs._1+ " " + prs._2).setName("Sorting_RDD").saveAsTextFile(args(1))
    //ranks.sortBy(f=>f._2, false,40).map(prs => prs._1+ " " + prs._2).saveAsTextFile(args(1))

    // Close the SparkContextobject, releasing its resources.
    sc.stop()
  }
  
   /**
   * Handles iterations - args(2) integer conversion. 
   * If a string representation can not be converted the returned default value is 10. 
   * No exception is raised since an automatic adjustment is placed to continue with 
   * the page rank calculation.
   * @param iter string representing the number of iterations.
   * @return iterations integer
   */
  def iterToInt(iter: String): Int = {
    try {
      iter.toInt
    } catch {
      case e: Exception => 10
    }
  }
  
   /**
   * Handles PageRank timeLimit - args(3) date conversion. 
   * If a string representation can not be converted the returned default value is the actual date. 
   * No exception is raised since an automatic adjustment is placed to continue with 
   * the page rank calculation.
   * @param date string representing an ISO8601-formatted date.
   * @return search date
   */
  def datetimeToISO8601(date: String): Long = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val now = new java.util.Date()
    format.format(now)
    try {
      format.parse(date).getTime
    } catch {
      case e: Exception => now.getTime
    }
  }
  
  /**
     * Selection and cleansing process for gathered data.  
     * @param text Text textinputformat data.
     * @return WikipediaRevision wrapping object.
     */ 
  def textToRevision(text: Text) : WikipediaRevision  = {
        val lines = text.toString().split("\n") //identify attributes delimited by lines break
        val revision = lines(0).split(" ") //split revision's attribute contents with spaces
        val revisionId = revision(2).trim().toLong //get Revision Id from revision's attribute
        val articleId = revision(3).trim() //get Parent article Id from revision's attribute
        val revisionTime = ISO8601.toTimeMS(revision(4).trim()) //get Revision time from revision's attribute
        val mainArray = lines(3).split(" ", 2) //split main's attribute contents with spaces
        var outlinks: String = null
        if (mainArray.length == 2) {
          outlinks = mainArray(1) //get Out-links from main's attribute
        }
        return new WikipediaRevision(articleId, revisionId, outlinks, revisionTime)
  }
}