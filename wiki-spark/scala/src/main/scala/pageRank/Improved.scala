package pageRank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

import pageRank.dataTypes.WikipediaRevision
import pageRank.dataTypes.WikipediaRevision

import org.apache.spark.api.java.JavaPairRDD
import java.util.ArrayList
import utils.ISO8601
import java.util.Date
import scala.collection.immutable.Set
import org.apache.spark.rdd.RDD

object Improved {

  def main(args: Array[String]) {
    if (args.length <  3) {
      System.err.println("MrPageRankII usage: PageRank <String input_filepath> <String output_filepath> <Int iterations> <Datetime YYYY-MM-DD'T'HH:mm:ss'Z' format>")
      System.exit(1)
    }
    
    // arguments handling
    val inputFile = args(0)
    val outputFile = new Path(args(1))
    val iterations = iterToInt(args(2))
    val timeLimit = if (args.length == 4) datetimeToISO8601(args(3)) else new Date().getTime
    
    
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

    // All revisions, filtered initially by date
    val revisions: RDD[WikipediaRevision] = sc.newAPIHadoopFile(inputFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      .values
      .map(textToRevision)
      .filter(r => r.revisionTime <= timeLimit)

    // Second, and final filtering step
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

        val cleanLinks = outlinks.split("\\s+").toSet - "" + articleId
        new WikipediaRevision(articleId, 0, cleanLinks.mkString(" "))
      }).values

    // Pairs in the form: (Parent, outlink)
    val links: RDD[(String, Array[String])] = singleRevisions
      .map(rev => (rev.articleId, rev.outlinks.split("\\s"))).cache()

    // Pairs in the form: (Parent, PageRank score)
    var ranks: RDD[(String, Double)] = links.mapValues(rev => 1.0)

    for (i <- 1 to iterations) {
      // Pairs in the form: (Children, PageRank contribution from parent)
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
      ranks = contribs.reduceByKey((a, b) => a + b).mapValues(v => 0.15 + v * 0.85);
    }
    
    ranks.sortBy(f=>f._2, false,40).map(prs => prs._1+ " " + prs._2).saveAsTextFile(args(1))
    //ranks.map(prs => prs._1 + " " + prs._2).saveAsTextFile(args(1))
    
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
  
  def textToRevision(text: Text) : WikipediaRevision  = {
        val lines = text.toString().split("\n")
        val revision = lines(0).split(" ")
        val revisionId = revision(2).trim().toLong
        val articleId = revision(3).trim()
        val revisionTime = ISO8601.toTimeMS(revision(4).trim())
        val mainArray = lines(3).split(" ", 2)
        var outlinks: String = null
        if (mainArray.length == 2) {
          outlinks = mainArray(1)
        }
        return new WikipediaRevision(articleId, revisionId, outlinks, revisionTime)
  }
}