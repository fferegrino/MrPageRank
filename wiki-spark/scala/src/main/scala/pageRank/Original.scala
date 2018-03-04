package pageRank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

import pageRank.dataTypes.WikipediaRevision
import pageRank.dataTypes.WikipediaRevision
import org.apache.spark.api.java.JavaPairRDD
import java.util.ArrayList

object Original {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val iterations = args(2).toInt

    val conf = new SparkConf().setAppName("Original Page Rank")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "\n\n")

    val revisions = sc.newAPIHadoopFile(inputFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      .map(v => v._2.toString())
      .map(record => {
        val lines = record.split("\n")
        val revision = lines(0).split(" ")
        val revisionId = revision(2).trim().toLong
        val articleId = revision(3).trim()
        val mainArray = lines(3).split(" ", 2)
        var outlinks: String = null
        if (mainArray.length == 2) {
          outlinks = mainArray(1)
        }
        new WikipediaRevision(articleId, revisionId, outlinks)
      })

    val singleRevisions = revisions.groupBy(revision => revision.articleId)
      .mapValues(revisions => {
        var latestRevisionId: Long = 0
        var articleId = ""
        var outlinks = ""

        for (revision <- revisions) {
          if (revision.revisionId > latestRevisionId) {
            articleId = revision.articleId
            latestRevisionId = revision.revisionId
            outlinks = revision.outlinks
          }
        }
        new WikipediaRevision(articleId, latestRevisionId, outlinks)
      })
      .map(t => t._2)

    val links = singleRevisions
      .keyBy(_.articleId)
      .mapValues(rev => rev.outlinks.split("\\s"))

    var ranks = links.keyBy(_._1).map(rev => (rev._1, 1.0))

    for (i <- 1 to iterations) {
      val contribs = links.join(ranks).values
        .flatMap((v) => {
          var res: List[Tuple2[String, Double]] = List[Tuple2[String, Double]]()
          var urlCount = v._1.length
          for (s <- v._1) {
            val tuple = new Tuple2[String, Double](s, v._2 / urlCount)
            res = res.+:(tuple)
          }
          res
        })
      ranks = contribs.reduceByKey((a, b) => a + b).mapValues(v => 0.15 + v * 0.85);
    }

    ranks.saveAsTextFile(outputFile)

  }
}