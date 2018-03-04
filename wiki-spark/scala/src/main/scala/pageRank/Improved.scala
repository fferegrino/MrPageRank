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
import utils.ISO8601
import java.util.Date
import scala.collection.immutable.Set
import org.apache.spark.rdd.RDD

object Improved {

  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val iterations = args(2).toInt
    val timeLimit = if (args.length > 3) ISO8601.toTimeMS(args(3)) else new Date().getTime

    val conf = new SparkConf().setAppName("Original Page Rank")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "\n\n")

    // All revisions, filtered initially by date
    val revisions: RDD[WikipediaRevision] = sc.newAPIHadoopFile(inputFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      .map(v => v._2.toString())
      .map(record => {
        val lines = record.split("\n")
        val revision = lines(0).split(" ")
        val revisionId = revision(2).trim().toLong
        val articleId = revision(3).trim()
        val revisionTime = ISO8601.toTimeMS(revision(4).trim())
        val mainArray = lines(3).split(" ", 2)
        var outlinks: String = null
        if (mainArray.length == 2) {
          outlinks = mainArray(1)
        }
        new WikipediaRevision(articleId, revisionId, outlinks, revisionTime)
      }).filter(r => r.revisionTime <= timeLimit)

    // Second, and final filtering step
    val singleRevisions: RDD[WikipediaRevision] = revisions.groupBy(revision => revision.articleId)
      .mapValues(revisions => {
        var latestRevisionId: Long = 0
        var articleId = ""
        var outlinks = ""
        var timeDifference = Long.MaxValue

        for (revision <- revisions) {
          val difference = timeLimit - revision.revisionTime
          if (difference < timeDifference) {
            timeDifference = difference
            articleId = revision.articleId
            latestRevisionId = revision.revisionId
            outlinks = if (revision.outlinks == null) "" else revision.outlinks
          }
        }

        val cleanLinks = outlinks.split("\\s+").toSet - "" + articleId
        new WikipediaRevision(articleId, latestRevisionId, cleanLinks.mkString(" "))
      })
      .map(t => t._2)

    // Pairs in the form: (Parent, outlink)
    val links: RDD[(String, Array[String])] = singleRevisions
      .keyBy(_.articleId)
      .mapValues(rev => rev.outlinks.split("\\s")).cache()

    // Pairs in the form: (Parent, PageRank score)
    var ranks: RDD[(String, Double)] = links.keyBy(_._1).map(rev => (rev._1, 1.0))

    for (i <- 1 to iterations) {

      // Pairs in the form: (Children, PageRank contribution from parent)
      val contribs: RDD[(String, Double)] = links.fullOuterJoin(ranks)
        .flatMap((v) => {
          val parent = v._1
          val outlinks = v._2._1.getOrElse(Array[String]())
          val pr = v._2._2.getOrElse(1.0)

          var res: List[Tuple2[String, Double]] = List[Tuple2[String, Double]]()
          var urlCount = outlinks.length - 1
          for (s <- outlinks) {
            if (parent != s) {
              val tuple = new Tuple2[String, Double](s, pr / urlCount)
              res = res.+:(tuple)
            } else { // No contribution!
              val t1 = new Tuple2(s, 0.0)
              res = res.+:(t1)
            }
          }
          res
        })
      ranks = contribs.reduceByKey((a, b) => a + b).mapValues(v => 0.15 + v * 0.85);
    }

    ranks.sortBy(f=>f._2, false).map(prs => prs._1 + " " + prs._2).saveAsTextFile(outputFile)

  }
}