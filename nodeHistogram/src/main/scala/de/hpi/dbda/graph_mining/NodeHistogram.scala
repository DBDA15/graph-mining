package de.hpi.fgis.tpch

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

object NodeHistogram extends App {

  case class TwitterEntry(ID: Int, FollowerCount: Int, FollwingCount: Int) {
    def this(ID: String, Pos: Int) =
      this(ID.toInt, 1 - Pos, Pos)
    def asTuple = (ID, this)
  }

  //print("hello World")

  val inputPath = args(0)
  val followerPath = args(1)
  val followingPath = args(2)
  val combinedPath = args(3)

  val conf = new SparkConf()
  conf.setAppName(NodeHistogram.getClass.getName)
  conf.set("spark.hadoop.validateOutputSpecs", "false");
  val context = new SparkContext(conf)

  val twitterEntries =
    context.textFile(inputPath)
      .map(line => new TwitterEntry(line.split("\t")(0), 0))
      //.map(line => new TwitterEntry(line.split("\t")(1), 1))
}