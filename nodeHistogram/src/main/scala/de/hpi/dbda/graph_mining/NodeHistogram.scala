package de.hpi.fgis.tpch

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

object NodeHistogram extends App {

  val splitCharacter_Twitter = "\t"
  val splitCharacter_Wikipedia = " "

  case class TwitterEntry(id: Int, followerCount: Int, followingCount: Int) {

    def this(id: String, pos: Int) =
      this(id.toInt, 1 - pos, pos)

    def asTuple = (id, this)
  }

  //print("hello World")

  @Override
  override def main(args: Array[String]) {
    val inputPath = args(0)

    val conf = new SparkConf()
    conf.setAppName(NodeHistogram.getClass.getName)
    conf.set("spark.hadoop.validateOutputSpecs", "false");
    val context = new SparkContext(conf)

    calculateIncomingOutcomingCount(context, inputPath, args)
  }

  def convertToBidirectedGraph(context:SparkContext, inputPath: String): Unit ={
    val output = args(1)

  }




  def calculateIncomingOutcomingCount(context:SparkContext, inputPath:String, args: Array[String]): Unit = {

    val followerPath = args(1)
    val followingPath = args(2)
    val combinedPath = args(3)

    val twitterEntries =
      context.textFile(inputPath)
        .flatMap(line => List(
        new TwitterEntry(line.split("\t")(0),0).asTuple,
        new TwitterEntry(line.split("\t")(1),1).asTuple))

    val relationCountEntries = twitterEntries.reduceByKey((entry1, entry2) =>
      new TwitterEntry(
        entry1.id,
        entry1.followerCount + entry2.followerCount,
        entry1.followingCount + entry2.followingCount))

    // calculate followerCount
    val followerCount = relationCountEntries.map(twitterEntry => (twitterEntry._2.followerCount, 1))
      .reduceByKey((f1, f2) => f1 + f2)

    followerCount.saveAsTextFile(followerPath)

    //calculate followingCount
    val followingCount = relationCountEntries.map(twitterEntry => (twitterEntry._2.followingCount, 1))
      .reduceByKey((f1, f2) => f1 + f2)

    followingCount.saveAsTextFile(followingPath)

    //calculate combinedCount
    val combinedCount = relationCountEntries.map(twitterEntry => (twitterEntry._2.followingCount + twitterEntry._2.followerCount, 1))
      .reduceByKey((f1, f2) => f1 + f2)

    combinedCount.saveAsTextFile(combinedPath)
    //val test = combinedCount.take(30)

  }
}