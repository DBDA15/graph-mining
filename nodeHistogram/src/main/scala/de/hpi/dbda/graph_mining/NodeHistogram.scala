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

  case class Edge(id1: Int, id2: Int, follows: Int, followed: Int) {

    def asTuple = (id1.toString + "\t" + id2.toString, this)

    def add(other: Edge): Edge = {
      val follows_ = follows + other.follows
      val followed_ = followed + other.followed
      new Edge(id1, id2, follows_, followed_)
    }
  }

  //print("hello World")

  @Override
  override def main(args: Array[String]) {
    val inputPath = args(0)

    val conf = new SparkConf()
    conf.setAppName(NodeHistogram.getClass.getName)
    conf.set("spark.hadoop.validateOutputSpecs", "false");
    val context = new SparkContext(conf)

    //calculateIncomingOutcomingCount(context, inputPath, args)
    convertToBidirectedGraph(context, inputPath, args)
  }

  def convertToBidirectedGraph(context:SparkContext, inputPath: String, args: Array[String]): Unit ={
    val output = args(1)
    
    val edges =
      context.textFile(inputPath)
        .flatMap(line => List(
//        new Edge(0,0,0,0).apply(line.split("\t")(0).toInt,line.split("\t")(1).toInt).asTuple))
       generateEdge(line.split("\t")(0).toInt,line.split("\t")(1).toInt).asTuple))

    val summedEdges = edges.reduceByKey((edge1, edge2) =>
      edge1.add(edge2))

    def isBidirectional(inp: Edge) = inp.follows >= 1 && inp.followed >= 1
    val bidirectionalEdges = summedEdges.filter(edge => isBidirectional(edge._2))

    bidirectionalEdges.keys.saveAsTextFile(args(1))
  }

  def generateEdge(id_1: Int, id_2: Int): Edge = {
    val id1_ = if (id_1 < id_2) id_1 else id_2
    val id2_ = if (id_1 < id_2) id_2 else id_1
    val follows_ = if (id_1 < id_2) 0 else 1
    val followed_ = if (id_1 < id_2) 1 else 0
    val newEdge = new Edge(id1_, id2_, follows_, followed_)
    newEdge
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