package de.hpi.dbda.graph_mining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import org.apache.log4j.{Level, Logger}

object GraphMiningSpark extends App {

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

    // ...
    val level = Level.WARN
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)


    val mode = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    var seperator = "\t"

    if (args.length > 3) seperator = args(3)

    val conf = new SparkConf()
    conf.setAppName(GraphMiningSpark.getClass.getName)
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val context = new SparkContext(conf)

    if (mode.equals("bidirect"))
    //calculateIncomingOutcomingCount(context, inputPath, args)
      convertToBidirectedGraph(context, inputPath, outputPath, seperator)

    if (mode.equals("triangle"))
      Truss.getTrianglesAndSave(context.textFile(inputPath), outputPath, seperator)

    if (mode.equals("truss"))
      Truss.calcTrussesAndSave(2, context.textFile(inputPath), outputPath, seperator)

    if(mode.equals("histo"))
      calculateIncomingOutcomingCount(context,inputPath, outputPath)

    if(mode.equals("clique"))
      CliqueWithTrusses.maximumClique(Truss.convertGraph(context.textFile(inputPath), seperator), outputPath, context)


    //TODO: Remove - Testing only: calculates the degree of all nodes and orders the result
    if(mode.equals("degree"))
      Truss.addDegreesToGraph(Truss.convertGraph(context.textFile(inputPath), seperator))

    //TODO: Remove - Testing only: calculates the cliques in graph
    if(mode.equals("cliqueSingle"))
      Clique.calculateCliques(Truss.convertGraph(context.textFile(inputPath), seperator).collect(), 0)

  }



  def convertToBidirectedGraph(context:SparkContext, inputPath: String, outputPath: String, seperator:String):  RDD[(GraphMiningSpark.Edge)] ={

    val edges =
      context.textFile(inputPath)
        .flatMap(line => List(
//        new Edge(0,0,0,0).apply(line.split("\t")(0).toInt,line.split("\t")(1).toInt).asTuple))
       generateEdge(line.split(seperator)(0).toInt,line.split(seperator)(1).toInt).asTuple))

    val summedEdges = edges.reduceByKey((edge1, edge2) =>
      edge1.add(edge2))

    def isBidirectional(inp: Edge) = inp.follows >= 1 && inp.followed >= 1
    val bidirectionalEdges = summedEdges.filter(edge => isBidirectional(edge._2))

    bidirectionalEdges.keys.saveAsTextFile(outputPath)
    bidirectionalEdges.values
  }

  def generateEdge(id_1: Int, id_2: Int): Edge = {
    val id1_ = if (id_1 < id_2) id_1 else id_2
    val id2_ = if (id_1 < id_2) id_2 else id_1
    val follows_ = if (id_1 < id_2) 0 else 1
    val followed_ = if (id_1 < id_2) 1 else 0
    val newEdge = new Edge(id1_, id2_, follows_, followed_)
    newEdge
  }


  def calculateIncomingOutcomingCount(context:SparkContext, inputPath:String, outputDir:String): Unit = {

    val followerPath = outputDir + "/follower"
    val followingPath = outputDir + "/following"
    val combinedPath = outputDir + "/combined"

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