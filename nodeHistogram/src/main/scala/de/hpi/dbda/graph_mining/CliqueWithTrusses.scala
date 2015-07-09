package de.hpi.dbda.graph_mining

import org.apache.spark.{SparkContext, HashPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by rice on 22.05.15.
 */
object CliqueWithTrusses {

  def maximumClique( graph:RDD[Truss.Edge], outputDir:String, sc:SparkContext): RDD[Array[Int]] ={
    val cliqueOut = outputDir + "/cliques"

    val degrees = Truss.calculateDegrees(graph).map(x=> x._2)
    val largestNDegrees = degrees.takeOrdered(Math.sqrt(degrees.count()).toInt)(Ordering[Int].reverse)

    //calculate initial k
    var k = 0
    var oldk = 0
    while ((k < largestNDegrees.length)  && (k < largestNDegrees(k))){
      k += 1
    }

    //var maxCliqueSize = sc.broadcast(0)
    //var maxClique = sc.broadcast(Array[Int]())


    var maxCliqueSize = 0
    var maxCliques: RDD[Array[Int]] = sc.emptyRDD

    while ((k > maxCliqueSize || (oldk != k -1 && oldk>maxCliqueSize)) && k > 2){
      println("k: " + k)
      val trusses = Truss.calculateTrusses(k-2, graph)._1

      //partiton
      val partitions = Math.max(trusses.map(_._1).distinct().count(), 100).toInt
      val partitionedEdgeComponents =
        trusses.partitionBy(new HashPartitioner(partitions))
          .persist(StorageLevel.MEMORY_AND_DISK)

      //all vertices in component, check if every edge exists -> if yes clique
      val groupedEdgesPerTruss = partitionedEdgeComponents.groupByKey()

     // var maxCliqueSizeLocal = 0//maxCliqueSize.value
     // var maxCliqueLocal = Array[Int]()//maxClique.value

      val result = groupedEdgesPerTruss.flatMap{truss =>
        val cliques = Clique.calculateCliques(truss._2.toArray, maxCliqueSize)
        if (cliques.length > 0 ) {
          val largestCliqueSize = cliques.maxBy(clique => clique.length).length
          val largestCliques = cliques.filter(c => c.length == largestCliqueSize)
          if (largestCliqueSize >= maxCliqueSize) {
            largestCliques
            //TODO broadCast?
          } else List()
        }else List()
      }

      result.persist(StorageLevel.MEMORY_AND_DISK)

    //  println("max clique size " + maxCliqueSize + " resultsize " + result.count())

      if (!result.isEmpty()) {
        maxCliqueSize = result.max()(Ordering[Int].on(e => e.length)).length

//        println("max clique size " + maxCliqueSize + " resultsize " + result.count())
//
        maxCliques = result.filter(c => c.length == maxCliqueSize)
        maxCliques.persist(StorageLevel.DISK_ONLY)
      }
//      if (maxCliqueSize.value < maxCliqueSizeLocal){
//        maxCliqueSize = sc.broadcast(maxCliqueSizeLocal)
//        maxClique = sc.broadcast(maxCliqueLocal)
//      }
      //maxClique.foreach(e => println(e))
      oldk = k
      k = k-Math.ceil(k*0.1).toInt
      partitionedEdgeComponents.unpersist()
      result.unpersist()
    }
    //maxCliques.foreach(e => println(e.foreach(i => print(i + " , "))))
    maxCliques
  }

}
