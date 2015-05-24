package de.hpi.dbda.graph_mining

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by rice on 22.05.15.
 */
object CliqueWithTrusses {

  def maximumClique( rawGraph:RDD[String], outputDir:String, seperator:String): Unit ={
    val cliqueOut = outputDir + "/cliques"

    val graph:RDD[Truss.Edge] = Truss.convertGraph(rawGraph, seperator)

    val degrees = Truss.calculateDegrees(graph).map(x=> x._2)
    val largestNDegrees = degrees.takeOrdered(Math.sqrt(degrees.count()).toInt)(Ordering[Int].reverse)

    //calculate initial k
    var k = 0
    while (k < largestNDegrees(k)){
      k += 1
    }

    var maxCliqueSize = 0
    var maxClique:Array[Int] = Array[Int]()

    while (k > maxCliqueSize){
      val trusses = Truss.calculateTrusses(k-2, graph)

      /*
        trusses.foreach truss
          cliques = calculate cliques in truss
          if cliques found:
            if size of largestClique > maxCliqueSize
              maxClique = clique
              broadcast new maxCliqueSize
            else
              nothing
          else
            nothing
        k = k - 1
      */
      //partiton
      val partitions = Math.max(trusses.map(_._1).distinct().count(), 100).toInt
      val partitionedEdgeComponents =
        trusses.partitionBy(new HashPartitioner(partitions))
          .persist(StorageLevel.MEMORY_AND_DISK)

      //all vertices in component, check if every edge exists -> if yes clique
      val groupedEdgesPerTruss = partitionedEdgeComponents.groupByKey()

      groupedEdgesPerTruss.foreach{truss =>
        val cliques = Clique.calculateCliques(truss._2.toArray)
        if (cliques.length > 0 ){
          val largestClique = cliques.maxBy(clique => clique.length)
          if (largestClique.length > maxCliqueSize){
            maxClique = largestClique
            maxCliqueSize = largestClique.length
            //TODO broadCast?
          }
        }
        k = k-1
      }
    }
  }

}
