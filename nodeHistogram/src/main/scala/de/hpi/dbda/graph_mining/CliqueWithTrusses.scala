package de.hpi.dbda.graph_mining

import org.apache.spark.{SparkContext, HashPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by rice on 22.05.15.
 */
object CliqueWithTrusses {

  def maximumClique( graph:RDD[Truss.Edge], outputDir:String, sc:SparkContext): Unit ={
    val cliqueOut = outputDir + "/cliques"

    val degrees = Truss.calculateDegrees(graph).map(x=> x._2)
    val largestNDegrees = degrees.takeOrdered(Math.sqrt(degrees.count()).toInt)(Ordering[Int].reverse)

    //calculate initial k
    var k = 0
    while ((k < largestNDegrees.length)  && (k < largestNDegrees(k))){
      k += 1
    }

    println("inital k: " + k)

    var maxCliqueSize = sc.broadcast(0)
    var maxClique = sc.broadcast(Array[Int]())

    while (k > maxCliqueSize.value && k > 2){
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


      var maxCliqueSizeLocal = 0//maxCliqueSize.value
      var maxCliqueLocal = Array[Int]()//maxClique.value

      val result = groupedEdgesPerTruss.map{truss =>
        val cliques = Clique.calculateCliques(truss._2.toArray)
        if (cliques.length > 0 ) {
          val largestClique = cliques.maxBy(clique => clique.length)
          if (largestClique.length > maxCliqueSizeLocal) {
            maxCliqueLocal = largestClique
            maxCliqueSizeLocal = largestClique.length

            maxCliqueLocal.foreach(e => println(e))
            maxCliqueLocal.toList
            //TODO broadCast?
          } else List()
        }else List()
      }

//      if (maxCliqueSize.value < maxCliqueSizeLocal){
//        maxCliqueSize = sc.broadcast(maxCliqueSizeLocal)
//        maxClique = sc.broadcast(maxCliqueLocal)
//      }
      println("##########################################################################################")
      result.foreach(e=> e.foreach(i => println(i)))
      maxCliqueLocal.foreach(e => println(e))
      k = k-1
    }
//    maxClique.value.foreach(e => println(e))
  }

}
