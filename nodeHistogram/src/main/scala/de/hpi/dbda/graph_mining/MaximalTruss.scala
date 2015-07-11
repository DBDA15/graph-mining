package de.hpi.dbda.graph_mining

import java.io.{File, PrintWriter}

import de.hpi.dbda.graph_mining.Truss.Edge
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by rice on 26.05.15.
 */
object MaximalTruss {

  def maximumTruss(graph: RDD[Edge], context:SparkContext, outputPath:String, stringk:String, partitioning:Int): RDD[Truss.Edge] ={
    val k = stringk.toInt

    val degreedGraph = Truss.addDegreesToGraph(graph).distinct()
//    val result = recursiveTruss(k, 0 ,2, List(degreedGraph), context)

    val result = maxTruss(k,degreedGraph, partitioning)
//    result.foreach{t =>
//      t.foreach(e => print(e + ", "))
//      println("")}

    result
   // result.foreach(t => t.saveAsTextFile(outputFile))

//    result.zipWithIndex.foreach { case (t, i) =>
//
//      val truss = t.collect()
//      val vertexSet = Clique.getVertexSet(truss)
//      val printWriter = new PrintWriter(new File(outputFile + i.toString))
//      vertexSet.foreach(a => {
//        printWriter.println(a)
//      })
//      printWriter.close()
//    }
  }


//  //returns list of subgraphes
//  def recursiveTruss(k:Int, maxK:Int, minK:Int, graphs: List[RDD[Edge]], context:SparkContext, partitioning:Int): List[RDD[Edge]] = {
//
//    println(k)
//    if (maxK == k || minK == k){
//      println("return k  " + k)
//      graphs
//    } else {
//
//      val foundTrusses = graphs.flatMap{ graph =>
//        val trusses = Truss.calculateTrusses(k-2, graph.filter(e => e.vertex1.degree >= k-1 && e.vertex2.degree >= k-1), partitioning)._1
//        val groupedEdgesPerTruss = trusses.groupByKey()
//
//        val x = groupedEdgesPerTruss.collect()
//          .map(e => context.parallelize(e._2.toSeq)).toList
//        x
//      }
//
//      if (foundTrusses.isEmpty){
//        val newK = minK + (k-minK)/2
//        recursiveTruss(newK, k, minK, graphs, context, partitioning)
//      } else {
//        if (maxK == 0){
//          val newK = 2*k
//          recursiveTruss(newK, maxK, k, foundTrusses, context, partitioning)
//        } else {
//          val newK = k + (maxK-k)/2
//          recursiveTruss(newK, maxK, k, foundTrusses, context, partitioning)
//        }
//      }
//    }
//  }

  def maxTruss(initialK:Int, graph:RDD[Truss.Edge], partitioning:Int): RDD[Edge] = {
    var graphs = graph
    var maxK = 0
    var minK = 2
    var k = initialK

    while (k != maxK && k != minK) {

      println("k is " + k)
      val filteredGraph = graphs.filter(e => e.vertex1.degree >= k - 2 && e.vertex2.degree >= k - 2)
      //      filteredGraph.print()

      val trusses = Truss.calculateTrusses(k-2, filteredGraph, partitioning)._1

      val foundTrusses: RDD[Edge] = trusses.map { truss =>
        truss._2.truss = truss._1
        truss._2
      }

      if (foundTrusses.count == 0) {
        val newK = minK + (k - minK) / 2
        maxK = k
        k = newK
      } else {
        if (maxK == 0) {
          val newK = 2 * k
          minK = k
          k = newK
        } else {
          val newK = k + (maxK - k) / 2
          minK = k
          k = newK
        }

        graphs = foundTrusses
      }

    }

    println("k is " + k)
    graphs



  }


}
