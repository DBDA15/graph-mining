package de.hpi.dbda.graph_mining

import de.hpi.dbda.graph_mining.Truss.Edge
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MaximalTruss {

  def maximumTruss(graph: RDD[Edge], context:SparkContext, outputPath:String, stringk:String, partitioning:Int): RDD[Truss.Edge] ={
    val k = stringk.toInt

    val degreedGraph = Truss.addDegreesToGraph(graph).distinct()

    val result = maxTruss(k,degreedGraph, partitioning)
    result

  }

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
