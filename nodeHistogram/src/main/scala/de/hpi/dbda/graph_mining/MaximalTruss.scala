package de.hpi.dbda.graph_mining

import java.io.{File, PrintWriter}

import de.hpi.dbda.graph_mining.Truss.Edge
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by rice on 26.05.15.
 */
object MaximalTruss {

  def maximumTruss(graph: RDD[Edge], context:SparkContext, outputPath:String, stringk:String): Unit ={

    val outputFile = outputPath + "/maximalTruss/truss"

    val k = stringk.toInt

    val degreedGraph = Truss.addDegreesToGraph(graph)
    val result = recursiveTruss(k, 0 ,2, List(degreedGraph), context)
//    result.foreach{t =>
//      t.foreach(e => print(e + ", "))
//      println("")}

   // result.foreach(t => t.saveAsTextFile(outputFile))

    result.zipWithIndex.foreach { case (t, i) =>

      val truss = t.collect()
      val vertexSet = Clique.getVertexSet(truss)
      val printWriter = new PrintWriter(new File(outputFile + i.toString))
      vertexSet.foreach(a => {
        printWriter.println(a)
      })
      printWriter.close()
    }
  }


  //returns list of subgraphes
  def recursiveTruss(k:Int, maxK:Int, minK:Int, graphs: List[RDD[Edge]], context:SparkContext): List[RDD[Edge]] = {

    println(k)
    if (maxK == k || minK == k){
      println("return k  " + k)
      graphs
    } else {

      val foundTrusses = graphs.flatMap{ graph =>
        val trusses = Truss.calculateTrusses(k-2, graph.filter(e => e.vertex1.degree >= k-1 && e.vertex2.degree >= k-1))
        val groupedEdgesPerTruss = trusses.groupByKey()

        val x = groupedEdgesPerTruss.collect()
          .map(e => context.parallelize(e._2.toSeq)).toList
        x
      }

      if (foundTrusses.isEmpty){
        val newK = minK + (k-minK)/2
        recursiveTruss(newK, k, minK, graphs, context)
      } else {
        if (maxK == 0){
          val newK = 2*k
          recursiveTruss(newK, maxK, k, foundTrusses, context)
        } else {
          val newK = k + (maxK-k)/2
          recursiveTruss(newK, maxK, k, foundTrusses, context)
        }
      }
    }
  }

}
