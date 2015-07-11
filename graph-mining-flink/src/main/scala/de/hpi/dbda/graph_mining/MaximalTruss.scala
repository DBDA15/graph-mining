package de.hpi.dbda.graph_mining

import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
import org.apache.flink.api.scala._

/**
 * Created by rice on 17.06.15.
 */
object MaximalTruss {

  def maxTruss(graph: DataSet[Edge], stringk:String): DataSet[Edge] ={

    var k = stringk.toInt
    var maxK = 0
    var minK = 2

    var result = graph

    while (k != maxK && k != minK){

      print ("############################ k is " + k +" #################################")
//      val filteredGraph = graphs.filter(e => e.vertex1.degree >= k-2 && e.vertex2.degree >= k-2)
////      filteredGraph.print()

      val trusses = Truss.calculateTruss(k, graph)

      val result:DataSet[Edge] = trusses.map{truss =>
//          truss._2.truss = truss._1
          truss._2
        }

      val trussCount = trusses.count()

      if ( trussCount == 0){
        val newK = minK + (k-minK)/2
        maxK = k
        k = newK
      } else {
        if (maxK == 0){
          val newK = 2*k
          minK = k
          k = newK
        } else {
          val newK = k + (maxK -k)/2
          minK = k
          k = newK
        }

        //graphs = foundTrusses
      }

    }

    print ("############################ final k is " + k +" #################################")
    result

  }

  def maxTrussWithWriting(graph: DataSet[Edge], stringk:String, executionEnvironment: ExecutionEnvironment): DataSet[Edge] ={

    var k = stringk.toInt
    var maxK = 0
    var minK = 2

    var result = graph

    while (k != maxK && k != minK){

      print ("############################ k is " + k +" #################################")
      //      val filteredGraph = graphs.filter(e => e.vertex1.degree >= k-2 && e.vertex2.degree >= k-2)
      ////      filteredGraph.print()

      val trusses = Truss.calculateTruss(k, graph)

      val trussCount = trusses.count()

      val result:DataSet[Edge] = trusses.map{truss =>
        //          truss._2.truss = truss._1
        truss._2
      }


      if ( trussCount == 0){
        val newK = minK + (k-minK)/2
        maxK = k
        k = newK
      } else {
        if (maxK == 0){
          val newK = 2*k
          minK = k
          k = newK
        } else {
          val newK = k + (maxK -k)/2
          minK = k
          k = newK
        }

        //graphs = foundTrusses
      }

    }

    print ("############################ final k is " + k +" #################################")
    result

  }




}
