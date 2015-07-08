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

    var graphs = graph

    while (k != maxK && k != minK){

      print ("############################ k is " + k +" #################################")
      val filteredGraph = graphs.filter(e => e.vertex1.degree >= k-2 && e.vertex2.degree >= k-2)
//      filteredGraph.print()

      val trusses = Truss.calculateTruss(k, filteredGraph)

      val foundTrusses:DataSet[Edge] = trusses.map{truss =>
          truss._2.truss = truss._1
          truss._2
        }

      val trussCount = foundTrusses.count

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

        graphs = foundTrusses
      }

    }

    graphs

  }


  def maxTruss1(graph: DataSet[Edge], stringk:String, executionEnvironment: ExecutionEnvironment): DataSet[Edge] ={

    var k = stringk.toInt
    var maxK = 0
    var minK = 2

    val graphs = graph

    val possibleKs = executionEnvironment.fromCollection(2 until 10000)

    graphs.iterateWithTermination(1000)({ currentGraph =>

      print ("############################ k is " + k +" #################################")
      val filteredGraph = currentGraph.filter(e => e.vertex1.degree >= k-2 && e.vertex2.degree >= k-2)
      //      filteredGraph.print()

      val trusses = Truss.calculateTruss(k, filteredGraph)

      val foundTrusses:DataSet[Edge] = trusses.map{truss =>
        truss._2.truss = truss._1
        truss._2
      }


      val trussCount = foundTrusses.count()

      if ( trussCount == 0){
        val newPossibleKs = possibleKs.filter(possibleK => possibleK < k)

        val newK = minK + (k-minK)/2
        maxK = k
        k = newK
        (currentGraph, newPossibleKs)

      } else {
        val newPossibleKs = possibleKs.filter(possibleK => possibleK > k)
        if (maxK == 0){
          val newK = 2*k
          minK = k
          k = newK
        } else {
          val newK = k + (maxK -k)/2
          minK = k
          k = newK
        }
        (foundTrusses, newPossibleKs)
      }
    })


  }





}
