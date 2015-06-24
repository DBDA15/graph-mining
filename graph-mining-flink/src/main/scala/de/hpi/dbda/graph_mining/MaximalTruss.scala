package de.hpi.dbda.graph_mining

import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}

/**
 * Created by rice on 17.06.15.
 */
object MaximalTruss {

  def maxTruss(graph: DataSet[Edge], stringk:String, env:ExecutionEnvironment): Unit ={

    val k = stringk.toInt
    var maxK = 0
    var minK = 2

    var graphs = List(graph)

    while (k != maxK && k != minK){
      graphs.flatMap{ graph =>
        val trusses = Truss.calculateTruss(k, graph) //TODO filter .filter(e => e.vertex1.degree >= k-1 && e.vertex2.degree >= k-1)
//        val groupedEdgesPerTruss = trusses.groupBy(0).reduceGroup(env.fromElements{(iterator) => iterator.map(e => e._2).toList}))



        List(graph)
      }



    }



  }


}
