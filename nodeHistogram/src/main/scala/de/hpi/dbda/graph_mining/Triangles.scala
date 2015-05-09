package de.hpi.dbda.graph_mining

import de.hpi.fgis.tpch.NodeHistogram
import org.apache.spark.rdd.RDD

/**
 * Created by rice on 05.05.15.
 */
object Triangles {

  def getTriangles(graph:RDD[(Int, Int)]): Unit ={

    val edgeCombinations = graph.map(edge => {
      if (edge._2 > edge._1) (edge._1, edge)
      else (edge._2, edge)
    })

 /*   edgeCombinations
      .join(edgeCombinations)
      .map (
      ((vertex:int, ((v1_edge1:int, v2_edge2:int), (v1_edge2:int, v2_edge2:int)))) => {
      (v1_edge1,v2_edge2)
    }
    )*/ //punkt aus edge ohne vertex, punkt aus edge1 ohne vertex sortiert
  }

}
