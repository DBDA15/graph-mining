package de.hpi.dbda.graph_mining

import de.hpi.fgis.tpch.NodeHistogram
import org.apache.spark.rdd.RDD

/**
 * Created by rice on 05.05.15.
 */
object Triangles {

  case class Edge(vertex1:Int, vertex2:Int, origin:(Int, Int)){
    def apply(vert1:Int, vert2:Int): Edge = {
      if (vert1 > vert2) new Edge(vert1, vert2, (vert1, vert2))
      else new Edge(vert2, vert1, (vert1, vert2))
    }
  }

  def getTriangles(graph:RDD[Edge]): Unit ={
    // sort edges
    val edgeCombinations = graph.map(edge => {
      (edge.vertex1, edge)
    })

    //(vertex: int, ((v1_edge1, v2_edge1: int), (v1_edge2: int, v2_edge2: int))))
    val missedEdges = edgeCombinations
      .join(edgeCombinations)
      .map( combination => {
        (getOuterTriangleVertices(combination), List(combination._2._1, combination._2._2))
       })

    val allEdges = graph.map(edge => (edge, List(edge)))
    val triangles = missedEdges
      .union(allEdges)  //include all single edges
      .reduceByKey((edges1, edges2) => edges1 ::: edges2) //build triangles
      .filter(edgeList => edgeList._2.length > 2) //remove all mappings which have only 2 edges

    triangles.saveAsTextFile("output/")
  }


  def getOuterTriangleVertices(combination:(Int, (Triangles.Edge, Triangles.Edge))): Triangles.Edge ={
    val innerVertex = combination._1
    val edge1 = combination._2._1
    val edge2 = combination._2._2

    val outerVertex1:Int = if (edge1.vertex1 != innerVertex) edge1.vertex1 else edge1.vertex2
    val outerVertex2:Int = if (edge2.vertex1 != innerVertex) edge2.vertex1 else edge2.vertex2
//key is probably not corect
    Edge.apply(outerVertex1, outerVertex2, (outerVertex1, outerVertex2))
  }

}
