package de.hpi.dbda.graph_mining

import org.apache.spark.rdd.RDD

/**
 * Created by rice on 05.05.15.
 */
object Triangles {

  case class Edge(vertex1:Int, vertex2:Int, origin:(Int, Int)){
  }

  def convertGraph(rawGraph: RDD[String]): RDD[Edge] ={
    rawGraph.map(line => createEdge(line.split("\t")(0).toInt, line.split("\t")(1).toInt))
  }

  def getTriangles(rawGraph:RDD[String], outputDir:String): Unit ={

    val graph = convertGraph(rawGraph)
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

    val allEdges = graph.map(edge => ((edge.vertex1, edge.vertex2), List(edge)))
    val triangles = missedEdges
      .join(allEdges)  //join with single edges
      .map(triangle => triangle._2._1 ::: triangle._2._2)

    triangles.saveAsTextFile(outputDir)
  }


  def getOuterTriangleVertices(combination:(Int, (Triangles.Edge, Triangles.Edge))): (Int, Int) ={
    val innerVertex = combination._1
    val edge1 = combination._2._1
    val edge2 = combination._2._2

    val outerVertex1:Int = if (edge1.vertex1 != innerVertex) edge1.vertex1 else edge1.vertex2
    val outerVertex2:Int = if (edge2.vertex1 != innerVertex) edge2.vertex1 else edge2.vertex2

    if (outerVertex1 > outerVertex2)
      (outerVertex1, outerVertex2)
    else (outerVertex2, outerVertex1)
  }

  def createEdge(vert1:Int, vert2:Int): Edge = {
    if (vert1 > vert2) new Edge(vert1, vert2, (vert1, vert2))
    else new Edge(vert2, vert1, (vert1, vert2))
  }

}
