package de.hpi.dbda.graph_mining

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

import scala.concurrent.duration.durationToPair

/**
 * Created by rice on 12.06.15.
 */

case class Vertex(id: Int, degree:Int)
case class Edge(vertex1:Vertex,vertex2:Vertex)
case class Triangle(edges:List[Edge])

object Truss {

  def convertGraph(rawGraph:DataSet[String], seperator:String): DataSet[Edge] = {
    val convertedGraph = rawGraph.map(line => {
        val splitted = line.split(seperator)
        val f = new Vertex(splitted(0).toInt, 1)
        val s = new Vertex(splitted(1).toInt, 1)
        createEdge(f, s)
      }
    )
    convertedGraph
  }

  def createEdge(vert1:Vertex, vert2:Vertex): Edge = {
    if (vert1.degree < vert2.degree) new Edge(vert1, vert2)
    else
    if (vert1.degree == vert2.degree && vert1.id < vert2.id)
      new Edge(vert1, vert2)
    else new Edge(vert2, vert1)
  }

  def getTriangles(graph:DataSet[Edge]): DataSet[Triangle] ={

    //TODO filter after degree
    val filteredGraph = graph

    val triads = filteredGraph.join(filteredGraph).where("vertex1").equalTo("vertex1")
      .filter(new FilterFunction[(Edge, Edge)] {
      override def filter(t: (Edge, Edge)): Boolean = {
        t._1.vertex2.id < t._2.vertex2.id
      }
     })
      .map(t => (getOuterTriangleVertices(t._1, t._2), List(t._1, t._2))
      ).name("Calculate Triads")


    val allEdges = graph.map(edge => (edge, List(edge)))

    val triangles = triads.join(allEdges).where(0).equalTo(0) {
      (trianglePart1, trianglePart2) => Triangle(trianglePart2._2 ::: trianglePart1._2)
    }

    println(triangles.count)
    triangles.print()

    triangles

  }

  def getOuterTriangleVertices(edge1:Edge, edge2:Edge): Edge ={
    createEdge(edge1.vertex2, edge2.vertex2)
  }

  def calculateTruss(k:Int, firstGraph:DataSet[Edge]): Unit ={

    var graphOldCount:Long = 0
    var graph = firstGraph
    var graphCount = graph.count


    //TODO implement with flink loop improvements
    
    while(graphCount != graphOldCount) {
      graphOldCount = graphCount

      val triangles = getTriangles(graph)

      val singleEdges = triangles.flatMap(triangle => triangle.edges).map((_, 1))

      val triangleCountPerEdge = singleEdges.groupBy(0).reduce{
        (edgeCount1, edgeCount2) => (edgeCount1._1, edgeCount1._2 + edgeCount2._2)}

      graph = triangleCountPerEdge
        .filter(count => count._2 >= k)
        .map(edgeCount => edgeCount._1)

      graphCount = graph.count

    }

  }

}
