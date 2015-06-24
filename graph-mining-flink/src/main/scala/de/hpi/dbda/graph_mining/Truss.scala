package de.hpi.dbda.graph_mining

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector



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

  def addDegrees(graph:DataSet[Edge]): DataSet[Edge] ={
    val degrees = graph
      .map{e => e.vertex1}
      .union(graph.map(e => e.vertex2))   //TODO improve?
      .groupBy(0)
      .sum(1)
      .map(v => (v.id, v) )

    val degreedGraph = graph
      .map(e => (e.vertex1.id, e.vertex2.id, e))
      .join(degrees).where(0).equalTo(0)
      .map(j => (j._1._2, new Edge(j._2._2, j._1._3.vertex2)))
      .join(degrees).where(0).equalTo(0)
      .map(j => createEdge(j._1._2.vertex1, j._2._2))

    degreedGraph
  }

  def getTriangles(graph:DataSet[Edge], k:Int): DataSet[Triangle] ={

    //TODO filter after degree
    val filteredGraph = graph.filter(e => {e.vertex1.degree > k && e.vertex2.degree > k})

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

  def calculateTruss(k:Int, firstGraph:DataSet[Edge]): DataSet[(Int, Edge)]={

    var graphOldCount:Long = 0
    var graph = firstGraph
    var graphCount = graph.count


    //TODO implement with flink loop improvements

//    val graph = firstGraph.iterateDelta()
    while(graphCount != graphOldCount) {
      graphOldCount = graphCount

      val triangles = getTriangles(graph, k)

      val singleEdges = triangles.flatMap(triangle => triangle.edges).map((_, 1))

      val triangleCountPerEdge = singleEdges.groupBy(0).reduce{
        (edgeCount1, edgeCount2) => (edgeCount1._1, edgeCount1._2 + edgeCount2._2)}

      graph = triangleCountPerEdge
        .filter(count => count._2 >= k)
        .map(edgeCount => edgeCount._1)

      graphCount = graph.count

    }

    val verticesWithComponents = findRemainingComponents(graph)

    val edgeInComponent = verticesWithComponents.join(graph).where(0).equalTo("vertex1") {
      (zone, edge) => (zone._2 , edge)
    }

    edgeInComponent
  }

  def findRemainingComponents(graph:DataSet[Edge]): DataSet[(Vertex, Int)] ={

    val vertices = graph.flatMap(edge =>
          List((edge.vertex1,  edge.vertex1.id), ( edge.vertex2, edge.vertex2.id)))
      .groupBy(0).reduce({(zone1, zone2) => zone1})

    //TODO max iterations
    val verticesWithComponents = vertices.iterateDelta(vertices, 100, Array(0)) {
      (s, ws) =>

        // apply the step logic: join with the edges
        val allNeighbors = ws.join(graph).where(0).equalTo("vertex1") { (vertex, edge) =>
          (edge.vertex2, vertex._2)
        }

        // select the minimum neighbor
        val minNeighbors = allNeighbors.groupBy(0).min(1)

        // update if the component of the candidate is smaller
        val updatedComponents = minNeighbors.join(s).where(0).equalTo(0) {
          (newVertex, oldVertex, out: Collector[(Vertex, Int)]) =>
            if (newVertex._2 < oldVertex._2) out.collect(newVertex)
        }

        // delta and new workset are identical
        println("jdsjlkdsja")
        (updatedComponents, updatedComponents)
    }

    verticesWithComponents

  }


  def findRemainingComponents2(graph:DataSet[Edge]): DataSet[(Vertex, Int)] ={

    val vertices = graph.flatMap(edge =>
      List((edge.vertex1,  edge.vertex1.id), ( edge.vertex2, edge.vertex2.id)))
      .groupBy(0).reduce({(zone1, zone2) => zone1})

    //TODO max iterations
    val verticesWithComponents = vertices.iterateDelta(vertices, 100, Array(0)) {
      (s, ws) =>

        // apply the step logic: join with the edges
        val allNeighbors = ws.join(graph).where(0).equalTo("vertex1")
        {(vertex, edge) => (edge.vertex2, vertex._2)}
          .union(ws.join(graph).where(0).equalTo("vertex2")
        {(vertex, edge) => (edge.vertex1, vertex._2)})


        // select the minimum neighbor
        val minNeighbors = allNeighbors.groupBy(0).min(1)

        // update if the component of the candidate is smaller
        val updatedComponents = minNeighbors.join(s).where(0).equalTo(0) {
          (newVertex, oldVertex, out: Collector[(Vertex, Int)]) =>
            if (newVertex._2 < oldVertex._2) out.collect(newVertex)
        }

        // delta and new workset are identical
        (updatedComponents, updatedComponents)
    }

    verticesWithComponents

  }


}
