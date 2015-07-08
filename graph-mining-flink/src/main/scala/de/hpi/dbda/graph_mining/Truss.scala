package de.hpi.dbda.graph_mining

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint



import scala.concurrent.duration.durationToPair

/**
 * Created by rice on 12.06.15.
 */

case class Vertex(id: Int, degree:Int)
case class Edge(vertex1:Vertex,vertex2:Vertex, var truss:Int = 1, var triangleCount:Int = -1)
case class Triangle(edge1:Edge, edge2:Edge, edge3:Edge)


object Truss {

  def convertGraph(rawGraph:DataSet[String], seperator:String): DataSet[Edge] = {
    val convertedGraph = rawGraph.map(line => {
        val splitted = line.split(seperator)
        val f = new Vertex(splitted(0).toInt, 1)
        val s = new Vertex(splitted(1).toInt, 1)
        createEdge(f, s, 1)
      }
    ).name("convert Graph")
    convertedGraph
  }

  def createEdge(vert1:Vertex, vert2:Vertex, truss:Int): Edge = {
    if (vert1.degree < vert2.degree) new Edge(vert1, vert2, truss)
    else
    if (vert1.degree == vert2.degree && vert1.id < vert2.id)
      new Edge(vert1, vert2, truss)
    else new Edge(vert2, vert1, truss)
  }

  def addDegrees(graph:DataSet[Edge]): DataSet[Edge] ={
    val truss = 1

    val degrees = graph
      .flatMap{e => List(e.vertex1, e.vertex2)}
      .groupBy(0)
      .sum(1).name("calculate degrees")
      .map(v => (v.id, v) )

    val degreedGraph = graph
      .map(e => (e.vertex1.id, e.vertex2.id, e))
      .join(degrees, JoinHint.BROADCAST_HASH_SECOND).where(0).equalTo(0) { //Repartition
      (e, v) => (e._2, new Edge(v._2, e._3.vertex2, truss))
    }
      .join(degrees, JoinHint.BROADCAST_HASH_SECOND).where(0).equalTo(0) {
      (e, v) => createEdge(e._2.vertex1, v._2, truss)
    }.name("join Degrees")

    degreedGraph
  }

  def getTriangles(graph:DataSet[Edge], k:Int): DataSet[Triangle] ={

    val triads = graph.join(graph).where("vertex1").equalTo("vertex1").name("create triads")
      .filter(new FilterFunction[(Edge, Edge)] {
      override def filter(t: (Edge, Edge)): Boolean = {
        t._1.vertex2.id < t._2.vertex2.id
      }
     }).name("filter duplicated triads")
      .map(t => (getOuterTriangleVertices(t._1, t._2), t._1, t._2)
      ).name("map Triads")

    val allEdges = graph.map(edge => (edge, edge)).name("triangleCalculation: map singlge edges")

    //TODO Parameterization of #nodes?
    val joinStrategy = if (k >= 8) JoinHint.BROADCAST_HASH_SECOND else JoinHint.REPARTITION_HASH_SECOND

    val triangles = triads.join(allEdges, joinStrategy).where(0).equalTo(0) {
      (triadPart, edgePart) => Triangle(edgePart._2, triadPart._2, triadPart._3)
    }.name("calculate triangles")

    triangles
  }

  def getOuterTriangleVertices(edge1:Edge, edge2:Edge): Edge ={
    createEdge(edge1.vertex2, edge2.vertex2, edge1.truss)
  }



  def calculateTruss(k:Int, firstGraph:DataSet[Edge]): DataSet[(Int, Edge)]={

    var graph = firstGraph

    val filteredGraph = graph.filter(e => {e.vertex1.degree > k-2 && e.vertex2.degree > k-2}).name("filter too small nodes")

    val triangles = getTriangles(filteredGraph, k)

    val filteredTriangles = triangles.iterateWithTermination(Int.MaxValue)({triangles =>
      val singleEdges = triangles.flatMap(triangle => List(triangle.edge1, triangle.edge2, triangle.edge3)).map((_, 1)).name("prepare triangle count per edge")

      val triangleCountPerEdge = singleEdges.groupBy(0).reduce{
        (edgeCount1, edgeCount2) => (edgeCount1._1, edgeCount1._2 + edgeCount2._2)}.name("count triangles per edge")

      graph = triangleCountPerEdge.map{edgeInt =>
        edgeInt._1.triangleCount  = edgeInt._2
        edgeInt._1
      }.name("give edge triangle count")
        .filter(edge => edge.triangleCount >= k-2).name("filter edge with too small triangleCount")

      val removableEdges = graph.filter(edge => edge.triangleCount < k-2)

      var joinedtriangles = triangles.join(graph).where({triangle => (triangle.edge1.vertex1, triangle.edge1.vertex2)}).equalTo("vertex1", "vertex2"){
        (triangle, edge) =>
          triangle.edge1.triangleCount = edge.triangleCount
          triangle
      }.name("first triangle edge join")

      joinedtriangles = joinedtriangles.join(graph).where({triangle => (triangle.edge2.vertex1, triangle.edge2.vertex2)}).equalTo("vertex1", "vertex2"){
        (triangle, edge) =>
          triangle.edge2.triangleCount = edge.triangleCount
          triangle
      }.name("second triangle edge join")

      joinedtriangles = joinedtriangles.join(graph).where({triangle => (triangle.edge3.vertex1, triangle.edge3.vertex2)}).equalTo("vertex1", "vertex2"){
        (triangle, edge) =>
          triangle.edge3.triangleCount = edge.triangleCount
          triangle
      }.name("third triangle edge join")


      (joinedtriangles, removableEdges)
    })

    graph = filteredTriangles.flatMap(triangle => List(triangle.edge1, triangle.edge2, triangle.edge3)).distinct

    val verticesWithComponents = findRemainingComponents(graph)

    val edgeInComponent = verticesWithComponents.join(graph, JoinHint.REPARTITION_HASH_FIRST).where(0).equalTo("vertex1") {
      (zone, edge) => (zone._2 , edge)
    }.name("edge in zone")

    edgeInComponent
  }

  def findRemainingComponents(graph:DataSet[Edge]): DataSet[(Vertex, Int)] ={

    val vertices = graph.flatMap(edge =>
          List((edge.vertex1,  edge.vertex1.id), ( edge.vertex2, edge.vertex2.id))).distinct.name("vertices map")

    val graphMap1 = graph.flatMap(edge => List((edge.vertex1, edge), (edge.vertex2, edge))).name("name edge to vertices")

    //TODO max iterations
    val verticesWithComponents = vertices.iterateDelta(vertices, Int.MaxValue, Array(0)) {
     (s, ws) =>

        // apply the step logic: join with the edges
        val edgeZones = ws.join(graphMap1, JoinHint.BROADCAST_HASH_FIRST).where(0).equalTo(0) { (vertex, vertexEdge) =>
          (vertexEdge._2, vertex._2)
        }.name("findRemaingGraphComponent: join ws with graph")

        // select the minimum neighbor
        val minEdgeZones = edgeZones.groupBy(0).min(1).name("findRemaingGraphComponent: min neighbor")

        val allNeighbors = minEdgeZones.flatMap(edge => List((edge._1.vertex1,  edge._2), ( edge._1.vertex2, edge._2)))

        val minNeighbors = allNeighbors.groupBy(0).min(1)

        // update if the component of the candidate is smaller
        val updatedComponents = minNeighbors.join(s, JoinHint.BROADCAST_HASH_FIRST).where(0).equalTo(0) {
          (newVertex, oldVertex, out: Collector[(Vertex, Int)]) =>
            if (newVertex._2 < oldVertex._2) out.collect(newVertex)
        }.name("findRemaingGraphComponent: get updated components")

        // delta and new workset are identical
        (updatedComponents, updatedComponents)

//    updatedComponents.print()
    }

    verticesWithComponents
  }

}
