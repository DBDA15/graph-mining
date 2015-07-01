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
case class Edge(vertex1:Vertex,vertex2:Vertex, var truss:Int = 1, var triangleCount:Int = -1)
case class Triangle(edges:List[Edge])


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
      .join(degrees).where(0).equalTo(0) {
      (e, v) => (e._2, new Edge(v._2, e._3.vertex2, truss))
    }
      .join(degrees).where(0).equalTo(0) {
      (e, v) => createEdge(e._2.vertex1, v._2, truss)
    }.name("join Degrees")

    degreedGraph
  }

  def getTriangles(graph:DataSet[Edge]): DataSet[Triangle] ={

    val triads = graph.join(graph).where("vertex1").equalTo("vertex1")
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
    }.name("calculate triangles")

    println(triangles.count)
    triangles.print()

    triangles
  }

  def getOuterTriangleVertices(edge1:Edge, edge2:Edge): Edge ={
    createEdge(edge1.vertex2, edge2.vertex2, edge1.truss)
  }



  def calculateTruss(k:Int, firstGraph:DataSet[Edge]): DataSet[(Int, Edge)]={

//    val filteredGraph = graph.filter(e => {e.vertex1.degree > k-2 && e.vertex2.degree > k-2})
//
//    var triangles = getTriangles(filteredGraph)
//
//    triangles.print()
//
//    val updatedGraph = filteredGraph.iterateDelta(filteredGraph, 10000, Array(0)){
//      (s, ws) =>
//
//        val filteredTriangles = triangles.filter{triangle => (triangle.edges(0).triangleCount >= k-2 || triangle.edges(0).triangleCount == -1)  && (triangle.edges(1).triangleCount >= k-2 || triangle.edges(1).triangleCount == -1) && (triangle.edges(2).triangleCount >= k-2 || triangle.edges(2).triangleCount == -1 )}.name("filter removed triangles")
//
//        val singleEdges = filteredTriangles.flatMap(triangle => triangle.edges).map((_, 1))
//
//        val triangleCountPerEdge = singleEdges.groupBy(0).reduce{
//          (edgeCount1, edgeCount2) => (edgeCount1._1, edgeCount1._2 + edgeCount2._2)}.name("count triangles per edge")
//
//        val edgesWithTriangleCount = triangleCountPerEdge.map{edgeInt =>
//          edgeInt._1.triangleCount  = edgeInt._2
//          edgeInt._1
//        }
//
////        val newSolutionSet = edgesWithTriangleCount.filter(edge => edge.triangleCount >= k-2)
//
//        val removableEdges = edgesWithTriangleCount.filter(edge => edge.triangleCount < k-2)
//
//        triangles = filteredTriangles.join(edgesWithTriangleCount).where({triangle => (triangle.edges(0).vertex1, triangle.edges(0).vertex2)}).equalTo("vertex1", "vertex2"){
//          (triangle, edge) =>
//            triangle.edges(0).triangleCount = edge.triangleCount
//            triangle
//        }.join(edgesWithTriangleCount).where({triangle => (triangle.edges(1).vertex1, triangle.edges(1).vertex2)}).equalTo("vertex1", "vertex2"){
//          (triangle, edge) =>
//            triangle.edges(1).triangleCount = edge.triangleCount
//            triangle
//        }.join(edgesWithTriangleCount).where({triangle => (triangle.edges(2).vertex1, triangle.edges(2).vertex2)}).equalTo("vertex1", "vertex2"){
//          (triangle, edge) =>
//            triangle.edges(2).triangleCount = edge.triangleCount
//            triangle
//        }
//
//        //val newWs = triangles.filter{triangle => !(triangle.edges(0).triangleCount >= k-2 && triangle.edges(1).triangleCount >= k-2 && triangle.edges(2).triangleCount >= k-2)}.name("filter removed triangles end delta")
//
//        (edgesWithTriangleCount, removableEdges)
//    }
//
//    updatedGraph.print()
//
//    val graph1 = updatedGraph.filter(edge => edge.triangleCount >= k-2)


    var graph = firstGraph
    var graphCount = graph.count()
    var graphOldCount:Long = 0

    var triangles = getTriangles(graph)

    while(graphCount != graphOldCount) {
      graphOldCount = graphCount


      val singleEdges = triangles.flatMap(triangle => triangle.edges).map((_, 1))

      val triangleCountPerEdge = singleEdges.groupBy(0).reduce{
        (edgeCount1, edgeCount2) => (edgeCount1._1, edgeCount1._2 + edgeCount2._2)}.name("count triangles per edge")

      graph = triangleCountPerEdge.map{edgeInt =>
        edgeInt._1.triangleCount  = edgeInt._2
        edgeInt._1
      }.filter(edge => edge.triangleCount >= k-2)


      triangles = triangles.join(graph).where({triangle => (triangle.edges(0).vertex1, triangle.edges(0).vertex2)}).equalTo("vertex1", "vertex2"){
        (triangle, edge) =>
          triangle.edges(0).triangleCount = edge.triangleCount
          triangle
      }

      triangles = triangles.join(graph).where({triangle => (triangle.edges(1).vertex1, triangle.edges(1).vertex2)}).equalTo("vertex1", "vertex2"){
        (triangle, edge) =>
          triangle.edges(1).triangleCount = edge.triangleCount
          triangle
      }

      triangles = triangles.join(graph).where({triangle => (triangle.edges(2).vertex1, triangle.edges(2).vertex2)}).equalTo("vertex1", "vertex2"){
        (triangle, edge) =>
          triangle.edges(2).triangleCount = edge.triangleCount
          triangle
      }

      graphCount = graph.count

    }

    val verticesWithComponents = findRemainingComponents(graph)

    val edgeInComponent = verticesWithComponents.join(graph).where(0).equalTo("vertex1") {
      (zone, edge) => (zone._2 , edge)
    }.name("edge in zone")

    edgeInComponent
  }

  def findRemainingComponents(graph:DataSet[Edge]): DataSet[(Vertex, Int)] ={

    val vertices = graph.flatMap(edge =>
          List((edge.vertex1,  edge.vertex1.id), ( edge.vertex2, edge.vertex2.id))).distinct

    val graphMap1 = graph.flatMap(edge => List((edge.vertex1, edge), (edge.vertex2, edge)))

    //TODO max iterations
    val verticesWithComponents = vertices.iterateDelta(vertices, 100, Array(0)) {
     (s, ws) =>

        // apply the step logic: join with the edges
        val edgeZones = ws.join(graphMap1).where(0).equalTo(0) { (vertex, vertexEdge) =>
          (vertexEdge._2, vertex._2)
        }.name("findRemaingGraphComponent: join ws with graph")

        // select the minimum neighbor
        val minEdgeZones = edgeZones.groupBy(0).min(1).name("findRemaingGraphComponent: min neighbor")

        val allNeighbors = minEdgeZones.flatMap(edge => List((edge._1.vertex1,  edge._2), ( edge._1.vertex2, edge._2)))

        val minNeighbors = allNeighbors.groupBy(0).min(1)

        // update if the component of the candidate is smaller
        val updatedComponents = minNeighbors.join(s).where(0).equalTo(0) {
          (newVertex, oldVertex, out: Collector[(Vertex, Int)]) =>
            if (newVertex._2 < oldVertex._2) out.collect(newVertex)
        }.name("findRemaingGraphComponent: get updated components")

        // delta and new workset are identical
        (updatedComponents, updatedComponents)

//    updatedComponents.print()
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
