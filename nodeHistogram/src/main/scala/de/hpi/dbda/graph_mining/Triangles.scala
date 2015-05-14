package de.hpi.dbda.graph_mining

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by ricarda schueler on 05.05.15.
 */
object Triangles {

  case class Vertex(id: Int, var degree:Int)

  case class Edge(vertex1:Vertex, vertex2:Vertex, original:Boolean){
  }

  case class Triangle(edges:List[Edge]){

    //check for circle
    def isCircular(): Boolean={
        var e1_v1 = this.edges.head.vertex1.id
        var e1_v2 = this.edges.head.vertex2.id
        var e2_v1 = this.edges(1).vertex1.id
        var e2_v2 = this.edges(1).vertex2.id
        var e3_v1 = this.edges(2).vertex1.id
        var e3_v2 = this.edges(2).vertex2.id

        if (!this.edges.head.original) {
          e1_v1 = this.edges.head.vertex2.id
          e1_v2 = this.edges.head.vertex1.id
        }

        if (!this.edges(1).original) {
          e2_v1 = this.edges(1).vertex2.id
          e2_v2 = this.edges(1).vertex1.id
        }

        if (!this.edges(2).original) {
          e3_v1 = this.edges(2).vertex2.id
          e3_v2 = this.edges(2).vertex1.id
        }

        xor(e1_v1 == e2_v2, e1_v1 == e3_v2) && xor(e2_v1 == e1_v2, e2_v1 == e3_v2)
    }

    def xor(x:Boolean, y:Boolean) = (x && !y) || (y && !x)
  }

  def convertGraph(rawGraph: RDD[String], seperator:String): RDD[Edge] ={
    rawGraph.map(
      line => {
        val splitted = line.split(seperator)
        val f = new Vertex(splitted(0).toInt, 1)
        val s = new Vertex(splitted(1).toInt, 1)
        createEdge(f, s)
      })
  }

  def getTrianglesAndSave(rawGraph:RDD[String], outputDir:String, seperator:String) = {
    val triangleOut = outputDir + "/all"
    val circularTriangleOut = outputDir + "/circular"
    val nonCircularTriangleOut = outputDir + "/nonCircular"


    val graph = convertGraph(rawGraph, seperator)
    // sort edges

    val uniqueTriangles = getTriangles(graph)
    uniqueTriangles.saveAsTextFile(triangleOut)

    val circularTriangles = uniqueTriangles.filter(triangle => triangle.isCircular())

    circularTriangles.saveAsTextFile(circularTriangleOut)

    /* //check for non circle
    val noncircularTriangles = uniqueTriangles.filter(edgeList =>{
     !(xor(edgeList(0)._1 == edgeList(1)._2, edgeList(0)._1 == edgeList(2)._2) && xor(edgeList(1)._1 == edgeList(0)._2, edgeList(1)._1 == edgeList(2)._2))
    })

    noncircularTriangles.saveAsTextFile(nonCircularTriangleOut)*/
  }

  def getTriangles(graph:RDD[Edge]): RDD[Triangle] ={
    val edgeCombinations = graph.map(edge => {
      (edge.vertex1, edge)
    })

    //(vertex: int, ((v1_edge1, v2_edge1: int), (v1_edge2: int, v2_edge2: int))))
    val missedEdges = edgeCombinations
      .join(edgeCombinations)
      .map( combination => {
      (getOuterTriangleVertices(combination), List(combination._2._1, combination._2._2))
    })

    val missedEdgesPersist = missedEdges.persist(StorageLevel.MEMORY_AND_DISK)

    val allEdges = graph.map(edge => ((edge.vertex1, edge.vertex2), List(edge)))
    val triangles = missedEdgesPersist
      .join(allEdges)  //join with single edges
      .map(triangle => Triangle(triangle._2._1 ::: triangle._2._2))

    //eliminate all duplicates
    triangles
      .filter(triangle => triangle.edges.head.vertex2.id > triangle.edges(1).vertex2.id)
  }

  def calculateTruss(k:Int, rawGraph:RDD[String], outputDir:String, seperator:String): Unit ={
    val triangleOut = outputDir + "/all"
    val circularTriangleOut = outputDir + "/circular"
    val nonCircularTriangleOut = outputDir + "/nonCircular"


    var graph:RDD[Triangles.Edge] = convertGraph(rawGraph, seperator)
    var graphOldCount:Long = 0

    while(graph.count() != graphOldCount) {

      val triangles = getTriangles(graph)

      val singleEdges = triangles.flatMap(triangle => triangle.edges).map(edge => (edge, 1))

      val triangleCountPerEdge = singleEdges.reduceByKey((count1, count2) => count1 + count2)
      graphOldCount = graph.count()
      graph = triangleCountPerEdge.filter(count => count._2 > k).map(edgeCount => edgeCount._1)
    }

    findRemainingGraphComponents(graph)
  }

  def findRemainingGraphComponents(graph:RDD[Triangles.Edge]): Unit ={
  //TODO
    
  }

  def getOuterTriangleVertices(combination:(Vertex, (Triangles.Edge, Triangles.Edge))): (Vertex, Vertex) ={
    val innerVertex = combination._1
    val edge1 = combination._2._1
    val edge2 = combination._2._2

    val outerVertex1:Vertex = if (edge1.vertex1.id != innerVertex.id) edge1.vertex1 else edge1.vertex2
    val outerVertex2:Vertex = if (edge2.vertex1.id != innerVertex.id) edge2.vertex1 else edge2.vertex2

    if (outerVertex1.id > outerVertex2.id)
      (outerVertex1, outerVertex2)
    else (outerVertex2, outerVertex1)
  }

  def createEdge(vert1:Vertex, vert2:Vertex): Edge = {
    if (vert1.id > vert2.id) new Edge(vert1, vert2, true)
    else new Edge(vert2, vert1, false)
  }

  def calculateDegree(graph:RDD[Edge]): Unit ={
    val degree = graph
      .flatMap(edge => List((edge.vertex1.id, 1), (edge.vertex2.id, 1)))
      .reduceByKey((vertex1, vertex2) => {
        vertex1 + vertex2
      })

//    val vertexGraph = graph.flatMap(edge => List((edge.vertex1.id, (edge.vertex1, edge)), (edge.vertex2.id, (edge.vertex2, edge))))
//    vertexGraph
//      .join(degree)
//      .map(degreeCombination =>{
//        degreeCombination._2._1.

    })

  }

}
