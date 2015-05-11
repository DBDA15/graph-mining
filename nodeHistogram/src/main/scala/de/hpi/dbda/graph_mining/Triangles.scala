package de.hpi.dbda.graph_mining

import de.hpi.fgis.tpch.NodeHistogram
import org.apache.spark.rdd.RDD

/**
 * Created by ricarda schueler on 05.05.15.
 */
object Triangles {

  case class Edge(vertex1:Int, vertex2:Int, original:Boolean){
  }

  def convertGraph(rawGraph: RDD[String], seperator:String): RDD[Edge] ={
    rawGraph.map(
      line => {
        println(line)
        val splitted = line.split(seperator)
        val f = splitted(0).toInt
        val s = splitted(1).toInt
        createEdge(f, s)
      })
  }

  def getTriangles(rawGraph:RDD[String], outputDir:String, seperator:String): Unit ={
    val triangleOut = outputDir + "/all"
    val circularTriangleOut = outputDir + "/circular"
    val nonCircularTriangleOut = outputDir + "/nonCircular"


    val graph = convertGraph(rawGraph, seperator)
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

    //eliminate all duplicates
    val uniqueTriangles = triangles
      .filter(triangle => triangle(0).vertex2 > triangle(1).vertex2)
      .map(edgeList => {
          List(edgeList(0)) ::: List(edgeList(1)) ::: List(edgeList(2))
     })
    uniqueTriangles.saveAsTextFile(triangleOut)

    //check for circle
    val circularTriangles = uniqueTriangles.filter(edgeList =>{
      var e1_v1 = edgeList(0).vertex1
      var e1_v2 = edgeList(0).vertex2
      var e2_v1 = edgeList(1).vertex1
      var e2_v2 = edgeList(1).vertex2
      var e3_v1 = edgeList(2).vertex1
      var e3_v2 = edgeList(2).vertex2

      if (!edgeList(0).original) {
        e1_v1 = edgeList(0).vertex2
        e1_v2 = edgeList(0).vertex1
      }

      if (!edgeList(1).original) {
        e2_v1 = edgeList(1).vertex2
        e2_v2 = edgeList(1).vertex1
      }

      if (!edgeList(2).original) {
        e3_v1 = edgeList(2).vertex2
        e3_v2 = edgeList(2).vertex1
      }

      xor(e1_v1 == e2_v2, e1_v1 == e3_v2) && xor(e2_v1 == e1_v2, e2_v1 == e3_v2)
    })

    circularTriangles.saveAsTextFile(circularTriangleOut)

    /* //check for non circle
    val noncircularTriangles = uniqueTriangles.filter(edgeList =>{
     !(xor(edgeList(0)._1 == edgeList(1)._2, edgeList(0)._1 == edgeList(2)._2) && xor(edgeList(1)._1 == edgeList(0)._2, edgeList(1)._1 == edgeList(2)._2))
    })

    noncircularTriangles.saveAsTextFile(nonCircularTriangleOut)*/
  }

  def xor(x:Boolean, y:Boolean) = (x && !y) || (y && !x)

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
    if (vert1 > vert2) new Edge(vert1, vert2, true)
    else new Edge(vert2, vert1, false)
  }

}
