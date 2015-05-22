package de.hpi.dbda.graph_mining

import org.apache.spark.rdd.RDD

/**
 * Created by rice on 22.05.15.
 */
object Clique {

  def calculateCliques(graph: RDD[Truss.Edge]): Unit ={
    val graphArray = graph.collect()
    val vertexSet = getVertexSet(graphArray)

    val cliques = bronKerbosch(Set(), vertexSet, Set(), graphArray, Array())

    cliques.foreach({c => c.foreach(v => print(v + ", "))
      println(" ")})
  }

  def getVertexSet(graph: Array[Truss.Edge]): Set[Int] ={
    var vertexSet : Set[Int] = Set()

    graph.foreach(e => vertexSet += (e.vertex1.id, e.vertex2.id))

    vertexSet
  }

  def getNeighbors(vertex:Int, graph: Array[Truss.Edge]): Set[Int] = {
    var neighborSet: Set[Int] = Set()

    graph.foreach(e => {
      if(e.vertex1.id == vertex)
        neighborSet += e.vertex2.id
      else if(e.vertex2.id == vertex)
        neighborSet += e.vertex1.id
    })

    neighborSet
  }

  def bronKerbosch(r: Set[Int], oldP: Set[Int], oldX: Set[Int], graph: Array[Truss.Edge], oldCliques: Array[Array[Int]]): Array[Array[Int]] = {
    var x = oldX
    var p = oldP
    var cliques = oldCliques
    if (p.isEmpty && x.isEmpty) {
      cliques = cliques :+ r.toArray
      return cliques
    }
    p.foreach(v => {
      val neighbors = getNeighbors(v, graph)
      cliques = cliques ++ bronKerbosch(r + v, p.intersect(neighbors), x.intersect(neighbors), graph, Array())
      p = p - v
      x = x + v
    })
    cliques
  }

}
