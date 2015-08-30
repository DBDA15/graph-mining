package de.hpi.dbda.graph_mining_spark

import org.apache.spark.{RangePartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Truss {

  case class Vertex(id: Int, var degree:Int)

  case class Edge(vertex1:Vertex, vertex2:Vertex, var truss:Int = 1, var triangleCount:Int = 0)

  case class Triangle(edges:List[Edge]){
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

  def getTrianglesAndSave(rawGraph:RDD[String], outputDir:String, seperator:String): Unit ={
    val graph = addDegreesToGraph(convertGraph(rawGraph, seperator))
    val uniqueTriangles = getTriangles(graph)
    val count = uniqueTriangles.count()
    println(count)
  }

  def getTrianglesNoSparkAndSave(rawGraph:RDD[String], outputDir:String, seperator:String): Unit ={
    val graph = addDegreesToGraph(convertGraph(rawGraph, seperator))

    val uniqueTriangles = getTrianglesNoSpark(graph)
    val count = uniqueTriangles.count()
    println(count)
  }

  def getTriangles(graph:RDD[Edge]): RDD[Triangle] ={
    val allEdges1 = graph.map(edge => (edge, List(edge)))
    val allEdges = allEdges1.persist(StorageLevel.MEMORY_AND_DISK)

    val edgeCombinations = graph.filter(edge => edge.vertex1.degree > 1)
        .keyBy(edge => edge.vertex1)

    val missedEdges = edgeCombinations
      .join(edgeCombinations)

    // triad = two edges that share a node = tringle candidate
    val triads = missedEdges
      .filter(e => e._2._1.vertex2.id < e._2._2.vertex2.id)
      .map( combination => {
      (getOuterTriangleVertices(combination), List(combination._2._1, combination._2._2))
    })

    val triangles = triads
      .join(allEdges)  //join with single edges
      .map(triangle => Triangle(triangle._2._1 ::: triangle._2._2))

    triangles
  }


  def getTrianglesNoSpark(graph:RDD[Edge]): RDD[Triangle] ={
    val allEdges1 = graph.map(edge => (edge, List(edge)))
    val allEdges = allEdges1.persist(StorageLevel.MEMORY_AND_DISK)


    val edgeCombinations = graph.filter(edge => edge.vertex1.degree > 1)
      .map{edge => (edge.vertex1, edge)}

    val triads = edgeCombinations
      .groupByKey()
      .flatMap{vedge  =>
          vedge._2.flatMap{ p => vedge._2.filter(p1 => p1 != p).map(p1 => (vedge._1, (p1, p))) }}
      .filter(e => e._2._1.vertex2.id < e._2._2.vertex2.id)
      .map( combination => {
      (getOuterTriangleVertices(combination), List(combination._2._1, combination._2._2))
    })

    val triadsAndSingleEdges = triads.union(allEdges)

    val triangles = triadsAndSingleEdges
      .groupByKey()
      .flatMap{p =>
        val edge = p._2.find(e => e.length == 1)
        edge match{
          case Some(s) =>
            val edgePairs = p._2.filterNot(e => e.length == 1)
            edgePairs.map(ep => Triangle(s ::: ep))
          case None => List()
        }
    }

    triangles
  }


  def calcTrussesAndSave(k:Int, rawGraph:RDD[String], outputDir:String, seperator:String, partitioning:Int): Unit ={
    val trussOut = outputDir + "/truss"

    val graph:RDD[Truss.Edge] = addDegreesToGraph(convertGraph(rawGraph, seperator))
    val trusses = calculateTrusses(k, graph, partitioning)._1
    trusses.saveAsTextFile(trussOut)
  }


  def calculateTrusses(k:Int, firstGraph:RDD[Truss.Edge], partitioning:Int): (RDD[(Int, Truss.Edge)], Long, Long, Long) ={


    var graphOldCount:Long = 0
    var graph = firstGraph
    var graphCount = graph.count()

    var triangles = getTrianglesNoSpark(graph.map(e => createEdge(e.vertex1, e.vertex2))).repartition(partitioning)
    val getTrianglesTime = java.lang.System.currentTimeMillis()

    while(graphCount != graphOldCount) {
      graphOldCount = graphCount

      val singleEdges = triangles.flatMap(triangle => triangle.edges).map(edge => (edge, 1))

      graph = singleEdges.reduceByKey((count1, count2) => count1 + count2)
        .map(t => {
        t._1.triangleCount = t._2
        t._1}
        )
        .filter(e => e.triangleCount >= k)

      val keyedGraph = graph.map(e => ((e.vertex1.id, e.vertex2.id), e)).persist(StorageLevel.MEMORY_AND_DISK)

      triangles =
        triangles.map(t => ((t.edges.head.vertex1.id, t.edges(0).vertex2.id), t))
          .join(keyedGraph)
          .map(t => ((t._2._1.edges(1).vertex1.id, t._2._1.edges(1).vertex2.id), new Triangle(List(t._2._2, t._2._1.edges(1), t._2._1.edges(2)))))
          .join(keyedGraph)
          .map(t => ((t._2._1.edges(2).vertex1.id, t._2._1.edges(2).vertex2.id), new Triangle(List(t._2._1.edges(0), t._2._2, t._2._1.edges(2)))))
          .join(keyedGraph)
          .map(t => new Triangle(List(t._2._1.edges(0), t._2._1.edges(1), t._2._2)))

      graphCount = graph.count()
    }

    val filterTriangleDegreesTime = java.lang.System.currentTimeMillis()

    val components = findRemainingGraphComponents(graph)
    val remainingGraphComponentsTime = java.lang.System.currentTimeMillis()

    //convert into zone => edge mappings
    val vertexInZComponent = components.map(zoneVertex => (zoneVertex._2, zoneVertex._1))
          .persist(StorageLevel.MEMORY_AND_DISK)
    val edgePerVertex = graph.map(edge => (edge.vertex1, edge))
    val edgeInComponent = edgePerVertex
      .join(vertexInZComponent)
      .map(e => (e._2._2, e._2._1))
    vertexInZComponent.unpersist()

    (edgeInComponent, getTrianglesTime, filterTriangleDegreesTime, remainingGraphComponentsTime)
  }

  def findRemainingGraphComponents(graph:RDD[Truss.Edge]): RDD[(Int, Vertex)] ={

    var interZoneEdgeCounter = 1

    //build zone file
    var zones = graph.flatMap(edge => List((edge.vertex1, (edge.vertex1,  edge.vertex1.id)), (edge.vertex2, ( edge.vertex2, edge.vertex2.id))))
    .reduceByKey((zone1, zone2) => zone1)
    .persist(StorageLevel.MEMORY_AND_DISK)

    val graphMap1 = graph.flatMap(edge => List((edge.vertex1, edge), (edge.vertex2, edge)))

    while (interZoneEdgeCounter != 0){
      //reduce1
      //each edge with zone (ine edge can appear multiple times if appears in multiple zones
      val edgeZones = graphMap1
        .join(zones)
        .map(edgeCombination => (edgeCombination._1, (List(edgeCombination._2._1), edgeCombination._2._2)))
        .reduceByKey((edgeCombination1, edgeCombination2) => (edgeCombination1._1 ::: edgeCombination2._1, edgeCombination1._2))
        .flatMap(comb => comb._2._1.map(edge => (edge , comb._2._2._2))) //edge => zone

      //reduce2
      // each edge has list with its zones
      val edgeZonesCombined =
        edgeZones
        .map(edgeZone => (edgeZone._1, List(edgeZone._2)))
        .reduceByKey((zone1, zone2) => {
          val list = zone1 ::: zone2
          list.distinct
        })
        .persist(StorageLevel.MEMORY_AND_DISK)

      //calculate interZoneCount
      if (edgeZonesCombined.isEmpty()) interZoneEdgeCounter = 0
      else {
        interZoneEdgeCounter = edgeZonesCombined
          .map(edgeZone => if (edgeZone._2.length > 1) 1 else 0)
          .reduce((zoneCount1, zoneCount2) => zoneCount1 + zoneCount2)
      }

      //calculate zone merging: result z => smallestZone
      val interZoneEdges = edgeZonesCombined.flatMap
      { edgeZone =>
        val sortedZones = edgeZone._2.sorted
        val smallestZone = sortedZones.head
        sortedZones.map(zone => (zone, smallestZone))
      }

      edgeZonesCombined.unpersist()

      //reduce3
      val bestZonePerZone = interZoneEdges.reduceByKey((zone1, zone2) => if (zone1< zone2) zone1 else zone2)
      val zoneVertex = zones.map(vertexZone => (vertexZone._2._2, vertexZone._1))
      val verticesWithNewZones = zoneVertex.join(bestZonePerZone)

      zones = verticesWithNewZones.map(v => (v._2._1, v._2))
        .persist(StorageLevel.MEMORY_AND_DISK)
    }

    zones.map(vertexZone => (vertexZone._2._2, vertexZone._1))
  }

  def getOuterTriangleVertices(combination:(Vertex, (Truss.Edge, Truss.Edge))): Edge ={
    val edge1 = combination._2._1
    val edge2 = combination._2._2

    createEdge(edge1.vertex2, edge2.vertex2)
  }

  def createEdge(vert1:Vertex, vert2:Vertex): Edge = {
    val truss = -1
    if (vert1.degree < vert2.degree) new Edge(vert1, vert2, truss)
    else
      if (vert1.degree == vert2.degree && vert1.id < vert2.id)
        new Edge(vert1, vert2, truss)
      else new Edge(vert2, vert1, truss)
  }

  def addDegreesToGraph(graph:RDD[Edge]): RDD[Edge] ={
    val degree = calculateDegrees(graph)
    .persist(StorageLevel.MEMORY_AND_DISK)

    graph
      .keyBy(e => e.vertex1.id)
      .join(degree)
      .map(e => (e._2._1.vertex2.id, new Edge(new Vertex(e._1, e._2._2), e._2._1.vertex2, -1)))
      .join(degree)
      .map(e => {
          createEdge(new Vertex(e._1, e._2._2), e._2._1.vertex1)
        })
  }

  def calculateDegrees(graph:RDD[Edge]): RDD[(Int, Int)] ={
   graph
      .flatMap(edge => List((edge.vertex1.id, 1), (edge.vertex2.id, 1)))
      .reduceByKey((vertex1, vertex2) => {
      vertex1 + vertex2
    })
  }
}
