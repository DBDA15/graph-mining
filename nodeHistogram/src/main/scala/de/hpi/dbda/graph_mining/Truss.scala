package de.hpi.dbda.graph_mining

import org.apache.spark.{HashPartitioner, RangePartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Truss {

  case class Vertex(id: Int, var degree:Int)

  case class Edge(var vertex1:Vertex, var vertex2:Vertex, var original:Boolean){
    def replace(newEdge:Edge): Unit ={
      vertex1 = newEdge.vertex1
      vertex2 = newEdge.vertex2
      original = newEdge.original
    }
  }

  case class Triangle(edges:List[Edge]){

    //check for circle
    def isCircular: Boolean={
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

  def getTrianglesAndSave(rawGraph:RDD[String], outputDir:String, seperator:String): Unit ={
    val triangleOut = outputDir + "/all"
    val circularTriangleOut = outputDir + "/circular"
//    val nonCircularTriangleOut = outputDir + "/nonCircular"


    val graph = addDegreesToGraph(convertGraph(rawGraph, seperator))
    // sort edges

    val uniqueTriangles = getTriangles(graph)
    uniqueTriangles.saveAsTextFile(triangleOut)

//    val circularTriangles = uniqueTriangles.filter(triangle => triangle.isCircular)

//    circularTriangles.saveAsTextFile(circularTriangleOut)

    /* //check for non circle
    val noncircularTriangles = uniqueTriangles.filter(edgeList =>{
     !(xor(edgeList(0)._1 == edgeList(1)._2, edgeList(0)._1 == edgeList(2)._2) && xor(edgeList(1)._1 == edgeList(0)._2, edgeList(1)._1 == edgeList(2)._2))
    })

    noncircularTriangles.saveAsTextFile(nonCircularTriangleOut)*/
  }


  def getTrianglesNoSparkAndSave(rawGraph:RDD[String], outputDir:String, seperator:String): Unit ={
    val triangleOut = outputDir + "/allNoSpark"
    val circularTriangleOut = outputDir + "/circularNoSpark"
    //    val nonCircularTriangleOut = outputDir + "/nonCircular"

    val graph = addDegreesToGraph(convertGraph(rawGraph, seperator))
    // sort edges

    val uniqueTriangles = getTrianglesNoSpark(graph)
    uniqueTriangles.saveAsTextFile(triangleOut)

//    val circularTriangles = uniqueTriangles.filter(triangle => triangle.isCircular)

  //  circularTriangles.saveAsTextFile(circularTriangleOut)

    /* //check for non circle
    val noncircularTriangles = uniqueTriangles.filter(edgeList =>{
     !(xor(edgeList(0)._1 == edgeList(1)._2, edgeList(0)._1 == edgeList(2)._2) && xor(edgeList(1)._1 == edgeList(0)._2, edgeList(1)._1 == edgeList(2)._2))
    })

    noncircularTriangles.saveAsTextFile(nonCircularTriangleOut)*/
  }

  def getTriangles(graph:RDD[Edge]): RDD[Triangle] ={
    val edgeCombinations = graph.filter(edge => edge.vertex1.degree > 1)
        .keyBy(edge => edge.vertex1)

    //(vertex: int, ((v1_edge1, v2_edge1: int), (v1_edge2: int, v2_edge2: int))))
    val missedEdges = edgeCombinations
      .join(edgeCombinations)
      .map( combination => {
      (getOuterTriangleVertices(combination), List(combination._2._1, combination._2._2))
    })

    missedEdges.persist(StorageLevel.MEMORY_AND_DISK)

    val allEdges = graph.map(edge => (edge, List(edge)))
    val triangles = missedEdges
      .join(allEdges)  //join with single edges
      .map(triangle => Triangle(triangle._2._1 ::: triangle._2._2))

    //eliminate all duplicates
//    val filteredTriangles = triangles
//      .filter(triangle => triangle.edges.head.vertex2.id > triangle.edges(1).vertex2.id)

  //  missedEdges.unpersist()

    triangles
  }


  def getTrianglesNoSpark(graph:RDD[Edge]): RDD[Triangle] ={
    val edgeCombinations = graph.filter(edge => edge.vertex1.degree > 1)
      .map{edge => (edge.vertex1, edge)}

    //(vertex: int, ((v1_edge1, v2_edge1: int), (v1_edge2: int, v2_edge2: int))))

    val triads = edgeCombinations
      .groupByKey()
      .flatMap{vedge  =>
          vedge._2.flatMap{ p => vedge._2.filter(p1 => p1 != p).map(p1 => (vedge._1, (p1, p))) }}
      .map( combination => {
      (getOuterTriangleVertices(combination), List(combination._2._1, combination._2._2))
    })


    val allEdges = graph.map(edge => (edge, List(edge)))
    val triadsAndSingleEdges = triads.union(allEdges)

   //reduce2
    val triangles = triadsAndSingleEdges
      .groupByKey()
      .flatMap{p =>
        val edge = p._2.find(e => e.length == 1)
        edge match{
          case Some(s) => {
            val edgePairs = p._2.filterNot(e => e.length == 1)
             edgePairs.map(ep => Triangle(s ::: ep))
        }
          case None => List()
        }
    }

//    //eliminate all duplicates
//    val filteredTriangles = triangles
//      .filter(triangle => triangle.edges.head.vertex2.id > triangle.edges(1).vertex2.id)

    //  missedEdges.unpersist()

    triangles
  }


  def calcTrussesAndSave(k:Int, rawGraph:RDD[String], outputDir:String, seperator:String): Unit ={
    val trussOut = outputDir + "/truss"

    val graph:RDD[Truss.Edge] = convertGraph(rawGraph, seperator)
    val trusses = calculateTrusses(k, graph)
    trusses.saveAsTextFile(trussOut)
  }


  def calculateTrusses(k:Int, firstGraph:RDD[Truss.Edge]): RDD[(Int, Truss.Edge)] ={

    var graphOldCount:Long = 0

    var graph = firstGraph

    var graphCount = graph.count()

    while(graphCount != graphOldCount) {
      graphOldCount = graphCount

      val triangles = getTriangles(graph)

      val singleEdges = triangles.flatMap(triangle => triangle.edges).map(edge => (edge, 1))

      val triangleCountPerEdge = singleEdges.reduceByKey((count1, count2) => count1 + count2)

      graph = triangleCountPerEdge.filter(count => count._2 >= k).map(edgeCount => edgeCount._1)

      graph.persist(StorageLevel.MEMORY_AND_DISK)
      graphCount = graph.count()
    }

    val components = findRemainingGraphComponents(graph)

    //convert into zone => edge mappings
    val vertexInZComponent = components.map(zoneVertex => (zoneVertex._2, zoneVertex._1))
    vertexInZComponent.persist(StorageLevel.MEMORY_AND_DISK)
    val edgePerVertex = graph.map(edge => (edge.vertex1, edge))
    val edgeInComponent = edgePerVertex
      .join(vertexInZComponent)
      .map(e => (e._2._2, e._2._1))
    vertexInZComponent.unpersist()

    edgeInComponent
  }

  def findRemainingGraphComponents(graph:RDD[Truss.Edge]): RDD[(Int, Vertex)] ={

    var interZoneEdgeCounter = 1

    //build zone file
    var zones = graph.flatMap(edge => List((edge.vertex1, (edge.vertex1,  edge.vertex1.id)), (edge.vertex2, ( edge.vertex2, edge.vertex2.id))))
    .reduceByKey((zone1, zone2) => zone1)

    zones.persist(StorageLevel.MEMORY_AND_DISK)

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

      edgeZonesCombined.persist(StorageLevel.MEMORY_AND_DISK)

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
      zones.persist(StorageLevel.MEMORY_AND_DISK)
    }

    zones.map(vertexZone => (vertexZone._2._2, vertexZone._1))
  }

  def getOuterTriangleVertices(combination:(Vertex, (Truss.Edge, Truss.Edge))): Edge ={
    val innerVertex = combination._1
    val edge1 = combination._2._1
    val edge2 = combination._2._2

    val outerVertex1:Vertex = if (edge1.vertex1.id != innerVertex.id) edge1.vertex1 else edge1.vertex2
    val outerVertex2:Vertex = if (edge2.vertex1.id != innerVertex.id) edge2.vertex1 else edge2.vertex2

    createEdge(outerVertex1, outerVertex2)
  }

  def createEdge(vert1:Vertex, vert2:Vertex): Edge = {
    if (vert1.degree > vert2.degree) new Edge(vert1, vert2, true)
    else
      if (vert1.degree == vert2.degree && vert1.id < vert2.id)
        new Edge(vert1, vert2, true)
      else new Edge(vert2, vert1, false)
  }

  def addDegreesToGraph(graph:RDD[Edge]): RDD[Edge] ={
    val degree = calculateDegrees(graph)
    .persist(StorageLevel.MEMORY_AND_DISK)

    graph
      .keyBy(e => e.vertex1.id)
      .join(degree)
      .map(e => (e._2._1.vertex2.id, new Edge(new Vertex(e._1, e._2._2), e._2._1.vertex2, e._2._1.original)))
      .join(degree)
      .map(e => {if (e._2._1.original)
          createEdge(e._2._1.vertex1, new Vertex(e._1, e._2._2))
        else
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
