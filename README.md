GRAPH MINING
-----

The Graph Mining application uses Apache Spark or Apache Flink to find either the maximum Truss or all specific k-Trusses in a graph.

The input file is expected to consist of one line per edge in the graph.
Each line must begin with an integer followed by a separator and another integer.
Both these integers must equate to node IDs in the graph.

Building the Projects
-----
Both Projects require Java 7 and Maven.

To build a project, execute "Maven Install" in the main project directory.

We recommend using IntelliJ when working on either project.


Program Modes
-----

triangle:

Calculates all unique triangles in the graph.


truss:

Calculates all k-trusses in the graph, where k is defined by the user.
A k-truss is a maximal subgraph in which every edge is part of at least k-2 triangles.


maxtruss:

Calculates all trusses with the maximum k value in the graph.
The initial k value for the algorithm is defined by the user.

Apache Spark Verison
-----

Launch Parameters:
* 0: mode [string] -- The portion of the program to be exectued. Valid inputs: triangle, truss, maxtruss
* 1: input path [string] -- path to the file containing the graph data, formatted as described above
* 2: output path [string] -- path where the output should be written to
* 3: separator [string] -- the separator used between an edge's nodes, as described above
* 4: partitioning [int] -- number of partitions the data should be split into, should be a multiple of the number of workers available
* 5: [optional depending on mode] (starting) k value [int] -- k value for the truss mode or initial k value for the maxtruss mode

Example Launch:
```
spark-submit --conf -Dspark.master=local spark.app.name="graph-mining" --class de.hpi.dbda.graph_mining_spark.GraphMiningSpark target/graph_mining_spark-0.0.1-SNAPSHOT.jar truss ../trussMini.txt ../output/ " " 10 4
```


Apache Flink Verison
-----

Launch Parameters:
* 0: mode [string] -- The portion of the program to be exectued. Valid inputs: triangle, truss, maxtruss
* 1: input path [string] -- path to the file containing the graph data, formatted as described above
* 2: output path [string] -- path where the output should be written to
* 3: separator [string] -- the separator used between an edge's nodes, as described above
* 4: [optional depending on mode] (starting) k value [int] -- k value for the truss mode or initial k value for the maxtruss mode

Example Launch:
```
$flink run --parallelism 10 --class de.hpi.dbda.graph_mining.GraphMiningFlink target/graph-mining-flink-1.0-SNAPSHOT.jar truss ../trussMini.txt ../output/ " " 4
```


Output
-----

truss/maxtruss:

The output has one line for each edge that is contained in a truss and looks as follows:
```
1 2 3 4 5
```
* 1: Truss ID
* 2: ID of the first vertex
* 3: Degree of the first vertex
* 4: ID of the second vertex
* 5: Degree of the second vertex


triangle:

The output has one line for each triangle and looks as follows:
```
1 2 3 4 5 6 7 8 9 10  11  12
```
Numbers 1-4 represent an edge, as do numbers 5-8, and numbers 9-12.
Each of these sets represents and edge of the triangle.
* 1/5/9: ID of the first vertex
* 2/6/10: Degree of the first vertex
* 3/7/11: ID of the second vertex
* 4/8/12: Degree of the second vertex
