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
spark-submit --conf -Dspark.master=local spark.app.name="graph-mining" --class de.hpi.dbda.graph_mining.GraphMiningSpark target/graph_mining_spark-0.0.1-SNAPSHOT.jar truss ../trussMini.txt ../output/ " " 10 4
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
