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
* 0: mode [string] -- The portion of the program to be exectued. Valid inputs: truss, maxtruss
* 1: input path [string] -- path to the file containing the graph data, formatted as described above
* 2: output path [string] -- path where the output should be written to
* 3: separator [string] -- the separator used between an edge's nodes, as described above
* 4: partitioning [int] -- number of partitions the data should be split into, should be a multiple of the number of workers available
* 5: (starting) k value [int] -- k value for the truss portion or initial k value for the maxtruss portion

Example Launch:
```
spark-submit --conf spark.app.name="graph-mining" --class de.hpi.dbda.graph_mining.GraphMiningSpark --master spark://*URL* target/graph-mining-spark-0.0.1-SNAPSHOT.jar maxtruss ../trussMini.txt ../output/ " " 20 28
```


Apache Flink Verison
-----

text

Launch Parameters:
* text
* text

Example Launch:
```
text
```
