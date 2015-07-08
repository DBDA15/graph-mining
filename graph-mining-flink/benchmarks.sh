#!/bin/bash

declare -A configurations

configurations=(
	["TrussK5C20"]="/opt/flink/flink-0.9.0/bin/flink run --class de.hpi.dbda.graph_mining.GraphMiningFlink --parallelism 20 -m tenemhead2:6123 target/graph-mining-flink-1.0-SNAPSHOT.jar truss hdfs://tenemhead2/data/graph-mining/biDirectionalWiki hdfs://tenemhead2/home/ricarda.schueler/output/ \"\t\" 20"
	["TrussK5C10"]="/opt/flink/flink-0.9.0/bin/flink run --class de.hpi.dbda.graph_mining.GraphMiningFlink --parallelism 10 -m tenemhead2:6123 target/graph-mining-flink-1.0-SNAPSHOT.jar truss hdfs://tenemhead2/data/graph-mining/biDirectionalWiki hdfs://tenemhead2/home/ricarda.schueler/output/ \"\t\" 20"
	["TrussK5C5"]="/opt/flink/flink-0.9.0/bin/flink run --class de.hpi.dbda.graph_mining.GraphMiningFlink --parallelism 5 -m tenemhead2:6123 target/graph-mining-flink-1.0-SNAPSHOT.jar truss hdfs://tenemhead2/data/graph-mining/biDirectionalWiki hdfs://tenemhead2/home/ricarda.schueler/output/ \"\t\" 20"
	["TrussK5C2"]="/opt/flink/flink-0.9.0/bin/flink run --class de.hpi.dbda.graph_mining.GraphMiningFlink --parallelism 2 -m tenemhead2:6123 target/graph-mining-flink-1.0-SNAPSHOT.jar truss hdfs://tenemhead2/data/graph-mining/biDirectionalWiki hdfs://tenemhead2/home/ricarda.schueler/output/ \"\t\" 20"
	["TrussK5C1"]="/opt/flink/flink-0.9.0/bin/flink run --class de.hpi.dbda.graph_mining.GraphMiningFlink --parallelism 1 -m tenemhead2:6123 target/graph-mining-flink-1.0-SNAPSHOT.jar truss hdfs://tenemhead2/data/graph-mining/biDirectionalWiki hdfs://tenemhead2/home/ricarda.schueler/output/ \"\t\" 20"
	["TrussK10C20"]="/opt/flink/flink-0.9.0/bin/flink run --class de.hpi.dbda.graph_mining.GraphMiningFlink --parallelism 20 -m tenemhead2:6123 target/graph-mining-flink-1.0-SNAPSHOT.jar truss hdfs://tenemhead2/data/graph-mining/biDirectionalWiki hdfs://tenemhead2/home/ricarda.schueler/output/ \"\t\" 10"
	["TrussK3C20"]="/opt/flink/flink-0.9.0/bin/flink run --class de.hpi.dbda.graph_mining.GraphMiningFlink --parallelism 20 -m tenemhead2:6123 target/graph-mining-flink-1.0-SNAPSHOT.jar truss hdfs://tenemhead2/data/graph-mining/biDirectionalWiki hdfs://tenemhead2/home/ricarda.schueler/output/ \"\t\" 8"
	["TrussK40C20"]="/opt/flink/flink-0.9.0/bin/flink run --class de.hpi.dbda.graph_mining.GraphMiningFlink --parallelism 20 -m tenemhead2:6123 target/graph-mining-flink-1.0-SNAPSHOT.jar truss hdfs://tenemhead2/data/graph-mining/biDirectionalWiki hdfs://tenemhead2/home/ricarda.schueler/output/ \"\t\" 40"
)

for config in "${!configurations[@]}"; do
	rm "output_$config.log" -f
	echo "====================================" >> time.log
	date "+%Y-%m-%d %H:%M:%S" >> time.log
	echo "${configurations["$config"]} - output in output_$config.log" >> time.log
	echo $config
	(time ${configurations["$config"]} >> "output_$config.log") &>> time.log
	echo "====================================" >> time.log
	echo "" >> time.log
done
