#!/bin/bash

program="cat test"


declare -A configurations

configurations=(
	["A"]="cat test"
	["B"]="tail test -n 2"
)

for config in "${!configurations[@]}"; do
	rm "output_$config.log" -f
	echo "====================================" >> time.log
	date "+%Y-%m-%d %H:%M:%S" >> time.log
	echo "${configurations["$config"]} - output in output_$config.log" >> time.log
	(time ${configurations["$config"]} >> "output_$config.log") &>> time.log
	echo "====================================" >> time.log
	echo "" >> time.log
done
