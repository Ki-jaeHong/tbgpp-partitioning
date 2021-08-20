#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd -P )"

# parse arguments
if [ $# -lt 1 ] ; then
    echo "Wrong num of arguments (at least 1 required)"
	exit 1
fi

JSON_FILE_PREFIX=$1
JSON_FILE_POSTFIX=".json"
SEARCH_KEY=$2
if [ -z "$SEARCH_KEY" ]; then
	SEARCH_KEY="ExecutionTimePerSuperstep"
fi
#SEARCH_KEY="ExecutionTimePerSuperstep"
#SEARCH_KEY="E_active_Step1"


MACHINE_ID_START=001
MACHINE_ID_END=005

echo "SEARCH_KEY=${SEARCH_KEY}"
for i in $(seq -f "%03g" $MACHINE_ID_START $MACHINE_ID_END); do
	printf "[${i}] | " 
	machineid=$(expr ${i} - 1)
	jqcommand="def roundit : . "\*" 1000.0|round/1000.0;
			  .${SEARCH_KEY} | map( roundit ) "
	ssh citeb${i} -n "cat $JSON_FILE_PREFIX.$machineid$JSON_FILE_POSTFIX" |	\
		$SCRIPT_DIR/bin/jq -c ' '"${jqcommand}"' '
		
done