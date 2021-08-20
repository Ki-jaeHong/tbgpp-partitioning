function exitIfExecutionFailed() {
	if [ $1 -ne 0 ]; then
   		echo "Fail execution... Now exiting"
   		exit 1
	fi
}

# [INFO] run spark program to change vids
# TODO call spark !!!!!

echo "[INFO] proceed step 1, 2 of partition-copart"

echo "[INFO] run bbp over machines"
# TODO set machine environment in the file
# source /home/tslee/jhko/Turbograph_jhko_partitioning/set_environment.sh
# ./run-bbp.sh
# exitIfExecutionFailed $?

echo "[INFO] proceed from step 3 of partition-copart"
