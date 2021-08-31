echo "[INFO] run turbograph"


# TODO change run-itbgpp-experiments.sh
# TODO change run-overall-performance.sh -> change input db type
# TODO change run-dynamic-exp.sh


nodeIdStart=001
nodeIdEnd=005

# DropPageCache before execution
clush -w citeb[${nodeIdStart}-${nodeIdEnd}] "/mnt/data2/jhko/tools/DropPageCache"

# Execute
cd /home/tslee/jhko/Turbographpp_sigmod2
. /home/tslee/jhko/Turbograph_jhko_partitioning/set_environment.sh	# source environment

# file / debug / ? / insertion only / version / string prefix / machine
./run-itbgpp-experiments.sh debug overall ins 10 "Debug" 001-005

echo "[INFO] Done."