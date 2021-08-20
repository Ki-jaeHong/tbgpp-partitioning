echo "[INFO] run turbograph"


# TODO change run-itbgpp-experiments.sh
# TODO change run-overall-performance.sh -> change input db type
# TODO change run-dynamic-exp.sh


nodeIdStart=001
nodeIdEnd=005

# DropPageCache before execution
for i in $(seq -f "%03g" $nodeIdStart $nodeIdEnd); do
	ssh citeb${i} -n "/mnt/data2/jhko/tools/DropPageCache"
	echo "machine ${i} : PageCache Dropped (status $?)"
done

# Execute
cd /home/tslee/jhko/Turbographpp_sigmod2
. /home/tslee/jhko/Turbograph_jhko_partitioning/set_environment.sh	# source environment

./run-itbgpp-experiments.sh debug overall ins 0 "Debug" 001-005

echo "[INFO] Done."