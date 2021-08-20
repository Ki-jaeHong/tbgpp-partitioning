function exitIfExecutionFailed() {
	if [ $1 -ne 0 ]; then
   		echo "Fail execution... Now exiting"
   		exit 1
	fi
}

function checkProceed () {
	if [[ $1 -ge $2 && $1 -le $3 ]]; then
		return 1
	else
		return 0
	fi
}

echo "[INFO] run spark program and perform partitioning"
# local mode setting

# export SBT_OPTS="-Xmx100G -XX:+UseG1GC "
# cd city-copartition/
# sbt "run 5 \
# 	/mnt/sdd1/jhko/ldbc-datagen/SF1000/P2P.txt \
# 	/mnt/sdd1/jhko/ldbc-datagen/SF1000/P2C.txt \
# 	/mnt/sdd1/jhko/ldbc-datagen/SF1000/Place2Place.txt	\
# 	/mnt/sdd1/jhko/ldbc-datagen/SF1000/Place2Place.txt	\
# 	/home/tslee/jhko/tbgpp-partitioning/tmp/copart-sf1000-continent-equi-split \
# 	"
# exitIfExecutionFailed $?
# /home/tslee/jhko/tools/jhkoAlert 'partitioning-done'
# exit

### COPART
TBGPP_PREFIX="tbgpp_"
BIN_SUBDIR=""
#### SF100 ####

#LOCAL_FILES_PREFIX="copart-sf100"
#DB_FOLDER_PREFIX="ldbc-snb-copart"
# LOG_V="19"
# LOG_E="24"

# LOCAL_FILES_PREFIX="copart-sf100-country-equi"
# DB_FOLDER_PREFIX="ldbc-snb-copart-country-equi"
# LOG_V="19"
# LOG_E="24"

# LOCAL_FILES_PREFIX="copart-sf100-country-hash"
# DB_FOLDER_PREFIX="ldbc-snb-copart-country-hash"
# LOG_V="20"
# LOG_E="24"

# LOCAL_FILES_PREFIX="copart-sf100-continent-equi"
# DB_FOLDER_PREFIX="ldbc-snb-copart-continent-equi"
# LOG_V="20"
# LOG_E="24"

# LOCAL_FILES_PREFIX="copart-sf100-continent-hash"
# DB_FOLDER_PREFIX="ldbc-snb-copart-continent-hash"
# LOG_V="20"
# LOG_E="24"

#### SF300 ####

# LOCAL_FILES_PREFIX="copart-sf300-country-equi"
# DB_FOLDER_PREFIX="ldbc-snb-copart-sf300-country-equi"
# LOG_V=""
# LOG_E="26"

# LOCAL_FILES_PREFIX="copart-sf300-continent-equi"
# DB_FOLDER_PREFIX="ldbc-snb-copart-sf300-continent-equi"
# LOG_V=""
# LOG_E="26"

#### SF1000 ####

# LOCAL_FILES_PREFIX="copart-sf1000-country-hash"
# DB_FOLDER_PREFIX="ldbc-snb-copart-sf1000-country-hash"
# LOG_V="22"
# LOG_E="28"

# LOCAL_FILES_PREFIX="copart-sf1000-country-equi"
# DB_FOLDER_PREFIX="ldbc-snb-copart-sf1000-country-equi"
# LOG_V="22"
# LOG_E="28"

LOCAL_FILES_PREFIX="copart-sf1000-continent-equi"
DB_FOLDER_PREFIX="ldbc-snb-copart-sf1000-continent-equi"
LOG_V="23"
LOG_E="28"

# #### BBP ####
# TBGPP_PREFIX=""
# BIN_SUBDIR="OutEdges_bin_edgelist"

# ### BBP SF1000 ###
# LOCAL_FILES_PREFIX="bbp-sf1000"
# DB_FOLDER_PREFIX="ldbc-snb-bbp-sf1000"
# LOG_V="22"
# LOG_E="28"

### ETC
HISTOGRAM_BIN_WIDTH=1024

MACHINE_ID_START=001
MACHINE_ID_END=005
NUM_MACHINE=5
NUM_PART_PER_NODE=1

LOCAL_FILES_RAWDATA_FOLDER="${LOCAL_FILES_PREFIX}-split"
LOCAL_FILES_BINDATA_FOLDER="${LOCAL_FILES_PREFIX}-bin-split"
LOCAL_FILES_DATA_PARTRANGE="${LOCAL_FILES_PREFIX}-partrange"

# parse arguments
if [ $# -lt 2 ] ; then
    echo "Wrong num of arguments (2 required)"
	exit 1
fi
STEP_START=$1
STEP_STOP=$2

echo "[TODO-MANUAL] Change partition name ordering to match its order with machine id (part-000XX -> XX)"
echo "[TODO-MANUAL] Prepare PartitionRanges folder"

read -p "Proceed? (Y/y)  " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "proceeding..."
else
	exit 0
fi

step=0
#########
# STEPS #
#########
step=1
checkProceed $step $STEP_START $STEP_STOP
if [[ $? -eq 1 ]]; then 
	echo "[INFO] step 1 - transform each partition to binary files"
	rm -r /home/tslee/jhko/tbgpp-partitioning/tmp/${LOCAL_FILES_BINDATA_FOLDER}
	mkdir /home/tslee/jhko/tbgpp-partitioning/tmp/${LOCAL_FILES_BINDATA_FOLDER}
	for filepath in /home/tslee/jhko/tbgpp-partitioning/tmp/${LOCAL_FILES_RAWDATA_FOLDER}/*; do
		filename=${filepath##*/}
		/mnt/penfolds/tools/edgelisttxt-to-edgelist8B \
			$filepath \
			/home/tslee/jhko/tbgpp-partitioning/tmp/${LOCAL_FILES_BINDATA_FOLDER}/${filename}
	done
	exitIfExecutionFailed $?
fi

step=2
checkProceed $step $STEP_START $STEP_STOP
if [[ $? -eq 1 ]]; then 
	echo "[INFO] step 2 - distribute binary files"
	inputdir=/home/tslee/jhko/tbgpp-partitioning/tmp/${LOCAL_FILES_BINDATA_FOLDER}
	outputdir="/mnt/data2/jhko/partitioning/${DB_FOLDER_PREFIX}_v${LOG_V}_e${LOG_E}_bin_edgelist/${BIN_SUBDIR}/"
	rank=0
	for i in $(seq -f "%03g" $MACHINE_ID_START $MACHINE_ID_END); do
			for j in {0..$NUM_PART_PER_NODE}; do
					clush -w citeb${i} "mkdir -p ${outputdir}"
					scp ${inputdir}/$rank citeb${i}:${outputdir}/${TBGPP_PREFIX}${rank}                # when file is located locally
					rank=$((rank + 1))
			done
	done
	exitIfExecutionFailed $?
fi

# TODO if BBP, run BBP in this step

step=3
checkProceed $step $STEP_START $STEP_STOP
if [[ $? -eq 1 ]]; then 
	echo "[INFO] step 3 - Preconfigure buildDB"
	# TODO only on non-BBP
	echo "copy partitionranges"
	rank=0
	for i in $(seq -f "%03g" $MACHINE_ID_START $MACHINE_ID_END); do
		clush -w citeb${i} "mkdir /mnt/data2/jhko/partitioning/${DB_FOLDER_PREFIX}_v${LOG_V}_e${LOG_E}_bin_edgelist/PartitionRanges"
		scp /home/tslee/jhko/tbgpp-partitioning/tmp/${LOCAL_FILES_DATA_PARTRANGE}/PartitionRanges/${rank}	\
			citeb${i}:/mnt/data2/jhko/partitioning/${DB_FOLDER_PREFIX}_v${LOG_V}_e${LOG_E}_bin_edgelist/PartitionRanges
		rank=$((rank + 1))
	done
	exitIfExecutionFailed $?

	echo "change dir sturucture"
	baseDir="/mnt/data2/jhko/partitioning/${DB_FOLDER_PREFIX}_v${LOG_V}_e${LOG_E}_bin_edgelist/" 
	rank=0
	for i in $(seq -f "%03g" $MACHINE_ID_START $MACHINE_ID_END); do
		clush -w citeb${i} "mkdir -p ${baseDir}/dir${NUM_MACHINE}/OutEdges"
		clush -w citeb${i} "cp -r ${baseDir}/tbgpp_* ${baseDir}/dir${NUM_MACHINE}/OutEdges"
		clush -w citeb${i} "cp -r ${baseDir}/PartitionRanges ${baseDir}/dir${NUM_MACHINE}/"
		rank=$((rank + 1))
	done
	exitIfExecutionFailed $?
fi

step=4
checkProceed $step $STEP_START $STEP_STOP
if [[ $? -eq 1 ]]; then
	checkProceed $step $STEP_START $STEP_STOP
	echo "[INFO] step 4 - build histogram"
	/home/tslee/script/histogram-jhko.sh	\
		/mnt/data2/jhko/partitioning/	\
		${DB_FOLDER_PREFIX}_v${LOG_V}_e${LOG_E}_bin_edgelist	\
		$MACHINE_ID_START	\
		$MACHINE_ID_END	\
		$NUM_MACHINE	\
		$HISTOGRAM_BIN_WIDTH
	exitIfExecutionFailed $?
fi

step=5
checkProceed $step $STEP_START $STEP_STOP
if [[ $? -eq 1 ]]; then
	echo "[INFO] step 5 - buildDB"
	echo "mkdir DB directory"
	for i in $(seq -f "%03g" $MACHINE_ID_START $MACHINE_ID_END); do
		clush -w citeb${i} "rm  -r   /mnt/data2/tbgppdb/${DB_FOLDER_PREFIX}_v${LOG_V}_e${LOG_E}/dir${NUM_MACHINE}_part1_subchunks2/ &> /dev/null"
		clush -w citeb${i} "mkdir -p /mnt/data2/tbgppdb/${DB_FOLDER_PREFIX}_v${LOG_V}_e${LOG_E}/dir${NUM_MACHINE}_part1_subchunks2/"
	done
	echo "buildDB"
	. /home/tslee/jhko/Turbograph_jhko_partitioning/set_environment.sh
	/home/tslee/Turbographpp/run-tbgpp-buildDB.sh \
		/home/tslee/jhko/tbgpp-partitioning/bbp/_machines/${MACHINE_ID_START}-${MACHINE_ID_END} \
		buildDB_release \
		${NUM_MACHINE} \
		/mnt/data2/jhko/partitioning/${DB_FOLDER_PREFIX}_v${LOG_V}_e${LOG_E}_bin_edgelist/dir${NUM_MACHINE}	\
		/mnt/data2/tbgppdb/${DB_FOLDER_PREFIX}_v${LOG_V}_e${LOG_E}/dir${NUM_MACHINE}_part1_subchunks2/ \
		0 1 2
	exitIfExecutionFailed $?
fi

step=6
checkProceed $step $STEP_START $STEP_STOP
if [[ $? -eq 1 ]]; then
	echo "[INFO] step 6 - change DB dir layout"
	baseDir="/mnt/data2/tbgppdb/${DB_FOLDER_PREFIX}_v${LOG_V}_e${LOG_E}/dir${NUM_MACHINE}_part1_subchunks2"
	for i in $(seq -f "%03g" $MACHINE_ID_START $MACHINE_ID_END); do
		ssh citeb${i} -n "mkdir $baseDir/../tmppp;
			mv $baseDir/* $baseDir/../tmppp;
			mkdir -p $baseDir/InEdge/Insert/0;
			mkdir -p $baseDir/OutEdge/Insert/0;
			cp -r $baseDir/../tmppp/* $baseDir/InEdge/Insert/0;
			cp -r $baseDir/../tmppp/* $baseDir/OutEdge/Insert/0;
			rm -r $baseDir/../tmppp;
			chmod -R 777 $baseDir/*;
			"
	done
	exitIfExecutionFailed $?
fi


echo "[INFO] Done."