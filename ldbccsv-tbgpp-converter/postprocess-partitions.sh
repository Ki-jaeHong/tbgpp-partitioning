function exitIfExecutionFailed() {
	if [ $1 -ne 0 ]; then
   		echo "Fail execution... Now exiting"
   		exit 1
	fi
}

function mergePartition {
	INPUT_DIR=$1
	OUTPUT_FILE_NAME=$2

	# 1. pop out header
	# 2. merges them into one OUTPUT_FILE_NAME
	if [ -f $OUTPUT_FILE_NAME ]; then
		rm $OUTPUT_FILE_NAME
		echo "" > $OUTPUT_FILE_NAME
	fi

	for f in $INPUT_DIR/*.csv; do
		# pop out header and write
		echo "processing file $f"
		tail -n +2 "$f" >> $OUTPUT_FILE_NAME
	done
}

# TODO write data path
RAW_FILES_BASEPATH=/mnt/sdd1/jhko/ldbc-datagen/out-SF1000
MERGED_BASEPATH=/mnt/sdd1/jhko/ldbc-datagen/SF1000

echo "[INFO] Postprocess partitions"
mkdir $MERGED_BASEPATH
mergePartition \
 	$RAW_FILES_BASEPATH/out/csv/interactive/composite-projected-fk/dynamic/Person_knows_Person/ \
	$MERGED_BASEPATH/P2P_raw.txt
exitIfExecutionFailed $?

mergePartition \
 	$RAW_FILES_BASEPATH/out/csv/interactive/composite-projected-fk/dynamic/Person_isLocatedIn_Place/ \
	$MERGED_BASEPATH/P2C_raw.txt
exitIfExecutionFailed $?

mergePartition \
	$RAW_FILES_BASEPATH/out/csv/interactive/composite-projected-fk/static/Place_isPartOf_Place/	\
	$MERGED_BASEPATH/Place2Place_raw.txt
exitIfExecutionFailed $?

echo "[INFO] Convert to graph format"
python3 ./convert.py P2P \
	$MERGED_BASEPATH/P2P_raw.txt \
	$MERGED_BASEPATH/P2P.txt
exitIfExecutionFailed $?

python3 ./convert.py P2C \
	$MERGED_BASEPATH/P2C_raw.txt \
	$MERGED_BASEPATH/P2C.txt
exitIfExecutionFailed $?

python3 ./convert.py Place2Place	\
	$MERGED_BASEPATH/Place2Place_raw.txt	\
	$MERGED_BASEPATH/Place2Place.txt
exitIfExecutionFailed $?

echo "[INFO] Done."