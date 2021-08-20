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

# TODO write SF
SF=1000

echo "[INFO] Postprocess partitions"
mkdir /mnt/sdd1/jhko/ldbc-datagen/SF${SF}
mergePartition \
 	/mnt/sdd1/jhko/ldbc-datagen/out-SF${SF}/out/csv/interactive/composite-projected-fk/dynamic/Person_knows_Person/ \
	/mnt/sdd1/jhko/ldbc-datagen/SF${SF}/P2P_raw.txt
exitIfExecutionFailed $?

mergePartition \
 	/mnt/sdd1/jhko/ldbc-datagen/out-SF${SF}/out/csv/interactive/composite-projected-fk/dynamic/Person_isLocatedIn_Place/ \
	/mnt/sdd1/jhko/ldbc-datagen/SF${SF}/P2C_raw.txt
exitIfExecutionFailed $?

mergePartition \
	/mnt/sdd1/jhko/ldbc-datagen/out-SF${SF}/out/csv/interactive/composite-projected-fk/static/Place_isPartOf_Place/	\
	/mnt/sdd1/jhko/ldbc-datagen/SF${SF}/Place2Place_raw.txt
exitIfExecutionFailed $?

echo "[INFO] Convert to graph format"
python3 ./convert.py P2P \
	/mnt/sdd1/jhko/ldbc-datagen/SF${SF}/P2P_raw.txt \
	/mnt/sdd1/jhko/ldbc-datagen/SF${SF}/P2P.txt
exitIfExecutionFailed $?

python3 ./convert.py P2C \
	/mnt/sdd1/jhko/ldbc-datagen/SF${SF}/P2C_raw.txt \
	/mnt/sdd1/jhko/ldbc-datagen/SF${SF}/P2C.txt
exitIfExecutionFailed $?

python3 ./convert.py Place2Place	\
	/mnt/sdd1/jhko/ldbc-datagen/SF${SF}/Place2Place_raw.txt	\
	/mnt/sdd1/jhko/ldbc-datagen/SF${SF}/Place2Place.txt
exitIfExecutionFailed $?

echo "[INFO] Done."