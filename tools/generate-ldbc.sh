# make folder
mkdir -p /mnt/sdd1/jhko/ldbc-datagen/out-SFXXXX/out

# symbolic link out folder
ln -s /mnt/sdd1/jhko/ldbc-datagen/out-SFXXXX/out ./out

# TODO clear contents in out/


# run
JAR="./target/scala-2.11/ldbc_snb_datagen-assembly-0.4.0-SNAPSHOT.jar"
#JAR="./target/ldbc_snb_datagen-0.4.0-SNAPSHOT.jar"

SF=300

./tools/run.py $JAR --cores 14 --parallelism 100 --memory 100G \
-- --format csv --scale-factor $SF --mode interactive --explode-edges --param-file conf/params_jhko_minoutput.ini

# merge partitions (csv files)
# TODO