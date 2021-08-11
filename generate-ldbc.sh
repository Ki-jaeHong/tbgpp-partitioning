# make folder
mkdir /mnt/sdd1/jhko/ldbc-datagen/out-SFXXXX
mkdir /mnt/sdd1/jhko/ldbc-datagen/out-SFXXXX/out

# symbolic link out folder
ln -s mkdir /mnt/sdd1/jhko/ldbc-datagen/out-SFXXXX/out ./out

# run
./tools/run.py ./target/ldbc_snb_datagen-0.4.0-SNAPSHOT.jar --cores 14 --parallelism 100 --memory 100G \
-- --format csv --scale-factor 100 --mode interactive --explode-edges --param-file conf/params_jhko_minoutput.ini

# merge partitions (csv files)
# TODO