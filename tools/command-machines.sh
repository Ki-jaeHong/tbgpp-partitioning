nodeIdStart=001
nodeIdEnd=005


baseDir="/mnt/data2/tbgppdb/ldbc-snb-copart_v19_e24/dir5_part1_subchunks2"

#command="rm -r /mnt/data2/jhko/partitioning/ldbc-snb-copart_v19_e24_bin_edgelist   && rm -r /mnt/data2/tbgppdb/ldbc-snb-copart_v19_e24/"
#command="rm -r /mnt/data2/jhko/partitioning/ldbc-snb-copart-country-equi_v19_e24_bin_edgelist   && rm -r /mnt/data2/tbgppdb/ldbc-snb-copart-country-equi_v19_e24/"

command="rm -r /mnt/data2/jhko/partitioning/ldbc-snb-bbp-sf1000_v22_e28_bin_edgelist"

for i in $(seq -f "%03g" $nodeIdStart $nodeIdEnd); do
	ssh citeb${i} -n $command
	#scp /home/tslee/jhko/turbograph-iotest/DropPageCache citeb${i}:/mnt/data2/jhko/tools
	echo "machine ${i} : code $?"
done