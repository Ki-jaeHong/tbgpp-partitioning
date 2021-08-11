# convert to graph format (P2P only)
convert.py P2P input_filename output_filename

# csv to binary
/mnt/penfolds/tools/edgelisttxt-to-edgelist8B \
 /home/tslee/jhko/tbgpp-partitioning/ldbccsv-tbgpp-converter/tmp/P2P \
 /home/tslee/jhko/tbgpp-partitioning/tmp/P2P_bin

# split binary files
# TODO

# distribute binary files
# TODO

# run bbp over machines
# TODO