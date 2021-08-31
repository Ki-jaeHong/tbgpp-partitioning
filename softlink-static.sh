#!/bin/bash

data=ldbc-snb-copart-sf1000-country-equi_v22_e28
#data=ldbc-snb-bbp-sf1000_v22_e28

#clush -w citeb[001-005] rm -r /mnt/data2/tbgppdb/$data/dir5_part1_subchunks2_update100Ki_split10_8k/OutEdge/Insert/0/
clush -w citeb[001-005] "ln -s /mnt/data2/tbgppdb/$data/dir5_part1_subchunks2_update0.1i_split10_8k/OutEdge/Insert/0/ \
		  /mnt/data2/tbgppdb/$data/dir5_part1_subchunks2_update100Ki_split10_8k/OutEdge/Insert/"

clush -w citeb[001-005] "ln -s /mnt/data2/tbgppdb/$data/dir5_part1_subchunks2_update0.1i_split10_8k/InEdge/Insert/0/ \
		  /mnt/data2/tbgppdb/$data/dir5_part1_subchunks2_update100Ki_split10_8k/InEdge/Insert/"