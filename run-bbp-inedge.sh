#!/bin/bash

# BBP Graph Partitioning
# ./tbgpp_gp_unattended.sh graph_name machine_file

# $1 = graph_file
# $2 = machine_file
# Graph file must have lines with format of
#     {name}_v{log_2(|V|)}_[e|r][{log_2(|V|)}|{|E|/|V|}]/{input_type}/{copy_original}

# Argument Check
#if [ $# -ne '0' ]; then
#    echo "Usage:"
#    echo "    $0 graph_name machine_file"
#    echo
#    echo "Graph file must have lines with format of:"
#    echo '    { name }_v{ log_2(|V|) }_[e|r][{ log_2(|V|) }|{ |E|/|V| }]/{ input_type }/{ copy_original }'
#    echo
#    exit
#fi


# FIXME root path
LOCAL_ROOT=/mnt/data2/jhko/partitioning/

# FIXME choose binary path
BIN_ROOT=/home/tslee/jhko/Turbograph_jhko_partitioning/bin
BIN_NAME=GraphPartitioning_inedge_release  # TODO
CONF_NAME=${BIN_NAME}.conf
BIN_PATH=${BIN_ROOT}/${BIN_NAME}
CONF_PATH=${BIN_ROOT}/${CONF_NAME}

# FIXME choose input folder
INPUT_FOLDER=OutEdges   # TODO changed

# FIXME choose temp folder
TEMP_FOLDER=temp
TEMP_PATH=${LOCAL_ROOT}/${TEMP_FOLDER}

log_time=$(date +"20%y-%m-%d-%Hh-%Mm-%Ss")
EXP_BEGIN=$log_time
my_name=$(whoami)
mkdir -p /mnt/sdb1/logs
mkdir -p /mnt/sdb1/logs/$my_name    
log_dir=/mnt/sdb1/logs/$my_name
if [ ! -d "$log_dir" ];
  then  
  mkdir -p "$log_dir"
fi
log_file_path=$log_dir/${EXP_BEGIN}

# FIXME graph_file
graph_file=/home/tslee/jhko/Turbograph_jhko_partitioning/graphs/inedge

# FIXME machine_file
machine_file=/home/tslee/jhko/tbgpp-partitioning/bbp/_machines/001-005

function remote_sh {
    argc=$#
    local_script=$1
    host=$2
    shift 2

    declare -a args

    count=0
    for arg in "$@"; do
    args[count]=$(printf '%q' "$arg")
    count=$((count+1))
    done

    ssh $host 'cat | bash /dev/stdin' "${args[@]}" < "$local_script"
}

function distribute_from_hdfs {
    echo ""
 }

function create_config {
    rm -f ${BIN_NAME}.conf base value
    echo 'source_path' >> base
    echo ${LOCAL_ROOT}/$1/${INPUT_FOLDER}/ >> value # for In-edge gp
    #echo ${LOCAL_ROOT}/$2/${INPUT_FOLDER}/ >> value # for original BBP
    echo 'destination_path' >> base
    echo ${LOCAL_ROOT}/$2/ >> value
    echo 'partitioned_range_path' >> base
    echo ${LOCAL_ROOT}/$2/PartitionRanges/ >> value
    echo 'mapping_info_path' >> base
    echo ${LOCAL_ROOT}/$2/MappingInfo/mapping.txt >> value
    echo 'subchunk_vid_range_path' >> base
    echo ${LOCAL_ROOT}/$2/EdgeSubChunkVidRanges/ >> value
    echo 'given_partition_range_path' >> base
    echo ${LOCAL_ROOT}/$2/PartitionRanges_backup/range.txt >> value
    echo 'temp_path' >> base
    echo ${TEMP_PATH}/ >> value
    echo 'temp_path2' >> base
    echo ${TEMP_PATH}/ >> value
    echo 'num_vertices' >> base
	echo 2 ^ $3 | bc >> value
	#echo 2 ^ $2 | bc >> value
    echo 'remove_mode' >> base
    echo 0 >> value
    echo 'bidirect_mode' >> base
	if [ "$5" == "e" ]; then
		echo 0 >> value
	else
		echo 1 >> value
	fi
	echo 'num_threads' >> base
    echo 8 >> value
    echo 'input_type' >> base
    echo $6 >> value
    echo 'random_mode' >> base
    echo 0 >> value
    echo 'num_edges' >> base
    if [ "$5" == "e" ]; then
        echo 2 ^ $4 | bc >> value
    else
        echo 2 ^ $3 \* $4 | bc >> value
    fi
	echo 'max_vertex_id' >> base
	echo 2 ^ $3 | bc >> value
	#echo 77809850 >> value
	#echo 2 ^ $2 | bc >> value
    echo 'mem_size' >> base
	#echo 30064771072 >> value # 28GB
	echo 25769803776 >> value #24GB
	#echo 68719476736 >> value #64GB
    echo 'data_mode' >> base
    echo 1 >> value
    echo 'degree' >> base
    echo 1 >> value
    echo 'uniqueness_test_mode' >> base
    echo 0 >> value
    echo 'diff_test_mode' >> base
    echo 0 >> value
	echo 'inout_test_mode' >> base
	if [ "$5" == "e" ]; then
		echo 0 >> value
	else
		echo 0 >> value
	fi
    echo 'redist_validation' >> base
    echo 0 >> value
    echo 'sort_validation' >> base
    echo 0 >> value
    echo 'partitioning_scheme' >> base
	echo 0 >> value
    echo 'build_db' >> base
	echo 0 >> value
    echo 'storing_inedges' >> base                      # TODO changed
	echo 1 >> value
    echo 'distribute_inedges_only' >> base              # TODO changed
	echo 1 >> value
    # FIXME partition by dst
    echo 'partition_by_dst' >> base
	echo 0 >> value
	echo 'renumber_using_mapping_info' >> base
	echo 0 >> value
    # FIXME from-to
        # 0 (for undirected)
        # 1 create_edge_colony
        # 2 renumbering
        # 5 rearrange_edge_distribution 
        # 6 redistribute_edges
        # 7 create_sorted_run
        # 8 merge_sorted_runs
	echo 'phase_from' >> base
	echo 1 >> value
	echo 'phase_to' >> base
	echo 8 >> value
	echo '========================CONFIG========================'
    paste -d= base value | tee ${CONF_NAME}
    echo '======================================================'
}

function prepare_machines {
    exec {FD_MACHINE}< $1
    while read -u ${FD_MACHINE} machine; do
        ssh ${machine} -n "mkdir -p $2/EdgeSubChunkVidRanges \
						 ; mkdir -p $2/PartitionRanges \
						 ; mkdir -p $2/MappingInfo \
						 ; mkdir -p $2/edgedb \
						 ; mkdir -p $2/ResultOutEdges \
						 ; mkdir -p ${TEMP_PATH} \
                         ; mkdir -p ${BIN_ROOT} \
                         ; sudo chmod 777 -R $2 \
                         ; sudo chmod 777 -R ${TEMP_PATH} \
                         ; sudo chmod 777 ${BIN_ROOT}" &
    done
    wait
    exec {FD_MACHINE}<&-

    exec {FD_MACHINE}< $1
    while read -u ${FD_MACHINE} machine; do
        scp ${BIN_PATH} ${machine}:${BIN_ROOT} &
        scp ${CONF_NAME} ${machine}:${BIN_ROOT} &
    done
    wait
    exec {FD_MACHINE}<&-
}

function remove_remainders {
    exec {FD_MACHINE}< $2
    while read -u ${FD_MACHINE} machine; do
        ssh ${machine} -n "rm -rf ${LOCAL_ROOT}/$1 ${TEMP_PATH}" &
    done
    wait
    exec {FD_MACHINE}<&-
}

function remove_temp {
    exec {FD_MACHINE}< $2
    while read -u ${FD_MACHINE} machine; do
        ssh ${machine} -n "rm -rf ${TEMP_PATH}" &
    done
    wait
    exec {FD_MACHINE}<&-
}

echo "START"
exec {FD_GRAPH}< ${graph_file}
while read -u ${FD_GRAPH} graph; do
    IFS='|' read -a parse <<< "${graph}"
	IFS='/' read -a graph_name <<< "${parse[0]}"
    #graph_name=${parse[0]}
    output_dir=${parse[1]}
    input_type=${parse[2]}
    copy_original=${parse[3]}
    IFS='_' read -a parse <<< "${graph_name}"
    vertex=${parse[1]:1}
    edge=${parse[2]:1}
    edge_mode=${parse[2]:0:1}
    echo ${graph_name}
    if [ "${copy_original}" != "0" ]; then
        distribute_from_hdfs ${HDFS_ROOT}/${graph_name} ${LOCAL_ROOT}/${graph_name} ${machine_file}
    fi
    echo create_config ${graph_name} ${output_dir} ${vertex} ${edge} ${edge_mode} ${input_type}
    create_config ${graph_name} ${output_dir} ${vertex} ${edge} ${edge_mode} ${input_type}
    echo prepare_machines ${machine_file} ${LOCAL_ROOT}/${output_dir}
    prepare_machines ${machine_file} ${LOCAL_ROOT}/${output_dir}

	I_MPI_PERHOST=1 \
	I_MPI_SCALABLE_OPTIMIZATION=1 \
	I_MPI_PIN_DOMAIN=node \
	I_MPI_SPIN_COUNT=5000 \
    I_MPI_FABRICS=tcp \
    I_MPI_FALLBACK=0 \
    I_MPI_PIN_DOMAIN=node \
    I_MPI_DYNAMIC_CONNECTION=0 \
    I_MPI_DAPL_TRANSLATION_CACHE=0 \
	I_MPI_ADJUST_ALLREDUCE=2 \
	I_MPI_EAGER_THRESHOLD=2097152 \
    MALLOC_CONF="narenas:4" \
    #perf record -g -e cpu-cycles:u -o ./perf_test.txt mpiexec.hydra -genvall -machinefile ${machine_file} ${BIN_PATH} ${CONF_PATH}
    mpiexec.hydra -genvall -machinefile ${machine_file} ${BIN_PATH} ${CONF_PATH} & > ${log_file_path}

	#sleep 3
    # FIXME remove tmpfile
	# echo remove temp files
	# remove_temp ${graph_name} ${machine_file}
	#sleep 3
done
exec {FD_GRAPH}<&-
rm -f ${CONF_NAME} base value

