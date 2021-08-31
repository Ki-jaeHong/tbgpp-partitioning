
prev=$1

cd /mnt/sdb1/logs/tslee


echo "Completed ==="
cat $(monitor -n $prev) |grep Completed

echo "TOTAL_IO ==="
cat $(monitor -n $prev) |grep TOTAL_IO