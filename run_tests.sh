#!/bin/bash


# Usage: $1 means the partition number, $2 means the client number
# run read tests on the last 20 slaves
read -ra slave_arr -d '' <<<"$SLAVES"
#SCRIPT="cd alluxio-load-balancing; ./bin/alluxio runSPReadTest $1 &>/dev/null &"
SCRIPT="cd alluxio-load-balancing; ./bin/alluxio runSPClientTest &>/dev/null &"
for ((i = 30; i <= $1 + 29; i++))
do
    slave="${slave_arr[$i]}"
    echo $slave
    ssh -l "root" "${slave_arr[$i]}" "${SCRIPT}"
done

