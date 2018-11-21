#!/bin/bash


# Usage: $1 means the client number, $2 means the total request number each clientshall submit, $3 the request arrival rate on each client
read -ra slave_arr -d '' <<<"$SLAVES"

SCRIPT="cd alluxio-load-balancing;PATH=$PATH python python/SPBenchmark.py $2 $3 > /tmp/pythonlog &"
echo $SCRIPT

#for ((i = 5; i <= $1 + 4; i++))
for ((i = 30; i <= $1 + 29; i++))
do
    echo $i
    slave="${slave_arr[$i]}"
    echo $slave
    ssh -l "root" "${slave_arr[$i]}" "${SCRIPT}" # error of alluxio: unknown command
   
done
echo Done
