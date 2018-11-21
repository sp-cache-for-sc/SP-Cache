#!/usr/bin/env bash

read -ra slave_arr -d '' <<<"$SLAVES"
#SCRIPT="pkill SPReadTest;pkill SPReadExecutor"
#SCRIPT="ps ax | grep SPReadExecutor |awk -F ' ' '{print $1}' | xargs kill -9"
SCRIPT="cd alluxio-load-balancing; bash killSPReader.sh"
for i in {0..49}
do
    slave="${slave_arr[$i]}"
    echo $slave
    ssh -l "root" "${slave_arr[$i]}" "${SCRIPT}"
done
