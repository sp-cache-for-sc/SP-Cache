#!/bin/bash


# $1 is the first parameter means the number of clients
# run read tests on the last $1 slaves
read -ra slave_arr -d '' <<<"$SLAVES"
rm results/*.txt
touch results/all_latency.txt
touch results/all_fileHit.txt
touch results/all_workerLoads.txt

#SCRIPT="cd alluxio-load-balancing; ./bin/alluxio readTest &>/dev/null &"
#for ((i = 5; i <= $1 + 4; i++))
for ((i = 30; i <= $1 + 29; i++))
do
    echo $i
    slave="${slave_arr[$i]}"
    echo $slave
    #scp root@${slave_arr[$i]}:/root/alluxio-load-balancing/test_files/readTimes.txt test_files/$i.txt
    scp root@${slave_arr[$i]}:/root/alluxio-load-balancing/logs/readLatency.txt /root/alluxio-load-balancing/results/${i}_latency.txt
    cat results/${i}_latency.txt >> results/all_latency.txt
    scp root@${slave_arr[$i]}:/root/alluxio-load-balancing/logs/fileHit.txt /root/alluxio-load-balancing/results/${i}_fileHit.txt
    cat results/${i}_fileHit.txt >> results/all_fileHit.txt
    scp root@${slave_arr[$i]}:/root/alluxio-load-balancing/logs/workerLoads.txt /root/alluxio-load-balancing/results/${i}_workerLoads.txt
    cat results/${i}_workerLoads.txt >> results/all_workerLoads.txt
done

# Collect all the results into a single file for the convience
#cd results
#rm all_results.txt
#cat *.txt > all_results.txt
#cd ..

