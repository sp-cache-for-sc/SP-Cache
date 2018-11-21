#!/bin/bash

#set conf/alluxio-env
rm conf/alluxio-env.sh
./bin/alluxio bootstrapConf $MASTER 

#set the alluxio master
echo $MASTER > 'conf/masters'
echo 'conf/masters set.'

#read -ra slave_arr -d '' <<<"$SLAVES"
#Add path to alluxio
#SCRIPT="echo 'export PATH=$PATH:~/alluxio-load-balancing/bin' >> ~/.bash_profile"
#for ((i = 0; i <= 49; i++))
#do
#    slave="${slave_arr[$i]}"
#    echo $slave
#    ssh -l "root" "${slave_arr[$i]}" "${SCRIPT}"
#done
#echo 'Path added.'

# set the first 30 slaves as the alluxio workers
read -ra slave_arr -d '' <<<"$SLAVES"
echo "#workers are listed below" > 'conf/workers'
#for i in {0..4}
for i in {0..29}
do
    echo ${slave_arr[$i]} >> 'conf/workers'
done
echo 'conf/workers set.'

# copy alluxio conf to all slaves in the cluster
/root/spark-ec2/copy-dir /root/alluxio-load-balancing/conf
./bin/alluxio format
# start alluxio
./bin/alluxio-start.sh all

