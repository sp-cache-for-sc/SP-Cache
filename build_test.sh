#!/bin/bash

cd loadbalance
mvn install
/root/spark-ec2/copy-dir /root/alluxio-load-balancing/loadbalance
