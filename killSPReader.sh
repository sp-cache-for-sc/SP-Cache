#!/bin/bash
ps ax | grep SPReadExecutor |awk -F ' ' '{print $1}' | xargs kill -9
