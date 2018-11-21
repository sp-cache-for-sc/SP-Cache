
import numpy
import os
import sys
from os.path import dirname
import subprocess
import time

'''
At every client node

Submit file requests according to the popularity profile.
'''

#totalCount = 3 # the number of total file requests

def SPBenchmark(totalCount, rate):


    # load the popularity vector
    popularity = list()

    tests_dir = os.path.expanduser('~') # for Linux
    #tests_dir = os.getcwd() # for mac OS
    print "tests dir:" + tests_dir

    fw = open(tests_dir+"/test_files/popularity.txt", "r")

    line = fw.readline()
    while(len(line)>0):
        popularity.append(float(line))
        line = fw.readline()

    fileNumber = len(popularity)
    #print popularity
    for i in range(0, totalCount):
        # get a file id from the popularity
        interval = numpy.random.exponential(1.0/rate)
        print "sleep for %s seconds" % interval
        time.sleep(interval)
        fileId = numpy.random.choice(numpy.arange(0, fileNumber), p=popularity)
        os.system('bin/alluxio runSPReadExecutor %s >> /tmp/log &' % fileId)

    os.system('wait')
    #line = os.popen('jobs').read()
    #print line
    #while(len(line)>0):
        #print line
        #line = os.popen('jobs').read()
    os.system('echo "All read requests submitted!" ')

if __name__ == "__main__":
    SPBenchmark(int(sys.argv[1]), float(sys.argv[2]))
