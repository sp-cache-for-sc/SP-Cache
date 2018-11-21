from scipy.stats import zipf
import os
from os.path import dirname
import numpy
import sys
from random import shuffle
import time
'''
At master node
1. Prepare the test files;
    1.1 Generate the popularity (zipf distribution)
    1.2 decide k
    1.3 write files into Alluxio (overwrite)
2. Distribute the popularity file across the client cluster
'''

def SPTestSetUp(fileSize, zipfFactor,flag): # file size in MB, flag: whether write the files
    #settings
	fileNumber = 10 #500
    #fileSize = 200 #MB
    #zipfFactor = 1.5
        machineNumber = 30 #30
	SPFactor = 6
    # generate popularity vector
	popularity = list()
	for i in range(1, fileNumber+1 ,1):
		popularity.append(zipf.pmf(i, zipfFactor))
	popularity/=sum(popularity)
	shuffle(popularity)	
	tests_dir = os.path.expanduser('~') # for Linux
	#tests_dir = os.getenv('HOME')# for mac OS
	print "tests dir:" + tests_dir

	if not os.path.exists(tests_dir+"/test_files"):
		os.makedirs(tests_dir+"/test_files")

	fw = open(tests_dir+"/test_files/popularity.txt", "wb")
	for item in popularity:
		fw.write("%s\n" % item)

    # calculate the partition_number, in the range of [1, machineNumber]
	kVector = [max(min(int(popularity[id] * 100 * SPFactor), machineNumber),1) for id in range(0, fileNumber)]
    #kVector =10*numpy.ones(fileNumber,dtype=numpy.int)
    # print partitionNumber
	fw = open(tests_dir+"/test_files/k.txt", "wb")
	for k in kVector:
		fw.write("%s\n" % k)
	fw.close()

    #create the file of given size
	with open(tests_dir+"/test_files/test_local_file", "wb") as out:
		out.seek((fileSize * 1000 * 1000) - 1)
		out.write('\0')
	out.close()

    # write the files to Alluxio given the kvalues profile
    # remember to add the path of alluxio
	if(flag==1):
		start = int(round(time.time() * 1000)) # in millisecond
		os.system('./bin/alluxio runSPPrepareFile')
		end = int(round(time.time() * 1000)) 
		print 'Write %s files takes %s' % (fileNumber,end-start)

if __name__ == "__main__":
    SPTestSetUp(int(sys.argv[1]), float(sys.argv[2]),int(sys.argv[3]))
