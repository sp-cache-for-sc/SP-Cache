
import os
import sys


def free_file(start_id, end_id):

    for i in range(start_id, end_id, 1):
        os.system('./bin/alluxio fs persist /tests/%s' % i)
        os.system('./bin/alluxio fs free /tests/%s' % i)


if __name__ == "__main__":
    free_file(int(sys.argv[1]), int(sys.argv[2]))
