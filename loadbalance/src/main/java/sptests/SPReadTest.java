package sptests;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.SPFileReader;
import alluxio.client.file.options.DeleteOptions;
import alluxio.examples.BasicOperations;
import alluxio.exception.AlluxioException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by renfei on 2017/12/1.
 *
 * This class only perform read tests.
 */
public class SPReadTest {

    private static final Logger LOG = LoggerFactory.getLogger(BasicOperations.class);

    /** Directory for the test generated files. */
    public static final String TEST_PATH = "/lb_tests_files";
    public int partNum = 1;
    private int repeatTime = 10;

    public SPReadTest() {}

    public static void main(String[] args) throws Exception {

        SPReadTest operator = new SPReadTest();
        FileSystem fs = FileSystem.Factory.get();
        try {
            operator.partNum = Integer.parseInt(args[0]);
            operator.repeatTime = Integer.parseInt(args[1]);
        }
        catch (NumberFormatException nfe) {
            System.out.println("The first argument must be an integer that stands for number of partitions. The second parameter stands for the number of repeats.");
            System.exit(1);
        }

        int ret = operator.runOperation(fs);
        System.exit(ret);
    }

    private int runOperation(FileSystem fs) throws Exception {
        testIOParallelism(fs);
        return 0;
    }

    private void testIOParallelism(FileSystem fs) throws Exception{

        // run read files and repeat for a couple of times
        String log = System.getProperty("user.dir") + "/test_files/repeatRead.txt"; // log the read latency
        FileWriter fw = new FileWriter(log,true); //the true will append the new data
        long tTotalTime = 0;
        for (int repeat = 0; repeat < repeatTime; repeat++) {
            AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, partNum));
            long tTimeTaken = readFileParallel(fs, filePath);
            tTotalTime = tTotalTime + tTimeTaken;
            fw.write("Read file: \t" + partNum + "\t" + tTimeTaken + "\t ms\n");
        }
        //long tAverageTime = tTotalTime / repeatTime;
        //fw.write("Read file time average Partition Number:" + partNum + "\t" + tAverageTime + " ms\n");
        fw.close();
    }

    // Read the file in parallel test.
    private long readFileParallel(FileSystem fileSystem, AlluxioURI filePath)
            throws Exception {
        LOG.debug("Reading data...");

        SPFileReader fileReader = new SPFileReader(fileSystem, filePath);
        return fileReader.runRead();
    }


}
