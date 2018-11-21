package sptests;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.RandomUniquePolicy;
import alluxio.examples.BasicOperations;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import alluxio.client.file.SPFileReader;
import selectpart.SPFileWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by renfei on 2017/11/24.
 *
 * This executable is used for testing the SP-Client.
 */
public class SPClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(BasicOperations.class);

    /** Directory for the test generated files. */
    public static final String TEST_PATH = "/lb_tests_files";
    private final String mLocalFile = System.getProperty("user.dir") + "/test_files/512MB.zip";

    public SPClientTest() {}

    public static void main(String[] args) throws Exception {
        AlluxioURI testDir = new AlluxioURI(TEST_PATH);

        SPClientTest operator = new SPClientTest();

        FileSystem fs = FileSystem.Factory.get();
        if (fs.exists(testDir)) {
            fs.delete(testDir, DeleteOptions.defaults().setRecursive(true).setUnchecked(true));
        }
        int ret = operator.runOperation(fs);
        System.exit(ret);
    }

    private int runOperation(FileSystem fs) throws Exception {
        LOG.info("Local file path: " + mLocalFile);
        testIOParallelism(fs, mLocalFile);
        return 0;
    }

    private void testIOParallelism(FileSystem fs, String localFile) throws Exception{

        // Test the effect of IO parallelism in a cluster, by distributing the file to different number of workers.
        int [] partitionNumber = {1, 3, 5, 10, 15, 20, 25, 30}; // number of partitions
        File file = new File(localFile);
        // copy a local file to Alluxio with different number of partitions
        for (int aPartitionNumber : partitionNumber) {
            // note that the actual partition number might be different
            AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, aPartitionNumber));
            copyFromLocal(filePath, aPartitionNumber, fs, localFile);
        }

        // run read files and repeat for a couple of times
        String log = System.getProperty("user.dir") + "/test_files/readTimes.txt"; // log the read latency
        FileWriter fw = new FileWriter(log,true); //the true will append the new data
        for (int aPartitionNumber : partitionNumber) {
            AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, aPartitionNumber));
            long timeTaken = readFileParallel(fs, filePath);
            fw.write("Read file time" + aPartitionNumber + "\t" + timeTaken + "\n");
        }
        fw.close();
    }

    // Read the file in parallel test.
    private long readFileParallel(FileSystem fileSystem, AlluxioURI filePath)
            throws Exception {
        LOG.debug("Reading data...");

        SPFileReader fileReader = new SPFileReader(fileSystem, filePath);
        return fileReader.runRead();
    }

    // First load local file in memory, then write the file into Alluxio
    private void copyFromLocal(AlluxioURI writePath, int k, FileSystem fileSystem, String localFile)
            throws IOException, AlluxioException {

        LOG.debug("Loading data into memory...");
        // Read file into memory
        FileInputStream is = new FileInputStream(localFile);
        int tFileLength = (int) new File(localFile).length();
        byte[] tBuf = new byte[tFileLength];
        // The total file is read into the buffer.
        int tBytesRead = is.read(tBuf);
        LOG.info("Read local file into memory with bytes:" + tBytesRead);
        // Then write the file from memory into Alluxio.
        SPFileWriter tSPWriter = new SPFileWriter(k, tFileLength, fileSystem, writePath);
        long tStartTimeMs = CommonUtils.getCurrentMs();
        tSPWriter.writeFile(tBuf);
        is.close();
        LOG.info(FormatUtils.formatTimeTakenMs(tStartTimeMs,
                "writing " +  localFile + " into Alluxio path " + writePath));
    }

}
