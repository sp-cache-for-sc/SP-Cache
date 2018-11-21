package lb;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by renfei on 2017/11/24.
 *
 * This executable is used for local test. The FileOperations is for distributed test.
 */
public class LocalFileTest {

    /** Directory for the test generated files. */
    public static final String TEST_PATH = "/lb_tests_files";

    private static final Logger LOG = LoggerFactory.getLogger(BasicOperations.class);

    private final AlluxioURI mFilePath;
    private final OpenFileOptions mReadOptions;
    private final CreateFileOptions mWriteOptions;
    private final String mLocalFile = System.getProperty("user.dir") + "/test_files/512MB.zip";

    public LocalFileTest(AlluxioURI filePath, ReadType readType, WriteType writeType, long blockSize) {
        mFilePath = filePath;
        mReadOptions = OpenFileOptions.defaults().setReadType(readType);
        mWriteOptions = CreateFileOptions.defaults().setWriteType(writeType);
        mWriteOptions.setBlockSizeBytes(blockSize);
        mWriteOptions.setLocationPolicy(new RandomUniquePolicy()); // so that each partition will be placed on a unique worker.
    }

    public static void main(String[] args) throws Exception {
        AlluxioURI testDir = new AlluxioURI(TEST_PATH);
        AlluxioURI filePath =
                new AlluxioURI(String.format("%s/%s_%s", TEST_PATH, ReadType.CACHE_PROMOTE, WriteType.ASYNC_THROUGH));

        LocalFileTest operator =
                new LocalFileTest(filePath, ReadType.CACHE_PROMOTE, WriteType.ASYNC_THROUGH, 10000);

        FileSystem fs = FileSystem.Factory.get();
        if (fs.exists(testDir)) {
            fs.delete(testDir, DeleteOptions.defaults().setRecursive(true).setUnchecked(true));
        }
        int ret = operator.runOperation(fs);
        System.exit(ret);
    }

    private int runOperation(FileSystem fs) throws Exception {
        LOG.debug("local file path: " + mLocalFile);
        testIOParallelism(fs, mLocalFile);
        return 0;
    }

    private void testIOParallelism(FileSystem fs, String localFile) throws Exception{
        // Test the effect of IO parallelism in a cluster, by distributing the file to different number of workers.

        int [] partitionNumber = {1, 3, 5, 10, 15, 20, 25, 30}; // number of partitions
        File file = new File(localFile);
        // copy a local file to Alluxio with different number of partitions
        for (int aPartitionNumber : partitionNumber) {
            long blockSize = file.length() / aPartitionNumber;// note that the actual partition number might be different

            AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, aPartitionNumber));
            LocalFileTest operator = new LocalFileTest(filePath, ReadType.CACHE_PROMOTE, WriteType.ASYNC_THROUGH, blockSize);
            operator.copyFromLocal(fs, localFile);
        }
        // read files and repeat for a couple of times
        String log = System.getProperty("user.dir") + "/test_files/testIO.txt"; // log the read latency
        FileWriter fw = new FileWriter(log,true); //the true will append the new data
        for (int repeat=0; repeat<=10; repeat++){
            for (int aPartitionNumber : partitionNumber) {
                AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, aPartitionNumber));
                long timeTaken = readFileParallel(fs, filePath, aPartitionNumber);
                fw.write(aPartitionNumber + "\t" + timeTaken + "\n");//append
            }
        }
        fw.close();
    }

    // Read the file in parallel test.
    private long readFileParallel(FileSystem fileSystem, AlluxioURI filePath, int k)
            throws IOException, AlluxioException {
        LOG.debug("Reading data...");
        final long startTimeMs = CommonUtils.getCurrentMs();

        ReadFileParallel fileReader = new ReadFileParallel(fileSystem, filePath, mReadOptions, k);
        fileReader.read();

        long finishTimeMs = CommonUtils.getCurrentMs();
        return finishTimeMs - startTimeMs;
    }

    // This function doesn't read file in multiple threads.
//    private long readFile(FileSystem fileSystem, AlluxioURI filePath)
//            throws IOException, AlluxioException {
//        LOG.debug("Reading data...");
//        final long startTimeMs = CommonUtils.getCurrentMs();
//        FileInStream is = fileSystem.openFile(filePath, mReadOptions);
//        byte[] buf = new byte[4* Constants.MB];
//        int nRead = 0;
//        while((nRead = is.read(buf)) != -1) {
//            LOG.debug("Reading number of bytes:" + nRead);
//            // just read. no nothing.
//        }
//        is.close();
//        long finishTimeMs = CommonUtils.getCurrentMs();
//        return finishTimeMs - startTimeMs;
//    }


    private void copyFromLocal(FileSystem fileSystem, String localFile)
            throws IOException, AlluxioException {
        //todo: set buf size to be the min(file_size, 4MB)

        byte[] buf = new byte[4*Constants.MB];
        LOG.debug("Copying data...");
        // read file
        FileInputStream is = new FileInputStream(localFile);
        FileOutStream os = fileSystem.createFile(mFilePath, mWriteOptions); // the write policy is uniquely random
        long startTimeMs = CommonUtils.getCurrentMs();

        int nRead;
        while((nRead = is.read(buf)) != -1) {
            os.write(buf, 0, nRead); // offset, length
        }
        is.close();
        os.close();
        LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "copy " +  localFile + " to file " + mFilePath));
    }

}
