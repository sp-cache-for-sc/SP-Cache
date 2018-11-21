package lb;


import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.RandomUniquePolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.Constants;
import alluxio.examples.BasicOperations;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.FileInputStream;
import java.io.File;
import java.io.FileWriter;

/**
 * Created by renfei on 2017/11/16.
 *
 */
public class FileOperations {

    /** Directory for the test generated files. */
    public static final String TEST_PATH = "/lb_tests_files";

    private static final Logger LOG = LoggerFactory.getLogger(BasicOperations.class);

    private static final int NUMBERS = 20;

    private final AlluxioURI mFilePath;
    private final OpenFileOptions mReadOptions;
    private final CreateFileOptions mWriteOptions;

    public FileOperations(AlluxioURI filePath, ReadType readType, WriteType writeType, long blockSize) {
        mFilePath = filePath;
        mReadOptions = OpenFileOptions.defaults().setReadType(readType);
        mWriteOptions = CreateFileOptions.defaults().setWriteType(writeType);
        mWriteOptions.setBlockSizeBytes(blockSize);
        mWriteOptions.setLocationPolicy(new RandomUniquePolicy());
    }

    public static void main(String[] args) throws Exception {
        AlluxioURI testDir = new AlluxioURI(TEST_PATH);

        AlluxioURI filePath =
                new AlluxioURI(String.format("%s/%s_%s", TEST_PATH, ReadType.CACHE_PROMOTE, WriteType.ASYNC_THROUGH));

        FileOperations operator = new FileOperations(filePath, ReadType.CACHE_PROMOTE, WriteType.ASYNC_THROUGH, 10000);

        FileSystem fs = FileSystem.Factory.get();
        if (fs.exists(testDir)) {
            fs.delete(testDir, DeleteOptions.defaults().setRecursive(true).setUnchecked(true));
        }

        // int ret = operator.runOperation(fs);
        String localFile = System.getProperty("user.dir") + "/test_files/512MB.zip";
        testIOParallelism(localFile);
        System.exit(0);
    }

    private int runOperation(FileSystem fs) throws Exception {
        // writeFile(fs);
        String localFile = System.getProperty("user.dir") + "/test_files/512MB.zip";
        LOG.debug("local file path: " + localFile);
        copyFromLocal(fs, localFile);
        readFile(fs);
        return 0;
    }

    private static void testIOParallelism(String localFile) throws Exception{
        // Test the effect of IO parallelism in a cluster, by distributing the file to different number of workers.

        int [] partitionNumber = {1, 3, 5, 10, 15, 20, 25, 30}; // number of partitions
        File file = new File(localFile);
        FileSystem fs = FileSystem.Factory.get();
        // copy a local file to Alluxio with different number of partitions
        for (int i = 0; i < partitionNumber.length; i++) {
            long blockSize = file.length() / partitionNumber[i];// note that the actual partition number might be different

            AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, partitionNumber[i]));
            FileOperations operator = new FileOperations(filePath, ReadType.CACHE_PROMOTE, WriteType.ASYNC_THROUGH, blockSize);
            operator.copyFromLocal(fs, localFile);
        }
        // read files and log the elapsed time
        String log= System.getProperty("user.dir") + "/test_files/testIO.txt";
        FileWriter fw = new FileWriter(log,true); //the true will append the new data
        for (int i = 0; i < partitionNumber.length; i++) {
          AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, partitionNumber[i]));
          long timeTaken = readFile(fs, filePath);
          fw.write(partitionNumber[i] + "\t" + timeTaken + "\n");//append
        }
        fw.close();
    }

    private void writeFile(FileSystem fileSystem)
            throws IOException, AlluxioException {
        ByteBuffer buf = ByteBuffer.allocate(NUMBERS * 4);
        buf.order(ByteOrder.nativeOrder());
        for (int k = 0; k < NUMBERS; k++) {
            buf.putInt(k);
        }
        LOG.debug("Writing data...");
        long startTimeMs = CommonUtils.getCurrentMs();
        FileOutStream os = fileSystem.createFile(mFilePath, mWriteOptions);
        os.write(buf.array());
        os.close();
        LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "writeFile to file " + mFilePath));
    }

    private boolean readFile(FileSystem fileSystem)
            throws IOException, AlluxioException {
        boolean pass = true;
        LOG.debug("Reading data...");
        final long startTimeMs = CommonUtils.getCurrentMs();
        FileInStream is = fileSystem.openFile(mFilePath, mReadOptions);
        ByteBuffer buf = ByteBuffer.allocate((int) is.remaining());
        is.read(buf.array());
        buf.order(ByteOrder.nativeOrder());
        for (int k = 0; k < NUMBERS; k++) {
            pass = pass && (buf.getInt() == k);
        }
        is.close();

        LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "readFile file " + mFilePath));
        return pass;
    }


    private static long readFile(FileSystem fileSystem, AlluxioURI filePath)
            throws IOException, AlluxioException {
        LOG.debug("Reading data...");
        final long startTimeMs = CommonUtils.getCurrentMs();
        FileInStream is = fileSystem.openFile(filePath, OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE));
        byte[] buf = new byte[4*Constants.MB];
        int nRead = 0;
        while((nRead = is.read(buf)) != -1) {
          continue; // just read. no nothing.
        }
        is.close();
        long finishTimeMs = CommonUtils.getCurrentMs();
        return finishTimeMs - startTimeMs;
    }


    private void copyFromLocal(FileSystem fileSystem, String localFile)
            throws IOException, AlluxioException {
        //todo: set buf size to be the min(file_size, 4MB)

        byte[] buf = new byte[4*Constants.MB];
        // buf.order(ByteOrder.nativeOrder());
        LOG.debug("Copying data...");
        // read file
        FileInputStream is = new FileInputStream(localFile);
        FileOutStream os = fileSystem.createFile(mFilePath, mWriteOptions);
        long startTimeMs = CommonUtils.getCurrentMs();

        int nRead = 0;
        while((nRead = is.read(buf)) != -1) {
          os.write(buf, 0, nRead); // offset, length
        }

        is.close();
        os.close();
        LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "copy " +  localFile + " to file " + mFilePath));
    }
}
