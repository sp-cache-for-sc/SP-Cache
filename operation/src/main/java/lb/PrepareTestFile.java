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
 * Created by Yinghao on 2017/11/30.
 *
 * This executable is used for preparing the data for tests.
 */
public class PrepareTestFile {

  /** Directory for the test generated files. */
  public static final String TEST_PATH = "/lb_tests_files"; // path in alluxio

  private static final Logger LOG = LoggerFactory.getLogger(BasicOperations.class);
  private final String mLocalFile = System.getProperty("user.dir") + "/test_files/512MB.zip"; //local file path
  private final CreateFileOptions mWriteOptions;

  public PrepareTestFile() {
    mWriteOptions = CreateFileOptions.defaults().setWriteType(WriteType.ASYNC_THROUGH);
    mWriteOptions.setLocationPolicy(new RandomUniquePolicy()); // so that each partition will be placed on a unique worker.
  }

  public static void main(String[] args) throws Exception {

    AlluxioURI testDir = new AlluxioURI(TEST_PATH);

    FileSystem fs = FileSystem.Factory.get();
    if (fs.exists(testDir)) {
      fs.delete(testDir, DeleteOptions.defaults().setRecursive(true).setUnchecked(true));
    }

    AlluxioURI filePath =
            new AlluxioURI(String.format("%s/%s_%s", TEST_PATH, ReadType.CACHE_PROMOTE, WriteType.ASYNC_THROUGH));

    PrepareTestFile operator = new PrepareTestFile();
    int ret = operator.runOperation(fs);
    System.exit(ret);
  }

  private int runOperation(FileSystem fs) throws Exception {
    LOG.debug("local file path: " + mLocalFile);
    prepare(fs, mLocalFile);
    return 0;
  }

  private void prepare(FileSystem fs, String localFile) throws Exception{
    // Test the effect of IO parallelism in a cluster, by distributing the file to different number of workers.
    int [] partitionNumber = {1, 3, 5, 10, 15, 20, 25, 30}; // number of partitions
    File file = new File(localFile);
    // copy a local file to Alluxio with different number of partitions
    for (int aPartitionNumber : partitionNumber) {
      long blockSize = file.length() / aPartitionNumber;// note that the actual partition number might be different
      AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, aPartitionNumber));
      copyFromLocal(fs, localFile, filePath, blockSize);
    }
  }

  private void copyFromLocal(FileSystem fileSystem, String localFile, AlluxioURI alluxioFilePath, long blockSize)
          throws IOException, AlluxioException {
    //todo: set buf size to be the min(file_size, 4MB)

    byte[] buf = new byte[4*Constants.MB];
    LOG.debug("Copying data...");
    // read file
    FileInputStream is = new FileInputStream(localFile);
    mWriteOptions.setBlockSizeBytes(blockSize);
    FileOutStream os = fileSystem.createFile(alluxioFilePath, mWriteOptions); // the write policy is uniquely random
    long startTimeMs = CommonUtils.getCurrentMs();

    int nRead;
    while((nRead = is.read(buf)) != -1) {
      os.write(buf, 0, nRead); // offset, length
    }
    is.close();
    os.close();
    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "copy " +  localFile + " to file " + alluxioFilePath));
  }

}
