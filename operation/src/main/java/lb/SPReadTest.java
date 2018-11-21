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
 * This executable is used for testing the parallel read.
 */
public class SPReadTest {

  /** Directory for the test files. */
  public static final String TEST_PATH = "/lb_tests_files"; // path in alluxio

  private static final Logger LOG = LoggerFactory.getLogger(BasicOperations.class);
  private final OpenFileOptions mReadOptions;

  public SPReadTest() {
    mReadOptions = OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE);
  }

  public static void main(String[] args) throws Exception {

    AlluxioURI testDir = new AlluxioURI(TEST_PATH);

    FileSystem fs = FileSystem.Factory.get();
    SPReadTest test = new SPReadTest();

    if(args.length >0) {// read a given file only
      // todo: maintain a map from file name to its partition number
      // now we need to specify the partition number manually
      String fileName = args[0];
      int partNumber = Integer.parseInt(args[1]);
      System.out.println(fileName);
      for (int repeat=0; repeat<=10; repeat++) {
        test.doRead(fs, fileName, partNumber);
      }
      System.exit(0);
    }
    else{
      int ret = test.randomRead(fs);
      System.exit(ret);
    }
  }

  private int randomRead(FileSystem fs) throws Exception{
    // Test the effect of IO parallelism in a cluster, by distributing the file to different number of workers.
    int [] partNumbers = {1, 3, 5, 10, 15, 20, 25, 30}; // number of partitions
    // read files and repeat for a couple of times
    for (int repeat=0; repeat<=10; repeat++){
      for (int aPartNumber : partNumbers) {
        doRead(fs, String.format("%s", aPartNumber), aPartNumber);
      }
    }
    return 0;
  }

  private int doRead(FileSystem fs, String fileName, int partNumber) throws Exception{

    String log = System.getProperty("user.dir") + "/test_files/testIO.txt"; // log the read latency
    FileWriter fw = new FileWriter(log, true); //the true will append the new data
    AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, fileName));
    long timeTaken = readFileParallel(fs, filePath, partNumber);
    fw.write(fileName + "\t" + timeTaken + "\n");//append
    fw.close();
    return 0;
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
}
