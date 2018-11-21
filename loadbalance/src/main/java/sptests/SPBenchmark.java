package sptests;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.RandomUniquePolicy;
import alluxio.client.file.SPFileReader;
import alluxio.examples.BasicOperations;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import selectpart.SPFileWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 * Created by yinghao on 2017/12/25.
 *
 * Benchmarks the performance of SP, given popularity and the k values decided a priori.
 */
public class SPBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(BasicOperations.class);


    public static final String TEST_PATH = "/tests"; //Directory in Alluxio for the test files.
    private final String mLocalFile = System.getProperty("user.dir") + "/test_files/512MB.zip";

    private ArrayList<Double> mPopularities =  new ArrayList<Double>();
    private String POPULARITY = System.getProperty("user.dir") + "/test_files/popularity.txt"; // normalized popularity
    private ArrayList<Integer> mKValues = new ArrayList<Integer>();
    private String KVALUE = System.getProperty("user.dir") + "/test_files/k.txt"; // k values
    private ArrayList<AlluxioURI> mURIList = new ArrayList<AlluxioURI>();

    private int mRepeatTime = 100;
    public SPBenchmark() {} // for default paths
    public SPBenchmark(String popularity, String kValue) throws IOException {

        POPULARITY = popularity;
        KVALUE = kValue;
    }

    public static void main(String[] args) throws Exception {

        SPBenchmark operator = new SPBenchmark();
        if(!operator.prepare())
        {
            System.exit(-1);
        }
        // todo: generate file requests according to the popularity distribution. launch the reads with multiple threads
        // todo: log the time here
        System.exit(0);
    }
    /**
    public Long call() throws Exception {
        SPFileReader fileReader = new SPFileReader(fs, alluxioURI);
        return fileReader.runRead();

    }
     **/
    private boolean prepare() throws Exception {
        // read the POPULARITY and KVALUE data
        BufferedReader br = new BufferedReader(new FileReader(POPULARITY));
        String line;
        while ((line = br.readLine()) != null) {
            mPopularities.add(Double.parseDouble(line));
        }
        br.close();
        br = new BufferedReader(new FileReader(KVALUE));
        while ((line = br.readLine()) != null) {
            mKValues.add(Integer.parseInt(line));
        }
        br.close();

        if(mPopularities.size() != mKValues.size()) {
            //false alarm
            System.out.print("The lengths of popularity and kValues do not match.");
            return false;
        }

        //generate the test files given kValues.
        AlluxioURI testDir = new AlluxioURI(TEST_PATH);
        FileSystem fs = FileSystem.Factory.get();
        if (fs.exists(testDir)) {
            fs.delete(testDir, DeleteOptions.defaults().setRecursive(true).setUnchecked(true));
        }
        for (int fileIndex = 0; fileIndex < mKValues.size(); fileIndex++){
            AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, fileIndex));
            copyFromLocal(filePath, mKValues.get(fileIndex), fs, mLocalFile);
        }
        return true;

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

