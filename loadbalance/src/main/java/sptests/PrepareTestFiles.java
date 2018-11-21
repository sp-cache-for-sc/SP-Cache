package sptests;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.DeleteOptions;
import alluxio.examples.BasicOperations;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import selectpart.SPFileWriter;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by yyuau on 4/1/2018.
 */
public class PrepareTestFiles {
    private static final Logger LOG = LoggerFactory.getLogger(BasicOperations.class);


    public static final String TEST_PATH = "/tests"; //Directory in Alluxio for the test files.
    private final String mLocalFile = System.getProperty("user.home") + "/test_files/test_local_file";


    private ArrayList<Integer> mKValues = new ArrayList<Integer>();
    private String KValuePath = System.getProperty("user.home") + "/test_files/k.txt"; // k values
    private ArrayList<AlluxioURI> mURIList = new ArrayList<AlluxioURI>();

    public PrepareTestFiles() {} // for default paths
    public PrepareTestFiles(String kValuePath) {

        KValuePath = kValuePath;
    }

    private boolean writeFiles() throws Exception {
        // read the POPULARITY and KVALUE data
        System.out.println("K values path: " + KValuePath);
        String line;
        BufferedReader br = new BufferedReader(new FileReader(KValuePath));
        while ((line = br.readLine()) != null) {
            mKValues.add(Integer.parseInt(line));
        }
        br.close();

        //generate the test files given kValues.
        AlluxioURI testDir = new AlluxioURI(TEST_PATH);
        FileSystem fs = FileSystem.Factory.get();
        if (fs.exists(testDir)) {
            fs.delete(testDir, DeleteOptions.defaults().setRecursive(true).setUnchecked(true));
        }
        FileInputStream is = new FileInputStream(mLocalFile);
        int tFileLength = (int) new File(mLocalFile).length();
        byte[] tBuf = new byte[tFileLength];
        int tBytesRead = is.read(tBuf);
        is.close();
        for (int fileIndex = 0; fileIndex < mKValues.size(); fileIndex++){
            AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, fileIndex));
            //copyFromLocal(filePath, mKValues.get(fileIndex), fs, mLocalFile);
            copyFromBuffer(filePath,mKValues.get(fileIndex),fs,tBuf);
        }
        fs.getK();
        return true;

    }
    public static void main(String[] args) throws Exception {
        PrepareTestFiles operator;
        if(args.length >0){
            operator = new PrepareTestFiles(args[0]);
        }else {
            operator = new PrepareTestFiles();
        }
        operator.writeFiles();
        System.exit(0);
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

    // Write the file with an in-memory byte buffer (should be faster than copying local file on disk)
    private void copyFromBuffer(AlluxioURI writePath, int k, FileSystem fileSystem, byte[] buf)
            throws IOException, AlluxioException {
        long tStartTimeMs = CommonUtils.getCurrentMs();
        int tFileLength = buf.length;
        SPFileWriter tSPWriter = new SPFileWriter(k, tFileLength, fileSystem, writePath);
        tSPWriter.writeFile(buf);
        LOG.info(FormatUtils.formatTimeTakenMs(tStartTimeMs,
                "writing" + writePath));
    }

}
