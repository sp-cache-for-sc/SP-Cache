package sptests;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.SPFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by yyuau on 28/12/2017.
 */
public class SPReadExecutor implements Callable<Long> {

    private static final Logger LOG = LoggerFactory.getLogger(SPReadExecutor.class);
    private AlluxioURI mAlluxioURI;
    private FileSystem mFileSystem = FileSystem.Factory.get();
    private SPFileReader mSPFileReader;
    public static final String TEST_PATH = "/tests";

    private static final String logDirPath = System.getProperty("user.dir") + "/logs";

    private static FileWriter mLog;

    public SPReadExecutor(AlluxioURI alluxioURI) throws Exception {
        // mFileSystem = fileSystem;
        mAlluxioURI = alluxioURI;
        mSPFileReader = new SPFileReader(mFileSystem, alluxioURI);

        File tLogDir = new File(logDirPath);
        if (!tLogDir.exists()){
            tLogDir.mkdir();
        }
        mLog = new FileWriter(logDirPath + "/readLatency.txt", true); // append
    }

    public Long call() throws Exception {
        return mSPFileReader.runRead();
    }


    // test the function of a single read executor
    public static void main(String[] args) throws Exception {
        String fileName = args[0];
        AlluxioURI alluxioURI = new AlluxioURI(String.format("%s/%s", TEST_PATH, fileName));
        ExecutorService executor = Executors.newCachedThreadPool();
        Future<Long> future =executor.submit(new SPReadExecutor(alluxioURI));
        Long tTimeTaken = future.get(); // block
        // Thread.sleep(5000L); // for debug
        LOG.info("spisbetter: Read " + fileName + " in " + tTimeTaken + " ms.");
        synchronized (mLog){
            mLog.write(String.format("%s\t%s\n",fileName, tTimeTaken));
        }
        executor.shutdown();
        mLog.close();
    }
}
