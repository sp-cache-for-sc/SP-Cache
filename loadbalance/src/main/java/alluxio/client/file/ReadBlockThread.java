package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import org.apache.commons.collections.set.SynchronizedSet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by renfei on 2017/11/24.
 *
 * This class reads a single block into a part of a buffer.
 */
public class ReadBlockThread implements Runnable {

    private FileSystem mFileSystem;
    private AlluxioURI mFilePath;
    private OpenFileOptions mReadOptions;
    private int mPartID;
    private static final Logger LOG = LoggerFactory.getLogger(ReadBlockThread.class);
    private byte[] mBlockBuf;
    private final String mLog; // for cache hits
    private static final Double[] StragglerProb = {0.0266,
            0.0237,
            0.0207,
            0.0178,
            0.0266,
            0.0325,
            0.0414,
            0.0296,
            0.0385,
            0.0414,
            0.0414,
            0.0325,
            0.0325,
            0.0385,
            0.0444,
            0.0385,
            0.0355,
            0.0385,
            0.0296,
            0.0355,
            0.0385,
            0.0296,
            0.0325,
            0.0237,
            0.0207,
            0.0178,
            0.0207,
            0.0178,
            0.0118,
            0.0118,
            0.0089,
            0.0028,
            0.0118,
            0.0089,
            0.0028,
            0.0030,
            0.0059,
            0.0030,
            0.0028,
            0.0028,
            0.0028,
            0.0028,
            0.0059,
            0.0028,
            0.0089,
            0.0028,
            0.0028,
            0.0028,
            0.0028,
            0.0028,
            0.0028,
            0.0030,
            0.0028,
            0.0028,
            0.0028,
            0.0028,
            0.0027};
    private static final Double[] StragglerEffect={ // straggler factor: (2+this_value)
            1.4980,
            1.5385,
            1.5385,
            1.5587,
            1.5587,
            1.5587,
            1.5992,
            1.5992,
            1.6194,
            1.6802,
            1.7004,
            1.7409,
            1.7814,
            1.8219,
            1.8826,
            1.9231,
            1.9838,
            2.0445,
            2.1255,
            2.2267,
            2.3279,
            2.4494,
            2.5709,
            2.7328,
            2.8543,
            3.0162,
            3.1579,
            3.4008,
            3.5830,
            3.8057,
            4.0486,
            4.2510,
            4.4737,
            4.7166,
            4.9393,
            5.1822,
            5.3644,
            5.5668,
            5.7692,
            5.9717,
            6.1741,
            6.3968,
            6.6397,
            6.8421,
            7.0648,
            7.2874,
            7.5101,
            7.7733,
            8.0162,
            8.2996,
            8.5425,
            8.7652,
            8.9676,
            9.2510,
            9.4737,
            9.6761,
            9.8988
        };
    private static Integer[] ARRAY = new Integer[10000]; // the array to draw random integers from
    private static double PROB = 0.05; // the probability that a straggler occurs
    private static String PROBPATH = System.getProperty("user.home") + "/test_files/strag_prob.txt";

    static {
        int indexProb = 0;
        int indexRA=0;
        for(;indexProb<StragglerProb.length;indexProb++){
        int count = ((Double)(StragglerProb[indexProb]* 10000)).intValue();
            for(int i=0; i<count; i++){
                ARRAY[indexRA] = indexProb;
                indexRA++;
            }
        }
        //LOG.info("yinghao debug:" + Arrays.toString(ARRAY));
        try{
            FileReader fr = new FileReader(PROBPATH); //the true will append the new data
            BufferedReader br = new BufferedReader(fr);
            PROB = Double.parseDouble(br.readLine());
            if(PROB > 1.0  || PROB <0.0)
                System.out.println("The straggler probability should be in the range of [0,1]");
            System.exit(-1);
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }



    public ReadBlockThread(byte[] buf, AlluxioURI filePath, FileSystem fileSystem,
                           OpenFileOptions readOption, int partID) {
        mBlockBuf = buf;
        mPartID = partID;
        mFileSystem = fileSystem;
        mFilePath = filePath;
        mReadOptions = readOption;
        mLog = System.getProperty("user.dir") + "/logs/blockHit.txt"; // log the cache hits in the block level
    }


    public void run() {
        try {
            final long startTimeMs = CommonUtils.getCurrentMs();
            mReadOptions.setForSP(false);
            FileInStream is = mFileSystem.openFile(mFilePath, mReadOptions);
            long tOffset = mPartID * is.mBlockSize;
            // Seek into the file to the right position.
            is.seek(tOffset);
            // Find the real block size as the last block may not be full;
            long tBlockSize = is.mBlockSize;
            if ((is.mFileLength - is.mPos) < tBlockSize) {
                tBlockSize = is.mFileLength - is.mPos;
            }
            // Read the block into the corresponding position in the buffer.
            int tBytesRead = is.read(mBlockBuf, (int) tOffset, (int) tBlockSize);
            //System.out.println("yinghao debug:" + is.mCurrentBlockInStream.Source());
            // log the block level cache hits and misses
            FileWriter fw = new FileWriter(mLog, true); //the true will append the new data
            //is.mCurrentBlockInStream.

            if(is.mCurrentBlockInStream.Source() == BlockInStream.BlockInStreamSource.UFS) {
                fw.write("\tmiss\n");
            }
            else{ // local or remote
                fw.write("hit\n");
            }
            fw.close();
            LOG.info("Read a block with size:" + tBytesRead);
            is.close();
            long finishTimeMs = CommonUtils.getCurrentMs();
            LOG.info("Read block No." + mPartID + " with time (ms):" + (finishTimeMs - startTimeMs));

            // now insert straggler effect
            Random rand = new Random();
            if(rand.nextInt(100) >= PROB*100) // straggler does not occur
                return;
            else {
                LOG.info("yinghao debug:" + Arrays.toString(ARRAY));
                /////System.out.println("" + ARRAY[rand.nextInt(ARRAY.length)]);
                int effectIndex = ARRAY[rand.nextInt(ARRAY.length)]; // random number from 0 to length-1
                System.out.println("" + effectIndex);
                Thread.sleep((long)((finishTimeMs - startTimeMs) *(1+StragglerEffect[effectIndex]))); // sleep for 1+this_value times longer
                LOG.info("Straggler debug: straggler effect"+StragglerEffect[effectIndex] + "\t slept for (ms):" +  (finishTimeMs - startTimeMs) *(1+StragglerEffect[effectIndex]));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
