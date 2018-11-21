package alluxio.worker.file;

/**
 * Created by yyuau on 7/9/2018.
 */

import alluxio.AlluxioURI;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;


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
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.client.file.ReadBlockThread.class);
  private byte[] mBlockBuf;
  private final String mLog; // for cache hits

  private static Integer[] ARRAY = new Integer[10000]; // the array to draw random integers from

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

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

