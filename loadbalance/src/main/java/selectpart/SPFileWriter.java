package selectpart;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.policy.RandomUniquePolicy;
import alluxio.examples.BasicOperations;
import alluxio.exception.AlluxioException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by renfei on 2017/11/24.
 *
 * This class is the client writer that writes files into Alluxio, given k and the byte[] to write.
 */
public class SPFileWriter {

    protected int mK;
    protected int mFileSize;
    private CreateFileOptions mWriteOptions;
    private FileSystem mFileSystem;
    private AlluxioURI mFilePath;
    private static final Logger LOG = LoggerFactory.getLogger(SPFileWriter.class);

    public SPFileWriter(int k, int fileSize, FileSystem fileSystem,
                        AlluxioURI writePath) {
        mK = k;
        mFileSize = fileSize;
        mFileSystem = fileSystem;
        mFilePath = writePath;
        long tBlockSize;
        if (fileSize % k == 0) {
            tBlockSize = fileSize / mK;
        } else {
            // NOTE: Assume that file size in bytes is much larger than the number of machines
            tBlockSize = fileSize / mK + 1;
        }
        mWriteOptions = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)
                .setBlockSizeBytes(tBlockSize)
                .setLocationPolicy(new RandomUniquePolicy())
                .setKValueForSP(mK);
        LOG.info("k value after set in write is " + mWriteOptions.getKValueForSP());
    }

    public void setWriteOption(CreateFileOptions writeOptions) {
        mWriteOptions = writeOptions;
    }

    public void writeFile(byte[] buf) {

        try {
            // the write policy is uniquely random
            FileOutStream os = mFileSystem.createFile(mFilePath, mWriteOptions);
            os.write(buf);
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (AlluxioException e) {
            e.printStackTrace();
        }
    }

}
