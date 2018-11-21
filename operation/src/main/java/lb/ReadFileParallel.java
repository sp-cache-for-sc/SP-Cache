package lb;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.ReadBlockThread;
import alluxio.client.file.options.OpenFileOptions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by renfei on 2017/11/24.
 *
 * This class reads the file in parallel from k machines and assemble them into a single buffer.
 */
public class ReadFileParallel {

    private int mK;
    private FileSystem mFileSystem;
    private AlluxioURI mFilePath;
    private OpenFileOptions mReadOption;


    public ReadFileParallel(FileSystem fileSystem, AlluxioURI filePath, OpenFileOptions readOption,
                            int k) {
        mFileSystem = fileSystem;
        mFilePath = filePath;
        mReadOption = readOption;
        mK = k;
    }

    public int read() {

        ExecutorService executorService = Executors.newCachedThreadPool();

        for (int i = 0; i < mK; i++) {
            executorService.execute(new ReadBlockThread(mFilePath, mFileSystem,
                    mReadOption, i));
        }
        // todo: assemble?
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return 0;
    }

}
