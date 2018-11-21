package alluxio.master.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by yuyinghao on 9/11/18.
 */
public class RepartitionThread implements Callable {
    int mfileId;
    List<Integer> mWorkerIndices;
    WorkerClient mClient;
    public RepartitionThread(WorkerClient client, int fileId, List<Integer> workerIndices){
        mfileId = fileId;
        mWorkerIndices = new ArrayList<Integer>(workerIndices);
        mClient = client;
    }
    public Void call() throws IOException {
        mClient.repartitionCommand(mfileId, mWorkerIndices);
        return null;
    }
}
