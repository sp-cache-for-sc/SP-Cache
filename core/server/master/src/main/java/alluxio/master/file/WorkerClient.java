package alluxio.master.file;

import alluxio.AbstractClient;
import alluxio.Constants;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.thrift.RepartitionCommandTOptions;
import alluxio.thrift.RepartitionTOptions;
import org.apache.commons.io.IOExceptionWithCause;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by yyuau on 6/9/2018.
 */

public class WorkerClient extends AbstractClient{
  private static final Logger LOG = LoggerFactory.getLogger(WorkerClient.class);

  private FileSystemWorkerClientService.Client mClient = null;

  private long mWorkId;

  public WorkerClient(Subject subject, InetSocketAddress address, long workerId) {
    super(subject, address);
    mWorkId = workerId;
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME;//FILE_SYSTEM_WORKER_MASTER_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = new FileSystemWorkerClientService.Client(mProtocol);
  }

  public Void repartitionCommand(final int fileId, final List<Integer> workerIndices) throws IOException{
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException{
        LOG.info("Repartition request to be sent via WorkerClient: fileId " + fileId + " workerIndices " + workerIndices.toString());
        mClient.RepartitionCommand(fileId, workerIndices, new RepartitionCommandTOptions());
        LOG.info("Repartition request sent out via WorkerClient.");
        return null;
      }
    });
    return null;
  }

}



