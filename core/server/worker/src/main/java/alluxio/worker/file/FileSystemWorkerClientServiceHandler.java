/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.file;

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.exception.AlluxioException;
import alluxio.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Handles incoming thrift requests from a worker file system client. These RPCs are no longer
 * supported as of 1.5.0.  All methods will throw {@link UnsupportedOperationException}.
 */
@NotThreadSafe
public final class FileSystemWorkerClientServiceHandler
    implements FileSystemWorkerClientService.Iface{
  private static final String UNSUPPORTED_MESSAGE = "Unsupported as of version 1.5.0";
  private static final Logger LOG =
          LoggerFactory.getLogger(FileSystemWorkerClientServiceHandler.class);
  private DefaultFileSystemWorker mFSWorker;
  /**
   * Creates a new instance of this class.
   */
  FileSystemWorkerClientServiceHandler() {}
  FileSystemWorkerClientServiceHandler(DefaultFileSystemWorker worker) {
    mFSWorker = worker;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public CancelUfsFileTResponse cancelUfsFile(final long sessionId, final long tempUfsFileId,
      final CancelUfsFileTOptions options) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public CloseUfsFileTResponse closeUfsFile(final long sessionId, final long tempUfsFileId,
      final CloseUfsFileTOptions options) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public CompleteUfsFileTReponse completeUfsFile(final long sessionId, final long tempUfsFileId,
      final CompleteUfsFileTOptions options) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public CreateUfsFileTResponse createUfsFile(final long sessionId, final String ufsUri,
      final CreateUfsFileTOptions options) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public OpenUfsFileTResponse openUfsFile(final long sessionId, final String ufsUri,
      final OpenUfsFileTOptions options) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public SessionFileSystemHeartbeatTResponse sessionFileSystemHeartbeat(final long sessionId,
      final List<Long> metrics, final SessionFileSystemHeartbeatTOptions options)
      throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public RepartitionCommandTResponse RepartitionCommand(final int fileId, final List<Integer> workerIndices, final RepartitionCommandTOptions options) throws AlluxioTException {
    return RpcUtils.callAndLog(LOG, new RpcUtils.RpcCallableThrowsIOException<RepartitionCommandTResponse>() {
      @Override
      public RepartitionCommandTResponse call() throws AlluxioException, IOException {
        LOG.info("Repartition Command Received at worker client handler");
        System.out.println("WorkerClientHandler: file id is: " + fileId);
        if(workerIndices == null){
          System.out.println("WorkerClientHandler: worker indice is null.");
        } else{
          System.out.println("WorkerClientHandler: file id is: " + workerIndices);
        }

        mFSWorker.repartition(fileId, workerIndices);
        return new RepartitionCommandTResponse();
      }
    });

  }
}
