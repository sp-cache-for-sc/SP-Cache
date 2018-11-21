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

import alluxio.*;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.SpecificHostsPolicy;
import alluxio.worker.file.ReadBlockThread;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.block.BlockWorker;
import alluxio.client.file.FileSystem;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is responsible for persisting files when requested by the master and a defunct
 * {@link FileSystemWorkerClientServiceHandler} which always returns UnsupportedOperation Exception.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class DefaultFileSystemWorker extends AbstractWorker implements FileSystemWorker {
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.<Class<? extends Server>>of(BlockWorker.class);

  /** Logic for managing file persistence. */
  private final FileDataManager mFileDataManager;
  /** Client for file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterWorkerClient;
  /** Logic for handling RPC requests. */
  private final FileSystemWorkerClientServiceHandler mServiceHandler;
  /** This worker's worker ID. May be updated by another thread if worker re-registration occurs. */
  private final AtomicReference<Long> mWorkerId;

  /** The service that persists files. */
  private Future<?> mFilePersistenceService;
  /** Handler to the ufs manager. */
  private final UfsManager mUfsManager;
  private static final Logger LOG = LoggerFactory.getLogger(DefaultFileSystemWorker.class);

  /**
   * Creates a new DefaultFileSystemWorker.
   *
   * @param blockWorker the block worker handle
   * @param ufsManager the ufs manager
   */
  DefaultFileSystemWorker(BlockWorker blockWorker, UfsManager ufsManager) {
    super(Executors.newFixedThreadPool(3,
        ThreadFactoryUtils.build("file-system-worker-heartbeat-%d", true)));
    mWorkerId = blockWorker.getWorkerId();
    mUfsManager = ufsManager;
    mFileDataManager = new FileDataManager(Preconditions.checkNotNull(blockWorker),
        RateLimiter.create(Configuration.getBytes(PropertyKey.WORKER_FILE_PERSIST_RATE_LIMIT)),
        mUfsManager);

    // Setup AbstractMasterClient
    mFileSystemMasterWorkerClient = new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));

    mServiceHandler = new FileSystemWorkerClientServiceHandler(this);


  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public String getName() {
    return Constants.FILE_SYSTEM_WORKER_NAME;
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME,
        new FileSystemWorkerClientService.Processor<>(mServiceHandler));
    return services;
  }

  @Override
  public void start(WorkerNetAddress address) {
    mFilePersistenceService = getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC,
            new FileWorkerMasterSyncExecutor(mFileDataManager, mFileSystemMasterWorkerClient,
                mWorkerId),
            Configuration.getInt(PropertyKey.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS)));
  }

  @Override
  public void stop() {
    if (mFilePersistenceService != null) {
      mFilePersistenceService.cancel(true);
    }
    // The executor shutdown needs to be done in a loop with retry because the interrupt
    // signal can sometimes be ignored.
    CommonUtils.waitFor("file system worker executor shutdown", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        getExecutorService().shutdownNow();
        try {
          return getExecutorService().awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
    mFileSystemMasterWorkerClient.close();
  }

  public void repartition(int fileId, List<Integer> workerIndices){
    // read the file
    LOG.info("Re-collecting file" + fileId);
    FileSystem fs = FileSystem.Factory.get();
    String filePath = String.format("/tests/%s",fileId);
    AlluxioURI fileUri = new AlluxioURI(filePath);
    OpenFileOptions readOption = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
    try {
      FileInStream is = fs.openFile(fileUri, readOption);
      long fileSize = is.mStatus.getLength();
      is.close();
      byte[] fileBuf = new byte[(int) fileSize];
      ExecutorService executorService = Executors.newCachedThreadPool();


      //LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)
      final long startTimeMs = CommonUtils.getCurrentMs();
      int prevK = fs.getStatus(fileUri).getKValueForSP();
      for (int i = 0; i < prevK; i++) {
        //System.out.print("Reading block #" + i);
        executorService.execute(new ReadBlockThread(fileBuf, fileUri, fs, readOption, i));
      }

      executorService.shutdown();
      try {
        executorService.awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      // delete the original file
      LOG.info("Deleting the old file" + fileId);
      fs.delete(new AlluxioURI(filePath));
      
      //write the new file
      //System.out.println("New worker indices" + workerIndices.toString());
      int k = workerIndices.size();
      int blockSize = (int)Math.ceil((double)(fileSize)/k);
      LOG.info("Rewriting file" + fileId + " into " + workerIndices.size() + " partitions.");
      CreateFileOptions writeOptions = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)
              .setBlockSizeBytes(blockSize)
              .setLocationPolicy(new SpecificHostsPolicy(workerIndices));

      FileOutStream os = fs.createFile(fileUri, writeOptions);
      os.write(fileBuf);
      os.close();

    }catch(Exception e){
      e.printStackTrace();
    }
  }


}
