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

package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;

/**
 * Return the specific workers by ids; special for the repartition scheme of SP-Cache.
 */

@ThreadSafe
public final class SpecificHostsPolicy implements FileWriteLocationPolicy, BlockLocationPolicy {
  private final List<Integer> mWorkerIndices;
  //private boolean mInitialized = false;
  private int mIndex=0;
  /**
   * Constructs the policy with the hostname.
   *
   * @param workerIndices the ids of the host workers
   */
  public SpecificHostsPolicy(List<Integer> workerIndices) {
    mWorkerIndices = new ArrayList<Integer>(workerIndices);
  }


  @Override
  public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) {
    // find the next worker
    if(mIndex>= mWorkerIndices.size()){
      mIndex =0;
    }
    int nextWorkerIndex = mWorkerIndices.get(mIndex++);
    List<BlockWorkerInfo> mWorkerInfoList = Lists.newArrayList(workerInfoList);
    return mWorkerInfoList.get(nextWorkerIndex).getNetAddress();
  }

  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    return getWorkerForNextBlock(options.getBlockWorkerInfos(), options.getBlockSize());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SpecificHostsPolicy)) {
      return false;
    }
    SpecificHostsPolicy that = (SpecificHostsPolicy) o;
    return Objects.equal(mWorkerIndices, that.mWorkerIndices);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWorkerIndices);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("worker indices", mWorkerIndices)
        .toString();
  }
}
