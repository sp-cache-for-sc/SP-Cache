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
import com.google.common.collect.Lists;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Created by renfei on 2017/11/22.
 *
 * Policy module to ensure that each block of these files are placed on unique (uniform) randomly chosen servers.
 */
@NotThreadSafe
public final class RandomUniquePolicy implements FileWriteLocationPolicy, BlockLocationPolicy {
    private List<BlockWorkerInfo> mWorkerInfoList;
    private int mIndex;
    private boolean mInitialized = false;
    /** This caches the {@link WorkerNetAddress} for the block IDs.*/
    private final HashMap<Long, WorkerNetAddress> mBlockLocationCache = new HashMap<>();

    /**
     * Constructs a new {@link RandomUniquePolicy}.
     */
    public RandomUniquePolicy() {}

    @Override
    public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList, long blockSizeBytes) {
        if (!mInitialized) {
            mWorkerInfoList = Lists.newArrayList(workerInfoList);
            Collections.shuffle(mWorkerInfoList);
            mIndex = 0;
            mInitialized = true;
        }

        WorkerNetAddress candidate = mWorkerInfoList.get(mIndex).getNetAddress();
        mIndex = mIndex + 1;
        if (mIndex >= mWorkerInfoList.size()) {
            Collections.shuffle(mWorkerInfoList);
            mIndex = mIndex % mWorkerInfoList.size();
        }

        return candidate;
    }

    @Override
    public WorkerNetAddress getWorker(GetWorkerOptions options) {
        WorkerNetAddress address = mBlockLocationCache.get(options.getBlockId());
        if (address != null) {
            return address;
        }
        address = getWorkerForNextBlock(options.getBlockWorkerInfos(), options.getBlockSize());
        mBlockLocationCache.put(options.getBlockId(), address);
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RandomUniquePolicy)) {
            return false;
        }
        RandomUniquePolicy that = (RandomUniquePolicy) o;
        return Objects.equal(mWorkerInfoList, that.mWorkerInfoList)
                && Objects.equal(mIndex, that.mIndex)
                && Objects.equal(mInitialized, that.mInitialized)
                && Objects.equal(mBlockLocationCache, that.mBlockLocationCache);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(mWorkerInfoList, mIndex, mInitialized, mBlockLocationCache);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("workerInfoList", mWorkerInfoList)
                .add("index", mIndex)
                .add("initialized", mInitialized)
                .add("blockLocationCache", mBlockLocationCache)
                .toString();
    }

}
