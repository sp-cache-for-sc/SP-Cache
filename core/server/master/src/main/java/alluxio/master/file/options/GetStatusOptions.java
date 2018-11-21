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

package alluxio.master.file.options;


import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.thrift.GetStatusTOptions;
import alluxio.wire.LoadMetadataType;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for getting the status of a path.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class GetStatusOptions {
  private LoadMetadataType mLoadMetadataType;
  private boolean mForSP;

  /**
   * @return the default {@link GetStatusOptions}
   */
  public static GetStatusOptions defaults() {
    return new GetStatusOptions();
  }

  private GetStatusOptions() {
    mLoadMetadataType =
            Configuration.getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class);
    mForSP = false;
  }

  /**
   * Create an instance of {@link GetStatusOptions} from a {@link GetStatusTOptions}.
   *
   * @param options the thrift representation of getFileInfo options
   */
  public GetStatusOptions(GetStatusTOptions options) {
    mLoadMetadataType = LoadMetadataType.Once;
    if (options.isSetLoadMetadataType()) {
      mLoadMetadataType = LoadMetadataType.fromThrift(options.getLoadMetadataType());
      mForSP = options.isForSP();
    }
  }

  /**
   * @return the load metadata type
   */
  public LoadMetadataType getLoadMetadataType() {
    return mLoadMetadataType;
  }

  public boolean getForSP() {
    return mForSP;
  }

  /**
   * @param loadMetadataType the loadMetataType
   * @return the updated options
   */
  public GetStatusOptions setLoadMetadataType(LoadMetadataType loadMetadataType) {
    mLoadMetadataType = loadMetadataType;
    return this;
  }

  public GetStatusOptions setForSP(boolean forSP) {
    mForSP = forSP;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetStatusOptions)) {
      return false;
    }
    GetStatusOptions that = (GetStatusOptions) o;
    return Objects.equal(mLoadMetadataType, that.mLoadMetadataType) && that.mForSP == this.mForSP;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLoadMetadataType, mForSP);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("loadMetadataType", mLoadMetadataType.toString()).add("forSP", mForSP)
            .toString();
  }

  /**
   * @return thrift representation of the options
   */
  public GetStatusTOptions toThrift() {
    GetStatusTOptions options = new GetStatusTOptions();
    options.setLoadMetadataType(LoadMetadataType.toThrift(mLoadMetadataType));
    options.setForSP(mForSP);
    return options;
  }
}