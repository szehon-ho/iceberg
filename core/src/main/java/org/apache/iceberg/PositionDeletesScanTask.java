/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class PositionDeletesScanTask extends BaseScanTask<DeleteFile, PositionDeletesScanTask> {

  private transient PartitionSpec spec = null;

  public PositionDeletesScanTask(DeleteFile file, String schemaString, String specString,
                                 ResidualEvaluator residuals) {
    super(file, schemaString, specString, residuals);
    Preconditions.checkArgument(file.content().equals(FileContent.POSITION_DELETES), "Trying to construct position" +
        "delete scan task with %content file", file.content());
  }

  @Override
  public Iterable<PositionDeletesScanTask> split(long targetSplitSize) {
    if (file().format().isSplittable()) {
      if (file().splitOffsets() != null) {
        return () -> new OffsetsAwareTargetSplitSizeScanTaskIterator<>(file().splitOffsets(), this);
      } else {
        return () -> new FixedSizeSplitScanTaskIterator<>(targetSplitSize, this);
      }
    }
    return ImmutableList.of(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("delete_file", file().path())
        .add("partition_data", file().partition())
        .add("residual", residual())
        .toString();
  }
}
