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

import java.util.List;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class BaseFileScanTask extends BaseScanTask<DataFile, FileScanTask> implements FileScanTask {
  private final DeleteFile[] deletes;

  private transient PartitionSpec spec = null;

  public BaseFileScanTask(DataFile file, DeleteFile[] deletes, String schemaString, String specString,
                   ResidualEvaluator residuals) {
    super(file, schemaString, specString, residuals);
    this.deletes = deletes != null ? deletes : new DeleteFile[0];
  }

  @Override
  public List<DeleteFile> deletes() {
    return ImmutableList.copyOf(deletes);
  }

  @Override
  public Iterable<FileScanTask> split(long targetSplitSize) {
    if (file().format().isSplittable()) {
      if (file().splitOffsets() != null) {
        return () -> new BaseScanTask.OffsetsAwareTargetSplitSizeScanTaskIterator<>(file().splitOffsets(), this);
      } else {
        return () -> new BaseScanTask.FixedSizeSplitScanTaskIterator<>(targetSplitSize, this);
      }
    }
    return ImmutableList.of(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("file", file().path())
        .add("delete_files", Joiner.on(",").join(deletes))
        .add("partition_data", file().partition())
        .add("residual", residual())
        .toString();
  }
}
