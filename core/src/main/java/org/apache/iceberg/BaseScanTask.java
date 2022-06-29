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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

abstract class BaseScanTask<FileT extends ContentFile<FileT>, ScanTaskT extends ContentScanTask<FileT>>
    implements ContentScanTask<FileT>, SplittableScanTask<ScanTaskT> {
  private final FileT file;
  private final String schemaString;
  private final String specString;
  private final ResidualEvaluator residuals;

  private transient PartitionSpec spec = null;

  public BaseScanTask(FileT file, String schemaString, String specString,
                      ResidualEvaluator residuals) {
    this.file = file;
    this.schemaString = schemaString;
    this.specString = specString;
    this.residuals = residuals;
  }

  @Override
  public FileT file() {
    return file;
  }

  @Override
  public PartitionSpec spec() {
    if (spec == null) {
      this.spec = PartitionSpecParser.fromJson(SchemaParser.fromJson(schemaString), specString);
    }
    return spec;
  }

  @Override
  public long start() {
    return 0;
  }

  @Override
  public long length() {
    return file.fileSizeInBytes();
  }

  @Override
  public Expression residual() {
    return residuals.residualFor(file.partition());
  }

  @Override
  public abstract Iterable<ScanTaskT> split(long targetSplitSize);

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("file", file.path())
        .add("partition_data", file.partition())
        .add("residual", residual())
        .toString();
  }

  /**
   * This iterator returns {@link FileScanTask} using guidance provided by split offsets.
   */
  @VisibleForTesting
  static final class OffsetsAwareTargetSplitSizeScanTaskIterator<FileT extends ContentFile<FileT>, ScanTaskT extends ContentScanTask<FileT>>
      implements Iterator<ScanTaskT> {
    private final List<Long> offsets;
    private final List<Long> splitSizes;
    private final BaseScanTask<FileT, ScanTaskT> parentScanTask;
    private int sizeIdx = 0;

    OffsetsAwareTargetSplitSizeScanTaskIterator(List<Long> offsetList, BaseScanTask<FileT, ScanTaskT> parentScanTask) {
      this.offsets = ImmutableList.copyOf(offsetList);
      this.parentScanTask = parentScanTask;
      this.splitSizes = Lists.newArrayListWithCapacity(offsets.size());
      if (offsets.size() > 0) {
        int lastIndex = offsets.size() - 1;
        for (int index = 0; index < lastIndex; index++) {
          splitSizes.add(offsets.get(index + 1) - offsets.get(index));
        }
        splitSizes.add(parentScanTask.length() - offsets.get(lastIndex));
      }
    }

    @Override
    public boolean hasNext() {
      return sizeIdx < splitSizes.size();
    }

    @Override
    public ScanTaskT next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      int offsetIdx = sizeIdx;
      long currentSize = splitSizes.get(sizeIdx);
      sizeIdx += 1; // Create 1 split per offset
      return (ScanTaskT) new SplitScanTask(offsets.get(offsetIdx), currentSize, parentScanTask);
    }

  }

  @VisibleForTesting
  static final class FixedSizeSplitScanTaskIterator<FileT extends ContentFile<FileT>, ScanTaskT extends ContentScanTask<FileT>> implements Iterator<ScanTaskT> {
    private long offset;
    private long remainingLen;
    private long splitSize;
    private final BaseScanTask<FileT, ScanTaskT> fileScanTask;

    FixedSizeSplitScanTaskIterator(long splitSize, BaseScanTask<FileT, ScanTaskT> fileScanTask) {
      this.offset = 0;
      this.remainingLen = fileScanTask.length();
      this.splitSize = splitSize;
      this.fileScanTask = fileScanTask;
    }

    @Override
    public boolean hasNext() {
      return remainingLen > 0;
    }

    @Override
    public ScanTaskT next() {
      long len = Math.min(splitSize, remainingLen);
      final SplitScanTask splitTask = new SplitScanTask(offset, len, fileScanTask);
      offset += len;
      remainingLen -= len;
      return (ScanTaskT) splitTask;
    }
  }

  private static final class SplitScanTask<FileT extends ContentFile<FileT>, ScanTaskT extends ContentScanTask<FileT>>
      implements ContentScanTask<FileT>, MergeableScanTask<SplitScanTask>, SplittableScanTask<ScanTaskT> {
    private final long len;
    private final long offset;
    private final BaseScanTask<FileT, ScanTaskT> fileScanTask;

    SplitScanTask(long offset, long len, BaseScanTask<FileT, ScanTaskT> fileScanTask) {
      this.offset = offset;
      this.len = len;
      this.fileScanTask = fileScanTask;
    }

    @Override
    public FileT file() {
      return fileScanTask.file();
    }

    @Override
    public PartitionSpec spec() {
      return fileScanTask.spec();
    }

    @Override
    public long start() {
      return offset;
    }

    @Override
    public long length() {
      return len;
    }

    @Override
    public Expression residual() {
      return fileScanTask.residual();
    }

    @Override
    public Iterable<ScanTaskT> split(long splitSize) {
      throw new UnsupportedOperationException("Cannot split a task which is already split");
    }

    @Override
    public boolean canMerge(ScanTask other) {
      if (other instanceof SplitScanTask) {
        SplitScanTask that = (SplitScanTask) other;
        return file().equals(that.file()) && offset + len == that.start();
      } else {
        return false;
      }
    }

    @Override
    public SplitScanTask merge(ScanTask other) {
      SplitScanTask that = (SplitScanTask) other;
      return new SplitScanTask(offset, len + that.length(), fileScanTask);
    }
  }
}
