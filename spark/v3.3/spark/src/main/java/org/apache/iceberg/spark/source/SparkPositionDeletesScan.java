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
package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.Objects;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.PositionDeletesScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;

class SparkPositionDeletesScan extends SparkScan {
  private final String taskSetID;
  private final long splitSize;
  private final int splitLookback;
  private final long splitOpenFileCost;

  private List<ScanTaskGroup<PositionDeletesScanTask>> tasks = null; // lazy cache of tasks

  SparkPositionDeletesScan(SparkSession spark, Table table, SparkReadConf readConf) {
    super(spark, table, readConf, table.schema(), ImmutableList.of());

    this.taskSetID = readConf.fileScanTaskSetId();
    this.splitSize = readConf.splitSize();
    this.splitLookback = readConf.splitLookback();
    this.splitOpenFileCost = readConf.splitOpenFileCost();
  }

  @Override
  protected List<ScanTaskGroup<PositionDeletesScanTask>> taskGroups() {
    if (tasks == null) {

      PositionDeletesScanTaskSetManager taskSetManager = PositionDeletesScanTaskSetManager.get();
      List<PositionDeletesScanTask> files = taskSetManager.fetchTasks(table(), taskSetID);
      ValidationException.check(
          files != null,
          "Task set manager has no tasks for table %s with id %s",
          table(),
          taskSetID);

      CloseableIterable<PositionDeletesScanTask> splitFiles =
          TableScanUtil.splitScanTasks(CloseableIterable.withNoopClose(files), splitSize);
      CloseableIterable<ScanTaskGroup<PositionDeletesScanTask>> scanTasks =
          TableScanUtil.planTaskGroups(
              splitFiles, splitSize,
              splitLookback, splitOpenFileCost);

      this.tasks = Lists.newArrayList(scanTasks);
    }

    return tasks;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    SparkPositionDeletesScan that = (SparkPositionDeletesScan) other;
    return table().name().equals(that.table().name())
        && Objects.equals(taskSetID, that.taskSetID)
        && Objects.equals(splitSize, that.splitSize)
        && Objects.equals(splitLookback, that.splitLookback)
        && Objects.equals(splitOpenFileCost, that.splitOpenFileCost);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table().name(), taskSetID, splitSize, splitSize, splitOpenFileCost);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergPositionDeletessScan(table=%s, type=%s, taskSetID=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), taskSetID, caseSensitive());
  }
}
