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

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.PropertyUtil;

public class PositionDeletesTableScan extends BaseMetadataTableScan {

  public PositionDeletesTableScan(TableOperations ops, Table table, Schema schema) {
    super(ops, table, schema, MetadataTableType.POSITION_DELETES);
  }

  PositionDeletesTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, MetadataTableType.POSITION_DELETES, context);
  }

  @Override
  protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    return new PositionDeletesTableScan(ops, table, schema, context);
  }

  @Override
  protected CloseableIterable<FileScanTask> doPlanFiles() {
    Expression rowFilter = context().rowFilter();
    String schemaString = SchemaParser.toJson(tableSchema());
    boolean ignoreResiduals = context().ignoreResiduals();
    Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : rowFilter;

    Map<Integer, PartitionSpec> transformedSpecs = table().specs()
        .entrySet()
        .stream()
        .map(e -> Pair.of(e.getKey(), BaseMetadataTable.transformSpec(tableSchema(), e.getValue())))
        .collect(Collectors.toMap(Pair::first, Pair::second));

    CloseableIterable<ManifestFile> deleteManifests = CloseableIterable.withNoopClose(
        snapshot().deleteManifests(tableOps().io()));
    CloseableIterable<CloseableIterable<FileScanTask>> results = CloseableIterable.transform(deleteManifests, m -> {

      // Filter partitions
      CloseableIterable<ManifestEntry<DeleteFile>> deleteFileEntries = ManifestFiles
          .readDeleteManifest(m, tableOps().io(), transformedSpecs)
          .filterRows(rowFilter)
          .liveEntries();

      // Filter delete file type
      CloseableIterable<ManifestEntry<DeleteFile>> positionDeleteEntries = CloseableIterable.filter(deleteFileEntries,
          entry -> entry.file().content().equals(FileContent.POSITION_DELETES));

      return CloseableIterable.transform(positionDeleteEntries, entry -> {
        PartitionSpec spec = transformedSpecs.get(entry.file().specId());
        ResidualEvaluator residuals = ResidualEvaluator.of(spec, filter, context().caseSensitive());
        String specString = PartitionSpecParser.toJson(spec);

        return new BaseFileScanTask(DataFiles.fromPositionDelete(entry.file(), spec),
            null, /* Deletes */
            schemaString,
            specString,
            residuals);
      });
    });

    return new ParallelIterable<>(results, planExecutor());
  }

  @Override
  public long targetSplitSize() {
    long tableValue = tableOps().current().propertyAsLong(
        TableProperties.SPLIT_SIZE,
        TableProperties.SPLIT_SIZE_DEFAULT);
    return PropertyUtil.propertyAsLong(options(), TableProperties.SPLIT_SIZE, tableValue);
  }
}
