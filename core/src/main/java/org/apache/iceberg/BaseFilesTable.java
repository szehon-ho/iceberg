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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;

/** Base class logic for files metadata tables */
abstract class BaseFilesTable extends BaseMetadataTable {

  static final Types.StructType READABLE_METRICS_VALUE =
      Types.StructType.of(
          optional(303, "column_size", Types.LongType.get(), "Total size on disk"),
          optional(304, "value_count", Types.LongType.get(), "Total count, including null and NaN"),
          optional(305, "null_value_count", Types.LongType.get(), "Null value count"),
          optional(306, "nan_value_count", Types.LongType.get(), "NaN value count"),
          optional(307, "lower_bound", Types.StringType.get(), "Lower bound in string form"),
          optional(308, "upper_bound", Types.StringType.get(), "Upper bound in string form"));

  static final Types.NestedField READABLE_METRICS =
      required(
          300,
          "readable_metrics",
          Types.MapType.ofRequired(301, 302, Types.StringType.get(), READABLE_METRICS_VALUE));

  BaseFilesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public Schema schema() {
    StructType partitionType = Partitioning.partitionType(table());
    Schema schema = new Schema(DataFile.getType(partitionType).fields());
    if (partitionType.fields().size() < 1) {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition
      // field
      schema = TypeUtil.selectNot(schema, Sets.newHashSet(DataFile.PARTITION_ID));
    }

    return TypeUtil.join(schema, new Schema(READABLE_METRICS));
  }

  private static CloseableIterable<FileScanTask> planFiles(
      Table table,
      CloseableIterable<ManifestFile> manifests,
      Schema tableSchema,
      Schema projectedSchema,
      TableScanContext context) {
    Expression rowFilter = context.rowFilter();
    boolean caseSensitive = context.caseSensitive();
    boolean ignoreResiduals = context.ignoreResiduals();

    LoadingCache<Integer, ManifestEvaluator> evalCache =
        Caffeine.newBuilder()
            .build(
                specId -> {
                  PartitionSpec spec = table.specs().get(specId);
                  PartitionSpec transformedSpec = BaseFilesTable.transformSpec(tableSchema, spec);
                  return ManifestEvaluator.forRowFilter(rowFilter, transformedSpec, caseSensitive);
                });

    CloseableIterable<ManifestFile> filteredManifests =
        CloseableIterable.filter(
            manifests, manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));

    String schemaString = SchemaParser.toJson(projectedSchema);
    String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
    Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : rowFilter;
    ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(filter);

    Map<Integer, String> fieldById = TypeUtil.indexNameById(table.schema().asStruct());

    return CloseableIterable.transform(
        filteredManifests,
        manifest ->
            new ManifestReadTask(
                table,
                manifest,
                tableSchema,
                projectedSchema,
                schemaString,
                specString,
                residuals,
                fieldById));
  }

  abstract static class BaseFilesTableScan extends BaseMetadataTableScan {

    protected BaseFilesTableScan(
        TableOperations ops, Table table, Schema schema, MetadataTableType tableType) {
      super(ops, table, schema, tableType);
    }

    protected BaseFilesTableScan(
        TableOperations ops,
        Table table,
        Schema schema,
        MetadataTableType tableType,
        TableScanContext context) {
      super(ops, table, schema, tableType, context);
    }

    /**
     * Returns an iterable of manifest files to explore for this files metadata table scan
     */
    protected abstract CloseableIterable<ManifestFile> manifests();

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      return BaseFilesTable.planFiles(table(), manifests(), tableSchema(), schema(), context());
    }
  }

  abstract static class BaseAllFilesTableScan extends BaseAllMetadataTableScan {

    protected BaseAllFilesTableScan(
        TableOperations ops, Table table, Schema schema, MetadataTableType tableType) {
      super(ops, table, schema, tableType);
    }

    protected BaseAllFilesTableScan(
        TableOperations ops,
        Table table,
        Schema schema,
        MetadataTableType tableType,
        TableScanContext context) {
      super(ops, table, schema, tableType, context);
    }

    /**
     * Returns an iterable of manifest files to explore for this all files metadata table scan
     */
    protected abstract CloseableIterable<ManifestFile> manifests();

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      return BaseFilesTable.planFiles(table(), manifests(), tableSchema(), schema(), context());
    }
  }

  static class ManifestReadTask extends BaseFileScanTask implements DataTask {
    private final FileIO io;
    private final Map<Integer, PartitionSpec> specsById;
    private final ManifestFile manifest;
    private final Schema dataTableSchema;
    private final Schema projectedSchema;
    private final Map<Integer, String> dataTableFields;
    private final boolean readableMetrics;

    ManifestReadTask(
        Table table,
        ManifestFile manifest,
        Schema schema,
        Schema projectedSchema,
        String schemaString,
        String specString,
        ResidualEvaluator residuals,
        Map<Integer, String> dataTableFields) {
      super(DataFiles.fromManifest(manifest), null, schemaString, specString, residuals);
      this.io = table.io();
      this.specsById = Maps.newHashMap(table.specs());
      this.manifest = manifest;
      this.dataTableSchema = table.schema();
      this.dataTableFields = dataTableFields;
//      this.isPartitioned = projectedSchema.findField(DataFile.PARTITION_ID) != null;
      this.projectedSchema = projectedSchema;
      this.readableMetrics = projectedSchema.findField(READABLE_METRICS.fieldId()) != null;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      return CloseableIterable.transform(
          manifestEntries(),
          fileEntry ->
              new FileEntryRow((StructLike) fileEntry,
                  readableMetrics ? MetricsUtil.readableMetricsMap(dataTableSchema, dataTableFields, fileEntry) : null));
    }

    private CloseableIterable<? extends ContentFile<?>> manifestEntries() {
      Schema fileProjection =
          TypeUtil.selectNot(projectedSchema, TypeUtil.getProjectedIds(READABLE_METRICS.type()));
      switch (manifest.content()) {
        case DATA:
          return ManifestFiles.read(manifest, io, specsById).project(fileProjection);
        case DELETES:
          return ManifestFiles.readDeleteManifest(manifest, io, specsById)
              .project(fileProjection);
        default:
          throw new IllegalArgumentException(
              "Unsupported manifest content type:" + manifest.content());
      }
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this); // don't split
    }

    @VisibleForTesting
    ManifestFile manifest() {
      return manifest;
    }

//    private List<Function<ContentFile<?>, Object>> accessors(boolean partitioned) {
//      List<Function<ContentFile<?>, Object>> accessors =
//          Lists.newArrayList(
//              file -> file.content().id(),
//              ContentFile::path,
//              file -> file.format().toString(),
//              ContentFile::specId,
//              ContentFile::partition,
//              ContentFile::recordCount,
//              ContentFile::fileSizeInBytes,
//              ContentFile::columnSizes,
//              ContentFile::valueCounts,
//              ContentFile::nullValueCounts,
//              ContentFile::nanValueCounts,
//              ContentFile::lowerBounds,
//              ContentFile::upperBounds,
//              ContentFile::keyMetadata,
//              ContentFile::splitOffsets,
//              ContentFile::equalityFieldIds,
//              ContentFile::sortOrderId,
//              file ->
//                  MetricsUtil.readableMetricsMap(
//                      dataTableSchema,
//                      dataTableFields,
//                      file));
//      return partitioned
//          ? accessors
//          : Stream.concat(
//                  accessors.subList(0, 4).stream(), accessors.subList(5, accessors.size()).stream())
//              .collect(Collectors.toList());
//    }
//
//    private List<Object> projectedFields(
//        ContentFile<?> file, List<Function<ContentFile<?>, Object>> accessors) {
//      Preconditions.checkArgument(
//          accessors.size() == filesTableSchema.columns().size(),
//          "There must be an accessor provided for every field in files metadata table, required %s but found %s",
//          filesTableSchema.columns().size(),
//          accessors.size());
//      List<Object> result = Lists.newArrayList();
//      int fieldIndex = 0;
//      for (Types.NestedField field : filesTableSchema.columns()) {
//        if (projectedSchema.findColumnName(field.fieldId()) != null) {
//          result.add(accessors.get(fieldIndex).apply(file));
//        }
//        fieldIndex++;
//      }
//      return result;
//    }
//  }
  }

  private static class FileEntryRow implements StructLike {
    private final StructLike fileAsStruct;
    private final Map<String, StructLike> readableMetrics;

    private FileEntryRow(StructLike fileAsStruct, Map<String, StructLike> readableMetrics) {
      this.fileAsStruct = fileAsStruct;
      this.readableMetrics = readableMetrics;
    }

    @Override
    public int size() {
      return readableMetrics == null ? fileAsStruct.size() : fileAsStruct.size() + 1;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      if (pos < fileAsStruct.size()) {
        return fileAsStruct.get(pos, javaClass);
      } else if (pos == fileAsStruct.size()) {
        if (readableMetrics == null) {
          return null;
        }
        return javaClass.cast(readableMetrics);
      } else {
        throw new IllegalArgumentException(String.format(
            "Illegal position access for FileRow: %d, max allowed is %d", pos, fileAsStruct.size()));
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("FileEntryRow is read only");
    }
  }
}
